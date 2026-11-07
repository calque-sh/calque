//! The mutation route `calque dev` and `calque serve` mount.
//!
//! `calque dev` serves one `crud:<table>` command per enrolled table, and
//! `calque serve` the commands its manifest advertises, through the shared
//! `mutation-http` router. A request is authorized the way bootstrap and
//! change requests are — the bearer names the principal, the policy derives
//! its scope, the scope token binds it to a plan this process granted — and
//! executed by `mutation-api` on one dedicated writer connection. The change
//! stream resolves the durable receipts execution leaves into
//! acknowledgements, so a mutation is confirmed by its CDC echo and by nothing
//! else.

use std::{future::Future, sync::Arc};

use mutation_api::{
    execute_batch, install_metadata, CommandCapability, CommandOperation, CommandRegistry,
    MutationBatchError,
};
use mutation_crud::{
    crud_command_summaries_per_table, register_crud_commands_per_table, CrudOperationName,
    CrudOperations, PerTableOperations,
};
use mutation_http::{BearerToken, MutationHttpBackend, MutationHttpError};
use postgres_connect::DropOffRuntime;
use sync_protocol::mutation::{MutationBatchRequest, MutationBatchResponse};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    guard::PostgresConnectionFamily, source::connect_owned, DatabaseUrl, LocalBackend,
    LocalSourceError, PreparedLocalSource,
};

#[derive(Debug, Error)]
pub enum LocalMutationError {
    #[error(transparent)]
    Source(#[from] LocalSourceError),
    #[error("mutation support could not be installed: {0}")]
    Mutation(#[from] MutationBatchError),
    #[error("mutation support is already enabled for this backend")]
    AlreadyEnabled,
}

/// The writer connection, the receipt metadata and the command registry the
/// mutation route and the change stream's acknowledgements share.
pub struct LocalMutationSupport {
    /// The last clone of the shared support can die on the runtime thread.
    writer: Mutex<DropOffRuntime<postgres::Client>>,
    registry: CommandRegistry,
    /// The commands and operations `registry` holds, derived from the same
    /// walk that registered them, for `MutationCommandPolicy::SharedBearer`.
    capability: CommandCapability,
}

impl LocalMutationSupport {
    /// Opens the writer connection, installs the receipt metadata it needs
    /// (before any request can reach the route) and registers a crud command
    /// for every enrolled table the v1 key codec can address, admitting every
    /// operation the catalog offers for it.
    ///
    /// This is the loopback harness's mount. A deployment that advertises a
    /// narrower surface per collection uses [`Self::open_with_operations`], so
    /// that the narrowing holds inside the registered command too.
    pub fn open(
        database_url: &DatabaseUrl,
        prepared: &PreparedLocalSource,
    ) -> Result<Self, LocalMutationError> {
        Self::open_with_operations(
            database_url,
            prepared,
            &PerTableOperations::Uniform(CrudOperations::ALL),
        )
    }

    /// `open`, with each table's command admitting exactly the operations this
    /// deployment mounts it with.
    ///
    /// A table `operations` does not name registers no command at all, rather
    /// than relying on the capability check alone.
    pub fn open_with_operations(
        database_url: &DatabaseUrl,
        prepared: &PreparedLocalSource,
        operations: &PerTableOperations,
    ) -> Result<Self, LocalMutationError> {
        let mut client = connect_owned(
            database_url,
            PostgresConnectionFamily::MutationWriter,
            "calque-mutations",
        )?;
        install_metadata(&mut client)?;
        let mut registry = CommandRegistry::default();
        register_crud_commands_per_table(&mut registry, prepared.registry.tables(), operations)?;
        let capability = CommandCapability::new(
            crud_command_summaries_per_table(prepared.registry.tables(), operations)
                .into_iter()
                .map(|summary| {
                    (
                        summary.name,
                        summary
                            .operations
                            .into_iter()
                            .map(command_operation)
                            .collect(),
                    )
                }),
        );
        Ok(Self {
            writer: Mutex::new(DropOffRuntime::new(client)),
            registry,
            capability,
        })
    }

    /// The capability naming exactly what this support registered.
    pub fn registered_capability(&self) -> CommandCapability {
        self.capability.clone()
    }

    /// Runs `work` on the writer connection from a blocking context.
    pub(crate) fn with_client<T>(&self, work: impl FnOnce(&mut postgres::Client) -> T) -> T {
        work(&mut self.writer.blocking_lock())
    }
}

/// `mutation-http`'s backend for the local harness.
#[derive(Clone)]
pub struct LocalMutationBackend {
    backend: LocalBackend,
    support: Arc<LocalMutationSupport>,
}

impl LocalMutationBackend {
    /// Mounts mutation support on `backend`: the route executes through it
    /// and the backend's change stream resolves acknowledgements through it.
    pub fn new(
        backend: LocalBackend,
        support: LocalMutationSupport,
    ) -> Result<Self, LocalMutationError> {
        let support = Arc::new(support);
        backend.enable_mutations(Arc::clone(&support))?;
        Ok(Self { backend, support })
    }
}

impl MutationHttpBackend for LocalMutationBackend {
    fn admit(
        &self,
        bearer: &BearerToken,
    ) -> impl Future<Output = Result<(), MutationHttpError>> + Send {
        // Resolved before the media type, the body and the decoder, so a
        // principal that holds no command capability gets the same 404 an
        // unmounted route gives and learns nothing else about this deployment.
        let backend = self.backend.clone();
        let bearer = bearer.expose().to_owned();
        async move { backend.admits_mutations(&bearer) }
    }

    fn execute(
        &self,
        bearer: BearerToken,
        request: MutationBatchRequest,
    ) -> impl Future<Output = Result<MutationBatchResponse, MutationHttpError>> + Send {
        let backend = self.backend.clone();
        let support = Arc::clone(&self.support);
        async move {
            // Shared across authorization and execution, so the fence cannot
            // read its head between the two. The blocking task holds it: a
            // request dropped while its write waits for the writer connection
            // or for PostgreSQL leaves the write running, and the fence must
            // wait for its commit.
            let fence = backend.fence_shared().await;
            let authorization = backend.authorize_mutation(bearer.expose(), &request)?;
            let server_time_unix_micros = backend.server_time_unix_micros()?;
            tokio::task::spawn_blocking(move || {
                let _fence = fence;
                support.with_client(|client| {
                    execute_batch(
                        client,
                        &support.registry,
                        &authorization,
                        request,
                        server_time_unix_micros,
                    )
                })
            })
            .await
            .map_err(|_| MutationHttpError::Unavailable)?
            .map_err(map_batch_error)
        }
    }
}

const fn command_operation(operation: CrudOperationName) -> CommandOperation {
    match operation {
        CrudOperationName::Insert => CommandOperation::Insert,
        CrudOperationName::Update => CommandOperation::Update,
        CrudOperationName::Delete => CommandOperation::Delete,
    }
}

fn map_batch_error(error: MutationBatchError) -> MutationHttpError {
    match error {
        MutationBatchError::UnsupportedProtocol
        | MutationBatchError::InvalidRequest
        | MutationBatchError::InvalidAuthorization => MutationHttpError::InvalidRequest,
        _ => MutationHttpError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_errors_map_to_the_http_classes_the_router_answers() {
        assert_eq!(
            map_batch_error(MutationBatchError::InvalidRequest),
            MutationHttpError::InvalidRequest
        );
        assert_eq!(
            map_batch_error(MutationBatchError::UnsupportedProtocol),
            MutationHttpError::InvalidRequest
        );
        assert_eq!(
            map_batch_error(MutationBatchError::CanonicalReceiptBindingMismatch),
            MutationHttpError::Unavailable
        );
    }
}
