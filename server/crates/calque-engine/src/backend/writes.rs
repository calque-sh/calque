//! Writes: the commands a deployment mounts and how a write is authorized.

use super::*;

/// The commands one deployment mounts: which collection maps to which
/// registered command, and which operations that command may be asked for here.
///
/// Built from the deployment's configuration, never from a request. A signed
/// capability is intersected with it, so it is the ceiling of what a grant can
/// reach.
#[derive(Debug, Clone, Default)]
pub struct MountedCommands {
    pub(super) by_collection: BTreeMap<String, (String, BTreeSet<CommandOperation>)>,
}

impl MountedCommands {
    /// Mounts one collection's command. An empty operation set mounts nothing,
    /// because a command that admits no operation authorizes no request.
    pub fn mount(
        &mut self,
        collection: impl Into<String>,
        command_name: impl Into<String>,
        operations: BTreeSet<CommandOperation>,
    ) {
        if operations.is_empty() {
            return;
        }
        self.by_collection
            .insert(collection.into(), (command_name.into(), operations));
    }

    /// The startup contract's rendering: `collection:op+op,collection:op`,
    /// in collection order, so an operator can read what is mounted and under
    /// which capability without parsing the manifest again.
    pub fn contract_value(&self) -> String {
        self.by_collection
            .iter()
            .map(|(collection, (_, operations))| {
                format!(
                    "{collection}:{}",
                    operations
                        .iter()
                        .map(|operation| operation.as_str())
                        .collect::<Vec<_>>()
                        .join("+")
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// What one signed capability may execute here: the intersection of what it
    /// grants and what this deployment mounts.
    pub(super) fn narrow(&self, granted: &GrantCommandCapabilities) -> CommandCapability {
        CommandCapability::new(self.by_collection.iter().map(
            |(collection, (command_name, mounted))| {
                let operations = mounted
                    .iter()
                    .copied()
                    .filter(|operation| granted.permits(collection, grant_operation(*operation)))
                    .collect::<BTreeSet<_>>();
                (command_name.clone(), operations)
            },
        ))
    }
}

/// The grant vocabulary's spelling of one execution operation. The enums stay
/// separate so a signed wire claim cannot be used as an execution verdict.
pub(super) const fn grant_operation(operation: CommandOperation) -> GrantCommandOperation {
    match operation {
        CommandOperation::Insert => GrantCommandOperation::Insert,
        CommandOperation::Update => GrantCommandOperation::Update,
        CommandOperation::Delete => GrantCommandOperation::Delete,
    }
}

/// How this process turns a presented credential into command capability.
///
/// Read authorization says nothing about writing; this says what, if anything,
/// the same credential may write.
pub enum MutationCommandPolicy {
    /// Every credential this process authenticates carries this capability.
    ///
    /// Pairs only with [`LocalCredentialPolicy::DevelopmentSharedBearer`]: one
    /// shared secret cannot express two writers. A deployment that
    /// authenticates several principals must use `SignedGrant`.
    SharedBearer(CommandCapability),
    /// The signed capability the presented grant carried, narrowed by what this
    /// deployment mounts.
    SignedGrant(MountedCommands),
}

impl fmt::Debug for MutationCommandPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SharedBearer(capability) => formatter
                .debug_tuple("SharedBearer")
                .field(capability)
                .finish(),
            Self::SignedGrant(mounted) => {
                formatter.debug_tuple("SignedGrant").field(mounted).finish()
            }
        }
    }
}

impl MutationCommandPolicy {
    /// The capability this credential holds here, or the refusal.
    ///
    /// `NotFound` rather than `Forbidden`: a principal that holds no write
    /// capability must not be able to tell that a write surface exists at all.
    pub(super) fn capability_for(
        &self,
        credential: &ResolvedCredential,
    ) -> Result<CommandCapability, MutationHttpError> {
        let capability = match self {
            Self::SharedBearer(capability) => capability.clone(),
            Self::SignedGrant(mounted) => mounted.narrow(&credential.commands),
        };
        if capability.is_empty() {
            return Err(MutationHttpError::NotFound);
        }
        Ok(capability)
    }
}

impl LocalBackend {
    /// Hands the change stream the receipts the mutation route writes. Called
    /// once, when the route is mounted.
    pub(crate) fn enable_mutations(
        &self,
        support: Arc<LocalMutationSupport>,
    ) -> Result<(), LocalMutationError> {
        self.inner
            .mutations
            .set(support)
            .map_err(|_| LocalMutationError::AlreadyEnabled)
    }

    /// Whether this bearer may reach the write surface at all, for
    /// `mutation-http`'s admission gate.
    ///
    /// It resolves the credential and the capability and discards both: the
    /// answer is whether the route exists for this caller, and nothing about
    /// the request it was going to make has been read yet.
    pub(crate) fn admits_mutations(&self, bearer: &str) -> Result<(), MutationHttpError> {
        self.resolve_command_capability(bearer).map(|_| ())
    }

    /// The credential's capability here, or the refusal, without touching a
    /// request. Both callers below go through this, so the admission gate and
    /// the execution gate cannot disagree about who holds what.
    pub(super) fn resolve_command_capability(
        &self,
        bearer: &str,
    ) -> Result<(ResolvedCredential, CommandCapability), MutationHttpError> {
        let credential = self
            .inner
            .credential
            .resolve(bearer, self.serving().generation())
            .map_err(|error| match error {
                BootstrapHttpError::Unavailable => MutationHttpError::Unavailable,
                _ => MutationHttpError::Unauthorized,
            })?;
        let capability = self.inner.command_policy.capability_for(&credential)?;
        Ok((credential, capability))
    }

    /// Authorizes one mutation batch the way bootstrap authorizes a plan and
    /// a change poll is authorized: the bearer names the principal, the
    /// policy supplies the scope, and the scope token binds the request to a
    /// plan this process granted to the same client instance and device.
    ///
    /// The caller holds the shared fence lock across this and the execution
    /// it authorizes, so a batch authorized under a generation commits before
    /// any fence of that generation reads its head.
    pub(crate) fn authorize_mutation(
        &self,
        bearer: &str,
        request: &MutationBatchRequest,
    ) -> Result<MutationAuthorization, MutationHttpError> {
        let serving = self.serving();
        let (credential, capability) = self.resolve_command_capability(bearer)?;
        let scope = match &self.inner.tenants {
            Some(_) => serving
                .policy
                .scope_for(credential.principal())
                .map_err(|_| MutationHttpError::Forbidden)?,
            None => serving.policy.scope(),
        };
        if self.inner.tenants.is_none() && scope.scope_hash() != serving.built_scope.scope_hash() {
            return Err(MutationHttpError::Forbidden);
        }
        let now = now_unix_micros().map_err(|_| MutationHttpError::Unavailable)?;
        self.inner.plans.authorize_write(
            &request.authorization_scope_token,
            &request.client_instance_id,
            &request.device_id,
            &scope,
            now,
        )?;
        MutationAuthorization::new(
            self.inner.source_id.clone(),
            &self.inner.activated,
            request.authorization_scope_token.clone(),
            scope,
            capability,
        )
        .map_err(|_| MutationHttpError::Unavailable)
    }

    pub(crate) fn server_time_unix_micros(&self) -> Result<u64, MutationHttpError> {
        now_unix_micros()
            .ok()
            .and_then(|micros| u64::try_from(micros).ok())
            .ok_or(MutationHttpError::Unavailable)
    }

    /// The durable-receipt proof for the pending ids one poll carries.
    /// Without mutation support the proof is empty, which makes a request
    /// carrying pending ids fail closed rather than acknowledge nothing.
    pub(super) async fn resolve_acknowledgements(
        &self,
        incoming: &IncomingChangeRequest,
        scope: &AuthorizedScope,
    ) -> Result<ResolvedMutationAcknowledgements, ChangeBatchError> {
        let Some(support) = self.inner.mutations.get() else {
            return Ok(ResolvedMutationAcknowledgements::default());
        };
        if incoming.pending_mutation_ids().is_empty() {
            return Ok(ResolvedMutationAcknowledgements::default());
        }
        let support = Arc::clone(support);
        let inner = Arc::clone(&self.inner);
        // The plan's own scope, which is the generation its receipts were
        // written under even after a fence has moved the deployment on.
        let scope = scope.clone();
        let device_id = incoming.device_id().to_owned();
        let requested = incoming.pending_mutation_ids().clone();
        tokio::task::spawn_blocking(move || {
            support.with_client(|client| {
                mutation_api::resolve_acknowledgements(
                    client,
                    &inner.source_id,
                    inner.activated.source_epoch(),
                    &scope,
                    &device_id,
                    &requested,
                )
            })
        })
        .await
        .map_err(|_| ChangeBatchError::Log(LogError::PostgresUnavailable))?
        .map_err(|error| match error {
            mutation_api::MutationBatchError::AcknowledgementResolution(_) => {
                ChangeBatchError::Log(LogError::PostgresUnavailable)
            }
            _ => ChangeBatchError::InvalidMutationAcknowledgementProof,
        })
    }
}
