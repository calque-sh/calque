//! What a source between two schema generations answers.

use mutation_http::MutationHttpBackend;
use sync_protocol::mutation::{MutationBatchRequest, MutationBatchResponse};

use super::*;

/// The Retry-After a page is given while its source is between generations.
/// A cut of the benchmark's 100k rows took under 1.5 s.
const BETWEEN_GENERATIONS_RETRY_AFTER_SECONDS: u32 = 1;

/// Why no generation is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BetweenGenerations {
    /// The next generation's cut is being built.
    Preparing,
    /// The live schema does not bind the configuration.
    Suspended,
}

/// The backend of the routes served while no generation is: every bootstrap
/// is refused, after its credential is checked, with `ARTIFACT_PREPARING` or
/// `SCHEMA_SUSPENDED`, and every change request with
/// `SCHEMA_GENERATION_MISMATCH`, so a page rebuilds and waits for the next
/// generation. A change request is refused unverified: the refusal says only
/// that the source is between generations, which `/readyz` says to anyone.
#[derive(Clone)]
pub struct MovingBackend {
    credential: Arc<LocalCredentialPolicy>,
    authorization_generation: u64,
    state: BetweenGenerations,
}

impl MovingBackend {
    pub fn new(
        credential: LocalCredentialPolicy,
        authorization_generation: u64,
        state: BetweenGenerations,
    ) -> Self {
        Self {
            credential: Arc::new(credential),
            authorization_generation,
            state,
        }
    }

    fn refusal(&self) -> BootstrapHttpError {
        let retry_after_seconds = BETWEEN_GENERATIONS_RETRY_AFTER_SECONDS;
        match self.state {
            BetweenGenerations::Preparing => BootstrapHttpError::ArtifactPreparing {
                retry_after_seconds,
            },
            BetweenGenerations::Suspended => BootstrapHttpError::SchemaSuspended {
                retry_after_seconds,
            },
        }
    }
}

impl BootstrapHttpBackend for MovingBackend {
    fn execute(
        &self,
        bearer: BearerToken,
        _request: BootstrapHttpRequest,
    ) -> impl Future<Output = Result<BootstrapPlan, BootstrapHttpError>> + Send {
        let result = self
            .credential
            .resolve(bearer.expose(), self.authorization_generation)
            .and_then(|_| Err(self.refusal()));
        async move { result }
    }
}

impl ChangeHttpBackend for MovingBackend {
    async fn execute(
        &self,
        _request: IncomingChangeRequest,
    ) -> Result<ChangeBatch, ChangeBatchError> {
        Err(ChangeBatchError::SchemaGenerationMismatch)
    }
}

impl MutationHttpBackend for MovingBackend {
    fn admit(
        &self,
        bearer: &mutation_http::BearerToken,
    ) -> impl Future<Output = Result<(), MutationHttpError>> + Send {
        let result = match self
            .credential
            .resolve(bearer.expose(), self.authorization_generation)
        {
            Ok(_) => Err(MutationHttpError::Unavailable),
            Err(BootstrapHttpError::Unavailable) => Err(MutationHttpError::Unavailable),
            Err(_) => Err(MutationHttpError::Unauthorized),
        };
        async move { result }
    }

    async fn execute(
        &self,
        _bearer: mutation_http::BearerToken,
        _request: MutationBatchRequest,
    ) -> Result<MutationBatchResponse, MutationHttpError> {
        Err(MutationHttpError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{header, HeaderMap, HeaderValue};

    use super::*;
    use crate::LOCAL_DEVELOPMENT_SUBJECT;

    fn bearer(token: &str) -> BearerToken {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        BearerToken::from_headers(&headers).unwrap()
    }

    fn request() -> BootstrapHttpRequest {
        BootstrapHttpRequest {
            required_tables: BTreeSet::new(),
            resident_state_proof: None,
            resident_tables: BTreeSet::new(),
            resident_cursor: None,
            cached_artifact_hints: BTreeSet::new(),
            accepted_encodings: Vec::new(),
            memory_budget_bytes: 1_000,
            client_heap_reserve_bytes: 0,
            client_instance_id: "client".to_owned(),
            device_id: "device".to_owned(),
        }
    }

    fn change_request() -> IncomingChangeRequest {
        change_api::parse_change_request(sync_protocol::change::ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(sync_protocol::change::Cursor {
                token: b"cursor".to_vec(),
            }),
            through_cursor_inclusive: None,
            tables: vec![sync_protocol::change::TableSubscription {
                table_id: "table:g1".to_owned(),
                shard_snapshot_cursor: None,
            }],
            authorization_scope_token: b"scope-token".to_vec(),
            max_batch_bytes: 1024 * 1024,
            max_transactions: 10,
            long_poll_timeout_ms: 0,
            client_instance_id: "client".to_owned(),
            device_id: "device".to_owned(),
            pending_mutation_ids: Vec::new(),
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            plan_id: "plan".to_owned(),
            stream: false,
        })
        .unwrap()
    }

    /// A bootstrap with the source's credential is told to retry while the
    /// next generation prepares or the source is suspended; one without it is
    /// refused as unauthorized, so the state reaches no one the source would
    /// not serve. A change request is refused as of another generation, which
    /// rebuilds the page, and a mutation is unavailable to the credential
    /// alone.
    #[tokio::test]
    async fn a_source_between_generations_answers_its_state_only_to_its_credential() {
        let token = SecretToken::generate().unwrap();
        for (state, refusal) in [
            (
                BetweenGenerations::Preparing,
                BootstrapHttpError::ArtifactPreparing {
                    retry_after_seconds: 1,
                },
            ),
            (
                BetweenGenerations::Suspended,
                BootstrapHttpError::SchemaSuspended {
                    retry_after_seconds: 1,
                },
            ),
        ] {
            let backend = MovingBackend::new(
                LocalCredentialPolicy::DevelopmentSharedBearer {
                    token: *token.as_bytes(),
                    principal: Principal::new(LOCAL_DEVELOPMENT_SUBJECT, BTreeMap::new()).unwrap(),
                },
                1,
                state,
            );
            let own =
                BootstrapHttpBackend::execute(&backend, bearer(&token.to_urlsafe()), request());
            assert_eq!(own.await.unwrap_err(), refusal);
            let other = BootstrapHttpBackend::execute(&backend, bearer("another"), request());
            assert_eq!(other.await.unwrap_err(), BootstrapHttpError::Unauthorized);

            assert!(matches!(
                ChangeHttpBackend::execute(&backend, change_request()).await,
                Err(ChangeBatchError::SchemaGenerationMismatch)
            ));
            assert!(matches!(
                backend.admit(&bearer(&token.to_urlsafe())).await,
                Err(MutationHttpError::Unavailable)
            ));
            assert!(matches!(
                backend.admit(&bearer("another")).await,
                Err(MutationHttpError::Unauthorized)
            ));
        }
    }
}
