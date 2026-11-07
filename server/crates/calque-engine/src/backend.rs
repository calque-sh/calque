use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    io,
    sync::{
        atomic::{AtomicU8, Ordering as AtomicOrdering},
        mpsc, Arc, Mutex as StdMutex, OnceLock, RwLock as StdRwLock,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use artifact_catalog::{PublishedArtifactQuery, SqliteArtifactCatalog};
use artifact_contract::ValidatedArtifactManifest;
use authz::{AuthorizationPolicy, AuthorizedScope, PartitionValue, Principal};
use axum::body::Bytes;
use blob_store::{ManagedBlobStore, ObjectMetadata};
use bootstrap_api::{
    AuthorizedBootstrapScope, BootstrapError, BootstrapPlan, BootstrapPlanner, BootstrapRequest,
    ClientMemoryBudget, DeliveryLocation, DeliveryPrincipal, ResidentStateProofCodec,
};
use bootstrap_http::{BearerToken, BootstrapHttpBackend, BootstrapHttpError, BootstrapHttpRequest};
use canonical_log::{
    CanonicalLogPlanRetention, CanonicalLogRead, CanonicalPage, LogBounds, LogError,
    PostgresCanonicalLogReader, PostgresFencedCanonicalLog,
};
use change_api::{
    ActivatedChangeScope, ChangeAuthorization, ChangeBatch, ChangeBatchBuilder, ChangeBatchError,
    IncomingChangeRequest, MintedRevocationBarrier, ScopeEncodingKey, ScopedChangePlan,
};
use change_http::{ChangeHttpBackend, ChangeStream};
use dataset_grant::{
    GrantCommandCapabilities, GrantCommandOperation, GrantPresentation, GrantVerifier,
};
use mutation_api::{
    CommandCapability, CommandOperation, MutationAuthorization, ResolvedMutationAcknowledgements,
};
use mutation_http::MutationHttpError;
use snapshot_pipeline::{CanonicalHeadSignal, PublicationAdmission};
use source_control::ActivatedSource;
use subtle::ConstantTimeEq;
use sync_protocol::{
    change::{Cursor, RevocationBarrier},
    mutation::MutationBatchRequest,
    verify_revocation_barrier, RevocationBarrierVerificationKey,
};
use sync_types::{CursorPosition, OpaqueCursor, PlanId, SourceEpoch, SourceId, TableId};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::{
    oneshot, watch, Mutex, OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, Semaphore,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    artifacts::{ArtifactCapabilities, ArtifactGrant},
    batch_room::{BatchRoom, LogBatchRoom, PlanRoom},
    build_fence::BuildFence,
    limits::LOG_BATCH_ROOM_BYTES,
    log_readers::{LogReader, LogReaders, PooledLogReader, SourceLogReaders},
    mutations::{LocalMutationError, LocalMutationSupport},
    plans::{request_table_starts, PlanClaims, PlanTerms, PlanTokens, VerifiedPlan, VerifiedPlans},
    recut::{
        newest_cut, retention_floor, trim, NewestCut, RecutConfig, TailMeter, MAINTENANCE_INTERVAL,
    },
    reencode::{
        reencode_and_publish_brotli, EncodingSpan, ReencodeConfig, ReencodeError, ReencodeOutcomes,
        ReencodeRequest, ReencodeStage, Reencoder,
    },
    revocation::{
        FenceWrite, FencedGeneration, LocalRevocationConfig, RevocationError, SignedBarrier,
        StoredRetirement,
    },
    signing::PlanKey,
    snapshot::published_query_for,
    tail::{self, ChangeTail},
    tenants::{TenantOrderRefusal, TenantReport, Tenants},
    ArtifactError, ArtifactService, CapacityAdmissionOutcome, LocalArtifactResolver,
    OperationErrorClass, OperationsMetrics, PreparedLocalSource, SecretToken,
    LOCAL_LONG_POLL_TIMEOUT_MS,
};

mod bootstrap;
mod changes;
mod credentials;
mod fence;
mod log_proxy;
mod maintenance;
#[cfg(test)]
mod tests;
mod writes;

use self::{bootstrap::*, changes::*, credentials::*, fence::*, log_proxy::*, maintenance::*};
pub use credentials::LocalCredentialPolicy;
pub use writes::{MountedCommands, MutationCommandPolicy};

/// Plans whose verified scope one process keeps, unless the deployment sets
/// another bound.
pub const DEFAULT_MAX_ACTIVE_PLANS: usize = 4_096;
/// Bootstraps planned at once; the others wait for one to finish.
pub(crate) const MAX_CONCURRENT_PLANNERS: usize = 2;
/// What the planner is given for the plan's token, which is signed once the
/// plan it names exists.
const UNSIGNED_PLAN_TOKEN: &str = "unsigned";
/// `Retry-After` while a just-fenced generation's artifact is being published.
/// Every client of the deployment waits on that one build, so they are told to
/// look again soon.
const FENCED_ARTIFACT_RETRY_AFTER_SECONDS: u32 = 1;
// The wire clamps `Retry-After` into its own bounds, so a value outside them
// would be advertised as something this composition never chose.
const _: () = assert!(
    FENCED_ARTIFACT_RETRY_AFTER_SECONDS >= 1
        && FENCED_ARTIFACT_RETRY_AFTER_SECONDS
            <= bootstrap_http::MAX_ARTIFACT_PREPARING_RETRY_AFTER_SECONDS
);

#[derive(Debug, Clone, Copy)]
pub struct LocalMemoryPolicy {
    pub active_artifact_capacity_bytes: u64,
    pub staging_overlap_bytes: u64,
    pub catch_up_buffer_bytes: u64,
    pub query_result_reserve_bytes: u64,
}

impl LocalMemoryPolicy {
    fn request_budget(
        self,
        request: &BootstrapHttpRequest,
    ) -> Result<ClientMemoryBudget, BootstrapHttpError> {
        let current_active_artifact_bytes = if request.resident_tables.is_empty() {
            0
        } else {
            self.active_artifact_capacity_bytes
        };
        Ok(ClientMemoryBudget {
            safe_budget_bytes: request.memory_budget_bytes,
            current_active_artifact_bytes,
            staging_overlap_bytes: self.staging_overlap_bytes,
            catch_up_buffer_bytes: self.catch_up_buffer_bytes,
            query_result_reserve_bytes: self.query_result_reserve_bytes,
            client_heap_reserve_bytes: request.client_heap_reserve_bytes,
        })
    }
}

#[derive(Clone)]
pub struct LocalBackend {
    inner: Arc<LocalBackendInner>,
}

/// The authorization generation this process serves, and the artifact it
/// serves it from. Replaced whole when the application fences a generation,
/// so one request never reads a policy of one generation and a scope of
/// another.
struct ServingAuthorization {
    policy: AuthorizationPolicy,
    /// The scope every artifact of this generation was built for. A policy
    /// whose scope differs is not served: handing this artifact set out under
    /// another scope is the disclosure the policy exists to prevent.
    built_scope: AuthorizedScope,
    artifact: AtomicU8,
}

const ARTIFACT_READY: u8 = 0;
const ARTIFACT_PREPARING: u8 = 1;
const ARTIFACT_FAILED: u8 = 2;

impl ServingAuthorization {
    fn new(policy: AuthorizationPolicy, built_scope: AuthorizedScope, artifact: u8) -> Self {
        Self {
            policy,
            built_scope,
            artifact: AtomicU8::new(artifact),
        }
    }

    fn generation(&self) -> u64 {
        self.policy.generation()
    }

    fn artifact(&self) -> u8 {
        self.artifact.load(AtomicOrdering::Acquire)
    }

    fn set_artifact(&self, state: u8) {
        self.artifact.store(state, AtomicOrdering::Release);
    }
}

struct LocalBackendInner {
    source_id: SourceId,
    activated: ActivatedSource,
    serving: StdRwLock<Arc<ServingAuthorization>>,
    /// Held shared by every plan commit and mutation, and exclusively by a
    /// fence, so the canonical head a fence reads is at or beyond everything
    /// any request of the retired generation was served or wrote under it.
    fence_lock: Arc<RwLock<()>>,
    /// The fence lock's side for change builds, which a fence takes too.
    build_fence: BuildFence,
    /// Bumped whenever a fence or a signed barrier changes what a parked
    /// long-poll would be served, so it wakes instead of sleeping out its wait.
    revocations: watch::Sender<u64>,
    /// Present only for `calque serve`: the durable fence and the means to
    /// publish a new generation's artifact.
    revocation: Option<LocalRevocationConfig>,
    /// Re-encodes each published artifact's zstd object in the background.
    reencoder: Option<Reencoder>,
    /// Re-cuts the served scope and trims the canonical log in the background.
    recut: Option<Recutter>,
    /// Present when the scope is partitioned: each grant reads one tenant's
    /// artifact, prepared on its first request.
    tenants: Option<Tenants>,
    credential: LocalCredentialPolicy,
    /// What an authenticated credential may write; `credential` and `policy`
    /// answer only who the caller is and what it may read.
    command_policy: MutationCommandPolicy,
    complete_tables: BTreeSet<TableId>,
    /// The enrolled source relations, spelled `schema.table`, and the wire
    /// table id each resolves to; see `resolve_required_tables`.
    relations: BTreeMap<String, TableId>,
    resident_proofs: ResidentStateProofCodec,
    catalog: Mutex<SqliteArtifactCatalog>,
    store: Arc<dyn ManagedBlobStore>,
    /// The CDC runtime's provider: a plan's retention lease, the readiness
    /// probe and a fence's head read go through it.
    log: Arc<Mutex<PostgresFencedCanonicalLog>>,
    /// What change builds, a bootstrap's planner and the change tail read the
    /// log through.
    log_readers: SourceLogReaders,
    /// The log's newest records, which builds read instead of the log when
    /// it holds their range. `None` when the deployment bounds it at zero.
    tail: Option<Arc<ChangeTail>>,
    /// One permit for each change stream held open.
    open_streams: Arc<Semaphore>,
    log_batch_room: LogBatchRoom,
    artifact_resolver: LocalArtifactResolver,
    artifact_capabilities: ArtifactCapabilities,
    plans: Plans,
    planner_permits: Arc<Semaphore>,
    parked_polls: Arc<StdMutex<ParkedPolls>>,
    head_signal: CanonicalHeadSignal,
    change_endpoint: DeliveryLocation,
    plan_ttl: Duration,
    max_change_batch_bytes: u64,
    memory: LocalMemoryPolicy,
    /// The decoder this composition plans for; see `bootstrap`.
    decoder_profile: String,
    metrics: OperationsMetrics,
    /// Set once `LocalMutationBackend` mounts. Until then the change stream
    /// resolves no acknowledgements and a poll carrying pending ids fails
    /// closed.
    mutations: OnceLock<Arc<LocalMutationSupport>>,
}

fn current_serving(serving: &StdRwLock<Arc<ServingAuthorization>>) -> Arc<ServingAuthorization> {
    // A poisoned lock still holds a whole `Arc`: every writer replaces it in
    // one assignment.
    Arc::clone(
        &serving
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}

pub struct LocalBackendConfig {
    pub source_id: SourceId,
    pub prepared: PreparedLocalSource,
    pub credential: LocalCredentialPolicy,
    /// How this deployment turns an authenticated credential into command
    /// capability.
    pub command_policy: MutationCommandPolicy,
    pub proof_secret: SecretToken,
    /// Signs this deployment's plans and artifact capabilities.
    pub plan_secret: SecretToken,
    pub catalog: SqliteArtifactCatalog,
    pub store: Arc<dyn ManagedBlobStore>,
    pub log: Arc<Mutex<PostgresFencedCanonicalLog>>,
    /// Readers of the same log, which change builds, bootstrap planners and
    /// the change tail take one each of instead of waiting for `log`.
    pub log_readers: SourceLogReaders,
    pub head_signal: CanonicalHeadSignal,
    pub artifact_resolver: LocalArtifactResolver,
    pub change_endpoint: String,
    pub plan_ttl: Duration,
    /// Plans whose verified scope is kept between change requests; a plan
    /// past it is rebuilt from its token. `DEFAULT_MAX_ACTIVE_PLANS` unless
    /// configured.
    pub max_active_plans: usize,
    /// Change streams held open at once; a stream beyond it is refused as
    /// unavailable and retried. `DEFAULT_MAX_OPEN_STREAMS` unless configured.
    pub max_open_streams: usize,
    /// What the change tail holds, in bytes: the newest records, and each
    /// scope's encoding of them. Zero holds nothing, and every build reads the
    /// log. `DEFAULT_CHANGE_TAIL_BYTES` unless configured.
    pub change_tail_bytes: u64,
    pub max_change_batch_bytes: u64,
    pub memory: LocalMemoryPolicy,
    pub metrics: OperationsMetrics,
    /// How the application may retire the generation `prepared` was compiled
    /// at. `None` serves that generation for the life of the process.
    pub revocation: Option<LocalRevocationConfig>,
    /// Where `spawn_reencode` re-encodes a published artifact. `None`
    /// re-encodes nothing.
    pub reencode: Option<ReencodeConfig>,
    /// How `spawn_recut` re-cuts the served scope and trims the canonical
    /// log. `None` does neither.
    pub recut: Option<RecutConfig>,
    /// The tenants of a partitioned scope, which then plans each grant
    /// against its own tenant's artifact.
    pub tenants: Option<Tenants>,
}

impl LocalBackend {
    pub fn new(config: LocalBackendConfig) -> Result<Self, LocalBackendError> {
        if config.plan_ttl.is_zero()
            || config.plan_ttl > Duration::from_secs(60 * 60)
            || config.max_active_plans == 0
            || config.max_open_streams == 0
            || config.max_change_batch_bytes == 0
            || config.max_change_batch_bytes > 64 * 1024 * 1024
            || config.memory.active_artifact_capacity_bytes == 0
            || config.memory.catch_up_buffer_bytes == 0
            || config.memory.query_result_reserve_bytes == 0
        {
            return Err(LocalBackendError::InvalidConfiguration);
        }
        let complete_tables = config
            .prepared
            .registry
            .tables()
            .iter()
            .map(|table| table.table_id.clone())
            .collect();
        let relations = config
            .prepared
            .registry
            .tables()
            .iter()
            .map(|table| {
                (
                    table.inspection.table.display_name(),
                    table.table_id.clone(),
                )
            })
            .collect();
        let generation = config.prepared.policy.generation();
        let mut retirements = Retirements::new(generation, config.plan_ttl);
        if let (Some(revocation), LocalCredentialPolicy::DatasetGrant(verifier)) =
            (&config.revocation, &config.credential)
        {
            for retirement in &revocation.retired {
                let retired = (revocation.recompile)(retirement.revoked_generation)
                    .ok()
                    .filter(|retired| retired.policy.generation() == retirement.revoked_generation)
                    .ok_or(LocalBackendError::InvalidConfiguration)?;
                retirements.hold(
                    retirement.clone(),
                    Arc::new(ServingAuthorization::new(
                        retired.policy,
                        retired.scope,
                        ARTIFACT_FAILED,
                    )),
                    |key_id| verifier.public_key(key_id),
                );
            }
        }
        let plan_key = PlanKey::new(&config.plan_secret);
        let plans = Plans {
            tokens: PlanTokens::new(
                plan_key.clone(),
                config.source_id.clone(),
                config.prepared.activated.source_epoch().clone(),
            ),
            verified: VerifiedPlans::new(config.max_active_plans),
            retirements: StdRwLock::new(retirements),
            source_id: config.source_id.clone(),
            activated: config.prepared.activated.clone(),
        };
        // The follower runs on the runtime the backend is built on, and a
        // backend built outside one reads every build from the log.
        let tail = tokio::runtime::Handle::try_current()
            .ok()
            .filter(|_| config.change_tail_bytes > 0)
            .map(|runtime| {
                let tail = Arc::new(ChangeTail::new(
                    config.source_id.clone(),
                    config.prepared.activated.source_epoch().clone(),
                    config.change_tail_bytes,
                ));
                runtime.spawn(tail::follow(
                    Arc::downgrade(&tail),
                    Arc::clone(&config.log_readers.tail),
                    config.head_signal.subscribe(),
                ));
                tail
            });
        let recut = config.recut.map(|recut| Recutter {
            config: recut,
            prepared: StdMutex::new(config.prepared.clone()),
        });
        Ok(Self {
            inner: Arc::new(LocalBackendInner {
                source_id: config.source_id,
                activated: config.prepared.activated,
                serving: StdRwLock::new(Arc::new(ServingAuthorization::new(
                    config.prepared.policy,
                    config.prepared.scope,
                    ARTIFACT_READY,
                ))),
                fence_lock: Arc::new(RwLock::new(())),
                build_fence: BuildFence::new(),
                revocations: watch::Sender::new(0),
                revocation: config.revocation,
                reencoder: config.reencode.map(Reencoder::new),
                recut,
                tenants: config.tenants,
                credential: config.credential,
                command_policy: config.command_policy,
                complete_tables,
                relations,
                resident_proofs: ResidentStateProofCodec::new(config.proof_secret.as_bytes())?,
                catalog: Mutex::new(config.catalog),
                store: config.store,
                log: config.log,
                log_readers: config.log_readers,
                tail,
                open_streams: Arc::new(Semaphore::new(config.max_open_streams)),
                log_batch_room: LogBatchRoom::new(LOG_BATCH_ROOM_BYTES),
                artifact_resolver: config.artifact_resolver,
                artifact_capabilities: ArtifactCapabilities::new(plan_key, generation),
                plans,
                planner_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_PLANNERS)),
                parked_polls: Arc::default(),
                head_signal: config.head_signal,
                change_endpoint: DeliveryLocation::new(config.change_endpoint, None)?,
                plan_ttl: config.plan_ttl,
                max_change_batch_bytes: config.max_change_batch_bytes,
                memory: config.memory,
                decoder_profile: crate::columnar::direct_columnar_decoder_profile(),
                metrics: config.metrics,
                mutations: OnceLock::new(),
            }),
        })
    }

    /// The route that serves the artifact objects this backend's plans name.
    pub fn artifact_service(&self) -> ArtifactService {
        ArtifactService::new(
            Arc::clone(&self.inner.store),
            self.inner.artifact_capabilities.clone(),
        )
    }

    fn serving(&self) -> Arc<ServingAuthorization> {
        current_serving(&self.inner.serving)
    }

    pub(crate) fn revocation_enabled(&self) -> bool {
        self.inner.revocation.is_some()
            && matches!(
                self.inner.credential,
                LocalCredentialPolicy::DatasetGrant(_)
            )
    }

    /// The shared side of the fence lock, which a mutation holds from
    /// authorization through execution.
    pub(crate) async fn fence_shared(&self) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&self.inner.fence_lock).read_owned().await
    }

    pub(crate) fn probe_operational_dependencies(&self) -> Result<(), OperationErrorClass> {
        // A fenced generation whose artifact could not be published has
        // nothing to serve, and a restart is what retries it.
        if self.serving().artifact() == ARTIFACT_FAILED {
            return Err(OperationErrorClass::ArtifactBuild);
        }
        {
            let log = self.inner.log.blocking_lock();
            log.probe_writable_current_fence()
                .map_err(|_| OperationErrorClass::CanonicalLog)?;
        }
        let catalog = self.inner.catalog.blocking_lock();
        catalog
            .probe_reachable()
            .map_err(|_| OperationErrorClass::ArtifactCatalog)
    }

    pub(crate) fn tenant_orders_enabled(&self) -> bool {
        self.inner.tenants.is_some()
            && matches!(
                self.inner.credential,
                LocalCredentialPolicy::DatasetGrant(_)
            )
    }

    /// Acts on one tenant order the application's backend signed. A dataset
    /// grant does not verify as one, so no browser can warm, pin or read the
    /// state of a tenant.
    pub(crate) async fn tenant_order(
        &self,
        order: &str,
    ) -> Result<TenantReport, TenantOrderRefusal> {
        let (Some(tenants), LocalCredentialPolicy::DatasetGrant(verifier)) =
            (&self.inner.tenants, &self.inner.credential)
        else {
            return Err(TenantOrderRefusal::Unavailable);
        };
        let now = now_unix_seconds().map_err(|_| TenantOrderRefusal::Unavailable)?;
        let order = verifier
            .verify_tenant_order(order, now)
            .map_err(|_| TenantOrderRefusal::Unauthorized)?;
        tenants.order(&order).await
    }
}

impl BootstrapHttpBackend for LocalBackend {
    fn execute(
        &self,
        bearer: BearerToken,
        request: BootstrapHttpRequest,
    ) -> impl Future<Output = Result<BootstrapPlan, BootstrapHttpError>> + Send {
        let backend = self.clone();
        async move {
            let result = backend.bootstrap(bearer, request).await;
            match &result {
                Ok(_) => backend
                    .inner
                    .metrics
                    .record_capacity_admission(CapacityAdmissionOutcome::Admitted),
                Err(BootstrapHttpError::CapacityExceeded) => {
                    backend
                        .inner
                        .metrics
                        .record_capacity_admission(CapacityAdmissionOutcome::Rejected);
                    backend
                        .inner
                        .metrics
                        .record_error(OperationErrorClass::Capacity);
                }
                Err(BootstrapHttpError::Unauthorized | BootstrapHttpError::Forbidden) => backend
                    .inner
                    .metrics
                    .record_error(OperationErrorClass::Authorization),
                Err(_) => backend
                    .inner
                    .metrics
                    .record_error(OperationErrorClass::Internal),
            }
            result
        }
    }
}

impl ChangeHttpBackend for LocalBackend {
    fn execute(
        &self,
        request: IncomingChangeRequest,
    ) -> impl Future<Output = Result<ChangeBatch, ChangeBatchError>> + Send {
        let backend = self.clone();
        async move { backend.changes(request).await }
    }

    fn execute_stream(
        &self,
        request: IncomingChangeRequest,
    ) -> impl Future<Output = Result<ChangeStream, ChangeBatchError>> + Send {
        let backend = self.clone();
        async move { backend.change_stream(request).await }
    }
}

fn now_unix_seconds() -> Result<u64, LocalBackendError> {
    u64::try_from(OffsetDateTime::now_utc().unix_timestamp()).map_err(|_| LocalBackendError::Clock)
}

fn now_unix_micros() -> Result<i64, LocalBackendError> {
    i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000)
        .map_err(|_| LocalBackendError::Clock)
}

#[derive(Debug, thiserror::Error)]
pub enum LocalBackendError {
    #[error("local backend configuration is invalid")]
    InvalidConfiguration,
    #[error("bootstrap planner returned an internally inconsistent plan")]
    InvalidPlan,
    #[error("system clock or timestamp is invalid")]
    Clock,
    #[error(transparent)]
    ResidentProof(#[from] bootstrap_api::ResidentProofError),
    #[error(transparent)]
    Delivery(#[from] bootstrap_api::DeliveryError),
    #[error(transparent)]
    Catalog(#[from] artifact_catalog::CatalogError),
    #[error(transparent)]
    Storage(#[from] blob_store::StorageError),
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    #[error(transparent)]
    State(#[from] crate::StateError),
    #[error(transparent)]
    Timestamp(#[from] time::error::Parse),
}
