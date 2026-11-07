//! The Calque engine: source enrollment and CDC, exact-cut snapshots, the
//! artifact pipeline, the bootstrap, change and mutation routes, durable
//! state, and recovery. The `calque` binary composes it twice: `serve`, with
//! verified dataset grants, and `dev`, on loopback behind a development bearer.

mod artifacts;
mod backend;
mod background;
mod batch_room;
mod build_fence;
mod columnar;
mod guard;
mod limits;
mod log_readers;
mod mutations;
mod operations;
mod plans;
mod recovery;
mod recut;
mod reencode;
mod revocation;
mod service;
mod signing;
mod slot_lag;
mod snapshot;
mod source;
mod state;
mod tail;
mod tenants;
#[cfg(test)]
pub(crate) mod test_support;

pub use artifacts::{ArtifactError, ArtifactService, LocalArtifactResolver};
pub use backend::{
    LocalBackend, LocalBackendConfig, LocalBackendError, LocalCredentialPolicy, LocalMemoryPolicy,
    MountedCommands, MutationCommandPolicy, DEFAULT_MAX_ACTIVE_PLANS,
};
pub use columnar::LocalColumnarError;
pub use guard::{AllowedOrigin, DatabaseUrl, GuardError, HttpBinding};
pub(crate) use limits::LOCAL_ARTIFACT_GROWTH_RESERVE_BYTES;
pub use limits::LOCAL_QUERY_RESULT_RESERVE_BYTES;
pub use limits::{
    DEFAULT_CHANGE_TAIL_BYTES, DEFAULT_MAX_INFLIGHT_REQUESTS, DEFAULT_MAX_OPEN_STREAMS,
    LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES, LOCAL_LONG_POLL_TIMEOUT_MS, MAX_LOCAL_ARTIFACT_BYTES,
};
pub use log_readers::SourceLogReaders;
/// Re-exported because `MountedCommands` is built from these and a caller that
/// mounts commands must be able to name them without taking its own dependency
/// edge on `mutation-api`.
pub use mutation_api::CommandOperation;
pub use mutations::{LocalMutationBackend, LocalMutationError, LocalMutationSupport};
pub use operations::{CapacityAdmissionOutcome, OperationErrorClass, OperationsMetrics};
pub use recovery::{
    create_recovery_backup, restore_recovery_backup, verify_restored_recovery_backup,
    ExistingSourceRows, RecoveryError, RecoveryObjects, RecoveryOracle, RecoveryReport,
};
pub use recut::{RecutConfig, RecutPolicy, DEFAULT_RECUT_MIN_INTERVAL, DEFAULT_RECUT_TAIL_RATIO};
pub use reencode::ReencodeConfig;
pub use revocation::{
    AuthorizationFenceError, AuthorizationFenceStore, GenerationArtifactPublisher,
    LocalRevocationConfig, RecompileAuthorization, StoredRetirement,
};
pub use service::{local_router, metrics_router};
pub use snapshot::{ArtifactBuilder, LocalSnapshotError, LocalSnapshotOutcome};
/// Re-exported so the `calque` binary takes no dependency edge on
/// `snapshot-pipeline`, whose build configuration carries a raw replication
/// connection string. Every PostgreSQL connection the binary opens is made
/// here, from a `DatabaseUrl`.
pub use snapshot_pipeline::CanonicalHeadSignal;
pub use source::{
    destroy_local_source, inspect_local_identity, inspect_local_source, prepare_local_source,
    start_local_source_runtime, CdcStopRemedy, LocalSourceError, LocalSourceHealth,
    LocalSourceRuntime, LocalSourceRuntimeConfig, PreparedLocalSource, LOCAL_DEVELOPMENT_SUBJECT,
};
pub use state::{LocalNames, LocalSecrets, LocalState, SecretToken, StateError, StateStore};
pub use tenants::{
    check_replication_capacity, TenantError, TenantReport, TenantSettings, Tenants, TenantsConfig,
    DEFAULT_MAX_TENANT_BUILDS, DEFAULT_TENANT_COMPRESS_ABOVE_BYTES, DEFAULT_TENANT_HOLD_OPEN,
    DEFAULT_TENANT_WARM_DAYS,
};
