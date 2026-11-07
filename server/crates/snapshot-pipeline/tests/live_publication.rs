use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use artifact_catalog::{ArtifactState, SqliteArtifactCatalog};
use artifact_contract::{ArtifactCompatibility, RepresentationEncoding, ValidatedArtifactManifest};
use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
use blob_store::{
    BlobStore, ByteStream, LocalFilesystemBlobStore, ObjectMetadata, PutOutcome, StorageError,
};
use canonical_log::{
    CanonicalBarrier, CanonicalPage, FileCanonicalLog, LogError, SnapshotBuildId,
    SnapshotBuildOwnerId, SnapshotBuildRetention, SnapshotBuildRetentionLease, TransactionKey,
};
use pgoutput_decoder::{DecodedChange, Decoder, DecoderEvent, DecoderLimits, LogicalMessageKind};
use postgres::{Client as BlockingClient, NoTls as BlockingNoTls};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use snapshot_builder::{
    CopyLoadLimits, DirectArtifactRequest, EncoderLimits, SnapshotBuildPlan,
    COLLATION_GENERATION_V1, NO_INDEX_PROFILE_HASH, PUBLICATION_GENERATION_V1,
    SOURCE_WIDE_SHARD_PROFILE_HASH, TYPE_CODEC_GENERATION_V1,
};
use snapshot_coordinator::{
    CutIdentity, SnapshotPressureLevel, SnapshotPressurePolicy, SnapshotPressureReason,
    SnapshotPressureThresholds, SnapshotRemainingWalThresholds, SnapshotStreamConfig,
};
use snapshot_pipeline::{
    BarrierWaitError, BarrierWaitRequest, CanonicalBarrierRead, CanonicalHeadSignal,
    SnapshotBuildConnections, SnapshotBuildDisposition, SnapshotBuildFailureCode,
    SnapshotBuildJobConfig, SnapshotBuildJobError, SnapshotBuildRecoveryOutcome,
    SnapshotBuildSafetyPolicy, SnapshotBuildStage, SnapshotBuildSupervisor,
    SnapshotPressureLimitKind, SnapshotPublicationRequest, SnapshotWatchdogConfig,
    SnapshotWatchdogFailureReason, SNAPSHOT_ARTIFACT_FILE_NAME,
};
use source_control::{
    inspect_source, ActivatedSource, EnrollmentRequirements, QualifiedTable, SchemaRegistry,
};
use sync_types::{ArtifactId, CursorCodec, SourceEpoch, SourceId};
use tokio::sync::Notify;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

/// Serialises the tests in this binary: each drops and recreates the same
/// `PUBLICATION`, `*_TABLE` relations and `calque_control` rows.
static FIXTURE_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Busy timeout of the artifact catalog in the catalog-write-timeout test, and
/// the hold applied to it by that test's lock fixture. See the invariant
/// documented at the `set_busy_timeout` call site.
const CATALOG_BUSY_TIMEOUT: Duration = Duration::from_millis(2000);
const CATALOG_LOCK_HOLD: Duration = Duration::from_millis(2500);

const PUBLICATION: &str = "calque_pipeline_test";
const TABLE: &str = "calque_pipeline_items";
const SECOND_TABLE: &str = "calque_pipeline_items_second";
const THIRD_TABLE: &str = "calque_pipeline_items_third";
const FOURTH_TABLE: &str = "calque_pipeline_items_fourth";
const OWNED_APPLICATION_NAME: &str = "calque_pit003_owned";

#[derive(Debug, Clone, Copy)]
enum ProviderGate {
    SecondUpload,
    FirstVerificationHead,
}

struct GatedBlobStore {
    inner: LocalFilesystemBlobStore,
    gate: ProviderGate,
    puts: AtomicUsize,
    heads: AtomicUsize,
    entered: Arc<Notify>,
}

impl GatedBlobStore {
    fn new(inner: LocalFilesystemBlobStore, gate: ProviderGate) -> Self {
        Self {
            inner,
            gate,
            puts: AtomicUsize::new(0),
            heads: AtomicUsize::new(0),
            entered: Arc::new(Notify::new()),
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for GatedBlobStore {
    async fn put_if_absent(
        &self,
        object: &sync_types::ObjectId,
        body: ByteStream,
        metadata: &ObjectMetadata,
    ) -> Result<PutOutcome, StorageError> {
        let ordinal = self.puts.fetch_add(1, Ordering::AcqRel);
        if matches!(self.gate, ProviderGate::SecondUpload) && ordinal == 1 {
            self.entered.notify_one();
            return std::future::pending().await;
        }
        self.inner.put_if_absent(object, body, metadata).await
    }

    async fn head(
        &self,
        object: &sync_types::ObjectId,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        let ordinal = self.heads.fetch_add(1, Ordering::AcqRel);
        if matches!(self.gate, ProviderGate::FirstVerificationHead) && ordinal == 0 {
            self.entered.notify_one();
            return std::future::pending().await;
        }
        self.inner.head(object).await
    }

    async fn get(&self, object: &sync_types::ObjectId) -> Result<Option<ByteStream>, StorageError> {
        self.inner.get(object).await
    }

    async fn delete(&self, object: &sync_types::ObjectId) -> Result<(), StorageError> {
        self.inner.delete(object).await
    }
}

/// Fails the second upload with an I/O error, leaving the first one registered.
/// The fault is injected inside the store because a deletion from outside
/// races the upload it targets.
struct FailSecondUploadStore {
    inner: LocalFilesystemBlobStore,
    puts: AtomicUsize,
}

impl FailSecondUploadStore {
    fn new(inner: LocalFilesystemBlobStore) -> Self {
        Self {
            inner,
            puts: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for FailSecondUploadStore {
    async fn put_if_absent(
        &self,
        object: &sync_types::ObjectId,
        body: ByteStream,
        metadata: &ObjectMetadata,
    ) -> Result<PutOutcome, StorageError> {
        if self.puts.fetch_add(1, Ordering::AcqRel) == 1 {
            return Err(StorageError::Io(std::io::Error::other(
                "injected provider failure during upload",
            )));
        }
        self.inner.put_if_absent(object, body, metadata).await
    }

    async fn head(
        &self,
        object: &sync_types::ObjectId,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        self.inner.head(object).await
    }

    async fn get(&self, object: &sync_types::ObjectId) -> Result<Option<ByteStream>, StorageError> {
        self.inner.get(object).await
    }

    async fn delete(&self, object: &sync_types::ObjectId) -> Result<(), StorageError> {
        self.inner.delete(object).await
    }
}

/// Both stores delay their first delete past the supervisor's cleanup timeout
/// so the retry path runs, and let every later delete through at full speed.
/// The delay is five times the `publication_cleanup_timeout` the call site
/// passes, and `CALQUE_TEST_TIME_SCALE` multiplies both, so the retry delete
/// gets the same slack the first one consumed. These are fixture numbers, not
/// product ones.
const CLEANUP_TIMEOUT_UNSCALED: Duration = Duration::from_millis(40);
const FIRST_DELETE_DELAY_UNSCALED: Duration = Duration::from_millis(200);

fn scaled_cleanup_timeout() -> Duration {
    CLEANUP_TIMEOUT_UNSCALED * test_time_scale()
}

fn scaled_first_delete_delay() -> Duration {
    FIRST_DELETE_DELAY_UNSCALED * test_time_scale()
}

/// The deletes a cleanup makes when its first delete times out: that one,
/// then one for each representation the build uploaded, when the retry
/// deletes them all. Counted from the uploads, so the bound follows the
/// representations a build publishes instead of pinning their number.
fn deletes_with_one_retry(uploaded: &Mutex<BTreeSet<sync_types::ObjectId>>) -> usize {
    uploaded.lock().unwrap().len() + 1
}

struct FailVerificationThenDelayFirstDeleteStore {
    inner: LocalFilesystemBlobStore,
    journal_root: PathBuf,
    build_id: SnapshotBuildId,
    failed_head: AtomicBool,
    uploaded: Mutex<BTreeSet<sync_types::ObjectId>>,
    deletes: AtomicUsize,
    observed_nonterminal_during_delete: AtomicBool,
}

struct DelayFirstDeleteStore {
    inner: LocalFilesystemBlobStore,
    canonical_root: PathBuf,
    build_id: SnapshotBuildId,
    uploaded: Mutex<BTreeSet<sync_types::ObjectId>>,
    deletes: AtomicUsize,
    first_retention_expiry: AtomicI64,
    retry_retention_expiry: AtomicI64,
}

impl DelayFirstDeleteStore {
    fn new(
        inner: LocalFilesystemBlobStore,
        canonical_root: PathBuf,
        build_id: SnapshotBuildId,
    ) -> Self {
        Self {
            inner,
            canonical_root,
            build_id,
            uploaded: Mutex::new(BTreeSet::new()),
            deletes: AtomicUsize::new(0),
            first_retention_expiry: AtomicI64::new(0),
            retry_retention_expiry: AtomicI64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for DelayFirstDeleteStore {
    async fn put_if_absent(
        &self,
        object: &sync_types::ObjectId,
        body: ByteStream,
        metadata: &ObjectMetadata,
    ) -> Result<PutOutcome, StorageError> {
        let outcome = self.inner.put_if_absent(object, body, metadata).await?;
        self.uploaded.lock().unwrap().insert(object.clone());
        Ok(outcome)
    }

    async fn head(
        &self,
        object: &sync_types::ObjectId,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        self.inner.head(object).await
    }

    async fn get(&self, object: &sync_types::ObjectId) -> Result<Option<ByteStream>, StorageError> {
        self.inner.get(object).await
    }

    async fn delete(&self, object: &sync_types::ObjectId) -> Result<(), StorageError> {
        let ordinal = self.deletes.fetch_add(1, Ordering::AcqRel);
        let expiry = snapshot_retention_expiry(&self.canonical_root, &self.build_id);
        if ordinal == 0 {
            self.first_retention_expiry.store(expiry, Ordering::Release);
            tokio::time::sleep(scaled_first_delete_delay()).await;
        } else if ordinal == 1 {
            self.retry_retention_expiry.store(expiry, Ordering::Release);
        }
        self.inner.delete(object).await
    }
}

impl FailVerificationThenDelayFirstDeleteStore {
    fn new(
        inner: LocalFilesystemBlobStore,
        journal_root: PathBuf,
        build_id: SnapshotBuildId,
    ) -> Self {
        Self {
            inner,
            journal_root,
            build_id,
            failed_head: AtomicBool::new(false),
            uploaded: Mutex::new(BTreeSet::new()),
            deletes: AtomicUsize::new(0),
            observed_nonterminal_during_delete: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for FailVerificationThenDelayFirstDeleteStore {
    async fn put_if_absent(
        &self,
        object: &sync_types::ObjectId,
        body: ByteStream,
        metadata: &ObjectMetadata,
    ) -> Result<PutOutcome, StorageError> {
        let outcome = self.inner.put_if_absent(object, body, metadata).await?;
        self.uploaded.lock().unwrap().insert(object.clone());
        Ok(outcome)
    }

    async fn head(
        &self,
        object: &sync_types::ObjectId,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        if !self.failed_head.swap(true, Ordering::AcqRel) {
            return Err(StorageError::Io(std::io::Error::other(
                "injected verification failure before cleanup",
            )));
        }
        self.inner.head(object).await
    }

    async fn get(&self, object: &sync_types::ObjectId) -> Result<Option<ByteStream>, StorageError> {
        self.inner.get(object).await
    }

    async fn delete(&self, object: &sync_types::ObjectId) -> Result<(), StorageError> {
        let ordinal = self.deletes.fetch_add(1, Ordering::AcqRel);
        if ordinal == 0 {
            let journal =
                snapshot_pipeline::SnapshotBuildJournal::reopen(&self.journal_root, &self.build_id)
                    .expect("reopen lifecycle journal during delayed provider cleanup");
            assert!(
                !journal.is_terminal(),
                "cleanup was terminalized before provider deletion completed"
            );
            assert!(
                journal
                    .progress()
                    .cleanup_completed_at_unix_millis
                    .is_none(),
                "cleanup completion was recorded before provider deletion completed"
            );
            self.observed_nonterminal_during_delete
                .store(true, Ordering::Release);
            tokio::time::sleep(scaled_first_delete_delay()).await;
        }
        self.inner.delete(object).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_owns_export_copy_cut_barrier_publication_and_cleanup() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new("pipeline-supervisor-epoch").unwrap();
    let source_id = SourceId::new("pipeline-supervisor-source").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([21_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let log = ScaledDeadlineFileLog::open(
        workspace.path().join("canonical"),
        source_id,
        source_epoch.clone(),
    )
    .unwrap();
    let provider = Arc::new(Mutex::new(log));
    let signal = CanonicalHeadSignal::new();

    let persistent_slot = format!("pipeline_persistent_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let barrier_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        provider.clone(),
        signal.clone(),
        source_epoch.clone(),
    ));

    let artifact_id = ArtifactId::new("pipeline-supervisor-artifact").unwrap();
    let build_id = SnapshotBuildId::new("pipeline-supervisor-build").unwrap();
    let artifact_path = workspace
        .path()
        .join("private-builds")
        .join(build_id.as_str())
        .join(SNAPSHOT_ARTIFACT_FILE_NAME);
    let journal_root = workspace.path().join("build-status");
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        test_safety_policy(),
    );
    let connections = SnapshotBuildConnections {
        control: connect_owned(&database_url).await,
        ddl_guard: Some(connect_owned(&database_url).await),
        copy_workers: vec![connect_owned(&database_url).await],
        pressure: Some(connect_owned(&database_url).await),
    };
    let mut catalog = SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3"))
        .expect("open catalog");
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
    let report = supervisor
        .run_controlled(
            Arc::clone(&provider),
            &signal,
            connections,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    barrier_task.await.unwrap();

    assert_eq!(report.manifest.manifest().artifact_id, artifact_id);
    assert_eq!(
        catalog.state(&artifact_id).unwrap(),
        Some(ArtifactState::Published)
    );
    assert!(!artifact_path.exists());
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(&journal_root, &build_id)
        .unwrap()
        .progress()
        .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Published);
    assert_eq!(progress.cleanup_completed_at_unix_millis, None);
    assert!(progress.retained_wal_bytes.is_some());
    assert!(progress.pressure_level.is_some());

    // A journal marked FAILED after the catalog commit: it must never override
    // authoritative catalog visibility. Startup recovery also fences/releases
    // the stale same-owner retention capability and removes every
    // build-private/staging byte.
    let status_path = journal_root.join(format!("{}.json", build_id.as_str()));
    let mut status: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&status_path).unwrap()).unwrap();
    let last_progress = status["lastProgressAtUnixMillis"].clone();
    status["stage"] = serde_json::json!("FAILED");
    status["endedAtUnixMillis"] = last_progress.clone();
    status["cleanupCompletedAtUnixMillis"] = last_progress;
    status["disposition"] = serde_json::json!("RETRY");
    status["failureCode"] = serde_json::json!("CLEANUP_FAILED");
    std::fs::write(&status_path, serde_json::to_vec(&status).unwrap()).unwrap();
    let private_dir = artifact_path.parent().unwrap();
    std::fs::create_dir_all(private_dir).unwrap();
    std::fs::write(
        private_dir.join("crash-left-representation.tmp"),
        b"private",
    )
    .unwrap();
    std::fs::write(
        object_root.join("staging").join("crash-left.tmp"),
        b"staging",
    )
    .unwrap();
    let stale_retention = provider
        .lock()
        .unwrap()
        .acquire_snapshot_build_retention(
            build_id.clone(),
            SnapshotBuildOwnerId::new("pipeline-supervisor-owner").unwrap(),
            Duration::from_secs(30),
        )
        .unwrap();
    let wrong_epoch_provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            workspace.path().join("canonical-wrong-published-epoch"),
            SourceId::new("pipeline-supervisor-wrong-source").unwrap(),
            SourceEpoch::new("pipeline-supervisor-wrong-epoch").unwrap(),
        )
        .unwrap(),
    ));
    let wrong_epoch_recovery = supervisor
        .recover_interrupted_local(wrong_epoch_provider, &mut catalog, &Arc::new(store.clone()))
        .await
        .unwrap();
    assert_eq!(
        wrong_epoch_recovery,
        SnapshotBuildRecoveryOutcome::Published {
            operational_cleanup: snapshot_pipeline::SnapshotOperationalCleanup {
                journal_reconciled: snapshot_pipeline::SnapshotCleanupStep::Complete,
                private_workspace_removed: snapshot_pipeline::SnapshotCleanupStep::Complete,
                blob_staging_removed: snapshot_pipeline::SnapshotCleanupStep::Complete,
                retention_released: snapshot_pipeline::SnapshotCleanupStep::Pending(
                    snapshot_pipeline::SnapshotCleanupFailureCode::RetentionSourceMismatch,
                ),
            }
        }
    );
    assert_eq!(
        catalog.state(&artifact_id).unwrap(),
        Some(ArtifactState::Published)
    );
    let recovered = supervisor
        .recover_interrupted_local(
            Arc::clone(&provider),
            &mut catalog,
            &Arc::new(store.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered,
        SnapshotBuildRecoveryOutcome::Published {
            operational_cleanup: snapshot_pipeline::SnapshotOperationalCleanup {
                journal_reconciled: snapshot_pipeline::SnapshotCleanupStep::Complete,
                private_workspace_removed: snapshot_pipeline::SnapshotCleanupStep::Complete,
                blob_staging_removed: snapshot_pipeline::SnapshotCleanupStep::Complete,
                retention_released: snapshot_pipeline::SnapshotCleanupStep::Complete,
            }
        }
    );
    assert!(!private_dir.exists());
    assert_eq!(count_files(&object_root.join("staging")), 0);
    assert!(matches!(
        provider
            .lock()
            .unwrap()
            .renew_snapshot_build_retention(&stale_retention, Duration::from_secs(30)),
        Err(LogError::SnapshotBuildRetentionLeaseRejected)
    ));
    assert_eq!(
        snapshot_pipeline::SnapshotBuildJournal::reopen(&journal_root, &build_id)
            .unwrap()
            .progress()
            .stage,
        SnapshotBuildStage::Published
    );
    persistent_control
        .query_one(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[&persistent_slot],
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_runs_two_copy_workers_and_publishes_complete_tables() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_parallel_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-parallel-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([62_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    assert_eq!(plan.tables().len(), 4);
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-parallel-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_parallel_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let barrier_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        Arc::clone(&provider),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-parallel-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-parallel-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        test_safety_policy(),
    );
    let connections = SnapshotBuildConnections {
        control: connect_owned(&database_url).await,
        ddl_guard: Some(connect_owned(&database_url).await),
        copy_workers: vec![
            connect_owned(&database_url).await,
            connect_owned(&database_url).await,
        ],
        pressure: Some(connect_owned(&database_url).await),
    };
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
    let report = supervisor
        .run_controlled(
            Arc::clone(&provider),
            &signal,
            connections,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    barrier_task.await.unwrap();

    assert_eq!(report.manifest.manifest().tables.len(), 4);
    assert!(report
        .manifest
        .manifest()
        .tables
        .iter()
        .all(|table| table.row_count == 1));
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.tables_total, 4);
    assert_eq!(progress.tables_completed, 4);
    assert_eq!(progress.peak_active_copy_workers, 2);
    assert_no_supervisor_transient_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
    persistent_control
        .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_restart_recovers_all_durable_catalog_states_and_cleanup() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .unwrap();
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();

    for (ordinal, state) in ["BUILDING", "UPLOADING", "UPLOADED", "ABANDONED"]
        .into_iter()
        .enumerate()
    {
        let workspace = tempfile::tempdir().unwrap();
        let build_id =
            SnapshotBuildId::new(format!("pipeline-restart-{}", state.to_ascii_lowercase()))
                .unwrap();
        let artifact_id = ArtifactId::new(format!("pipeline-restart-artifact-{ordinal}")).unwrap();
        let source_epoch = SourceEpoch::new(format!("pipeline-restart-epoch-{ordinal}")).unwrap();
        let source_id = SourceId::new(format!("pipeline-restart-source-{ordinal}")).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "snapshot_recovery_child_exits_without_cleanup",
                "--ignored",
                "--exact",
                "--nocapture",
            ])
            .env("PIT003_RECOVERY_CHILD_WORKSPACE", workspace.path())
            .env("PIT003_RECOVERY_CHILD_STATE", state)
            .env("PIT003_RECOVERY_CHILD_BUILD", build_id.as_str())
            .env("PIT003_RECOVERY_CHILD_ARTIFACT", artifact_id.as_str())
            .env("PIT003_RECOVERY_CHILD_EPOCH", source_epoch.as_str())
            .env("PIT003_RECOVERY_CHILD_SOURCE", source_id.as_str())
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(86),
            "restart fixture child did not stop at {state}: {}",
            String::from_utf8_lossy(&child.stderr)
        );

        let activated_source = ActivatedSource::new(
            &registry,
            source_epoch.clone(),
            CursorCodec::new([70_u8.saturating_add(ordinal as u8); 32]).unwrap(),
            source_control::PgoutputLaunchProfile::V1,
        );
        let supervisor = test_supervisor(
            workspace.path(),
            &database_url,
            &registry,
            &plan,
            &build_id,
            &artifact_id,
            Duration::from_secs(2),
            test_safety_policy(),
        );
        let canonical_root = workspace.path().join("canonical");
        let provider = Arc::new(Mutex::new(
            ScaledDeadlineFileLog::open(
                &canonical_root,
                source_id,
                activated_source.source_epoch().clone(),
            )
            .unwrap(),
        ));
        let mut catalog =
            SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
        let object_root = workspace.path().join("objects");
        let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

        let catalog_before_wrong_epoch = catalog.state(&artifact_id).unwrap();
        let progress_before_wrong_epoch = snapshot_pipeline::SnapshotBuildJournal::reopen(
            workspace.path().join("build-status"),
            &build_id,
        )
        .unwrap()
        .progress()
        .clone();
        let objects_before_wrong_epoch = count_files(&object_root);
        let wrong_epoch_provider = Arc::new(Mutex::new(
            ScaledDeadlineFileLog::open(
                workspace.path().join("canonical-wrong-epoch"),
                SourceId::new(format!("pipeline-restart-wrong-source-{ordinal}")).unwrap(),
                SourceEpoch::new(format!("pipeline-restart-wrong-epoch-{ordinal}")).unwrap(),
            )
            .unwrap(),
        ));
        assert!(matches!(
            supervisor
                .recover_interrupted_local(
                    wrong_epoch_provider,
                    &mut catalog,
                    &Arc::new(store.clone())
                )
                .await,
            Err(snapshot_pipeline::SnapshotBuildJobError::RecoverySourceEpochMismatch)
        ));
        assert_eq!(
            catalog.state(&artifact_id).unwrap(),
            catalog_before_wrong_epoch
        );
        assert_eq!(count_files(&object_root), objects_before_wrong_epoch);
        assert!(workspace
            .path()
            .join("private-builds")
            .join(build_id.as_str())
            .exists());
        assert_eq!(
            snapshot_pipeline::SnapshotBuildJournal::reopen(
                workspace.path().join("build-status"),
                &build_id,
            )
            .unwrap()
            .progress(),
            &progress_before_wrong_epoch
        );

        let recovered = supervisor
            .recover_interrupted_local(
                Arc::clone(&provider),
                &mut catalog,
                &Arc::new(store.clone()),
            )
            .await
            .unwrap();
        assert_eq!(
            recovered,
            SnapshotBuildRecoveryOutcome::InterruptedBuildCleaned,
            "wrong recovery result for durable catalog state {state}"
        );
        assert_eq!(catalog.state(&artifact_id).unwrap(), None);
        let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
            workspace.path().join("build-status"),
            &build_id,
        )
        .unwrap()
        .progress()
        .clone();
        assert_eq!(progress.stage, SnapshotBuildStage::Failed);
        assert_eq!(
            progress.failure_code,
            Some(SnapshotBuildFailureCode::ProcessInterrupted)
        );
        assert!(progress.cleanup_completed_at_unix_millis.is_some());
        assert_no_supervisor_residue(
            &database_url,
            workspace.path(),
            &canonical_root,
            &object_root,
        )
        .await;
    }
}

/// Not a test: the abrupt-exit child process that
/// `supervisor_restart_recovers_all_durable_catalog_states_and_cleanup` spawns
/// with the `PIT003_RECOVERY_CHILD_*` environment set, re-entering this binary
/// to reach the same fixtures. Without that environment it returns, so
/// `cargo test -- --ignored` passes over it; a partial environment panics,
/// since it means the parent spawned the child wrongly.
#[test]
#[ignore = "invoked only as the abrupt process fixture for restart recovery"]
fn snapshot_recovery_child_exits_without_cleanup() {
    const CHILD_VARS: [&str; 6] = [
        "PIT003_RECOVERY_CHILD_WORKSPACE",
        "PIT003_RECOVERY_CHILD_STATE",
        "PIT003_RECOVERY_CHILD_BUILD",
        "PIT003_RECOVERY_CHILD_ARTIFACT",
        "PIT003_RECOVERY_CHILD_EPOCH",
        "PIT003_RECOVERY_CHILD_SOURCE",
    ];
    let present = CHILD_VARS
        .iter()
        .filter(|name| env::var_os(name).is_some())
        .count();
    if present == 0 {
        return;
    }
    assert_eq!(
        present,
        CHILD_VARS.len(),
        "restart recovery child spawned with an incomplete fixture environment"
    );
    let Ok(workspace) = env::var("PIT003_RECOVERY_CHILD_WORKSPACE") else {
        panic!("restart recovery child may only run with its fixture environment");
    };
    let workspace = Path::new(&workspace);
    let state = env::var("PIT003_RECOVERY_CHILD_STATE").unwrap();
    let build_id = SnapshotBuildId::new(env::var("PIT003_RECOVERY_CHILD_BUILD").unwrap()).unwrap();
    let artifact_id = ArtifactId::new(env::var("PIT003_RECOVERY_CHILD_ARTIFACT").unwrap()).unwrap();
    let source_epoch = SourceEpoch::new(env::var("PIT003_RECOVERY_CHILD_EPOCH").unwrap()).unwrap();
    let source_id = SourceId::new(env::var("PIT003_RECOVERY_CHILD_SOURCE").unwrap()).unwrap();
    let owner = SnapshotBuildOwnerId::new("pipeline-supervisor-owner").unwrap();
    let private_dir = workspace.join("private-builds").join(build_id.as_str());
    fs::create_dir_all(&private_dir).unwrap();
    fs::write(
        private_dir.join(SNAPSHOT_ARTIFACT_FILE_NAME),
        b"private-build",
    )
    .unwrap();
    let mut journal = snapshot_pipeline::SnapshotBuildJournal::create(
        workspace.join("build-status"),
        &build_id,
        &owner,
        &artifact_id,
        &private_dir,
        source_epoch.clone(),
        1,
        unix_millis(),
    )
    .unwrap();
    journal
        .transition(SnapshotBuildStage::RetentionLeased, unix_millis())
        .unwrap();
    let mut provider =
        FileCanonicalLog::open(workspace.join("canonical"), source_id, source_epoch).unwrap();
    provider
        .acquire_snapshot_build_retention(build_id, owner, Duration::from_secs(30))
        .unwrap();

    let mut catalog = SqliteArtifactCatalog::open(workspace.join("catalog.sqlite3")).unwrap();
    let manifest = recovery_manifest(&artifact_id);
    let registration = catalog
        .register_or_resume_build(&manifest, "crashed-owner", 1, 1)
        .unwrap();
    if matches!(state.as_str(), "UPLOADING" | "UPLOADED" | "ABANDONED") {
        catalog.mark_uploading(registration.lease(), 1).unwrap();
    }
    if matches!(state.as_str(), "UPLOADED" | "ABANDONED") {
        catalog.mark_uploaded(registration.lease(), 1).unwrap();
    }
    if state == "ABANDONED" {
        catalog
            .abandon_build(
                registration.lease(),
                "crashed-cleaner",
                1,
                0,
                1,
                "crash during cleanup",
            )
            .unwrap();
    }
    let store =
        LocalFilesystemBlobStore::open(workspace.join("objects"), 32 * 1024 * 1024).unwrap();
    if matches!(state.as_str(), "UPLOADING" | "UPLOADED" | "ABANDONED") {
        let representations = manifest.manifest().representations.clone();
        let upload_count = if state == "UPLOADING" {
            1
        } else {
            representations.len()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for (index, representation) in representations.into_iter().take(upload_count).enumerate() {
            let bytes = recovery_representation_bytes(representation.encoding);
            let path = workspace.join(format!("recovery-upload-{index}.bin"));
            fs::write(&path, &bytes).unwrap();
            runtime.block_on(async {
                let file = tokio::fs::File::open(&path).await.unwrap();
                store
                    .put_if_absent(
                        &representation.object_id,
                        Box::pin(file),
                        &ObjectMetadata::new(
                            representation.encoded_bytes,
                            representation.encoded_sha256,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            });
            fs::remove_file(path).unwrap();
        }
    }
    fs::write(
        store.root().join("staging").join("crash-left.tmp"),
        b"staging",
    )
    .unwrap();
    std::process::exit(86);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_renews_retention_during_writes_and_clamps_marker_gc() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new("pipeline-renewal-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([25_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-renewal-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_renewal_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let mirror_task = tokio::spawn({
        let database_url = database_url.clone();
        let provider = provider.clone();
        let signal = signal.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(1_200)).await;
            mirror_cut_to_file_log_controlled(
                database_url,
                persistent_slot.clone(),
                provider,
                signal,
                source_epoch,
                true,
            )
            .await;
            persistent_slot
        }
    });
    let writer_task = tokio::spawn({
        let database_url = database_url.clone();
        async move {
            let writer = connect(&database_url).await;
            for revision in 0..30_i32 {
                writer
                    .execute(
                        &format!("UPDATE public.{TABLE} SET value = $1 WHERE id = 1"),
                        &[&format!("write-{revision}")],
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        }
    });
    let build_id = SnapshotBuildId::new("pipeline-renewal-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-renewal-artifact").unwrap();
    let supervisor = test_supervisor_with_retention(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(5),
        test_safety_policy(),
        Duration::from_secs(1),
        Duration::from_millis(100),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let store =
        LocalFilesystemBlobStore::open(workspace.path().join("objects"), 32 * 1024 * 1024).unwrap();

    let report = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    writer_task.await.unwrap();
    let persistent_slot = mirror_task.await.unwrap();
    assert_eq!(report.manifest.manifest().artifact_id, artifact_id);
    assert_eq!(
        catalog.state(&artifact_id).unwrap(),
        Some(ArtifactState::Published)
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(canonical_root.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["minimumRetainedSequence"], 1);
    assert_eq!(
        manifest["snapshotBuildRetentionLeases"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    persistent_control
        .query_one(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[&persistent_slot],
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_retention_renewal_loss_and_stall_fail_closed_before_expiry() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    for (ordinal, mode) in [RenewalFailureMode::Error, RenewalFailureMode::Stall]
        .into_iter()
        .enumerate()
    {
        let setup_url = database_url.clone();
        let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
            .await
            .unwrap();
        let source_epoch =
            SourceEpoch::new(format!("pipeline-renew-loss-epoch-{ordinal}")).unwrap();
        let activated_source = ActivatedSource::new(
            &registry,
            source_epoch.clone(),
            CursorCodec::new([80_u8.saturating_add(ordinal as u8); 32]).unwrap(),
            source_control::PgoutputLaunchProfile::V1,
        );
        let plan =
            SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let canonical_root = workspace.path().join("canonical");
        let provider = Arc::new(Mutex::new(RenewalFailureRetentionProvider {
            log: FileCanonicalLog::open(
                &canonical_root,
                SourceId::new(format!("pipeline-renew-loss-source-{ordinal}")).unwrap(),
                source_epoch,
            )
            .unwrap(),
            mode,
            scale: test_time_scale(),
            first_lease: None,
            last_renewed_lease: None,
        }));
        let build_id =
            SnapshotBuildId::new(format!("pipeline-renew-loss-build-{ordinal}")).unwrap();
        let artifact_id =
            ArtifactId::new(format!("pipeline-renew-loss-artifact-{ordinal}")).unwrap();
        let supervisor = test_supervisor_with_retention(
            workspace.path(),
            &database_url,
            &registry,
            &plan,
            &build_id,
            &artifact_id,
            Duration::from_secs(2),
            test_safety_policy(),
            Duration::from_secs(10),
            Duration::from_millis(100),
        );
        let connections = test_supervisor_connections(&database_url).await;
        let owned_backend_pids = Arc::new(Mutex::new(vec![
            connections
                .control
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0),
            connections
                .ddl_guard
                .as_ref()
                .unwrap()
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0),
            connections.copy_workers[0]
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0),
            connections
                .pressure
                .as_ref()
                .unwrap()
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0),
        ]));
        let observer_pids = Arc::clone(&owned_backend_pids);
        let observer_url = database_url.clone();
        let mut catalog =
            SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
        let object_root = workspace.path().join("objects");
        let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

        let result = supervisor
            .run_controlled(
                Arc::clone(&provider),
                &CanonicalHeadSignal::new(),
                connections,
                &registry,
                &activated_source,
                plan,
                &mut catalog,
                &store,
                &tokio_util::sync::CancellationToken::new(),
                move |stage| {
                    if stage == SnapshotBuildStage::Exported {
                        observer_pids
                            .lock()
                            .unwrap()
                            .push(temporary_snapshot_exporter_pid_blocking(&observer_url));
                        std::thread::sleep(Duration::from_millis(350));
                    }
                },
            )
            .await;
        assert!(
            matches!(
                &result,
                Err(SnapshotBuildJobError::Lease(
                    snapshot_pipeline::SnapshotBuildLeaseGuardError::RenewalFailed(_)
                ))
            ),
            "unexpected renewal-loss result for case {ordinal}: {result:?}"
        );
        assert_eq!(catalog.state(&artifact_id).unwrap(), None);
        let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
            workspace.path().join("build-status"),
            &build_id,
        )
        .unwrap()
        .progress()
        .clone();
        assert_eq!(progress.stage, SnapshotBuildStage::Failed);
        assert_eq!(
            progress.failure_code,
            Some(SnapshotBuildFailureCode::LeaseLost)
        );
        assert!(progress.cleanup_completed_at_unix_millis.is_some());
        let (old_lease, authoritative_lease) = {
            let provider = provider.lock().unwrap();
            let old = provider.first_lease.clone().unwrap();
            let authoritative = provider
                .last_renewed_lease
                .clone()
                .unwrap_or_else(|| old.clone());
            (old, authoritative)
        };
        assert!(unix_millis() < authoritative_lease.expires_at_unix_millis());
        assert!(matches!(
            provider
                .lock()
                .unwrap()
                .log
                .acquire_snapshot_build_retention(
                    build_id.clone(),
                    SnapshotBuildOwnerId::new("replacement-before-expiry").unwrap(),
                    Duration::from_secs(10),
                ),
            Err(LogError::SnapshotBuildRetentionLeaseConflict)
        ));
        let wait_millis = authoritative_lease
            .expires_at_unix_millis()
            .saturating_sub(unix_millis())
            .saturating_add(25) as u64;
        tokio::time::sleep(Duration::from_millis(wait_millis)).await;
        let replacement = {
            let mut provider = provider.lock().unwrap();
            assert!(matches!(
                provider
                    .log
                    .renew_snapshot_build_retention(&old_lease, Duration::from_secs(10)),
                Err(LogError::SnapshotBuildRetentionLeaseRejected)
            ));
            let replacement = provider
                .log
                .acquire_snapshot_build_retention(
                    build_id.clone(),
                    SnapshotBuildOwnerId::new("replacement-after-expiry").unwrap(),
                    Duration::from_secs(10),
                )
                .unwrap();
            assert!(replacement.token() > authoritative_lease.token());
            provider
                .log
                .release_snapshot_build_retention(&replacement)
                .unwrap();
            replacement
        };
        assert!(
            replacement.expires_at_unix_millis() > authoritative_lease.expires_at_unix_millis()
        );
        assert_no_supervisor_residue(
            &database_url,
            workspace.path(),
            &canonical_root,
            &object_root,
        )
        .await;
        let owned_backend_pids = owned_backend_pids.lock().unwrap().clone();
        assert_backend_pids_gone(&database_url, &owned_backend_pids).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_barrier_timeout_fails_closed_and_leaves_no_residue() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new("pipeline-timeout-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([22_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-timeout-source").unwrap(),
            source_epoch,
        )
        .unwrap(),
    ));
    let build_id = SnapshotBuildId::new("pipeline-timeout-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-timeout-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_millis(150),
        test_safety_policy(),
    );
    let connections = test_supervisor_connections(&database_url).await;
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

    let error = supervisor
        .run_controlled(
            provider,
            &CanonicalHeadSignal::new(),
            connections,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        SnapshotBuildJobError::Barrier(BarrierWaitError::Timeout)
    ));
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::PersistentBarrierTimeout)
    );
    assert_eq!(progress.disposition, Some(SnapshotBuildDisposition::Retry));
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_lease_acquisition_failure_is_durably_terminal() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new("pipeline-lease-failure-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([27_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(FailingRetentionProvider {
        log: FileCanonicalLog::open(
            &canonical_root,
            SourceId::new("pipeline-lease-failure-source").unwrap(),
            source_epoch,
        )
        .unwrap(),
    }));
    let build_id = SnapshotBuildId::new("pipeline-lease-failure-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-lease-failure-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(5),
        test_safety_policy(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

    let error = supervisor
        .run_controlled(
            provider,
            &CanonicalHeadSignal::new(),
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SnapshotBuildJobError::Lease(_)));
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::LeaseLost)
    );
    assert_eq!(progress.disposition, Some(SnapshotBuildDisposition::Retry));
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_pressure_hard_limit_aborts_and_leaves_no_residue() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new("pipeline-pressure-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([23_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-pressure-source").unwrap(),
            source_epoch,
        )
        .unwrap(),
    ));
    let build_id = SnapshotBuildId::new("pipeline-pressure-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-pressure-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(5),
        hard_catalog_pressure_policy(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

    let error = supervisor
        .run_controlled(
            provider,
            &CanonicalHeadSignal::new(),
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        SnapshotBuildJobError::Watchdog(SnapshotWatchdogFailureReason::PressureHardLimit {
            kind: SnapshotPressureLimitKind::CatalogXidAge
        })
    ));
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::CatalogXidAgeLimit)
    );
    assert_eq!(
        progress.disposition,
        Some(SnapshotBuildDisposition::OperatorAction)
    );
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_wal_and_copy_age_hard_limits_are_typed_and_cleanup_safe() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    for (ordinal, expected_kind, policy) in [
        (
            0_u8,
            SnapshotPressureLimitKind::WalPressure,
            hard_wal_pressure_policy(),
        ),
        (
            1_u8,
            SnapshotPressureLimitKind::CopyTransactionAge,
            hard_copy_age_pressure_policy(),
        ),
    ] {
        let setup_url = database_url.clone();
        let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
            .await
            .unwrap();
        let source_epoch =
            SourceEpoch::new(format!("pipeline-pressure-kind-epoch-{ordinal}")).unwrap();
        let activated_source = ActivatedSource::new(
            &registry,
            source_epoch.clone(),
            CursorCodec::new([94_u8.saturating_add(ordinal); 32]).unwrap(),
            source_control::PgoutputLaunchProfile::V1,
        );
        let plan =
            SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let canonical_root = workspace.path().join("canonical");
        let provider = Arc::new(Mutex::new(
            ScaledDeadlineFileLog::open(
                &canonical_root,
                SourceId::new(format!("pipeline-pressure-kind-source-{ordinal}")).unwrap(),
                source_epoch,
            )
            .unwrap(),
        ));
        let build_id =
            SnapshotBuildId::new(format!("pipeline-pressure-kind-build-{ordinal}")).unwrap();
        let artifact_id =
            ArtifactId::new(format!("pipeline-pressure-kind-artifact-{ordinal}")).unwrap();
        let supervisor = test_supervisor(
            workspace.path(),
            &database_url,
            &registry,
            &plan,
            &build_id,
            &artifact_id,
            Duration::from_secs(5),
            policy,
        );
        let observer_url = database_url.clone();
        let mut catalog =
            SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
        let object_root = workspace.path().join("objects");
        let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
        let result = supervisor
            .run_controlled(
                provider,
                &CanonicalHeadSignal::new(),
                test_supervisor_connections(&database_url).await,
                &registry,
                &activated_source,
                plan,
                &mut catalog,
                &store,
                &tokio_util::sync::CancellationToken::new(),
                move |stage| {
                    if stage == SnapshotBuildStage::Exported {
                        if ordinal == 0 {
                            generate_retained_wal_blocking(&observer_url);
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                },
            )
            .await;
        assert!(
            matches!(
                &result,
                Err(SnapshotBuildJobError::Watchdog(
                    SnapshotWatchdogFailureReason::PressureHardLimit { kind }
                )) if *kind == expected_kind
            ),
            "wrong pressure result for {expected_kind:?}: {result:?}"
        );
        let expected_code = match expected_kind {
            SnapshotPressureLimitKind::WalPressure => SnapshotBuildFailureCode::WalPressureLimit,
            SnapshotPressureLimitKind::CopyTransactionAge => {
                SnapshotBuildFailureCode::CopyTransactionAgeLimit
            }
            SnapshotPressureLimitKind::CatalogXidAge => unreachable!(),
        };
        let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
            workspace.path().join("build-status"),
            &build_id,
        )
        .unwrap()
        .progress()
        .clone();
        assert_eq!(progress.stage, SnapshotBuildStage::Failed);
        assert_eq!(progress.failure_code, Some(expected_code));
        assert_eq!(
            progress.disposition,
            Some(SnapshotBuildDisposition::OperatorAction)
        );
        assert_eq!(catalog.state(&artifact_id).unwrap(), None);
        assert_no_supervisor_residue(
            &database_url,
            workspace.path(),
            &canonical_root,
            &object_root,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_pressure_warning_is_durable_observable_and_nonfatal() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-pressure-warning-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([96_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-pressure-warning-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_pressure_warning_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let mirror_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        Arc::clone(&provider),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-pressure-warning-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-pressure-warning-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        warning_catalog_pressure_policy(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
    let report = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |stage| {
                if stage == SnapshotBuildStage::Copying {
                    std::thread::sleep(Duration::from_millis(50));
                }
            },
        )
        .await
        .unwrap();
    mirror_task.await.unwrap();
    persistent_control
        .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
        .await
        .unwrap();
    assert_eq!(report.manifest.manifest().artifact_id, artifact_id);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Published);
    assert_eq!(
        progress.pressure_level,
        Some(SnapshotPressureLevel::Warning)
    );
    assert!(progress.pressure_reasons.iter().any(|reason| matches!(
        reason,
        SnapshotPressureReason::CatalogXidAge {
            level: SnapshotPressureLevel::Warning,
            ..
        }
    )));
    assert_no_supervisor_transient_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_watchdog_failure_at_publish_abandons_registered_objects() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new("pipeline-publish-watchdog-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([26_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-publish-watchdog-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_publish_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let barrier_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        provider.clone(),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-publish-watchdog-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-publish-watchdog-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(5),
        short_no_progress_policy(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

    let error = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |stage| {
                if stage == SnapshotBuildStage::Publishing {
                    std::thread::sleep(Duration::from_millis(6_300));
                }
            },
        )
        .await
        .unwrap_err();
    barrier_task.await.unwrap();
    assert!(matches!(
        error,
        SnapshotBuildJobError::Watchdog(SnapshotWatchdogFailureReason::NoProgress)
    ));
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::NoProgressLimit)
    );
    assert_eq!(progress.disposition, Some(SnapshotBuildDisposition::Retry));
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
    persistent_control
        .query_one(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[&persistent_slot],
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_cancellation_during_barrier_wait_cleans_every_private_resource() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new("pipeline-cancel-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([24_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-cancel-source").unwrap(),
            source_epoch,
        )
        .unwrap(),
    ));
    let build_id = SnapshotBuildId::new("pipeline-cancel-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-cancel-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(5),
        test_safety_policy(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let observer_cancellation = cancellation.clone();

    let error = supervisor
        .run_controlled(
            provider,
            &CanonicalHeadSignal::new(),
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &cancellation,
            move |stage| {
                if stage == SnapshotBuildStage::WaitingForPersistentBarrier {
                    observer_cancellation.cancel();
                }
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SnapshotBuildJobError::Cancelled));
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Cancelled);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::CallerCancelled)
    );
    assert_eq!(
        progress.disposition,
        Some(SnapshotBuildDisposition::Cancelled)
    );
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_mid_copy_cancellation_rolls_back_and_joins_every_backend() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_wide_copy_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-mid-copy-cancel-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([91_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-mid-copy-cancel-source").unwrap(),
            source_epoch,
        )
        .unwrap(),
    ));
    let build_id = SnapshotBuildId::new("pipeline-mid-copy-cancel-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-mid-copy-cancel-artifact").unwrap();
    let supervisor = test_supervisor_with_limits(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(2),
        test_safety_policy(),
        Duration::from_secs(30),
        Duration::from_secs(5),
        snapshot_builder::BoundedCopyLimits {
            table: CopyLoadLimits {
                // `setup_wide_copy_fixture` writes 4 KiB values, so the two
                // per-value bounds rise with the fixture. The decoded-byte
                // ceiling is above the whole table, so a build that ignored the
                // cancellation fails on the assertions below rather than on a
                // byte limit reporting `CopyWorkerFailed`.
                max_rows: 25_000,
                max_value_bytes: 8 * 1024,
                max_row_bytes: 16 * 1024,
                max_decoded_value_bytes: 128 * 1024 * 1024,
                max_batch_bytes: 64 * 1024,
                batch_rows: 64,
            },
            max_workers: 1,
            max_inflight_batch_bytes: 64 * 1024,
        },
        Duration::from_secs(10),
    );
    let connections = test_supervisor_connections(&database_url).await;
    let mut owned_backend_pids = vec![
        connections
            .control
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0),
        connections
            .ddl_guard
            .as_ref()
            .unwrap()
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0),
        connections.copy_workers[0]
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0),
        connections
            .pressure
            .as_ref()
            .unwrap()
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0),
    ];
    let journal_path = workspace
        .path()
        .join("build-status")
        .join(format!("{}.json", build_id.as_str()));
    let cancellation = tokio_util::sync::CancellationToken::new();
    let monitor_cancellation = cancellation.clone();
    let monitor_database_url = database_url.clone();
    // 15 s at scale 1 for the whole monitor: slack, not a budget the test
    // proves, since both conditions below hold for the whole copy phase.
    let monitor_deadline = Duration::from_secs(15) * test_time_scale();
    // Phase 2 gets its own budget, measured from the moment phase 1 finishes,
    // so a slow start does not leave it with no time to ask its one question
    // and report a construction failure that is really a slow runner.
    let observation_budget = Duration::from_secs(1) * test_time_scale();
    let monitor = tokio::spawn(async move {
        let client = connect(&monitor_database_url).await;
        let deadline = tokio::time::Instant::now() + monitor_deadline;
        let mut last_rows = 0_u64;
        let mut max_rows = 0_u64;
        let mut samples = 0_u64;

        // Phase 1: wait for the pipeline's own progress signal. `rowsCopied`
        // moving off zero means the builder has committed at least one batch,
        // so the COPY is open and rows are flowing through it.
        let rows_before_cancel = loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the build never reported a partial rowsCopied after {samples} samples \
                 (last {last_rows}, max {max_rows}); the copy phase was never entered"
            );
            samples += 1;
            if let Ok(bytes) = tokio::fs::read(&journal_path).await {
                let progress: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                let rows = progress["rowsCopied"].as_u64().unwrap_or(0);
                last_rows = rows;
                max_rows = max_rows.max(rows);
                if rows > 0 && rows < 20_000 {
                    break rows;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };

        // Phase 2: an assertion, not a trigger. The consumer has taken one
        // batch of the 78 MB `setup_wide_copy_fixture` produces, far more than
        // the path from the backend to this process can buffer, so the backend
        // is still inside the COPY. The retry covers only this task's round
        // trips; running out of deadline means the construction broke, and the
        // panic prints what `pg_stat_activity` held.
        let observation_deadline = tokio::time::Instant::now() + observation_budget;
        let (copy_backend_pid, exporter_pid) = loop {
            if tokio::time::Instant::now() >= observation_deadline {
                let activity = client
                    .query(
                        "SELECT pid, state, wait_event_type, wait_event,
                                query IS NULL AS query_hidden, left(query, 120)
                           FROM pg_catalog.pg_stat_activity
                          WHERE datname = current_database()
                            AND pid <> pg_backend_pid()",
                        &[],
                    )
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| {
                        format!(
                            "pid={} state={:?} wait={:?}/{:?} query_hidden={} query={:?}",
                            row.get::<_, i32>(0),
                            row.get::<_, Option<String>>(1),
                            row.get::<_, Option<String>>(2),
                            row.get::<_, Option<String>>(3),
                            row.get::<_, bool>(4),
                            row.get::<_, Option<String>>(5),
                        )
                    })
                    .collect::<Vec<_>>();
                panic!(
                    "no backend was inside the snapshot COPY after {rows_before_cancel} rows \
                     had already been copied, so the cancellation below would not have been \
                     mid-COPY; pg_stat_activity held {activity:#?}"
                );
            }
            let copy_backend_pid = client
                .query_opt(
                    "SELECT pid
                       FROM pg_catalog.pg_stat_activity
                      WHERE datname = current_database()
                        AND query ILIKE 'COPY%TO STDOUT%FORMAT BINARY%'
                        AND state = 'active'
                      ORDER BY pid
                      LIMIT 1",
                    &[],
                )
                .await
                .unwrap()
                .map(|row| row.get::<_, i32>(0));
            let exporter_pid = client
                .query_opt(
                    "SELECT active_pid
                       FROM pg_catalog.pg_replication_slots
                      WHERE temporary
                        AND slot_name LIKE 'snapshot_build_%'
                        AND active_pid IS NOT NULL
                      ORDER BY slot_name
                      LIMIT 1",
                    &[],
                )
                .await
                .unwrap()
                .map(|row| row.get::<_, i32>(0));
            if let (Some(copy_backend_pid), Some(exporter_pid)) = (copy_backend_pid, exporter_pid) {
                break (copy_backend_pid, exporter_pid);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };

        monitor_cancellation.cancel();
        (rows_before_cancel, copy_backend_pid, exporter_pid)
    });
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

    let result = supervisor
        .run_controlled(
            provider,
            &CanonicalHeadSignal::new(),
            connections,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &cancellation,
            |_| {},
        )
        .await;
    let (rows_before_cancel, copy_backend_pid, exporter_pid) = monitor.await.unwrap();
    assert!(rows_before_cancel > 0 && rows_before_cancel < 20_000);
    // The backend the monitor found inside the COPY has to be the supervisor's
    // own copy worker. Without this the mid-COPY observation could be satisfied
    // by any other COPY on the database, and `assert_backend_pids_gone` below
    // would then be joining a backend nobody proved was copying.
    assert_eq!(
        copy_backend_pid, owned_backend_pids[2],
        "the backend observed inside the COPY was not the supervisor's copy worker"
    );
    assert!(
        result.is_err(),
        "mid-COPY cancellation unexpectedly published"
    );
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Cancelled);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::CallerCancelled)
    );
    assert!(progress.rows_copied >= rows_before_cancel);
    assert!(progress.rows_copied < 20_000);
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
    owned_backend_pids.push(exporter_pid);
    assert_backend_pids_gone(&database_url, &owned_backend_pids).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_mid_upload_provider_failure_removes_registered_objects() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_large_copy_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-mid-upload-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([92_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-mid-upload-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_mid_upload_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let mirror_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        Arc::clone(&provider),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-mid-upload-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-mid-upload-artifact").unwrap();
    let supervisor = test_supervisor_with_limits(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        test_safety_policy(),
        Duration::from_secs(30),
        Duration::from_secs(5),
        snapshot_builder::BoundedCopyLimits {
            table: CopyLoadLimits {
                max_rows: 25_000,
                max_value_bytes: 1024,
                max_row_bytes: 2048,
                max_decoded_value_bytes: 8 * 1024 * 1024,
                max_batch_bytes: 64 * 1024,
                batch_rows: 64,
            },
            max_workers: 1,
            max_inflight_batch_bytes: 64 * 1024,
        },
        Duration::from_secs(10),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
    let store = FailSecondUploadStore::new(store);

    let result = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await;
    mirror_task.await.unwrap();
    persistent_control
        .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
        .await
        .unwrap();
    assert!(
        matches!(
            &result,
            Err(SnapshotBuildJobError::Publication(
                snapshot_pipeline::SnapshotPublicationError::Storage(_)
            ))
        ),
        "unexpected mid-upload result: {result:?}"
    );
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::StorageFailed)
    );
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_active_upload_and_catalog_verification_cancellation_cleans_objects() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    for (ordinal, gate) in [
        ProviderGate::SecondUpload,
        ProviderGate::FirstVerificationHead,
    ]
    .into_iter()
    .enumerate()
    {
        let setup_url = database_url.clone();
        let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
            .await
            .unwrap();
        let source_epoch =
            SourceEpoch::new(format!("pipeline-active-publish-cancel-epoch-{ordinal}")).unwrap();
        let activated_source = ActivatedSource::new(
            &registry,
            source_epoch.clone(),
            CursorCodec::new([94_u8.saturating_add(ordinal as u8); 32]).unwrap(),
            source_control::PgoutputLaunchProfile::V1,
        );
        let plan =
            SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let canonical_root = workspace.path().join("canonical");
        let provider = Arc::new(Mutex::new(
            ScaledDeadlineFileLog::open(
                &canonical_root,
                SourceId::new(format!("pipeline-active-publish-cancel-source-{ordinal}")).unwrap(),
                source_epoch.clone(),
            )
            .unwrap(),
        ));
        let signal = CanonicalHeadSignal::new();
        let persistent_slot = format!("p_active_cancel_{}_{}", ordinal, Uuid::new_v4().simple());
        let persistent_control = connect(&database_url).await;
        persistent_control
            .query_one(
                "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&persistent_slot],
            )
            .await
            .unwrap();
        let mirror_task = tokio::spawn(mirror_cut_to_file_log(
            database_url.clone(),
            persistent_slot.clone(),
            Arc::clone(&provider),
            signal.clone(),
            source_epoch,
        ));
        let build_id =
            SnapshotBuildId::new(format!("pipeline-active-publish-cancel-build-{ordinal}"))
                .unwrap();
        let artifact_id =
            ArtifactId::new(format!("pipeline-active-publish-cancel-artifact-{ordinal}")).unwrap();
        let supervisor = test_supervisor(
            workspace.path(),
            &database_url,
            &registry,
            &plan,
            &build_id,
            &artifact_id,
            Duration::from_secs(15),
            test_safety_policy(),
        );
        let mut catalog =
            SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
        let object_root = workspace.path().join("objects");
        let store = GatedBlobStore::new(
            LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap(),
            gate,
        );
        let cancellation = tokio_util::sync::CancellationToken::new();
        let monitor_cancellation = cancellation.clone();
        let entered = Arc::clone(&store.entered);
        let monitor = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(20), entered.notified())
                .await
                .expect("publication gate was not reached");
            monitor_cancellation.cancel();
        });

        let result = supervisor
            .run_controlled(
                provider,
                &signal,
                test_supervisor_connections(&database_url).await,
                &registry,
                &activated_source,
                plan,
                &mut catalog,
                &store,
                &cancellation,
                |_| {},
            )
            .await;
        monitor.await.unwrap();
        mirror_task.await.unwrap();
        persistent_control
            .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "active publication cancellation unexpectedly published for {gate:?}"
        );
        assert_eq!(catalog.state(&artifact_id).unwrap(), None);
        let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
            workspace.path().join("build-status"),
            &build_id,
        )
        .unwrap()
        .progress()
        .clone();
        assert_eq!(progress.stage, SnapshotBuildStage::Cancelled);
        assert_eq!(
            progress.failure_code,
            Some(SnapshotBuildFailureCode::CallerCancelled)
        );
        assert!(progress.cleanup_completed_at_unix_millis.is_some());
        assert_no_supervisor_residue(
            &database_url,
            workspace.path(),
            &canonical_root,
            &object_root,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_catalog_write_timeout_abandons_and_cleans_uploaded_objects() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-catalog-timeout-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([93_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-catalog-timeout-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_catalog_timeout_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let mirror_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        Arc::clone(&provider),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-catalog-timeout-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-catalog-timeout-artifact").unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        test_safety_policy(),
    );
    let catalog_path = workspace.path().join("catalog.sqlite3");
    let mut catalog = SqliteArtifactCatalog::open(&catalog_path).unwrap();
    // The catalog write under test must fail on this timeout while the fixture
    // below holds the catalog locked, and the cleanup that follows must then
    // succeed on the same connection:
    //
    //     CATALOG_BUSY_TIMEOUT < CATALOG_LOCK_HOLD < 2 * CATALOG_BUSY_TIMEOUT
    //
    // The 500 ms of slack covers a write that reaches SQLite late on a loaded
    // host.
    catalog.set_busy_timeout(CATALOG_BUSY_TIMEOUT).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
    let lock_thread = Arc::new(Mutex::new(None));
    let observer_lock_thread = Arc::clone(&lock_thread);
    let observer_catalog_path = catalog_path.clone();
    let result = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            move |stage| {
                if stage == SnapshotBuildStage::Publishing {
                    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
                    let path = observer_catalog_path.clone();
                    let handle = std::thread::spawn(move || {
                        let connection = Connection::open(path).unwrap();
                        connection.busy_timeout(CATALOG_LOCK_HOLD * 4).unwrap();
                        connection.execute_batch("BEGIN EXCLUSIVE").unwrap();
                        ready_tx.send(()).unwrap();
                        std::thread::sleep(CATALOG_LOCK_HOLD);
                        connection.execute_batch("COMMIT").unwrap();
                    });
                    ready_rx.recv().unwrap();
                    *observer_lock_thread.lock().unwrap() = Some(handle);
                }
            },
        )
        .await;
    lock_thread.lock().unwrap().take().unwrap().join().unwrap();
    mirror_task.await.unwrap();
    persistent_control
        .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
        .await
        .unwrap();
    assert!(
        matches!(
            &result,
            Err(SnapshotBuildJobError::Publication(
                snapshot_pipeline::SnapshotPublicationError::Catalog(_)
            ))
        ),
        "unexpected catalog-timeout result: {result:?}"
    );
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::CatalogFailed)
    );
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_hung_provider_delete_retries_after_cleanup_lease_expiry() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-cleanup-retry-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([97_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-cleanup-retry-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_cleanup_retry_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let mirror_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        Arc::clone(&provider),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-cleanup-retry-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-cleanup-retry-artifact").unwrap();
    let supervisor = test_supervisor_with_limits(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        test_safety_policy(),
        Duration::from_secs(30),
        Duration::from_secs(5),
        snapshot_builder::BoundedCopyLimits {
            table: CopyLoadLimits {
                max_rows: 100,
                max_value_bytes: 1024,
                max_row_bytes: 2048,
                max_decoded_value_bytes: 4096,
                max_batch_bytes: 4096,
                batch_rows: 2,
            },
            max_workers: 1,
            max_inflight_batch_bytes: 4096,
        },
        scaled_cleanup_timeout(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = FailVerificationThenDelayFirstDeleteStore::new(
        LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap(),
        workspace.path().join("build-status"),
        build_id.clone(),
    );

    let result = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            |_| {},
        )
        .await;
    mirror_task.await.unwrap();
    persistent_control
        .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
        .await
        .unwrap();

    assert!(
        matches!(
            &result,
            Err(SnapshotBuildJobError::Publication(
                snapshot_pipeline::SnapshotPublicationError::Catalog(_)
            ))
        ),
        "unexpected cleanup-retry result: {result:?}"
    );
    assert!(
        store
            .observed_nonterminal_during_delete
            .load(Ordering::Acquire),
        "delayed provider cleanup did not observe the unresolved journal"
    );
    let deletes = store.deletes.load(Ordering::Acquire);
    let expected = deletes_with_one_retry(&store.uploaded);
    assert!(
        deletes >= expected,
        "cleanup did not retry after the first timed-out delete: {deletes} deletes, expected at least {expected}"
    );
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::CatalogFailed)
    );
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_publishing_cancellation_with_hung_delete_renews_and_retries_cleanup() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-cancel-cleanup-retry-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([98_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-cancel-cleanup-retry-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_cancel_cleanup_retry_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let mirror_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        Arc::clone(&provider),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-cancel-cleanup-retry-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-cancel-cleanup-retry-artifact").unwrap();
    let supervisor = test_supervisor_with_limits(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        test_safety_policy(),
        Duration::from_secs(1),
        Duration::from_millis(20),
        snapshot_builder::BoundedCopyLimits {
            table: CopyLoadLimits {
                max_rows: 100,
                max_value_bytes: 1024,
                max_row_bytes: 2048,
                max_decoded_value_bytes: 4096,
                max_batch_bytes: 4096,
                batch_rows: 2,
            },
            max_workers: 1,
            max_inflight_batch_bytes: 4096,
        },
        scaled_cleanup_timeout(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = DelayFirstDeleteStore::new(
        LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap(),
        canonical_root.clone(),
        build_id.clone(),
    );
    let cancellation = tokio_util::sync::CancellationToken::new();
    let observer_cancellation = cancellation.clone();

    let result = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &cancellation,
            move |stage| {
                if stage == SnapshotBuildStage::Publishing {
                    observer_cancellation.cancel();
                }
            },
        )
        .await;
    mirror_task.await.unwrap();
    persistent_control
        .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
        .await
        .unwrap();

    assert!(
        matches!(&result, Err(SnapshotBuildJobError::Cancelled)),
        "unexpected cancellation/cleanup-retry result: {result:?}"
    );
    let deletes = store.deletes.load(Ordering::Acquire);
    let expected = deletes_with_one_retry(&store.uploaded);
    assert!(
        deletes >= expected,
        "cancellation cleanup did not retry after the timed-out first delete: {deletes} deletes, expected at least {expected}"
    );
    let first_expiry = store.first_retention_expiry.load(Ordering::Acquire);
    let retry_expiry = store.retry_retention_expiry.load(Ordering::Acquire);
    assert!(
        first_expiry > 0,
        "first cleanup attempt saw no retention lease"
    );
    assert!(
        retry_expiry > first_expiry,
        "retention was not renewed through cancellation cleanup: first={first_expiry}, retry={retry_expiry}"
    );
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Cancelled);
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::CallerCancelled)
    );
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_private_cleanup_failure_joins_retention_owner() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .unwrap();
    let source_epoch = SourceEpoch::new("pipeline-cleanup-owner-epoch").unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([96_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let plan = SnapshotBuildPlan::from_registry(&registry, &source_wide_scope(&registry)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new("pipeline-cleanup-owner-source").unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let persistent_slot = format!("pipeline_cleanup_owner_{}", Uuid::new_v4().simple());
    let persistent_control = connect(&database_url).await;
    persistent_control
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&persistent_slot],
        )
        .await
        .unwrap();
    let mirror_task = tokio::spawn(mirror_cut_to_file_log(
        database_url.clone(),
        persistent_slot.clone(),
        Arc::clone(&provider),
        signal.clone(),
        source_epoch,
    ));
    let build_id = SnapshotBuildId::new("pipeline-cleanup-owner-build").unwrap();
    let artifact_id = ArtifactId::new("pipeline-cleanup-owner-artifact").unwrap();
    let supervisor = test_supervisor_with_retention(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(15),
        test_safety_policy(),
        Duration::from_secs(1),
        Duration::from_millis(100),
    );
    let artifact_path = workspace
        .path()
        .join("private-builds")
        .join(build_id.as_str())
        .join(SNAPSHOT_ARTIFACT_FILE_NAME);
    let poison_target = workspace.path().join("cleanup-poison-target");
    fs::write(&poison_target, b"do-not-follow").unwrap();
    let observer_artifact_path = artifact_path.clone();
    let observer_poison_target = poison_target.clone();
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

    let result = supervisor
        .run_controlled(
            Arc::clone(&provider),
            &signal,
            test_supervisor_connections(&database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            move |stage| {
                if stage == SnapshotBuildStage::Publishing {
                    fs::remove_file(&observer_artifact_path).unwrap();
                    std::os::unix::fs::symlink(&observer_poison_target, &observer_artifact_path)
                        .unwrap();
                }
            },
        )
        .await;
    mirror_task.await.unwrap();
    persistent_control
        .query_one("SELECT pg_drop_replication_slot($1)", &[&persistent_slot])
        .await
        .unwrap();
    assert!(
        matches!(
            &result,
            Err(SnapshotBuildJobError::Cleanup(_))
                | Err(SnapshotBuildJobError::Publication(
                    snapshot_pipeline::SnapshotPublicationError::Progress(_)
                ))
        ),
        "unexpected private-cleanup failure: {result:?}"
    );
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);

    let expiry_before = snapshot_retention_expiry(&canonical_root, &build_id);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        snapshot_retention_expiry(&canonical_root, &build_id),
        expiry_before,
        "renewal owner continued after the supervisor returned"
    );
    let remaining = expiry_before
        .saturating_sub(unix_millis())
        .saturating_add(25);
    tokio::time::sleep(Duration::from_millis(u64::try_from(remaining).unwrap_or(0))).await;
    {
        let mut provider = provider.lock().unwrap();
        let replacement = provider
            .acquire_snapshot_build_retention(
                build_id.clone(),
                SnapshotBuildOwnerId::new("cleanup-owner-replacement").unwrap(),
                Duration::from_millis(600),
            )
            .unwrap();
        provider
            .release_snapshot_build_retention(&replacement)
            .unwrap();
    }
    fs::remove_file(&artifact_path).unwrap();
    fs::remove_dir(artifact_path.parent().unwrap()).unwrap();
    fs::remove_file(poison_target).unwrap();
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_cancellation_matrix_cleans_every_lifecycle_stage() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    for (ordinal, stage) in [
        SnapshotBuildStage::Admitted,
        SnapshotBuildStage::RetentionLeased,
        SnapshotBuildStage::Exported,
        SnapshotBuildStage::RollingForward,
        SnapshotBuildStage::Copying,
        SnapshotBuildStage::WaitingForPersistentBarrier,
        SnapshotBuildStage::Finalizing,
        SnapshotBuildStage::Uploading,
        SnapshotBuildStage::Publishing,
    ]
    .into_iter()
    .enumerate()
    {
        run_supervisor_cancellation_case(&database_url, stage, ordinal as u8).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_maximum_duration_aborts_owned_resources() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    run_forced_lifecycle_failure(ForcedLifecycleFailure::MaximumDuration).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_pressure_sampling_uncertainty_fails_closed() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    run_forced_lifecycle_failure(ForcedLifecycleFailure::PressureSamplerLost).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_exporter_backend_loss_removes_temporary_slot() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    run_forced_lifecycle_failure(ForcedLifecycleFailure::ExporterLost).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_temporary_stream_loss_during_roll_forward_fails_closed() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    run_forced_lifecycle_failure(ForcedLifecycleFailure::TemporaryStreamLostDuringRollForward)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn supervisor_copy_backend_loss_rolls_back_imported_transaction() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    run_forced_lifecycle_failure(ForcedLifecycleFailure::CopyWorkerLost).await;
}

#[derive(Debug, Clone, Copy)]
enum ForcedLifecycleFailure {
    MaximumDuration,
    PressureSamplerLost,
    ExporterLost,
    TemporaryStreamLostDuringRollForward,
    CopyWorkerLost,
}

async fn run_forced_lifecycle_failure(case: ForcedLifecycleFailure) {
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .unwrap();
    let suffix = match case {
        ForcedLifecycleFailure::MaximumDuration => "maximum-duration",
        ForcedLifecycleFailure::PressureSamplerLost => "pressure-lost",
        ForcedLifecycleFailure::ExporterLost => "exporter-lost",
        ForcedLifecycleFailure::TemporaryStreamLostDuringRollForward => "stream-lost-roll-forward",
        ForcedLifecycleFailure::CopyWorkerLost => "copy-lost",
    };
    let source_epoch = SourceEpoch::new(format!("pipeline-{suffix}-epoch")).unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([61_u8; 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new(format!("pipeline-{suffix}-source")).unwrap(),
            source_epoch,
        )
        .unwrap(),
    ));
    let build_id = SnapshotBuildId::new(format!("pipeline-{suffix}-build")).unwrap();
    let artifact_id = ArtifactId::new(format!("pipeline-{suffix}-artifact")).unwrap();
    let safety = if matches!(case, ForcedLifecycleFailure::MaximumDuration) {
        SnapshotBuildSafetyPolicy::new(
            SnapshotWatchdogConfig::new(Duration::from_millis(250), Duration::from_secs(5))
                .unwrap(),
            Duration::from_millis(20),
            Duration::from_millis(100),
            test_pressure_policy(),
        )
        .unwrap()
    } else if matches!(case, ForcedLifecycleFailure::PressureSamplerLost) {
        SnapshotBuildSafetyPolicy::new(
            SnapshotWatchdogConfig::new(Duration::from_secs(10), Duration::from_secs(5)).unwrap(),
            Duration::from_millis(10),
            Duration::from_millis(100),
            test_pressure_policy(),
        )
        .unwrap()
    } else if matches!(
        case,
        ForcedLifecycleFailure::ExporterLost
            | ForcedLifecycleFailure::TemporaryStreamLostDuringRollForward
            | ForcedLifecycleFailure::CopyWorkerLost
    ) {
        SnapshotBuildSafetyPolicy::new(
            SnapshotWatchdogConfig::new(Duration::from_secs(10), Duration::from_secs(5)).unwrap(),
            Duration::from_secs(5),
            Duration::from_millis(100),
            test_pressure_policy(),
        )
        .unwrap()
    } else {
        test_safety_policy()
    };
    let supervisor = test_supervisor(
        workspace.path(),
        &database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(2),
        safety,
    );
    let connections = test_supervisor_connections(&database_url).await;
    let control_pid: i32 = connections
        .control
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let ddl_guard_pid: i32 = connections
        .ddl_guard
        .as_ref()
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let pressure_pid: i32 = connections
        .pressure
        .as_ref()
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let copy_pid: i32 = connections.copy_workers[0]
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let observed_backend_pids = Arc::new(Mutex::new(vec![
        control_pid,
        ddl_guard_pid,
        pressure_pid,
        copy_pid,
    ]));
    let observer_backend_pids = Arc::clone(&observed_backend_pids);
    let killer_url = database_url.clone();
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();

    let result = supervisor
        .run_controlled(
            provider,
            &CanonicalHeadSignal::new(),
            connections,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &tokio_util::sync::CancellationToken::new(),
            move |stage| {
                let exporter_pid = (stage == SnapshotBuildStage::Exported).then(|| {
                    let exporter_pid = temporary_snapshot_exporter_pid_blocking(&killer_url);
                    observer_backend_pids.lock().unwrap().push(exporter_pid);
                    exporter_pid
                });
                match case {
                    ForcedLifecycleFailure::MaximumDuration
                        if stage == SnapshotBuildStage::Exported =>
                    {
                        std::thread::sleep(Duration::from_millis(400));
                    }
                    ForcedLifecycleFailure::PressureSamplerLost
                        if stage == SnapshotBuildStage::Exported =>
                    {
                        assert!(terminate_backend_blocking(&killer_url, pressure_pid));
                    }
                    ForcedLifecycleFailure::ExporterLost
                        if stage == SnapshotBuildStage::Exported =>
                    {
                        assert!(terminate_backend_blocking(
                            &killer_url,
                            exporter_pid.expect("exporter PID observed at exported stage"),
                        ));
                    }
                    ForcedLifecycleFailure::TemporaryStreamLostDuringRollForward
                        if stage == SnapshotBuildStage::RollingForward =>
                    {
                        let exporter_pid = *observer_backend_pids
                            .lock()
                            .unwrap()
                            .last()
                            .expect("exporter PID observed before roll-forward");
                        assert!(terminate_backend_blocking(&killer_url, exporter_pid));
                    }
                    ForcedLifecycleFailure::CopyWorkerLost
                        if stage == SnapshotBuildStage::Copying =>
                    {
                        assert!(terminate_backend_blocking(&killer_url, copy_pid));
                    }
                    _ => {}
                }
            },
        )
        .await;
    assert!(
        result.is_err(),
        "forced lifecycle failure unexpectedly published"
    );
    let result_error = format!("{:?}", result.as_ref().err());
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Failed);
    match case {
        ForcedLifecycleFailure::MaximumDuration => assert_eq!(
            progress.failure_code,
            Some(SnapshotBuildFailureCode::BuildDurationLimit)
        ),
        ForcedLifecycleFailure::PressureSamplerLost => assert_eq!(
            progress.failure_code,
            Some(SnapshotBuildFailureCode::PressureSamplingUncertain)
        ),
        ForcedLifecycleFailure::ExporterLost => assert!(matches!(
            progress.failure_code,
            Some(SnapshotBuildFailureCode::TemporaryStreamLost)
        )),
        ForcedLifecycleFailure::TemporaryStreamLostDuringRollForward => assert!(
            matches!(
                progress.failure_code,
                Some(SnapshotBuildFailureCode::TemporaryStreamLost)
            ),
            "unexpected roll-forward stream-loss code: {:?}; supervisor error: {result_error}",
            progress.failure_code,
        ),
        ForcedLifecycleFailure::CopyWorkerLost => assert!(
            matches!(
                progress.failure_code,
                Some(SnapshotBuildFailureCode::CopyWorkerFailed)
            ),
            "unexpected COPY-worker failure code: {:?}; supervisor error: {result_error}",
            progress.failure_code,
        ),
    }
    assert_no_supervisor_residue(
        &database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
    let observed_backend_pids = observed_backend_pids.lock().unwrap().clone();
    assert_backend_pids_gone(&database_url, &observed_backend_pids).await;
}

fn terminate_backend_blocking(database_url: &str, backend_pid: i32) -> bool {
    let database_url = database_url.to_owned();
    std::thread::spawn(move || {
        let mut client = BlockingClient::connect(&database_url, BlockingNoTls).unwrap();
        client
            .query_one("SELECT pg_terminate_backend($1)", &[&backend_pid])
            .unwrap()
            .get(0)
    })
    .join()
    .unwrap()
}

fn generate_retained_wal_blocking(database_url: &str) {
    let database_url = database_url.to_owned();
    std::thread::spawn(move || {
        let mut client = BlockingClient::connect(&database_url, BlockingNoTls).unwrap();
        client
            .execute(
                &format!("UPDATE public.{TABLE} SET value = repeat('w', 524288) WHERE id = 1"),
                &[],
            )
            .unwrap();
    })
    .join()
    .unwrap();
}

fn temporary_snapshot_exporter_pid_blocking(database_url: &str) -> i32 {
    let database_url = database_url.to_owned();
    std::thread::spawn(move || {
        let mut client = BlockingClient::connect(&database_url, BlockingNoTls).unwrap();
        client
            .query_one(
                "SELECT active_pid
                   FROM pg_replication_slots
                  WHERE temporary AND slot_name LIKE 'snapshot_build_%'
                    AND active_pid IS NOT NULL
                  ORDER BY slot_name
                  LIMIT 1",
                &[],
            )
            .unwrap()
            .get(0)
    })
    .join()
    .unwrap()
}

fn recovery_manifest(artifact_id: &ArtifactId) -> ValidatedArtifactManifest {
    let bytes = b"restart-recovery-object";
    let digest = hex::encode(Sha256::digest(bytes));
    let mut zstd_bytes = vec![b'z'];
    zstd_bytes.extend_from_slice(bytes);
    let zstd_digest = hex::encode(Sha256::digest(&zstd_bytes));
    let value = serde_json::json!({
        "protocolVersion": 1,
        "artifactId": artifact_id.as_str(),
        "createdAt": "2026-07-14T12:00:00Z",
        "decoded": {
            "objectId": format!("sha256:{digest}"),
            "sha256": digest,
            "bytes": bytes.len(),
            "mediaType": "application/vnd.calque.direct-columnar.v4",
            "format": "directColumnar",
            "schemaHash": format!("sha256:{}", "a".repeat(64)),
            "collectionSetHash": format!("sha256:{}", "8".repeat(64)),
            "formatVersion": 4,
            "identity": { "manifestVersion": "manifest-1", "authorizationScopeVersion": "scope-1" },
            "schema": {
                "format": "calque-direct-columnar",
                "version": 4,
                "tables": [{
                    "name": "tasks",
                    "primaryKey": ["id"],
                    "columns": [{ "name": "id", "type": "int64", "nullable": false }]
                }]
            },
            "chunks": { "bytes": 4194304, "sha256": [format!("sha256:{digest}")] }
        },
        "pit": {
            "sourceEpoch": "restart-recovery-epoch",
            "snapshotCursor": "restart-recovery-cursor",
            "copyConsistentLsn": "0/A",
            "cutMessageLsn": "0/B",
            "cutMarkerId": "restart-recovery-build"
        },
        "compatibility": {
            "publicationGeneration": 1,
            "schemaGeneration": 2,
            "authorizationGeneration": 3,
            "authorizationPolicyHash": format!("sha256:{}", "9".repeat(64)),
            "authorizationScopeHash": format!("sha256:{}", "b".repeat(64)),
            "tableSetHash": format!("sha256:{}", "c".repeat(64)),
            "shardProfileHash": format!("sha256:{}", "d".repeat(64)),
            "indexProfileHash": format!("sha256:{}", "e".repeat(64)),
            "typeCodecGeneration": 4,
            "collationGeneration": 5
        },
        "tables": [{
            "tableId": "public.tasks",
            "sourceSchema": "public",
            "sourceTable": "tasks",
            "localSchema": "main",
            "localTable": "tasks",
            "schemaHash": format!("sha256:{}", "f".repeat(64)),
            "columnProfileHash": format!("sha256:{}", "1".repeat(64)),
            "rowCount": 1,
            "primaryKeyCodec": "pk-v1/protobuf",
            "columns": [{ "columnId": 1, "name": "id" }]
        }],
        "representations": [
            {
                "representationId": format!("sha256:{zstd_digest}"),
                "objectId": format!("sha256:{zstd_digest}"),
                "encoding": "zstd",
                "encodedSha256": zstd_digest,
                "encodedBytes": zstd_bytes.len(),
                "zstd": { "level": 3, "windowLog": 23 }
            },
            {
                "representationId": format!("sha256:{digest}"),
                "objectId": format!("sha256:{digest}"),
                "encoding": "identity",
                "encodedSha256": digest,
                "encodedBytes": bytes.len()
            }
        ],
        "memory": {
            "estimatedPeakBytes": 1000,
            "recommendedGrowthReserveBytes": 500
        }
    });
    ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn recovery_representation_bytes(encoding: RepresentationEncoding) -> Vec<u8> {
    let bytes = b"restart-recovery-object";
    match encoding {
        RepresentationEncoding::Zstd => {
            let mut encoded = vec![b'z'];
            encoded.extend_from_slice(bytes);
            encoded
        }
        RepresentationEncoding::Identity => bytes.to_vec(),
        RepresentationEncoding::BrotliLargeWindow => {
            let mut encoded = vec![b'b'];
            encoded.extend_from_slice(bytes);
            encoded
        }
        RepresentationEncoding::PackedBrotliLargeWindow => {
            let mut encoded = vec![b'p'];
            encoded.extend_from_slice(bytes);
            encoded
        }
    }
}

fn unix_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn snapshot_retention_expiry(canonical_root: &Path, build_id: &SnapshotBuildId) -> i64 {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(canonical_root.join("manifest.json")).unwrap()).unwrap();
    manifest["snapshotBuildRetentionLeases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|lease| lease["buildId"].as_str() == Some(build_id.as_str()))
        .and_then(|lease| lease["expiresMs"].as_i64())
        .expect("snapshot retention lease remains until authoritative expiry")
}

fn test_safety_policy() -> SnapshotBuildSafetyPolicy {
    SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(30), Duration::from_secs(10)).unwrap(),
        Duration::from_millis(50),
        Duration::from_secs(2),
        test_pressure_policy(),
    )
    .unwrap()
}

fn test_pressure_policy() -> SnapshotPressurePolicy {
    SnapshotPressurePolicy::new(
        SnapshotPressureThresholds::new(64 * 1024 * 1024, 128 * 1024 * 1024).unwrap(),
        SnapshotPressureThresholds::new(100_000_000, 200_000_000).unwrap(),
        Duration::from_secs(30),
        Duration::from_secs(60),
        SnapshotRemainingWalThresholds::new(64 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn hard_catalog_pressure_policy() -> SnapshotBuildSafetyPolicy {
    SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(30), Duration::from_secs(10)).unwrap(),
        Duration::from_millis(10),
        Duration::from_secs(2),
        SnapshotPressurePolicy::new(
            SnapshotPressureThresholds::new(u64::MAX - 1, u64::MAX).unwrap(),
            SnapshotPressureThresholds::new(1, 1).unwrap(),
            Duration::from_secs(30),
            Duration::from_secs(60),
            SnapshotRemainingWalThresholds::new(1, 0).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn hard_wal_pressure_policy() -> SnapshotBuildSafetyPolicy {
    SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(30), Duration::from_secs(10)).unwrap(),
        Duration::from_millis(10),
        Duration::from_secs(2),
        SnapshotPressurePolicy::new(
            SnapshotPressureThresholds::new(1, 1).unwrap(),
            SnapshotPressureThresholds::new(u64::MAX - 1, u64::MAX).unwrap(),
            Duration::from_secs(30),
            Duration::from_secs(60),
            SnapshotRemainingWalThresholds::new(1, 0).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn hard_copy_age_pressure_policy() -> SnapshotBuildSafetyPolicy {
    SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(30), Duration::from_secs(10)).unwrap(),
        Duration::from_millis(10),
        Duration::from_secs(2),
        SnapshotPressurePolicy::new(
            SnapshotPressureThresholds::new(u64::MAX - 1, u64::MAX).unwrap(),
            SnapshotPressureThresholds::new(u64::MAX - 1, u64::MAX).unwrap(),
            Duration::from_millis(1),
            Duration::from_millis(1),
            SnapshotRemainingWalThresholds::new(1, 0).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn warning_catalog_pressure_policy() -> SnapshotBuildSafetyPolicy {
    SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(30), Duration::from_secs(10)).unwrap(),
        Duration::from_millis(10),
        Duration::from_secs(2),
        SnapshotPressurePolicy::new(
            SnapshotPressureThresholds::new(u64::MAX - 1, u64::MAX).unwrap(),
            SnapshotPressureThresholds::new(1, u64::MAX).unwrap(),
            Duration::from_secs(30),
            Duration::from_secs(60),
            SnapshotRemainingWalThresholds::new(1, 0).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn short_no_progress_policy() -> SnapshotBuildSafetyPolicy {
    SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(30), Duration::from_secs(5)).unwrap(),
        Duration::from_millis(25),
        Duration::from_secs(2),
        SnapshotPressurePolicy::new(
            SnapshotPressureThresholds::new(64 * 1024 * 1024, 128 * 1024 * 1024).unwrap(),
            SnapshotPressureThresholds::new(100_000_000, 200_000_000).unwrap(),
            Duration::from_secs(30),
            Duration::from_secs(60),
            SnapshotRemainingWalThresholds::new(64 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn test_supervisor_connections(database_url: &str) -> SnapshotBuildConnections {
    SnapshotBuildConnections {
        control: connect_owned(database_url).await,
        ddl_guard: Some(connect_owned(database_url).await),
        copy_workers: vec![connect_owned(database_url).await],
        pressure: Some(connect_owned(database_url).await),
    }
}

async fn run_supervisor_cancellation_case(
    database_url: &str,
    cancellation_stage: SnapshotBuildStage,
    ordinal: u8,
) {
    let setup_url = database_url.to_owned();
    let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join fixture setup");
    let source_epoch = SourceEpoch::new(format!("pipeline-cancel-matrix-epoch-{ordinal}")).unwrap();
    let activated_source = ActivatedSource::new(
        &registry,
        source_epoch.clone(),
        CursorCodec::new([40_u8.saturating_add(ordinal); 32]).unwrap(),
        source_control::PgoutputLaunchProfile::V1,
    );
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let canonical_root = workspace.path().join("canonical");
    let provider = Arc::new(Mutex::new(
        ScaledDeadlineFileLog::open(
            &canonical_root,
            SourceId::new(format!("pipeline-cancel-matrix-source-{ordinal}")).unwrap(),
            source_epoch.clone(),
        )
        .unwrap(),
    ));
    let signal = CanonicalHeadSignal::new();
    let needs_persistent_barrier = matches!(
        cancellation_stage,
        SnapshotBuildStage::Finalizing
            | SnapshotBuildStage::Uploading
            | SnapshotBuildStage::Publishing
    );
    let persistent = if needs_persistent_barrier {
        let slot = format!("pipeline_cancel_{}_{}", ordinal, Uuid::new_v4().simple());
        let control = connect(database_url).await;
        control
            .query_one(
                "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .unwrap();
        let task = tokio::spawn(mirror_cut_to_file_log(
            database_url.to_owned(),
            slot.clone(),
            provider.clone(),
            signal.clone(),
            source_epoch,
        ));
        Some((control, slot, task))
    } else {
        None
    };
    let build_id = SnapshotBuildId::new(format!("pipeline-cancel-matrix-build-{ordinal}")).unwrap();
    let artifact_id =
        ArtifactId::new(format!("pipeline-cancel-matrix-artifact-{ordinal}")).unwrap();
    let supervisor = test_supervisor(
        workspace.path(),
        database_url,
        &registry,
        &plan,
        &build_id,
        &artifact_id,
        Duration::from_secs(5),
        test_safety_policy(),
    );
    let mut catalog =
        SqliteArtifactCatalog::open(workspace.path().join("catalog.sqlite3")).unwrap();
    let object_root = workspace.path().join("objects");
    let store = LocalFilesystemBlobStore::open(&object_root, 32 * 1024 * 1024).unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let observer_cancellation = cancellation.clone();

    let error = supervisor
        .run_controlled(
            provider,
            &signal,
            test_supervisor_connections(database_url).await,
            &registry,
            &activated_source,
            plan,
            &mut catalog,
            &store,
            &cancellation,
            move |stage| {
                if stage == cancellation_stage {
                    observer_cancellation.cancel();
                }
            },
        )
        .await
        .expect_err("stage cancellation must fail the private build");
    assert!(
        matches!(
            error,
            SnapshotBuildJobError::Cancelled
                | SnapshotBuildJobError::Builder(_)
                | SnapshotBuildJobError::Publication(_)
        ),
        "unexpected error after cancelling at {cancellation_stage:?}: {error:?}"
    );
    if let Some((control, slot, task)) = persistent {
        task.await.unwrap();
        control
            .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
            .await
            .unwrap();
    }
    assert_eq!(catalog.state(&artifact_id).unwrap(), None);
    let progress = snapshot_pipeline::SnapshotBuildJournal::reopen(
        workspace.path().join("build-status"),
        &build_id,
    )
    .unwrap()
    .progress()
    .clone();
    assert_eq!(
        progress.stage,
        SnapshotBuildStage::Cancelled,
        "wrong terminal stage after cancelling at {cancellation_stage:?}"
    );
    assert_eq!(
        progress.failure_code,
        Some(SnapshotBuildFailureCode::CallerCancelled)
    );
    assert_eq!(
        progress.disposition,
        Some(SnapshotBuildDisposition::Cancelled)
    );
    assert!(progress.cleanup_completed_at_unix_millis.is_some());
    assert_no_supervisor_residue(
        database_url,
        workspace.path(),
        &canonical_root,
        &object_root,
    )
    .await;
}

async fn assert_no_supervisor_residue(
    database_url: &str,
    workspace: &Path,
    canonical_root: &Path,
    object_root: &Path,
) {
    assert_no_supervisor_transient_residue(database_url, workspace, canonical_root, object_root)
        .await;
    assert_eq!(
        count_files(object_root),
        0,
        "blob store left object/staging files"
    );
}

async fn assert_no_supervisor_transient_residue(
    database_url: &str,
    workspace: &Path,
    canonical_root: &Path,
    object_root: &Path,
) {
    assert_eq!(
        count_files_if_present(&workspace.join("private-builds")),
        0,
        "private build workspace left files"
    );
    assert_eq!(
        count_files_if_present(&object_root.join("staging")),
        0,
        "blob store left staging files"
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(canonical_root.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["snapshotBuildRetentionLeases"]
            .as_array()
            .unwrap()
            .len(),
        0,
        "snapshot retention lease was not released"
    );
    let client = connect(database_url).await;
    let temporary_slots: i64 = client
        .query_one(
            "SELECT count(*)
               FROM pg_catalog.pg_replication_slots
              WHERE temporary
                AND database = current_database()
                AND slot_name LIKE 'snapshot_build_%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(temporary_slots, 0, "temporary snapshot slot leaked");
    for _ in 0..60 {
        let snapshot_backends: i64 = client
            .query_one(
                "SELECT count(*)
                   FROM pg_catalog.pg_stat_activity
                  WHERE datname = current_database()
                    AND pid <> pg_backend_pid()
                    AND application_name = $1",
                &[&OWNED_APPLICATION_NAME],
            )
            .await
            .unwrap()
            .get(0);
        if snapshot_backends == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("snapshot supervisor left an owned PostgreSQL session open");
}

async fn assert_backend_pids_gone(database_url: &str, backend_pids: &[i32]) {
    let client = connect(database_url).await;
    for _ in 0..60 {
        let remaining: i64 = client
            .query_one(
                "SELECT count(*)
                   FROM pg_catalog.pg_stat_activity
                  WHERE pid = ANY($1::integer[])",
                &[&backend_pids],
            )
            .await
            .unwrap()
            .get(0);
        if remaining == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("owned PostgreSQL backends remained after cleanup: {backend_pids:?}");
}

fn count_files(root: &Path) -> usize {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| if path.is_dir() { count_files(&path) } else { 1 })
        .sum()
}

fn count_files_if_present(root: &Path) -> usize {
    if root.exists() {
        count_files(root)
    } else {
        0
    }
}

struct FailingRetentionProvider {
    log: FileCanonicalLog,
}

/// Multiplier for the deadlines a fixture's provider enforces on itself, read
/// from `CALQUE_TEST_TIME_SCALE` (default 1). CI sets it for slow shared
/// runners. It changes no product constant: fixture wrappers such as
/// `ScaledDeadlineFileLog` apply it, and only to budgets the test does not
/// prove.
fn test_time_scale() -> u32 {
    match env::var("CALQUE_TEST_TIME_SCALE") {
        Err(env::VarError::NotPresent) => 1,
        Ok(value) => value
            .trim()
            .parse::<u32>()
            .ok()
            .filter(|scale| *scale >= 1)
            .unwrap_or_else(|| {
                panic!("CALQUE_TEST_TIME_SCALE must be a positive integer, found {value:?}")
            }),
        Err(env::VarError::NotUnicode(value)) => {
            panic!("CALQUE_TEST_TIME_SCALE must be a positive integer, found {value:?}")
        }
    }
}

/// `FileCanonicalLog` with its retention-operation deadline multiplied by
/// `CALQUE_TEST_TIME_SCALE`; every operation itself delegates unchanged.
///
/// The supervisor tests use the file log in place of the Postgres-fenced one
/// and prove nothing about its 3 s retention deadline, which on a shared disk
/// is bounded by fsync latency. The 50 ms `maximum_barrier_read_duration()` is
/// not scaled: `wait_for_persistent_barrier` refuses a provider whose read
/// deadline exceeds `min(request timeout, max(poll interval, 250 ms))`, 250 ms
/// for this fixture, with `Barrier(ProviderTimeout)`.
struct ScaledDeadlineFileLog {
    log: FileCanonicalLog,
    scale: u32,
}

impl ScaledDeadlineFileLog {
    /// Same shape as `FileCanonicalLog::open`, so a call site swaps one name
    /// and keeps its `.unwrap()`.
    fn open(
        root: impl AsRef<Path>,
        source_id: SourceId,
        source_epoch: SourceEpoch,
    ) -> Result<Self, LogError> {
        Ok(Self {
            log: FileCanonicalLog::open(root, source_id, source_epoch)?,
            scale: test_time_scale(),
        })
    }

    fn report(&self, operation: &str, elapsed: Duration, unscaled: Duration) {
        eprintln!(
            "live_publication: retention {operation} took {elapsed:?}; the provider's own \
             deadline is {unscaled:?}, scaled to {:?} by CALQUE_TEST_TIME_SCALE={}",
            unscaled * self.scale,
            self.scale
        );
    }
}

/// `FileCanonicalLog`'s own retention deadline, multiplied by the scale.
/// `test_supervisor_with_limits` reads it to keep every fixture TTL inside the
/// lease guard's `operation_timeout <= ttl / 3` bound, so it is read from the
/// product type through a throwaway log rather than copied as a literal.
fn scaled_file_log_retention_deadline() -> Duration {
    static UNSCALED: OnceLock<Duration> = OnceLock::new();
    let unscaled = *UNSCALED.get_or_init(|| {
        let probe = tempfile::tempdir().expect("probe workspace for the retention deadline");
        FileCanonicalLog::open(
            probe.path().join("canonical"),
            SourceId::new("retention-deadline-probe").unwrap(),
            SourceEpoch::new("retention-deadline-probe").unwrap(),
        )
        .expect("open the probe log")
        .maximum_retention_operation_duration()
    });
    unscaled * test_time_scale()
}

/// The floor holds only while it is the product's own deadline. This runs
/// without PostgreSQL.
#[test]
fn the_scaled_floor_is_the_product_deadline_times_the_scale() {
    let workspace = tempfile::tempdir().unwrap();
    let log = FileCanonicalLog::open(
        workspace.path().join("canonical"),
        SourceId::new("retention-deadline-check").unwrap(),
        SourceEpoch::new("retention-deadline-check").unwrap(),
    )
    .unwrap();
    assert_eq!(
        scaled_file_log_retention_deadline(),
        log.maximum_retention_operation_duration() * test_time_scale()
    );
}

impl SnapshotBuildRetention for ScaledDeadlineFileLog {
    fn maximum_retention_operation_duration(&self) -> Duration {
        self.log.maximum_retention_operation_duration() * self.scale
    }

    fn acquire_snapshot_build_retention(
        &mut self,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        let started = Instant::now();
        let result = self
            .log
            .acquire_snapshot_build_retention(build_id, owner_id, ttl);
        self.report(
            "acquire",
            started.elapsed(),
            self.log.maximum_retention_operation_duration(),
        );
        result
    }

    fn renew_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        let started = Instant::now();
        let result = self.log.renew_snapshot_build_retention(lease, ttl);
        self.report(
            "renew",
            started.elapsed(),
            self.log.maximum_retention_operation_duration(),
        );
        result
    }

    fn release_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError> {
        self.log.release_snapshot_build_retention(lease)
    }
}

impl CanonicalBarrierRead for ScaledDeadlineFileLog {
    fn maximum_barrier_read_duration(&self) -> Duration {
        // Unscaled on purpose; see the type's documentation.
        self.log.maximum_barrier_read_duration()
    }

    fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
        CanonicalBarrierRead::source_epoch(&self.log)
    }

    fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
        self.log.minimum_retained_sequence()
    }

    fn durable_head_sequence(&self) -> Result<u64, LogError> {
        self.log.durable_head_sequence()
    }

    fn read_page_after_bounded(
        &self,
        after: u64,
        through: u64,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        CanonicalBarrierRead::read_page_after_bounded(
            &self.log,
            after,
            through,
            max_records,
            max_bytes,
        )
    }
}

/// The persistent-barrier mirror appends into the file log directly, through
/// the wrapper the supervisor sees. A bare `FileCanonicalLog` does not
/// implement this, so a new fixture cannot hand the supervisor an unscaled
/// budget and mirror into it.
trait FileLogAccess {
    fn file_log(&mut self) -> &mut FileCanonicalLog;
}

impl FileLogAccess for ScaledDeadlineFileLog {
    fn file_log(&mut self) -> &mut FileCanonicalLog {
        &mut self.log
    }
}

#[derive(Clone, Copy)]
enum RenewalFailureMode {
    Error,
    Stall,
}

/// The stall mode proves the guard notices a renewal that overruns the
/// provider's own deadline, so the deadline and the stall are one ratio
/// (180 ms over 150 ms). `CALQUE_TEST_TIME_SCALE` multiplies both: the ratio is
/// what the test proves, and the absolute values only have to sit above the
/// host's fsync latency.
struct RenewalFailureRetentionProvider {
    log: FileCanonicalLog,
    mode: RenewalFailureMode,
    scale: u32,
    first_lease: Option<SnapshotBuildRetentionLease>,
    last_renewed_lease: Option<SnapshotBuildRetentionLease>,
}

impl SnapshotBuildRetention for RenewalFailureRetentionProvider {
    fn maximum_retention_operation_duration(&self) -> Duration {
        Duration::from_millis(150) * self.scale
    }

    fn acquire_snapshot_build_retention(
        &mut self,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        let lease = self
            .log
            .acquire_snapshot_build_retention(build_id, owner_id, ttl)?;
        if self.first_lease.is_none() {
            self.first_lease = Some(lease.clone());
        }
        Ok(lease)
    }

    fn renew_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        match self.mode {
            RenewalFailureMode::Error => Err(LogError::PostgresUnavailable),
            RenewalFailureMode::Stall => {
                std::thread::sleep(Duration::from_millis(180) * self.scale);
                let renewed = self.log.renew_snapshot_build_retention(lease, ttl)?;
                self.last_renewed_lease = Some(renewed.clone());
                Ok(renewed)
            }
        }
    }

    fn release_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError> {
        self.log.release_snapshot_build_retention(lease)
    }
}

impl CanonicalBarrierRead for RenewalFailureRetentionProvider {
    fn maximum_barrier_read_duration(&self) -> Duration {
        Duration::from_millis(50)
    }

    fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
        Ok(self.log.source_epoch().clone())
    }

    fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
        Ok(self.log.minimum_retained().sequence)
    }

    fn durable_head_sequence(&self) -> Result<u64, LogError> {
        Ok(self.log.head().sequence)
    }

    fn read_page_after_bounded(
        &self,
        after: u64,
        through: u64,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        self.log
            .read_page_after_bounded(after, Some(through), max_records, max_bytes)
    }
}

impl SnapshotBuildRetention for FailingRetentionProvider {
    fn maximum_retention_operation_duration(&self) -> Duration {
        Duration::from_millis(50)
    }

    fn acquire_snapshot_build_retention(
        &mut self,
        _build_id: SnapshotBuildId,
        _owner_id: SnapshotBuildOwnerId,
        _ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        Err(LogError::PostgresUnavailable)
    }

    fn renew_snapshot_build_retention(
        &mut self,
        _lease: &SnapshotBuildRetentionLease,
        _ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        Err(LogError::PostgresUnavailable)
    }

    fn release_snapshot_build_retention(
        &mut self,
        _lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError> {
        Err(LogError::PostgresUnavailable)
    }
}

impl CanonicalBarrierRead for FailingRetentionProvider {
    fn maximum_barrier_read_duration(&self) -> Duration {
        Duration::from_millis(50)
    }

    fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
        Ok(self.log.source_epoch().clone())
    }

    fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
        Ok(self.log.minimum_retained().sequence)
    }

    fn durable_head_sequence(&self) -> Result<u64, LogError> {
        Ok(self.log.head().sequence)
    }

    fn read_page_after_bounded(
        &self,
        after: u64,
        through: u64,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        self.log
            .read_page_after_bounded(after, Some(through), max_records, max_bytes)
    }
}

#[allow(clippy::too_many_arguments)]
fn test_supervisor(
    workspace: &Path,
    database_url: &str,
    registry: &SchemaRegistry,
    plan: &SnapshotBuildPlan,
    build_id: &SnapshotBuildId,
    artifact_id: &ArtifactId,
    barrier_timeout: Duration,
    safety: SnapshotBuildSafetyPolicy,
) -> SnapshotBuildSupervisor {
    test_supervisor_with_retention(
        workspace,
        database_url,
        registry,
        plan,
        build_id,
        artifact_id,
        barrier_timeout,
        safety,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
}

#[allow(clippy::too_many_arguments)]
fn test_supervisor_with_retention(
    workspace: &Path,
    database_url: &str,
    registry: &SchemaRegistry,
    plan: &SnapshotBuildPlan,
    build_id: &SnapshotBuildId,
    artifact_id: &ArtifactId,
    barrier_timeout: Duration,
    safety: SnapshotBuildSafetyPolicy,
    retention_ttl: Duration,
    retention_renewal_interval: Duration,
) -> SnapshotBuildSupervisor {
    test_supervisor_with_limits(
        workspace,
        database_url,
        registry,
        plan,
        build_id,
        artifact_id,
        barrier_timeout,
        safety,
        retention_ttl,
        retention_renewal_interval,
        snapshot_builder::BoundedCopyLimits {
            table: CopyLoadLimits {
                max_rows: 100,
                max_value_bytes: 1024,
                max_row_bytes: 2048,
                max_decoded_value_bytes: 4096,
                max_batch_bytes: 4096,
                batch_rows: 2,
            },
            max_workers: plan.tables().len().min(2),
            max_inflight_batch_bytes: 4096 * plan.tables().len().min(2) as u64,
        },
        Duration::from_secs(10),
    )
}

#[allow(clippy::too_many_arguments)]
fn test_supervisor_with_limits(
    workspace: &Path,
    database_url: &str,
    registry: &SchemaRegistry,
    plan: &SnapshotBuildPlan,
    build_id: &SnapshotBuildId,
    artifact_id: &ArtifactId,
    barrier_timeout: Duration,
    safety: SnapshotBuildSafetyPolicy,
    retention_ttl: Duration,
    retention_renewal_interval: Duration,
    copy_limits: snapshot_builder::BoundedCopyLimits,
    publication_cleanup_timeout: Duration,
) -> SnapshotBuildSupervisor {
    // `SnapshotBuildLeaseGuard::acquire` refuses a provider whose declared
    // operation deadline exceeds `ttl / 3`. The fixture providers declare the
    // file log's deadline scaled by `CALQUE_TEST_TIME_SCALE`, so a TTL under
    // three times that rises to it, or the guard would fail the build as
    // `InvalidTiming` before any test assertion runs. At scale 1 that is every
    // TTL under 9 s.
    let retention_ttl = retention_ttl.max(scaled_file_log_retention_deadline() * 3);
    let authorization = plan.authorization_identity();
    SnapshotBuildSupervisor::new(SnapshotBuildJobConfig {
        build_id: build_id.clone(),
        owner_id: SnapshotBuildOwnerId::new("pipeline-supervisor-owner").unwrap(),
        journal_root: workspace.join("build-status"),
        artifact_path: workspace
            .join("private-builds")
            .join(build_id.as_str())
            .join(SNAPSHOT_ARTIFACT_FILE_NAME),
        encoder_limits: EncoderLimits::default(),
        replication_connection: replication_connection_string(database_url),
        stream: SnapshotStreamConfig::new(
            format!("snapshot_build_{}", Uuid::new_v4().simple()),
            PUBLICATION,
        )
        .unwrap(),
        retention_ttl,
        retention_renewal_interval,
        copy_limits,
        roll_forward_timeout: Duration::from_secs(15),
        decoder_limits: DecoderLimits::default(),
        barrier_wait: BarrierWaitRequest {
            timeout: barrier_timeout,
            poll_interval: Duration::from_millis(20),
            max_records: 100,
            max_bytes: 1024 * 1024,
        },
        safety,
        registry_revalidation: database_url.parse().unwrap(),
        publication: SnapshotPublicationRequest {
            artifact: DirectArtifactRequest {
                artifact_id: artifact_id.clone(),
                created_at: "2026-07-14T12:00:00Z".to_owned(),
                compatibility: ArtifactCompatibility {
                    publication_generation: PUBLICATION_GENERATION_V1,
                    schema_generation: registry.schema_generation(),
                    authorization_generation: authorization.generation(),
                    authorization_policy_hash: authorization.policy_hash().to_string(),
                    authorization_scope_hash: authorization.scope_hash().to_string(),
                    table_set_hash: plan.table_set_hash().to_owned(),
                    shard_profile_hash: SOURCE_WIDE_SHARD_PROFILE_HASH.to_owned(),
                    index_profile_hash: NO_INDEX_PROFILE_HASH.to_owned(),
                    type_codec_generation: TYPE_CODEC_GENERATION_V1,
                    collation_generation: COLLATION_GENERATION_V1,
                },
            },
            growth_reserve_bytes: 1024 * 1024,
            lease_owner: "pipeline-supervisor-owner".to_owned(),
            lease_duration_ms: 30_000,
            catalog_lease_renewal_interval: Duration::from_secs(5),
            publication_cleanup_timeout,
            max_encoded_representation_bytes: 16 * 1024 * 1024,
            admission: None,
        },
    })
    .unwrap()
}

async fn mirror_cut_to_file_log<P: FileLogAccess + Send + 'static>(
    database_url: String,
    slot_name: String,
    provider: Arc<Mutex<P>>,
    signal: CanonicalHeadSignal,
    source_epoch: SourceEpoch,
) {
    mirror_cut_to_file_log_controlled(
        database_url,
        slot_name,
        provider,
        signal,
        source_epoch,
        false,
    )
    .await;
}

async fn mirror_cut_to_file_log_controlled<P: FileLogAccess + Send + 'static>(
    database_url: String,
    slot_name: String,
    provider: Arc<Mutex<P>>,
    signal: CanonicalHeadSignal,
    source_epoch: SourceEpoch,
    force_retention_while_build_lease_is_active: bool,
) {
    let client = connect(&database_url).await;
    let mut decoder = Decoder::for_launch(
        source_control::PgoutputLaunchProfile::V1,
        DecoderLimits::default(),
    )
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "persistent SQL decoder timed out waiting for the cut marker"
        );
        let rows = client
            .query(
                "SELECT lsn, data
                   FROM pg_catalog.pg_logical_slot_get_binary_changes(
                     $1, NULL, 128,
                     'proto_version', '1',
                     'publication_names', $2,
                     'messages', 'true'
                   )",
                &[&slot_name, &PUBLICATION],
            )
            .await
            .unwrap();
        for row in rows {
            let lsn: tokio_postgres::types::PgLsn = row.get(0);
            let data: Vec<u8> = row.get(1);
            for event in decoder
                .decode(pgoutput_decoder::PgLsn::new(u64::from(lsn)), &data)
                .unwrap()
            {
                let DecoderEvent::TransactionCommitted(transaction) = event else {
                    continue;
                };
                let Some(message) = transaction.changes.iter().find_map(|change| match change {
                    DecodedChange::LogicalMessage(message)
                        if message.kind == LogicalMessageKind::CutMarker =>
                    {
                        Some(message)
                    }
                    _ => None,
                }) else {
                    continue;
                };
                let identity: CutIdentity = serde_json::from_slice(&message.content).unwrap();
                {
                    let mut provider = provider.lock().unwrap();
                    let log = provider.file_log();
                    log.append_barrier(CanonicalBarrier {
                        key: TransactionKey {
                            source_epoch: source_epoch.clone(),
                            end_lsn: tokio_postgres::types::PgLsn::from(transaction.end_lsn.get())
                                .to_string(),
                            postgres_xid: transaction.xid,
                        },
                        build_id: identity.build_id,
                        nonce: identity.nonce,
                        message_lsn: tokio_postgres::types::PgLsn::from(message.message_lsn.get())
                            .to_string(),
                        commit_lsn: tokio_postgres::types::PgLsn::from(
                            transaction.commit_lsn.get(),
                        )
                        .to_string(),
                        commit_time_unix_micros: 1,
                    })
                    .unwrap();
                    if force_retention_while_build_lease_is_active {
                        log.retain_from(u64::MAX).unwrap();
                    }
                }
                signal.notify_head_advanced();
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn source_wide_scope(registry: &SchemaRegistry) -> authz::AuthorizedScope {
    let policy = AuthorizationPolicy::compile_for_registry(
        PolicyDefinition {
            generation: 0,
            tables: registry
                .tables()
                .iter()
                .map(|table| TablePermitDefinition {
                    table_id: table.table_id.clone(),
                    permitted_columns: table.column_ids.clone(),
                })
                .collect(),
        },
        registry,
    )
    .expect("compile source-wide authorization policy");
    policy.scope()
}

fn setup_fixture(database_url: &str) -> SchemaRegistry {
    let mut setup = BlockingClient::connect(database_url, BlockingNoTls).unwrap();
    setup
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {PUBLICATION};
             DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, value text NOT NULL);
             INSERT INTO public.{TABLE} VALUES (1, 'at-s');
             CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};"
        ))
        .unwrap();
    let tables = BTreeSet::from([QualifiedTable {
        schema: "public".to_owned(),
        table: TABLE.to_owned(),
    }]);
    let snapshot = inspect_source(&mut setup, PUBLICATION, &tables).unwrap();
    SchemaRegistry::initialize(
        &snapshot,
        &EnrollmentRequirements {
            expected_identity: snapshot.identity.clone(),
            publication_name: PUBLICATION.to_owned(),
            tables,
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        },
    )
    .unwrap()
}

fn setup_parallel_fixture(database_url: &str) -> SchemaRegistry {
    let mut setup = BlockingClient::connect(database_url, BlockingNoTls).unwrap();
    setup
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {PUBLICATION};
             DROP TABLE IF EXISTS public.{TABLE};
             DROP TABLE IF EXISTS public.{SECOND_TABLE};
             DROP TABLE IF EXISTS public.{THIRD_TABLE};
             DROP TABLE IF EXISTS public.{FOURTH_TABLE};
             CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, value text NOT NULL);
             CREATE TABLE public.{SECOND_TABLE} (id bigint PRIMARY KEY, value text NOT NULL);
             CREATE TABLE public.{THIRD_TABLE} (id bigint PRIMARY KEY, value text NOT NULL);
             CREATE TABLE public.{FOURTH_TABLE} (id bigint PRIMARY KEY, value text NOT NULL);
             INSERT INTO public.{TABLE} VALUES (1, 'left-at-s');
             INSERT INTO public.{SECOND_TABLE} VALUES (1, 'right-at-s');
             INSERT INTO public.{THIRD_TABLE} VALUES (1, 'third-at-s');
             INSERT INTO public.{FOURTH_TABLE} VALUES (1, 'fourth-at-s');
             CREATE PUBLICATION {PUBLICATION}
                 FOR TABLE public.{TABLE}, public.{SECOND_TABLE},
                           public.{THIRD_TABLE}, public.{FOURTH_TABLE};"
        ))
        .unwrap();
    let tables = BTreeSet::from([
        QualifiedTable {
            schema: "public".to_owned(),
            table: TABLE.to_owned(),
        },
        QualifiedTable {
            schema: "public".to_owned(),
            table: SECOND_TABLE.to_owned(),
        },
        QualifiedTable {
            schema: "public".to_owned(),
            table: THIRD_TABLE.to_owned(),
        },
        QualifiedTable {
            schema: "public".to_owned(),
            table: FOURTH_TABLE.to_owned(),
        },
    ]);
    let snapshot = inspect_source(&mut setup, PUBLICATION, &tables).unwrap();
    SchemaRegistry::initialize(
        &snapshot,
        &EnrollmentRequirements {
            expected_identity: snapshot.identity.clone(),
            publication_name: PUBLICATION.to_owned(),
            tables,
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        },
    )
    .unwrap()
}

/// Same shape as `setup_large_copy_fixture`, with each row wide enough that
/// the COPY cannot finish server-side while the test is still watching it.
///
/// `supervisor_mid_copy_cancellation_rolls_back_and_joins_every_backend` has
/// to cancel while the backend is inside the COPY. The narrow fixture's output
/// fits in the buffering between the backend and the client, so its COPY
/// finishes server-side early. 4 KiB rows make 78 MB, far past that buffering,
/// so the backend blocks writing to the client until the consumer drains it.
/// `repeat('x', 4096)` compresses on disk and COPY detoasts it on the way out,
/// so the rows cost almost nothing to make or store.
///
/// The other `setup_large_copy_fixture` callers hold the whole table in
/// memory, so this is a separate fixture. Both drop and recreate the same
/// table under `FIXTURE_GUARD`.
fn setup_wide_copy_fixture(database_url: &str) -> SchemaRegistry {
    let mut setup = BlockingClient::connect(database_url, BlockingNoTls).unwrap();
    setup
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {PUBLICATION};
             DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, value text NOT NULL);
             INSERT INTO public.{TABLE}
             SELECT id, repeat('x', 4096) || id::text
               FROM generate_series(1, 20000) AS id;
             CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};"
        ))
        .unwrap();
    let tables = BTreeSet::from([QualifiedTable {
        schema: "public".to_owned(),
        table: TABLE.to_owned(),
    }]);
    let snapshot = inspect_source(&mut setup, PUBLICATION, &tables).unwrap();
    SchemaRegistry::initialize(
        &snapshot,
        &EnrollmentRequirements {
            expected_identity: snapshot.identity.clone(),
            publication_name: PUBLICATION.to_owned(),
            tables,
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        },
    )
    .unwrap()
}

fn setup_large_copy_fixture(database_url: &str) -> SchemaRegistry {
    let mut setup = BlockingClient::connect(database_url, BlockingNoTls).unwrap();
    setup
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {PUBLICATION};
             DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, value text NOT NULL);
             INSERT INTO public.{TABLE}
             SELECT id, repeat('x', 256) || id::text
               FROM generate_series(1, 20000) AS id;
             CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};"
        ))
        .unwrap();
    let tables = BTreeSet::from([QualifiedTable {
        schema: "public".to_owned(),
        table: TABLE.to_owned(),
    }]);
    let snapshot = inspect_source(&mut setup, PUBLICATION, &tables).unwrap();
    SchemaRegistry::initialize(
        &snapshot,
        &EnrollmentRequirements {
            expected_identity: snapshot.identity.clone(),
            publication_name: PUBLICATION.to_owned(),
            tables,
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        },
    )
    .unwrap()
}

async fn connect(database_url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn connect_owned(database_url: &str) -> Client {
    let mut config: tokio_postgres::Config = database_url.parse().unwrap();
    config.application_name(OWNED_APPLICATION_NAME);
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn replication_connection_string(database_url: &str) -> String {
    if database_url.contains('?') {
        format!(
            "{database_url}&replication=database&connect_timeout=5&application_name={OWNED_APPLICATION_NAME}"
        )
    } else {
        format!(
            "{database_url}?replication=database&connect_timeout=5&application_name={OWNED_APPLICATION_NAME}"
        )
    }
}
