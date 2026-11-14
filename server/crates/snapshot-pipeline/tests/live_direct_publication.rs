//! A direct-columnar build against a live PostgreSQL: it publishes exactly the
//! rows at its cut, and an interrupted build leaves nothing visible.
//!
//! The supervisor's stage observer runs right after each stage transition, so
//! it places writes exactly: at `RollingForward` the stream has started and
//! the cut marker is not yet emitted, and at `Copying` the cut is behind the
//! build and COPY has not started.

use std::{
    collections::BTreeSet,
    env, fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use artifact_catalog::{ArtifactState, SqliteArtifactCatalog};
use artifact_contract::{
    ArtifactChunks, ArtifactCompatibility, DecodedArtifactFormat, DIRECT_COLUMNAR_CHUNK_BYTES,
};
use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
use blob_store::LocalFilesystemBlobStore;
use canonical_log::{
    CanonicalBarrier, FileCanonicalLog, SnapshotBuildId, SnapshotBuildOwnerId, TransactionKey,
};
use direct_columnar::{schema_hash, Encoder, EncoderLimits, Value};
use pgoutput_decoder::{DecodedChange, Decoder, DecoderEvent, DecoderLimits, LogicalMessageKind};
use postgres::{Client as BlockingClient, NoTls as BlockingNoTls};
use sha2::{Digest, Sha256};
use snapshot_builder::{
    direct_columnar_artifact_identity, direct_columnar_workspace_schema, BoundedCopyLimits,
    CopyLoadLimits, DirectArtifactRequest, SnapshotBuildPlan, COLLATION_GENERATION_V1,
    NO_INDEX_PROFILE_HASH, PUBLICATION_GENERATION_V1, SOURCE_WIDE_SHARD_PROFILE_HASH,
    TYPE_CODEC_GENERATION_V1,
};
use snapshot_coordinator::{
    CutIdentity, SnapshotPressurePolicy, SnapshotPressureThresholds,
    SnapshotRemainingWalThresholds, SnapshotStreamConfig,
};
use snapshot_pipeline::{
    BarrierWaitRequest, CanonicalHeadSignal, SnapshotBuildConnections, SnapshotBuildDisposition,
    SnapshotBuildFailureCode, SnapshotBuildJobConfig, SnapshotBuildJobError, SnapshotBuildJournal,
    SnapshotBuildSafetyPolicy, SnapshotBuildStage, SnapshotBuildSupervisor,
    SnapshotPublicationRequest, SnapshotWatchdogConfig, SNAPSHOT_ARTIFACT_FILE_NAME,
};
use source_control::{
    inspect_source, ActivatedSource, EnrollmentRequirements, QualifiedTable, SchemaRegistry,
};
use sync_types::{ArtifactId, CursorCodec, SourceEpoch, SourceId};
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Every test here drops and recreates the same publication and table.
static FIXTURE_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const PUBLICATION: &str = "calque_direct_test";
const TABLE: &str = "calque_direct_items";
const OWNED_APPLICATION_NAME: &str = "calque_direct_owned";
/// Stored out of line and uncompressed (`STORAGE EXTERNAL`), so an update
/// that leaves it alone reaches pgoutput as an unchanged-TOAST marker.
const TOASTED_BYTES: usize = 20_000;

/// The key column's type. Every row is named by a number, and [`Keys::key`]
/// turns it into this type's key.
#[derive(Clone, Copy)]
enum Keys {
    /// `id bigint`.
    Int,
    /// `id text` under an ICU collation that orders the keys differently from
    /// their UTF-8 bytes (`a-1 < B-2 < c-3`, where bytes put `B-2` first). Row
    /// 4's key is 2,500 bytes stored out of line, so an update that leaves it
    /// alone reaches pgoutput as an unchanged-TOAST marker in the new tuple and
    /// PostgreSQL logs the old key instead.
    Text,
}

impl Keys {
    fn key(self, number: i64) -> Value {
        match self {
            Self::Int => Value::Int64(number),
            Self::Text => Value::Text(match number {
                1 => "a-1".to_owned(),
                2 => "B-2".to_owned(),
                3 => "c-3".to_owned(),
                4 => toasted_key(),
                5 => "é-5".to_owned(),
                6 => "f-6".to_owned(),
                9 => "Z-9-\u{1d11e}".to_owned(),
                20 => "t-20".to_owned(),
                30 => "x-30".to_owned(),
                _ => unreachable!("no text key for row {number}"),
            }),
        }
    }

    fn literal(self, number: i64) -> String {
        match self.key(number) {
            Value::Int64(value) => value.to_string(),
            Value::Text(value) => format!("'{value}'"),
            _ => unreachable!(),
        }
    }

    fn storage(self) -> String {
        match self {
            Self::Int => String::new(),
            Self::Text => {
                format!("ALTER TABLE public.{TABLE} ALTER COLUMN id SET STORAGE EXTERNAL;")
            }
        }
    }

    fn column(self) -> &'static str {
        match self {
            Self::Int => "id bigint PRIMARY KEY",
            Self::Text => "id text COLLATE \"en-x-icu\" PRIMARY KEY",
        }
    }
}

/// Committed after the export and before the cut marker: one transaction that
/// updates a row, deletes one, inserts one, moves one to a new key and updates
/// the row whose TOASTed body it does not touch.
fn before_cut(keys: Keys) -> String {
    let key = |number| keys.literal(number);
    format!(
        "BEGIN;
    UPDATE public.{TABLE} SET title = 'one-changed' WHERE id = {};
    DELETE FROM public.{TABLE} WHERE id = {};
    INSERT INTO public.{TABLE} VALUES ({}, 'nine', NULL);
    UPDATE public.{TABLE} SET id = {} WHERE id = {};
    UPDATE public.{TABLE} SET title = 'four-changed' WHERE id = {};
    COMMIT;",
        key(1),
        key(2),
        key(9),
        key(20),
        key(3),
        key(4)
    )
}

/// Committed after the cut: none of it may reach the artifact.
fn after_cut(keys: Keys) -> String {
    let key = |number| keys.literal(number);
    format!(
        "BEGIN;
    UPDATE public.{TABLE} SET title = 'after-cut' WHERE id = {};
    INSERT INTO public.{TABLE} VALUES ({}, 'after-cut', NULL);
    DELETE FROM public.{TABLE} WHERE id = {};
    COMMIT;",
        key(5),
        key(30),
        key(6)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn direct_build_publishes_changes_before_its_cut_and_none_after() {
    publishes_changes_before_its_cut_and_none_after(Keys::Int, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn direct_build_keys_rows_by_a_text_primary_key() {
    publishes_changes_before_its_cut_and_none_after(Keys::Text, 20).await;
}

async fn publishes_changes_before_its_cut_and_none_after(keys: Keys, ordinal: u8) {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let fixture = Fixture::new(&database_url, ordinal, keys).await;
    let barrier = fixture.mirror_cut(&database_url).await;
    let (supervisor, artifact_id) = fixture.supervisor(&database_url);
    let mut catalog = fixture.catalog();
    let store = fixture.store();

    let writes_url = database_url.clone();
    let report = supervisor
        .run_controlled(
            Arc::clone(&fixture.provider),
            &fixture.signal,
            connections(&database_url).await,
            &fixture.registry,
            &fixture.activated,
            fixture.plan(),
            &mut catalog,
            &store,
            &CancellationToken::new(),
            move |stage| match stage {
                SnapshotBuildStage::RollingForward => {
                    execute_blocking(&writes_url, before_cut(keys));
                }
                SnapshotBuildStage::Copying => execute_blocking(&writes_url, after_cut(keys)),
                _ => {}
            },
        )
        .await
        .expect("direct build publishes");
    barrier.finish().await;

    let manifest = report.manifest.manifest();
    assert_eq!(manifest.artifact_id, artifact_id);
    let DecodedArtifactFormat::DirectColumnar { chunks, .. } = &manifest.decoded.format;
    assert_eq!(
        catalog.state(&artifact_id).unwrap(),
        Some(ArtifactState::Published)
    );
    assert_eq!(manifest.tables.len(), 1);
    assert_eq!(manifest.tables[0].row_count, 6);

    // COPY reads the exported snapshot in key order under the key's own
    // collation; the overlay replaces each changed row where COPY returns it,
    // drops the deleted one, and appends the inserted one after the last
    // copied row.
    let toasted = Value::Text(toasted_body());
    let row =
        |number, title: &str, body| vec![keys.key(number), Value::Text(title.to_owned()), body];
    let expected = [
        row(1, "one-changed", Value::Null),
        row(20, "three", Value::Null),
        row(4, "four-changed", toasted),
        row(5, "five", Value::Null),
        row(6, "six", Value::Null),
        row(9, "nine", Value::Null),
    ];
    let expected_bytes = fixture.encode(manifest.pit.snapshot_cursor.as_str(), &expected);
    assert_eq!(manifest.decoded.bytes, expected_bytes.len() as u64);
    assert_eq!(
        manifest.decoded.sha256,
        hex::encode(Sha256::digest(&expected_bytes))
    );
    // The chunk digests the client verifies name the same bytes.
    assert_eq!(
        *chunks,
        ArtifactChunks {
            bytes: DIRECT_COLUMNAR_CHUNK_BYTES,
            sha256: expected_bytes
                .chunks(DIRECT_COLUMNAR_CHUNK_BYTES as usize)
                .map(|chunk| format!("sha256:{}", hex::encode(Sha256::digest(chunk))))
                .collect(),
        }
    );

    let progress = SnapshotBuildJournal::reopen(fixture.journal_root(), &fixture.build_id)
        .unwrap()
        .progress()
        .clone();
    assert_eq!(progress.stage, SnapshotBuildStage::Published);
    assert_eq!(progress.tables_completed, 1);
    fixture.assert_no_build_residue(&database_url).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn an_interrupted_direct_build_leaves_nothing_visible() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    for (ordinal, stage) in [
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
        let ordinal = u8::try_from(ordinal).unwrap() + 2;
        let fixture = Fixture::new(&database_url, ordinal, Keys::Int).await;
        let barrier = fixture.mirror_cut(&database_url).await;
        let (supervisor, artifact_id) = fixture.supervisor(&database_url);
        let mut catalog = fixture.catalog();
        let store = fixture.store();
        let cancellation = CancellationToken::new();
        let observer = cancellation.clone();

        let error = supervisor
            .run_controlled(
                Arc::clone(&fixture.provider),
                &fixture.signal,
                connections(&database_url).await,
                &fixture.registry,
                &fixture.activated,
                fixture.plan(),
                &mut catalog,
                &store,
                &cancellation,
                move |observed| {
                    if observed == stage {
                        observer.cancel();
                    }
                },
            )
            .await
            .expect_err("a build cancelled before PUBLISHED fails");
        assert!(
            matches!(
                error,
                SnapshotBuildJobError::Cancelled
                    | SnapshotBuildJobError::Builder(_)
                    | SnapshotBuildJobError::Coordinator(_)
                    | SnapshotBuildJobError::Publication(_)
            ),
            "unexpected error after cancelling at {stage:?}: {error:?}"
        );
        barrier.abandon().await;

        assert_eq!(
            catalog.state(&artifact_id).unwrap(),
            None,
            "catalog row left after cancelling at {stage:?}"
        );
        let progress = SnapshotBuildJournal::reopen(fixture.journal_root(), &fixture.build_id)
            .unwrap()
            .progress()
            .clone();
        assert_eq!(progress.stage, SnapshotBuildStage::Cancelled, "{stage:?}");
        assert_eq!(
            progress.failure_code,
            Some(SnapshotBuildFailureCode::CallerCancelled)
        );
        assert_eq!(
            progress.disposition,
            Some(SnapshotBuildDisposition::Cancelled)
        );
        assert!(progress.cleanup_completed_at_unix_millis.is_some());
        assert_eq!(
            count_files_if_present(&fixture.workspace.path().join("objects")),
            0,
            "objects left after cancelling at {stage:?}"
        );
        fixture.assert_no_build_residue(&database_url).await;
    }
}

struct Fixture {
    workspace: tempfile::TempDir,
    registry: SchemaRegistry,
    activated: ActivatedSource,
    provider: Arc<Mutex<FileCanonicalLog>>,
    signal: CanonicalHeadSignal,
    build_id: SnapshotBuildId,
    ordinal: u8,
}

impl Fixture {
    async fn new(database_url: &str, ordinal: u8, keys: Keys) -> Self {
        let setup_url = database_url.to_owned();
        let registry = tokio::task::spawn_blocking(move || setup_fixture(&setup_url, keys))
            .await
            .expect("join fixture setup");
        let source_epoch = SourceEpoch::new(format!("direct-epoch-{ordinal}")).unwrap();
        let activated = ActivatedSource::new(
            &registry,
            source_epoch.clone(),
            CursorCodec::new([90_u8.saturating_add(ordinal); 32]).unwrap(),
            source_control::PgoutputLaunchProfile::V1,
        );
        let workspace = tempfile::tempdir().unwrap();
        let provider = Arc::new(Mutex::new(
            FileCanonicalLog::open(
                workspace.path().join("canonical"),
                SourceId::new(format!("direct-source-{ordinal}")).unwrap(),
                source_epoch,
            )
            .unwrap(),
        ));
        Self {
            workspace,
            registry,
            activated,
            provider,
            signal: CanonicalHeadSignal::new(),
            build_id: SnapshotBuildId::new(format!("direct-build-{ordinal}")).unwrap(),
            ordinal,
        }
    }

    fn plan(&self) -> SnapshotBuildPlan {
        SnapshotBuildPlan::from_registry(&self.registry, &source_wide_scope(&self.registry))
            .unwrap()
    }

    fn journal_root(&self) -> std::path::PathBuf {
        self.workspace.path().join("build-status")
    }

    fn catalog(&self) -> SqliteArtifactCatalog {
        SqliteArtifactCatalog::open(self.workspace.path().join("catalog.sqlite3")).unwrap()
    }

    fn store(&self) -> LocalFilesystemBlobStore {
        LocalFilesystemBlobStore::open(self.workspace.path().join("objects"), 32 * 1024 * 1024)
            .unwrap()
    }

    /// The artifact the plan's encoder writes for `rows` at `cursor`.
    fn encode(&self, cursor: &str, rows: &[Vec<Value>]) -> Vec<u8> {
        let plan = self.plan();
        let schema = direct_columnar_workspace_schema(&plan).unwrap();
        let identity = direct_columnar_artifact_identity(
            &plan,
            self.activated.source_epoch(),
            &schema_hash(&schema).unwrap(),
            cursor,
        );
        let mut encoder = Encoder::new(identity, schema, EncoderLimits::default()).unwrap();
        for values in rows {
            encoder.push_row(TABLE, values).unwrap();
        }
        let mut bytes = Vec::new();
        encoder.finish(&mut bytes).unwrap();
        bytes
    }

    fn supervisor(&self, database_url: &str) -> (SnapshotBuildSupervisor, ArtifactId) {
        let plan = self.plan();
        let authorization = plan.authorization_identity();
        let artifact_id = ArtifactId::new(format!("direct-artifact-{}", self.ordinal)).unwrap();
        let supervisor = SnapshotBuildSupervisor::new(SnapshotBuildJobConfig {
            build_id: self.build_id.clone(),
            owner_id: SnapshotBuildOwnerId::new("direct-owner").unwrap(),
            journal_root: self.journal_root(),
            artifact_path: self
                .workspace
                .path()
                .join("private-builds")
                .join(self.build_id.as_str())
                .join(SNAPSHOT_ARTIFACT_FILE_NAME),
            encoder_limits: EncoderLimits::default(),
            replication_connection: replication_connection_string(database_url),
            stream: SnapshotStreamConfig::new(
                format!("snapshot_build_{}", Uuid::new_v4().simple()),
                PUBLICATION,
            )
            .unwrap(),
            retention_ttl: Duration::from_secs(30),
            retention_renewal_interval: Duration::from_secs(5),
            copy_limits: BoundedCopyLimits {
                table: CopyLoadLimits {
                    max_rows: 100,
                    max_value_bytes: 64 * 1024,
                    max_row_bytes: 128 * 1024,
                    max_decoded_value_bytes: 1024 * 1024,
                    max_batch_bytes: 256 * 1024,
                    batch_rows: 2,
                },
                max_workers: 1,
                max_inflight_batch_bytes: 256 * 1024,
            },
            roll_forward_timeout: Duration::from_secs(15),
            decoder_limits: DecoderLimits::default(),
            barrier_wait: BarrierWaitRequest {
                timeout: Duration::from_secs(15),
                poll_interval: Duration::from_millis(20),
                max_records: 100,
                max_bytes: 1024 * 1024,
            },
            safety: safety_policy(),
            registry_revalidation: database_url.parse().unwrap(),
            publication: SnapshotPublicationRequest {
                artifact: DirectArtifactRequest {
                    artifact_id: artifact_id.clone(),
                    created_at: "2026-09-25T12:00:00Z".to_owned(),
                    compatibility: ArtifactCompatibility {
                        publication_generation: PUBLICATION_GENERATION_V1,
                        schema_generation: self.registry.schema_generation(),
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
                lease_owner: "direct-owner".to_owned(),
                lease_duration_ms: 30_000,
                catalog_lease_renewal_interval: Duration::from_secs(5),
                publication_cleanup_timeout: Duration::from_secs(10),
                max_encoded_representation_bytes: 16 * 1024 * 1024,
                shared_build_zstd: None,
                admission: None,
            },
        })
        .unwrap();
        (supervisor, artifact_id)
    }

    /// Nothing of the build outlives it: no private file, staged object,
    /// retention lease, temporary slot or owned session.
    async fn assert_no_build_residue(&self, database_url: &str) {
        let workspace = self.workspace.path();
        assert_eq!(
            count_files_if_present(&workspace.join("private-builds")),
            0,
            "private work directory left files"
        );
        assert_eq!(
            count_files_if_present(&workspace.join("objects").join("staging")),
            0,
            "blob store left staging files"
        );
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(workspace.join("canonical").join("manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            manifest["snapshotBuildRetentionLeases"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "snapshot retention lease was not released"
        );
        assert_no_owned_postgres_residue(database_url).await;
    }

    /// Mirrors the cut marker from a persistent slot into the file log, which
    /// is what joins the build to its persistent barrier.
    async fn mirror_cut(&self, database_url: &str) -> CutMirror {
        let slot = format!("direct_persistent_{}", Uuid::new_v4().simple());
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
            Arc::clone(&self.provider),
            self.signal.clone(),
            self.activated.source_epoch().clone(),
        ));
        CutMirror {
            control,
            slot,
            task,
        }
    }
}

struct CutMirror {
    control: Client,
    slot: String,
    task: tokio::task::JoinHandle<()>,
}

impl CutMirror {
    async fn finish(self) {
        let Self {
            control,
            slot,
            task,
        } = self;
        task.await.unwrap();
        drop_slot(&control, &slot).await;
    }

    /// A build cancelled before its marker never gives the mirror a cut.
    async fn abandon(self) {
        let Self {
            control,
            slot,
            task,
        } = self;
        task.abort();
        let _ = task.await;
        drop_slot(&control, &slot).await;
    }
}

/// An aborted mirror's backend can hold the slot for a moment after its
/// client is dropped.
async fn drop_slot(control: &Client, slot: &str) {
    for _ in 0..100 {
        let active: bool = control
            .query_one(
                "SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .unwrap()
            .get(0);
        if !active {
            control
                .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
                .await
                .unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("persistent slot {slot} stayed active");
}

fn toasted_body() -> String {
    hash_chain(b"calque-direct-toast", TOASTED_BYTES)
}

/// Row 4's text key: under the B-tree's 2,704-byte entry limit, and over the
/// size at which PostgreSQL moves a `STORAGE EXTERNAL` value out of line.
fn toasted_key() -> String {
    format!("d-4-{}", hash_chain(b"calque-direct-toast-key", 2_500))
}

/// A hash chain, so a value the overlay truncated or reordered cannot still
/// match.
fn hash_chain(seed: &[u8], bytes: usize) -> String {
    let mut chain = String::with_capacity(bytes);
    let mut block = Sha256::digest(seed);
    while chain.len() < bytes {
        chain.push_str(&hex::encode(block));
        block = Sha256::digest(block);
    }
    chain.truncate(bytes);
    chain
}

fn setup_fixture(database_url: &str, keys: Keys) -> SchemaRegistry {
    let mut setup = BlockingClient::connect(database_url, BlockingNoTls).unwrap();
    let key = |number| keys.literal(number);
    // One row per statement, in key order, so COPY returns them in key order.
    setup
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {PUBLICATION};
             DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} ({}, title text NOT NULL, body text);
             ALTER TABLE public.{TABLE} ALTER COLUMN body SET STORAGE EXTERNAL;
             {}
             INSERT INTO public.{TABLE} VALUES ({}, 'one', NULL);
             INSERT INTO public.{TABLE} VALUES ({}, 'two', NULL);
             INSERT INTO public.{TABLE} VALUES ({}, 'three', NULL);",
            keys.column(),
            keys.storage(),
            key(1),
            key(2),
            key(3)
        ))
        .unwrap();
    setup
        .execute(
            &format!("INSERT INTO public.{TABLE} VALUES ({}, 'four', $1)", key(4)),
            &[&toasted_body()],
        )
        .unwrap();
    setup
        .batch_execute(&format!(
            "INSERT INTO public.{TABLE} VALUES ({}, 'five', NULL);
             INSERT INTO public.{TABLE} VALUES ({}, 'six', NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};",
            key(5),
            key(6)
        ))
        .unwrap();
    let compressed: bool = setup
        .query_one(
            &format!(
                "SELECT pg_column_size(body) < octet_length(body) FROM public.{TABLE} WHERE id = {}",
                key(4)
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(!compressed, "the body must be stored uncompressed");
    if matches!(keys, Keys::Text) {
        // By title, not key: an index-only scan on the key would read the
        // index's inline copy.
        let key_out_of_line: bool = setup
            .query_one(
                &format!(
                    "SELECT pg_column_toast_chunk_id(id) IS NOT NULL FROM public.{TABLE} WHERE title = 'four'"
                ),
                &[],
            )
            .unwrap()
            .get(0);
        assert!(
            key_out_of_line,
            "row 4's text key must be stored out of line"
        );
        let byte_order: Vec<String> = setup
            .query(
                &format!("SELECT left(id, 3) FROM public.{TABLE} ORDER BY id COLLATE \"C\""),
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            byte_order,
            ["B-2", "a-1", "c-3", "d-4", "f-6", "é-5"],
            "byte order must differ from the key's collation order"
        );
    }
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

/// Runs `sql` on its own blocking client and thread: the stage observer is
/// synchronous and runs on the async runtime.
fn execute_blocking(database_url: &str, sql: String) {
    let database_url = database_url.to_owned();
    std::thread::spawn(move || {
        BlockingClient::connect(&database_url, BlockingNoTls)
            .unwrap()
            .batch_execute(&sql)
            .unwrap();
    })
    .join()
    .expect("fixture write");
}

fn source_wide_scope(registry: &SchemaRegistry) -> authz::AuthorizedScope {
    AuthorizationPolicy::compile_for_registry(
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
    .unwrap()
    .scope()
}

fn safety_policy() -> SnapshotBuildSafetyPolicy {
    SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(60), Duration::from_secs(20)).unwrap(),
        Duration::from_millis(200),
        Duration::from_secs(2),
        SnapshotPressurePolicy::new(
            SnapshotPressureThresholds::new(512 * 1024 * 1024, 1024 * 1024 * 1024).unwrap(),
            SnapshotPressureThresholds::new(100_000_000, 200_000_000).unwrap(),
            Duration::from_secs(120),
            Duration::from_secs(300),
            SnapshotRemainingWalThresholds::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn connections(database_url: &str) -> SnapshotBuildConnections {
    SnapshotBuildConnections {
        control: connect_owned(database_url).await,
        ddl_guard: Some(connect_owned(database_url).await),
        copy_workers: vec![connect_owned(database_url).await],
        pressure: Some(connect_owned(database_url).await),
    }
}

async fn mirror_cut_to_file_log(
    database_url: String,
    slot_name: String,
    provider: Arc<Mutex<FileCanonicalLog>>,
    signal: CanonicalHeadSignal,
    source_epoch: SourceEpoch,
) {
    let client = connect(&database_url).await;
    let mut decoder = Decoder::for_launch(
        source_control::PgoutputLaunchProfile::V1,
        DecoderLimits::default(),
    )
    .unwrap();
    loop {
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
                provider
                    .lock()
                    .unwrap()
                    .append_barrier(CanonicalBarrier {
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
                signal.notify_head_advanced();
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn assert_no_owned_postgres_residue(database_url: &str) {
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
        let owned: i64 = client
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
        if owned == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the build left an owned PostgreSQL session open");
}

fn count_files_if_present(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            if path.is_dir() {
                count_files_if_present(&path)
            } else {
                1
            }
        })
        .sum()
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
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!(
        "{database_url}{separator}replication=database&connect_timeout=5&application_name={OWNED_APPLICATION_NAME}"
    )
}
