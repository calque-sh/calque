use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, OpenOptions},
    io::Write,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
use canonical_log::{
    CanonicalEntry, CanonicalOperationKind, CellValue as CanonicalCellValue, FencedWriterIdentity,
    PostgresDatastoreIdentity, PostgresFencedCanonicalLog,
};
use mutation_api::install_metadata;
use pg_walstream::parse_lsn;
use pgoutput_decoder::DecoderLimits;
use postgres::{Client, NoTls};
use prost::Message;
use source_control::{
    inspect_source, ActivatedSource, EnrollmentRequirements, PgoutputLaunchProfile, QualifiedTable,
    SchemaRegistry,
};
use source_runtime::{
    establish_persistent_source, provision_persistent_slot, AcquireRequest, FenceAuthority,
    FenceError, FenceStore, OwnerId, PersistentIngestBoundary, PersistentIngestConfig,
    PersistentIngestObserver, PostgresFenceStore, ProvisionalLease, SlotProvisioningConfig,
};
use sync_protocol::{change::cell_value, decode_primary_key, mutation::MutationLogicalMarker};
use sync_types::{CursorCodec, OpaqueCursor, SourceEpoch, SourceId};

const CURSOR_KEY: [u8; 32] = [91; 32];
const LEASE_TTL: Duration = Duration::from_millis(750);
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy)]
enum ScenarioMode {
    Kill,
    Revoke,
    RevokeRace,
}

impl ScenarioMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Kill => "kill",
            Self::Revoke => "revoke",
            Self::RevokeRace => "revoke-race",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Scenario {
    seed: u64,
    boundary: PersistentIngestBoundary,
    mode: ScenarioMode,
}

#[derive(Debug, Clone)]
struct ScenarioEvidence {
    scenario: Scenario,
    canonical_head: u64,
    reference_rows: usize,
    confirmed_flush_lsn: String,
    last_canonical_end_lsn: String,
    trace: Vec<String>,
}

const REQUIRED_BOUNDARIES: [PersistentIngestBoundary; 22] = [
    PersistentIngestBoundary::TransactionSpilled,
    PersistentIngestBoundary::BeforeNormalize,
    PersistentIngestBoundary::AfterNormalize,
    PersistentIngestBoundary::BeforeCanonicalAppend,
    PersistentIngestBoundary::CanonicalFenceAuthorized,
    PersistentIngestBoundary::CanonicalSequenceAllocated,
    PersistentIngestBoundary::CanonicalRecordInserted,
    PersistentIngestBoundary::CanonicalHeadUpdated,
    PersistentIngestBoundary::CanonicalTransactionCommitted,
    PersistentIngestBoundary::AfterCanonicalAppend,
    PersistentIngestBoundary::BeforeReceiptMark,
    PersistentIngestBoundary::AfterReceiptMark,
    PersistentIngestBoundary::BeforeCheckpoint,
    PersistentIngestBoundary::AfterCheckpoint,
    PersistentIngestBoundary::BeforeFinalFence,
    PersistentIngestBoundary::AfterFinalFence,
    PersistentIngestBoundary::BeforeFeedbackEncode,
    PersistentIngestBoundary::AfterFeedbackEncode,
    PersistentIngestBoundary::BeforeFeedbackCopyData,
    PersistentIngestBoundary::FeedbackCopyDataQueued,
    PersistentIngestBoundary::BeforeFeedbackTransportFlush,
    PersistentIngestBoundary::FeedbackTransportFlushed,
];

const fn scenario(seed: u64, boundary: PersistentIngestBoundary, mode: ScenarioMode) -> Scenario {
    Scenario {
        seed,
        boundary,
        mode,
    }
}

const SCENARIOS: [Scenario; 29] = [
    scenario(
        0,
        PersistentIngestBoundary::TransactionSpilled,
        ScenarioMode::Kill,
    ),
    scenario(
        1,
        PersistentIngestBoundary::BeforeNormalize,
        ScenarioMode::Kill,
    ),
    scenario(
        2,
        PersistentIngestBoundary::AfterNormalize,
        ScenarioMode::Kill,
    ),
    scenario(
        3,
        PersistentIngestBoundary::BeforeCanonicalAppend,
        ScenarioMode::Revoke,
    ),
    scenario(
        4,
        PersistentIngestBoundary::CanonicalFenceAuthorized,
        ScenarioMode::Kill,
    ),
    scenario(
        5,
        PersistentIngestBoundary::CanonicalSequenceAllocated,
        ScenarioMode::Kill,
    ),
    scenario(
        6,
        PersistentIngestBoundary::CanonicalRecordInserted,
        ScenarioMode::Kill,
    ),
    scenario(
        7,
        PersistentIngestBoundary::CanonicalHeadUpdated,
        ScenarioMode::Kill,
    ),
    scenario(
        8,
        PersistentIngestBoundary::CanonicalTransactionCommitted,
        ScenarioMode::Kill,
    ),
    scenario(
        9,
        PersistentIngestBoundary::AfterCanonicalAppend,
        ScenarioMode::Kill,
    ),
    scenario(
        10,
        PersistentIngestBoundary::BeforeReceiptMark,
        ScenarioMode::Revoke,
    ),
    scenario(
        11,
        PersistentIngestBoundary::AfterReceiptMark,
        ScenarioMode::Kill,
    ),
    scenario(
        12,
        PersistentIngestBoundary::BeforeCheckpoint,
        ScenarioMode::Kill,
    ),
    scenario(
        13,
        PersistentIngestBoundary::AfterCheckpoint,
        ScenarioMode::Kill,
    ),
    scenario(
        14,
        PersistentIngestBoundary::BeforeFinalFence,
        ScenarioMode::Revoke,
    ),
    scenario(
        15,
        PersistentIngestBoundary::AfterFinalFence,
        ScenarioMode::Kill,
    ),
    scenario(
        16,
        PersistentIngestBoundary::BeforeFeedbackEncode,
        ScenarioMode::Kill,
    ),
    scenario(
        17,
        PersistentIngestBoundary::FeedbackTransportFlushed,
        ScenarioMode::Kill,
    ),
    scenario(
        18,
        PersistentIngestBoundary::BeforeCanonicalAppend,
        ScenarioMode::Kill,
    ),
    scenario(
        19,
        PersistentIngestBoundary::BeforeNormalize,
        ScenarioMode::Revoke,
    ),
    scenario(
        20,
        PersistentIngestBoundary::AfterNormalize,
        ScenarioMode::Revoke,
    ),
    scenario(
        21,
        PersistentIngestBoundary::BeforeReceiptMark,
        ScenarioMode::Kill,
    ),
    scenario(
        22,
        PersistentIngestBoundary::AfterReceiptMark,
        ScenarioMode::Revoke,
    ),
    scenario(
        23,
        PersistentIngestBoundary::BeforeFinalFence,
        ScenarioMode::Kill,
    ),
    scenario(
        24,
        PersistentIngestBoundary::BeforeFeedbackCopyData,
        ScenarioMode::RevokeRace,
    ),
    scenario(
        25,
        PersistentIngestBoundary::AfterFeedbackEncode,
        ScenarioMode::Kill,
    ),
    scenario(
        26,
        PersistentIngestBoundary::BeforeFeedbackCopyData,
        ScenarioMode::Kill,
    ),
    scenario(
        27,
        PersistentIngestBoundary::FeedbackCopyDataQueued,
        ScenarioMode::Kill,
    ),
    scenario(
        28,
        PersistentIngestBoundary::BeforeFeedbackTransportFlush,
        ScenarioMode::Kill,
    ),
];

/// The state directory a refusal of a log an earlier release wrote names.
fn state_dir() -> &'static Path {
    Path::new("/srv/calque/state")
}

fn database_url(name: &str, fallback: &str) -> String {
    env::var(name)
        .or_else(|_| env::var(fallback))
        .unwrap_or_else(|_| panic!("{name} or {fallback} must be set"))
}

fn replication_url(database_url: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}replication=database")
}

/// Process-wide ordinal that makes generated identifiers unique between tests
/// running in parallel within one binary. `SystemTime::now()` has only
/// microsecond granularity on some platforms, macOS included, so pid and clock
/// alone can collide.
fn next_id_ordinal() -> u64 {
    static ORDINAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    ORDINAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn unique_suffix() -> String {
    format!(
        "{}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        next_id_ordinal()
    )
}

/// Publish a handshake file so that a process waiting on it sees the whole
/// record or no file at all. `fs::write` creates the destination before it
/// writes, so the record is written beside it and renamed into place.
fn publish_file(path: &Path, contents: &str) {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("a handshake path has a file name");
    let staged = path.with_file_name(format!("{name}.partial"));
    fs::write(&staged, contents).unwrap();
    fs::rename(&staged, path).unwrap();
}

/// The contents of `path` if it holds a complete handshake record, `None`
/// otherwise. Every record ends with a newline, so a read without one is a
/// fragment the caller must keep waiting on.
fn complete_record(path: &Path) -> Option<String> {
    match fs::read_to_string(path) {
        Ok(contents) if contents.ends_with('\n') => Some(contents),
        _ => None,
    }
}

fn append_trace(path: &Path, event: impl AsRef<str>) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{}", event.as_ref()).unwrap();
    file.sync_all().unwrap();
}

fn read_trace(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn validate_scenario_trace(scenario: Scenario, trace: &[String]) {
    let pause_target = format!("pause-target:{}", scenario.boundary.as_str());
    let pause_index = trace
        .iter()
        .position(|event| event.ends_with(&pause_target))
        .expect("worker trace must contain the armed pause target");
    let parent_pause_index = trace
        .iter()
        .position(|event| event == &format!("parent:worker-paused:{}", scenario.boundary.as_str()))
        .expect("parent trace must observe the worker pause");
    assert!(
        pause_index < parent_pause_index,
        "worker pause target must precede parent pause observation"
    );
    if scenario.boundary.is_feedback_path() {
        let checkpoint_index = trace
            .iter()
            .position(|event| event.ends_with("boundary:AfterCheckpoint"))
            .expect("transaction feedback schedule must pass its durable checkpoint");
        assert!(
            checkpoint_index < pause_index,
            "feedback target must belong to the transaction ACK, not an earlier keepalive"
        );
    }
}

fn activated_fixture(
    client: &mut Client,
    publication: &str,
    table: &str,
    epoch: &SourceEpoch,
) -> (ActivatedSource, SchemaRegistry) {
    let tables = BTreeSet::from([QualifiedTable {
        schema: "public".to_owned(),
        table: table.to_owned(),
    }]);
    let snapshot = inspect_source(client, publication, &tables).unwrap();
    let requirements = EnrollmentRequirements {
        expected_identity: snapshot.identity.clone(),
        publication_name: publication.to_owned(),
        tables,
        pgoutput_profile: PgoutputLaunchProfile::V1,
    };
    let registry = SchemaRegistry::initialize(&snapshot, &requirements).unwrap();
    let activated = ActivatedSource::new(
        &registry,
        epoch.clone(),
        CursorCodec::new(CURSOR_KEY).unwrap(),
        PgoutputLaunchProfile::V1,
    );
    (activated, registry)
}

fn source_wide_policy(registry: &SchemaRegistry) -> AuthorizationPolicy {
    AuthorizationPolicy::compile_for_registry(
        PolicyDefinition {
            generation: 1,
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
}

fn acquire_request(source_id: &SourceId, epoch: &SourceEpoch, owner: String) -> AcquireRequest {
    AcquireRequest {
        source_id: source_id.clone(),
        source_epoch: epoch.clone(),
        owner_id: OwnerId::new(owner).unwrap(),
        ttl: LEASE_TTL,
    }
}

struct Fixture {
    admin_url: String,
    source_url: String,
    control_url: String,
    replication_url: String,
    source_id: SourceId,
    epoch: SourceEpoch,
    table: String,
    publication: String,
    slot: String,
    run_directory: PathBuf,
    report_path: PathBuf,
    postgres_version_num: u32,
}

impl Fixture {
    fn create() -> Self {
        let source_url = database_url("CALQUE_SOURCE_DATABASE_URL", "TEST_DATABASE_URL");
        let admin_url =
            env::var("CALQUE_ADMIN_DATABASE_URL").unwrap_or_else(|_| source_url.clone());
        let control_url =
            env::var("CALQUE_CONTROL_DATABASE_URL").unwrap_or_else(|_| admin_url.clone());
        let replication_url = replication_url(&source_url);
        let suffix = unique_suffix();
        let mut source = Client::connect(&source_url, NoTls).unwrap();
        let postgres_version_num = source
            .query_one("SHOW server_version_num", &[])
            .unwrap()
            .get::<_, String>(0)
            .parse::<u32>()
            .unwrap();
        assert_eq!(postgres_version_num / 10_000, 18);
        let fixture = Self {
            admin_url,
            source_url,
            control_url,
            replication_url,
            source_id: SourceId::new(format!("takeover-source-{suffix}")).unwrap(),
            epoch: SourceEpoch::new("epoch-1").unwrap(),
            table: format!("rm_takeover_table_{suffix}"),
            publication: format!("rm_takeover_pub_{suffix}"),
            slot: format!("rm_takeover_slot_{suffix}"),
            run_directory: PathBuf::from("target/takeover-matrix").join(&suffix),
            report_path: PathBuf::from("target/takeover-matrix/reports")
                .join(format!("{suffix}.json")),
            postgres_version_num,
        };
        fs::create_dir_all(&fixture.run_directory).unwrap();

        let mut admin = Client::connect(&fixture.admin_url, NoTls).unwrap();
        install_metadata(&mut admin).unwrap();
        let source_role: String = source.query_one("SELECT current_user", &[]).unwrap().get(0);
        let quoted_source_role: String = admin
            .query_one("SELECT quote_ident($1)", &[&source_role])
            .unwrap()
            .get(0);
        admin
            .batch_execute(&format!(
                "GRANT USAGE ON SCHEMA calque_internal TO {quoted_source_role};
                 GRANT UPDATE ON calque_internal.mutation_receipts TO {quoted_source_role};"
            ))
            .unwrap();
        admin
            .batch_execute(&format!(
                "CREATE TABLE public.{} (id bigint PRIMARY KEY, value text NOT NULL);
                 CREATE PUBLICATION {} FOR TABLE public.{};",
                fixture.table, fixture.publication, fixture.table
            ))
            .unwrap();
        let (activated, _) = activated_fixture(
            &mut source,
            &fixture.publication,
            &fixture.table,
            &fixture.epoch,
        );
        let store = PostgresFenceStore::connect(&fixture.control_url).unwrap();
        store
            .enroll_source(&fixture.source_id, &fixture.epoch)
            .unwrap();
        let authority = FenceAuthority::new(store);
        let provisional = authority
            .acquire(acquire_request(
                &fixture.source_id,
                &fixture.epoch,
                "matrix-provision".to_owned(),
            ))
            .unwrap();
        provision_persistent_slot(
            &authority,
            &provisional,
            &fixture.source_url,
            &activated,
            &SlotProvisioningConfig::new(&fixture.slot).unwrap(),
        )
        .unwrap();
        authority.release(&provisional).unwrap();
        fixture
    }

    fn path(&self, seed: u64, kind: &str) -> PathBuf {
        self.run_directory.join(format!("{seed}-{kind}"))
    }

    fn child_command(&self, scenario: Scenario) -> ChildGuard {
        let ready = self.path(scenario.seed, "ready");
        let paused = self.path(scenario.seed, "paused");
        let release = self.path(scenario.seed, "release");
        let trace = self.path(scenario.seed, "trace");
        for path in [&ready, &paused, &release, &trace] {
            let _ = fs::remove_file(path);
        }
        let child = Command::new(env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "deterministic_process_takeover_matrix",
                "--nocapture",
            ])
            .env("CALQUE_TAKEOVER_ROLE", "worker")
            .env("CALQUE_TAKEOVER_SEED", scenario.seed.to_string())
            .env("CALQUE_TAKEOVER_BOUNDARY", scenario.boundary.as_str())
            .env("CALQUE_TAKEOVER_READY", &ready)
            .env("CALQUE_TAKEOVER_PAUSED", &paused)
            .env("CALQUE_TAKEOVER_RELEASE", &release)
            .env("CALQUE_TAKEOVER_TRACE", &trace)
            .env("CALQUE_SOURCE_DATABASE_URL", &self.source_url)
            .env("CALQUE_CONTROL_DATABASE_URL", &self.control_url)
            .env("CALQUE_TAKEOVER_SOURCE_ID", self.source_id.as_str())
            .env("CALQUE_TAKEOVER_EPOCH", self.epoch.as_str())
            .env("CALQUE_TAKEOVER_TABLE", &self.table)
            .env("CALQUE_TAKEOVER_PUBLICATION", &self.publication)
            .env("CALQUE_TAKEOVER_SLOT", &self.slot)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        ChildGuard(Some(child))
    }

    fn append_source_transaction(&self, scenario: Scenario) {
        let mutation_id = format!("takeover-mutation-{}", scenario.seed);
        let mut server_transaction_id = [0xA5_u8; 16];
        server_transaction_id[..8].copy_from_slice(&scenario.seed.to_be_bytes());
        let marker = MutationLogicalMarker {
            protocol_version: 1,
            mutation_ids: vec![mutation_id.clone()],
            server_transaction_id: server_transaction_id.to_vec(),
        }
        .encode_to_vec();
        let mut admin = Client::connect(&self.admin_url, NoTls).unwrap();
        admin
            .execute(
                "INSERT INTO calque_internal.mutation_receipts (
                     mutation_id, source_id, source_epoch, authorization_generation,
                     authorization_policy_hash, authorization_scope_hash,
                     command_name, command_version, canonical_input_sha256,
                     server_transaction_id, device_id, client_instance_id
                 ) VALUES ($1, $2, $3, 0, $4, $5, 'matrix.insert', 1, $6, $7, 'device', 'client')",
                &[
                    &mutation_id,
                    &self.source_id.as_str(),
                    &self.epoch.as_str(),
                    &&[13_u8; 32][..],
                    &&[17_u8; 32][..],
                    &&[29_u8; 32][..],
                    &&server_transaction_id[..],
                ],
            )
            .unwrap();
        let mut transaction = admin.transaction().unwrap();
        let first_id = i64::try_from(scenario.seed).unwrap() * 2 + 1;
        transaction
            .execute(
                &format!(
                    "INSERT INTO public.{} (id, value) VALUES ($1, $2), ($3, $4)",
                    self.table
                ),
                &[
                    &first_id,
                    &format!("value-{}-a", scenario.seed),
                    &(first_id + 1),
                    &format!("value-{}-b", scenario.seed),
                ],
            )
            .unwrap();
        transaction
            .query_one(
                "SELECT pg_logical_emit_message(true, $1, $2::bytea)",
                &[&"sync.mutation.v1", &marker],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    fn acquire_takeover(
        &self,
        scenario: Scenario,
    ) -> (Arc<FenceAuthority<PostgresFenceStore>>, ProvisionalLease) {
        let authority = Arc::new(FenceAuthority::new(
            PostgresFenceStore::connect(&self.control_url).unwrap(),
        ));
        self.acquire_with_authority(scenario, authority)
    }

    fn acquire_with_authority(
        &self,
        scenario: Scenario,
        authority: Arc<FenceAuthority<PostgresFenceStore>>,
    ) -> (Arc<FenceAuthority<PostgresFenceStore>>, ProvisionalLease) {
        let trace = self.path(scenario.seed, "trace");
        append_trace(&trace, "parent:takeover-acquire-started");
        let deadline = Instant::now() + WAIT_TIMEOUT;
        let provisional = loop {
            match authority.acquire(acquire_request(
                &self.source_id,
                &self.epoch,
                format!("matrix-new-{}", scenario.seed),
            )) {
                Ok(lease) => break lease,
                Err(FenceError::LeaseHeld { .. }) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                result => panic!(
                    "seed={} boundary={} takeover failed: {result:?}",
                    scenario.seed, scenario.boundary
                ),
            }
        };
        append_trace(
            &trace,
            format!(
                "parent:takeover-acquired:token:{}",
                provisional.identity().token().get()
            ),
        );
        (authority, provisional)
    }

    fn race_takeover(
        &self,
        scenario: Scenario,
        authority: Arc<FenceAuthority<PostgresFenceStore>>,
        control_backend_pid: i32,
    ) -> RacingTakeover {
        let source_id = self.source_id.clone();
        let epoch = self.epoch.clone();
        let trace = self.path(scenario.seed, "trace");
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let handle = thread::spawn(move || {
            append_trace(&trace, "parent:takeover-acquire-started");
            started_sender.send(control_backend_pid).unwrap();
            let deadline = Instant::now() + WAIT_TIMEOUT;
            let provisional = loop {
                match authority.acquire(acquire_request(
                    &source_id,
                    &epoch,
                    format!("matrix-new-{}", scenario.seed),
                )) {
                    Ok(lease) => break lease,
                    Err(FenceError::LeaseHeld { .. }) if Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    result => panic!(
                        "seed={} boundary={} racing takeover failed: {result:?}",
                        scenario.seed, scenario.boundary
                    ),
                }
            };
            append_trace(
                &trace,
                format!(
                    "parent:takeover-acquired:token:{}",
                    provisional.identity().token().get()
                ),
            );
            result_sender.send((authority, provisional)).unwrap();
        });
        RacingTakeover {
            started: started_receiver,
            result: result_receiver,
            handle: Some(handle),
            control_url: self.control_url.clone(),
            control_backend_pid,
        }
    }

    fn recover_and_verify(&self, scenario: Scenario, expected_records: usize) -> ScenarioEvidence {
        let (authority, provisional) = self.acquire_takeover(scenario);
        self.recover_with_provisional(scenario, expected_records, authority, provisional)
    }

    fn recover_with_provisional(
        &self,
        scenario: Scenario,
        expected_records: usize,
        authority: Arc<FenceAuthority<PostgresFenceStore>>,
        provisional: ProvisionalLease,
    ) -> ScenarioEvidence {
        let trace = self.path(scenario.seed, "trace");
        append_trace(&trace, "parent:new-owner-establish-started");
        let mut source = Client::connect(&self.source_url, NoTls).unwrap();
        let (activated, registry) =
            activated_fixture(&mut source, &self.publication, &self.table, &self.epoch);
        let authorization_policy = source_wide_policy(&registry);
        // Registry inspection runs outside the lease-owned runtime, so the
        // provisional lease is refreshed after it; otherwise scheduler
        // contention could use up the establishment window before the watchdog
        // renews.
        authority.renew(&provisional, LEASE_TTL).unwrap();
        let mut session = establish_persistent_source(
            Arc::clone(&authority),
            provisional,
            &self.source_url,
            &self.replication_url,
            &activated,
        )
        .unwrap();
        let writer_token = session.lease().identity().token().get();
        let stats = session
            .ingest_bounded(
                &registry,
                &authorization_policy,
                PersistentIngestConfig::new(state_dir())
                    .with_bounds(1, Duration::from_secs(2))
                    .unwrap(),
            )
            .unwrap();
        assert!(
            stats.committed_transactions <= 1,
            "one schedule may expose at most one source transaction"
        );
        session.shutdown().unwrap();
        append_trace(&trace, "parent:new-owner-session-shutdown");

        let mut control = Client::connect(&self.control_url, NoTls).unwrap();
        let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
        let reader = PostgresFencedCanonicalLog::connect(
            &self.control_url,
            FencedWriterIdentity::new(
                self.source_id.clone(),
                self.epoch.clone(),
                writer_token,
                datastore,
            )
            .unwrap(),
            state_dir(),
        )
        .unwrap();
        let records = reader
            .read_after_bounded(0, None, expected_records + 1, 16 * 1024 * 1024)
            .unwrap();
        assert_eq!(records.len(), expected_records);
        assert_eq!(reader.head().unwrap().sequence, expected_records as u64);

        let source_rows: BTreeMap<i64, String> = source
            .query(
                &format!("SELECT id, value FROM public.{} ORDER BY id", self.table),
                &[],
            )
            .unwrap()
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let mut canonical_rows = BTreeMap::new();
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record.position.sequence, index as u64 + 1);
            let CanonicalEntry::Transaction(transaction) = &record.entry else {
                panic!("takeover matrix expected only canonical transactions");
            };
            assert_eq!(transaction.operations.len(), 2);
            assert_eq!(transaction.mutation_ids.len(), 1);
            for operation in &transaction.operations {
                let key = decode_primary_key(&operation.primary_key).unwrap();
                let id = match key.parts[0]
                    .value
                    .as_ref()
                    .and_then(|value| value.kind.as_ref())
                {
                    Some(cell_value::Kind::Int64Value(value)) => *value,
                    other => panic!("unexpected canonical key: {other:?}"),
                };
                let CanonicalOperationKind::InsertFullRow { columns } = &operation.kind else {
                    panic!("takeover matrix expected inserts");
                };
                let value = columns
                    .iter()
                    .find_map(|column| match &column.value {
                        CanonicalCellValue::Text(value) => Some(value.clone()),
                        _ => None,
                    })
                    .expect("insert contains text payload");
                assert!(canonical_rows.insert(id, value).is_none());
            }
            let observed_cursor: Vec<u8> = source
                .query_one(
                    "SELECT observed_canonical_cursor
                       FROM calque_internal.mutation_receipts
                      WHERE source_id = $1 AND mutation_id = $2",
                    &[
                        &self.source_id.as_str(),
                        &transaction.mutation_ids[0].as_str(),
                    ],
                )
                .unwrap()
                .get(0);
            let observed_cursor =
                OpaqueCursor::parse(std::str::from_utf8(&observed_cursor).unwrap().to_owned())
                    .unwrap();
            assert_eq!(
                CursorCodec::new(CURSOR_KEY)
                    .unwrap()
                    .decode_for_epoch(&observed_cursor, &self.epoch)
                    .unwrap(),
                record.position.clone()
            );
        }
        assert_eq!(canonical_rows, source_rows);

        let confirmed_lsn: String = source
            .query_one(
                "SELECT confirmed_flush_lsn::text
                   FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .unwrap()
            .get(0);
        let latest_end_lsn = records
            .last()
            .map(|record| record.entry.key().end_lsn.clone())
            .unwrap();
        assert_eq!(
            parse_lsn(&confirmed_lsn).unwrap(),
            parse_lsn(&latest_end_lsn).unwrap(),
            "PostgreSQL feedback must expose exactly the durable canonical transaction end"
        );
        ScenarioEvidence {
            scenario,
            canonical_head: reader.head().unwrap().sequence,
            reference_rows: source_rows.len(),
            confirmed_flush_lsn: confirmed_lsn,
            last_canonical_end_lsn: latest_end_lsn,
            trace: read_trace(&trace),
        }
    }

    fn verify_successful_cleanup(&self) {
        let mut source = Client::connect(&self.source_url, NoTls).unwrap();
        let active_pid: Option<i32> = source
            .query_one(
                "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .unwrap()
            .get(0);
        assert!(
            active_pid.is_none(),
            "matrix slot remained active at cleanup"
        );
        let dropped = source
            .query(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name)
                   FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .unwrap();
        assert_eq!(dropped.len(), 1, "matrix slot was missing at cleanup");
        let mut admin = Client::connect(&self.admin_url, NoTls).unwrap();
        admin
            .execute(
                "DELETE FROM calque_internal.mutation_receipts WHERE source_id = $1",
                &[&self.source_id.as_str()],
            )
            .unwrap();
        admin
            .batch_execute(&format!(
                "DROP PUBLICATION {};
                 DROP TABLE public.{};",
                self.publication, self.table
            ))
            .unwrap();

        let mut control = Client::connect(&self.control_url, NoTls).unwrap();
        for table in [
            "canonical_log_record",
            "canonical_log_head",
            "canonical_log_writer_fence",
            "source_slot_enrollment",
            "source_fence_lease",
        ] {
            control
                .execute(
                    &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                    &[&self.source_id.as_str()],
                )
                .unwrap();
            let remaining: i64 = control
                .query_one(
                    &format!("SELECT count(*) FROM calque_control.{table} WHERE source_id = $1"),
                    &[&self.source_id.as_str()],
                )
                .unwrap()
                .get(0);
            assert_eq!(remaining, 0, "matrix cleanup left {table} state");
        }
        fs::remove_dir_all(&self.run_directory).unwrap();
        assert!(!self.run_directory.exists());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Ok(mut admin) = Client::connect(&self.admin_url, NoTls) {
            let _ = admin.execute(
                "SELECT pg_terminate_backend(active_pid)
                   FROM pg_catalog.pg_replication_slots
                  WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&self.slot],
            );
            let _ = admin.execute(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name)
                   FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            );
            let _ = admin.execute(
                "DELETE FROM calque_internal.mutation_receipts WHERE source_id = $1",
                &[&self.source_id.as_str()],
            );
            let _ = admin.batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {};
                 DROP TABLE IF EXISTS public.{};",
                self.publication, self.table
            ));
        }
        if let Ok(mut control) = Client::connect(&self.control_url, NoTls) {
            for table in [
                "canonical_log_record",
                "canonical_log_head",
                "canonical_log_writer_fence",
                "source_slot_enrollment",
                "source_fence_lease",
            ] {
                let _ = control.execute(
                    &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                    &[&self.source_id.as_str()],
                );
            }
        }
        let _ = fs::remove_dir_all(&self.run_directory);
    }
}

struct ChildGuard(Option<Child>);

struct RacingTakeover {
    started: mpsc::Receiver<i32>,
    result: mpsc::Receiver<(Arc<FenceAuthority<PostgresFenceStore>>, ProvisionalLease)>,
    handle: Option<thread::JoinHandle<()>>,
    control_url: String,
    control_backend_pid: i32,
}

impl RacingTakeover {
    fn wait_started(&self) -> i32 {
        self.started.recv_timeout(WAIT_TIMEOUT).unwrap()
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(Arc<FenceAuthority<PostgresFenceStore>>, ProvisionalLease), mpsc::RecvTimeoutError>
    {
        self.result.recv_timeout(timeout)
    }

    fn join(&mut self) {
        self.handle.take().unwrap().join().unwrap();
    }
}

impl Drop for RacingTakeover {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        if let Ok(mut control) = Client::connect(&self.control_url, NoTls) {
            let _ = control.query_one(
                "SELECT pg_terminate_backend($1)",
                &[&self.control_backend_pid],
            );
        }
        let _ = handle.join();
    }
}

impl ChildGuard {
    fn id(&self) -> u32 {
        self.0.as_ref().unwrap().id()
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.0.as_mut().unwrap().try_wait().unwrap()
    }

    fn kill_and_wait(&mut self) {
        let child = self.0.as_mut().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        self.0 = None;
    }

    fn wait_success(&mut self) {
        let status = self.0.as_mut().unwrap().wait().unwrap();
        self.0 = None;
        assert!(status.success(), "stale worker exited with {status}");
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Debug)]
struct FilePauseObserver {
    target: PersistentIngestBoundary,
    transaction_feedback_armed: AtomicBool,
    paused: PathBuf,
    release: PathBuf,
    trace: PathBuf,
}

impl PersistentIngestObserver for FilePauseObserver {
    fn observe(&self, boundary: PersistentIngestBoundary) {
        append_trace(
            &self.trace,
            format!("worker:{}:boundary:{boundary:?}", std::process::id()),
        );
        if boundary == PersistentIngestBoundary::AfterCheckpoint {
            self.transaction_feedback_armed
                .store(true, Ordering::Release);
        }
        if boundary.is_feedback_path() && !self.transaction_feedback_armed.load(Ordering::Acquire) {
            return;
        }
        if boundary != self.target {
            return;
        }
        append_trace(
            &self.trace,
            format!(
                "worker:{}:pause-target:{}",
                std::process::id(),
                boundary.as_str()
            ),
        );
        publish_file(&self.paused, &format!("{}\n", std::process::id()));
        while !self.release.exists() {
            thread::sleep(Duration::from_millis(5));
        }
    }
}

fn run_worker() {
    let source_url = env::var("CALQUE_SOURCE_DATABASE_URL").unwrap();
    let control_url = env::var("CALQUE_CONTROL_DATABASE_URL").unwrap();
    let source_id = SourceId::new(env::var("CALQUE_TAKEOVER_SOURCE_ID").unwrap()).unwrap();
    let epoch = SourceEpoch::new(env::var("CALQUE_TAKEOVER_EPOCH").unwrap()).unwrap();
    let table = env::var("CALQUE_TAKEOVER_TABLE").unwrap();
    let publication = env::var("CALQUE_TAKEOVER_PUBLICATION").unwrap();
    let ready = PathBuf::from(env::var("CALQUE_TAKEOVER_READY").unwrap());
    let paused = PathBuf::from(env::var("CALQUE_TAKEOVER_PAUSED").unwrap());
    let release = PathBuf::from(env::var("CALQUE_TAKEOVER_RELEASE").unwrap());
    let trace = PathBuf::from(env::var("CALQUE_TAKEOVER_TRACE").unwrap());
    let seed: u64 = env::var("CALQUE_TAKEOVER_SEED").unwrap().parse().unwrap();
    let boundary = PersistentIngestBoundary::parse(&env::var("CALQUE_TAKEOVER_BOUNDARY").unwrap())
        .expect("takeover boundary must be a stable persistent-ingest boundary");
    let store = PostgresFenceStore::connect(&control_url).unwrap();
    let control_pid = store.control_backend_pid().unwrap();
    let authority = Arc::new(FenceAuthority::new(store));
    let provisional = authority
        .acquire(acquire_request(
            &source_id,
            &epoch,
            format!("matrix-old-{seed}"),
        ))
        .unwrap();
    let mut source = Client::connect(&source_url, NoTls).unwrap();
    let (activated, registry) = activated_fixture(&mut source, &publication, &table, &epoch);
    let authorization_policy = source_wide_policy(&registry);
    let mut session = establish_persistent_source(
        authority,
        provisional,
        &source_url,
        &replication_url(&source_url),
        &activated,
    )
    .unwrap();
    publish_file(
        &ready,
        &format!("{control_pid} {}\n", session.backend_pid()),
    );
    append_trace(
        &trace,
        format!(
            "worker:{}:ready:control-pid:{control_pid}:source-pid:{}",
            std::process::id(),
            session.backend_pid()
        ),
    );
    let observer = Arc::new(FilePauseObserver {
        target: boundary,
        transaction_feedback_armed: AtomicBool::new(false),
        paused,
        release,
        trace: trace.clone(),
    });
    let limits = DecoderLimits {
        max_in_memory_transaction_bytes: 1,
        ..DecoderLimits::default()
    };
    let result = session.ingest_bounded(
        &registry,
        &authorization_policy,
        PersistentIngestConfig::new(state_dir())
            .with_bounds(1, WAIT_TIMEOUT)
            .unwrap()
            .with_decoder_limits(limits)
            .with_observer(observer),
    );
    // This fenced-out worker expects a refusal within `WAIT_TIMEOUT`. `Ok`
    // means either that it ingested a transaction or that the budget expired,
    // so the two are reported apart.
    match &result {
        Err(_) => {}
        Ok(stats) if stats.committed_transactions == 0 => panic!(
            "released stale worker was neither refused nor fed anything within {WAIT_TIMEOUT:?}: \
             Ok({stats:?})"
        ),
        Ok(stats) => panic!("released stale worker unexpectedly completed: Ok({stats:?})"),
    }
    append_trace(&trace, format!("worker:{}:stale-exit", std::process::id()));
    assert!(!session.socket_is_open());
}

fn wait_for_file(path: &Path, child: &mut ChildGuard, scenario: Scenario) -> String {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if let Some(contents) = complete_record(path) {
            return contents;
        }
        if let Some(status) = child.try_wait() {
            panic!(
                "seed={} boundary={} worker {} exited before {}: {status}",
                scenario.seed,
                scenario.boundary,
                child.id(),
                path.display()
            );
        }
        assert!(
            Instant::now() < deadline,
            "seed={} boundary={} timed out waiting for {}",
            scenario.seed,
            scenario.boundary,
            path.display()
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn wait_for_lock_wait(control_url: &str, pid: i32, scenario: Scenario) {
    let mut control = Client::connect(control_url, NoTls).unwrap();
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        let waiting: bool = control
            .query_one(
                "SELECT COALESCE((
                    SELECT wait_event_type = 'Lock'
                      FROM pg_catalog.pg_stat_activity
                     WHERE pid = $1
                ), false)",
                &[&pid],
            )
            .unwrap()
            .get(0);
        if waiting {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "seed={} boundary={} takeover backend {pid} never entered a PostgreSQL lock wait",
            scenario.seed,
            scenario.boundary
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn write_report(
    fixture: &Fixture,
    completed: &[ScenarioEvidence],
    failure: Option<Scenario>,
    finalize: bool,
) {
    let required_boundaries = REQUIRED_BOUNDARIES
        .iter()
        .map(|boundary| boundary.as_str())
        .collect::<Vec<_>>();
    let mut json = format!(
        "{{\n  \"postgresMajor\": 18,\n  \"postgresVersionNum\": {},\n  \"profile\": \"pgoutput-v1-no-streaming\",\n  \"requiredBoundaries\": {:?},\n  \"skippedSchedules\": 0,\n  \"schedules\": [\n",
        fixture.postgres_version_num, required_boundaries
    );
    for (index, evidence) in completed.iter().enumerate() {
        if index > 0 {
            json.push_str(",\n");
        }
        let scenario = evidence.scenario;
        json.push_str(&format!(
            "    {{\"seed\":{},\"boundary\":\"{}\",\"mode\":\"{}\",\"status\":\"PASS\",\"canonicalHead\":{},\"referenceRows\":{},\"confirmedFlushLsn\":\"{}\",\"lastCanonicalEndLsn\":\"{}\",\"trace\":{:?}}}",
            scenario.seed,
            scenario.boundary,
            scenario.mode.as_str(),
            evidence.canonical_head,
            evidence.reference_rows,
            evidence.confirmed_flush_lsn,
            evidence.last_canonical_end_lsn,
            evidence.trace,
        ));
    }
    if let Some(scenario) = failure {
        if !completed.is_empty() {
            json.push_str(",\n");
        }
        let trace = read_trace(&fixture.path(scenario.seed, "trace"));
        let report_stem = fixture
            .report_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap();
        let persistent_trace = fixture.report_path.with_file_name(format!(
            "{report_stem}-failure-seed-{}.trace",
            scenario.seed
        ));
        fs::create_dir_all(persistent_trace.parent().unwrap()).unwrap();
        fs::write(&persistent_trace, trace.join("\n")).unwrap();
        json.push_str(&format!(
            "    {{\"seed\":{},\"boundary\":\"{}\",\"mode\":\"{}\",\"status\":\"FAIL\",\"traceFile\":\"{}\",\"trace\":{:?}}}",
            scenario.seed,
            scenario.boundary,
            scenario.mode.as_str(),
            persistent_trace.display(),
            trace,
        ));
    }
    json.push_str("\n  ]\n}\n");
    fs::create_dir_all("target/takeover-matrix").unwrap();
    if finalize {
        fs::create_dir_all(fixture.report_path.parent().unwrap()).unwrap();
        let mut report = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&fixture.report_path)
            .unwrap();
        report.write_all(json.as_bytes()).unwrap();
        report.sync_all().unwrap();
    }
    fs::write("target/takeover-matrix/report.json", json).unwrap();
}

fn run_parent() {
    let fixture = Fixture::create();
    assert_eq!(
        SCENARIOS
            .iter()
            .map(|scenario| (scenario.boundary, scenario.mode.as_str()))
            .collect::<BTreeSet<_>>()
            .len(),
        SCENARIOS.len(),
        "takeover schedules must exercise distinct boundary/mode pairs"
    );
    assert!(
        SCENARIOS.len() >= 25,
        "the takeover matrix must keep at least 25 schedules; lower this floor on purpose, not by deleting one"
    );
    assert_eq!(
        SCENARIOS
            .iter()
            .map(|scenario| scenario.seed)
            .collect::<Vec<_>>(),
        (0..u64::try_from(SCENARIOS.len()).unwrap()).collect::<Vec<_>>(),
        "takeover seeds must be unique and contiguous"
    );
    let required = REQUIRED_BOUNDARIES.into_iter().collect::<BTreeSet<_>>();
    let covered = SCENARIOS
        .iter()
        .map(|scenario| scenario.boundary)
        .collect::<BTreeSet<_>>();
    assert_eq!(covered, required, "takeover boundary coverage drifted");
    assert!(
        REQUIRED_BOUNDARIES.iter().all(|boundary| {
            SCENARIOS.iter().any(|scenario| {
                scenario.boundary == *boundary && matches!(scenario.mode, ScenarioMode::Kill)
            })
        }),
        "every required boundary needs a process-kill schedule"
    );
    let only_seed = env::var("CALQUE_TAKEOVER_ONLY_SEED")
        .ok()
        .map(|value| value.parse::<u64>().unwrap());
    let exact_seed = env::var("CALQUE_TAKEOVER_EXACT_SEED")
        .ok()
        .map(|value| value.parse::<u64>().unwrap());
    assert!(
        only_seed.is_none() || exact_seed.is_none(),
        "prefix and exact takeover seed selectors are mutually exclusive"
    );
    let scenarios: Vec<_> = match exact_seed {
        Some(seed) => vec![*SCENARIOS
            .iter()
            .find(|scenario| scenario.seed == seed)
            .expect("requested exact takeover seed is not defined")],
        None => {
            let final_index = match only_seed {
                Some(seed) => SCENARIOS
                    .iter()
                    .position(|scenario| scenario.seed == seed)
                    .expect("requested takeover seed is not defined"),
                None => SCENARIOS.len() - 1,
            };
            SCENARIOS.into_iter().take(final_index + 1).collect()
        }
    };
    assert!(
        !scenarios.is_empty(),
        "requested takeover seed is not defined"
    );
    let mut completed = Vec::new();
    for (index, scenario) in scenarios.into_iter().enumerate() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            fixture.append_source_transaction(scenario);
            let takeover_store = PostgresFenceStore::connect(&fixture.control_url).unwrap();
            let takeover_control_pid = takeover_store.control_backend_pid().unwrap();
            let takeover_authority = Arc::new(FenceAuthority::new(takeover_store));
            let mut child = fixture.child_command(scenario);
            let trace = fixture.path(scenario.seed, "trace");
            let ready = fixture.path(scenario.seed, "ready");
            let paused = fixture.path(scenario.seed, "paused");
            let release = fixture.path(scenario.seed, "release");
            let ready = wait_for_file(&ready, &mut child, scenario);
            let mut ready_fields = ready.split_whitespace();
            let control_pid: i32 = ready_fields.next().unwrap().parse().unwrap();
            let stale_source_pid: i32 = ready_fields.next().unwrap().parse().unwrap();
            assert!(ready_fields.next().is_none());
            append_trace(
                &trace,
                format!(
                    "parent:worker-ready:control-pid:{control_pid}:source-pid:{stale_source_pid}"
                ),
            );
            wait_for_file(&paused, &mut child, scenario);
            append_trace(
                &trace,
                format!("parent:worker-paused:{}", scenario.boundary),
            );
            let mut evidence = match scenario.mode {
                ScenarioMode::Kill => {
                    append_trace(&trace, "parent:kill-worker-process");
                    child.kill_and_wait();
                    let (authority, provisional) =
                        fixture.acquire_with_authority(scenario, takeover_authority);
                    fixture.recover_with_provisional(scenario, index + 1, authority, provisional)
                }
                ScenarioMode::Revoke => {
                    append_trace(
                        &trace,
                        format!("parent:terminate-control-backend:{control_pid}"),
                    );
                    let mut control = Client::connect(&fixture.control_url, NoTls).unwrap();
                    assert!(control
                        .query_one("SELECT pg_terminate_backend($1)", &[&control_pid])
                        .unwrap()
                        .get::<_, bool>(0));
                    let (authority, provisional) =
                        fixture.acquire_with_authority(scenario, takeover_authority);
                    fixture.recover_with_provisional(scenario, index + 1, authority, provisional)
                }
                ScenarioMode::RevokeRace => {
                    let mut racing =
                        fixture.race_takeover(scenario, takeover_authority, takeover_control_pid);
                    assert_eq!(racing.wait_started(), takeover_control_pid);
                    wait_for_lock_wait(&fixture.control_url, takeover_control_pid, scenario);
                    assert!(matches!(
                        racing.recv_timeout(Duration::from_millis(500)),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    ));
                    append_trace(&trace, "parent:takeover-blocked-by-feedback-fence");
                    let observed_source_pid: Option<i32> = Client::connect(&fixture.source_url, NoTls)
                        .unwrap()
                        .query_one(
                            "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                            &[&fixture.slot],
                        )
                        .unwrap()
                        .get(0);
                    append_trace(
                        &trace,
                        format!("parent:stale-source-backend:{observed_source_pid:?}"),
                    );
                    assert_eq!(
                        observed_source_pid,
                        Some(stale_source_pid),
                        "feedback race must begin with the exact stale worker backend owning the slot"
                    );
                    append_trace(
                        &trace,
                        format!("parent:terminate-control-backend:{control_pid}"),
                    );
                    let mut control = Client::connect(&fixture.control_url, NoTls).unwrap();
                    assert!(control
                        .query_one("SELECT pg_terminate_backend($1)", &[&control_pid])
                        .unwrap()
                        .get::<_, bool>(0));
                    let (authority, provisional) = racing
                        .recv_timeout(WAIT_TIMEOUT)
                        .expect("higher-token takeover did not acquire after feedback fence ended");
                    racing.join();
                    let recovered = fixture.recover_with_provisional(
                        scenario,
                        index + 1,
                        authority,
                        provisional,
                    );
                    let active_after_recovery: Option<i32> =
                        Client::connect(&fixture.source_url, NoTls)
                            .unwrap()
                            .query_one(
                                "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                                &[&fixture.slot],
                            )
                            .unwrap()
                            .get(0);
                    assert!(
                        active_after_recovery.is_none(),
                        "recovered owner shutdown must leave the revoked stale backend absent"
                    );
                    append_trace(&trace, "parent:new-owner-recovered-before-stale-release");
                    recovered
                }
            };
            assert_eq!(evidence.canonical_head, index as u64 + 1);
            if matches!(
                scenario.mode,
                ScenarioMode::Revoke | ScenarioMode::RevokeRace
            ) {
                append_trace(&trace, "parent:release-stale-observer");
                publish_file(&release, "continue\n");
                child.wait_success();
                let after_stale_release = fixture.recover_and_verify(scenario, index + 1);
                assert_eq!(after_stale_release.canonical_head, evidence.canonical_head);
                assert_eq!(
                    after_stale_release.confirmed_flush_lsn,
                    evidence.confirmed_flush_lsn
                );
                assert_eq!(
                    after_stale_release.last_canonical_end_lsn,
                    evidence.last_canonical_end_lsn
                );
                append_trace(&trace, "parent:stale-release-did-not-advance-state");
            }
            evidence.trace = read_trace(&trace);
            validate_scenario_trace(scenario, &evidence.trace);
            evidence
        }));
        let evidence = match result {
            Ok(evidence) => evidence,
            Err(payload) => {
                write_report(&fixture, &completed, Some(scenario), true);
                eprintln!(
                    "replay with CALQUE_TAKEOVER_ONLY_SEED={} and the documented live command",
                    scenario.seed
                );
                panic::resume_unwind(payload);
            }
        };
        completed.push(evidence);
        write_report(&fixture, &completed, None, false);
    }
    fixture.verify_successful_cleanup();
    write_report(&fixture, &completed, None, true);
    eprintln!("takeover matrix report: {}", fixture.report_path.display());
}

#[test]
#[ignore = "requires live PostgreSQL 18 source/control URLs and process kill support"]
fn deterministic_process_takeover_matrix() {
    if env::var("CALQUE_TAKEOVER_ROLE").as_deref() == Ok("worker") {
        run_worker();
    } else {
        run_parent();
    }
}

/// The reader half of the handshake: a destination created and not yet
/// filled, or a record whose terminating newline has not landed, is not a
/// record.
#[test]
fn a_created_but_unwritten_handshake_file_is_not_a_record() {
    let directory = scratch_directory("unwritten-handshake");
    let ready = directory.join("7-ready");

    assert_eq!(
        complete_record(&ready),
        None,
        "a missing file is not a record"
    );

    fs::write(&ready, "").unwrap();
    assert_eq!(
        complete_record(&ready),
        None,
        "an empty destination is not a record"
    );

    fs::write(&ready, "4271 4288").unwrap();
    assert_eq!(
        complete_record(&ready),
        None,
        "a record without its terminating newline is not a record"
    );

    publish_file(&ready, "4271 4288\n");
    let contents = complete_record(&ready).expect("a published record reads back");
    let mut fields = contents.split_whitespace();
    assert_eq!(fields.next().unwrap().parse::<i32>().unwrap(), 4271);
    assert_eq!(fields.next().unwrap().parse::<i32>().unwrap(), 4288);
    assert!(fields.next().is_none());

    fs::remove_dir_all(&directory).unwrap();
}

/// Publishing leaves the destination and no staged `.partial` file.
#[test]
fn publishing_a_handshake_file_leaves_no_fragment_behind() {
    let directory = scratch_directory("published-handshake");
    let paused = directory.join("7-paused");

    publish_file(&paused, "4271\n");

    assert_eq!(fs::read_to_string(&paused).unwrap(), "4271\n");
    let entries = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, vec![std::ffi::OsString::from("7-paused")]);

    fs::remove_dir_all(&directory).unwrap();
}

/// A private directory for one of the tests above, named so that two of them
/// running as concurrent threads cannot collide.
fn scratch_directory(purpose: &str) -> PathBuf {
    let directory =
        env::temp_dir().join(format!("calque-takeover-{purpose}-{}", std::process::id()));
    if directory.exists() {
        fs::remove_dir_all(&directory).unwrap();
    }
    fs::create_dir_all(&directory).unwrap();
    directory
}
