use std::{
    collections::BTreeSet,
    env,
    panic::{catch_unwind, AssertUnwindSafe},
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
use canonical_log::LogError;
use mutation_api::install_metadata;
use pg_walstream::parse_lsn;
use postgres::{Client, NoTls};
use prost::Message;
use source_control::{
    inspect_source, ActivatedSource, EnrollmentRequirements, PgoutputLaunchProfile, QualifiedTable,
    SchemaRegistry,
};
use source_runtime::{
    establish_persistent_source, provision_persistent_slot, AcquireRequest, ActiveSourceSession,
    FenceAuthority, FenceError, FenceStore, OwnerId, PersistentIngestBoundary,
    PersistentIngestConfig, PersistentIngestDisposition, PersistentIngestError,
    PersistentIngestObserver, PersistentIngestStats, PostgresFenceStore, SlotProvisioningConfig,
};
use sync_protocol::mutation::MutationLogicalMarker;
use sync_types::{CursorCodec, SourceEpoch, SourceId};

/// The state directory a refusal of a log an earlier release wrote names.
fn state_dir() -> &'static Path {
    Path::new("/srv/calque/state")
}

fn database_url() -> Option<String> {
    env::var("CALQUE_SOURCE_DATABASE_URL")
        .ok()
        .or_else(|| env::var("TEST_DATABASE_URL").ok())
}

fn control_url(source_url: &str) -> String {
    env::var("CALQUE_CONTROL_DATABASE_URL").unwrap_or_else(|_| source_url.to_owned())
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

fn acquire_request(source_id: &SourceId, epoch: &SourceEpoch, owner: &str) -> AcquireRequest {
    AcquireRequest {
        source_id: source_id.clone(),
        source_epoch: epoch.clone(),
        owner_id: OwnerId::new(owner).unwrap(),
        ttl: Duration::from_secs(2),
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
        CursorCodec::new([41_u8; 32]).unwrap(),
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

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn & 0xffff_ffff)
}

fn slot_confirmed_lsn(client: &mut Client, slot: &str) -> u64 {
    let value: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text
               FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);
    parse_lsn(&value).unwrap()
}

/// Holds the shared logical-message stream of the test database for one test.
///
/// `pgoutput` delivers transactional logical messages to every slot on the
/// database, and publication filtering does not apply to them, so a
/// `sync.mutation.v1` marker one test emits is decoded by any other session
/// reading at the time and refused as `CanonicalReceiptBindingMismatch`. The
/// lock lives in the database because the stream does; PostgreSQL releases it
/// when this connection closes, including in a panicking test.
///
/// It covers only this binary. `takeover_matrix` emits markers without it,
/// which is safe only while cargo runs test binaries one at a time against a
/// given `TEST_DATABASE_URL`.
struct LogicalStreamLock {
    /// The lock is the connection: PostgreSQL drops a session-level advisory
    /// lock when its session ends, so this field is never read, only held.
    _connection: Client,
}

impl LogicalStreamLock {
    /// Spells `calq_wal` in ASCII. Advisory keys share one namespace per
    /// database, so it only has to be unused elsewhere.
    const KEY: i64 = 0x6361_6c71_5f77_616c;

    fn acquire(database_url: &str) -> Self {
        let mut client = Client::connect(database_url, NoTls).unwrap();
        // A wedged holder must report itself rather than hang the suite until
        // the job times out with nothing to read.
        client.batch_execute("SET lock_timeout = '120s'").unwrap();
        client
            .query_one("SELECT pg_catalog.pg_advisory_lock($1)", &[&Self::KEY])
            .expect("another live test held the logical-message stream for too long");
        Self {
            _connection: client,
        }
    }
}

/// The outer budget `ingest_until_decided` gives the source to say something.
/// No assertion depends on where in it the answer lands.
const INGEST_SETTLE_BUDGET: Duration = Duration::from_secs(60);

/// Calls `ingest_bounded` until the source decides something, and returns that
/// error or committed transaction unchanged.
///
/// A bounded call that returns `Ok` with nothing committed decided nothing: its
/// budget expired. A test asserting that something eventually arrives uses
/// this; a test asserting that nothing arrives bounds one call itself, since
/// this would spin until the outer deadline. `observer`, when given, is drained
/// after each empty call, which can record only a keepalive reply.
fn ingest_until_decided(
    session: &mut ActiveSourceSession,
    registry: &SchemaRegistry,
    authorization_policy: &AuthorizationPolicy,
    config: &PersistentIngestConfig,
    observer: Option<&RecordingObserver>,
) -> Result<PersistentIngestStats, PersistentIngestError> {
    let deadline = Instant::now() + INGEST_SETTLE_BUDGET;
    let mut attempts = 0_u32;
    loop {
        let result = session.ingest_bounded(registry, authorization_policy, config.clone());
        attempts += 1;
        match &result {
            Ok(stats) if stats.committed_transactions == 0 => {}
            _ => return result,
        }
        assert!(
            Instant::now() < deadline,
            "no source transaction reached the session in {INGEST_SETTLE_BUDGET:?} \
             ({attempts} bounded calls, each returning Ok with nothing committed)"
        );
        if let Some(observer) = observer {
            observer.take();
        }
    }
}

#[derive(Debug, Default)]
struct RecordingObserver {
    boundaries: Mutex<Vec<PersistentIngestBoundary>>,
    appended_entries: Mutex<Vec<usize>>,
}

impl PersistentIngestObserver for RecordingObserver {
    fn observe(&self, boundary: PersistentIngestBoundary) {
        self.boundaries.lock().unwrap().push(boundary);
    }

    fn observe_appended_entries(&self, entries: usize) {
        self.appended_entries.lock().unwrap().push(entries);
    }
}

impl RecordingObserver {
    fn take(&self) -> Vec<PersistentIngestBoundary> {
        std::mem::take(&mut *self.boundaries.lock().unwrap())
    }

    /// Each canonical-log commit's count of newly appended entries, in order.
    fn appended_entries(&self) -> Vec<usize> {
        self.appended_entries.lock().unwrap().clone()
    }
}

#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn receipt_failure_blocks_feedback_then_replay_marks_receipt_and_acks() {
    let source_url = database_url().expect("live source database URL");
    let control_url = control_url(&source_url);
    let replication_url = replication_url(&source_url);
    let suffix = unique_suffix();
    let table = format!("rm_ingest_table_{suffix}");
    let publication = format!("rm_ingest_pub_{suffix}");
    let slot = format!("rm_ingest_slot_{suffix}");
    let source_id = SourceId::new(format!("ingest-source-{suffix}")).unwrap();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mutation_id = format!("mutation-{suffix}");
    let server_transaction_id = [73_u8; 16];

    let _logical_stream = LogicalStreamLock::acquire(&source_url);
    let mut source = Client::connect(&source_url, NoTls).unwrap();
    install_metadata(&mut source).unwrap();
    source
        .batch_execute(&format!(
            "CREATE TABLE public.{table} (id bigint PRIMARY KEY, value text NOT NULL);
             CREATE PUBLICATION {publication} FOR TABLE public.{table};"
        ))
        .unwrap();
    let (activated, registry) = activated_fixture(&mut source, &publication, &table, &epoch);
    let authorization_policy = source_wide_policy(&registry);

    let store_a = PostgresFenceStore::connect(&control_url).unwrap();
    store_a.enroll_source(&source_id, &epoch).unwrap();
    let authority_a = Arc::new(FenceAuthority::new(store_a));
    let provisional_a = authority_a
        .acquire(acquire_request(&source_id, &epoch, "ingest-a"))
        .unwrap();
    let enrollment = provision_persistent_slot(
        &authority_a,
        &provisional_a,
        &source_url,
        &activated,
        &SlotProvisioningConfig::new(&slot).unwrap(),
    )
    .unwrap();
    let initial_lsn = enrollment.initial_lsn().unwrap();
    let mut session_a = establish_persistent_source(
        Arc::clone(&authority_a),
        provisional_a,
        &source_url,
        &replication_url,
        &activated,
    )
    .unwrap();

    let marker = MutationLogicalMarker {
        protocol_version: 1,
        mutation_ids: vec![mutation_id.clone()],
        server_transaction_id: server_transaction_id.to_vec(),
    }
    .encode_to_vec();
    let mut rolled_back = source.transaction().unwrap();
    rolled_back
        .execute(
            &format!("INSERT INTO public.{table} VALUES (0, 'rolled-back')"),
            &[],
        )
        .unwrap();
    rolled_back.rollback().unwrap();
    let mut transaction = source.transaction().unwrap();
    transaction
        .execute(
            &format!("INSERT INTO public.{table} VALUES (1, 'committed')"),
            &[],
        )
        .unwrap();
    transaction
        .query_one(
            "SELECT pg_logical_emit_message(true, $1, $2::bytea)",
            &[&"sync.mutation.v1", &marker],
        )
        .unwrap();
    transaction.commit().unwrap();

    let first_observer = Arc::new(RecordingObserver::default());
    let first = ingest_until_decided(
        &mut session_a,
        &registry,
        &authorization_policy,
        &PersistentIngestConfig::new(state_dir())
            .with_bounds(1, Duration::from_secs(5))
            .unwrap()
            .with_observer(first_observer.clone()),
        Some(&first_observer),
    );
    assert_eq!(
        first.as_ref().unwrap_err().disposition(),
        PersistentIngestDisposition::OperatorAction
    );
    assert!(matches!(first, Err(PersistentIngestError::Ingest(_))));
    assert_eq!(
        first_observer.take(),
        [
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
        ]
    );
    assert!(!session_a.socket_is_open());
    drop(session_a);

    assert_eq!(slot_confirmed_lsn(&mut source, &slot), initial_lsn);
    let mut control = Client::connect(&control_url, NoTls).unwrap();
    let head: i64 = control
        .query_one(
            "SELECT head_sequence
               FROM calque_control.canonical_log_head WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(head, 1, "canonical append must precede receipt marking");
    let checkpoint: String = control
        .query_one(
            "SELECT durable_canonical_lsn
               FROM calque_control.source_slot_enrollment WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(parse_lsn(&checkpoint).unwrap(), initial_lsn);

    let authorization_generation = i64::try_from(authorization_policy.generation()).unwrap();
    source
        .execute(
            "INSERT INTO calque_internal.mutation_receipts (
                 mutation_id, source_id, source_epoch, authorization_generation,
                 authorization_policy_hash, authorization_scope_hash,
                 command_name, command_version, canonical_input_sha256,
                 server_transaction_id, device_id, client_instance_id
             ) VALUES ($1, $2, $3, $4, $5, $6, 'test.commit', 1, $7, $8, 'device', 'client')",
            &[
                &mutation_id,
                &source_id.as_str(),
                &epoch.as_str(),
                &authorization_generation,
                &authorization_policy.hash().as_bytes().as_slice(),
                &&[17_u8; 32][..],
                &&[29_u8; 32][..],
                &&server_transaction_id[..],
            ],
        )
        .unwrap();

    let authority_b = Arc::new(FenceAuthority::new(
        PostgresFenceStore::connect(&control_url).unwrap(),
    ));
    let takeover_deadline = Instant::now() + Duration::from_secs(4);
    let provisional_b = loop {
        match authority_b.acquire(acquire_request(&source_id, &epoch, "ingest-b")) {
            Ok(lease) => break lease,
            Err(FenceError::LeaseHeld { .. }) if Instant::now() < takeover_deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            other => panic!("takeover failed: {other:?}"),
        }
    };
    let mut session_b = establish_persistent_source(
        Arc::clone(&authority_b),
        provisional_b,
        &source_url,
        &replication_url,
        &activated,
    )
    .unwrap();
    let replay_observer = Arc::new(RecordingObserver::default());
    let stats = ingest_until_decided(
        &mut session_b,
        &registry,
        &authorization_policy,
        &PersistentIngestConfig::new(state_dir())
            .with_bounds(1, Duration::from_secs(5))
            .unwrap()
            .with_observer(replay_observer.clone()),
        Some(&replay_observer),
    )
    .unwrap();
    assert_eq!(
        replay_observer.take(),
        [
            PersistentIngestBoundary::BeforeNormalize,
            PersistentIngestBoundary::AfterNormalize,
            PersistentIngestBoundary::BeforeCanonicalAppend,
            PersistentIngestBoundary::CanonicalFenceAuthorized,
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
        ]
    );
    assert_eq!(stats.committed_transactions, 1);
    assert_eq!(stats.last_canonical_position.as_ref().unwrap().sequence, 1);
    assert!(stats.flushed_lsn > initial_lsn);
    assert_eq!(session_b.slot().confirmed_flush_lsn, stats.flushed_lsn);

    let feedback_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let advanced: bool = source
            .query_one(
                "SELECT confirmed_flush_lsn >= $2::text::pg_lsn
                   FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot, &format_lsn(stats.flushed_lsn)],
            )
            .unwrap()
            .get(0);
        if advanced {
            break;
        }
        assert!(
            Instant::now() < feedback_deadline,
            "feedback was not flushed"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let observed_cursor: Vec<u8> = source
        .query_one(
            "SELECT observed_canonical_cursor
               FROM calque_internal.mutation_receipts WHERE mutation_id = $1",
            &[&mutation_id],
        )
        .unwrap()
        .get(0);
    let expected_cursor = activated
        .cursor_codec()
        .encode(stats.last_canonical_position.as_ref().unwrap());
    assert_eq!(observed_cursor, expected_cursor.as_str().as_bytes());
    let final_head: i64 = control
        .query_one(
            "SELECT head_sequence
               FROM calque_control.canonical_log_head WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(final_head, 1, "replay must deduplicate canonical append");
    let durable_canonical_lsn: String = control
        .query_one(
            "SELECT durable_canonical_lsn
               FROM calque_control.source_slot_enrollment WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        parse_lsn(&durable_canonical_lsn).unwrap(),
        stats.flushed_lsn
    );

    source
        .execute(
            &format!("INSERT INTO public.{table} VALUES (2, 'next-call')"),
            &[],
        )
        .unwrap();
    let second = ingest_until_decided(
        &mut session_b,
        &registry,
        &authorization_policy,
        &PersistentIngestConfig::new(state_dir())
            .with_bounds(1, Duration::from_secs(5))
            .unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(second.committed_transactions, 1);
    assert_eq!(second.last_canonical_position.as_ref().unwrap().sequence, 2);
    assert!(second.flushed_lsn > stats.flushed_lsn);
    assert_eq!(session_b.slot().confirmed_flush_lsn, second.flushed_lsn);
    let second_head: i64 = control
        .query_one(
            "SELECT head_sequence
               FROM calque_control.canonical_log_head WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(second_head, 2);
    let second_checkpoint: String = control
        .query_one(
            "SELECT durable_canonical_lsn
               FROM calque_control.source_slot_enrollment WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(parse_lsn(&second_checkpoint).unwrap(), second.flushed_lsn);

    // Not `ingest_until_decided`: this call asserts that nothing arrives, and
    // only a bounded wait can show that.
    let idle = session_b
        .ingest_bounded(
            &registry,
            &authorization_policy,
            PersistentIngestConfig::new(state_dir())
                .with_bounds(1, Duration::from_millis(50))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(idle.committed_transactions, 0);
    assert_eq!(idle.flushed_lsn, second.flushed_lsn);
    assert!(
        session_b.socket_is_open(),
        "idle yield must retain ownership"
    );

    source
        .execute(
            &format!("INSERT INTO public.{table} VALUES (3, 'after-idle')"),
            &[],
        )
        .unwrap();
    let after_idle = ingest_until_decided(
        &mut session_b,
        &registry,
        &authorization_policy,
        &PersistentIngestConfig::new(state_dir())
            .with_bounds(1, Duration::from_secs(5))
            .unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(after_idle.committed_transactions, 1);
    assert_eq!(
        after_idle
            .last_canonical_position
            .as_ref()
            .unwrap()
            .sequence,
        3
    );
    assert!(after_idle.flushed_lsn > idle.flushed_lsn);
    assert!(session_b.socket_is_open());

    session_b.shutdown().unwrap();
    source
        .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    source
        .execute(
            "DELETE FROM calque_internal.mutation_receipts WHERE mutation_id = $1",
            &[&mutation_id],
        )
        .unwrap();
    source
        .batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE public.{table};"
        ))
        .unwrap();
}

/// One source table, its publication and slot, enrolled under a provisional
/// lease that `establish` activates.
struct GroupFixture {
    source_url: String,
    control_url: String,
    replication_url: String,
    table: String,
    publication: String,
    slot: String,
    source_id: SourceId,
    epoch: SourceEpoch,
    source: Client,
    activated: ActivatedSource,
    registry: SchemaRegistry,
    policy: AuthorizationPolicy,
    initial_lsn: u64,
    authority: Arc<FenceAuthority<PostgresFenceStore>>,
    provisional: Option<source_runtime::ProvisionalLease>,
}

impl GroupFixture {
    fn start(name: &str) -> Self {
        let source_url = database_url().expect("live source database URL");
        let control_url = control_url(&source_url);
        let replication_url = replication_url(&source_url);
        let suffix = unique_suffix();
        let table = format!("rm_{name}_{suffix}");
        let publication = format!("rm_{name}_pub_{suffix}");
        let slot = format!("rm_{name}_slot_{suffix}");
        let source_id = SourceId::new(format!("{name}-source-{suffix}")).unwrap();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let mut source = Client::connect(&source_url, NoTls).unwrap();
        source
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (id bigint PRIMARY KEY, value text NOT NULL);
                 CREATE PUBLICATION {publication} FOR TABLE public.{table};"
            ))
            .unwrap();
        let (activated, registry) = activated_fixture(&mut source, &publication, &table, &epoch);
        let policy = source_wide_policy(&registry);
        let store = PostgresFenceStore::connect(&control_url).unwrap();
        store.enroll_source(&source_id, &epoch).unwrap();
        let authority = Arc::new(FenceAuthority::new(store));
        let provisional = authority
            .acquire(acquire_request(&source_id, &epoch, "group-a"))
            .unwrap();
        let initial_lsn = provision_persistent_slot(
            &authority,
            &provisional,
            &source_url,
            &activated,
            &SlotProvisioningConfig::new(&slot).unwrap(),
        )
        .unwrap()
        .initial_lsn()
        .unwrap();
        Self {
            source_url,
            control_url,
            replication_url,
            table,
            publication,
            slot,
            source_id,
            epoch,
            source,
            activated,
            registry,
            policy,
            initial_lsn,
            authority,
            provisional: Some(provisional),
        }
    }

    /// Starts streaming from the slot's initial position, and waits for what
    /// is already committed to reach the session.
    ///
    /// PostgreSQL shows a walsender's sent position only to a role with
    /// pg_read_all_stats, which the least-privileged test role lacks, so the
    /// wait is a bound: a few committed transactions take milliseconds to
    /// decode and send.
    fn establish(&mut self) -> ActiveSourceSession {
        let session = self.establish_now();
        thread::sleep(Duration::from_secs(1));
        session
    }

    /// Establishes the session and returns as soon as its stream has started,
    /// while the walsender may still be decoding the WAL before it.
    fn establish_now(&mut self) -> ActiveSourceSession {
        establish_persistent_source(
            Arc::clone(&self.authority),
            self.provisional
                .take()
                .expect("the fixture establishes once"),
            &self.source_url,
            &self.replication_url,
            &self.activated,
        )
        .unwrap()
    }

    /// Commits one row per id, each in its own source transaction, and returns
    /// their xids in commit order.
    fn commit_each(&mut self, ids: &[i64]) -> Vec<i64> {
        let mut xids = Vec::with_capacity(ids.len());
        for id in ids {
            let mut transaction = self.source.transaction().unwrap();
            transaction
                .execute(
                    &format!("INSERT INTO public.{} VALUES ($1, 'grouped')", self.table),
                    &[id],
                )
                .unwrap();
            xids.push(
                transaction
                    .query_one("SELECT pg_catalog.pg_current_xact_id()::text::bigint", &[])
                    .unwrap()
                    .get(0),
            );
            transaction.commit().unwrap();
        }
        xids
    }

    /// The canonical log's xids in sequence order, and the durable checkpoint.
    fn canonical_state(&self) -> (Vec<i64>, u64) {
        let mut control = Client::connect(&self.control_url, NoTls).unwrap();
        let xids = control
            .query(
                "SELECT (transaction_identity::jsonb ->> 'postgresXid')::bigint
                   FROM calque_control.canonical_log_record
                  WHERE source_id = $1
                  ORDER BY sequence",
                &[&self.source_id.as_str()],
            )
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        let checkpoint: String = control
            .query_one(
                "SELECT durable_canonical_lsn
                   FROM calque_control.source_slot_enrollment WHERE source_id = $1",
                &[&self.source_id.as_str()],
            )
            .unwrap()
            .get(0);
        (xids, parse_lsn(&checkpoint).unwrap())
    }

    fn config(&self, max_transactions: usize) -> PersistentIngestConfig {
        PersistentIngestConfig::new(state_dir())
            .with_bounds(max_transactions, Duration::from_secs(5))
            .unwrap()
    }

    fn take_over(&self) -> ActiveSourceSession {
        let authority = Arc::new(FenceAuthority::new(
            PostgresFenceStore::connect(&self.control_url).unwrap(),
        ));
        let deadline = Instant::now() + Duration::from_secs(4);
        let provisional = loop {
            match authority.acquire(acquire_request(&self.source_id, &self.epoch, "group-b")) {
                Ok(lease) => break lease,
                Err(FenceError::LeaseHeld { .. }) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                other => panic!("takeover failed: {other:?}"),
            }
        };
        establish_persistent_source(
            authority,
            provisional,
            &self.source_url,
            &self.replication_url,
            &self.activated,
        )
        .unwrap()
    }

    /// Rewrites record `sequence` in JSON, the form an earlier release
    /// stored every entry in, with a checksum that matches the new bytes.
    fn rewrite_as_an_earlier_release(&self, sequence: i64) {
        let rewritten = Client::connect(&self.control_url, NoTls)
            .unwrap()
            .execute(
                "UPDATE calque_control.canonical_log_record
                    SET entry_bytes = $3, entry_sha256 = encode(sha256($3), 'hex')
                  WHERE source_id = $1 AND sequence = $2",
                &[
                    &self.source_id.as_str(),
                    &sequence,
                    &br#"{"recordKind":"TRANSACTION","record":{}}"#.as_slice(),
                ],
            )
            .unwrap();
        assert_eq!(rewritten, 1);
    }

    fn remove(mut self, session: ActiveSourceSession) {
        session.shutdown().unwrap();
        self.source
            .query_one(
                "SELECT pg_catalog.pg_drop_replication_slot($1)",
                &[&self.slot],
            )
            .unwrap();
        self.source
            .batch_execute(&format!(
                "DROP PUBLICATION {}; DROP TABLE public.{};",
                self.publication, self.table
            ))
            .unwrap();
    }
}

#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn transactions_already_received_append_as_one_group_under_one_acknowledgement() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("group");
    let xids = fixture.commit_each(&[1, 2, 3]);
    let mut session = fixture.establish();

    let observer = Arc::new(RecordingObserver::default());
    let stats = ingest_until_decided(
        &mut session,
        &fixture.registry,
        &fixture.policy,
        &fixture.config(3).with_observer(observer.clone()),
        Some(&observer),
    )
    .unwrap();
    assert_eq!(
        observer.take(),
        [
            PersistentIngestBoundary::BeforeNormalize,
            PersistentIngestBoundary::AfterNormalize,
            PersistentIngestBoundary::BeforeNormalize,
            PersistentIngestBoundary::AfterNormalize,
            PersistentIngestBoundary::BeforeNormalize,
            PersistentIngestBoundary::AfterNormalize,
            PersistentIngestBoundary::BeforeCanonicalAppend,
            PersistentIngestBoundary::BeforeCheckpoint,
            PersistentIngestBoundary::CanonicalFenceAuthorized,
            PersistentIngestBoundary::CanonicalSequenceAllocated,
            PersistentIngestBoundary::CanonicalRecordInserted,
            PersistentIngestBoundary::CanonicalHeadUpdated,
            PersistentIngestBoundary::AfterCheckpoint,
            PersistentIngestBoundary::CanonicalTransactionCommitted,
            PersistentIngestBoundary::AfterCanonicalAppend,
            PersistentIngestBoundary::BeforeFinalFence,
            PersistentIngestBoundary::AfterFinalFence,
            PersistentIngestBoundary::BeforeFeedbackEncode,
            PersistentIngestBoundary::AfterFeedbackEncode,
            PersistentIngestBoundary::BeforeFeedbackCopyData,
            PersistentIngestBoundary::FeedbackCopyDataQueued,
            PersistentIngestBoundary::BeforeFeedbackTransportFlush,
            PersistentIngestBoundary::FeedbackTransportFlushed,
        ],
        "three transactions already received must append in one commit and \
         be acknowledged by one status update"
    );
    assert_eq!(
        observer.appended_entries(),
        [3],
        "the one commit must report its three entries"
    );
    assert_eq!(stats.committed_transactions, 3);
    assert_eq!(stats.feedback_writes, 1);
    assert_eq!(stats.last_canonical_position.as_ref().unwrap().sequence, 3);
    assert!(stats.flushed_lsn > fixture.initial_lsn);
    assert_eq!(fixture.canonical_state(), (xids, stats.flushed_lsn));
    assert_eq!(session.slot().confirmed_flush_lsn, stats.flushed_lsn);

    fixture.remove(session);
}

/// Panics when the group's commit has returned, before the group is
/// acknowledged: a crash between the commit and the acknowledgement.
#[derive(Debug)]
struct CrashAfterCommit;

impl PersistentIngestObserver for CrashAfterCommit {
    fn observe(&self, boundary: PersistentIngestBoundary) {
        if boundary == PersistentIngestBoundary::AfterCanonicalAppend {
            panic!("crash between the group's commit and its acknowledgement");
        }
    }
}

#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn a_crash_between_a_group_commit_and_its_acknowledgement_replays_the_group_once() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("group_crash");
    let xids = fixture.commit_each(&[1, 2, 3]);
    let mut session = fixture.establish();

    let crashed = catch_unwind(AssertUnwindSafe(|| {
        session.ingest_bounded(
            &fixture.registry,
            &fixture.policy,
            fixture.config(3).with_observer(Arc::new(CrashAfterCommit)),
        )
    }));
    assert!(crashed.is_err(), "the observer must crash the ingest");
    assert!(!session.socket_is_open());
    drop(session);
    let (appended, _) = fixture.canonical_state();
    assert!(
        !appended.is_empty() && xids.starts_with(&appended),
        "the crashed group committed {appended:?} of {xids:?}"
    );
    assert_eq!(
        slot_confirmed_lsn(&mut fixture.source, &fixture.slot),
        fixture.initial_lsn,
        "nothing may be acknowledged before the group's commit is followed by its feedback"
    );

    // The next owner replays from the slot's confirmed position: what the
    // crashed group committed deduplicates, and the rest appends once.
    let mut session = fixture.take_over();
    let replay_observer = Arc::new(RecordingObserver::default());
    let mut committed = 0;
    let mut flushed_lsn = fixture.initial_lsn;
    while committed < xids.len() {
        let stats = ingest_until_decided(
            &mut session,
            &fixture.registry,
            &fixture.policy,
            &fixture
                .config(xids.len() - committed)
                .with_observer(replay_observer.clone()),
            None,
        )
        .unwrap();
        committed += stats.committed_transactions;
        flushed_lsn = stats.flushed_lsn;
    }
    assert_eq!(committed, xids.len());
    assert_eq!(
        replay_observer.appended_entries().iter().sum::<usize>(),
        xids.len() - appended.len(),
        "the replay must count only the entries the crashed group did not commit"
    );
    assert_eq!(fixture.canonical_state(), (xids, flushed_lsn));
    assert_eq!(session.slot().confirmed_flush_lsn, flushed_lsn);

    fixture.remove(session);
}

/// How long the backlog test's transactions wait, committed, before the
/// session that sends them starts.
const LATE_SEND: Duration = Duration::from_millis(1_200);

/// A cycle that fills its transaction bound stops reading, so it has not
/// caught up with its connection; the next takes what is left and waits with
/// nothing more arriving, so it has. Transactions committed before the
/// session started were sent at least `LATE_SEND` after their commits, and
/// both cycles say so.
///
/// The fixture's lease waits out `LATE_SEND` unrenewed, so the session also
/// starts under a lease older than one renewal interval, which a watchdog
/// that waited a whole interval before renewing would let expire.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn a_cycle_that_fills_its_bound_has_not_caught_up_and_a_backlog_reads_late() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("backlog");
    fixture.commit_each(&[1, 2, 3]);
    thread::sleep(LATE_SEND);
    let mut session = fixture.establish();

    let filled = ingest_until_decided(
        &mut session,
        &fixture.registry,
        &fixture.policy,
        &fixture.config(2),
        None,
    )
    .unwrap();
    assert_eq!(filled.committed_transactions, 2);
    assert!(!filled.caught_up);
    assert!(
        filled.longest_send_delay >= LATE_SEND,
        "{:?}",
        filled.longest_send_delay
    );

    let rest = session
        .ingest_bounded(
            &fixture.registry,
            &fixture.policy,
            PersistentIngestConfig::new(state_dir())
                .with_bounds(2, Duration::from_millis(200))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(rest.committed_transactions, 1);
    assert!(rest.caught_up);
    assert!(
        rest.longest_send_delay >= LATE_SEND,
        "{:?}",
        rest.longest_send_delay
    );

    fixture.remove(session);
}

/// A cycle that takes each transaction as it arrives has caught up, though it
/// fills its bound: a source that commits more than one bound of transactions
/// a cycle can still be current. The writer commits while the session
/// streams, so each commit is sent at once.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn a_cycle_that_takes_transactions_as_they_arrive_catches_up_at_its_bound() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("arriving");
    let mut session = fixture.establish();
    let source_url = fixture.source_url.clone();
    let table = fixture.table.clone();
    let writer = thread::spawn(move || {
        let mut source = Client::connect(&source_url, NoTls).unwrap();
        for id in [1_i64, 2] {
            thread::sleep(Duration::from_millis(500));
            source
                .execute(
                    &format!("INSERT INTO public.{table} VALUES ($1, 'arriving')"),
                    &[&id],
                )
                .unwrap();
        }
    });

    let stats = session
        .ingest_bounded(&fixture.registry, &fixture.policy, fixture.config(2))
        .unwrap();
    writer.join().unwrap();
    assert_eq!(stats.committed_transactions, 2);
    assert!(stats.caught_up);
    assert!(
        stats.longest_send_delay < Duration::from_secs(1),
        "{:?}",
        stats.longest_send_delay
    );

    fixture.remove(session);
}

/// A cycle that ends once current ends as soon as its session's stream has
/// reached the WAL PostgreSQL had flushed when the session started it, long
/// before its timeout. A session is current that way once: its next such
/// cycle, with nothing committed, waits its timeout out.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn a_cycle_ending_once_current_ends_when_the_stream_reaches_the_session_start() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("current");
    let mut session = fixture.establish();

    let started = Instant::now();
    let first = session
        .ingest_bounded(
            &fixture.registry,
            &fixture.policy,
            fixture.config(2).ending_once_current(),
        )
        .unwrap();
    let first_took = started.elapsed();
    assert_eq!(first.committed_transactions, 0);
    assert!(first.caught_up);
    assert!(first_took < Duration::from_secs(2), "{first_took:?}");

    let started = Instant::now();
    let second = session
        .ingest_bounded(
            &fixture.registry,
            &fixture.policy,
            PersistentIngestConfig::new(state_dir())
                .with_bounds(2, Duration::from_millis(500))
                .unwrap()
                .ending_once_current(),
        )
        .unwrap();
    let second_took = started.elapsed();
    assert_eq!(second.committed_transactions, 0);
    assert!(second.caught_up);
    assert!(second_took >= Duration::from_millis(500), "{second_took:?}");

    fixture.remove(session);
}

/// A cycle that ends once current does not end before what was committed
/// before its session started has arrived, though nothing is waiting yet: the
/// walsender is still decoding a large transaction outside the publication
/// that precedes it.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn a_cycle_ending_once_current_waits_for_what_was_committed_before_the_session() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("behind");
    let bulk = format!("{}_bulk", fixture.table);
    fixture
        .source
        .batch_execute(&format!(
            "CREATE TABLE public.{bulk} (id bigint NOT NULL, value text NOT NULL);
             INSERT INTO public.{bulk} SELECT g, 'outside' FROM generate_series(1, 500000) AS g;"
        ))
        .unwrap();
    let xids = fixture.commit_each(&[1]);
    let mut session = fixture.establish_now();

    let stats = session
        .ingest_bounded(
            &fixture.registry,
            &fixture.policy,
            fixture.config(10).ending_once_current(),
        )
        .unwrap();
    assert_eq!(stats.committed_transactions, 1);
    assert!(stats.caught_up);
    assert_eq!(fixture.canonical_state().0, xids);

    fixture
        .source
        .batch_execute(&format!("DROP TABLE public.{bulk};"))
        .unwrap();
    fixture.remove(session);
}

/// A cycle that ends once current still takes what is waiting when its stream
/// reaches the session's start: transactions committed after the session
/// started, which have arrived by the time the cycle reads.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn a_cycle_ending_once_current_takes_what_is_waiting() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("waiting");
    let mut session = fixture.establish();
    let xids = fixture.commit_each(&[1, 2, 3]);
    thread::sleep(Duration::from_millis(500));

    let stats = session
        .ingest_bounded(
            &fixture.registry,
            &fixture.policy,
            fixture.config(10).ending_once_current(),
        )
        .unwrap();
    assert_eq!(stats.committed_transactions, 3);
    assert!(stats.caught_up);
    assert_eq!(fixture.canonical_state().0, xids);

    fixture.remove(session);
}

/// A session's first ingest connects its own canonical-log writer, and that
/// connect refuses a log an earlier release wrote. The refusal names the state
/// directory the ingest configuration carries.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn a_session_refuses_a_log_an_earlier_release_wrote_and_names_the_state_dir() {
    let _logical_stream = LogicalStreamLock::acquire(&database_url().unwrap());
    let mut fixture = GroupFixture::start("earlier_log");
    fixture.commit_each(&[1]);
    let mut session = fixture.establish();
    let stats = ingest_until_decided(
        &mut session,
        &fixture.registry,
        &fixture.policy,
        &fixture.config(1),
        None,
    )
    .unwrap();
    assert_eq!(stats.last_canonical_position.unwrap().sequence, 1);
    session.shutdown().unwrap();

    fixture.rewrite_as_an_earlier_release(1);
    let mut session = fixture.take_over();
    let refused = session
        .ingest_bounded(&fixture.registry, &fixture.policy, fixture.config(1))
        .unwrap_err();
    assert!(
        matches!(
            &refused,
            PersistentIngestError::CanonicalLog(LogError::EarlierEntryFormat { state_dir: named })
                if named == state_dir()
        ),
        "{refused:?}"
    );
    assert_eq!(
        refused.disposition(),
        PersistentIngestDisposition::OperatorAction
    );
    assert_eq!(
        refused.to_string(),
        "/srv/calque/state serves a canonical log an earlier release of calque wrote, and calque does not migrate it."
    );

    fixture.remove(session);
}
