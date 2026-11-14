use std::{
    collections::BTreeSet,
    env,
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use canonical_log::{
    CanonicalBarrier, FencedWriterIdentity, LogError, PostgresDatastoreIdentity,
    PostgresFencedCanonicalLog, TransactionKey,
};
use postgres::{Client, NoTls, Transaction};
use source_control::{
    inspect_source, ActivatedSource, EnrollmentRequirements, PgoutputLaunchProfile, QualifiedTable,
    SchemaRegistry,
};
use source_runtime::{
    establish_persistent_source, provision_persistent_slot, AcquireRequest, ActiveSourceSession,
    FenceAuthority, FenceError, FenceStore, OpenTransaction, OwnerId, PostgresFenceStore,
    SlotProvisioningConfig, SourceRuntimeError,
};
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

fn datastore_identity(database_url: &str) -> PostgresDatastoreIdentity {
    let mut client = Client::connect(database_url, NoTls).unwrap();
    PostgresDatastoreIdentity::inspect(&mut client).unwrap()
}

fn connect_database(database_url: &str, database: &str) -> Client {
    let mut config: postgres::Config = database_url.parse().unwrap();
    config.dbname(database);
    config.connect(NoTls).unwrap()
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

fn acquire_request(
    source_id: &SourceId,
    epoch: &SourceEpoch,
    owner: &str,
    ttl: Duration,
) -> AcquireRequest {
    AcquireRequest {
        source_id: source_id.clone(),
        source_epoch: epoch.clone(),
        owner_id: OwnerId::new(owner).unwrap(),
        ttl,
    }
}

fn activated_fixture(
    client: &mut Client,
    publication: &str,
    table: &str,
    epoch: &SourceEpoch,
) -> ActivatedSource {
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
    ActivatedSource::new(
        &registry,
        epoch.clone(),
        CursorCodec::new([29_u8; 32]).unwrap(),
        PgoutputLaunchProfile::V1,
    )
}

fn barrier(epoch: &SourceEpoch, sequence: u64) -> CanonicalBarrier {
    CanonicalBarrier {
        key: TransactionKey {
            source_epoch: epoch.clone(),
            end_lsn: format!("0/{:X}", sequence + 1),
            postgres_xid: sequence as u32,
        },
        build_id: format!("build-{sequence}"),
        nonce: format!("nonce-{sequence}"),
        message_lsn: format!("0/{sequence:X}"),
        commit_lsn: format!("0/{:X}", sequence + 1),
        commit_time_unix_micros: sequence,
    }
}

#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn persistent_slot_takeover_revokes_backend_and_stale_log_writer() {
    let source_url = database_url().expect("live source database URL");
    let control_url = control_url(&source_url);
    let replication_url = replication_url(&source_url);
    let suffix = unique_suffix();
    let table = format!("rm_slot_table_{suffix}");
    let publication = format!("rm_slot_pub_{suffix}");
    let slot = format!("rm_slot_{suffix}");
    let source_id = SourceId::new(format!("slot-source-{suffix}")).unwrap();
    let epoch = SourceEpoch::new("epoch-1").unwrap();

    let mut source = Client::connect(&source_url, NoTls).unwrap();
    source
        .batch_execute(&format!(
            "CREATE TABLE public.{table} (id bigint PRIMARY KEY, value text NOT NULL);
             CREATE PUBLICATION {publication} FOR TABLE public.{table};"
        ))
        .unwrap();
    let activated = activated_fixture(&mut source, &publication, &table, &epoch);

    let store_a = PostgresFenceStore::connect(&control_url).unwrap();
    let control_pid_a = store_a.control_backend_pid().unwrap();
    store_a.enroll_source(&source_id, &epoch).unwrap();
    let authority_a = Arc::new(FenceAuthority::new(store_a));
    let provisional_a = authority_a
        .acquire(acquire_request(
            &source_id,
            &epoch,
            "runtime-a",
            Duration::from_secs(1),
        ))
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
    let session_a = establish_persistent_source(
        Arc::clone(&authority_a),
        provisional_a,
        &source_url,
        &replication_url,
        &activated,
    )
    .unwrap();
    assert!(session_a.socket_is_open());
    assert_eq!(session_a.slot().confirmed_flush_lsn, initial_lsn);

    let old_log = PostgresFencedCanonicalLog::connect(
        &control_url,
        FencedWriterIdentity::new(
            source_id.clone(),
            epoch.clone(),
            session_a.lease().identity().token().get(),
            datastore_identity(&control_url),
        )
        .unwrap(),
        state_dir(),
    )
    .unwrap();
    assert_eq!(
        old_log
            .append_barrier(barrier(&epoch, 1))
            .unwrap()
            .position()
            .sequence,
        1
    );

    // No caller touches the session for longer than its original TTL. The
    // owned watchdog, rather than application code, must keep it authorized.
    thread::sleep(Duration::from_millis(1_150));
    assert!(session_a.socket_is_open());
    authority_a.validate_active(session_a.lease()).unwrap();

    // Kill the control connection. The owned watchdog must revoke local
    // authority and terminate the exact PostgreSQL slot backend without any
    // caller-driven renew/validate operation.
    let mut killer = Client::connect(&control_url, NoTls).unwrap();
    assert!(killer
        .query_one(
            "SELECT pg_catalog.pg_terminate_backend($1, $2)",
            &[&control_pid_a, &1_000_i64],
        )
        .unwrap()
        .get::<_, bool>(0));
    let watchdog_deadline = Instant::now() + Duration::from_secs(4);
    while session_a.lease().is_locally_valid() && Instant::now() < watchdog_deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(!session_a.lease().is_locally_valid());
    let backend_deadline = Instant::now() + Duration::from_secs(3);
    let slot_pid = loop {
        let slot_pid: Option<i32> = source
            .query_one(
                "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .unwrap()
            .get(0);
        if slot_pid.is_none() || Instant::now() >= backend_deadline {
            break slot_pid;
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(slot_pid, None);

    let store_b = PostgresFenceStore::connect(&control_url).unwrap();
    let authority_b = Arc::new(FenceAuthority::new(store_b));
    let takeover_deadline = Instant::now() + Duration::from_secs(4);
    let provisional_b = loop {
        match authority_b.acquire(acquire_request(
            &source_id,
            &epoch,
            "runtime-b",
            Duration::from_secs(1),
        )) {
            Ok(lease) => break lease,
            Err(FenceError::LeaseHeld { .. }) if Instant::now() < takeover_deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            other => panic!("takeover failed: {other:?}"),
        }
    };
    assert!(
        provisional_b.identity().token() > session_a.lease().identity().token(),
        "takeover must allocate a higher token"
    );
    let session_b = establish_persistent_source(
        Arc::clone(&authority_b),
        provisional_b,
        &source_url,
        &replication_url,
        &activated,
    )
    .unwrap();
    assert!(session_b.socket_is_open());
    assert_eq!(session_b.slot().confirmed_flush_lsn, initial_lsn);

    assert!(matches!(
        old_log.append_barrier(barrier(&epoch, 2)),
        Err(LogError::FencedWriterRejected)
    ));
    assert_eq!(old_log.head().unwrap().sequence, 1);
    let current_log = PostgresFencedCanonicalLog::connect(
        &control_url,
        FencedWriterIdentity::new(
            source_id.clone(),
            epoch.clone(),
            session_b.lease().identity().token().get(),
            datastore_identity(&control_url),
        )
        .unwrap(),
        state_dir(),
    )
    .unwrap();
    assert_eq!(
        current_log
            .append_barrier(barrier(&epoch, 2))
            .unwrap()
            .position()
            .sequence,
        2
    );

    session_b.shutdown().unwrap();
    drop(session_a);
    let slot_row = source
        .query_one(
            "SELECT active, confirmed_flush_lsn::text
               FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap();
    assert!(!slot_row.get::<_, bool>(0));
    assert!(slot_row.get::<_, Option<String>>(1).is_some());

    source
        .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    source
        .batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE public.{table};"
        ))
        .unwrap();
}

#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn missing_enrolled_slot_is_a_reseed_boundary_not_auto_creation() {
    let source_url = database_url().expect("live source database URL");
    let control_url = control_url(&source_url);
    let suffix = unique_suffix();
    let table = format!("rm_missing_table_{suffix}");
    let publication = format!("rm_missing_pub_{suffix}");
    let slot = format!("rm_missing_slot_{suffix}");
    let source_id = SourceId::new(format!("missing-slot-source-{suffix}")).unwrap();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut source = Client::connect(&source_url, NoTls).unwrap();
    source
        .batch_execute(&format!(
            "CREATE TABLE public.{table} (id bigint PRIMARY KEY);
             CREATE PUBLICATION {publication} FOR TABLE public.{table};"
        ))
        .unwrap();
    let activated = activated_fixture(&mut source, &publication, &table, &epoch);
    let store = PostgresFenceStore::connect(&control_url).unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let authority = Arc::new(FenceAuthority::new(store));
    let provision = authority
        .acquire(acquire_request(
            &source_id,
            &epoch,
            "provisioner",
            Duration::from_secs(1),
        ))
        .unwrap();
    let enrollment = provision_persistent_slot(
        &authority,
        &provision,
        &source_url,
        &activated,
        &SlotProvisioningConfig::new(&slot).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        provision_persistent_slot(
            &authority,
            &provision,
            &source_url,
            &activated,
            &SlotProvisioningConfig::new(&slot).unwrap(),
        ),
        Err(source_runtime::SourceRuntimeError::Fence(
            FenceError::SlotLifecycleAlreadyInitialized
        ))
    ));
    authority.release(&provision).unwrap();
    let mut control = Client::connect(&control_url, NoTls).unwrap();
    control
        .execute(
            "UPDATE calque_control.source_slot_enrollment
                SET durable_canonical_lsn = '0/0'
              WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    let ahead = authority
        .acquire(acquire_request(
            &source_id,
            &epoch,
            "ahead-check",
            Duration::from_secs(1),
        ))
        .unwrap();
    let error = establish_persistent_source(
        Arc::clone(&authority),
        ahead,
        &source_url,
        &replication_url(&source_url),
        &activated,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        source_runtime::SourceRuntimeError::SlotAheadOfCanonicalTruth { .. }
    ));
    control
        .execute(
            "UPDATE calque_control.source_slot_enrollment
                SET durable_canonical_lsn = initial_lsn
              WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert_eq!(enrollment.pgoutput_profile(), "v1-no-streaming");
    let wrong_epoch_source = activated_fixture(
        &mut source,
        &publication,
        &table,
        &SourceEpoch::new("wrong-epoch").unwrap(),
    );
    let wrong_epoch = authority
        .acquire(acquire_request(
            &source_id,
            &epoch,
            "wrong-epoch",
            Duration::from_secs(1),
        ))
        .unwrap();
    assert!(matches!(
        establish_persistent_source(
            Arc::clone(&authority),
            wrong_epoch,
            &source_url,
            &replication_url(&source_url),
            &wrong_epoch_source,
        ),
        Err(source_runtime::SourceRuntimeError::ActivatedSourceLeaseMismatch)
    ));

    source
        .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    source
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot(
                $1, 'test_decoding', false, false
             )",
            &[&slot],
        )
        .unwrap();
    let wrong_plugin = authority
        .acquire(acquire_request(
            &source_id,
            &epoch,
            "wrong-plugin",
            Duration::from_secs(1),
        ))
        .unwrap();
    assert!(matches!(
        establish_persistent_source(
            Arc::clone(&authority),
            wrong_plugin,
            &source_url,
            &replication_url(&source_url),
            &activated,
        ),
        Err(source_runtime::SourceRuntimeError::UnexpectedSlotMetadata)
    ));
    source
        .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    let other_database = format!("rm_other_{suffix}");
    source
        .batch_execute(&format!("CREATE DATABASE {other_database}"))
        .unwrap();
    let mut other = connect_database(&source_url, &other_database);
    other
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot(
                $1, 'pgoutput', false, false
             )",
            &[&slot],
        )
        .unwrap();
    let wrong_database = authority
        .acquire(acquire_request(
            &source_id,
            &epoch,
            "wrong-database",
            Duration::from_secs(1),
        ))
        .unwrap();
    assert!(matches!(
        establish_persistent_source(
            Arc::clone(&authority),
            wrong_database,
            &source_url,
            &replication_url(&source_url),
            &activated,
        ),
        Err(source_runtime::SourceRuntimeError::UnexpectedSlotMetadata)
    ));
    other
        .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    drop(other);
    source
        .batch_execute(&format!("DROP DATABASE {other_database}"))
        .unwrap();
    let runtime = authority
        .acquire(acquire_request(
            &source_id,
            &epoch,
            "runtime",
            Duration::from_secs(1),
        ))
        .unwrap();
    let error = establish_persistent_source(
        Arc::clone(&authority),
        runtime,
        &source_url,
        &replication_url(&source_url),
        &activated,
    )
    .unwrap_err();
    assert!(
        matches!(
            &error,
            source_runtime::SourceRuntimeError::SlotMissing { slot_name } if *slot_name == slot
        ),
        "{error}"
    );
    let slot_count: i64 = source
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert_eq!(slot_count, 0);
    source
        .batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE public.{table};"
        ))
        .unwrap();
}

/// A table and publication ready to enroll, under a fresh source id.
struct OpenTransactionFixture {
    source_url: String,
    source: Client,
    table: String,
    publication: String,
    slot: String,
    source_id: SourceId,
    epoch: SourceEpoch,
    activated: ActivatedSource,
    authority: FenceAuthority<PostgresFenceStore>,
}

impl OpenTransactionFixture {
    fn create(label: &str) -> Self {
        let source_url = database_url().expect("live source database URL");
        let suffix = unique_suffix();
        let table = format!("rm_{label}_table_{suffix}");
        let publication = format!("rm_{label}_pub_{suffix}");
        let slot = format!("rm_{label}_slot_{suffix}");
        let source_id = SourceId::new(format!("{label}-source-{suffix}")).unwrap();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let mut source = Client::connect(&source_url, NoTls).unwrap();
        source
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (id bigint PRIMARY KEY);
                 CREATE PUBLICATION {publication} FOR TABLE public.{table};"
            ))
            .unwrap();
        let activated = activated_fixture(&mut source, &publication, &table, &epoch);
        let store = PostgresFenceStore::connect(&control_url(&source_url)).unwrap();
        store.enroll_source(&source_id, &epoch).unwrap();
        Self {
            source_url,
            source,
            table,
            publication,
            slot,
            source_id,
            epoch,
            activated,
            authority: FenceAuthority::new(store),
        }
    }

    /// Opens a transaction that writes to the table from a second session,
    /// the way an application transaction does, and rolls it back after
    /// `open_for`. Returns once the write has run, with the session's
    /// `pg_stat_activity` entry as slot creation reports it.
    fn hold_open_write(&self, open_for: Duration) -> (OpenTransaction, thread::JoinHandle<()>) {
        hold_open_write(&self.source_url, &self.table, open_for)
    }

    fn slot_exists(&mut self) -> bool {
        self.source
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .unwrap()
            .is_some()
    }

    fn remove(mut self) {
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

/// Slot creation waits for every open transaction that has written, and a
/// wait longer than the lease TTL must not let the provisional lease expire
/// before the enrollment it guards is recorded: an expired lease would leave
/// the slot behind with no enrollment, which the next start refuses as
/// `SlotAlreadyExists`.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn slot_creation_keeps_its_lease_while_it_waits_for_an_open_transaction() {
    let mut fixture = OpenTransactionFixture::create("slot_wait");
    let ttl = Duration::from_secs(1);
    let provisional = fixture
        .authority
        .acquire(acquire_request(
            &fixture.source_id,
            &fixture.epoch,
            "provisioner",
            ttl,
        ))
        .unwrap();
    let (_, holder) = fixture.hold_open_write(Duration::from_millis(2_500));
    let started = Instant::now();
    let enrolled = provision_persistent_slot(
        &fixture.authority,
        &provisional,
        &fixture.source_url,
        &fixture.activated,
        &SlotProvisioningConfig::new(&fixture.slot).unwrap(),
    );
    let waited = started.elapsed();
    holder.join().unwrap();
    let enrollment = enrolled.expect("enrollment outlasts a transaction open for 2.5 s");
    assert!(
        waited > Duration::from_secs(2),
        "the slot was created after {waited:?}, before the open transaction ended"
    );
    assert_eq!(enrollment.slot_name(), fixture.slot);
    assert!(fixture.slot_exists());
    fixture.authority.release(&provisional).unwrap();
    fixture.remove();
}

/// A transaction still open at the bound ends slot creation with an error
/// naming it, leaves no slot behind, and keeps the lease, so a retry once
/// the transaction ends enrolls.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn slot_creation_names_the_open_transaction_it_stopped_waiting_for() {
    let mut fixture = OpenTransactionFixture::create("slot_bound");
    let provisional = fixture
        .authority
        .acquire(acquire_request(
            &fixture.source_id,
            &fixture.epoch,
            "provisioner",
            Duration::from_secs(10),
        ))
        .unwrap();
    // Past the reply bound of a controller connection whose statements run
    // for 2 s, so slot creation's connection has to be bounded by its wait.
    let bound = Duration::from_secs(6);
    let (open, holder) = fixture.hold_open_write(Duration::from_secs(8));
    let started = Instant::now();
    let refused = provision_persistent_slot(
        &fixture.authority,
        &provisional,
        &fixture.source_url,
        &fixture.activated,
        &SlotProvisioningConfig::new(&fixture.slot)
            .unwrap()
            .with_open_transaction_wait(bound),
    );
    let waited = started.elapsed();
    let slot_left = fixture.slot_exists();
    match refused {
        Err(SourceRuntimeError::SlotCreationWaitedForOpenTransactions {
            slot_name,
            waited: reported,
            open: still_open,
        }) => {
            assert_eq!(slot_name, fixture.slot);
            assert_eq!(reported, bound);
            // Other tests' transactions may be open on the same server too.
            assert!(
                still_open.contains(&open),
                "{open} is missing from {still_open:?}"
            );
        }
        Err(error) => panic!("slot creation failed with {error:?}, not at the wait bound"),
        Ok(_) => panic!("slot creation succeeded while the transaction was open"),
    }
    assert!(
        waited >= bound && waited < bound + Duration::from_secs(2),
        "slot creation gave up after {waited:?}, not at the {bound:?} bound"
    );
    assert!(!slot_left, "a slot outlived the refusal");
    holder.join().unwrap();

    provision_persistent_slot(
        &fixture.authority,
        &provisional,
        &fixture.source_url,
        &fixture.activated,
        &SlotProvisioningConfig::new(&fixture.slot).unwrap(),
    )
    .expect("enrollment succeeds once the transaction ends");
    fixture.authority.release(&provisional).unwrap();
    fixture.remove();
}

/// Long enough that a renewal can wait out the control connection's 1 s
/// `lock_timeout` and still retry: the watchdog renews every 2 s.
const WATCHDOG_RETRY_TTL: Duration = Duration::from_secs(6);

fn lease_expiry_millis(observer: &mut Client, source_id: &SourceId) -> i64 {
    observer
        .query_one(
            "SELECT floor(extract(epoch FROM expires_at) * 1000)::bigint
               FROM calque_control.source_fence_lease WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0)
}

/// Returns just after the watchdog's next renewal commits, so that its next
/// attempt comes one renewal interval later.
fn wait_for_renewal(observer: &mut Client, source_id: &SourceId) {
    let before = lease_expiry_millis(observer, source_id);
    let deadline = Instant::now() + WATCHDOG_RETRY_TTL;
    while lease_expiry_millis(observer, source_id) == before {
        assert!(Instant::now() < deadline, "the watchdog stopped renewing");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Locks the source's lease row from another connection, as a canonical
/// append group that splits into several statements does, until the returned
/// transaction ends.
fn lock_lease_row<'a>(holder: &'a mut Client, source_id: &SourceId) -> Transaction<'a> {
    let mut transaction = holder.transaction().unwrap();
    transaction
        .query_one(
            "SELECT 1 FROM calque_control.source_fence_lease WHERE source_id = $1 FOR UPDATE",
            &[&source_id.as_str()],
        )
        .unwrap();
    transaction
}

/// The start of the control connection's transaction if it is waiting on a
/// lock now. Each renewal attempt is a transaction of its own.
fn waiting_renewal(observer: &mut Client, control_pid: i32) -> Option<String> {
    observer
        .query_opt(
            "SELECT xact_start::text FROM pg_catalog.pg_stat_activity
              WHERE pid = $1 AND wait_event_type = 'Lock'",
            &[&control_pid],
        )
        .unwrap()
        .map(|row| row.get(0))
}

/// A persistent source session on a table, publication and slot of its own,
/// with the control connection's backend PID so a test can watch its renewals.
struct WatchedSession {
    source: Client,
    table: String,
    publication: String,
    slot: String,
    source_id: SourceId,
    control_url: String,
    control_pid: i32,
}

impl WatchedSession {
    fn establish(label: &str, ttl: Duration) -> (Self, ActiveSourceSession) {
        let source_url = database_url().expect("live source database URL");
        let control_url = control_url(&source_url);
        let suffix = unique_suffix();
        let table = format!("rm_{label}_table_{suffix}");
        let publication = format!("rm_{label}_pub_{suffix}");
        let slot = format!("rm_{label}_slot_{suffix}");
        let source_id = SourceId::new(format!("{label}-source-{suffix}")).unwrap();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let mut source = Client::connect(&source_url, NoTls).unwrap();
        source
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (id bigint PRIMARY KEY);
                 CREATE PUBLICATION {publication} FOR TABLE public.{table};"
            ))
            .unwrap();
        let activated = activated_fixture(&mut source, &publication, &table, &epoch);
        let store = PostgresFenceStore::connect(&control_url).unwrap();
        let control_pid = store.control_backend_pid().unwrap();
        store.enroll_source(&source_id, &epoch).unwrap();
        let authority = Arc::new(FenceAuthority::new(store));
        let provisional = authority
            .acquire(acquire_request(&source_id, &epoch, "runtime-a", ttl))
            .unwrap();
        provision_persistent_slot(
            &authority,
            &provisional,
            &source_url,
            &activated,
            &SlotProvisioningConfig::new(&slot).unwrap(),
        )
        .unwrap();
        let session = establish_persistent_source(
            authority,
            provisional,
            &source_url,
            &replication_url(&source_url),
            &activated,
        )
        .unwrap();
        let fixture = Self {
            source,
            table,
            publication,
            slot,
            source_id,
            control_url,
            control_pid,
        };
        (fixture, session)
    }

    fn control_client(&self) -> Client {
        Client::connect(&self.control_url, NoTls).unwrap()
    }

    /// The slot's `active_pid` once it is clear, or as it stands after
    /// `within`.
    fn slot_backend_after(&mut self, within: Duration) -> Option<i32> {
        let deadline = Instant::now() + within;
        loop {
            let slot_pid: Option<i32> = self
                .source
                .query_one(
                    "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot],
                )
                .unwrap()
                .get(0);
            if slot_pid.is_none() || Instant::now() >= deadline {
                return slot_pid;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Call after the session is dropped: a slot still in use cannot be
    /// dropped.
    fn remove(mut self) {
        assert_eq!(self.slot_backend_after(Duration::from_secs(5)), None);
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

/// A canonical append group that splits into several statements holds the
/// lease row for longer than the watchdog's 1 s `lock_timeout`. The watchdog
/// must keep the session through such a hold while the lease's TTL allows a
/// retry, and must still revoke, before the lease expires, a hold it cannot
/// outlast.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn the_session_watchdog_retries_a_locked_lease_row_until_the_ttl_no_longer_allows_it() {
    let (mut fixture, session) = WatchedSession::establish("retry", WATCHDOG_RETRY_TTL);
    let source_id = fixture.source_id.clone();
    let control_pid = fixture.control_pid;
    let mut observer = fixture.control_client();
    let mut holder = fixture.control_client();

    // Held from just after a renewal for 3.5 s: the attempt 2 s later waits
    // out its lock timeout, and the retry after it renews once the row is free.
    wait_for_renewal(&mut observer, &source_id);
    let hold = lock_lease_row(&mut holder, &source_id);
    let released_at = Instant::now() + Duration::from_millis(3_500);
    let mut waits = BTreeSet::new();
    while Instant::now() < released_at {
        waits.extend(waiting_renewal(&mut observer, control_pid));
        thread::sleep(Duration::from_millis(10));
    }
    hold.rollback().unwrap();
    assert!(
        waits.len() >= 2,
        "no renewal attempt waited out its lock timeout and retried: {waits:?}"
    );
    wait_for_renewal(&mut observer, &source_id);
    assert!(session.socket_is_open());

    // Held from just after a renewal until the watchdog gives up: it must
    // revoke while the lease is still unexpired in the control store, and end
    // the slot's backend.
    wait_for_renewal(&mut observer, &source_id);
    let hold = lock_lease_row(&mut holder, &source_id);
    let held_from = Instant::now();
    let mut waits = BTreeSet::new();
    while session.lease().is_locally_valid() {
        assert!(
            held_from.elapsed() < WATCHDOG_RETRY_TTL,
            "the watchdog kept a lease it could not renew"
        );
        waits.extend(waiting_renewal(&mut observer, control_pid));
        thread::sleep(Duration::from_millis(10));
    }
    let unexpired: bool = observer
        .query_one(
            "SELECT expires_at > clock_timestamp()
               FROM calque_control.source_fence_lease WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert!(
        unexpired,
        "the watchdog revoked only after the lease expired"
    );
    assert!(
        waits.len() >= 2,
        "the watchdog revoked without retrying: {waits:?}"
    );
    assert!(!session.socket_is_open());
    assert_eq!(fixture.slot_backend_after(Duration::from_secs(3)), None);
    hold.rollback().unwrap();

    drop(session);
    fixture.remove();
}

/// Closing a session stops its watchdog between renewal attempts. A session
/// closed while the lease row is held must not wait for the watchdog to use
/// up the TTL on retries: only the attempt already waiting on the row may
/// finish first.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn closing_a_session_stops_a_watchdog_that_is_retrying_a_locked_lease_row() {
    // The first attempt waits from 3.3 s after a renewal, and retrying would
    // go on for more than 4 s after the second attempt starts.
    let ttl = Duration::from_secs(10);
    let (fixture, session) = WatchedSession::establish("retry_stop", ttl);
    let source_id = fixture.source_id.clone();
    let mut observer = fixture.control_client();
    let mut holder = fixture.control_client();

    wait_for_renewal(&mut observer, &source_id);
    let hold = lock_lease_row(&mut holder, &source_id);
    let deadline = Instant::now() + ttl;
    let mut waits = BTreeSet::new();
    while waits.len() < 2 {
        assert!(Instant::now() < deadline, "the watchdog did not retry");
        waits.extend(waiting_renewal(&mut observer, fixture.control_pid));
        thread::sleep(Duration::from_millis(10));
    }

    let closing = Instant::now();
    drop(session);
    let closed_after = closing.elapsed();
    hold.rollback().unwrap();
    assert!(
        closed_after < Duration::from_millis(2_500),
        "closing waited {closed_after:?} for the watchdog"
    );
    fixture.remove();
}

/// Opens a transaction that writes to `table` and holds it for `open_for`,
/// once it holds a transaction id.
fn hold_open_write(
    source_url: &str,
    table: &str,
    open_for: Duration,
) -> (OpenTransaction, thread::JoinHandle<()>) {
    let (opened, opened_rx) = std::sync::mpsc::channel();
    let url = source_url.to_owned();
    let insert = format!("INSERT INTO public.{table} (id) VALUES (1)");
    let holder = thread::spawn(move || {
        let mut client = Client::connect(&url, NoTls).unwrap();
        let mut transaction = client.transaction().unwrap();
        transaction.batch_execute(&insert).unwrap();
        let row = transaction
            .query_one(
                "SELECT pid, usename::text, datname::text, xact_start::text
                   FROM pg_catalog.pg_stat_activity WHERE pid = pg_catalog.pg_backend_pid()",
                &[],
            )
            .unwrap();
        opened
            .send(OpenTransaction::Session {
                pid: row.get(0),
                role: row.get(1),
                database: row.get(2),
                started: row.get(3),
            })
            .unwrap();
        thread::sleep(open_for);
        transaction.rollback().unwrap();
    });
    let open = opened_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the holder session wrote in its transaction");
    (open, holder)
}

/// PostgreSQL waits for the transactions open when slot creation begins, then
/// for those open once they have ended, each on its own lock. The wait bound
/// covers them all: a transaction opened during the first wait, and still open
/// when the bound runs out, is named like one open from the start.
#[test]
#[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
fn slot_creation_bounds_its_wait_across_transactions_it_waits_for_in_turn() {
    let mut fixture = OpenTransactionFixture::create("slot_in_turn");
    let provisional = fixture
        .authority
        .acquire(acquire_request(
            &fixture.source_id,
            &fixture.epoch,
            "provisioner",
            Duration::from_secs(10),
        ))
        .unwrap();
    let bound = Duration::from_secs(4);
    let (_first, first_holder) = fixture.hold_open_write(Duration::from_secs(3));
    let (url, table) = (fixture.source_url.clone(), fixture.table.clone());
    let later = thread::spawn(move || {
        thread::sleep(Duration::from_secs(1));
        hold_open_write(&url, &table, Duration::from_secs(10))
    });
    let started = Instant::now();
    let refused = provision_persistent_slot(
        &fixture.authority,
        &provisional,
        &fixture.source_url,
        &fixture.activated,
        &SlotProvisioningConfig::new(&fixture.slot)
            .unwrap()
            .with_open_transaction_wait(bound),
    );
    let waited = started.elapsed();
    let (opened_later, later_holder) = later.join().unwrap();
    match refused {
        Err(SourceRuntimeError::SlotCreationWaitedForOpenTransactions { open, .. }) => {
            assert!(
                open.contains(&opened_later),
                "{opened_later} is missing from {open:?}"
            );
        }
        other => panic!("slot creation ended with {other:?}, not at the wait bound"),
    }
    assert!(
        waited >= bound && waited < bound + Duration::from_secs(2),
        "slot creation gave up after {waited:?}, not at the {bound:?} bound"
    );
    assert!(!fixture.slot_exists(), "a slot outlived the refusal");
    first_holder.join().unwrap();
    later_holder.join().unwrap();

    provision_persistent_slot(
        &fixture.authority,
        &provisional,
        &fixture.source_url,
        &fixture.activated,
        &SlotProvisioningConfig::new(&fixture.slot).unwrap(),
    )
    .expect("enrollment succeeds once both transactions end");
    fixture.authority.release(&provisional).unwrap();
    fixture.remove();
}
