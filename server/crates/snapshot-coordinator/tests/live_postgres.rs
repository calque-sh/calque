use futures_util::TryStreamExt;
use pg_walstream::CancellationToken;
use pgoutput_decoder::DecoderLimits;
use postgres::{Client as BlockingClient, NoTls as BlockingNoTls};
use snapshot_coordinator::{
    emit_cut_marker, import_snapshot, sample_snapshot_pressure, CoordinatorError, CopyRead,
    CutIdentity, ImportBarrier, SnapshotCoordinator, SnapshotDdlGuard, SnapshotPressureTarget,
    SnapshotStreamConfig,
};
use source_control::{inspect_source, EnrollmentRequirements, QualifiedTable, SchemaRegistry};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
use sync_types::TableId;
use tokio_postgres::{error::SqlState, Client, NoTls};
use uuid::Uuid;

/// Serialises the tests in this binary: they build the same fixture with
/// `DROP TABLE IF EXISTS` and `CREATE TABLE`, which race when run concurrently.
static FIXTURE_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The disposable database these tests run against. There is no fallback: the
/// compose file's `calque:calque@localhost` is the bootstrap superuser, which
/// enrollment refuses, and a guess could run against a database the operator
/// did not prepare.
fn regular_connection_string() -> String {
    std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .expect("set DATABASE_URL or TEST_DATABASE_URL to a disposable PostgreSQL 18 database")
}

fn replication_connection_string() -> String {
    let regular = regular_connection_string();
    if regular.contains('?') {
        format!("{regular}&replication=database&connect_timeout=5")
    } else {
        format!("{regular}?replication=database&connect_timeout=5")
    }
}

async fn connect(connection_string: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
        .await
        .expect("connect PostgreSQL client");
    tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });
    client
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires DATABASE_URL or TEST_DATABASE_URL and a live PostgreSQL server with wal_level=logical"]
async fn exported_snapshot_and_transactional_marker_define_a_temp_stream_cut() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let regular_dsn = regular_connection_string();
    let replication_dsn = replication_connection_string();
    let mut setup = connect(&regular_dsn).await;
    setup
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_pit_proof;
             DROP TABLE IF EXISTS calque_pit_tasks;
             CREATE TABLE calque_pit_tasks (
               id bigint PRIMARY KEY,
               value text NOT NULL
             );
             CREATE PUBLICATION calque_pit_proof
               FOR TABLE calque_pit_tasks;
             INSERT INTO calque_pit_tasks (id, value)
             VALUES (1, 'before');",
        )
        .await
        .expect("create PIT fixture");

    let slot_name = format!("snapshot_build_{}", Uuid::new_v4().simple());
    let config =
        SnapshotStreamConfig::new(&slot_name, "calque_pit_proof").expect("valid stream config");
    let registry_dsn = regular_dsn.clone();
    let registry = tokio::task::spawn_blocking(move || registry(&registry_dsn))
        .await
        .expect("join source enrollment");

    // PostgreSQL lock-queue fairness puts a compatible ACCESS SHARE request
    // behind an already queued ACCESS EXCLUSIVE request. The reservation must
    // fail immediately so orchestration can abort and restart from a new S.
    let contended_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .expect("acquire contention-test guard");
    let mut first_copy_client = connect(&regular_dsn).await;
    let first_reservation = contended_guard
        .reserve_copy(&mut first_copy_client)
        .await
        .expect("reserve first worker before DDL queues");
    let ddl_client = connect(&regular_dsn).await;
    let ddl_pid: i32 = ddl_client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let queued_ddl = tokio::spawn(async move {
        ddl_client
            .batch_execute("BEGIN; LOCK TABLE calque_pit_tasks IN ACCESS EXCLUSIVE MODE; ROLLBACK;")
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = setup
                .query_one(
                    "SELECT COALESCE(wait_event_type = 'Lock', false) \
                       FROM pg_catalog.pg_stat_activity WHERE pid = $1",
                    &[&ddl_pid],
                )
                .await
                .unwrap()
                .get(0);
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("DDL must queue behind the guard");
    let mut second_copy_client = connect(&regular_dsn).await;
    assert!(matches!(
        contended_guard.reserve_copy(&mut second_copy_client).await,
        Err(snapshot_coordinator::CoordinatorError::SnapshotCopyReservationContended)
    ));
    first_reservation.abort().await.unwrap();
    contended_guard.abort().await.unwrap();
    queued_ddl.await.unwrap();

    let first_contended_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .expect("acquire first-reservation contention guard");
    let ddl_client = connect(&regular_dsn).await;
    let ddl_pid: i32 = ddl_client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let queued_ddl = tokio::spawn(async move {
        ddl_client
            .batch_execute("BEGIN; LOCK TABLE calque_pit_tasks IN ACCESS EXCLUSIVE MODE; ROLLBACK;")
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = setup
                .query_one(
                    "SELECT COALESCE(wait_event_type = 'Lock', false) \
                       FROM pg_catalog.pg_stat_activity WHERE pid = $1",
                    &[&ddl_pid],
                )
                .await
                .unwrap()
                .get(0);
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("DDL must queue before the first reservation");
    let mut contended_copy_client = connect(&regular_dsn).await;
    assert!(matches!(
        first_contended_guard
            .reserve_copy(&mut contended_copy_client)
            .await,
        Err(snapshot_coordinator::CoordinatorError::SnapshotCopyReservationContended)
    ));
    first_contended_guard.abort().await.unwrap();
    queued_ddl.await.unwrap();

    let wrong_slot = format!("snapshot_build_{}", Uuid::new_v4().simple());
    let wrong_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .expect("lock enrolled tables");
    assert!(matches!(
        SnapshotCoordinator::export_controlled(
            &replication_dsn,
            &mut setup,
            &registry,
            wrong_guard,
            SnapshotStreamConfig::new(&wrong_slot, "other_publication").unwrap(),
            &CancellationToken::new(),
            Duration::from_secs(30),
        )
        .await,
        Err(snapshot_coordinator::CoordinatorError::RegistryPublicationMismatch)
    ));
    let ddl_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .expect("lock enrolled tables");
    let mut copy_client = connect(&regular_dsn).await;
    let copy_backend_pid: i32 = copy_client
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await
        .expect("inspect COPY backend PID")
        .get(0);
    let copy_reservation = ddl_guard
        .reserve_copy(&mut copy_client)
        .await
        .expect("reserve pre-locked COPY worker");
    let mut coordinator = SnapshotCoordinator::export_controlled(
        &replication_dsn,
        &mut setup,
        &registry,
        ddl_guard,
        config,
        &CancellationToken::new(),
        Duration::from_secs(30),
    )
    .await
    .expect("export temporary logical-slot snapshot");
    let exported = coordinator.exported_snapshot().clone();

    let table_id = registry.tables()[0].table_id.clone();
    let mut barrier = ImportBarrier::new(&exported, [("copy-0", [table_id.clone()])])
        .expect("create import barrier");
    let mut copy_worker = import_snapshot(copy_reservation, &exported, &mut barrier, "copy-0")
        .await
        .expect("import exported snapshot before exporter advances");
    let pressure_target =
        SnapshotPressureTarget::new(&slot_name, [copy_backend_pid]).expect("valid pressure target");
    let pressure = sample_snapshot_pressure(&setup, &pressure_target)
        .await
        .expect("sample temporary-slot and COPY transaction pressure");
    assert_eq!(pressure.copy_backend_count, 1);
    assert!(pressure.exporter_backend_pid > 0);
    assert!(pressure.oldest_copy_or_export_transaction_millis.is_some());
    assert!(matches!(
        pressure.wal_status,
        snapshot_coordinator::SnapshotWalStatus::Reserved
            | snapshot_coordinator::SnapshotWalStatus::Extended
    ));

    // This commits after S. It must be absent from the base COPY and present in
    // the temporary stream before the cut-marker commit.
    setup
        .execute(
            "INSERT INTO calque_pit_tasks (id, value) VALUES (2, 'after-snapshot')",
            &[],
        )
        .await
        .expect("commit post-snapshot row");
    coordinator
        .start_stream_after_import_controlled(
            &barrier,
            None,
            &CancellationToken::new(),
            Duration::from_secs(5),
        )
        .await
        .expect("start the temporary stream once the worker imported S");

    let identity = CutIdentity::generate();
    let emitted = emit_cut_marker(&mut setup, identity.clone())
        .await
        .expect("commit dedicated transactional cut marker");
    let mut applied_transactions = Vec::new();
    let observed = coordinator
        .roll_forward_to_cut_controlled(
            &emitted,
            Duration::from_secs(15),
            DecoderLimits::default(),
            &CancellationToken::new(),
            |transaction| {
                applied_transactions.push(transaction.xid);
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .await
        .expect("consume same temporary stream through marker commit");

    assert_eq!(observed.identity, identity);
    assert_eq!(observed.message_lsn, emitted.message_lsn);
    assert!(observed.commit_lsn >= observed.message_lsn);
    assert!(observed.end_lsn >= observed.commit_lsn);
    assert_eq!(observed.committed_application_transactions_before_cut, 1);
    assert_eq!(applied_transactions.len(), 1);

    let copy_sql =
        "COPY (SELECT id, value FROM calque_pit_tasks ORDER BY id) TO STDOUT WITH (FORMAT csv)";
    let copy = copy_worker
        .copy_table(table_id.clone(), copy_sql, CopyRead::Planned)
        .await
        .expect("COPY from imported snapshot");
    let stream = copy
        .into_stream_for(&exported, &table_id, copy_sql)
        .expect("consume exact COPY capability");
    futures_util::pin_mut!(stream);
    let mut base = Vec::new();
    while let Some(chunk) = stream.try_next().await.expect("read COPY stream") {
        base.extend_from_slice(&chunk);
    }
    assert_eq!(base, b"1,before\n");

    let completion = copy_worker.commit().await.expect("finish COPY transaction");
    barrier
        .record_completion(completion)
        .expect("record successful COPY completion");

    drop(coordinator);
    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let slot_still_exists: bool = setup
            .query_one(
                "SELECT EXISTS (
               SELECT 1 FROM pg_replication_slots WHERE slot_name = $1
             )",
                &[&slot_name],
            )
            .await
            .expect("inspect temporary slot cleanup")
            .get(0);
        if !slot_still_exists {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "temporary slot must be removed when its session closes"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    setup
        .batch_execute(
            "DROP PUBLICATION calque_pit_proof;
             DROP TABLE calque_pit_tasks;",
        )
        .await
        .expect("clean PIT fixture");
}

/// The direct-columnar build starts the temporary stream once its COPY worker
/// has imported S, rolls forward to the cut, and only then copies. The worker
/// must still read S, the worker's own locks must still exclude rewriting DDL
/// after the guard is released, and the held stream must outlive a
/// `wal_sender_timeout` while COPY runs.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires DATABASE_URL or TEST_DATABASE_URL and a live PostgreSQL server with wal_level=logical"]
async fn copy_after_the_cut_reads_the_exported_snapshot_while_the_stream_is_held() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let regular_dsn = regular_connection_string();
    // One second: a hold that stops answering keepalives loses the slot well
    // inside the three seconds the COPY below waits.
    let replication_dsn = format!(
        "{}&options=-c%20wal_sender_timeout%3D1000",
        replication_connection_string()
    );
    let mut setup = connect(&regular_dsn).await;
    setup
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_pit_proof;
             DROP TABLE IF EXISTS calque_pit_tasks;
             CREATE TABLE calque_pit_tasks (
               id bigint PRIMARY KEY,
               value text NOT NULL
             );
             CREATE PUBLICATION calque_pit_proof
               FOR TABLE calque_pit_tasks;
             INSERT INTO calque_pit_tasks (id, value)
             VALUES (1, 'before');",
        )
        .await
        .expect("create PIT fixture");
    let slot_name = format!("snapshot_build_{}", Uuid::new_v4().simple());
    let registry_dsn = regular_dsn.clone();
    let registry = tokio::task::spawn_blocking(move || registry(&registry_dsn))
        .await
        .expect("join source enrollment");

    let ddl_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .expect("lock enrolled tables");
    let mut copy_client = connect(&regular_dsn).await;
    let copy_reservation = ddl_guard
        .reserve_copy(&mut copy_client)
        .await
        .expect("reserve pre-locked COPY worker");
    let mut coordinator = SnapshotCoordinator::export_controlled(
        &replication_dsn,
        &mut setup,
        &registry,
        ddl_guard,
        SnapshotStreamConfig::new(&slot_name, "calque_pit_proof").unwrap(),
        &CancellationToken::new(),
        Duration::from_secs(30),
    )
    .await
    .expect("export temporary logical-slot snapshot");
    let exported = coordinator.exported_snapshot().clone();
    let table_id = registry.tables()[0].table_id.clone();
    let mut barrier = ImportBarrier::new(&exported, [("copy-0", [table_id.clone()])]).unwrap();
    assert!(matches!(
        coordinator
            .start_stream_after_import_controlled(
                &barrier,
                None,
                &CancellationToken::new(),
                Duration::from_secs(5),
            )
            .await,
        Err(CoordinatorError::ImportBarrierIncomplete(_))
    ));
    let mut copy_worker = import_snapshot(copy_reservation, &exported, &mut barrier, "copy-0")
        .await
        .expect("import exported snapshot");
    coordinator
        .start_stream_after_import_controlled(
            &barrier,
            None,
            &CancellationToken::new(),
            Duration::from_secs(5),
        )
        .await
        .expect("start the stream once every worker imported S");
    let rewrite = setup
        .batch_execute(
            "BEGIN; LOCK TABLE calque_pit_tasks IN ACCESS EXCLUSIVE MODE NOWAIT; ROLLBACK;",
        )
        .await
        .expect_err("the imported worker still excludes rewriting DDL");
    assert_eq!(rewrite.code(), Some(&SqlState::LOCK_NOT_AVAILABLE));
    setup.batch_execute("ROLLBACK").await.unwrap();
    assert!(matches!(
        coordinator
            .hold_after_cut_controlled(&CancellationToken::new(), &CancellationToken::new())
            .await,
        Err(CoordinatorError::CutNotObserved)
    ));

    setup
        .execute(
            "INSERT INTO calque_pit_tasks (id, value) VALUES (2, 'before-cut')",
            &[],
        )
        .await
        .unwrap();
    let emitted = emit_cut_marker(&mut setup, CutIdentity::generate())
        .await
        .expect("commit cut marker");
    let mut applied = 0;
    coordinator
        .roll_forward_to_cut_controlled(
            &emitted,
            Duration::from_secs(15),
            DecoderLimits::default(),
            &CancellationToken::new(),
            |_| {
                applied += 1;
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .await
        .expect("roll forward to the cut before any COPY");
    assert_eq!(applied, 1);
    setup
        .execute(
            "INSERT INTO calque_pit_tasks (id, value) VALUES (3, 'after-cut')",
            &[],
        )
        .await
        .unwrap();

    let stop = CancellationToken::new();
    let never = CancellationToken::new();
    let copy_sql =
        "COPY (SELECT id, value FROM calque_pit_tasks ORDER BY id) TO STDOUT WITH (FORMAT csv)";
    let (held, base) = tokio::join!(
        coordinator.hold_after_cut_controlled(&stop, &never),
        async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let copy = copy_worker
                .copy_table(table_id.clone(), copy_sql, CopyRead::Planned)
                .await
                .expect("COPY from imported snapshot");
            let stream = copy
                .into_stream_for(&exported, &table_id, copy_sql)
                .expect("consume exact COPY capability");
            futures_util::pin_mut!(stream);
            let mut base = Vec::new();
            while let Some(chunk) = stream.try_next().await.expect("read COPY stream") {
                base.extend_from_slice(&chunk);
            }
            stop.cancel();
            base
        }
    );
    held.expect("hold the stream while COPY runs");
    assert_eq!(base, b"1,before\n");
    let active: bool = setup
        .query_one(
            "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .expect("the held slot outlived wal_sender_timeout")
        .get(0);
    assert!(active);
    barrier
        .record_completion(copy_worker.commit().await.unwrap())
        .unwrap();

    coordinator.abort().await.unwrap();
    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    while setup
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    {
        assert!(Instant::now() < cleanup_deadline, "temporary slot must go");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    setup
        .batch_execute(
            "DROP PUBLICATION calque_pit_proof;
             DROP TABLE calque_pit_tasks;",
        )
        .await
        .expect("clean PIT fixture");
}

/// Drives one attempt at the timed-out-start scenario.
///
/// Returns `false` when the replication start beat the deadline, which leaves
/// the revocation path unexercised; see the caller for why that is retried
/// rather than asserted away.
async fn attempt_timed_out_start_revocation() -> bool {
    let regular_dsn = regular_connection_string();
    let replication_dsn = replication_connection_string();
    let mut setup = connect(&regular_dsn).await;
    setup
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_pit_proof;
             DROP TABLE IF EXISTS calque_pit_tasks;
             CREATE TABLE calque_pit_tasks (
               id bigint PRIMARY KEY,
               value text NOT NULL
             );
             INSERT INTO calque_pit_tasks VALUES (1, 'before');
             CREATE PUBLICATION calque_pit_proof FOR TABLE calque_pit_tasks;",
        )
        .await
        .unwrap();
    let registry_dsn = regular_dsn.clone();
    let registry = tokio::task::spawn_blocking(move || registry(&registry_dsn))
        .await
        .unwrap();
    let ddl_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .unwrap();
    let mut copy_client = connect(&regular_dsn).await;
    let reservation = ddl_guard.reserve_copy(&mut copy_client).await.unwrap();
    let slot_name = format!("snapshot_build_{}", Uuid::new_v4().simple());
    let mut coordinator = SnapshotCoordinator::export_controlled(
        &replication_dsn,
        &mut setup,
        &registry,
        ddl_guard,
        SnapshotStreamConfig::new(&slot_name, "calque_pit_proof").unwrap(),
        &CancellationToken::new(),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let exporter_pid: i32 = setup
        .query_one(
            "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .unwrap()
        .get(0);
    let exported = coordinator.exported_snapshot().clone();
    let table_id = registry.tables()[0].table_id.clone();
    let mut barrier =
        ImportBarrier::new(&exported, [("copy-local-abort", [table_id.clone()])]).unwrap();
    let mut worker = import_snapshot(reservation, &exported, &mut barrier, "copy-local-abort")
        .await
        .unwrap();
    let copy_sql =
        "COPY (SELECT id, value FROM calque_pit_tasks ORDER BY id) TO STDOUT WITH (FORMAT csv)";
    let copy = worker
        .copy_table(table_id.clone(), copy_sql, CopyRead::Planned)
        .await
        .unwrap();
    let stream = copy
        .into_stream_for(&exported, &table_id, copy_sql)
        .unwrap();
    futures_util::pin_mut!(stream);
    while stream.try_next().await.unwrap().is_some() {}
    barrier
        .record_completion(worker.commit().await.unwrap())
        .unwrap();

    let cancellation = tokio_util::sync::CancellationToken::new();
    let timed_out = coordinator
        .start_stream_after_import_controlled(
            &barrier,
            None,
            &cancellation,
            Duration::from_nanos(1),
        )
        .await;
    match timed_out {
        Err(snapshot_coordinator::CoordinatorError::ReplicationOperationTimedOut) => {}
        // The replication start completed before the deadline fired, and work
        // that finished is not cancelled. This attempt did not reach the
        // revocation path, so it tears its fixture down and the caller retries.
        Ok(()) => {
            drop(coordinator);
            setup
                .batch_execute(
                    "DROP PUBLICATION IF EXISTS calque_pit_proof;
                     DROP TABLE IF EXISTS calque_pit_tasks;",
                )
                .await
                .unwrap();
            return false;
        }
        other => panic!("expected a replication deadline outcome, got {other:?}"),
    }
    drop(coordinator);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let residue: bool = setup
                .query_one(
                    "SELECT EXISTS(
                         SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1
                         UNION ALL
                         SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $2
                     )",
                    &[&exporter_pid, &slot_name],
                )
                .await
                .unwrap()
                .get(0);
            if !residue {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("local abort must join the owner and drop its temporary slot");
    setup
        .batch_execute(
            "DROP PUBLICATION calque_pit_proof;
             DROP TABLE calque_pit_tasks;",
        )
        .await
        .unwrap();
    true
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires DATABASE_URL or TEST_DATABASE_URL and a live PostgreSQL server with wal_level=logical"]
async fn local_socket_revocation_joins_timed_out_start_without_control_client() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    // A 1 ns deadline cannot be made to win outright: tokio's timer has
    // millisecond granularity, so the sleep is not ready on the first poll,
    // while a local `START_REPLICATION` sometimes completes inside that window.
    // Constraining the blocking pool to slow the start would deadlock, since
    // `join_revoked_replication_task` needs a second blocking thread to shut
    // down the socket the first one waits on. So the scenario is retried until
    // the deadline wins; each attempt builds its own fixture and slot name.
    const ATTEMPTS: usize = 25;
    for _ in 0..ATTEMPTS {
        if attempt_timed_out_start_revocation().await {
            return;
        }
    }
    panic!(
        "start_stream_after_import_controlled never reported \
         ReplicationOperationTimedOut in {ATTEMPTS} attempts"
    );
}

/// Each COPY capability is bound to one registry, one worker's tables and one
/// statement. A guard or a reservation taken over another registry, a table
/// the worker was not assigned or has already copied, and a stream presented
/// for another table are each refused, and the refused import records no
/// worker.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires DATABASE_URL or TEST_DATABASE_URL and a live PostgreSQL server with wal_level=logical"]
async fn a_copy_bound_to_another_registry_or_table_is_refused() {
    let _fixture_guard = FIXTURE_GUARD.lock().await;
    let regular_dsn = regular_connection_string();
    let replication_dsn = replication_connection_string();
    let mut setup = connect(&regular_dsn).await;
    setup
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_pit_proof;
             DROP PUBLICATION IF EXISTS calque_pit_other;
             DROP TABLE IF EXISTS calque_pit_tasks;
             CREATE TABLE calque_pit_tasks (
               id bigint PRIMARY KEY,
               value text NOT NULL
             );
             INSERT INTO calque_pit_tasks VALUES (1, 'before');
             CREATE PUBLICATION calque_pit_proof FOR TABLE calque_pit_tasks;
             CREATE PUBLICATION calque_pit_other FOR TABLE calque_pit_tasks;",
        )
        .await
        .unwrap();
    let registry_dsn = regular_dsn.clone();
    let (registry, other_registry) = tokio::task::spawn_blocking(move || {
        (
            registry(&registry_dsn),
            registry_for(&registry_dsn, "calque_pit_other"),
        )
    })
    .await
    .unwrap();
    let slot_name = format!("snapshot_build_{}", Uuid::new_v4().simple());
    let config = || SnapshotStreamConfig::new(&slot_name, "calque_pit_proof").unwrap();

    let other_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &other_registry)
        .await
        .unwrap();
    let mut other_copy_client = connect(&regular_dsn).await;
    let other_reservation = other_guard
        .reserve_copy(&mut other_copy_client)
        .await
        .unwrap();
    assert!(matches!(
        SnapshotCoordinator::export_controlled(
            &replication_dsn,
            &mut setup,
            &registry,
            other_guard,
            config(),
            &CancellationToken::new(),
            Duration::from_secs(30),
        )
        .await,
        Err(CoordinatorError::SnapshotDdlGuardMismatch)
    ));

    let ddl_guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .unwrap();
    let mut copy_client = connect(&regular_dsn).await;
    let reservation = ddl_guard.reserve_copy(&mut copy_client).await.unwrap();
    let coordinator = SnapshotCoordinator::export_controlled(
        &replication_dsn,
        &mut setup,
        &registry,
        ddl_guard,
        config(),
        &CancellationToken::new(),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let exported = coordinator.exported_snapshot().clone();
    let table_id = registry.tables()[0].table_id.clone();
    let absent = TableId::new("table/absent").unwrap();
    let mut barrier = ImportBarrier::new(&exported, [("copy-0", [table_id.clone()])]).unwrap();
    assert!(matches!(
        import_snapshot(other_reservation, &exported, &mut barrier, "copy-0").await,
        Err(CoordinatorError::SnapshotCopyReservationMismatch)
    ));
    let mut worker = import_snapshot(reservation, &exported, &mut barrier, "copy-0")
        .await
        .unwrap();

    let copy_sql =
        "COPY (SELECT id, value FROM calque_pit_tasks ORDER BY id) TO STDOUT WITH (FORMAT csv)";
    assert!(matches!(
        worker.copy_table(absent.clone(), copy_sql, CopyRead::Planned).await,
        Err(CoordinatorError::UnassignedOrDuplicateTableCopy(table)) if table == absent
    ));
    let copy = worker
        .copy_table(table_id.clone(), copy_sql, CopyRead::Planned)
        .await
        .unwrap();
    assert!(matches!(
        worker.copy_table(table_id.clone(), copy_sql, CopyRead::Planned).await,
        Err(CoordinatorError::UnassignedOrDuplicateTableCopy(table)) if table == table_id
    ));
    assert!(matches!(
        copy.into_stream_for(&exported, &absent, copy_sql),
        Err(CoordinatorError::CopyCapabilityMismatch)
    ));
    worker.abort().await.unwrap();

    drop(coordinator);
    setup
        .batch_execute(
            "DROP PUBLICATION calque_pit_proof;
             DROP PUBLICATION calque_pit_other;
             DROP TABLE calque_pit_tasks;",
        )
        .await
        .unwrap();
}

fn registry(database_url: &str) -> SchemaRegistry {
    registry_for(database_url, "calque_pit_proof")
}

/// The registry of `calque_pit_tasks` enrolled through `publication`.
fn registry_for(database_url: &str, publication: &str) -> SchemaRegistry {
    let mut client = BlockingClient::connect(database_url, BlockingNoTls).unwrap();
    let tables = BTreeSet::from([QualifiedTable {
        schema: "public".to_owned(),
        table: "calque_pit_tasks".to_owned(),
    }]);
    let snapshot = inspect_source(&mut client, publication, &tables).unwrap();
    let requirements = EnrollmentRequirements {
        expected_identity: snapshot.identity.clone(),
        publication_name: publication.to_owned(),
        tables,
        pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
    };
    SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
}

/// The guard and a COPY reservation lock every enrolled relation by a name
/// they quote themselves, so a table whose name needs quoting is locked.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires DATABASE_URL or TEST_DATABASE_URL and a live PostgreSQL server with wal_level=logical"]
async fn the_guard_and_a_reservation_lock_a_table_whose_name_needs_quoting() {
    let regular_dsn = regular_connection_string();
    let setup = connect(&regular_dsn).await;
    setup
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_quoted_proof;
             DROP TABLE IF EXISTS public.\"Calque \"\"Quoted\"\" Tasks\";
             CREATE TABLE public.\"Calque \"\"Quoted\"\" Tasks\" (id bigint PRIMARY KEY);
             CREATE PUBLICATION calque_quoted_proof
               FOR TABLE public.\"Calque \"\"Quoted\"\" Tasks\";",
        )
        .await
        .expect("create quoted-name fixture");
    let registry_dsn = regular_dsn.clone();
    let registry = tokio::task::spawn_blocking(move || {
        let mut client = BlockingClient::connect(&registry_dsn, BlockingNoTls).unwrap();
        let tables = BTreeSet::from([QualifiedTable {
            schema: "public".to_owned(),
            table: "Calque \"Quoted\" Tasks".to_owned(),
        }]);
        let snapshot = inspect_source(&mut client, "calque_quoted_proof", &tables).unwrap();
        let requirements = EnrollmentRequirements {
            expected_identity: snapshot.identity.clone(),
            publication_name: "calque_quoted_proof".to_owned(),
            tables,
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        };
        SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
    })
    .await
    .expect("join source enrollment");

    let guard_client = connect(&regular_dsn).await;
    let guard_pid = backend_pid(&guard_client).await;
    let guard = SnapshotDdlGuard::acquire(guard_client, &registry)
        .await
        .expect("acquire guard over a quoted name");
    let mut copy_client = connect(&regular_dsn).await;
    let copy_pid = backend_pid(&copy_client).await;
    let reservation = guard
        .reserve_copy(&mut copy_client)
        .await
        .expect("reserve a worker over a quoted name");
    let holders: Vec<i32> = setup
        .query(
            "SELECT pid FROM pg_catalog.pg_locks
              WHERE locktype = 'relation' AND granted AND mode = 'AccessShareLock'
                AND relation = 'public.\"Calque \"\"Quoted\"\" Tasks\"'::regclass
              ORDER BY pid",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    let mut expected = vec![guard_pid, copy_pid];
    expected.sort_unstable();
    assert_eq!(holders, expected);

    reservation.abort().await.unwrap();
    guard.abort().await.unwrap();
    setup
        .batch_execute(
            "DROP PUBLICATION calque_quoted_proof;
             DROP TABLE public.\"Calque \"\"Quoted\"\" Tasks\";",
        )
        .await
        .expect("remove quoted-name fixture");
}

/// A key-index read runs with sequential scans, bitmap scans, sorts and JIT
/// off, and the planned read after it on the same worker with the server's
/// own settings.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires DATABASE_URL or TEST_DATABASE_URL and a live PostgreSQL server with wal_level=logical"]
async fn a_key_index_read_turns_off_what_the_planned_read_after_it_keeps() {
    let regular_dsn = regular_connection_string();
    let replication_dsn = replication_connection_string();
    let mut setup = connect(&regular_dsn).await;
    setup
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_read_proof;
             DROP TABLE IF EXISTS calque_read_keyed, calque_read_shared;
             CREATE TABLE calque_read_keyed (
               tenant_id bigint, id bigint, PRIMARY KEY (tenant_id, id)
             );
             CREATE TABLE calque_read_shared (id bigint PRIMARY KEY);
             INSERT INTO calque_read_keyed VALUES (1, 1);
             INSERT INTO calque_read_shared VALUES (1);
             CREATE PUBLICATION calque_read_proof
               FOR TABLE calque_read_keyed, calque_read_shared;",
        )
        .await
        .expect("create read-mode fixture");
    let registry_dsn = regular_dsn.clone();
    let registry = tokio::task::spawn_blocking(move || {
        let mut client = BlockingClient::connect(&registry_dsn, BlockingNoTls).unwrap();
        let tables = BTreeSet::from(["calque_read_keyed", "calque_read_shared"].map(|table| {
            QualifiedTable {
                schema: "public".to_owned(),
                table: table.to_owned(),
            }
        }));
        let snapshot = inspect_source(&mut client, "calque_read_proof", &tables).unwrap();
        let requirements = EnrollmentRequirements {
            expected_identity: snapshot.identity.clone(),
            publication_name: "calque_read_proof".to_owned(),
            tables,
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        };
        SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
    })
    .await
    .expect("join source enrollment");

    let guard = SnapshotDdlGuard::acquire(connect(&regular_dsn).await, &registry)
        .await
        .unwrap();
    let mut copy_client = connect(&regular_dsn).await;
    let reservation = guard.reserve_copy(&mut copy_client).await.unwrap();
    let slot_name = format!("snapshot_build_{}", Uuid::new_v4().simple());
    let coordinator = SnapshotCoordinator::export_controlled(
        &replication_dsn,
        &mut setup,
        &registry,
        guard,
        SnapshotStreamConfig::new(&slot_name, "calque_read_proof").unwrap(),
        &CancellationToken::new(),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let exported = coordinator.exported_snapshot().clone();
    let [keyed, shared] = [0, 1].map(|index| registry.tables()[index].table_id.clone());
    let mut barrier =
        ImportBarrier::new(&exported, [("copy-0", [keyed.clone(), shared.clone()])]).unwrap();
    let mut worker = import_snapshot(reservation, &exported, &mut barrier, "copy-0")
        .await
        .unwrap();

    let settings = "current_setting('enable_seqscan'), current_setting('enable_bitmapscan'),
                    current_setting('enable_sort'), current_setting('jit')";
    let copy_sql = format!(
        "COPY (SELECT {settings} FROM calque_read_keyed ORDER BY tenant_id, id)
         TO STDOUT WITH (FORMAT csv)"
    );
    let mut read = Vec::new();
    for (table_id, mode) in [(&keyed, CopyRead::KeyIndex), (&shared, CopyRead::Planned)] {
        let copy = worker
            .copy_table(table_id.clone(), &copy_sql, mode)
            .await
            .unwrap();
        let stream = copy
            .into_stream_for(&exported, table_id, &copy_sql)
            .unwrap();
        futures_util::pin_mut!(stream);
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.try_next().await.unwrap() {
            bytes.extend_from_slice(&chunk);
        }
        read.push(String::from_utf8(bytes).unwrap());
    }
    worker.commit().await.unwrap();
    let defaults = setup
        .query_one(&format!("SELECT concat_ws(',', {settings})"), &[])
        .await
        .unwrap()
        .get::<_, String>(0);

    assert!(
        !defaults.contains("off"),
        "the server's own settings must leave all four on: {defaults}"
    );
    assert_eq!(
        read,
        ["off,off,off,off\n".to_owned(), format!("{defaults}\n")]
    );

    drop(coordinator);
    setup
        .batch_execute(
            "DROP PUBLICATION calque_read_proof;
             DROP TABLE calque_read_keyed, calque_read_shared;",
        )
        .await
        .expect("remove read-mode fixture");
}

async fn backend_pid(client: &Client) -> i32 {
    client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0)
}
