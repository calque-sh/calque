use std::{
    env,
    sync::{mpsc, Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls, Transaction};
use source_runtime::{
    AcquireRequest, FenceAuthority, FenceError, FenceStore, LeasePhase, OwnerId, PostgresFenceStore,
};
use sync_types::{SourceEpoch, SourceId};

fn database_url() -> Option<String> {
    env::var("CALQUE_CONTROL_DATABASE_URL")
        .ok()
        .or_else(|| env::var("TEST_DATABASE_URL").ok())
}

/// Process-wide ordinal that makes generated identifiers unique between tests
/// running in parallel within one binary. `SystemTime::now()` has only
/// microsecond granularity on some platforms, macOS included, so pid and clock
/// alone can collide.
fn next_id_ordinal() -> u64 {
    static ORDINAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    ORDINAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn unique_source(prefix: &str) -> SourceId {
    SourceId::new(format!(
        "{prefix}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        next_id_ordinal()
    ))
    .unwrap()
}

fn request(source_id: &SourceId, epoch: &SourceEpoch, owner: &str, ttl_ms: u64) -> AcquireRequest {
    AcquireRequest {
        source_id: source_id.clone(),
        source_epoch: epoch.clone(),
        owner_id: OwnerId::new(owner).unwrap(),
        ttl: Duration::from_millis(ttl_ms),
    }
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn two_clients_enforce_monotonic_takeover_and_exact_ownership() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-takeover");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let store_a = PostgresFenceStore::connect(&url).unwrap();
    let store_b = PostgresFenceStore::connect(&url).unwrap();
    store_a.enroll_source(&source_id, &epoch).unwrap();
    let authority_a = FenceAuthority::new(store_a);
    let authority_b = FenceAuthority::new(store_b);

    let lease_a = authority_a
        .acquire(request(&source_id, &epoch, "runtime-a", 100))
        .unwrap();
    assert!(matches!(
        authority_b.acquire(request(&source_id, &epoch, "runtime-b", 500)),
        Err(FenceError::LeaseHeld { .. })
    ));

    thread::sleep(Duration::from_millis(140));
    let lease_b = authority_b
        .acquire(request(&source_id, &epoch, "runtime-b", 500))
        .unwrap();
    assert!(lease_b.identity().token() > lease_a.identity().token());
    let stale_identity = lease_a.identity().clone();
    let stale_store = PostgresFenceStore::connect(&url).unwrap();
    assert!(matches!(
        stale_store.validate(&stale_identity),
        Err(FenceError::LeaseLost)
    ));
    assert!(matches!(
        stale_store.renew(&stale_identity, Duration::from_secs(1)),
        Err(FenceError::LeaseLost)
    ));
    assert!(matches!(
        stale_store.bind_backend(&stale_identity, 1234),
        Err(FenceError::LeaseLost)
    ));
    assert!(matches!(
        stale_store.release(&stale_identity),
        Err(FenceError::LeaseLost)
    ));
    assert!(matches!(
        authority_a.validate(&lease_a),
        Err(FenceError::LeaseLost)
    ));
    assert!(!lease_a.is_locally_valid());
    assert!(matches!(
        authority_a.renew(&lease_a, Duration::from_secs(1)),
        Err(FenceError::LeaseLost)
    ));
    assert!(matches!(
        authority_a.bind_backend(&lease_a, 1234),
        Err(FenceError::LeaseLost)
    ));
    assert!(matches!(
        authority_a.release(&lease_a),
        Err(FenceError::LeaseLost)
    ));

    let snapshot = authority_b.validate(&lease_b).unwrap();
    assert_eq!(snapshot.phase, LeasePhase::Provisional);
    assert_eq!(snapshot.source_backend_pid, None);
    authority_b.release(&lease_b).unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn reconnect_preserves_tokens_and_epoch_and_missing_rows_fail_closed() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-reconnect");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let first_token = {
        let store = PostgresFenceStore::connect(&url).unwrap();
        store.enroll_source(&source_id, &epoch).unwrap();
        let authority = FenceAuthority::new(store);
        let lease = authority
            .acquire(request(&source_id, &epoch, "before-restart", 500))
            .unwrap();
        let token = lease.identity().token();
        authority.release(&lease).unwrap();
        token
    };

    let store = PostgresFenceStore::connect(&url).unwrap();
    let authority = FenceAuthority::new(store);
    let lease = authority
        .acquire(request(&source_id, &epoch, "after-restart", 500))
        .unwrap();
    assert!(lease.identity().token() > first_token);
    authority.release(&lease).unwrap();

    let missing = unique_source("fence-missing");
    assert!(matches!(
        authority.acquire(request(&missing, &epoch, "owner", 500)),
        Err(FenceError::SourceNotEnrolled(id)) if id == missing
    ));
    assert!(matches!(
        authority.acquire(request(
            &source_id,
            &SourceEpoch::new("wrong-epoch").unwrap(),
            "owner",
            500,
        )),
        Err(FenceError::EpochMismatch { .. })
    ));
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn backend_binding_preserves_provisional_phase() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-phase");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let store = PostgresFenceStore::connect(&url).unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let snapshot = store
        .acquire(&request(&source_id, &epoch, "runtime-a", 1_000))
        .unwrap();
    let identity = snapshot.identity();
    let bound = store.bind_backend(&identity, 7_654).unwrap();
    assert_eq!(bound.phase, LeasePhase::Provisional);
    assert_eq!(bound.source_backend_pid, Some(7_654));

    let mut direct = Client::connect(&url, NoTls).unwrap();
    let row = direct
        .query_one(
            "SELECT lease_phase, source_backend_pid FROM calque_control.source_fence_lease WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "provisional");
    assert_eq!(row.get::<_, Option<i32>>(1), Some(7_654));
    store.release(&identity).unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn reacquire_takes_back_only_this_owners_lease() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-reacquire");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let store = PostgresFenceStore::connect(&url).unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let authority_a = FenceAuthority::new(store);
    let ttl = Duration::from_secs(5);

    // The lease is still held and unexpired, as it is when PostgreSQL restarts
    // under a running session: `acquire` refuses, `reacquire` takes it back
    // from a fresh control connection with a higher token.
    let original = authority_a
        .acquire(request(&source_id, &epoch, "runtime-a", 5_000))
        .unwrap();
    let restarted = FenceAuthority::new(PostgresFenceStore::connect(&url).unwrap());
    assert!(matches!(
        restarted.acquire(request(&source_id, &epoch, "runtime-a", 5_000)),
        Err(FenceError::LeaseHeld { .. })
    ));
    let taken_back = restarted.reacquire(original.identity(), ttl).unwrap();
    assert!(taken_back.identity().token() > original.identity().token());
    assert_eq!(taken_back.identity().owner_id().as_str(), "runtime-a");
    assert_eq!(
        restarted.validate(&taken_back).unwrap().phase,
        LeasePhase::Provisional
    );
    assert!(matches!(
        authority_a.validate(&original),
        Err(FenceError::LeaseLost)
    ));

    // A reacquire whose reply was lost leaves a token the caller never saw.
    // The row still names this owner, so the older identity may take it.
    let after_lost_reply = restarted.reacquire(original.identity(), ttl).unwrap();
    assert!(after_lost_reply.identity().token() > taken_back.identity().token());

    // A released lease keeps its token and can be taken back by it.
    restarted.release(&after_lost_reply).unwrap();
    let after_release = restarted
        .reacquire(after_lost_reply.identity(), ttl)
        .unwrap();
    restarted.release(&after_release).unwrap();

    // Once another owner has held the source, neither this owner's token nor
    // its name gives the lease back, whether that owner still holds it or has
    // released it. The refused attempts leave the other owner's row intact.
    let authority_b = FenceAuthority::new(PostgresFenceStore::connect(&url).unwrap());
    let lease_b = authority_b
        .acquire(request(&source_id, &epoch, "runtime-b", 5_000))
        .unwrap();
    assert!(matches!(
        restarted.reacquire(after_release.identity(), ttl),
        Err(FenceError::LeaseSuperseded)
    ));
    assert!(matches!(
        restarted.reacquire(original.identity(), ttl),
        Err(FenceError::LeaseSuperseded)
    ));
    let held = authority_b.validate(&lease_b).unwrap();
    assert_eq!(held.token, lease_b.identity().token());
    assert_eq!(held.owner_id.as_str(), "runtime-b");
    authority_b.release(&lease_b).unwrap();
    assert!(matches!(
        restarted.reacquire(after_release.identity(), ttl),
        Err(FenceError::LeaseSuperseded)
    ));
    // The refusal is about ownership, not about the row being released: the
    // other owner's identity still takes its own released lease back.
    let lease_b_again = restarted.reacquire(lease_b.identity(), ttl).unwrap();
    assert_eq!(lease_b_again.identity().owner_id().as_str(), "runtime-b");
    restarted.release(&lease_b_again).unwrap();

    let mut direct = Client::connect(&url, NoTls).unwrap();
    direct
        .execute(
            "UPDATE calque_control.source_fence_lease SET source_epoch = 'epoch-2' WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert!(matches!(
        restarted.reacquire(lease_b.identity(), ttl),
        Err(FenceError::EpochMismatch { .. })
    ));
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn simultaneous_acquire_has_exactly_one_winner() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-race");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let setup = PostgresFenceStore::connect(&url).unwrap();
    setup.enroll_source(&source_id, &epoch).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for owner in ["racer-a", "racer-b"] {
        let url = url.clone();
        let source_id = source_id.clone();
        let epoch = epoch.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let store = PostgresFenceStore::connect(&url).unwrap();
            barrier.wait();
            store.acquire(&request(&source_id, &epoch, owner, 1_000))
        }));
    }
    barrier.wait();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(FenceError::LeaseHeld { .. })))
            .count(),
        1
    );
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    setup.release(&winner.identity()).unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn uncertain_control_store_result_revokes_the_local_guard() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-uncertain");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let store = PostgresFenceStore::connect(&url).unwrap();
    let control_pid = store.control_backend_pid().unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let authority = FenceAuthority::new(store);
    let lease = authority
        .acquire(request(&source_id, &epoch, "runtime-a", 1_000))
        .unwrap();

    let mut killer = Client::connect(&url, NoTls).unwrap();
    assert!(killer
        .query_one("SELECT pg_terminate_backend($1)", &[&control_pid])
        .unwrap()
        .get::<_, bool>(0));
    assert!(matches!(
        authority.validate(&lease),
        Err(FenceError::ControlStoreUnavailable)
    ));
    assert!(!lease.is_locally_valid());
    assert!(matches!(
        authority.renew(&lease, Duration::from_secs(1)),
        Err(FenceError::LeaseLost)
    ));
}

/// Locks the source's lease row from another connection, as a canonical
/// append group that splits into several statements does, until the returned
/// transaction ends.
fn lock_lease_row<'a>(client: &'a mut Client, source_id: &SourceId) -> Transaction<'a> {
    let mut transaction = client.transaction().unwrap();
    transaction
        .query_one(
            "SELECT 1 FROM calque_control.source_fence_lease WHERE source_id = $1 FOR UPDATE",
            &[&source_id.as_str()],
        )
        .unwrap();
    transaction
}

/// Only a wait that runs out the control connection's `lock_timeout` is a
/// locked row, which the active watchdog retries. A connection lost during
/// the same wait is an uncertain result.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn a_lease_row_lock_that_outlasts_lock_timeout_is_reported_as_a_locked_row() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-row-locked");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let store = PostgresFenceStore::connect(&url).unwrap();
    let control_pid = store.control_backend_pid().unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let identity = store
        .acquire(&request(&source_id, &epoch, "runtime-a", 30_000))
        .unwrap()
        .identity();
    let ttl = Duration::from_secs(30);
    let mut holder = Client::connect(&url, NoTls).unwrap();

    let hold = lock_lease_row(&mut holder, &source_id);
    let started = Instant::now();
    assert!(matches!(
        store.renew(&identity, ttl),
        Err(FenceError::LeaseRowLocked)
    ));
    assert!(started.elapsed() >= Duration::from_secs(1));
    hold.rollback().unwrap();
    // The wait changed nothing: the lease is still this owner's.
    store.renew(&identity, ttl).unwrap();

    let hold = lock_lease_row(&mut holder, &source_id);
    let killer_url = url.clone();
    let killer = thread::spawn(move || {
        let mut killer = Client::connect(&killer_url, NoTls).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while killer
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_stat_activity
                  WHERE pid = $1 AND wait_event_type = 'Lock'",
                &[&control_pid],
            )
            .unwrap()
            .is_none()
        {
            assert!(Instant::now() < deadline, "the renewal never waited");
            thread::sleep(Duration::from_millis(5));
        }
        killer
            .query_one("SELECT pg_terminate_backend($1)", &[&control_pid])
            .unwrap()
            .get::<_, bool>(0)
    });
    assert!(matches!(
        store.renew(&identity, ttl),
        Err(FenceError::ControlStoreUnavailable)
    ));
    assert!(killer.join().unwrap());
    hold.rollback().unwrap();
}

/// The renewal locks the lease row with a `WHERE` on the source key alone.
/// When the transaction it waits behind has updated the row, PostgreSQL
/// returns the updated version to the locking read, so a takeover committed
/// during the wait shows its owner and token and the renewal is refused.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn a_takeover_committed_while_a_renewal_waits_on_the_lease_row_refuses_the_renewal() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-takeover-wait");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let store = PostgresFenceStore::connect(&url).unwrap();
    let control_pid = store.control_backend_pid().unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let identity = store
        .acquire(&request(&source_id, &epoch, "runtime-a", 30_000))
        .unwrap()
        .identity();

    let (updated, updated_rx) = mpsc::channel();
    let taker_url = url.clone();
    let taker_source = source_id.clone();
    let taker = thread::spawn(move || {
        let mut taker = Client::connect(&taker_url, NoTls).unwrap();
        let mut observer = Client::connect(&taker_url, NoTls).unwrap();
        // The row change a takeover's acquire makes, left uncommitted until
        // the renewal is waiting on it.
        let mut takeover = taker.transaction().unwrap();
        let taken: (i64, String) = takeover
            .query_one(
                "UPDATE calque_control.source_fence_lease
                    SET fence_token = nextval('calque_control.source_fence_token_seq'),
                        owner_id = 'runtime-b', lease_phase = 'provisional',
                        expires_at = clock_timestamp() + interval '30 seconds',
                        source_backend_pid = NULL
                  WHERE source_id = $1
              RETURNING fence_token, expires_at::text",
                &[&taker_source.as_str()],
            )
            .map(|row| (row.get(0), row.get(1)))
            .unwrap();
        updated.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while observer
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_stat_activity
                  WHERE pid = $1 AND wait_event_type = 'Lock'",
                &[&control_pid],
            )
            .unwrap()
            .is_none()
        {
            assert!(Instant::now() < deadline, "the renewal never waited");
            thread::sleep(Duration::from_millis(5));
        }
        takeover.commit().unwrap();
        taken
    });
    updated_rx.recv().unwrap();
    let renewal = store.renew(&identity, Duration::from_secs(30));
    let (token, expires_at) = taker.join().unwrap();
    assert!(matches!(renewal, Err(FenceError::LeaseLost)), "{renewal:?}");

    let mut observer = Client::connect(&url, NoTls).unwrap();
    let row = observer
        .query_one(
            "SELECT owner_id, fence_token, expires_at::text
               FROM calque_control.source_fence_lease WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, i64>(1),
            row.get::<_, String>(2)
        ),
        ("runtime-b".to_owned(), token, expires_at)
    );
}

/// A renewal waiting behind a transaction that only locked the lease row, as
/// a canonical append does, must not renew a lease that expired during the
/// wait. The wait here stays under `lock_timeout`, so the renewal gets the
/// lock rather than a `LeaseRowLocked`.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn a_lease_that_expires_while_its_renewal_waits_on_the_lease_row_is_not_renewed() {
    let url = database_url().expect("live control-store database URL");
    let source_id = unique_source("fence-expiry-wait");
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let store = PostgresFenceStore::connect(&url).unwrap();
    let control_pid = store.control_backend_pid().unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let identity = store
        .acquire(&request(&source_id, &epoch, "runtime-a", 30_000))
        .unwrap()
        .identity();

    let (locked, locked_rx) = mpsc::channel();
    let holder_url = url.clone();
    let holder_source = source_id.clone();
    let holder = thread::spawn(move || {
        let mut holder = Client::connect(&holder_url, NoTls).unwrap();
        let mut observer = Client::connect(&holder_url, NoTls).unwrap();
        let expires_at: String = observer
            .query_one(
                "UPDATE calque_control.source_fence_lease
                    SET expires_at = clock_timestamp() + interval '500 milliseconds'
                  WHERE source_id = $1
              RETURNING expires_at::text",
                &[&holder_source.as_str()],
            )
            .unwrap()
            .get(0);
        let hold = lock_lease_row(&mut holder, &holder_source);
        let held_at = Instant::now();
        locked.send(()).unwrap();
        // The renewal must be seen waiting while the lease is still unexpired,
        // or the test would not tell a check made before the wait from one
        // made after it.
        let unexpired_while_waiting: bool = loop {
            let waiting = observer
                .query_opt(
                    "SELECT lease.expires_at > clock_timestamp()
                       FROM pg_catalog.pg_stat_activity AS activity,
                            calque_control.source_fence_lease AS lease
                      WHERE activity.pid = $1 AND activity.wait_event_type = 'Lock'
                        AND lease.source_id = $2",
                    &[&control_pid, &holder_source.as_str()],
                )
                .unwrap();
            if let Some(row) = waiting {
                break row.get(0);
            }
            assert!(
                held_at.elapsed() < Duration::from_millis(400),
                "the renewal never waited"
            );
            thread::sleep(Duration::from_millis(5));
        };
        thread::sleep(Duration::from_millis(900).saturating_sub(held_at.elapsed()));
        let expired_at_release: bool = observer
            .query_one(
                "SELECT expires_at <= clock_timestamp()
                   FROM calque_control.source_fence_lease WHERE source_id = $1",
                &[&holder_source.as_str()],
            )
            .unwrap()
            .get(0);
        hold.commit().unwrap();
        (expires_at, unexpired_while_waiting, expired_at_release)
    });
    locked_rx.recv().unwrap();
    let renewal = store.renew(&identity, Duration::from_secs(30));
    let (expires_at, unexpired_while_waiting, expired_at_release) = holder.join().unwrap();
    assert!(unexpired_while_waiting);
    assert!(expired_at_release);
    assert!(matches!(renewal, Err(FenceError::LeaseLost)), "{renewal:?}");

    let mut observer = Client::connect(&url, NoTls).unwrap();
    let stored: String = observer
        .query_one(
            "SELECT expires_at::text FROM calque_control.source_fence_lease WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(stored, expires_at);
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn exhausted_token_sequence_fails_closed() {
    let url = database_url().expect("live control-store database URL");
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let database_name = format!("calque_fence_overflow_{suffix}");
    let (base, _) = url.rsplit_once('/').expect("database URL has a path");
    let admin_url = format!("{base}/postgres");
    let isolated_url = format!("{base}/{database_name}");
    let mut admin = Client::connect(&admin_url, NoTls).unwrap();
    admin
        .batch_execute(&format!("CREATE DATABASE {database_name}"))
        .unwrap();

    let result = {
        let store = PostgresFenceStore::connect(&isolated_url).unwrap();
        let source_id = SourceId::new("overflow-source").unwrap();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        store.enroll_source(&source_id, &epoch).unwrap();
        let mut control = Client::connect(&isolated_url, NoTls).unwrap();
        control
            .query_one(
                "SELECT setval('calque_control.source_fence_token_seq', $1, true)",
                &[&i64::MAX],
            )
            .unwrap();
        store.acquire(&request(&source_id, &epoch, "runtime-a", 1_000))
    };
    assert!(matches!(result, Err(FenceError::TokenExhausted)));

    admin
        .batch_execute(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .unwrap();
}

/// Connecting takes no lock that an autovacuum of a control table holds. The
/// store installs its schema on every connect and reconnect, under a 1 s lock
/// timeout; the holder here keeps SHARE UPDATE EXCLUSIVE, an autovacuum's
/// lock, on `source_slot_enrollment` for the whole connect.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL or TEST_DATABASE_URL"]
fn connecting_waits_on_no_autovacuum_of_a_control_table() {
    let url = database_url().expect("live control-store database URL");
    // The first connect creates the tables if no earlier test has.
    drop(PostgresFenceStore::connect(&url).unwrap());
    let mut holder = Client::connect(&url, NoTls).unwrap();
    let mut transaction = holder.transaction().unwrap();
    transaction
        .batch_execute(
            "LOCK TABLE calque_control.source_slot_enrollment IN SHARE UPDATE EXCLUSIVE MODE",
        )
        .unwrap();
    let connected = PostgresFenceStore::connect(&url);
    transaction.rollback().unwrap();
    connected.expect("the control store connects while the table is locked");
}
