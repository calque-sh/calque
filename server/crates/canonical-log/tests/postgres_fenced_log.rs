use canonical_log::{
    AppendOutcome, CanonicalBarrier, CanonicalEntry, CanonicalLogPlanRetention, CanonicalLogRead,
    CanonicalOperation, CanonicalOperationKind, CanonicalTransaction, CellValue, ColumnValue,
    FencedWriterIdentity, LogBounds, LogError, PostgresCanonicalLogReader,
    PostgresDatastoreIdentity, PostgresFencedCanonicalLog, SnapshotBuildId, SnapshotBuildOwnerId,
    SnapshotBuildRetention, TransactionKey, CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY,
};
use postgres::{Client, NoTls};
use postgres_connect::relay::SilenceableRelay;
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use sync_protocol::{
    change::{cell_value, CellValue as WireCellValue, ColumnValue as WireColumnValue, PrimaryKey},
    encode_primary_key, PRIMARY_KEY_CODEC_VERSION,
};
use sync_types::{ColumnId, PlanId, SourceEpoch, SourceId, TableId};

/// Process-wide ordinal that makes generated identifiers unique between tests
/// running in parallel within one binary. `SystemTime::now()` has only
/// microsecond granularity on some platforms, macOS included, so `pid + nanos`
/// alone collides; pid and clock still separate runs.
fn next_id_ordinal() -> u64 {
    static ORDINAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    ORDINAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// The state directory a refusal of a log an earlier release wrote names.
fn state_dir() -> &'static Path {
    Path::new("/srv/calque/state")
}

fn unique_source_id() -> SourceId {
    SourceId::new(format!(
        "fenced-log-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        next_id_ordinal()
    ))
    .unwrap()
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

/// A transaction that updates row `sequence` of `public.tasks`.
fn update(epoch: &SourceEpoch, sequence: u64) -> CanonicalTransaction {
    let id = i64::try_from(sequence).unwrap();
    CanonicalTransaction {
        key: TransactionKey {
            source_epoch: epoch.clone(),
            end_lsn: format!("0/{:X}", sequence + 1),
            postgres_xid: sequence as u32,
        },
        transaction_id: format!("tx-{sequence}"),
        begin_lsn: format!("0/{sequence:X}"),
        commit_lsn: format!("0/{sequence:X}"),
        commit_time_unix_micros: sequence,
        operations: vec![CanonicalOperation {
            table_id: TableId::new("public.tasks").unwrap(),
            primary_key: encode_primary_key(&PrimaryKey {
                codec_version: PRIMARY_KEY_CODEC_VERSION,
                parts: vec![WireColumnValue {
                    column_id: 1,
                    value: Some(WireCellValue {
                        kind: Some(cell_value::Kind::Int64Value(id)),
                    }),
                }],
            })
            .unwrap(),
            kind: CanonicalOperationKind::UpdatePatch {
                changed_columns: vec![ColumnValue {
                    column_id: ColumnId::new(2),
                    value: CellValue::Text(format!("task {sequence}")),
                }],
            },
        }],
        mutation_ids: Vec::new(),
    }
}

/// Installs the fence tables and marks `source_id`/`epoch` as actively owned.
///
/// The DDL runs under [`CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY`], as the production
/// installers do: these tests run in parallel and each also installs through
/// `PostgresFencedCanonicalLog::connect`, so without the shared lock
/// `CREATE SCHEMA IF NOT EXISTS` races itself.
fn install_active_writer_fence(control: &mut Client, source_id: &SourceId, epoch: &SourceEpoch) {
    let mut transaction = control.transaction().unwrap();
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock($1)",
            &[&CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY],
        )
        .unwrap();
    transaction
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS calque_control;
             CREATE TABLE IF NOT EXISTS calque_control.source_fence_lease (
                source_id text PRIMARY KEY,
                source_epoch text NOT NULL,
                fence_token bigint NOT NULL DEFAULT 0 CHECK (fence_token >= 0),
                owner_id text,
                lease_phase text NOT NULL DEFAULT 'unowned'
                    CHECK (lease_phase IN ('unowned', 'provisional', 'active')),
                expires_at timestamptz,
                source_backend_pid integer,
                CHECK (
                    (lease_phase = 'unowned' AND owner_id IS NULL
                        AND expires_at IS NULL AND source_backend_pid IS NULL)
                    OR
                    (lease_phase = 'provisional' AND owner_id IS NOT NULL
                        AND expires_at IS NOT NULL)
                    OR
                    (lease_phase = 'active' AND owner_id IS NOT NULL
                        AND expires_at IS NOT NULL AND source_backend_pid IS NOT NULL)
                )
             );
             CREATE TABLE IF NOT EXISTS calque_control.canonical_log_writer_fence (
                source_id text PRIMARY KEY,
                source_epoch text NOT NULL,
                fence_token bigint NOT NULL CHECK (fence_token > 0)
             );
             CREATE TABLE IF NOT EXISTS calque_control.source_slot_enrollment (
                source_id text PRIMARY KEY,
                source_epoch text NOT NULL,
                slot_name text NOT NULL,
                publication_name text NOT NULL,
                system_identifier text NOT NULL,
                database_name text NOT NULL,
                timeline_id bigint NOT NULL CHECK (timeline_id >= 0),
                previous_timeline_id bigint NOT NULL CHECK (previous_timeline_id >= 0),
                pgoutput_profile text NOT NULL,
                initial_lsn text NOT NULL,
                durable_canonical_lsn text NOT NULL
             );",
        )
        .unwrap();
    transaction.commit().unwrap();
    control
        .execute(
            "INSERT INTO calque_control.source_fence_lease
                (source_id, source_epoch, fence_token, owner_id, lease_phase,
                 expires_at, source_backend_pid)
             VALUES ($1, $2, 1, 'owner-a', 'active',
                     clock_timestamp() + interval '5 minutes', pg_backend_pid())",
            &[&source_id.as_str(), &epoch.as_str()],
        )
        .unwrap();
    control
        .execute(
            "INSERT INTO calque_control.canonical_log_writer_fence
                (source_id, source_epoch, fence_token)
             VALUES ($1, $2, 1)",
            &[&source_id.as_str(), &epoch.as_str()],
        )
        .unwrap();
}

fn install_slot_enrollment(control: &mut Client, source_id: &SourceId, epoch: &SourceEpoch) {
    control
        .execute(
            "INSERT INTO calque_control.source_slot_enrollment (
                source_id, source_epoch, slot_name, publication_name,
                system_identifier, database_name, timeline_id,
                previous_timeline_id, pgoutput_profile, initial_lsn,
                durable_canonical_lsn
             ) VALUES ($1, $2, 'test-slot', 'test-publication',
                       'test-system', 'test-database', 1, 1,
                       'v1-no-streaming', '0/1', '0/1')",
            &[&source_id.as_str(), &epoch.as_str()],
        )
        .unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn append_with_checkpoint_is_atomic_monotonic_and_idempotent() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    install_slot_enrollment(&mut control, &source_id, &epoch);

    let log = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    let entry = CanonicalEntry::Barrier(barrier(&epoch, 1));
    assert_eq!(
        log.append_with_checkpoint(entry.clone(), 0x20)
            .unwrap()
            .position()
            .sequence,
        1
    );
    assert!(matches!(
        log.append_with_checkpoint(entry, 0x10).unwrap(),
        AppendOutcome::AlreadyPresent(_)
    ));

    let state = control
        .query_one(
            "SELECT head.head_sequence, enrollment.durable_canonical_lsn
               FROM calque_control.canonical_log_head AS head
               JOIN calque_control.source_slot_enrollment AS enrollment USING (source_id)
              WHERE head.source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert_eq!(state.get::<_, i64>(0), 1);
    assert_eq!(state.get::<_, String>(1), "0/20");

    control
        .execute(
            "DELETE FROM calque_control.source_slot_enrollment WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert!(matches!(
        log.append_with_checkpoint(CanonicalEntry::Barrier(barrier(&epoch, 2)), 0x30),
        Err(LogError::PostgresHeadCorrupt)
    ));
    let unchanged = control
        .query_one(
            "SELECT head.head_sequence, count(record.sequence)::bigint
               FROM calque_control.canonical_log_head AS head
               LEFT JOIN calque_control.canonical_log_record AS record USING (source_id)
              WHERE head.source_id = $1
              GROUP BY head.head_sequence",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert_eq!(unchanged.get::<_, i64>(0), 1);
    assert_eq!(unchanged.get::<_, i64>(1), 1);

    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_record",
        "canonical_log_head",
        "source_slot_enrollment",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

fn barriers(epoch: &SourceEpoch, sequences: &[u64]) -> Vec<CanonicalEntry> {
    sequences
        .iter()
        .map(|sequence| CanonicalEntry::Barrier(barrier(epoch, *sequence)))
        .collect()
}

/// Each outcome as its sequence, negative when the entry was already present.
fn outcome_sequences(outcomes: &[AppendOutcome]) -> Vec<i64> {
    outcomes
        .iter()
        .map(|outcome| match outcome {
            AppendOutcome::Appended(position) => position.sequence as i64,
            AppendOutcome::AlreadyPresent(position) => -(position.sequence as i64),
        })
        .collect()
}

/// The head, every record's sequence and xid in sequence order, the number of
/// transactions that wrote those records, and the durable checkpoint.
fn durable_state(
    control: &mut Client,
    source_id: &SourceId,
) -> (i64, Vec<(i64, i64)>, i64, String) {
    let head = control
        .query_opt(
            "SELECT head_sequence FROM calque_control.canonical_log_head WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .map_or(0, |row| row.get(0));
    let records = control
        .query(
            "SELECT sequence, (transaction_identity::jsonb ->> 'postgresXid')::bigint
               FROM calque_control.canonical_log_record
              WHERE source_id = $1
              ORDER BY sequence",
            &[&source_id.as_str()],
        )
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let writers: i64 = control
        .query_one(
            "SELECT count(DISTINCT xmin::text)::bigint
               FROM calque_control.canonical_log_record WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    let checkpoint = control
        .query_one(
            "SELECT durable_canonical_lsn
               FROM calque_control.source_slot_enrollment WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);
    (head, records, writers, checkpoint)
}

fn remove_source(control: &mut Client, source_id: &SourceId) {
    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_record",
        "canonical_log_head",
        "source_slot_enrollment",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_group_appends_in_order_in_one_commit_with_its_checkpoint() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    install_slot_enrollment(&mut control, &source_id, &epoch);
    let log = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();

    let outcomes = log
        .append_group(barriers(&epoch, &[1, 2, 3]), Some(0x40))
        .unwrap();
    assert_eq!(outcome_sequences(&outcomes), [1, 2, 3]);
    assert_eq!(
        durable_state(&mut control, &source_id),
        (3, vec![(1, 1), (2, 2), (3, 3)], 1, "0/40".to_owned())
    );

    // A replayed prefix keeps its sequences, the rest extends the log in
    // order, and a replayed WAL position never moves the checkpoint back.
    let outcomes = log
        .append_group(barriers(&epoch, &[2, 3, 4, 5]), Some(0x30))
        .unwrap();
    assert_eq!(outcome_sequences(&outcomes), [-2, -3, 4, 5]);
    let (head, records, writers, checkpoint) = durable_state(&mut control, &source_id);
    assert_eq!((head, writers, checkpoint.as_str()), (5, 2, "0/40"));
    assert_eq!(records, [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]);

    // A key repeated inside a group takes a second statement in the same
    // commit, and deduplicates against its first occurrence.
    let outcomes = log
        .append_group(barriers(&epoch, &[6, 6, 7]), Some(0x70))
        .unwrap();
    assert_eq!(outcome_sequences(&outcomes), [6, -6, 7]);
    let (head, records, writers, checkpoint) = durable_state(&mut control, &source_id);
    assert_eq!((head, writers, checkpoint.as_str()), (7, 3, "0/70"));
    assert_eq!(records.last(), Some(&(7, 7)));

    remove_source(&mut control, &source_id);
}

/// An append whose server stops answering mid-statement fails once the log
/// connection's reply bound runs out, where it used to wait on the socket
/// without end and hold the ingest loop with it.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn an_append_whose_server_stops_answering_fails_once_the_reply_bound_runs_out() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let relay = SilenceableRelay::start(&url);
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    install_slot_enrollment(&mut control, &source_id, &epoch);
    let log = PostgresFencedCanonicalLog::connect(
        relay.url(),
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    log.append_group(barriers(&epoch, &[1]), Some(0x10))
        .unwrap();

    relay.silence();
    let (sender, receiver) = mpsc::channel();
    let appending = epoch.clone();
    thread::spawn(move || {
        let started = Instant::now();
        let result = log
            .append_group(barriers(&appending, &[2]), Some(0x20))
            .map(|_| ());
        let _ = sender.send((result, started.elapsed()));
    });
    let (result, elapsed) = receiver
        .recv_timeout(Duration::from_secs(15))
        .expect("the append on a silent socket had not returned after 15 s");
    assert!(
        matches!(result, Err(LogError::PostgresUnavailable)),
        "{result:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(8),
        "the append returned after {elapsed:?}"
    );
    assert_eq!(durable_state(&mut control, &source_id).0, 1);
    remove_source(&mut control, &source_id);
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_group_that_fails_partway_commits_none_of_it_nor_its_checkpoint() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    install_slot_enrollment(&mut control, &source_id, &epoch);
    let log = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    log.append_group(barriers(&epoch, &[1]), Some(0x10))
        .unwrap();
    let before = durable_state(&mut control, &source_id);
    assert_eq!(before, (1, vec![(1, 1)], 1, "0/10".to_owned()));

    // An entry that conflicts with a durable one rejects the whole group, and
    // the statement that reports it writes neither records nor checkpoint,
    // whether or not the group also holds a new entry.
    let conflicting = CanonicalEntry::Barrier(CanonicalBarrier {
        build_id: "a-different-build".to_owned(),
        ..barrier(&epoch, 1)
    });
    let mut with_new = barriers(&epoch, &[2]);
    with_new.push(conflicting.clone());
    for group in [with_new, vec![conflicting]] {
        let size = group.len();
        let result = log.append_group(group, Some(0x50));
        assert!(
            matches!(result, Err(LogError::DuplicateTransactionConflict)),
            "a group of {size}: {result:?}"
        );
        assert_eq!(
            durable_state(&mut control, &source_id),
            before,
            "a group of {size}"
        );
    }

    // A record already at sequence 3 makes the group's second insert fail
    // after its first succeeded: in one statement, and across two statements
    // of one transaction when a repeated key splits the group.
    let stray = r#"{"sourceEpoch":"epoch-1","endLsn":"0/F00","postgresXid":999}"#;
    control
        .execute(
            "INSERT INTO calque_control.canonical_log_record
                (source_id, source_epoch, sequence, transaction_identity,
                 entry_bytes, entry_sha256)
             VALUES ($1, $2, 3, $3, '\\x7b7d', 'stray')",
            &[&source_id.as_str(), &epoch.as_str(), &stray],
        )
        .unwrap();
    let with_stray = durable_state(&mut control, &source_id);
    assert!(log
        .append_group(barriers(&epoch, &[2, 3]), Some(0x30))
        .is_err());
    assert_eq!(durable_state(&mut control, &source_id), with_stray);
    assert!(log
        .append_group(barriers(&epoch, &[2, 2, 3]), Some(0x30))
        .is_err());
    assert_eq!(durable_state(&mut control, &source_id), with_stray);

    control
        .execute(
            "DELETE FROM calque_control.canonical_log_record
              WHERE source_id = $1 AND transaction_identity = $2",
            &[&source_id.as_str(), &stray],
        )
        .unwrap();
    let outcomes = log
        .append_group(barriers(&epoch, &[2, 3]), Some(0x30))
        .unwrap();
    assert_eq!(outcome_sequences(&outcomes), [2, 3]);
    assert_eq!(
        durable_state(&mut control, &source_id),
        (3, vec![(1, 1), (2, 2), (3, 3)], 2, "0/30".to_owned())
    );

    remove_source(&mut control, &source_id);
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_group_after_the_fence_is_lost_appends_nothing() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    install_slot_enrollment(&mut control, &source_id, &epoch);
    let old = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    old.append_group(barriers(&epoch, &[1, 2]), Some(0x20))
        .unwrap();
    let before = durable_state(&mut control, &source_id);

    let mut takeover = control.transaction().unwrap();
    takeover
        .execute(
            "UPDATE calque_control.source_fence_lease
                SET fence_token = 2, owner_id = 'owner-b', lease_phase = 'active',
                    expires_at = clock_timestamp() + interval '1 minute',
                    source_backend_pid = pg_backend_pid()
              WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    takeover
        .execute(
            "UPDATE calque_control.canonical_log_writer_fence
                SET fence_token = 2
              WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    takeover.commit().unwrap();

    assert!(matches!(
        old.append_group(barriers(&epoch, &[3, 4]), Some(0x40)),
        Err(LogError::FencedWriterRejected)
    ));
    assert!(matches!(
        old.append_group(barriers(&epoch, &[3, 3, 4]), None),
        Err(LogError::FencedWriterRejected)
    ));
    assert_eq!(durable_state(&mut control, &source_id), before);

    remove_source(&mut control, &source_id);
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn stale_postgres_writer_cannot_append_after_takeover() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);

    let old = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore.clone()).unwrap(),
        state_dir(),
    )
    .unwrap();
    assert_eq!(
        old.append_barrier(barrier(&epoch, 1))
            .unwrap()
            .position()
            .sequence,
        1
    );
    assert!(matches!(
        old.append_barrier(barrier(&epoch, 1)).unwrap(),
        AppendOutcome::AlreadyPresent(_)
    ));

    let mut takeover = control.transaction().unwrap();
    takeover
        .execute(
            "UPDATE calque_control.source_fence_lease
                SET fence_token = 2, owner_id = 'owner-b', lease_phase = 'active',
                    expires_at = clock_timestamp() + interval '1 minute',
                    source_backend_pid = pg_backend_pid()
              WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    takeover
        .execute(
            "UPDATE calque_control.canonical_log_writer_fence
                SET fence_token = 2
              WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    takeover.commit().unwrap();

    assert!(matches!(
        old.append_barrier(barrier(&epoch, 2)),
        Err(LogError::FencedWriterRejected)
    ));
    assert_eq!(old.head().unwrap().sequence, 1);

    let current = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 2, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    assert_eq!(
        current
            .append_barrier(barrier(&epoch, 2))
            .unwrap()
            .position()
            .sequence,
        2
    );
    let records = current
        .read_after_bounded(0, None, 10, 1024 * 1024)
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].position.sequence, 1);
    assert_eq!(records[1].position.sequence, 2);
    let page =
        CanonicalLogRead::read_page_after_bounded(&current, 0, None, 1, 1024 * 1024).unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(page.stored_bytes > 64);
    assert_eq!(
        CanonicalLogRead::read_page_after_bounded(&current, 0, None, 10, page.stored_bytes,)
            .unwrap()
            .records
            .len(),
        1
    );
    assert!(matches!(
        CanonicalLogRead::read_page_after_bounded(&current, 0, None, 10, page.stored_bytes - 1,),
        Err(LogError::ReadLimitTooSmall { .. })
    ));

    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_record",
        "canonical_log_head",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn writability_probe_requires_the_current_fence_without_advancing_the_head() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);

    let current = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    current.probe_writable_current_fence().unwrap();
    assert_eq!(current.head().unwrap().sequence, 0);

    control
        .execute(
            "UPDATE calque_control.canonical_log_writer_fence
                SET fence_token = 2 WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    assert!(matches!(
        current.probe_writable_current_fence(),
        Err(LogError::FencedWriterRejected)
    ));

    control
        .execute(
            "DELETE FROM calque_control.canonical_log_head WHERE source_id = $1;",
            &[&source_id.as_str()],
        )
        .unwrap();
    control
        .execute(
            "DELETE FROM calque_control.canonical_log_writer_fence WHERE source_id = $1;",
            &[&source_id.as_str()],
        )
        .unwrap();
    control
        .execute(
            "DELETE FROM calque_control.source_fence_lease WHERE source_id = $1;",
            &[&source_id.as_str()],
        )
        .unwrap();
}

/// A writer can lose its fence without its token changing: its lease expires,
/// leaves the `active` phase, or names another epoch, or the writer fence
/// names another epoch than the lease. The tests above fence a writer by
/// bumping the token, which the token equalities refuse before any of these
/// predicates decides, so each case here changes one fact and keeps the token.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_writer_that_keeps_its_token_is_fenced_by_lease_expiry_phase_and_epoch() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    let cases: [(&str, &[&str]); 4] = [
        (
            "an expired lease",
            &["UPDATE calque_control.source_fence_lease
                  SET expires_at = clock_timestamp() - interval '1 second'
                WHERE source_id = $1"],
        ),
        (
            "a provisional lease",
            &["UPDATE calque_control.source_fence_lease
                  SET lease_phase = 'provisional'
                WHERE source_id = $1"],
        ),
        (
            "a lease and writer fence of another epoch",
            &[
                "UPDATE calque_control.source_fence_lease
                    SET source_epoch = 'epoch-2'
                  WHERE source_id = $1",
                "UPDATE calque_control.canonical_log_writer_fence
                    SET source_epoch = 'epoch-2'
                  WHERE source_id = $1",
            ],
        ),
        (
            "a writer fence of another epoch than its lease",
            &["UPDATE calque_control.canonical_log_writer_fence
                  SET source_epoch = 'epoch-2'
                WHERE source_id = $1"],
        ),
    ];
    for (case, statements) in cases {
        let source_id = unique_source_id();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        install_active_writer_fence(&mut control, &source_id, &epoch);
        install_slot_enrollment(&mut control, &source_id, &epoch);
        let log = PostgresFencedCanonicalLog::connect(
            &url,
            FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore.clone())
                .unwrap(),
            state_dir(),
        )
        .unwrap();
        log.append_group(barriers(&epoch, &[1, 2]), Some(0x20))
            .unwrap();
        log.probe_writable_current_fence().unwrap();
        let before = durable_state(&mut control, &source_id);

        for statement in statements {
            assert_eq!(
                control.execute(*statement, &[&source_id.as_str()]).unwrap(),
                1,
                "{case}"
            );
        }

        assert!(
            matches!(
                log.append_group(barriers(&epoch, &[3, 4]), Some(0x40)),
                Err(LogError::FencedWriterRejected)
            ),
            "{case}: append_group"
        );
        assert!(
            matches!(
                log.probe_writable_current_fence(),
                Err(LogError::FencedWriterRejected)
            ),
            "{case}: probe_writable_current_fence"
        );
        assert_eq!(durable_state(&mut control, &source_id), before, "{case}");

        remove_source(&mut control, &source_id);
    }
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL with database-create privilege"]
fn writer_claim_rejects_a_different_control_datastore() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let database_name = format!("calque_identity_{suffix}");
    let (base, _) = url.rsplit_once('/').expect("database URL has a path");
    let admin_url = format!("{base}/postgres");
    let isolated_url = format!("{base}/{database_name}");
    let mut admin = Client::connect(&admin_url, NoTls).unwrap();
    admin
        .batch_execute(&format!("CREATE DATABASE {database_name}"))
        .unwrap();
    let mut isolated = Client::connect(&isolated_url, NoTls).unwrap();
    let isolated_identity = PostgresDatastoreIdentity::inspect(&mut isolated).unwrap();
    drop(isolated);

    let result = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(
            SourceId::new("identity-probe").unwrap(),
            SourceEpoch::new("epoch-1").unwrap(),
            1,
            isolated_identity,
        )
        .unwrap(),
        state_dir(),
    );
    assert!(matches!(result, Err(LogError::DatastoreIdentityMismatch)));

    admin
        .batch_execute(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .unwrap();
}

/// The head, floor and page one combined read returns are the ones the bounds
/// read and the page read return apart, refusals included.
fn assert_combined_read_matches(
    read: &dyn CanonicalLogRead,
    from: u64,
    through: Option<u64>,
    records: usize,
    bytes: u64,
) {
    let (bounds, page) = read
        .read_bounds_and_page_after_bounded(from, through, records, bytes)
        .unwrap();
    let case = format!("from {from}, through {through:?}, {records} records, {bytes} bytes");
    assert_eq!(bounds, read.bounds().unwrap(), "{case}");
    assert_eq!(
        format!("{page:?}"),
        format!(
            "{:?}",
            read.read_page_after_bounded(from, through, records, bytes)
        ),
        "{case}"
    );
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_reader_reads_the_head_floor_and_pages_the_writer_log_reads() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    let writer = FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap();
    let mut log = PostgresFencedCanonicalLog::connect(&url, writer.clone(), state_dir()).unwrap();
    let reader = PostgresCanonicalLogReader::connect(&url, &writer, state_dir()).unwrap();
    assert_eq!(CanonicalLogRead::source_id(&reader), &source_id);
    assert_eq!(CanonicalLogRead::source_epoch(&reader), &epoch);

    // Before anything is appended, both read an empty log at sequence 0.
    assert_eq!(reader.head().unwrap(), log.head().unwrap());
    assert_eq!(reader.head().unwrap().sequence, 0);
    assert_eq!(
        reader.minimum_retained().unwrap(),
        log.minimum_retained().unwrap()
    );
    let bounds_alone = |read: &dyn CanonicalLogRead| LogBounds {
        head: read.head().unwrap(),
        minimum_retained: read.minimum_retained().unwrap(),
    };
    assert_eq!(reader.bounds().unwrap(), bounds_alone(&reader));
    assert_eq!(
        reader.bounds().unwrap(),
        CanonicalLogRead::bounds(&log).unwrap()
    );
    for read in [&reader as &dyn CanonicalLogRead, &log] {
        assert_combined_read_matches(read, 0, None, 10, 1 << 20);
    }

    for sequence in 1..=3 {
        log.append(update(&epoch, sequence)).unwrap();
    }
    log.append_barrier(barrier(&epoch, 4)).unwrap();
    assert_eq!(reader.head().unwrap().sequence, 4);
    assert_eq!(reader.head().unwrap(), log.head().unwrap());

    let whole = CanonicalLogRead::read_page_after_bounded(&log, 0, None, 10, 1 << 20).unwrap();
    assert_eq!(whole.records.len(), 4);
    for (from, through, records, bytes) in [
        (0, None, 10, 1 << 20),
        (1, None, 2, 1 << 20),
        (0, Some(3), 10, 1 << 20),
        (
            0,
            None,
            10,
            whole.records.len() as u64 * 64 + whole.stored_bytes / 2,
        ),
        (4, None, 10, 1 << 20),
    ] {
        assert_eq!(
            reader
                .read_page_after_bounded(from, through, records, bytes)
                .unwrap(),
            CanonicalLogRead::read_page_after_bounded(&log, from, through, records, bytes).unwrap(),
            "from {from}, through {through:?}, {records} records, {bytes} bytes"
        );
    }
    // A fence past the head reads to the head, and a byte limit below the
    // next record is refused, by the combined read as by the page read.
    for (from, through, records, bytes) in [
        (0, None, 10, 1 << 20),
        (1, Some(3), 2, 1 << 20),
        (0, Some(9), 10, 1 << 20),
        (4, None, 10, 1 << 20),
        (0, None, 10, 1),
    ] {
        for read in [&reader as &dyn CanonicalLogRead, &log] {
            assert_combined_read_matches(read, from, through, records, bytes);
        }
    }
    assert!(matches!(
        reader.read_page_after_bounded(0, None, 10, 1),
        Err(LogError::ReadLimitTooSmall { .. })
    ));

    log.retain_from(3).unwrap();
    assert_eq!(reader.minimum_retained().unwrap().sequence, 3);
    assert_eq!(reader.bounds().unwrap(), bounds_alone(&reader));
    assert_eq!(
        reader.bounds().unwrap(),
        CanonicalLogRead::bounds(&log).unwrap()
    );
    assert_eq!(reader.bounds().unwrap().head.sequence, 4);
    assert_eq!(
        reader.minimum_retained().unwrap(),
        log.minimum_retained().unwrap()
    );
    assert!(matches!(
        reader.read_page_after_bounded(0, None, 10, 1 << 20),
        Err(LogError::BeforeRetention {
            requested: 0,
            minimum: 3
        })
    ));
    for read in [&reader as &dyn CanonicalLogRead, &log] {
        assert_combined_read_matches(read, 0, None, 10, 1 << 20);
        assert_combined_read_matches(read, 2, None, 10, 1 << 20);
    }
    assert_eq!(
        reader
            .read_page_after_bounded(2, None, 10, 1 << 20)
            .unwrap(),
        CanonicalLogRead::read_page_after_bounded(&log, 2, None, 10, 1 << 20).unwrap()
    );
    assert!(!reader.is_closed());

    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_retired_record",
        "canonical_log_record",
        "canonical_log_head",
        "canonical_log_retention",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

/// A float cell appended to the log is read back as it was appended, by the
/// reader change builds read through.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_float_cell_reads_back_as_it_was_appended() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    let writer = FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap();
    let log = PostgresFencedCanonicalLog::connect(&url, writer.clone(), state_dir()).unwrap();
    let reader = PostgresCanonicalLogReader::connect(&url, &writer, state_dir()).unwrap();

    let mut floats = update(&epoch, 1);
    floats.operations[0].kind = CanonicalOperationKind::UpdatePatch {
        changed_columns: vec![
            ColumnValue {
                column_id: ColumnId::new(2),
                value: CellValue::Float64(-42.25),
            },
            ColumnValue {
                column_id: ColumnId::new(3),
                value: CellValue::Float64(2.0),
            },
        ],
    };
    log.append(floats.clone()).unwrap();
    let page = reader
        .read_page_after_bounded(0, None, 10, 1 << 20)
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].entry, CanonicalEntry::Transaction(floats));

    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_retired_record",
        "canonical_log_record",
        "canonical_log_head",
        "canonical_log_retention",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_page_read_refuses_a_record_whose_digest_does_not_match_and_a_gap() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    let writer = FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap();
    let log = PostgresFencedCanonicalLog::connect(&url, writer.clone(), state_dir()).unwrap();
    let reader = PostgresCanonicalLogReader::connect(&url, &writer, state_dir()).unwrap();
    for sequence in 1..=4 {
        log.append(update(&epoch, sequence)).unwrap();
    }
    let set_digest = |control: &mut Client, sequence: i64, digest: &str| {
        control
            .execute(
                "UPDATE calque_control.canonical_log_record SET entry_sha256 = $3
                  WHERE source_id = $1 AND sequence = $2",
                &[&source_id.as_str(), &sequence, &digest],
            )
            .unwrap();
    };
    let stored_digest: String = control
        .query_one(
            "SELECT entry_sha256 FROM calque_control.canonical_log_record
              WHERE source_id = $1 AND sequence = 2",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0);

    set_digest(&mut control, 2, &"0".repeat(64));
    for refused in [
        reader.read_page_after_bounded(0, None, 10, 1 << 20),
        CanonicalLogRead::read_page_after_bounded(&log, 0, None, 10, 1 << 20),
    ] {
        assert!(matches!(refused, Err(LogError::PostgresHeadCorrupt)));
    }
    for read in [&reader as &dyn CanonicalLogRead, &log] {
        assert!(matches!(
            read.read_bounds_and_page_after_bounded(0, None, 10, 1 << 20),
            Ok((_, Err(LogError::PostgresHeadCorrupt)))
        ));
        assert_combined_read_matches(read, 0, None, 10, 1 << 20);
    }
    // The page after the record does not read it.
    assert_eq!(
        reader
            .read_page_after_bounded(2, None, 10, 1 << 20)
            .unwrap()
            .records
            .len(),
        2
    );
    set_digest(&mut control, 2, &stored_digest);
    assert_eq!(
        reader
            .read_page_after_bounded(0, None, 10, 1 << 20)
            .unwrap()
            .records
            .len(),
        4
    );

    control
        .execute(
            "DELETE FROM calque_control.canonical_log_record
              WHERE source_id = $1 AND sequence = 3",
            &[&source_id.as_str()],
        )
        .unwrap();
    for refused in [
        reader.read_page_after_bounded(0, None, 10, 1 << 20),
        CanonicalLogRead::read_page_after_bounded(&log, 0, None, 10, 1 << 20),
    ] {
        assert!(matches!(
            refused,
            Err(LogError::SequenceGap {
                expected: 3,
                actual: 4
            })
        ));
    }
    for read in [&reader as &dyn CanonicalLogRead, &log] {
        assert!(matches!(
            read.read_bounds_and_page_after_bounded(0, None, 10, 1 << 20),
            Ok((
                _,
                Err(LogError::SequenceGap {
                    expected: 3,
                    actual: 4
                })
            ))
        ));
        assert_combined_read_matches(read, 0, None, 10, 1 << 20);
        assert_combined_read_matches(read, 0, Some(2), 10, 1 << 20);
    }
    // A page that ends before the gap is whole.
    assert_eq!(
        reader
            .read_page_after_bounded(0, Some(2), 10, 1 << 20)
            .unwrap()
            .records
            .len(),
        2
    );
    // So is one that starts after it.
    assert_eq!(
        reader
            .read_page_after_bounded(3, None, 10, 1 << 20)
            .unwrap()
            .records
            .len(),
        1
    );

    drop(log);
    remove_source(&mut control, &source_id);
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn postgres_snapshot_build_lease_pins_retention_and_rejects_stale_capabilities() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);

    let mut log = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    for sequence in 1..=3 {
        log.append_barrier(barrier(&epoch, sequence)).unwrap();
    }
    let server_now_ms: i64 = control
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .unwrap()
        .get(0);
    let plan_id = PlanId::new("bootstrap-plan-live").unwrap();
    let plan_expiry_ms = server_now_ms + 60_000;
    log.acquire_plan_retention_lease(plan_id.clone(), 2, plan_expiry_ms, i64::MIN)
        .unwrap();
    log.acquire_plan_retention_lease(plan_id.clone(), 2, plan_expiry_ms, i64::MAX)
        .unwrap();
    assert!(matches!(
        log.acquire_plan_retention_lease(plan_id.clone(), 3, plan_expiry_ms, server_now_ms),
        Err(LogError::RetentionLeaseConflict)
    ));
    assert!(matches!(
        log.acquire_plan_retention_lease(
            PlanId::new("expired-bootstrap-plan").unwrap(),
            2,
            server_now_ms - 1,
            server_now_ms - 10_000,
        ),
        Err(LogError::InvalidRetentionLease)
    ));
    log.retain_from(4).unwrap();
    assert_eq!(
        CanonicalLogRead::minimum_retained(&log).unwrap().sequence,
        2
    );
    let build_id = SnapshotBuildId::new("snapshot-build-live").unwrap();
    let owner_id = SnapshotBuildOwnerId::new("snapshot-worker-a").unwrap();
    let lease = log
        .acquire_snapshot_build_retention(
            build_id.clone(),
            owner_id.clone(),
            Duration::from_secs(60),
        )
        .unwrap();
    assert_eq!(lease.minimum_sequence(), 4);
    assert_eq!(lease.token(), 1);
    let replacement = log
        .acquire_snapshot_build_retention(
            build_id.clone(),
            owner_id.clone(),
            Duration::from_secs(120),
        )
        .unwrap();
    assert!(replacement.token() > lease.token());
    assert_eq!(replacement.minimum_sequence(), lease.minimum_sequence());
    assert!(matches!(
        log.renew_snapshot_build_retention(&lease, Duration::from_secs(60)),
        Err(LogError::SnapshotBuildRetentionLeaseRejected)
    ));
    assert!(matches!(
        log.release_snapshot_build_retention(&lease),
        Err(LogError::SnapshotBuildRetentionLeaseRejected)
    ));
    assert!(matches!(
        log.acquire_snapshot_build_retention(
            build_id.clone(),
            SnapshotBuildOwnerId::new("snapshot-worker-b").unwrap(),
            Duration::from_secs(60),
        ),
        Err(LogError::SnapshotBuildRetentionLeaseConflict)
    ));
    let server_now_ms: i64 = control
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(replacement.expires_at_unix_millis() > server_now_ms);

    log.append_barrier(barrier(&epoch, 4)).unwrap();
    log.retain_from(5).unwrap();
    assert_eq!(log.minimum_retained().unwrap().sequence, 2);
    control
        .execute(
            "UPDATE calque_control.canonical_log_plan_retention_lease
                SET expires_at = clock_timestamp() - interval '1 second'
              WHERE source_id = $1 AND source_epoch = $2 AND plan_id = $3",
            &[&source_id.as_str(), &epoch.as_str(), &plan_id.as_str()],
        )
        .unwrap();
    log.retain_from(5).unwrap();
    assert_eq!(log.minimum_retained().unwrap().sequence, 4);
    assert_eq!(
        log.read_after_bounded(3, None, 10, 1024 * 1024)
            .unwrap()
            .len(),
        1
    );
    let renewed = log
        .renew_snapshot_build_retention(&replacement, Duration::from_secs(60))
        .unwrap();
    assert_eq!(renewed.token(), replacement.token());
    control
        .execute(
            "UPDATE calque_control.canonical_log_build_retention_lease
                SET expires_at = clock_timestamp() - interval '1 second'
              WHERE source_id = $1 AND source_epoch = $2 AND build_id = $3",
            &[&source_id.as_str(), &epoch.as_str(), &build_id.as_str()],
        )
        .unwrap();
    assert!(matches!(
        log.renew_snapshot_build_retention(&renewed, Duration::from_secs(60)),
        Err(LogError::SnapshotBuildRetentionLeaseRejected)
    ));
    assert!(matches!(
        log.release_snapshot_build_retention(&renewed),
        Err(LogError::SnapshotBuildRetentionLeaseRejected)
    ));
    let replacement = log
        .acquire_snapshot_build_retention(build_id, owner_id, Duration::from_secs(60))
        .unwrap();
    assert!(replacement.token() > renewed.token());
    assert_eq!(replacement.minimum_sequence(), 5);
    assert!(matches!(
        log.release_snapshot_build_retention(&renewed),
        Err(LogError::SnapshotBuildRetentionLeaseRejected)
    ));
    log.release_snapshot_build_retention(&replacement).unwrap();
    log.retain_from(5).unwrap();
    assert_eq!(log.minimum_retained().unwrap().sequence, 5);
    assert!(matches!(
        log.read_after_bounded(3, None, 10, 1024 * 1024),
        Err(LogError::BeforeRetention {
            requested: 3,
            minimum: 5
        })
    ));
    assert_eq!(
        log.append_barrier(barrier(&epoch, 1)).unwrap(),
        AppendOutcome::AlreadyPresent(sync_types::CursorPosition {
            source_epoch: epoch.clone(),
            sequence: 1,
        })
    );

    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_plan_retention_lease",
        "canonical_log_build_retention_lease",
        "canonical_log_retired_record",
        "canonical_log_record",
        "canonical_log_head",
        "canonical_log_retention",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

/// A plan's lease holds the floor at its start until the plan expires and no
/// longer, and the tail's stored bytes are counted from the cut to the head.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn postgres_plan_leases_hold_the_floor_until_their_plans_expire() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    let mut log = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    for sequence in 1..=6 {
        log.append(update(&epoch, sequence)).unwrap();
    }
    let stored: Vec<i64> = control
        .query(
            "SELECT octet_length(entry_bytes)::bigint FROM calque_control.canonical_log_record
              WHERE source_id = $1 ORDER BY sequence",
            &[&source_id.as_str()],
        )
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    let between = |after: usize, through: usize| -> u64 {
        stored[after..through]
            .iter()
            .map(|bytes| u64::try_from(*bytes).unwrap())
            .sum()
    };
    assert_eq!(log.entry_bytes_between(0, 6).unwrap(), between(0, 6));
    assert_eq!(log.entry_bytes_between(2, 5).unwrap(), between(2, 5));
    assert_eq!(log.entry_bytes_between(6, 6).unwrap(), 0);

    let server_now_ms = |control: &mut Client| -> i64 {
        control
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
                &[],
            )
            .unwrap()
            .get(0)
    };
    let wait_past = |control: &mut Client, at_ms: i64| {
        while server_now_ms(control) <= at_ms {
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    let now_ms = server_now_ms(&mut control);
    let (sooner, later) = (now_ms + 3_000, now_ms + 6_000);
    log.acquire_plan_retention_lease(PlanId::new("plan-sooner").unwrap(), 2, sooner, now_ms)
        .unwrap();
    log.acquire_plan_retention_lease(PlanId::new("plan-later").unwrap(), 4, later, now_ms)
        .unwrap();
    log.retain_from(6).unwrap();
    assert_eq!(log.minimum_retained().unwrap().sequence, 2);

    wait_past(&mut control, sooner);
    log.retain_from(6).unwrap();
    assert_eq!(log.minimum_retained().unwrap().sequence, 4);
    assert_eq!(log.entry_bytes_between(0, 6).unwrap(), between(3, 6));
    assert!(matches!(
        log.read_after_bounded(2, None, 10, 1024 * 1024),
        Err(LogError::BeforeRetention {
            requested: 2,
            minimum: 4
        })
    ));
    assert_eq!(
        log.read_after_bounded(3, None, 10, 1024 * 1024)
            .unwrap()
            .len(),
        3
    );

    wait_past(&mut control, later);
    log.retain_from(6).unwrap();
    assert_eq!(log.minimum_retained().unwrap().sequence, 6);

    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_plan_retention_lease",
        "canonical_log_retired_record",
        "canonical_log_record",
        "canonical_log_head",
        "canonical_log_retention",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn postgres_plan_lease_racing_retention_is_atomic_and_fails_closed() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);

    let writer =
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore.clone()).unwrap();
    let appender = PostgresFencedCanonicalLog::connect(&url, writer, state_dir()).unwrap();
    for sequence in 1..=3 {
        appender.append_barrier(barrier(&epoch, sequence)).unwrap();
    }
    drop(appender);

    let lease_writer =
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore.clone()).unwrap();
    let retention_writer =
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore.clone()).unwrap();
    let mut lease_log =
        PostgresFencedCanonicalLog::connect(&url, lease_writer, state_dir()).unwrap();
    let mut retention_log =
        PostgresFencedCanonicalLog::connect(&url, retention_writer, state_dir()).unwrap();
    let server_now_ms: i64 = control
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .unwrap()
        .get(0);
    let start = std::sync::Arc::new(std::sync::Barrier::new(3));
    let lease_start = start.clone();
    let retention_start = start.clone();
    let lease = std::thread::spawn(move || {
        lease_start.wait();
        lease_log.acquire_plan_retention_lease(
            PlanId::new("racing-bootstrap-plan").unwrap(),
            2,
            server_now_ms + 60_000,
            server_now_ms,
        )
    });
    let retention = std::thread::spawn(move || {
        retention_start.wait();
        retention_log.retain_from(4)
    });
    start.wait();
    let lease_result = lease.join().unwrap();
    retention.join().unwrap().unwrap();

    let reader = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    let retained = reader.minimum_retained().unwrap().sequence;
    match lease_result {
        Ok(()) => assert_eq!(retained, 2),
        Err(LogError::InvalidRetentionLease) => assert_eq!(retained, 4),
        Err(error) => panic!("unexpected plan-retention race result: {error}"),
    }

    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_plan_retention_lease",
        "canonical_log_build_retention_lease",
        "canonical_log_retired_record",
        "canonical_log_record",
        "canonical_log_head",
        "canonical_log_retention",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}

#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_log_an_earlier_release_wrote_is_refused_at_connect_and_on_read() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    install_slot_enrollment(&mut control, &source_id, &epoch);
    let writer = FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap();
    let log = PostgresFencedCanonicalLog::connect(&url, writer.clone(), state_dir()).unwrap();
    assert_eq!(
        log.append(update(&epoch, 1)).unwrap().position().sequence,
        1
    );
    assert_eq!(
        log.append_barrier(barrier(&epoch, 2))
            .unwrap()
            .position()
            .sequence,
        2
    );
    drop(PostgresFencedCanonicalLog::connect(&url, writer.clone(), state_dir()).unwrap());

    // An earlier release stored each entry as its JSON.
    for (sequence, entry) in [
        (1_i64, CanonicalEntry::Transaction(update(&epoch, 1))),
        (2, CanonicalEntry::Barrier(barrier(&epoch, 2))),
    ] {
        let earlier = serde_json::to_vec(&entry).unwrap();
        assert_eq!(
            control
                .execute(
                    "UPDATE calque_control.canonical_log_record
                        SET entry_bytes = $3, entry_sha256 = $4
                      WHERE source_id = $1 AND sequence = $2",
                    &[
                        &source_id.as_str(),
                        &sequence,
                        &earlier,
                        &hex::encode(Sha256::digest(&earlier)),
                    ],
                )
                .unwrap(),
            1
        );
    }

    let refused = log.read_page_after_bounded(0, None, 16, 1 << 20);
    assert!(matches!(
        &refused,
        Err(LogError::EarlierEntryFormat { state_dir: named }) if named == state_dir()
    ));
    assert!(matches!(
        PostgresFencedCanonicalLog::connect(&url, writer, state_dir()),
        Err(LogError::EarlierEntryFormat { state_dir: named }) if named == state_dir()
    ));
    assert_eq!(
        refused.err().unwrap().to_string(),
        "/srv/calque/state serves a canonical log an earlier release of calque wrote, and calque does not migrate it."
    );

    drop(log);
    remove_source(&mut control, &source_id);
}

/// `value` as PostgreSQL writes an LSN.
fn lsn(value: u64) -> String {
    format!("{:X}/{:X}", value >> 32, value & 0xFFFF_FFFF)
}

/// The barrier at `sequence`, ending at `end_lsn`.
fn barrier_ending_at(epoch: &SourceEpoch, sequence: u64, end_lsn: u64) -> CanonicalBarrier {
    let mut barrier = barrier(epoch, sequence);
    barrier.key.end_lsn = lsn(end_lsn);
    barrier
}

/// A logical slot of the test database, dropped with the guard so a failed
/// test does not leave it holding WAL.
struct LogicalSlot {
    url: String,
    name: String,
}

impl LogicalSlot {
    fn create(url: &str, control: &mut Client) -> Self {
        let name = format!(
            "calque_compaction_{}_{}",
            std::process::id(),
            next_id_ordinal()
        );
        control
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&name],
            )
            .unwrap();
        Self {
            url: url.to_owned(),
            name,
        }
    }

    fn confirmed_flush(&self, control: &mut Client) -> u64 {
        u64::from(
            control
                .query_one(
                    "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
                    &[&self.name],
                )
                .unwrap()
                .get::<_, postgres::types::PgLsn>(0),
        )
    }
}

impl Drop for LogicalSlot {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.url, NoTls) {
            let _ = client.execute("SELECT pg_drop_replication_slot($1)", &[&self.name]);
        }
    }
}

fn retired_sequences(control: &mut Client, source_id: &SourceId) -> Vec<i64> {
    control
        .query(
            "SELECT original_sequence FROM calque_control.canonical_log_retired_record
              WHERE source_id = $1 ORDER BY original_sequence",
            &[&source_id.as_str()],
        )
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

fn compacted_through(control: &mut Client, source_id: &SourceId) -> Option<String> {
    control
        .query_one(
            "SELECT retired_compacted_through_lsn::text
               FROM calque_control.canonical_log_retention WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap()
        .get(0)
}

/// A trimmed entry keeps its identity only while PostgreSQL could send its
/// transaction again: while it ends after the source slot's confirmed flush
/// or after the log's durable checkpoint. A trim under a lost fence, or one
/// that cannot see the enrolled slot, forgets nothing; a later one forgets the
/// backlog; and an entry the log forgot is refused if it ever comes again
/// rather than appended a second time.
#[test]
#[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
fn a_trim_forgets_the_retired_identities_postgresql_cannot_send_again() {
    let url =
        std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
    let source_id = unique_source_id();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut control = Client::connect(&url, NoTls).unwrap();
    let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
    install_active_writer_fence(&mut control, &source_id, &epoch);
    install_slot_enrollment(&mut control, &source_id, &epoch);
    let slot = LogicalSlot::create(&url, &mut control);
    let set_durable = |control: &mut Client, durable: u64| {
        control
            .execute(
                "UPDATE calque_control.source_slot_enrollment
                    SET slot_name = $2, database_name = current_database(),
                        system_identifier = (
                            SELECT system_identifier::text FROM pg_control_system()
                        ),
                        durable_canonical_lsn = $3
                  WHERE source_id = $1",
                &[&source_id.as_str(), &slot.name, &lsn(durable)],
            )
            .unwrap();
    };
    let set_lease = |control: &mut Client, interval: &str| {
        control
            .execute(
                &format!(
                    "UPDATE calque_control.source_fence_lease
                        SET expires_at = clock_timestamp() + interval '{interval}'
                      WHERE source_id = $1"
                ),
                &[&source_id.as_str()],
            )
            .unwrap();
    };
    let flushed = slot.confirmed_flush(&mut control);
    let mut log = PostgresFencedCanonicalLog::connect(
        &url,
        FencedWriterIdentity::new(source_id.clone(), epoch.clone(), 1, datastore).unwrap(),
        state_dir(),
    )
    .unwrap();
    let ends = [
        flushed - 2,
        flushed - 1,
        flushed + 1,
        flushed + 2,
        flushed + 3,
    ];
    for (sequence, end) in (1..).zip(&ends[..4]) {
        log.append_barrier(barrier_ending_at(&epoch, sequence, *end))
            .unwrap();
    }
    let position = |sequence| sync_types::CursorPosition {
        source_epoch: epoch.clone(),
        sequence,
    };

    // The checkpoint is past the slot's confirmed flush, which binds.
    set_durable(&mut control, flushed + 1);
    set_lease(&mut control, "-1 second");
    log.retain_from(5).unwrap();
    assert_eq!(retired_sequences(&mut control, &source_id), [1, 2, 3, 4]);
    assert_eq!(compacted_through(&mut control, &source_id), None);
    set_lease(&mut control, "5 minutes");
    // A slot of that name on another cluster says nothing about this one.
    control
        .execute(
            "UPDATE calque_control.source_slot_enrollment
                SET system_identifier = 'another-cluster' WHERE source_id = $1",
            &[&source_id.as_str()],
        )
        .unwrap();
    log.retain_from(5).unwrap();
    assert_eq!(retired_sequences(&mut control, &source_id), [1, 2, 3, 4]);
    assert_eq!(compacted_through(&mut control, &source_id), None);
    set_durable(&mut control, flushed + 1);
    log.retain_from(5).unwrap();
    assert_eq!(retired_sequences(&mut control, &source_id), [3, 4]);
    assert_eq!(
        compacted_through(&mut control, &source_id),
        Some(lsn(flushed))
    );
    assert!(matches!(
        log.append_barrier(barrier_ending_at(&epoch, 2, ends[1])),
        Err(LogError::ReplayBehindCompaction)
    ));
    assert_eq!(
        log.append_barrier(barrier_ending_at(&epoch, 3, ends[2]))
            .unwrap(),
        AppendOutcome::AlreadyPresent(position(3))
    );
    assert!(matches!(
        log.append_group(
            vec![
                CanonicalEntry::Barrier(barrier_ending_at(&epoch, 5, ends[4])),
                CanonicalEntry::Barrier(barrier_ending_at(&epoch, 1, ends[0])),
            ],
            None,
        ),
        Err(LogError::ReplayBehindCompaction)
    ));
    assert_eq!(log.head().unwrap(), position(4));
    assert_eq!(
        log.append_barrier(barrier_ending_at(&epoch, 5, ends[4]))
            .unwrap(),
        AppendOutcome::Appended(position(5))
    );

    // The slot moves past the checkpoint, which binds now. The entry trimmed
    // after the checkpoint keeps its identity.
    control
        .execute(
            "SELECT pg_replication_slot_advance($1, $2::text::pg_lsn)",
            &[&slot.name, &lsn(flushed + 3)],
        )
        .unwrap();
    assert!(slot.confirmed_flush(&mut control) >= flushed + 3);
    set_durable(&mut control, flushed + 2);
    log.retain_from(6).unwrap();
    assert_eq!(retired_sequences(&mut control, &source_id), [5]);
    assert_eq!(
        compacted_through(&mut control, &source_id),
        Some(lsn(flushed + 2))
    );
    assert!(matches!(
        log.append_barrier(barrier_ending_at(&epoch, 4, ends[3])),
        Err(LogError::ReplayBehindCompaction)
    ));
    assert_eq!(
        log.append_barrier(barrier_ending_at(&epoch, 5, ends[4]))
            .unwrap(),
        AppendOutcome::AlreadyPresent(position(5))
    );

    drop(log);
    let mut cleanup = control.transaction().unwrap();
    for table in [
        "canonical_log_retired_record",
        "canonical_log_record",
        "canonical_log_head",
        "canonical_log_retention",
        "source_slot_enrollment",
        "canonical_log_writer_fence",
        "source_fence_lease",
    ] {
        cleanup
            .execute(
                &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                &[&source_id.as_str()],
            )
            .unwrap();
    }
    cleanup.commit().unwrap();
}
