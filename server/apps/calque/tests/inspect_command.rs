//! `calque inspect` run through the binary: its database URL refusals, and the
//! enrollment blockers it reports against a live PostgreSQL 18.
//!
//! The live test is `#[ignore]`d and runs through
//! `scripts/live-postgres-suite.sh`; without `TEST_DATABASE_URL` it fails
//! rather than silently passes.

use std::{
    net::TcpListener,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use postgres::{Client, NoTls};

const TLS_REQUIRED: &str = "is reached over verified TLS";

/// Runs `calque inspect` on `public.issues` with `CALQUE_DATABASE_URL` set to
/// `database_url` and returns `(status success, stderr)`.
fn inspect(database_url: &str) -> (bool, String) {
    inspect_table(database_url, "calque_test", "public.issues")
}

fn inspect_table(database_url: &str, publication: &str, table: &str) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "inspect",
            "--source",
            "primary",
            "--publication",
            publication,
            "--table",
            table,
        ])
        .env("CALQUE_DATABASE_URL", database_url)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

/// A loopback listener that counts the connections it is offered and closes
/// each one at once, so a client that reaches it fails instead of waiting for
/// a PostgreSQL handshake.
fn counting_listener() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    thread::spawn(move || {
        for stream in listener.incoming() {
            if stream.is_ok() {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    (port, accepted)
}

/// `inspect` is held to the database URL guard: a host name without TLS is
/// refused before it is resolved or connected to. `localhost` makes that observable, because
/// resolving it would land the connection on the counting listener. A
/// numeric loopback address on the same listener is the control: it is
/// connected to, so the listener does see a connection when one is made.
/// An explicit `sslmode=disable` on the host name is connected to as well,
/// after a warning that names the host.
#[test]
fn inspect_refuses_a_host_name_before_connecting() {
    let (success, stderr) = inspect("postgresql://db.example.com/app");
    assert!(!success);
    assert!(stderr.contains(TLS_REQUIRED), "{stderr}");

    let (port, accepted) = counting_listener();
    let (success, stderr) = inspect(&format!("postgresql://localhost:{port}/app"));
    assert!(!success);
    assert!(stderr.contains(TLS_REQUIRED), "{stderr}");
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        0,
        "inspect connected to a host the guard refuses"
    );

    let (success, stderr) = inspect(&format!("postgresql://127.0.0.1:{port}/app"));
    assert!(!success);
    assert!(!stderr.contains(TLS_REQUIRED), "{stderr}");
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "{stderr}");

    let (success, stderr) = inspect(&format!(
        "postgresql://localhost:{port}/app?sslmode=disable"
    ));
    assert!(!success);
    assert!(!stderr.contains(TLS_REQUIRED), "{stderr}");
    assert!(
        stderr.contains("warning:") && stderr.contains("localhost"),
        "{stderr}"
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 2, "{stderr}");
}

/// A table and its publication on the live source, dropped when the test ends.
struct LiveRelation {
    client: Client,
    table: String,
    publication: String,
}

impl LiveRelation {
    fn create(columns: &str) -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL is required; run through scripts/live-postgres-suite.sh");
        let mut client = Client::connect(&database_url, NoTls).unwrap();
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let table = format!("inspect_blockers_{suffix}");
        let publication = format!("inspect_blockers_{suffix}");
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table} ({columns}); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table};"
            ))
            .unwrap();
        Self {
            client,
            table,
            publication,
        }
    }
}

impl Drop for LiveRelation {
    fn drop(&mut self) {
        let _ = self.client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}; DROP TABLE IF EXISTS public.{};",
            self.publication, self.table
        ));
    }
}

/// `inspect` reports each blocker in the enrollment report's own form,
/// `CODE: message [table]`, rather than as a debug dump of the diagnostics,
/// and each message names the change that clears it.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn inspect_names_each_blocker_and_its_fix() {
    let mut relation = LiveRelation::create(
        "id uuid NOT NULL, title text, \
         title_length integer GENERATED ALWAYS AS (length(title)) STORED",
    );
    let table = format!("public.{}", relation.table);
    relation
        .client
        .batch_execute(&format!("ALTER TABLE {table} REPLICA IDENTITY NOTHING"))
        .unwrap();
    let (success, stderr) = inspect_table(
        &std::env::var("TEST_DATABASE_URL").unwrap(),
        &relation.publication,
        &table,
    );
    assert!(!success);
    assert!(
        stderr.starts_with("calque: source inspection is not ready: "),
        "{stderr}"
    );
    for blocker in [
        format!(
            "PRIMARY_KEY_REQUIRED: table {table} has no primary key; \
             add one with ALTER TABLE {table} ADD PRIMARY KEY (...) [{table}]"
        ),
        format!(
            "REPLICA_IDENTITY_INSUFFICIENT: replica identity NOTHING is not supported; \
             use DEFAULT, which is the primary key, or FULL: \
             ALTER TABLE {table} REPLICA IDENTITY DEFAULT [{table}]"
        ),
        format!(
            "GENERATED_COLUMN_UNSUPPORTED: generated columns are not supported, \
             because logical replication does not carry their values by default; \
             leave the column out of the publication's column list, replace it with \
             an ordinary one, or compute the value in the application \
             [{table}.title_length]"
        ),
    ] {
        assert!(stderr.contains(&blocker), "{blocker}\n{stderr}");
    }
    assert!(!stderr.contains("EnrollmentDiagnostic"), "{stderr}");
}

/// `inspect` warns on stderr, after a successful inspection, exactly when the
/// server bounds neither the WAL a slot may hold back nor how long it may sit
/// idle.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn inspect_warns_when_nothing_bounds_the_wal_a_slot_holds() {
    let mut relation = LiveRelation::create("id bigint PRIMARY KEY, title text");
    let table = format!("public.{}", relation.table);
    let unbounded: bool = relation
        .client
        .query_one(
            "SELECT current_setting('max_slot_wal_keep_size')::text = '-1' \
                AND current_setting('idle_replication_slot_timeout')::text = '0'",
            &[],
        )
        .unwrap()
        .get(0);
    let (success, stderr) = inspect_table(
        &std::env::var("TEST_DATABASE_URL").unwrap(),
        &relation.publication,
        &table,
    );
    assert!(success, "{stderr}");
    let warning = "calque: warning: neither max_slot_wal_keep_size nor \
                   idle_replication_slot_timeout is set on this PostgreSQL";
    assert_eq!(stderr.contains(warning), unbounded, "{stderr}");
}
