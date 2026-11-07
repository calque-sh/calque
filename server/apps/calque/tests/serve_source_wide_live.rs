//! `calque serve` on a source-wide dataset, against a live PostgreSQL 18.
//!
//! Checks that the published artifact is one the public client mounts, that a
//! grant carrying no command capability reaches no write surface, that a
//! `serve` killed with SIGKILL under live writers leaves a log matching the
//! source table, and that the scrape is served on the metrics listener alone.
//!
//! These tests are `#[ignore]`d and run through
//! `scripts/live-postgres-suite.sh`; without `TEST_DATABASE_URL` they fail
//! rather than silently pass.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io::{BufRead, BufReader, Read as _, Write as _},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, ChildStderr, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use dataset_grant_issuer::GrantIssuer;
use postgres::{Client, NoTls};
use serde_json::Value;

const GRANT_KEY_ENV: &str = "CALQUE_LIVE_SOURCE_WIDE_GRANT_KEY";
const GRANT_AUDIENCE: &str = "source-wide.test";
const DATABASE_ENV: &str = "CALQUE_LIVE_SOURCE_WIDE_DATABASE_URL";
const READY_TIMEOUT: Duration = Duration::from_secs(300);
/// The artifact profile and encoding set `bootstrap-http` accepts.
const ARTIFACT_PROFILE: &str = "chromium-native-artifact-v1";

/// One `calque serve` at a time from this binary: each drives a fenced
/// canonical writer, a replication slot and a control schema against the one
/// shared PostgreSQL.
static ONE_SERVER_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn exclusive() -> std::sync::MutexGuard<'static, ()> {
    ONE_SERVER_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn reserved_port() -> u16 {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    port
}

fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL is required; run through scripts/live-postgres-suite.sh")
}

/// A disposable source-wide dataset: one table, one publication, one slot, and
/// the application backend that mints the grants these tests present.
struct Fixture {
    suffix: String,
    directory: tempfile::TempDir,
    issuer: GrantIssuer,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self {
            suffix: format!("{name}_{}", uuid::Uuid::new_v4().simple()),
            directory: tempfile::tempdir().unwrap(),
            issuer: GrantIssuer::generate("live-source-wide-key", GRANT_AUDIENCE)
                .unwrap()
                .0,
        }
    }

    /// A source-wide grant: no attributes, because the compiled policy binds
    /// none. It is still a real signed grant, verified by the served process.
    fn grant(&self, generation: u64, lifetime_seconds: u64) -> String {
        self.issuer
            .issue(
                "source-wide-member".to_owned(),
                std::collections::BTreeMap::new(),
                generation,
                unix_seconds(),
                lifetime_seconds,
            )
            .unwrap()
    }

    fn table(&self) -> String {
        format!("tasks_{}", self.suffix)
    }

    fn publication(&self) -> String {
        format!("calque_pub_{}", self.suffix)
    }

    fn slot(&self) -> String {
        format!("calque_slot_{}", self.suffix)
    }

    fn source_id(&self) -> String {
        format!("primary-{}", self.suffix)
    }

    fn seed(&self, rows: usize) {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (
                     id uuid NOT NULL PRIMARY KEY,
                     title text NOT NULL,
                     status text NOT NULL
                 );
                 ALTER TABLE {table} REPLICA IDENTITY FULL;
                 CREATE PUBLICATION {publication} FOR TABLE {table};",
                table = self.table(),
                publication = self.publication(),
            ))
            .unwrap();
        let mut transaction = client.transaction().unwrap();
        let statement = transaction
            .prepare(&format!(
                "INSERT INTO {} (id, title, status) VALUES ($1, $2, $3)",
                self.table()
            ))
            .unwrap();
        for row in 0..rows {
            let mut id = [0_u8; 16];
            id[0..8].copy_from_slice(&(row as u64 + 1).to_be_bytes());
            transaction
                .execute(
                    &statement,
                    &[&uuid::Uuid::from_bytes(id), &format!("task {row}"), &"open"],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    fn drop_source(&self) {
        let Ok(mut client) = Client::connect(&database_url(), NoTls) else {
            return;
        };
        let _ = client.batch_execute(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                 WHERE slot_name = '{slot}';
             DELETE FROM calque_control.source_fence_lease WHERE source_id = '{source}';
             DELETE FROM calque_control.source_slot_enrollment WHERE source_id = '{source}';
             DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};",
            slot = self.slot(),
            source = self.source_id(),
            publication = self.publication(),
            table = self.table(),
        ));
    }

    /// The source-wide manifest, in the shape `calque init` renders: one
    /// collection and one crud command for it.
    fn config(&self, commands: Value) -> PathBuf {
        let document = serde_json::json!({
            "application": { "id": "source-wide-ops", "version": "1.0.0" },
            "protocol": { "manifest": 1, "artifact": 1, "changeStream": 1, "mutation": 1 },
            "sources": [{
                "id": self.source_id(),
                "connectionEnv": DATABASE_ENV,
                "publication": self.publication(),
                "slot": self.slot(),
            }],
            "scopes": [{
                "id": "all",
                "source": self.source_id(),
                "kind": "source-wide",
                "dataVersion": 1,
            }],
            "collections": [{
                "id": self.table(),
                "source": self.source_id(),
                "kind": "table",
                "relation": { "schema": "public", "table": self.table() },
                "columns": [
                    { "name": "id", "postgresType": "uuid", "nullable": false },
                    { "name": "title", "postgresType": "text", "nullable": false },
                    { "name": "status", "postgresType": "text", "nullable": false },
                ],
                "primaryKey": ["id"],
                "scope": "all",
                "adapter": "direct-columnar",
                "encoding": "calque-columnar",
            }],
            "commands": commands,
        });
        let path = self.directory.path().join("calque.config.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        path
    }

    fn default_commands(&self) -> Value {
        serde_json::json!([{
            "id": "edit-task",
            "kind": "crud",
            "collection": self.table(),
            "operations": ["update"],
            "writableColumns": ["title", "status"],
        }])
    }
}

/// A `calque serve` child, killed when the guard drops.
struct Serving {
    child: Child,
    lines: Vec<String>,
    stderr: StderrTail,
    port: u16,
    stopped: bool,
}

type StderrTail = Arc<Mutex<VecDeque<String>>>;

impl Serving {
    /// Stops the process the way an operator does, and returns whether it
    /// exited successfully. `serve` releases its fence lease and replication
    /// slot on SIGTERM.
    fn shutdown(mut self) -> bool {
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let status = self.child.wait().unwrap();
        self.stopped = true;
        status.success()
    }

    /// Ends the process the way a crash does: SIGKILL, with no chance to
    /// release its lease or slot or to finish anything in flight.
    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.stopped = true;
    }

    fn signal(&self, signal: &str) {
        let status = Command::new("kill")
            .args([signal, &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success(), "kill {signal} failed");
    }

    fn line(&self, key: &str) -> Option<&str> {
        self.lines
            .iter()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let tail = self
                .stderr
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let last = tail.iter().skip(tail.len().saturating_sub(40));
            eprintln!(
                "serve's last stderr lines:\n{}",
                last.cloned().collect::<Vec<_>>().join("\n")
            );
        }
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Starts `calque serve` and reads its startup lines until it reports
/// readiness or exits.
fn serve(fixture: &Fixture, commands: Value) -> Serving {
    serve_with(fixture, commands, &[])
}

/// [`serve`], with `extra` arguments after the ones every test passes.
fn serve_with(fixture: &Fixture, commands: Value, extra: &[&str]) -> Serving {
    let key_environment = fixture.issuer.verification_key_environment().unwrap();
    let port = reserved_port();
    let started = Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "serve",
            "--config",
            fixture.config(commands).to_str().unwrap(),
            "--state-dir",
            fixture.directory.path().join("state").to_str().unwrap(),
            "--grant-audience",
            GRANT_AUDIENCE,
            "--grant-verification-key-env",
            GRANT_KEY_ENV,
            "--allow-origin",
            "http://127.0.0.1:5173",
            "--port",
            &port.to_string(),
        ])
        .args(extra)
        .env(DATABASE_ENV, database_url())
        .env(GRANT_KEY_ENV, key_environment)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (stderr, reader) = keep_stderr_tail(child.stderr.take().unwrap());

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let mut lines = Vec::new();
    let mut ready = false;
    for line in stdout.lines() {
        let line = line.unwrap();
        ready = line.starts_with("Calque serve is ready");
        lines.push(line);
        if ready {
            break;
        }
        assert!(
            started.elapsed() < READY_TIMEOUT,
            "serve did not report readiness within {READY_TIMEOUT:?}: {lines:#?}"
        );
    }
    if !ready {
        let _ = child.wait();
        let _ = reader.join();
        panic!(
            "serve exited before readiness.\nstdout: {lines:#?}\nstderr: {:#?}",
            stderr.lock().unwrap()
        );
    }
    Serving {
        child,
        lines,
        stderr,
        port,
        stopped: false,
    }
}

/// Keeps the last lines `serve` writes to stderr. A thread reads the pipe
/// from the start: once it is full, each of serve's threads blocks on its next
/// log line.
fn keep_stderr_tail(pipe: ChildStderr) -> (StderrTail, std::thread::JoinHandle<()>) {
    let tail = Arc::new(Mutex::new(VecDeque::new()));
    let sink = Arc::clone(&tail);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut tail = sink.lock().unwrap();
            if tail.len() == 200 {
                tail.pop_front();
            }
            tail.push_back(line);
        }
    });
    (tail, reader)
}

/// Runs `calque serve` to completion, for the refusal cases, and returns
/// `(success, stdout, stderr)`.
///
/// The grant verification key is absent, so a manifest that passes every
/// check stops at the grant configuration instead of serving forever; the
/// control case reaches that later refusal.
fn serve_to_completion(fixture: &Fixture, commands: Value, state: &str) -> (bool, String, String) {
    let port = reserved_port();
    let output = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "serve",
            "--config",
            fixture.config(commands).to_str().unwrap(),
            "--state-dir",
            fixture.directory.path().join(state).to_str().unwrap(),
            "--grant-audience",
            GRANT_AUDIENCE,
            "--grant-verification-key-env",
            "CALQUE_ABSENT_SOURCE_WIDE_GRANT_KEY",
            "--allow-origin",
            "http://127.0.0.1:5173",
            "--port",
            &port.to_string(),
        ])
        .env(DATABASE_ENV, database_url())
        .env_remove("CALQUE_ABSENT_SOURCE_WIDE_GRANT_KEY")
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

impl HttpResponse {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| {
            panic!(
                "response body is not JSON ({error}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

/// One HTTP/1.1 exchange, without a client dependency. Every request closes
/// its connection, so the body is whatever the server wrote before hanging up.
fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Option<&[u8]>,
) -> HttpResponse {
    http_to_host(
        port,
        &format!("127.0.0.1:{port}"),
        method,
        path,
        headers,
        body,
    )
}

/// [`http`], sending `host` as the `Host` header.
fn http_to_host(
    port: u16,
    host: &str,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Option<&[u8]>,
) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let timeout = Duration::from_secs(120);
    stream.set_read_timeout(Some(timeout)).unwrap();
    stream.set_write_timeout(Some(timeout)).unwrap();
    let mut request = format!("{method} {path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n");
    if let Some(body) = body {
        request.push_str(&format!("content-length: {}\r\n", body.len()));
    }
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).unwrap();
    if let Some(body) = body {
        stream.write_all(body).unwrap();
    }
    stream.flush().unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response has no header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("response has no status code");
    HttpResponse {
        status,
        body: raw[split + 4..].to_vec(),
    }
}

/// A bootstrap request in the shape `bootstrap-http` accepts: the artifact
/// profile header, the three body encodings in order, and the relation name
/// the server resolves to a wire table id. `accept_encoding` narrows what the
/// server may choose, which is how a test asks for the container itself.
fn bootstrap(port: u16, grant: &str, relation: &str, accept_encoding: &str) -> HttpResponse {
    let document = serde_json::json!({
        "protocolVersion": 1,
        "requiredTables": [relation],
        "cachedArtifactHints": [],
        "acceptedEncodings": ["zstd", "identity"],
        "memoryBudgetBytes": 1_073_741_824_u64,
        "clientHeapReserveBytes": 67_108_864_u64,
        "clientInstanceId": "source-wide-live",
        "deviceId": "source-wide-live-device",
    });
    http(
        port,
        "POST",
        "/v1/bootstrap",
        &[
            ("authorization", format!("Bearer {grant}")),
            ("content-type", "application/json".to_owned()),
            ("accept", "application/json".to_owned()),
            ("accept-encoding", accept_encoding.to_owned()),
            ("x-calque-artifact-profile", ARTIFACT_PROFILE.to_owned()),
        ],
        Some(serde_json::to_vec(&document).unwrap().as_slice()),
    )
}

/// The client's direct-columnar engine refuses any artifact whose schema is
/// not `format: "calque-direct-columnar"`.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn serve_publishes_a_direct_columnar_artifact_the_public_client_accepts() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("columnar");
    fixture.seed(32);
    let serving = serve(&fixture, fixture.default_commands());

    assert_eq!(serving.line("scope_mode"), Some("source-wide"));
    assert_eq!(serving.line("ready"), Some("true"));

    // `identity` only, so the bytes fetched below are the container itself
    // rather than a compressed representation of it.
    let response = bootstrap(
        serving.port,
        &fixture.grant(1, 600),
        &format!("public.{}", fixture.table()),
        "identity",
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let plan = response.json();
    let artifact = &plan["artifacts"][0];
    let runtime = &artifact["runtime"];

    // What the client checks, checked here in the same words.
    assert_eq!(runtime["format"], "directColumnar", "{runtime}");
    assert_eq!(
        runtime["schema"]["format"], "calque-direct-columnar",
        "{runtime}"
    );
    assert_eq!(
        runtime["schema"]["tables"][0]["name"],
        Value::String(fixture.table()),
        "{runtime}"
    );
    assert_eq!(runtime["rowCount"].as_u64(), None, "{runtime}");
    assert_eq!(
        runtime["tables"][0]["rowCount"].as_u64(),
        Some(32),
        "{runtime}"
    );

    // The bytes the plan points at are the direct-columnar container, whose
    // magic is `CALQART1`.
    let representation = &artifact["selectedRepresentation"];
    assert_eq!(representation["encoding"], "identity", "{representation}");
    let download = representation["downloadUrl"]
        .as_str()
        .expect("plan names its bytes");
    let path = download
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map_or(download, |(_, path)| path);
    let bytes = http(
        serving.port,
        "GET",
        &format!("/{}", path.trim_start_matches('/')),
        &[("accept-encoding", "identity".to_owned())],
        None,
    );
    assert_eq!(
        bytes.status,
        200,
        "{}",
        String::from_utf8_lossy(&bytes.body)
    );
    assert_eq!(
        &bytes.body[..8],
        b"CALQART1",
        "artifact is not a direct-columnar container"
    );

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}

/// The startup contract, end to end: the process starts, `/readyz` answers,
/// a bootstrap is served, the verified grant that served it reaches no
/// mutation route, and SIGTERM ends the process with a zero exit status.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn serve_starts_reaches_readyz_serves_a_bootstrap_and_shuts_down_cleanly() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("readyz");
    fixture.seed(8);
    let serving = serve(&fixture, fixture.default_commands());

    assert_eq!(serving.line("commands"), Some("1"));
    assert_eq!(
        serving.line("mutations"),
        Some("mounted-per-collection-command-grant"),
        "the startup contract has to say what the mutation route is mounted behind"
    );
    assert_eq!(
        serving.line("mutation_commands"),
        Some(format!("{}:update", fixture.table()).as_str()),
        "and exactly which collections and operations it mounts"
    );
    assert_eq!(serving.line("listener"), Some("loopback"));
    assert_eq!(
        serving.line("expected_host"),
        Some(format!("127.0.0.1:{}", serving.port).as_str())
    );
    // Not asked for one, the process starts no metrics listener and says so,
    // and the HTTP listener has no scrape of its own.
    assert_eq!(serving.line("metrics_listener"), Some("off"));
    assert_eq!(serving.line("metrics_expected_host"), None);
    assert_eq!(http(serving.port, "GET", "/metrics", &[], None).status, 404);
    assert_eq!(
        serving.line("authorization"),
        Some("verified-dataset-grants")
    );

    assert_eq!(http(serving.port, "GET", "/healthz", &[], None).status, 204);
    assert_eq!(http(serving.port, "GET", "/readyz", &[], None).status, 204);

    let grant = fixture.grant(1, 600);
    let response = bootstrap(
        serving.port,
        &grant,
        &format!("public.{}", fixture.table()),
        "zstd, br, identity",
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );

    // The grant that just planned a read carries no command capability, so
    // the mounted write route answers it with the 404 an unmounted route
    // gives. `serve_command_grant_live` covers a capable grant.
    let scope_token = response.json()["authorization"]["scopeToken"]
        .as_str()
        .expect("a served plan carries a scope token")
        .to_owned();
    assert!(!scope_token.is_empty());
    let mutation = http(
        serving.port,
        "POST",
        "/v1/mutations",
        &[
            ("content-type", "application/octet-stream".to_owned()),
            ("authorization", format!("Bearer {grant}")),
        ],
        Some(b""),
    );
    assert_eq!(
        mutation.status, 404,
        "a grant carrying no command capability must reach no write surface"
    );
    // An anonymous caller gets the same 404, for any method: a 401 would let
    // anyone discover the mount state without presenting anything.
    for (method, headers) in [
        (
            "POST",
            vec![("content-type", "application/octet-stream".to_owned())],
        ),
        ("GET", Vec::new()),
        ("PUT", Vec::new()),
    ] {
        let anonymous = http(serving.port, method, "/v1/mutations", &headers, Some(b""));
        assert_eq!(
            anonymous.status, 404,
            "an anonymous {method} learned whether the write surface is mounted"
        );
    }

    let port = serving.port;
    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    // Readiness is gone with the process, and the port is free again.
    assert!(TcpListener::bind(("127.0.0.1", port)).is_ok());
    fixture.drop_source();
}

/// With `--metrics-bind-address`, the scrape is served there, to the Host of
/// that binding alone, and the HTTP listener still answers `/metrics` as a
/// path it does not route.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn serve_serves_the_scrape_on_its_metrics_listener_alone() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("metrics");
    fixture.seed(4);
    let metrics_port = reserved_port();
    let metrics_address = format!("127.0.0.1:{metrics_port}");
    let serving = serve_with(
        &fixture,
        fixture.default_commands(),
        &["--metrics-bind-address", &metrics_address],
    );

    assert_eq!(serving.line("metrics_listener"), Some("loopback"));
    assert_eq!(
        serving.line("metrics_expected_host"),
        Some(metrics_address.as_str())
    );

    let scrape = http(metrics_port, "GET", "/metrics", &[], None);
    assert_eq!(scrape.status, 200);
    let body = String::from_utf8(scrape.body).unwrap();
    assert!(
        body.contains("# TYPE calque_canonical_entries_appended counter"),
        "{body}"
    );
    assert!(body.ends_with("# EOF\n"), "{body}");

    // The Host check is the metrics binding's own: the HTTP listener's Host
    // and a name for the same address are both refused.
    for host in [
        format!("127.0.0.1:{}", serving.port),
        format!("localhost:{metrics_port}"),
    ] {
        assert_eq!(
            http_to_host(metrics_port, &host, "GET", "/metrics", &[], None).status,
            421,
            "{host}"
        );
    }
    // The metrics listener routes the scrape and nothing else.
    for path in ["/healthz", "/readyz"] {
        assert_eq!(
            http(metrics_port, "GET", path, &[], None).status,
            404,
            "{path}"
        );
    }
    assert_eq!(http(serving.port, "GET", "/metrics", &[], None).status, 404);
    assert_eq!(http(serving.port, "GET", "/readyz", &[], None).status, 204);

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    // The metrics listener stops with the process.
    assert!(TcpListener::bind(("127.0.0.1", metrics_port)).is_ok());
    fixture.drop_source();
}

/// A manifest whose commands do not match the enrolled tables is refused
/// before CDC, the listener, and readiness -- not at the first request that
/// names a command.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn serve_refuses_an_unadvertised_enrolled_table_before_binding() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("unadvertised");
    fixture.seed(4);

    // The control: a manifest that advertises the enrolled table, with the
    // writable columns the catalog gives it, passes the command check and
    // stops at the absent grant key instead.
    let (success, stdout, stderr) = serve_to_completion(
        &fixture,
        serde_json::json!([
            {
                "id": "edit-task",
                "kind": "crud",
                "collection": fixture.table(),
                "operations": ["update"],
                "writableColumns": ["title", "status"],
            },
        ]),
        "control",
    );
    assert!(!success);
    assert!(
        !stderr.contains("E_SERVE_COMMAND"),
        "a matching manifest must pass the command check: {stderr}"
    );
    assert!(
        stderr.contains("CALQUE_ABSENT_SOURCE_WIDE_GRANT_KEY"),
        "the control must reach the grant configuration: {stderr}"
    );
    assert!(!stdout.contains("ready=true"), "{stdout}");

    // Nothing advertises crud for the enrolled table the mutation route would
    // mount one for.
    let (success, stdout, stderr) = serve_to_completion(&fixture, serde_json::json!([]), "refused");
    assert!(!success);
    assert!(
        stderr.contains("E_SERVE_COMMAND_INVALID"),
        "a manifest with no crud command is refused: {stderr}"
    );
    assert!(
        !stderr.contains("CALQUE_ABSENT_SOURCE_WIDE_GRANT_KEY"),
        "the command manifest is refused before the grant configuration: {stderr}"
    );
    assert!(!stdout.contains("ready=true"), "{stdout}");
    fixture.drop_source();
}

/// How long a restarted `serve` gets to wait out the killed one's lease and
/// catch up with what was committed while nothing was reading.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(300);

/// `serve` is killed with SIGKILL while it works through a backlog, a new
/// `serve` takes the source over on the same state, and the canonical log it
/// leaves holds every committed transaction once, in commit order, and replays
/// over the snapshot to the table PostgreSQL holds.
///
/// Group commit acknowledges the WAL only after a group's fenced commit, so the
/// kill can land between the two, which replays transactions the log already
/// has, or inside a group that never commits, which replays ones it lacks. Four
/// writers keep committing throughout, the downtime included.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_serve_killed_mid_stream_resumes_with_every_transaction_exactly_once() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("killed");
    fixture.seed(64);
    let source_id = fixture.source_id();
    let mut control = Client::connect(&database_url(), NoTls).unwrap();
    // What the snapshot holds: `serve` creates its slot at startup, and
    // nothing writes to the table until it is ready.
    let snapshot = source_rows(&mut control, &fixture.table());

    // Re-cutting stays off, so the log keeps every record from the snapshot
    // on and replays to the table.
    let no_recut = ["--recut-tail-ratio", "0"];
    let serving = serve_with(&fixture, fixture.default_commands(), &no_recut);
    let stderr = Arc::clone(&serving.stderr);
    let stop = Arc::new(AtomicBool::new(false));
    let _stop_on_drop = StopOnDrop(Arc::clone(&stop));
    let committed = Arc::new(AtomicUsize::new(0));
    let writers = (0..4_u8)
        .map(|writer| {
            let table = fixture.table();
            let (stop, committed) = (Arc::clone(&stop), Arc::clone(&committed));
            std::thread::spawn(move || write_until_stopped(&table, writer, &stop, &committed))
        })
        .collect::<Vec<_>>();
    wait_until("serve appends the first transactions", &stderr, || {
        canonical_head(&mut control, &source_id) >= 200
    });

    // A backlog, so the kill lands while `serve` is appending groups: stopped,
    // it reads nothing while the writers commit, and resumed, it is killed
    // as soon as its first group lands. The stop stays well inside the lease
    // TTL, so the resumed process still holds its lease.
    serving.signal("-STOP");
    let head_at_stop = canonical_head(&mut control, &source_id);
    let committed_at_stop = committed.load(Ordering::Acquire);
    let stopped = Instant::now();
    while committed.load(Ordering::Acquire) < committed_at_stop + 1_500
        && stopped.elapsed() < Duration::from_secs(3)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let backlog = committed.load(Ordering::Acquire);
    serving.signal("-CONT");
    let resumed = Instant::now();
    while canonical_head(&mut control, &source_id) == head_at_stop {
        assert!(
            resumed.elapsed() < CATCH_UP_TIMEOUT,
            "the resumed serve appended nothing"
        );
    }
    serving.kill();
    let head_at_kill = canonical_head(&mut control, &source_id);
    assert!(
        usize::try_from(head_at_kill).unwrap() < backlog,
        "the kill has to land before serve catches up: {head_at_kill} records appended, \
         {backlog} transactions committed before it resumed"
    );

    let serving = serve_with(&fixture, fixture.default_commands(), &no_recut);
    let stderr = Arc::clone(&serving.stderr);
    wait_until("the restarted serve appends past the kill", &stderr, || {
        canonical_head(&mut control, &source_id) >= head_at_kill + 200
    });
    stop.store(true, Ordering::Release);
    let writers = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();
    for xids in &writers {
        let last = i64::from(*xids.last().unwrap());
        wait_until(
            "the restarted serve appends each writer's last transaction",
            &stderr,
            || logged(&mut control, &source_id, last),
        );
    }
    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");

    // The log trims only what lies behind the snapshot's cut: the retired
    // sequences and the kept ones run from 1 together, and a writer's
    // transaction that was retired is counted lost below.
    let retired = control
        .query(
            "SELECT original_sequence FROM calque_control.canonical_log_retired_record
              WHERE source_id = $1 ORDER BY original_sequence",
            &[&source_id],
        )
        .unwrap()
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<Vec<_>>();
    let records = canonical_records(&mut control, &source_id);
    let sequences = retired
        .iter()
        .copied()
        .chain(records.iter().map(|(sequence, _)| *sequence));
    assert!(
        sequences.eq(1..=i64::try_from(retired.len() + records.len()).unwrap()),
        "the log's sequences are not contiguous from 1"
    );
    // Canonical order is commit order: each record ends later in the WAL than
    // the one before it.
    let ends = records
        .iter()
        .map(|(_, entry)| lsn(entry["record"]["key"]["endLsn"].as_str().unwrap()))
        .collect::<Vec<_>>();
    assert!(
        ends.windows(2).all(|pair| pair[0] < pair[1]),
        "the log is not in commit order"
    );

    // Each writer commits in program order, so its transactions have to
    // appear in the log exactly as it committed them, and nothing else may.
    let appended = records
        .iter()
        .filter(|(_, entry)| entry["recordKind"] == "TRANSACTION")
        .map(|(_, entry)| {
            u32::try_from(entry["record"]["key"]["postgresXid"].as_u64().unwrap()).unwrap()
        })
        .collect::<Vec<_>>();
    let committed_total = writers.iter().map(Vec::len).sum::<usize>();
    let mut counts = HashMap::<u32, usize>::new();
    for xid in &appended {
        *counts.entry(*xid).or_default() += 1;
    }
    let duplicated = counts.values().filter(|count| **count > 1).count();
    let lost = writers
        .iter()
        .flatten()
        .filter(|xid| !counts.contains_key(xid))
        .count();
    assert_eq!(
        (appended.len(), lost, duplicated),
        (committed_total, 0, 0),
        "(transactions logged, lost, duplicated) against the {committed_total} committed"
    );
    for (writer, xids) in writers.iter().enumerate() {
        let own = xids.iter().collect::<std::collections::HashSet<_>>();
        let logged = appended
            .iter()
            .filter(|xid| own.contains(xid))
            .copied()
            .collect::<Vec<_>>();
        assert!(
            logged == *xids,
            "writer {writer}'s transactions are out of its commit order in the log"
        );
    }

    let replayed = replay(snapshot, &records);
    let table = source_rows(&mut control, &fixture.table());
    let differing = replayed
        .keys()
        .chain(table.keys())
        .find(|id| replayed.get(*id) != table.get(*id));
    assert!(
        differing.is_none(),
        "the log replays to {} rows and the table holds {}; they differ first at {:?}: \
         log {:?}, table {:?}",
        replayed.len(),
        table.len(),
        differing.map(|id| uuid::Uuid::from_bytes(*id)),
        differing.and_then(|id| replayed.get(id)),
        differing.and_then(|id| table.get(id)),
    );
    fixture.drop_source();
}

/// A table's rows by id, as `(title, status)`.
type Rows = BTreeMap<[u8; 16], (String, String)>;

struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn written_id(writer: u8, transaction: u64, row: u8) -> uuid::Uuid {
    let mut id = [0_u8; 16];
    id[0] = 0xAA;
    id[1] = writer;
    id[2..10].copy_from_slice(&transaction.to_be_bytes());
    id[10] = row;
    uuid::Uuid::from_bytes(id)
}

/// Commits into `table` as fast as one connection can until `stop`, and
/// returns each committed transaction's xid in commit order.
///
/// Transaction `n` inserts four rows, updates one that this writer's
/// transaction `n - 1` inserted and deletes one its transaction `n - 2`
/// inserted, so the log carries all three operation kinds and every update
/// and delete names a row an earlier logged insert created.
fn write_until_stopped(
    table: &str,
    writer: u8,
    stop: &AtomicBool,
    committed: &AtomicUsize,
) -> Vec<u32> {
    let mut client = Client::connect(&database_url(), NoTls).unwrap();
    let xid = client
        .prepare("SELECT pg_current_xact_id()::text::bigint")
        .unwrap();
    let insert = client
        .prepare(&format!(
            "INSERT INTO {table} (id, title, status)
             VALUES ($1, $2, 'open'), ($3, $4, 'open'), ($5, $6, 'open'), ($7, $8, 'open')"
        ))
        .unwrap();
    let update = client
        .prepare(&format!(
            "UPDATE {table} SET title = $2, status = 'done' WHERE id = $1"
        ))
        .unwrap();
    let delete = client
        .prepare(&format!("DELETE FROM {table} WHERE id = $1"))
        .unwrap();
    let mut xids = Vec::new();
    for n in 0_u64.. {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let ids = [0, 1, 2, 3].map(|row| written_id(writer, n, row));
        let titles = [0, 1, 2, 3].map(|row| format!("written {writer}.{n}.{row}"));
        let mut transaction = client.transaction().unwrap();
        let assigned: i64 = transaction.query_one(&xid, &[]).unwrap().get(0);
        transaction
            .execute(
                &insert,
                &[
                    &ids[0], &titles[0], &ids[1], &titles[1], &ids[2], &titles[2], &ids[3],
                    &titles[3],
                ],
            )
            .unwrap();
        if let Some(previous) = n.checked_sub(1) {
            let updated = transaction
                .execute(
                    &update,
                    &[
                        &written_id(writer, previous, 0),
                        &format!("updated by {writer}.{n}"),
                    ],
                )
                .unwrap();
            assert_eq!(updated, 1);
        }
        if let Some(earlier) = n.checked_sub(2) {
            let deleted = transaction
                .execute(&delete, &[&written_id(writer, earlier, 1)])
                .unwrap();
            assert_eq!(deleted, 1);
        }
        transaction.commit().unwrap();
        // `pg_current_xact_id` carries the epoch; pgoutput and the log carry
        // the 32-bit xid.
        xids.push(u32::try_from(assigned & 0xFFFF_FFFF).unwrap());
        committed.fetch_add(1, Ordering::AcqRel);
    }
    xids
}

/// An LSN in PostgreSQL's `X/Y` text form, as a number.
fn lsn(text: &str) -> u64 {
    let (high, low) = text.split_once('/').unwrap();
    (u64::from_str_radix(high, 16).unwrap() << 32) | u64::from_str_radix(low, 16).unwrap()
}

fn wait_until(what: &str, stderr: &Mutex<VecDeque<String>>, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        if started.elapsed() > CATCH_UP_TIMEOUT {
            let tail = stderr.lock().unwrap().iter().cloned().collect::<Vec<_>>();
            panic!(
                "{what}: not within {CATCH_UP_TIMEOUT:?}. serve's last stderr lines:\n{tail:#?}"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn canonical_head(control: &mut Client, source_id: &str) -> i64 {
    control
        .query_opt(
            "SELECT head_sequence FROM calque_control.canonical_log_head WHERE source_id = $1",
            &[&source_id],
        )
        .unwrap()
        .map_or(0, |row| row.get(0))
}

/// Whether the log holds the transaction with this xid.
fn logged(control: &mut Client, source_id: &str, xid: i64) -> bool {
    control
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM calque_control.canonical_log_record
                  WHERE source_id = $1
                    AND (transaction_identity::jsonb ->> 'postgresXid')::bigint = $2
             )",
            &[&source_id, &xid],
        )
        .unwrap()
        .get(0)
}

/// Every record the log holds for the source, as `(sequence, entry)` in
/// sequence order.
fn canonical_records(control: &mut Client, source_id: &str) -> Vec<(i64, Value)> {
    control
        .query(
            "SELECT sequence, entry_bytes FROM calque_control.canonical_log_record
              WHERE source_id = $1 ORDER BY sequence",
            &[&source_id],
        )
        .unwrap()
        .iter()
        .map(|row| {
            let entry = canonical_log::CanonicalEntry::decode(row.get::<_, &[u8]>(1)).unwrap();
            (row.get(0), serde_json::to_value(entry).unwrap())
        })
        .collect()
}

fn source_rows(client: &mut Client, table: &str) -> Rows {
    client
        .query(&format!("SELECT id, title, status FROM {table}"), &[])
        .unwrap()
        .iter()
        .map(|row| {
            let id: uuid::Uuid = row.get(0);
            (*id.as_bytes(), (row.get(1), row.get(2)))
        })
        .collect()
}

/// The table the log describes: `snapshot` with every TRANSACTION record
/// applied in sequence order. Column ids are ordinals: 1 is `id`, 2 `title`
/// and 3 `status`.
fn replay(mut rows: Rows, records: &[(i64, Value)]) -> Rows {
    // An update or delete names its row by encoded primary key; the insert
    // that created the row pairs that key with the id.
    let mut ids = HashMap::<Vec<u8>, [u8; 16]>::new();
    for (sequence, entry) in records {
        if entry["recordKind"] != "TRANSACTION" {
            continue;
        }
        for operation in entry["record"]["operations"].as_array().unwrap() {
            let key = bytes(&operation["primaryKey"]);
            match operation["kind"].as_str().unwrap() {
                "INSERT_FULL_ROW" => {
                    let cells = cells(&operation["columns"]);
                    let id = uuid16(cells[&1]);
                    let row = (text(cells[&2]), text(cells[&3]));
                    assert!(
                        rows.insert(id, row).is_none() && ids.insert(key, id).is_none(),
                        "record {sequence} inserts a row the log already holds"
                    );
                }
                "UPDATE_PATCH" => {
                    let id = *ids.get(&key).unwrap_or_else(|| {
                        panic!("record {sequence} updates a row no logged insert created")
                    });
                    let row = rows.get_mut(&id).unwrap();
                    // Snake case: the operation kind renames its variants,
                    // not their fields.
                    for (column, cell) in cells(&operation["changed_columns"]) {
                        match column {
                            1 => assert_eq!(uuid16(cell), id, "record {sequence} moves a key"),
                            2 => row.0 = text(cell),
                            3 => row.1 = text(cell),
                            other => panic!("record {sequence} names column {other}"),
                        }
                    }
                }
                "DELETE_BY_KEY" => {
                    let id = ids.remove(&key).unwrap_or_else(|| {
                        panic!("record {sequence} deletes a row no logged insert created")
                    });
                    rows.remove(&id).unwrap();
                }
                other => panic!("record {sequence} is a {other}, which this writer never makes"),
            }
        }
    }
    rows
}

fn cells(columns: &Value) -> HashMap<u64, &Value> {
    columns
        .as_array()
        .unwrap()
        .iter()
        .map(|column| (column["columnId"].as_u64().unwrap(), &column["value"]))
        .collect()
}

fn text(cell: &Value) -> String {
    assert_eq!(cell["kind"], "TEXT", "{cell}");
    cell["value"].as_str().unwrap().to_owned()
}

fn uuid16(cell: &Value) -> [u8; 16] {
    assert_eq!(cell["kind"], "UUID16", "{cell}");
    bytes(&cell["value"]).try_into().unwrap()
}

fn bytes(value: &Value) -> Vec<u8> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| u8::try_from(byte.as_u64().unwrap()).unwrap())
        .collect()
}
