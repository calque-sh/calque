//! Per-tenant working sets on a running `calque serve`, against a live
//! PostgreSQL 18.
//!
//! The test plays the application backend: it mints each user's grant with
//! the tenant attribute and signs the tenant orders. Every tenant repeats the
//! same key values but the tenant column, so a leak is a row with the right id
//! under the wrong tenant. Each tenant's rows carry a marker of their own, and
//! an artifact is checked for the markers of every other tenant in its raw
//! bytes, the way a client would receive them.
//!
//! These tests are `#[ignore]`d and run through
//! `scripts/live-postgres-suite.sh`; without `TEST_DATABASE_URL` they fail
//! rather than silently pass.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Cursor as IoCursor, Read as _, Write as _},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dataset_grant::{
    GrantAttributeValue, GrantCommandCapability, GrantCommandOperation, TenantAction,
};
use dataset_grant_issuer::GrantIssuer;
use postgres::{Client, NoTls};
use prost::Message as _;
use serde_json::Value;
use sync_protocol::{
    change::{
        self, cell_value::Kind, change_stream_frame, operation::Payload, CellValue,
        ChangeStreamFrame, ColumnValue, Operation, OperationKind, PrimaryKey, RowPatch,
        StreamErrorCode,
    },
    encode_primary_key,
    mutation::{
        AffectedRow, MutationBatchRequest, MutationBatchResponse, MutationEnvelope,
        MutationFootprint, MutationOutcomeCode,
    },
    FrameDecoder,
};

const GRANT_KEY_ENV: &str = "CALQUE_LIVE_TENANTS_GRANT_KEY";
const GRANT_AUDIENCE: &str = "tenants.test";
const DATABASE_ENV: &str = "CALQUE_LIVE_TENANTS_DATABASE_URL";
const READY_TIMEOUT: Duration = Duration::from_secs(300);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a tenant may keep answering ARTIFACT_PREPARING.
const PREPARATION_TIMEOUT: Duration = Duration::from_secs(120);
const ARTIFACT_PROFILE: &str = "chromium-native-artifact-v1";
const MUTATION_CONTENT_TYPE: &str = "application/vnd.calque.mutation.v1+protobuf";
const CHANGE_CONTENT_TYPE: &str = "application/vnd.calque.change.v1+protobuf";
/// The most decoded bytes one tenant may hold in these tests: the small
/// tenants fit, the large one does not.
const TENANT_BOUND_BYTES: u64 = 256 * 1024;
const SMALL_TENANT: i64 = 1;
const OTHER_TENANT: i64 = 2;
const EMPTY_TENANT: i64 = 3;
const WARMED_TENANT: i64 = 4;
const LARGE_TENANT: i64 = 9;
/// Holds a third of `tasks` in the key-order test, its rows interleaved with
/// another tenant's in an order its key does not follow.
const CROWDED_TENANT: i64 = 5;

/// One `calque serve` at a time from this binary.
static ONE_SERVER_AT_A_TIME: Mutex<()> = Mutex::new(());

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

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn unix_micros() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros(),
    )
    .unwrap()
}

fn marker(tenant: i64) -> String {
    format!("tenant-{tenant}-only")
}

/// A shared `priorities` table and two tables partitioned by `tenant_id`.
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
            issuer: GrantIssuer::generate("live-tenants-key", GRANT_AUDIENCE)
                .unwrap()
                .0,
        }
    }

    fn table(&self, name: &str) -> String {
        format!("{name}_{}", self.suffix)
    }

    fn relation(&self, name: &str) -> String {
        format!("public.{}", self.table(name))
    }

    fn relations(&self) -> Vec<String> {
        ["priorities", "projects", "tasks"]
            .into_iter()
            .map(|name| self.relation(name))
            .collect()
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

    fn state_dir(&self) -> PathBuf {
        self.directory.path().join("state")
    }

    /// A grant of `generation` for `tenant` that may update tasks.
    fn grant(&self, tenant: Option<GrantAttributeValue>, generation: u64) -> String {
        let attributes = tenant
            .map(|tenant| BTreeMap::from([("tenant_id".to_owned(), tenant)]))
            .unwrap_or_default();
        self.issuer
            .issue_with_commands(
                "tenant-member".to_owned(),
                attributes,
                vec![GrantCommandCapability {
                    collection: self.table("tasks"),
                    operations: vec![GrantCommandOperation::Update],
                }],
                generation,
                unix_seconds(),
                600,
            )
            .unwrap()
    }

    fn tenant_grant(&self, tenant: i64, generation: u64) -> String {
        self.grant(Some(int8(tenant)), generation)
    }

    fn order(&self, action: TenantAction, tenant: i64) -> String {
        self.issuer
            .issue_tenant_order(action, int8(tenant), unix_seconds(), 60)
            .unwrap()
    }

    fn client(&self) -> Client {
        Client::connect(&database_url(), NoTls).unwrap()
    }

    fn execute(&self, sql: &str) {
        self.client().batch_execute(sql).unwrap();
    }

    /// Tenants 1 and 2 hold rows with the same ids; tenant 3 holds none;
    /// tenant 4 holds one; tenant 9 holds more than one tenant may.
    fn seed(&self) {
        let priorities = self.table("priorities");
        let projects = self.table("projects");
        let tasks = self.table("tasks");
        self.execute(&format!(
            "CREATE TABLE {priorities} (id int8 NOT NULL PRIMARY KEY, label text NOT NULL);
             CREATE TABLE {projects} (tenant_id int8 NOT NULL, id int8 NOT NULL,
                 name text NOT NULL, PRIMARY KEY (tenant_id, id));
             CREATE TABLE {tasks} (tenant_id int8 NOT NULL, id int8 NOT NULL,
                 project_id int8 NOT NULL, title text NOT NULL, done bool NOT NULL,
                 PRIMARY KEY (tenant_id, id));
             ALTER TABLE {priorities} REPLICA IDENTITY FULL;
             ALTER TABLE {projects} REPLICA IDENTITY FULL;
             ALTER TABLE {tasks} REPLICA IDENTITY FULL;
             INSERT INTO {priorities} SELECT n, 'priority-' || n FROM generate_series(1, 3) n;
             INSERT INTO {projects} SELECT 1, n, 'tenant-1-only project ' || n FROM generate_series(1, 3) n;
             INSERT INTO {projects} SELECT 2, n, 'tenant-2-only project ' || n FROM generate_series(1, 2) n;
             INSERT INTO {tasks} SELECT 1, n, 1, 'tenant-1-only task ' || n, false FROM generate_series(1, 5) n;
             INSERT INTO {tasks} SELECT 2, n, 1, 'tenant-2-only task ' || n, false FROM generate_series(1, 4) n;
             INSERT INTO {tasks} VALUES (4, 1, 1, 'tenant-4-only task 1', false);
             INSERT INTO {tasks} SELECT 9, n, 1, 'tenant-9-only task ' || n || ' ' || repeat('x', 200), false
                 FROM generate_series(1, 4000) n;
             CREATE PUBLICATION {publication} FOR TABLE {priorities}, {projects}, {tasks};",
            publication = self.publication(),
        ));
    }

    fn title_of(&self, tenant: i64, id: i64) -> Option<String> {
        self.client()
            .query_opt(
                &format!(
                    "SELECT title FROM {} WHERE tenant_id = $1 AND id = $2",
                    self.table("tasks")
                ),
                &[&tenant, &id],
            )
            .unwrap()
            .map(|row| row.get(0))
    }

    /// Retitles task `(tenant, id)` in its own transaction `times` times.
    fn retitle(&self, tenant: i64, id: i64, title: &str, times: usize) {
        let mut client = self.client();
        let tasks = self.table("tasks");
        for _ in 0..times {
            client
                .execute(
                    &format!("UPDATE {tasks} SET title = $1 WHERE tenant_id = $2 AND id = $3"),
                    &[&title, &tenant, &id],
                )
                .unwrap();
        }
    }

    /// The canonical log's head sequence and the lowest sequence it keeps.
    fn log_bounds(&self) -> (i64, i64) {
        let row = self
            .client()
            .query_one(
                "SELECT h.head_sequence, r.minimum_retained_sequence
                   FROM calque_control.canonical_log_head h
                   JOIN calque_control.canonical_log_retention r USING (source_id)
                  WHERE source_id = $1",
                &[&self.source_id()],
            )
            .unwrap();
        (row.get(0), row.get(1))
    }

    fn drop_source(&self) {
        let Ok(mut client) = Client::connect(&database_url(), NoTls) else {
            return;
        };
        // The stopped server's WAL sender can hold the slot for a moment.
        let slot = self.slot();
        let deadline = Instant::now() + Duration::from_secs(10);
        while client
            .query_opt(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                 WHERE slot_name = $1",
                &[&slot],
            )
            .is_err()
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = client.batch_execute(&format!(
            "DELETE FROM calque_control.source_fence_lease WHERE source_id = '{source}';
             DELETE FROM calque_control.source_slot_enrollment WHERE source_id = '{source}';
             DELETE FROM calque_control.authorization_fence WHERE source_id = '{source}';
             DELETE FROM calque_control.revocation_barrier WHERE source_id = '{source}';
             DELETE FROM calque_control.authorization_retirement WHERE source_id = '{source}';
             DELETE FROM calque_control.authorization_scope_issued WHERE source_id = '{source}';
             DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {priorities}, {projects}, {tasks};",
            source = self.source_id(),
            publication = self.publication(),
            priorities = self.table("priorities"),
            projects = self.table("projects"),
            tasks = self.table("tasks"),
        ));
    }

    fn config(&self) -> PathBuf {
        let column = |name: &str, kind: &str| serde_json::json!({ "name": name, "postgresType": kind, "nullable": false });
        let collection = |name: &str, columns: Vec<Value>, key: Vec<&str>, partitioned: bool| {
            let mut document = serde_json::json!({
                "id": self.table(name),
                "source": self.source_id(),
                "kind": "table",
                "relation": { "schema": "public", "table": self.table(name) },
                "columns": columns,
                "primaryKey": key,
                "scope": "tenant",
                "adapter": "direct-columnar",
                "encoding": "calque-columnar",
            });
            if partitioned {
                document["partitionColumn"] = Value::from("tenant_id");
            } else {
                document["shared"] = Value::from(true);
            }
            document
        };
        let document = serde_json::json!({
            "application": { "id": "tenants-live", "version": "1.0.0" },
            "protocol": { "manifest": 1, "artifact": 1, "changeStream": 1, "mutation": 1 },
            "sources": [{
                "id": self.source_id(),
                "connectionEnv": DATABASE_ENV,
                "publication": self.publication(),
                "slot": self.slot(),
            }],
            "scopes": [{
                "id": "tenant",
                "source": self.source_id(),
                "kind": "partitioned",
                "attribute": "tenant_id",
                "dataVersion": 1,
            }],
            "collections": [
                collection(
                    "priorities",
                    vec![column("id", "int8"), column("label", "text")],
                    vec!["id"],
                    false,
                ),
                collection(
                    "projects",
                    vec![column("tenant_id", "int8"), column("id", "int8"), column("name", "text")],
                    vec!["tenant_id", "id"],
                    true,
                ),
                collection(
                    "tasks",
                    vec![
                        column("tenant_id", "int8"),
                        column("id", "int8"),
                        column("project_id", "int8"),
                        column("title", "text"),
                        column("done", "bool"),
                    ],
                    vec!["tenant_id", "id"],
                    true,
                ),
            ],
            "commands": [
                {
                    "id": "edit-priority",
                    "kind": "crud",
                    "collection": self.table("priorities"),
                    "operations": ["update"],
                    "writableColumns": ["label"],
                },
                {
                    "id": "edit-project",
                    "kind": "crud",
                    "collection": self.table("projects"),
                    "operations": ["update"],
                    "writableColumns": ["name"],
                },
                {
                    "id": "edit-task",
                    "kind": "crud",
                    "collection": self.table("tasks"),
                    "operations": ["update"],
                    "writableColumns": ["project_id", "title", "done"],
                },
            ],
        });
        let path = self.directory.path().join("calque.config.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        path
    }

    /// Moves every open and warm order of `tenant` back by `days`, as a
    /// process stopped that long ago would have left them.
    fn age_directory(&self, tenant: i64, days: i64) {
        let directory =
            rusqlite::Connection::open(self.state_dir().join("tenants.sqlite3")).unwrap();
        let shift = days * 24 * 60 * 60 * 1000;
        let changed = directory
            .execute(
                "UPDATE tenants SET last_opened_ms = last_opened_ms - ?1,
                        warmed_ms = warmed_ms - ?1
                  WHERE tenant = ?2",
                rusqlite::params![shift, tenant.to_string()],
            )
            .unwrap();
        assert_eq!(changed, 1, "tenant {tenant} has a directory row");
    }

    /// Every tenant the directory keeps a record of.
    fn recorded_tenants(&self) -> Vec<i64> {
        let directory =
            rusqlite::Connection::open(self.state_dir().join("tenants.sqlite3")).unwrap();
        let mut statement = directory
            .prepare("SELECT tenant FROM tenants ORDER BY tenant")
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|tenant| tenant.unwrap().parse().unwrap())
            .collect()
    }

    /// Every open the directory recorded for `tenant`: `(size, warm)`.
    fn opens(&self, tenant: i64) -> Vec<(Option<i64>, bool)> {
        let directory =
            rusqlite::Connection::open(self.state_dir().join("tenants.sqlite3")).unwrap();
        let mut statement = directory
            .prepare("SELECT size_bytes, warm FROM opens WHERE tenant = ?1 ORDER BY at_ms")
            .unwrap();
        statement
            .query_map([tenant.to_string()], |row| {
                Ok((row.get(0)?, row.get::<_, i64>(1)? != 0))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
}

fn int8(value: i64) -> GrantAttributeValue {
    GrantAttributeValue::Int8(value.to_string())
}

/// A `calque serve` child, killed when the guard drops.
struct Serving {
    child: Child,
    lines: Vec<String>,
    port: u16,
    stopped: bool,
    stderr: Arc<Mutex<Vec<String>>>,
}

impl Serving {
    /// Stops the process with SIGTERM and asserts that it exited cleanly.
    fn shutdown(mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let status = self.child.wait().unwrap();
        self.stopped = true;
        std::thread::sleep(Duration::from_millis(200));
        let stderr = self.stderr.lock().unwrap();
        let from = stderr
            .iter()
            .position(|line| line.contains("panicked at"))
            .unwrap_or(stderr.len().saturating_sub(40));
        let tail = stderr[from..]
            .iter()
            .filter(|line| {
                line.contains("calque") || line.contains("panicked") || line.contains("drop")
            })
            .take(60)
            .collect::<Vec<_>>();
        assert!(
            status.success(),
            "serve must exit cleanly on SIGTERM, exited {status}: {tail:#?}"
        );
    }

    fn line(&self, key: &str) -> Option<&str> {
        self.lines
            .iter()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
    }

    /// The tenant subsystem's log lines so far.
    fn tenant_lines(&self) -> Vec<String> {
        self.stderr
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.contains("tenant") && !line.contains("its cut is retired"))
            .cloned()
            .collect()
    }

    /// How many log lines so far contain every one of `parts`.
    fn logged(&self, parts: &[&str]) -> usize {
        self.stderr
            .lock()
            .unwrap()
            .iter()
            .filter(|line| parts.iter().all(|part| line.contains(part)))
            .count()
    }
}

/// A test that fails still drops its slot, so the next run's sources find
/// one free.
impl Drop for Fixture {
    fn drop(&mut self) {
        self.drop_source();
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn serve_command(fixture: &Fixture, port: u16, extra: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_calque"));
    command
        .args([
            "serve",
            "--config",
            fixture.config().to_str().unwrap(),
            "--state-dir",
            fixture.state_dir().to_str().unwrap(),
            "--grant-audience",
            GRANT_AUDIENCE,
            "--grant-verification-key-env",
            GRANT_KEY_ENV,
            "--allow-origin",
            "http://127.0.0.1:5173",
            "--port",
            &port.to_string(),
            "--max-tenant-bytes",
            &TENANT_BOUND_BYTES.to_string(),
        ])
        .args(extra)
        .env(DATABASE_ENV, database_url())
        .env(
            GRANT_KEY_ENV,
            fixture.issuer.verification_key_environment().unwrap(),
        );
    command
}

fn serve(fixture: &Fixture) -> Serving {
    serve_with(fixture, &[])
}

fn serve_with(fixture: &Fixture, extra: &[&str]) -> Serving {
    let port = reserved_port();
    let started = Instant::now();
    let mut child = serve_command(fixture, port, extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&stderr);
    let handle = BufReader::new(child.stderr.take().unwrap());
    std::thread::spawn(move || {
        for line in handle.lines().map_while(Result::ok) {
            sink.lock().unwrap().push(line);
        }
    });

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
        std::thread::sleep(Duration::from_millis(200));
        panic!(
            "serve exited before readiness.\nstdout: {lines:#?}\nstderr: {:#?}",
            stderr.lock().unwrap()
        );
    }
    Serving {
        child,
        lines,
        port,
        stopped: false,
        stderr,
    }
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

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Option<&[u8]>,
) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let timeout = Duration::from_secs(120);
    stream.set_read_timeout(Some(timeout)).unwrap();
    stream.set_write_timeout(Some(timeout)).unwrap();
    let mut request =
        format!("{method} {path} HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n");
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
    let mut body = raw[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body);
    }
    HttpResponse { status, body }
}

fn dechunk(mut body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let line_end = body
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("a chunk size line");
        let size =
            usize::from_str_radix(std::str::from_utf8(&body[..line_end]).unwrap().trim(), 16)
                .unwrap();
        body = &body[line_end + 2..];
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&body[..size]);
        body = &body[size + 2..];
    }
}

fn tenant_order(port: u16, order: &str) -> HttpResponse {
    http(
        port,
        "POST",
        "/v1/tenants",
        &[("content-type", "application/json".to_owned())],
        Some(
            serde_json::to_vec(&serde_json::json!({ "order": order }))
                .unwrap()
                .as_slice(),
        ),
    )
}

fn tenant_state(fixture: &Fixture, port: u16, tenant: i64) -> Value {
    let response = tenant_order(port, &fixture.order(TenantAction::State, tenant));
    assert_eq!(response.status, 200, "{}", response.text());
    response.json()
}

/// One table of a plan.
#[derive(Debug, Clone)]
struct PlanTable {
    table_id: String,
    rows: u64,
    columns: BTreeMap<String, u32>,
}

/// What one bootstrap plan gives this test.
#[derive(Debug, Clone)]
struct Plan {
    plan_id: String,
    artifact_id: String,
    scope_hash: String,
    scope_token: Vec<u8>,
    activation_cursor: String,
    /// Where its catch-up starts: the cut of the artifact it reads.
    catch_up_from: String,
    /// By `schema.table`.
    tables: BTreeMap<String, PlanTable>,
    download: String,
}

impl Plan {
    fn parse(value: &Value) -> Self {
        let artifact = &value["artifacts"][0];
        let tables = artifact["runtime"]["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|table| {
                let columns = table["columns"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|column| {
                        (
                            column["name"].as_str().unwrap().to_owned(),
                            u32::try_from(column["columnId"].as_u64().unwrap()).unwrap(),
                        )
                    })
                    .collect();
                (
                    format!(
                        "{}.{}",
                        table["sourceSchema"].as_str().unwrap(),
                        table["sourceTable"].as_str().unwrap()
                    ),
                    PlanTable {
                        table_id: table["tableId"].as_str().unwrap().to_owned(),
                        rows: table["rowCount"].as_u64().unwrap(),
                        columns,
                    },
                )
            })
            .collect();
        let download = artifact["selectedRepresentation"]["downloadUrl"]
            .as_str()
            .expect("plan names its bytes");
        let download = download
            .split_once("://")
            .and_then(|(_, rest)| rest.split_once('/'))
            .map_or(download.to_owned(), |(_, path)| format!("/{path}"));
        Self {
            plan_id: value["planId"].as_str().unwrap().to_owned(),
            artifact_id: artifact["artifactId"].as_str().unwrap().to_owned(),
            scope_hash: value["authorization"]["scopeHash"]
                .as_str()
                .unwrap()
                .to_owned(),
            scope_token: value["authorization"]["scopeToken"]
                .as_str()
                .unwrap()
                .as_bytes()
                .to_vec(),
            activation_cursor: value["activationCursor"].as_str().unwrap().to_owned(),
            catch_up_from: value["changeStream"]["fromCursorExclusive"]
                .as_str()
                .unwrap()
                .to_owned(),
            tables,
            download,
        }
    }

    fn table(&self, relation: &str) -> &PlanTable {
        self.tables
            .get(relation)
            .unwrap_or_else(|| panic!("the plan holds {relation}: {:?}", self.tables))
    }

    fn rows(&self, relation: &str) -> u64 {
        self.table(relation).rows
    }

    fn column(&self, relation: &str, name: &str) -> u32 {
        self.table(relation).columns[name]
    }

    fn table_ids(&self) -> Vec<String> {
        self.tables
            .values()
            .map(|table| table.table_id.clone())
            .collect()
    }
}

fn bootstrap_response(fixture: &Fixture, port: u16, grant: &str) -> HttpResponse {
    let document = serde_json::json!({
        "protocolVersion": 1,
        "requiredTables": fixture.relations(),
        "cachedArtifactHints": [],
        "acceptedEncodings": ["zstd", "identity"],
        "memoryBudgetBytes": 1_073_741_824_u64,
        "clientHeapReserveBytes": 67_108_864_u64,
        "clientInstanceId": "tenants-live",
        "deviceId": "tenants-live-device",
    });
    http(
        port,
        "POST",
        "/v1/bootstrap",
        &[
            ("authorization", format!("Bearer {grant}")),
            ("content-type", "application/json".to_owned()),
            ("accept", "application/json".to_owned()),
            ("accept-encoding", "identity".to_owned()),
            ("x-calque-artifact-profile", ARTIFACT_PROFILE.to_owned()),
        ],
        Some(serde_json::to_vec(&document).unwrap().as_slice()),
    )
}

/// Bootstraps `grant`, retrying while its tenant answers ARTIFACT_PREPARING,
/// and says how many times it did.
fn bootstrap(fixture: &Fixture, port: u16, grant: &str) -> (Plan, usize) {
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    let mut preparing = 0;
    loop {
        let response = bootstrap_response(fixture, port, grant);
        match response.status {
            200 => return (Plan::parse(&response.json()), preparing),
            503 if response.text().contains("ARTIFACT_PREPARING") => preparing += 1,
            status => panic!("bootstrap answered {status}: {}", response.text()),
        }
        assert!(
            Instant::now() < deadline,
            "the tenant was not prepared within {PREPARATION_TIMEOUT:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The artifact's bytes exactly as the plan serves them.
fn artifact_bytes(port: u16, plan: &Plan) -> Vec<u8> {
    let response = http(
        port,
        "GET",
        &plan.download,
        &[("accept-encoding", "identity".to_owned())],
        None,
    );
    assert_eq!(response.status, 200, "{}", response.text());
    assert_eq!(&response.body[..8], b"CALQART1");
    response.body
}

fn holds(bytes: &[u8], text: &str) -> bool {
    bytes
        .windows(text.len())
        .any(|window| window == text.as_bytes())
}

/// Asserts the artifact holds `tenant`'s marker and no other tenant's.
fn assert_only_tenant(port: u16, plan: &Plan, tenant: Option<i64>) {
    let bytes = artifact_bytes(port, plan);
    for other in [SMALL_TENANT, OTHER_TENANT, WARMED_TENANT, LARGE_TENANT] {
        assert_eq!(
            holds(&bytes, &marker(other)),
            Some(other) == tenant,
            "tenant {tenant:?}'s artifact and tenant {other}'s marker"
        );
    }
    assert!(
        holds(&bytes, "priority-1"),
        "every tenant holds shared rows"
    );
}

fn change_request(plan: &Plan, from: &str) -> change::ChangeRequest {
    change::ChangeRequest {
        protocol_version: 1,
        from_cursor_exclusive: Some(change::Cursor {
            token: from.as_bytes().to_vec(),
        }),
        through_cursor_inclusive: None,
        tables: plan
            .table_ids()
            .into_iter()
            .map(|table_id| change::TableSubscription {
                table_id,
                shard_snapshot_cursor: None,
            })
            .collect(),
        authorization_scope_token: plan.scope_token.clone(),
        max_batch_bytes: 4 * 1024 * 1024,
        max_transactions: 512,
        long_poll_timeout_ms: 0,
        client_instance_id: "tenants-live".to_owned(),
        device_id: "tenants-live-device".to_owned(),
        pending_mutation_ids: Vec::new(),
        accept_transaction_fragmentation: true,
        accept_revocation_barrier: true,
        plan_id: plan.plan_id.clone(),
        stream: false,
    }
}

/// One change response as this test reads it.
#[derive(Debug, Default)]
struct Batch {
    /// Every text an inserted or updated row carried, in delivery order.
    texts: Vec<String>,
    through: String,
    up_to_date: bool,
    /// The error the response carried instead of a batch.
    error: Option<i32>,
}

fn poll(port: u16, plan: &Plan, from: &str) -> (u16, Batch) {
    send(port, &change_request(plan, from))
}

fn send(port: u16, request: &change::ChangeRequest) -> (u16, Batch) {
    let response = http(
        port,
        "POST",
        "/v1/changes",
        &[("content-type", CHANGE_CONTENT_TYPE.to_owned())],
        Some(request.encode_to_vec().as_slice()),
    );
    let mut decoder =
        FrameDecoder::with_max_payload_bytes(IoCursor::new(&response.body), 4 * 1024 * 1024);
    let mut batch = Batch::default();
    while let Some(frame) = decoder.read_message::<ChangeStreamFrame>().unwrap() {
        match frame.body {
            Some(change_stream_frame::Body::TransactionFragment(fragment)) => {
                for operation in fragment.operations {
                    let columns = match operation.payload {
                        Some(Payload::Insert(full)) => full.columns,
                        Some(Payload::Update(patch)) => patch.changed_columns,
                        _ => Vec::new(),
                    };
                    for column in columns {
                        if let Some(Kind::TextValue(text)) = column.value.and_then(|v| v.kind) {
                            batch.texts.push(text);
                        }
                    }
                }
            }
            Some(change_stream_frame::Body::BatchEnd(end)) => {
                batch.through =
                    String::from_utf8(end.through_cursor_inclusive.unwrap().token).unwrap();
                batch.up_to_date = end.currently_up_to_date;
            }
            Some(change_stream_frame::Body::Error(error)) => batch.error = Some(error.code),
            _ => {}
        }
    }
    (response.status, batch)
}

/// Polls `plan` from `from` until it is at the durable head, and returns every
/// text delivered on the way and the cursor it ended at.
fn drain(port: u16, plan: &Plan, from: &str) -> (Vec<String>, String) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut cursor = from.to_owned();
    let mut texts = Vec::new();
    loop {
        let (status, batch) = poll(port, plan, &cursor);
        assert_eq!(status, 200, "a poll was refused: {batch:?}");
        texts.extend(batch.texts);
        cursor = batch.through;
        if batch.up_to_date {
            return (texts, cursor);
        }
        assert!(Instant::now() < deadline, "the plan never reached the head");
    }
}

/// Polls `plan` from `from` until a batch carries `text`, and returns the
/// cursor it ended at.
fn wait_for(port: u16, plan: &Plan, from: &str, text: &str) -> String {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut cursor = from.to_owned();
    loop {
        let (status, batch) = poll(port, plan, &cursor);
        assert_eq!(status, 200, "a poll was refused: {batch:?}");
        cursor = batch.through;
        if batch.texts.iter().any(|delivered| delivered == text) {
            return cursor;
        }
        assert!(
            Instant::now() < deadline,
            "{text} was not delivered within {DELIVERY_TIMEOUT:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Polls `plan` from `from` until the change endpoint refuses it, and returns
/// the status and the error code.
fn wait_for_refusal(port: u16, plan: &Plan, from: &str) -> (u16, i32) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut cursor = from.to_owned();
    loop {
        let (status, batch) = poll(port, plan, &cursor);
        if let Some(code) = batch.error {
            return (status, code);
        }
        cursor = batch.through;
        assert!(
            Instant::now() < deadline,
            "the plan was not refused within {DELIVERY_TIMEOUT:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Renames task `(key_tenant, id)` through `plan`, as `grant`.
fn rename(
    fixture: &Fixture,
    port: u16,
    plan: &Plan,
    grant: &str,
    key_tenant: i64,
    id: i64,
    title: &str,
) -> HttpResponse {
    let relation = fixture.relation("tasks");
    let tasks = plan.table(&relation).table_id.clone();
    let key = encode_primary_key(&PrimaryKey {
        codec_version: 1,
        parts: vec![
            ColumnValue {
                column_id: plan.column(&relation, "tenant_id"),
                value: Some(CellValue {
                    kind: Some(Kind::Int64Value(key_tenant)),
                }),
            },
            ColumnValue {
                column_id: plan.column(&relation, "id"),
                value: Some(CellValue {
                    kind: Some(Kind::Int64Value(id)),
                }),
            },
        ],
    })
    .unwrap();
    let operation = Operation {
        table_id: tasks.clone(),
        kind: OperationKind::UpdatePatch as i32,
        primary_key: key.clone(),
        payload: Some(Payload::Update(RowPatch {
            changed_columns: vec![ColumnValue {
                column_id: plan.column(&relation, "title"),
                value: Some(CellValue {
                    kind: Some(Kind::TextValue(title.to_owned())),
                }),
            }],
        })),
    };
    let request = MutationBatchRequest {
        protocol_version: 1,
        client_instance_id: "tenants-live".to_owned(),
        device_id: "tenants-live-device".to_owned(),
        authorization_scope_token: plan.scope_token.clone(),
        mutations: vec![MutationEnvelope {
            mutation_id: format!("mutation-{}", uuid::Uuid::new_v4()),
            command_name: format!("crud:{tasks}"),
            command_version: 1,
            canonical_input: operation.encode_to_vec(),
            base_cursor: Some(sync_protocol::mutation::Cursor {
                token: plan.activation_cursor.as_bytes().to_vec(),
            }),
            created_at_unix_micros: unix_micros(),
            expected_versions: Vec::new(),
            declared_footprint: Some(MutationFootprint {
                rows: vec![AffectedRow {
                    table_id: tasks.clone(),
                    primary_key: key,
                }],
                table_ids: vec![tasks],
            }),
        }],
    };
    http(
        port,
        "POST",
        "/v1/mutations",
        &[
            ("authorization", format!("Bearer {grant}")),
            ("content-type", MUTATION_CONTENT_TYPE.to_owned()),
        ],
        Some(request.encode_to_vec().as_slice()),
    )
}

fn accepted(response: &HttpResponse) -> bool {
    response.status == 200
        && MutationBatchResponse::decode(response.body.as_slice())
            .expect("a protobuf response")
            .outcomes
            .first()
            .is_some_and(|outcome| {
                outcome.code == MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32
            })
}

/// Cold builds, isolation, live delivery, writes and moves on one deployment.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn each_tenant_is_built_on_its_first_request_and_holds_and_receives_only_its_rows() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("isolation");
    fixture.seed();
    // Any write past a tenant's cut is due its refresh.
    let serving = serve_with(
        &fixture,
        &["--recut-tail-ratio", "0.001", "--recut-min-interval", "0"],
    );
    let port = serving.port;
    assert_eq!(serving.line("scope_mode"), Some("partitioned"));
    assert_eq!(serving.line("partition_attribute"), Some("tenant_id"));
    assert_eq!(serving.line("artifact"), Some("per-tenant"));
    let priorities = fixture.relation("priorities");
    let projects = fixture.relation("projects");
    let tasks = fixture.relation("tasks");

    // A grant without the tenant attribute, or with one of another type, has
    // no scope.
    let unscoped = bootstrap_response(&fixture, port, &fixture.grant(None, 1));
    assert_eq!(unscoped.status, 403, "{}", unscoped.text());
    let mistyped = bootstrap_response(
        &fixture,
        port,
        &fixture.grant(Some(GrantAttributeValue::Int4(1)), 1),
    );
    assert_eq!(mistyped.status, 403, "{}", mistyped.text());

    // Two first requests for one cold tenant wait on one build.
    let small_grant = fixture.tenant_grant(SMALL_TENANT, 1);
    let first = std::thread::scope(|scope| {
        let requests = (0..2)
            .map(|_| scope.spawn(|| bootstrap(&fixture, port, &small_grant)))
            .collect::<Vec<_>>();
        requests
            .into_iter()
            .map(|request| request.join().unwrap())
            .collect::<Vec<_>>()
    });
    let (small, small_waits) = first[0].clone();
    assert_eq!(first[1].0.artifact_id, small.artifact_id);
    assert_eq!(first[1].0.scope_hash, small.scope_hash);
    println!("the cold tenant answered ARTIFACT_PREPARING {small_waits} times first");
    assert_eq!(
        serving.logged(&["tenant build started", &format!("tenant={SMALL_TENANT} ")]),
        1,
        "one build served both requests"
    );
    assert_eq!(small.rows(&priorities), 3);
    assert_eq!(small.rows(&projects), 3);
    assert_eq!(small.rows(&tasks), 5);
    assert_only_tenant(port, &small, Some(SMALL_TENANT));
    let (again, waits) = bootstrap(&fixture, port, &small_grant);
    assert_eq!(waits, 0, "a prepared tenant answers at once");
    assert_eq!(again.artifact_id, small.artifact_id);

    // The same ids under another tenant are another artifact and scope.
    let other_grant = fixture.tenant_grant(OTHER_TENANT, 1);
    let (other, _) = bootstrap(&fixture, port, &other_grant);
    assert_ne!(other.scope_hash, small.scope_hash);
    assert_ne!(other.artifact_id, small.artifact_id);
    assert_eq!(other.rows(&projects), 2);
    assert_eq!(other.rows(&tasks), 4);
    assert_only_tenant(port, &other, Some(OTHER_TENANT));

    // A tenant with no rows publishes an empty working set of its own.
    let (empty, _) = bootstrap(&fixture, port, &fixture.tenant_grant(EMPTY_TENANT, 1));
    assert_eq!(empty.rows(&priorities), 3);
    assert_eq!(empty.rows(&projects), 0);
    assert_eq!(empty.rows(&tasks), 0);
    assert_only_tenant(port, &empty, None);

    // A tenant past the bound on one artifact is refused whole, at once on
    // the next request.
    let large_grant = fixture.tenant_grant(LARGE_TENANT, 1);
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    let refused = loop {
        let response = bootstrap_response(&fixture, port, &large_grant);
        if response.status != 503 {
            break response;
        }
        assert!(Instant::now() < deadline, "the large tenant never ended");
    };
    assert_eq!(refused.status, 422, "{}", refused.text());
    assert!(refused.text().contains("CAPACITY_EXCEEDED"));
    let started = Instant::now();
    assert_eq!(bootstrap_response(&fixture, port, &large_grant).status, 422);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        tenant_state(&fixture, port, LARGE_TENANT)["refused"],
        Value::Bool(true)
    );

    // Live: a row of one tenant reaches that tenant alone; a shared row
    // reaches both.
    let (_, small_head) = drain(port, &small, &small.activation_cursor);
    let (_, other_head) = drain(port, &other, &other.activation_cursor);
    fixture.execute(&format!(
        "INSERT INTO {} VALUES (2, 100, 1, 'tenant-2-only live', false);
         INSERT INTO {} VALUES (100, 'priority-live');",
        fixture.table("tasks"),
        fixture.table("priorities"),
    ));
    let other_head = wait_for(port, &other, &other_head, "priority-live");
    let (seen, small_head) = drain(port, &small, &small_head);
    assert!(seen.iter().any(|text| text == "priority-live"), "{seen:?}");
    assert!(
        !seen.iter().any(|text| text.contains(&marker(OTHER_TENANT))),
        "tenant 1 received tenant 2's row: {seen:?}"
    );

    // A write is admitted only for a row of the grant's own tenant, even
    // through a plan of that tenant.
    let foreign = rename(
        &fixture,
        port,
        &small,
        &small_grant,
        OTHER_TENANT,
        1,
        "foreign",
    );
    assert!(
        !accepted(&foreign),
        "a write to another tenant's row was accepted"
    );
    let own = rename(
        &fixture,
        port,
        &small,
        &small_grant,
        SMALL_TENANT,
        1,
        "tenant-1-only renamed",
    );
    assert!(accepted(&own), "{}", own.text());
    // Tenant 2's grant cannot write through tenant 1's plan.
    let crossed = rename(
        &fixture,
        port,
        &small,
        &other_grant,
        SMALL_TENANT,
        2,
        "crossed",
    );
    assert!(
        !accepted(&crossed),
        "a write through another tenant's plan was accepted"
    );
    // Nor its own row: a plan takes writes from its own tenant's grants alone.
    let borrowed = rename(
        &fixture,
        port,
        &small,
        &other_grant,
        OTHER_TENANT,
        1,
        "borrowed",
    );
    assert_eq!(borrowed.status, 403, "{}", borrowed.text());
    let small_head = wait_for(port, &small, &small_head, "tenant-1-only renamed");
    assert_eq!(
        fixture.title_of(OTHER_TENANT, 1).as_deref(),
        Some("tenant-2-only task 1")
    );
    assert_eq!(
        fixture.title_of(SMALL_TENANT, 2).as_deref(),
        Some("tenant-1-only task 2")
    );

    // Once the bytes routed to it outgrow the ratio of its artifact, a warm
    // tenant is cut again.
    let refreshed_from = other.artifact_id.clone();
    fixture.execute(&format!(
        "UPDATE {} SET done = NOT done WHERE tenant_id = 2;
         UPDATE {} SET done = NOT done WHERE tenant_id = 2;",
        fixture.table("tasks"),
        fixture.table("tasks"),
    ));
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    let refreshed = loop {
        let (plan, _) = bootstrap(&fixture, port, &other_grant);
        if plan.artifact_id != refreshed_from {
            break plan;
        }
        assert!(Instant::now() < deadline, "the tenant was never cut again");
        std::thread::sleep(Duration::from_millis(500));
    };
    assert_eq!(refreshed.rows(&tasks), 5);
    assert_only_tenant(port, &refreshed, Some(OTHER_TENANT));
    let _ = other_head;

    // A row that moves between tenants is a rebuild boundary for both.
    let (_, other_head) = drain(port, &refreshed, &refreshed.activation_cursor);
    fixture.execute(&format!(
        "UPDATE {} SET tenant_id = 2, title = 'tenant-2-only moved' WHERE tenant_id = 1 AND id = 5",
        fixture.table("tasks"),
    ));
    for (plan, from) in [(&small, &small_head), (&refreshed, &other_head)] {
        let (status, code) = wait_for_refusal(port, plan, from);
        assert_eq!(status, 409);
        assert_eq!(code, StreamErrorCode::PartitionMoved as i32);
    }
    let (small_after, _) = bootstrap(&fixture, port, &small_grant);
    assert_ne!(small_after.artifact_id, small.artifact_id);
    assert_eq!(small_after.rows(&tasks), 4);
    assert_only_tenant(port, &small_after, Some(SMALL_TENANT));
    let (other_after, _) = bootstrap(&fixture, port, &other_grant);
    assert_ne!(other_after.artifact_id, refreshed.artifact_id);
    assert_eq!(other_after.rows(&tasks), 6);
    assert_only_tenant(port, &other_after, Some(OTHER_TENANT));

    // Every open is recorded, cold or warm, with the size served.
    let opens = fixture.opens(SMALL_TENANT);
    assert!(opens.len() >= 3, "{opens:?}");
    assert!(opens
        .iter()
        .any(|(size, warm)| *warm && size.is_some_and(|size| size > 0)));
    assert!(opens.iter().any(|(_, warm)| !warm));

    serving.shutdown();
}

/// The application endpoint, recency, pins, eviction, a restart and a fence.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn the_backend_warms_and_pins_tenants_that_survive_a_restart_and_the_rest_are_evicted() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("warmth");
    fixture.seed();
    let serving = serve(&fixture);
    let port = serving.port;

    // Only an order signed with the application's key, under the order's own
    // domain and still fresh, is acted on.
    let as_grant = tenant_order(port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(as_grant.status, 401, "{}", as_grant.text());
    let as_fence = tenant_order(
        port,
        &fixture
            .issuer
            .issue_fence_order(1, 2, unix_seconds(), 60)
            .unwrap(),
    );
    assert_eq!(as_fence.status, 401, "{}", as_fence.text());
    let impostor = GrantIssuer::generate(fixture.issuer.key_id(), GRANT_AUDIENCE)
        .unwrap()
        .0;
    let forged = tenant_order(
        port,
        &impostor
            .issue_tenant_order(TenantAction::Warm, int8(WARMED_TENANT), unix_seconds(), 60)
            .unwrap(),
    );
    assert_eq!(forged.status, 401, "{}", forged.text());
    let expired = tenant_order(
        port,
        &fixture
            .issuer
            .issue_tenant_order(
                TenantAction::Warm,
                int8(WARMED_TENANT),
                unix_seconds() - 600,
                60,
            )
            .unwrap(),
    );
    assert_eq!(expired.status, 401, "{}", expired.text());
    let mistyped = tenant_order(
        port,
        &fixture
            .issuer
            .issue_tenant_order(
                TenantAction::Warm,
                GrantAttributeValue::Int4(4),
                unix_seconds(),
                60,
            )
            .unwrap(),
    );
    assert_eq!(mistyped.status, 400, "{}", mistyped.text());
    assert_eq!(tenant_state(&fixture, port, WARMED_TENANT)["state"], "cold");
    assert_eq!(
        serving.logged(&["tenant build started", &format!("tenant={WARMED_TENANT} ")]),
        0,
        "no refused order started a build"
    );

    // A warm order prepares the tenant before any user asks for it, and is
    // acted on once.
    let warm_order = fixture.order(TenantAction::Warm, WARMED_TENANT);
    let warmed = tenant_order(port, &warm_order);
    assert_eq!(warmed.status, 200, "{}", warmed.text());
    let replayed = tenant_order(port, &warm_order);
    assert_eq!(replayed.status, 409, "{}", replayed.text());
    assert!(replayed.text().contains("REPLAYED"), "{}", replayed.text());
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    let report = loop {
        let report = tenant_state(&fixture, port, WARMED_TENANT);
        if report["state"] == "warm" {
            break report;
        }
        assert!(
            Instant::now() < deadline,
            "the warm order never finished: {report}"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(report["sizeBytes"].as_u64().unwrap() > 0, "{report}");
    assert!(report["cutAgeSeconds"].as_u64().is_some(), "{report}");
    assert_eq!(report["lastOpenedUnixSeconds"], Value::Null);
    let pin_order = fixture
        .issuer
        .issue_tenant_order(TenantAction::Pin, int8(WARMED_TENANT), unix_seconds(), 300)
        .unwrap();
    let pinned = tenant_order(port, &pin_order);
    assert_eq!(pinned.status, 200);
    assert_eq!(pinned.json()["pinned"], Value::Bool(true));
    let (warmed_plan, waits) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(waits, 0, "a warmed tenant answers at once");
    assert_only_tenant(port, &warmed_plan, Some(WARMED_TENANT));

    // Two tenants opened by users, one of which will not be opened again.
    let (small, _) = bootstrap(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 1));
    let (other, _) = bootstrap(&fixture, port, &fixture.tenant_grant(OTHER_TENANT, 1));
    serving.shutdown();

    // Thirty days later the pinned tenant and the one opened recently are
    // warm and served from the cut they had; the other is evicted, and once
    // nothing waits on it, forgotten.
    fixture.age_directory(WARMED_TENANT, 30);
    fixture.age_directory(OTHER_TENANT, 30);
    let serving = serve(&fixture);
    let port = serving.port;
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while fixture.recorded_tenants() != [SMALL_TENANT, WARMED_TENANT] {
        assert!(
            Instant::now() < deadline,
            "the directory still records {:?}",
            fixture.recorded_tenants()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    // An order acted on before the restart is not acted on again.
    let replayed = tenant_order(port, &pin_order);
    assert_eq!(replayed.status, 409, "{}", replayed.text());
    // A tenant named only by a state order is not recorded.
    assert_eq!(tenant_state(&fixture, port, 77)["state"], "cold");
    assert_eq!(fixture.recorded_tenants(), [SMALL_TENANT, WARMED_TENANT]);
    for (tenant, plan) in [(SMALL_TENANT, &small), (WARMED_TENANT, &warmed_plan)] {
        let report = tenant_state(&fixture, port, tenant);
        assert_eq!(report["state"], "warm", "tenant {tenant}: {report}");
        let (restored, waits) = bootstrap(&fixture, port, &fixture.tenant_grant(tenant, 1));
        assert_eq!(waits, 0);
        assert_eq!(restored.artifact_id, plan.artifact_id, "tenant {tenant}");
    }
    assert_eq!(
        tenant_state(&fixture, port, WARMED_TENANT)["pinned"],
        Value::Bool(true)
    );
    let evicted = tenant_state(&fixture, port, OTHER_TENANT);
    assert_eq!(evicted["state"], "cold", "{evicted}");
    let (reopened, _) = bootstrap(&fixture, port, &fixture.tenant_grant(OTHER_TENANT, 1));
    assert_ne!(reopened.artifact_id, other.artifact_id);
    assert_only_tenant(port, &reopened, Some(OTHER_TENANT));

    // A fence retires every tenant's artifact; the warm ones are cut again
    // under the new generation and the old generation's grant is refused.
    let fenced = http(
        port,
        "POST",
        "/v1/authorization/fence",
        &[("content-type", "application/json".to_owned())],
        Some(
            serde_json::to_vec(&serde_json::json!({
                "order": fixture.issuer.issue_fence_order(1, 2, unix_seconds(), 60).unwrap(),
            }))
            .unwrap()
            .as_slice(),
        ),
    );
    assert_eq!(fenced.status, 200, "{}", fenced.text());
    assert_eq!(
        bootstrap_response(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 1)).status,
        401
    );
    let (fenced_plan, _) = bootstrap(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 2));
    assert_ne!(fenced_plan.artifact_id, small.artifact_id);
    assert_ne!(fenced_plan.scope_hash, small.scope_hash);
    assert_only_tenant(port, &fenced_plan, Some(SMALL_TENANT));
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    loop {
        if tenant_state(&fixture, port, WARMED_TENANT)["state"] == "warm" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the pinned tenant was not cut again"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // An evict order drops the pinned tenant's artifact at once, and its
    // next open is cold.
    let (pinned_plan, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 2));
    let cold_opens = || {
        serving.logged(&[
            "tenant opened",
            &format!("tenant={WARMED_TENANT} "),
            "warm=false",
        ])
    };
    let cold_before = cold_opens();
    let evicted = tenant_order(port, &fixture.order(TenantAction::Evict, WARMED_TENANT));
    assert_eq!(evicted.status, 200, "{}", evicted.text());
    assert_eq!(evicted.json()["state"], "cold", "{}", evicted.text());
    assert_eq!(evicted.json()["pinned"], Value::Bool(false));
    let (rebuilt, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 2));
    assert_ne!(rebuilt.artifact_id, pinned_plan.artifact_id);
    assert_eq!(cold_opens(), cold_before + 1);
    assert_only_tenant(port, &rebuilt, Some(WARMED_TENANT));

    serving.shutdown();
}

/// Waits until the canonical log's head passes `sequence`.
fn head_past(fixture: &Fixture, sequence: i64) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    loop {
        let (head, _) = fixture.log_bounds();
        if head > sequence {
            return;
        }
        assert!(Instant::now() < deadline, "the log's head stayed at {head}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Commits `sql` and waits until the canonical log holds it.
fn commit(fixture: &Fixture, sql: &str) {
    let (head, _) = fixture.log_bounds();
    fixture.execute(sql);
    head_past(fixture, head);
}

/// Waits until the tenant directory records the follower at the log's head.
fn followed_to_head(fixture: &Fixture) {
    let (head, _) = fixture.log_bounds();
    let directory =
        rusqlite::Connection::open(fixture.state_dir().join("tenants.sqlite3")).unwrap();
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    loop {
        let followed: i64 = directory
            .query_row("SELECT sequence FROM follower WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        if followed >= head {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the follower stayed at {followed} of {head}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Sets a directory value to what a directory an earlier build wrote holds.
fn forget(fixture: &Fixture, sql: &str) {
    let directory =
        rusqlite::Connection::open(fixture.state_dir().join("tenants.sqlite3")).unwrap();
    assert_eq!(directory.execute(sql, []).unwrap(), 1, "{sql}");
}

/// A plan activates at its tenant's cut, with nothing to catch up, only
/// while nothing the tenant reads has committed since the cut, across a
/// restart too.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_tenant_nothing_changed_for_since_its_cut_is_planned_with_nothing_to_catch_up() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("unchanged");
    fixture.seed();
    let tasks = fixture.table("tasks");
    let retitle = |tenant: i64, id: i64, title: &str| {
        commit(
            &fixture,
            &format!(
                "UPDATE {tasks} SET title = '{title}' WHERE tenant_id = {tenant} AND id = {id}"
            ),
        );
    };
    let at_cut = |plan: &Plan| plan.activation_cursor == plan.catch_up_from;
    let serving = serve(&fixture);
    let port = serving.port;

    // Tenant 4 is built after tenant 1, and tenant 2's rows change: the log
    // moves past tenant 1's cut with nothing tenant 1 reads.
    let (first, _) = bootstrap(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 1));
    let (fourth, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    retitle(OTHER_TENANT, 1, "tenant-2-only retitled");
    let (unchanged, _) = bootstrap(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 1));
    assert_eq!(unchanged.artifact_id, first.artifact_id);
    assert!(
        at_cut(&unchanged),
        "tenant 1 is planned from its cut to the head"
    );
    let (texts, live) = drain(port, &unchanged, &unchanged.catch_up_from);
    assert!(texts.is_empty(), "{texts:?}");

    // Its own change is caught up to, and reaches the plan that went live
    // at the cut.
    retitle(SMALL_TENANT, 2, "tenant-1-only retitled");
    let (changed, _) = bootstrap(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 1));
    assert_eq!(changed.artifact_id, first.artifact_id);
    assert!(!at_cut(&changed));
    assert!(replay(port, &changed).contains(&"tenant-1-only retitled".to_owned()));
    wait_for(port, &unchanged, &live, "tenant-1-only retitled");

    // So is a shared table's change, by every tenant.
    let (fourth_unchanged, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert!(at_cut(&fourth_unchanged));
    commit(
        &fixture,
        &format!(
            "UPDATE {} SET label = 'shared relabelled' WHERE id = 1",
            fixture.table("priorities")
        ),
    );
    let (shared, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(shared.artifact_id, fourth.artifact_id);
    assert!(!at_cut(&shared));
    assert!(replay(port, &shared).contains(&"shared relabelled".to_owned()));
    serving.shutdown();

    // A restart keeps what the follower knew.
    let serving = serve(&fixture);
    let port = serving.port;
    let (first_restored, _) = bootstrap(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 1));
    assert_eq!(first_restored.artifact_id, first.artifact_id);
    assert!(
        !at_cut(&first_restored),
        "tenant 1 changed before the restart"
    );
    let (fourth_restored, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(fourth_restored.artifact_id, fourth.artifact_id);
    assert!(
        !at_cut(&fourth_restored),
        "the shared table changed before the restart"
    );

    // Cut again after every change, and unchanged since but for another
    // tenant's rows, it is planned from its cut after a restart.
    let rebuild = |port: u16| {
        let evicted = tenant_order(port, &fixture.order(TenantAction::Evict, WARMED_TENANT));
        assert_eq!(evicted.status, 200, "{}", evicted.text());
        let (rebuilt, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
        retitle(
            OTHER_TENANT,
            1,
            &format!("tenant-2-only after {}", rebuilt.artifact_id),
        );
        rebuilt
    };
    let rebuilt = rebuild(port);
    assert_ne!(rebuilt.artifact_id, fourth.artifact_id);
    serving.shutdown();
    let serving = serve(&fixture);
    let port = serving.port;
    let (restored, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(restored.artifact_id, rebuilt.artifact_id);
    assert!(at_cut(&restored), "tenant 4 is unchanged since its cut");

    // A change the follower read before a restart is not read again after
    // it, and is still caught up to.
    retitle(WARMED_TENANT, 1, "tenant-4-only retitled");
    followed_to_head(&fixture);
    serving.shutdown();
    let serving = serve(&fixture);
    let port = serving.port;
    let (changed_restored, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(changed_restored.artifact_id, rebuilt.artifact_id);
    assert!(
        !at_cut(&changed_restored),
        "tenant 4 changed before the restart"
    );
    assert!(replay(port, &changed_restored).contains(&"tenant-4-only retitled".to_owned()));
    let rebuilt = rebuild(port);
    serving.shutdown();

    // A directory that does not say when the shared tables or a tenant last
    // changed reads as their having changed up to the follower's position.
    forget(&fixture, "UPDATE follower SET shared_touched_at = NULL");
    let serving = serve(&fixture);
    let port = serving.port;
    let (shared_unknown, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(shared_unknown.artifact_id, rebuilt.artifact_id);
    assert!(!at_cut(&shared_unknown));
    let rebuilt = rebuild(port);
    serving.shutdown();
    forget(
        &fixture,
        &format!("UPDATE tenants SET touched_at = NULL WHERE tenant = '{WARMED_TENANT}'"),
    );
    let serving = serve(&fixture);
    let port = serving.port;
    let (tenant_unknown, _) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(tenant_unknown.artifact_id, rebuilt.artifact_id);
    assert!(!at_cut(&tenant_unknown));

    serving.shutdown();
}

/// Catches `plan` up from its artifact's cut to its activation, as a page
/// does, refusing any answer but a batch, and returns every text delivered.
fn replay(port: u16, plan: &Plan) -> Vec<String> {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let cursor_of = |token: &str| change::Cursor {
        token: token.as_bytes().to_vec(),
    };
    let mut cursor = plan.catch_up_from.clone();
    let mut texts = Vec::new();
    while cursor != plan.activation_cursor {
        let mut request = change_request(plan, &cursor);
        request.through_cursor_inclusive = Some(cursor_of(&plan.activation_cursor));
        for table in &mut request.tables {
            table.shard_snapshot_cursor = Some(cursor_of(&plan.catch_up_from));
        }
        let (status, batch) = send(port, &request);
        assert!(
            status == 200 && batch.error.is_none(),
            "the catch-up was refused with {status}: {batch:?}"
        );
        texts.extend(batch.texts);
        cursor = batch.through;
        assert!(
            Instant::now() < deadline,
            "the plan never reached its activation"
        );
    }
    texts
}

/// The log is trimmed behind every tenant: a tenant's cut keeps what a plan
/// from it replays, and so does a plan still held once its tenant is evicted.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn the_log_is_trimmed_behind_each_tenant_s_cut_and_each_plan_held_from_one() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("trim");
    fixture.seed();
    let serving = serve(&fixture);
    let port = serving.port;
    let settled = || std::thread::sleep(Duration::from_secs(4));

    // With no tenant prepared, nothing but the follower holds the log.
    fixture.retitle(OTHER_TENANT, 1, "churn", 40);
    settled();
    let (head, floor) = fixture.log_bounds();
    assert!(head >= 40 && floor > head - 5, "head {head}, floor {floor}");

    let warmed = tenant_order(port, &fixture.order(TenantAction::Warm, WARMED_TENANT));
    assert_eq!(warmed.status, 200, "{}", warmed.text());
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    while tenant_state(&fixture, port, WARMED_TENANT)["state"] != "warm" {
        assert!(Instant::now() < deadline, "the warm order never finished");
        std::thread::sleep(Duration::from_millis(200));
    }
    let (cut_bound, _) = fixture.log_bounds();
    fixture.retitle(WARMED_TENANT, 1, "tenant-4-only after its cut", 1);
    fixture.retitle(OTHER_TENANT, 1, "churn", 40);
    settled();
    let (head, floor) = fixture.log_bounds();
    assert!(
        head > cut_bound + 40,
        "head {head}, cut at or before {cut_bound}"
    );
    assert!(
        floor <= cut_bound + 1,
        "the log was trimmed to {floor}, past the warm tenant's cut at or before {cut_bound}"
    );

    let (plan, waits) = bootstrap(&fixture, port, &fixture.tenant_grant(WARMED_TENANT, 1));
    assert_eq!(waits, 0, "the warm tenant answers from its cut");
    assert_eq!(
        serving.logged(&["tenant build started", &format!("tenant={WARMED_TENANT} ")]),
        1
    );
    let after_cut = "tenant-4-only after its cut".to_owned();
    assert!(replay(port, &plan).contains(&after_cut));

    // Evicted, the tenant no longer holds the log; the plan from its cut does.
    let evicted = tenant_order(port, &fixture.order(TenantAction::Evict, WARMED_TENANT));
    assert_eq!(evicted.json()["state"], "cold", "{}", evicted.text());
    fixture.retitle(OTHER_TENANT, 1, "churn", 40);
    settled();
    let (_, floor) = fixture.log_bounds();
    assert!(
        floor <= cut_bound + 1,
        "the log was trimmed to {floor}, past a held plan's start at or before {cut_bound}"
    );
    assert!(replay(port, &plan).contains(&after_cut));

    serving.shutdown();
}

/// Each tenant build holds a replication slot and a WAL sender, so a cap the
/// server cannot hold is refused before anything is served.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn serve_refuses_a_build_cap_postgres_has_no_replication_capacity_for() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("capacity");
    fixture.seed();
    let mut child = serve_command(&fixture, reserved_port(), &["--max-tenant-builds", "64"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + READY_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("serve did not refuse the build cap");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let mut stdout = String::new();
    let mut stderr = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(!status.success(), "stdout: {stdout}\nstderr: {stderr}");
    assert!(
        stderr.contains("replication slots") && stderr.contains("--max-tenant-builds"),
        "stderr: {stderr}"
    );
    assert!(!stdout.contains("Calque serve is ready"), "{stdout}");
}

/// Holds a transaction with an id open, which a replication slot's creation
/// waits on, so a tenant build stays in progress until it is released.
fn hold_builds(fixture: &Fixture) -> Client {
    let mut client = fixture.client();
    client.batch_execute("BEGIN").unwrap();
    client.query_one("SELECT txid_current()", &[]).unwrap();
    client
}

/// An evict that arrives while the tenant is built drops the build, and a
/// request waiting on it is asked to come again.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn an_evict_during_a_build_drops_the_build_and_its_result() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("evictbuild");
    fixture.seed();
    let serving = serve(&fixture);
    let port = serving.port;
    let started = || serving.logged(&["tenant build started", &format!("tenant={WARMED_TENANT} ")]);

    let mut held = hold_builds(&fixture);
    let warmed = tenant_order(port, &fixture.order(TenantAction::Warm, WARMED_TENANT));
    assert_eq!(warmed.status, 200, "{}", warmed.text());
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while started() == 0 {
        assert!(Instant::now() < deadline, "the warm order started no build");
        std::thread::sleep(Duration::from_millis(20));
    }
    let grant = fixture.tenant_grant(WARMED_TENANT, 1);
    let answered = std::thread::scope(|scope| {
        let waiting = scope.spawn(|| bootstrap_response(&fixture, port, &grant));
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(
            tenant_state(&fixture, port, WARMED_TENANT)["state"],
            "preparing"
        );
        let evicted = tenant_order(port, &fixture.order(TenantAction::Evict, WARMED_TENANT));
        assert_eq!(evicted.status, 200, "{}", evicted.text());
        assert_eq!(evicted.json()["state"], "cold", "{}", evicted.text());
        waiting.join().unwrap()
    });
    assert_eq!(answered.status, 503, "{}", answered.text());
    assert!(
        answered.text().contains("ARTIFACT_PREPARING"),
        "{}",
        answered.text()
    );
    held.batch_execute("COMMIT").unwrap();

    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    let tenant = format!("tenant={WARMED_TENANT}");
    while serving.logged(&["tenant build cancelled", &tenant])
        + serving.logged(&["a build the tenant no longer runs was discarded", &tenant])
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "the dropped build never ended: {:#?}",
            serving.tenant_lines()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_secs(1));
    let report = tenant_state(&fixture, port, WARMED_TENANT);
    assert_eq!(report["state"], "cold", "{report}");
    assert_eq!(report["sizeBytes"], Value::Null, "{report}");

    // Its next open builds it afresh.
    let (plan, _) = bootstrap(&fixture, port, &grant);
    assert_only_tenant(port, &plan, Some(WARMED_TENANT));
    assert_eq!(started(), 2);

    serving.shutdown();
}

/// The plan cap bounds a cache of verified plans, not the plans served: a
/// plan the cache dropped is rebuilt from its token, for its own tenant.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_plan_the_cache_dropped_is_rebuilt_from_its_token_for_its_tenant() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("plancap");
    fixture.seed();
    let serving = serve_with(&fixture, &["--max-active-plans", "1"]);
    let port = serving.port;

    let (small, _) = bootstrap(&fixture, port, &fixture.tenant_grant(SMALL_TENANT, 1));
    let (_, small_head) = drain(port, &small, &small.activation_cursor);
    let (other, _) = bootstrap(&fixture, port, &fixture.tenant_grant(OTHER_TENANT, 1));
    assert_only_tenant(port, &other, Some(OTHER_TENANT));
    let (_, other_head) = drain(port, &other, &other.activation_cursor);

    fixture.execute(&format!(
        "INSERT INTO {tasks} VALUES (1, 100, 1, 'tenant-1-only rebuilt', false);
         INSERT INTO {tasks} VALUES (2, 100, 1, 'tenant-2-only rebuilt', false);",
        tasks = fixture.table("tasks"),
    ));
    wait_for(port, &small, &small_head, "tenant-1-only rebuilt");
    wait_for(port, &other, &other_head, "tenant-2-only rebuilt");
    let (seen, _) = drain(port, &small, &small_head);
    assert!(
        seen.contains(&"tenant-1-only rebuilt".to_owned()),
        "{seen:?}"
    );
    assert!(
        !seen.iter().any(|text| text.contains(&marker(OTHER_TENANT))),
        "tenant 1's rebuilt plan received tenant 2's row: {seen:?}"
    );

    serving.shutdown();
}

/// The sequential scans PostgreSQL counts for `table`, and the rows plain
/// index scans fetched through its indexes.
fn scans(fixture: &Fixture, table: &str) -> (i64, i64) {
    let row = fixture
        .client()
        .query_one(
            "SELECT t.seq_scan, coalesce(sum(i.idx_tup_fetch), 0)::int8
               FROM pg_stat_user_tables t
               JOIN pg_stat_user_indexes i USING (relid)
              WHERE t.relname = $1
              GROUP BY t.seq_scan",
            &[&table],
        )
        .unwrap();
    (row.get(0), row.get(1))
}

/// A tenant whose rows are spread through a table in an order its key does
/// not follow is one PostgreSQL would read with a scan and a sort; its build
/// walks the primary-key index instead.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_tenant_s_build_walks_its_key_range_where_postgres_would_scan_and_sort() {
    const ROWS: i64 = 3000;
    let _exclusive = exclusive();
    let fixture = Fixture::new("keyindex");
    fixture.seed();
    let tasks = fixture.table("tasks");
    fixture.execute(&format!(
        "INSERT INTO {tasks}
             SELECT tenant, n, 1, 'tenant-' || tenant || '-only task ' || n, false
               FROM generate_series(1, {ROWS}) n,
                    (VALUES ({CROWDED_TENANT}), ({CROWDED_TENANT} + 1)) tenants (tenant)
              ORDER BY random();
         ANALYZE {tasks};"
    ));
    let planned = fixture
        .client()
        .query(
            &format!(
                "EXPLAIN SELECT * FROM {tasks} WHERE tenant_id = {CROWDED_TENANT}
                  ORDER BY tenant_id, id"
            ),
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        planned.contains("Sort"),
        "PostgreSQL would walk the index anyway:\n{planned}"
    );
    let serving = serve(&fixture);
    let port = serving.port;

    let (scanned, fetched) = scans(&fixture, &tasks);
    let (plan, _) = bootstrap(&fixture, port, &fixture.tenant_grant(CROWDED_TENANT, 1));
    assert_only_tenant(port, &plan, Some(CROWDED_TENANT));
    assert!(holds(&artifact_bytes(port, &plan), &marker(CROWDED_TENANT)));
    // A backend reports its counts once it has been idle for a while.
    let deadline = Instant::now() + Duration::from_secs(30);
    let counts = loop {
        let counts = scans(&fixture, &tasks);
        if counts.1 >= fetched + ROWS || Instant::now() > deadline {
            break counts;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(
        counts.1 >= fetched + ROWS,
        "the build fetched {} rows through the key index",
        counts.1 - fetched
    );
    assert_eq!(counts.0, scanned, "the build scanned the table");

    serving.shutdown();
}

/// Warm orders for many tenants, queued long enough to age, hold back a
/// request a user waits on by one start at most.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn aged_warm_orders_hold_back_a_waiting_request_by_one_start_at_most() {
    const HEAVY_TENANT: i64 = 99;
    const FLEET: std::ops::Range<i64> = 100..130;
    let _exclusive = exclusive();
    let fixture = Fixture::new("aging");
    fixture.seed();
    // The heavy tenant's build holds the only slot while the fleet's orders
    // and the request queue behind it.
    fixture.execute(&format!(
        "INSERT INTO {} SELECT {HEAVY_TENANT}, n, 1, md5(n::text) || repeat('x', 200), false
             FROM generate_series(1, 200000) n",
        fixture.table("tasks")
    ));
    let serving = serve_with(
        &fixture,
        &["--max-tenant-builds", "1", "--recut-min-interval", "0"],
    );
    let port = serving.port;
    for tenant in std::iter::once(HEAVY_TENANT).chain(FLEET) {
        let warmed = tenant_order(port, &fixture.order(TenantAction::Warm, tenant));
        assert_eq!(warmed.status, 200, "{}", warmed.text());
    }
    let (plan, _) = bootstrap(&fixture, port, &fixture.tenant_grant(1, 1));
    assert_only_tenant(port, &plan, Some(1));
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    while FLEET
        .clone()
        .any(|tenant| serving.logged(&["tenant build started", &format!("tenant={tenant} ")]) == 0)
    {
        assert!(
            Instant::now() < deadline,
            "the fleet's builds never all started: {:#?}",
            serving.tenant_lines()
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let lines = serving.tenant_lines();
    let position = |what: &str| {
        lines
            .iter()
            .position(|line| line.contains(what) && line.contains("tenant=1 "))
            .unwrap_or_else(|| panic!("no {what} line for tenant 1: {lines:#?}"))
    };
    let (opened, started) = (position("tenant opened"), position("tenant build started"));
    let starts = |lines: &[String]| {
        lines
            .iter()
            .filter(|line| line.contains("tenant build started"))
            .count()
    };
    assert!(
        starts(&lines[opened..started]) + starts(&lines[started + 1..]) >= 5,
        "the warm orders were no longer queued when the request arrived: {lines:#?}"
    );
    assert!(
        starts(&lines[opened..started]) <= 1,
        "{} builds started ahead of the waiting request",
        starts(&lines[opened..started])
    );

    serving.shutdown();
}

/// A row that moves into or out of a tenant while it is built ends that
/// build, and the tenant is built again, five times at most; past that it is
/// built only when it is asked for again.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_tenant_whose_rows_move_during_its_builds_is_built_again_up_to_a_bound() {
    const MOVING_TENANT: i64 = 7;
    let _exclusive = exclusive();
    let fixture = Fixture::new("movebuild");
    fixture.seed();
    let tasks = fixture.table("tasks");
    fixture.execute(&format!(
        "INSERT INTO {tasks} VALUES ({MOVING_TENANT}, 1, 1, 'tenant-7-only task', false)"
    ));
    let serving = serve(&fixture);
    let port = serving.port;

    let stop = std::sync::atomic::AtomicBool::new(false);
    let gave_up = || {
        serving.logged(&[
            "rows kept moving into or out of the tenant during its builds",
            &format!("tenant={MOVING_TENANT} "),
        ])
    };
    let plan = std::thread::scope(|scope| {
        struct Stop<'a>(&'a std::sync::atomic::AtomicBool);
        impl Drop for Stop<'_> {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let stopping = Stop(&stop);
        let mover = scope.spawn(|| {
            let mut client = fixture.client();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                client
                    .batch_execute(&format!(
                        "UPDATE {tasks} SET tenant_id = 15 - tenant_id
                          WHERE id = 1 AND tenant_id IN (7, 8)"
                    ))
                    .unwrap();
            }
        });
        let request =
            scope.spawn(|| bootstrap(&fixture, port, &fixture.tenant_grant(MOVING_TENANT, 1)).0);
        let deadline = Instant::now() + PREPARATION_TIMEOUT;
        while gave_up() == 0 {
            assert!(
                Instant::now() < deadline && !request.is_finished(),
                "no build met a move: {:#?}",
                serving.tenant_lines()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(stopping);
        mover.join().unwrap();
        request.join().unwrap()
    });
    let rows: i64 = fixture
        .client()
        .query_one(
            &format!("SELECT count(*) FROM {tasks} WHERE tenant_id = $1"),
            &[&MOVING_TENANT],
        )
        .unwrap()
        .get(0);
    assert_eq!(plan.rows(&fixture.relation("tasks")), rows as u64);
    let lines = serving.tenant_lines();
    let during = |text: &str| lines.iter().filter(|line| line.contains(text)).count();
    assert!(
        during("a row moved into or out of the tenant during its build") >= 1
            && during("tenant built again after a move") >= 5,
        "{lines:#?}"
    );
    let warned = lines
        .iter()
        .position(|line| line.contains("rows kept moving"))
        .unwrap();
    let reopened = lines[warned..]
        .iter()
        .position(|line| line.contains("tenant opened"))
        .map_or(lines.len(), |offset| warned + offset);
    assert!(
        !lines[warned..reopened]
            .iter()
            .any(|line| line.contains("tenant build started")),
        "a tenant that gave up was built before it was asked for again: {lines:#?}"
    );

    serving.shutdown();
}

/// One gauge of a metrics scrape.
fn gauge(port: u16, name: &str) -> u64 {
    let scrape = http(port, "GET", "/metrics", &[], None);
    assert_eq!(scrape.status, 200);
    let body = String::from_utf8(scrape.body).unwrap();
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("calque_{name} ")))
        .unwrap_or_else(|| panic!("the scrape has no {name}: {body}"))
        .parse::<f64>()
        .unwrap() as u64
}

/// A warm tenant is cut again by the re-cut's rule: once the bytes it
/// receives since its cut outgrow the ratio of its artifact, at most once per
/// interval. The log-tail gauges follow that tenant.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_warm_tenant_is_refreshed_by_the_re_cut_rule_and_the_tail_gauges_follow_it() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("refresh");
    fixture.seed();
    let metrics_port = reserved_port();
    let metrics = format!("127.0.0.1:{metrics_port}");
    let start = |interval: &str| {
        serve_with(
            &fixture,
            &[
                "--recut-tail-ratio",
                "0.5",
                "--recut-min-interval",
                interval,
                "--metrics-bind-address",
                &metrics,
            ],
        )
    };
    let serving = start("0");
    let port = serving.port;
    let builds = |serving: &Serving| {
        serving.logged(&["tenant build started", &format!("tenant={WARMED_TENANT} ")])
    };
    let warm = |serving: &Serving| {
        let deadline = Instant::now() + PREPARATION_TIMEOUT;
        loop {
            let report = tenant_state(&fixture, serving.port, WARMED_TENANT);
            if report["state"] == "warm" {
                return report;
            }
            assert!(Instant::now() < deadline, "the tenant never got warm");
            std::thread::sleep(Duration::from_millis(200));
        }
    };
    let settled = || std::thread::sleep(Duration::from_secs(3));

    let warmed = tenant_order(port, &fixture.order(TenantAction::Warm, WARMED_TENANT));
    assert_eq!(warmed.status, 200, "{}", warmed.text());
    let size = warm(&serving)["sizeBytes"].as_u64().unwrap();
    settled();
    let threshold = gauge(metrics_port, "artifact_recut_threshold_bytes");
    assert_eq!(threshold, size.div_ceil(2));
    assert_eq!(gauge(metrics_port, "canonical_log_tail_bytes"), 0);

    // A write well under the threshold is counted and refreshes nothing.
    fixture.retitle(WARMED_TENANT, 1, "tenant-4-only small", 1);
    settled();
    let tail = gauge(metrics_port, "canonical_log_tail_bytes");
    assert!(
        tail > 0 && tail < threshold,
        "tail {tail}, threshold {threshold}"
    );
    assert!(gauge(metrics_port, "canonical_log_tail_entries") >= 1);
    assert_eq!(builds(&serving), 1);

    // Writes past it refresh the tenant once, and its tail starts again.
    let title = format!("tenant-4-only {}", "y".repeat(200));
    let writes = usize::try_from(threshold / 200).unwrap() + 1;
    fixture.retitle(WARMED_TENANT, 1, &title, writes);
    let deadline = Instant::now() + PREPARATION_TIMEOUT;
    while builds(&serving) < 2 {
        assert!(Instant::now() < deadline, "the tenant was not cut again");
        std::thread::sleep(Duration::from_millis(200));
    }
    warm(&serving);
    settled();
    assert_eq!(builds(&serving), 2);
    assert!(gauge(metrics_port, "canonical_log_tail_bytes") < threshold);
    serving.shutdown();

    // Within the interval of its cut, a tenant past the ratio waits.
    let serving = start("3600");
    warm(&serving);
    fixture.retitle(WARMED_TENANT, 1, &title, writes * 2);
    settled();
    assert!(gauge(metrics_port, "canonical_log_tail_bytes") >= threshold);
    assert_eq!(builds(&serving), 0, "the restored cut was not replaced");

    serving.shutdown();
}
