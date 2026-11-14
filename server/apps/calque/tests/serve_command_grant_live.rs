//! `calque serve`'s mutation route, gated by the signed command capability,
//! against a live PostgreSQL 18.
//!
//! Mounting `mutation-http` on `serve` must not make every principal that can
//! bootstrap a writer of PostgreSQL rows: the signed command capability a
//! dataset grant carries gates the route.
//!
//! Every case asserts the PostgreSQL rows, not only the response code, because
//! a refusal that still wrote is the failure that matters.
//!
//! These tests are `#[ignore]`d and run through
//! `scripts/live-postgres-suite.sh`; without `TEST_DATABASE_URL` they fail
//! rather than silently pass.

use std::{
    collections::{BTreeMap, VecDeque},
    io::{BufRead, BufReader, Read as _, Write as _},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, ChildStderr, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dataset_grant::{GrantCommandCapability, GrantCommandOperation};
use dataset_grant_issuer::GrantIssuer;
use postgres::{Client, NoTls};
use prost::Message as _;
use serde_json::Value;
use sync_protocol::{
    change::{
        cell_value::Kind, operation::Payload, CellValue, ColumnValue, Delete, FullRow, Operation,
        OperationKind, PrimaryKey, RowPatch,
    },
    encode_primary_key,
    mutation::{
        AffectedRow, Cursor, MutationBatchRequest, MutationBatchResponse, MutationEnvelope,
        MutationFootprint, MutationOutcomeCode,
    },
};

const GRANT_KEY_ENV: &str = "CALQUE_LIVE_COMMAND_GRANT_KEY";
const GRANT_AUDIENCE: &str = "command-grant.test";
const DATABASE_ENV: &str = "CALQUE_LIVE_COMMAND_GRANT_DATABASE_URL";
const READY_TIMEOUT: Duration = Duration::from_secs(300);
const ARTIFACT_PROFILE: &str = "chromium-native-artifact-v1";
const MUTATION_CONTENT_TYPE: &str = "application/vnd.calque.mutation.v1+protobuf";
const CLIENT_INSTANCE: &str = "command-grant-live";
const DEVICE: &str = "command-grant-device";

/// One `calque serve` at a time from this binary.
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

/// `url` with `database` as its database and, when given, `credentials` as
/// its role and password.
fn url_for(url: &str, credentials: Option<(&str, &str)>, database: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("a URL has a scheme");
    let (userinfo, location) = rest.rsplit_once('@').expect("the suite's URL names a role");
    let (host, path) = location
        .split_once('/')
        .expect("the suite's URL names a database");
    let query = path.split_once('?').map_or("", |(_, query)| query);
    let userinfo = credentials.map_or_else(
        || userinfo.to_owned(),
        |(role, password)| format!("{role}:{password}"),
    );
    let query = if query.is_empty() {
        String::new()
    } else {
        format!("?{query}")
    };
    format!("{scheme}://{userinfo}@{host}/{database}{query}")
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

/// A two-collection source-wide dataset. Two tables, because "a grant
/// authorizing collection A cannot write collection B" is not a claim one table
/// can carry.
struct Fixture {
    suffix: String,
    directory: tempfile::TempDir,
    issuer: GrantIssuer,
    /// The connection that owns the tables and the publication.
    owner_url: String,
    /// The connection `calque serve` and `calque destroy` get.
    serve_url: String,
    /// The database and role [`Fixture::least_privilege`] made, which
    /// [`Fixture::drop_source`] drops.
    least_privilege: Option<String>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self {
            suffix: format!("{name}_{}", uuid::Uuid::new_v4().simple()),
            directory: tempfile::tempdir().unwrap(),
            issuer: GrantIssuer::generate("live-command-grant-key", GRANT_AUDIENCE)
                .unwrap()
                .0,
            owner_url: database_url(),
            serve_url: database_url(),
            least_privilege: None,
        }
    }

    /// A dataset in a database of its own, whose tables the suite's role
    /// owns, served by a role that owns nothing there. That role may log in
    /// and replicate, create Calque's schemas in the database, and select,
    /// insert, update and delete the tables' rows; nothing more. The
    /// publication is left to the test.
    fn least_privilege(name: &str) -> Self {
        let mut fixture = Self::new(name);
        let name = format!("calque_lp_{}", fixture.suffix);
        let password = uuid::Uuid::new_v4().simple().to_string();
        let mut suite = Client::connect(&database_url(), NoTls).unwrap();
        suite
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .unwrap();
        suite
            .batch_execute(&format!(
                "CREATE ROLE {name} LOGIN REPLICATION PASSWORD '{password}'"
            ))
            .unwrap();
        fixture.least_privilege = Some(name.clone());
        fixture.owner_url = url_for(&database_url(), None, &name);
        fixture.serve_url = url_for(&database_url(), Some((&name, &password)), &name);
        fixture.seed_tables();
        let mut owner = Client::connect(&fixture.owner_url, NoTls).unwrap();
        owner
            .batch_execute(&format!(
                "GRANT CREATE ON DATABASE {name} TO {name};
                 GRANT SELECT, INSERT, UPDATE, DELETE ON {tasks}, {notes} TO {name};",
                tasks = fixture.tasks(),
                notes = fixture.notes(),
            ))
            .unwrap();
        fixture
    }

    fn tasks(&self) -> String {
        format!("tasks_{}", self.suffix)
    }

    fn notes(&self) -> String {
        format!("notes_{}", self.suffix)
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

    /// A read-only grant: no command capability at all.
    fn read_grant(&self) -> String {
        self.grant(Vec::new(), 1, unix_seconds(), 600)
    }

    fn grant(
        &self,
        commands: Vec<GrantCommandCapability>,
        generation: u64,
        issued_at: u64,
        lifetime: u64,
    ) -> String {
        self.issuer
            .issue_with_commands(
                "command-grant-member".to_owned(),
                BTreeMap::new(),
                commands,
                generation,
                issued_at,
                lifetime,
            )
            .unwrap()
    }

    fn seed(&self) {
        self.seed_tables();
        self.publish();
    }

    fn publish(&self) {
        Client::connect(&self.owner_url, NoTls)
            .unwrap()
            .batch_execute(&format!(
                "CREATE PUBLICATION {publication} FOR TABLE {tasks}, {notes}",
                tasks = self.tasks(),
                notes = self.notes(),
                publication = self.publication(),
            ))
            .unwrap();
    }

    fn seed_tables(&self) {
        let mut client = Client::connect(&self.owner_url, NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE TABLE {tasks} (
                     id uuid NOT NULL PRIMARY KEY,
                     title text NOT NULL,
                     status text NOT NULL
                 );
                 CREATE TABLE {notes} (
                     id uuid NOT NULL PRIMARY KEY,
                     body text NOT NULL
                 );
                 ALTER TABLE {tasks} REPLICA IDENTITY FULL;
                 ALTER TABLE {notes} REPLICA IDENTITY FULL;",
                tasks = self.tasks(),
                notes = self.notes(),
            ))
            .unwrap();
        let mut transaction = client.transaction().unwrap();
        for row in 0..4_u64 {
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {} (id, title, status) VALUES ($1, $2, $3)",
                        self.tasks()
                    ),
                    &[&row_id(row), &format!("task {row}"), &"open"],
                )
                .unwrap();
            transaction
                .execute(
                    &format!("INSERT INTO {} (id, body) VALUES ($1, $2)", self.notes()),
                    &[&row_id(row), &format!("note {row}")],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    fn drop_source(&self) {
        let Ok(mut client) = Client::connect(&database_url(), NoTls) else {
            return;
        };
        if let Some(name) = &self.least_privilege {
            let _ = client.batch_execute(&format!(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                     WHERE slot_name = '{}'",
                self.slot()
            ));
            let _ = client.batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"));
            let _ = client.batch_execute(&format!("DROP ROLE IF EXISTS {name}"));
            return;
        }
        let _ = client.batch_execute(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                 WHERE slot_name = '{slot}';
             DELETE FROM calque_control.source_fence_lease WHERE source_id = '{source}';
             DELETE FROM calque_control.source_slot_enrollment WHERE source_id = '{source}';
             DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {tasks};
             DROP TABLE IF EXISTS {notes};",
            slot = self.slot(),
            source = self.source_id(),
            publication = self.publication(),
            tasks = self.tasks(),
            notes = self.notes(),
        ));
    }

    /// Both collections advertise all three operations, so every refusal below
    /// comes from the grant rather than from the manifest's own ceiling.
    fn config(&self) -> PathBuf {
        let document = serde_json::json!({
            "application": { "id": "command-grant-ops", "version": "1.0.0" },
            "protocol": { "manifest": 1, "artifact": 1, "changeStream": 1, "mutation": 1 },
            "sources": [{
                "id": self.source_id(),
                "databaseUrlEnv": DATABASE_ENV,
                "publication": self.publication(),
                "slot": self.slot(),
            }],
            "scopes": [{
                "id": "all",
                "source": self.source_id(),
                "kind": "source-wide",
                "dataVersion": 1,
            }],
            "collections": [
                {
                    "id": self.tasks(),
                    "source": self.source_id(),
                    "kind": "table",
                    "relation": { "schema": "public", "table": self.tasks() },
                    "columns": [
                        { "name": "id", "postgresType": "uuid", "nullable": false },
                        { "name": "title", "postgresType": "text", "nullable": false },
                        { "name": "status", "postgresType": "text", "nullable": false },
                    ],
                    "primaryKey": ["id"],
                    "scope": "all",
                    "adapter": "direct-columnar",
                    "encoding": "calque-columnar",
                },
                {
                    "id": self.notes(),
                    "source": self.source_id(),
                    "kind": "table",
                    "relation": { "schema": "public", "table": self.notes() },
                    "columns": [
                        { "name": "id", "postgresType": "uuid", "nullable": false },
                        { "name": "body", "postgresType": "text", "nullable": false },
                    ],
                    "primaryKey": ["id"],
                    "scope": "all",
                    "adapter": "direct-columnar",
                    "encoding": "calque-columnar",
                },
            ],
            "commands": [
                {
                    "id": "edit-task",
                    "kind": "crud",
                    "collection": self.tasks(),
                    "operations": ["insert", "update", "delete"],
                    "writableColumns": ["title", "status"],
                },
                {
                    "id": "edit-note",
                    "kind": "crud",
                    "collection": self.notes(),
                    "operations": ["insert", "update", "delete"],
                    "writableColumns": ["body"],
                },
            ],
        });
        let path = self.directory.path().join("calque.config.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        path
    }

    fn title_of(&self, row: u64) -> Option<String> {
        let mut client = Client::connect(&self.owner_url, NoTls).unwrap();
        client
            .query_opt(
                &format!("SELECT title FROM {} WHERE id = $1", self.tasks()),
                &[&row_id(row)],
            )
            .unwrap()
            .map(|row| row.get(0))
    }

    fn body_of(&self, row: u64) -> Option<String> {
        let mut client = Client::connect(&self.owner_url, NoTls).unwrap();
        client
            .query_opt(
                &format!("SELECT body FROM {} WHERE id = $1", self.notes()),
                &[&row_id(row)],
            )
            .unwrap()
            .map(|row| row.get(0))
    }

    fn task_count(&self) -> i64 {
        let mut client = Client::connect(&self.owner_url, NoTls).unwrap();
        client
            .query_one(&format!("SELECT count(*) FROM {}", self.tasks()), &[])
            .unwrap()
            .get(0)
    }

    fn receipt_count(&self, mutation_id: &str) -> i64 {
        let mut client = Client::connect(&self.serve_url, NoTls).unwrap();
        client
            .query_one(
                "SELECT count(*) FROM calque_internal.mutation_receipts WHERE mutation_id = $1",
                &[&mutation_id],
            )
            .unwrap()
            .get(0)
    }
}

fn row_id(row: u64) -> uuid::Uuid {
    let mut id = [0_u8; 16];
    id[0..8].copy_from_slice(&(row + 1).to_be_bytes());
    uuid::Uuid::from_bytes(id)
}

fn capability(collection: &str, operations: &[GrantCommandOperation]) -> GrantCommandCapability {
    GrantCommandCapability {
        collection: collection.to_owned(),
        operations: operations.to_vec(),
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
    fn shutdown(mut self) -> bool {
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let status = self.child.wait().unwrap();
        self.stopped = true;
        status.success()
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

fn serve(fixture: &Fixture) -> Serving {
    serve_with_key(
        fixture,
        &fixture.issuer.verification_key_environment().unwrap(),
    )
}

/// `serve`, started against a verification key this test did not generate:
/// the CLI round trip below configures it with the key `calque-dev-grant`
/// wrote, as an operator would.
fn serve_with_key(fixture: &Fixture, key_environment: &str) -> Serving {
    let port = reserved_port();
    let started = Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "serve",
            "--config",
            fixture.config().to_str().unwrap(),
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
        .env(DATABASE_ENV, &fixture.serve_url)
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
    HttpResponse {
        status,
        body: raw[split + 4..].to_vec(),
    }
}

/// The facts one bootstrap plan gives a writer: the scope token that binds a
/// batch to this plan, the cursor a mutation is based on, and the wire identity
/// of each collection.
struct Plan {
    scope_token: Vec<u8>,
    activation_cursor: Vec<u8>,
    tables: BTreeMap<String, PlanTable>,
}

struct PlanTable {
    table_id: String,
    columns: BTreeMap<String, u32>,
}

impl Plan {
    fn table(&self, source_table: &str) -> &PlanTable {
        self.tables
            .get(source_table)
            .unwrap_or_else(|| panic!("the plan carries {source_table}: {:?}", self.tables.keys()))
    }
}

fn bootstrap(port: u16, grant: &str, relations: &[String]) -> Plan {
    let document = serde_json::json!({
        "protocolVersion": 1,
        "requiredTables": relations,
        "cachedArtifactHints": [],
        "acceptedEncodings": ["zstd", "identity"],
        "memoryBudgetBytes": 1_073_741_824_u64,
        "clientHeapReserveBytes": 67_108_864_u64,
        "clientInstanceId": CLIENT_INSTANCE,
        "deviceId": DEVICE,
    });
    let response = http(
        port,
        "POST",
        "/v1/bootstrap",
        &[
            ("authorization", format!("Bearer {grant}")),
            ("content-type", "application/json".to_owned()),
            ("accept", "application/json".to_owned()),
            ("accept-encoding", "zstd, br, identity".to_owned()),
            ("x-calque-artifact-profile", ARTIFACT_PROFILE.to_owned()),
        ],
        Some(serde_json::to_vec(&document).unwrap().as_slice()),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let plan = response.json();
    let mut tables = BTreeMap::new();
    for artifact in plan["artifacts"].as_array().expect("a plan has artifacts") {
        for table in artifact["runtime"]["tables"]
            .as_array()
            .expect("a runtime names its tables")
        {
            tables.insert(
                table["sourceTable"]
                    .as_str()
                    .expect("source table")
                    .to_owned(),
                PlanTable {
                    table_id: table["tableId"].as_str().expect("table id").to_owned(),
                    columns: table["columns"]
                        .as_array()
                        .expect("columns")
                        .iter()
                        .map(|column| {
                            (
                                column["name"].as_str().expect("column name").to_owned(),
                                u32::try_from(column["columnId"].as_u64().expect("column id"))
                                    .expect("a wire column id is 32-bit"),
                            )
                        })
                        .collect(),
                },
            );
        }
    }
    Plan {
        scope_token: plan["authorization"]["scopeToken"]
            .as_str()
            .expect("a served plan carries a scope token")
            .as_bytes()
            .to_vec(),
        activation_cursor: plan["activationCursor"]
            .as_str()
            .expect("a served plan carries an activation cursor")
            .as_bytes()
            .to_vec(),
        tables,
    }
}

fn cell(column_id: u32, kind: Kind) -> ColumnValue {
    ColumnValue {
        column_id,
        value: Some(CellValue { kind: Some(kind) }),
    }
}

fn uuid_cell(column_id: u32, id: uuid::Uuid) -> ColumnValue {
    cell(column_id, Kind::Uuid16(id.as_bytes().to_vec()))
}

fn text_cell(column_id: u32, value: &str) -> ColumnValue {
    cell(column_id, Kind::TextValue(value.to_owned()))
}

fn primary_key(table: &PlanTable, id: uuid::Uuid) -> Vec<u8> {
    encode_primary_key(&PrimaryKey {
        codec_version: 1,
        parts: vec![uuid_cell(table.columns["id"], id)],
    })
    .expect("a single uuid key encodes")
}

/// One mutation envelope, in the shape `mutation-crud` parses. `kind` and
/// `payload` are the only things that vary across the operation cases below.
fn envelope(
    plan: &Plan,
    table: &PlanTable,
    mutation_id: &str,
    id: uuid::Uuid,
    kind: OperationKind,
    payload: Payload,
) -> MutationEnvelope {
    let key = primary_key(table, id);
    let operation = Operation {
        table_id: table.table_id.clone(),
        kind: kind as i32,
        primary_key: key.clone(),
        payload: Some(payload),
    };
    MutationEnvelope {
        mutation_id: mutation_id.to_owned(),
        command_name: format!("crud:{}", table.table_id),
        command_version: 1,
        canonical_input: operation.encode_to_vec(),
        base_cursor: Some(Cursor {
            token: plan.activation_cursor.clone(),
        }),
        created_at_unix_micros: unix_micros(),
        expected_versions: Vec::new(),
        declared_footprint: Some(MutationFootprint {
            rows: vec![AffectedRow {
                table_id: table.table_id.clone(),
                primary_key: key,
            }],
            table_ids: vec![table.table_id.clone()],
        }),
    }
}

fn update(
    plan: &Plan,
    table: &PlanTable,
    id: uuid::Uuid,
    column: &str,
    value: &str,
) -> MutationEnvelope {
    envelope(
        plan,
        table,
        &format!("mutation-{}", uuid::Uuid::new_v4()),
        id,
        OperationKind::UpdatePatch,
        Payload::Update(RowPatch {
            changed_columns: vec![text_cell(table.columns[column], value)],
        }),
    )
}

fn post_mutation(port: u16, grant: &str, plan: &Plan, envelope: MutationEnvelope) -> HttpResponse {
    let request = MutationBatchRequest {
        protocol_version: 1,
        client_instance_id: CLIENT_INSTANCE.to_owned(),
        device_id: DEVICE.to_owned(),
        authorization_scope_token: plan.scope_token.clone(),
        mutations: vec![envelope],
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

fn outcome_code(response: &HttpResponse) -> i32 {
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let decoded =
        MutationBatchResponse::decode(response.body.as_slice()).expect("a protobuf response");
    assert_eq!(decoded.outcomes.len(), 1);
    decoded.outcomes[0].code
}

/// The startup contract states that a write surface exists and exactly what it
/// is, and a grant that carries no command capability still finds nothing.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_read_only_grant_finds_no_write_surface_and_changes_no_row() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("readonly");
    fixture.seed();
    let serving = serve(&fixture);

    assert_eq!(serving.line("commands"), Some("2"));
    assert_eq!(
        serving.line("mutations"),
        Some("mounted-per-collection-command-grant"),
        "the startup contract has to say the route is mounted and under what"
    );
    assert_eq!(
        serving.line("mutation_commands"),
        Some(
            format!(
                "{}:insert+update+delete,{}:insert+update+delete",
                fixture.notes(),
                fixture.tasks()
            )
            .as_str()
        ),
        "and exactly which collections and operations it mounts"
    );

    let read_grant = fixture.read_grant();
    let plan = bootstrap(
        serving.port,
        &read_grant,
        &[
            format!("public.{}", fixture.tasks()),
            format!("public.{}", fixture.notes()),
        ],
    );
    let tasks = plan.table(&fixture.tasks());

    // A grant that planned a read reaches no write path, and cannot tell that
    // one is there.
    let refused = post_mutation(
        serving.port,
        &read_grant,
        &plan,
        update(&plan, tasks, row_id(0), "title", "widened by a read grant"),
    );
    assert_eq!(
        refused.status, 404,
        "a grant with no command capability must not reach the mutation route"
    );
    assert_eq!(fixture.title_of(0).as_deref(), Some("task 0"));

    // And no request shape lets it learn more than that.
    for (content_type, body) in [
        ("application/json", b"{}".to_vec()),
        (MUTATION_CONTENT_TYPE, vec![0xff, 0xff, 0xff]),
        (MUTATION_CONTENT_TYPE, Vec::new()),
    ] {
        let probe = http(
            serving.port,
            "POST",
            "/v1/mutations",
            &[
                ("authorization", format!("Bearer {read_grant}")),
                ("content-type", content_type.to_owned()),
            ],
            Some(body.as_slice()),
        );
        assert_eq!(probe.status, 404, "content-type {content_type}");
    }

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}

/// The capability, exercised in every direction it can be wrong, with the rows
/// checked after each one.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_command_grant_writes_exactly_the_collection_and_operation_it_names() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("capability");
    fixture.seed();
    let serving = serve(&fixture);

    let tasks_update = fixture.grant(
        vec![capability(
            &fixture.tasks(),
            &[GrantCommandOperation::Update],
        )],
        1,
        unix_seconds(),
        600,
    );
    let plan = bootstrap(
        serving.port,
        &tasks_update,
        &[
            format!("public.{}", fixture.tasks()),
            format!("public.{}", fixture.notes()),
        ],
    );
    let tasks = plan.table(&fixture.tasks());
    let notes = plan.table(&fixture.notes());

    // 1. What the grant names, on the collection it names: accepted, and the
    //    row is what changed.
    let accepted = update(&plan, tasks, row_id(0), "title", "authorized update");
    let mutation_id = accepted.mutation_id.clone();
    assert_eq!(
        outcome_code(&post_mutation(serving.port, &tasks_update, &plan, accepted)),
        MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32
    );
    assert_eq!(fixture.title_of(0).as_deref(), Some("authorized update"));
    assert_eq!(fixture.receipt_count(&mutation_id), 1);

    // 2. Collection A's grant cannot write collection B. The capability is
    //    non-empty, so the route exists for this principal -- and the command
    //    it did not authorize is still refused, without touching the row.
    let cross = post_mutation(
        serving.port,
        &tasks_update,
        &plan,
        update(
            &plan,
            notes,
            row_id(0),
            "body",
            "widened across collections",
        ),
    );
    assert_eq!(
        outcome_code(&cross),
        MutationOutcomeCode::RejectedAuthorization as i32
    );
    assert_eq!(fixture.body_of(0).as_deref(), Some("note 0"));

    // 3. An update capability is not a delete capability.
    let deleted = envelope(
        &plan,
        tasks,
        &format!("mutation-{}", uuid::Uuid::new_v4()),
        row_id(1),
        OperationKind::DeleteByKey,
        Payload::Delete(Delete {}),
    );
    let deleted_id = deleted.mutation_id.clone();
    assert_eq!(
        outcome_code(&post_mutation(serving.port, &tasks_update, &plan, deleted)),
        MutationOutcomeCode::RejectedAuthorization as i32
    );
    assert_eq!(fixture.title_of(1).as_deref(), Some("task 1"));
    assert_eq!(fixture.receipt_count(&deleted_id), 0);

    // 4. Nor an insert capability.
    let inserted_id = row_id(9);
    let insert = envelope(
        &plan,
        tasks,
        &format!("mutation-{}", uuid::Uuid::new_v4()),
        inserted_id,
        OperationKind::InsertFullRow,
        Payload::Insert(FullRow {
            columns: vec![
                uuid_cell(tasks.columns["id"], inserted_id),
                text_cell(tasks.columns["title"], "inserted"),
                text_cell(tasks.columns["status"], "open"),
            ],
        }),
    );
    assert_eq!(
        outcome_code(&post_mutation(serving.port, &tasks_update, &plan, insert)),
        MutationOutcomeCode::RejectedAuthorization as i32
    );
    assert_eq!(fixture.task_count(), 4);
    assert_eq!(fixture.title_of(9), None);

    // 5. The other direction, so that step 3 is not passing because deletes
    //    never work here: a grant that does name delete deletes the row.
    let tasks_delete = fixture.grant(
        vec![capability(
            &fixture.tasks(),
            &[GrantCommandOperation::Delete],
        )],
        1,
        unix_seconds(),
        600,
    );
    let delete_plan = bootstrap(
        serving.port,
        &tasks_delete,
        &[format!("public.{}", fixture.tasks())],
    );
    let delete_tasks = delete_plan.table(&fixture.tasks());
    let authorized_delete = envelope(
        &delete_plan,
        delete_tasks,
        &format!("mutation-{}", uuid::Uuid::new_v4()),
        row_id(2),
        OperationKind::DeleteByKey,
        Payload::Delete(Delete {}),
    );
    assert_eq!(
        outcome_code(&post_mutation(
            serving.port,
            &tasks_delete,
            &delete_plan,
            authorized_delete
        )),
        MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32
    );
    assert_eq!(fixture.title_of(2), None);
    assert_eq!(fixture.task_count(), 3);
    // ...and that same grant still cannot update, which is the pair that makes
    // the capability per operation rather than per collection.
    assert_eq!(
        outcome_code(&post_mutation(
            serving.port,
            &tasks_delete,
            &delete_plan,
            update(
                &delete_plan,
                delete_tasks,
                row_id(3),
                "title",
                "widened by a delete grant"
            ),
        )),
        MutationOutcomeCode::RejectedAuthorization as i32
    );
    assert_eq!(fixture.title_of(3).as_deref(), Some("task 3"));

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}

#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn an_expired_forged_or_replayed_capability_writes_nothing() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("forged");
    fixture.seed();
    let serving = serve(&fixture);

    let valid = fixture.grant(
        vec![capability(
            &fixture.tasks(),
            &[GrantCommandOperation::Update],
        )],
        1,
        unix_seconds(),
        600,
    );
    let plan = bootstrap(
        serving.port,
        &valid,
        &[format!("public.{}", fixture.tasks())],
    );
    let tasks = plan.table(&fixture.tasks());

    // Expired: issued far enough in the past that its 300-second window closed.
    let expired = fixture.grant(
        vec![capability(
            &fixture.tasks(),
            &[GrantCommandOperation::Update],
        )],
        1,
        unix_seconds() - 600,
        300,
    );

    // Forged: the capability rewritten to `delete` after signing, and one
    // grafted onto a grant that never carried one.
    let forged = rewrite_payload(&valid, "\"update\"", "\"delete\"");
    let grafted = rewrite_payload(
        &fixture.read_grant(),
        "\"expiresAtUnixSeconds\"",
        &format!(
            "\"commands\":[{{\"collection\":\"{}\",\"operations\":[\"update\"]}}],\"expiresAtUnixSeconds\"",
            fixture.tasks()
        ),
    );

    // Replayed against a generation this deployment does not serve.
    let wrong_generation = fixture.grant(
        vec![capability(
            &fixture.tasks(),
            &[GrantCommandOperation::Update],
        )],
        2,
        unix_seconds(),
        600,
    );

    for (name, token) in [
        ("expired", expired),
        ("forged", forged),
        ("grafted", grafted),
        ("wrong-generation", wrong_generation),
    ] {
        let response = post_mutation(
            serving.port,
            &token,
            &plan,
            update(&plan, tasks, row_id(0), "title", "widened by a bad grant"),
        );
        assert_eq!(
            response.status, 401,
            "a {name} grant reached the mutation route"
        );
        assert_eq!(
            fixture.title_of(0).as_deref(),
            Some("task 0"),
            "a {name} grant changed a row"
        );
    }

    // The control, so the four refusals above are about the grants and not
    // about the request: the same request under the valid grant is accepted.
    assert_eq!(
        outcome_code(&post_mutation(
            serving.port,
            &valid,
            &plan,
            update(
                &plan,
                tasks,
                row_id(0),
                "title",
                "authorized after the refusals"
            ),
        )),
        MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32
    );
    assert_eq!(
        fixture.title_of(0).as_deref(),
        Some("authorized after the refusals")
    );

    // A replay of one accepted mutation id writes once: the capability does
    // not turn idempotency off.
    let replayed = update(&plan, tasks, row_id(1), "title", "replayed once");
    let replayed_id = replayed.mutation_id.clone();
    let first = post_mutation(serving.port, &valid, &plan, replayed.clone());
    assert_eq!(
        outcome_code(&first),
        MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32
    );
    // The replay is answered from the first one's receipt: still awaiting its
    // echo, or already canonical when CDC carried the first write back before
    // the replay arrived. A second execution would answer `Accepted…`.
    let second = outcome_code(&post_mutation(serving.port, &valid, &plan, replayed));
    assert!(
        second == MutationOutcomeCode::AlreadyAcceptedAwaitingCanonicalEcho as i32
            || second == MutationOutcomeCode::AlreadyCanonical as i32,
        "a replayed mutation id was answered with outcome {second}, not from its receipt"
    );
    assert_eq!(fixture.receipt_count(&replayed_id), 1);
    assert_eq!(fixture.title_of(1).as_deref(), Some("replayed once"));

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}

/// The `calque-dev-grant` binary, built on demand: it belongs to
/// `dataset-grant-issuer`, so `CARGO_BIN_EXE_` does not name it and
/// `cargo test -p calque` would otherwise run a stale or missing binary.
fn dev_grant_binary() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_calque"))
        .parent()
        .expect("the test binary's directory")
        .join("calque-dev-grant");
    let built = Command::new(env!("CARGO"))
        .args([
            "build",
            "--quiet",
            "-p",
            "dataset-grant-issuer",
            "--bin",
            "calque-dev-grant",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("cargo build runs");
    assert!(built.success(), "calque-dev-grant did not build");
    assert!(
        path.exists(),
        "calque-dev-grant is not at {}",
        path.display()
    );
    path
}

/// Every other case mints through `GrantIssuer` in process; this one runs the
/// binary an operator runs, through its JSON document and environment
/// variable, to a committed PostgreSQL row.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_grant_minted_by_the_cli_writes_a_row() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("clianddev");
    fixture.seed();

    let key_file = fixture.directory.path().join("dev/grant-key.pk8");
    let mint = |extra: &[&str]| {
        let output = Command::new(dev_grant_binary())
            .args([
                "--key-file",
                key_file.to_str().unwrap(),
                "--key-id",
                "live-cli-key",
                "--audience",
                GRANT_AUDIENCE,
                "--subject",
                "cli-member",
                "--generation",
                "1",
                "--lifetime-seconds",
                "600",
            ])
            .args(extra)
            .output()
            .expect("calque-dev-grant runs");
        assert!(
            output.status.success(),
            "calque-dev-grant failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).expect("calque-dev-grant prints JSON")
    };

    // `tasks` may be updated; `notes` is not named at all. The manifest
    // advertises all three operations for both, so every refusal below is the
    // grant's, and the grant came out of the binary.
    let capable = mint(&["--command", &format!("{}=update", fixture.tasks())]);
    assert_eq!(
        capable["commands"],
        serde_json::json!([format!("{}:update", fixture.tasks())])
    );

    // The server is configured with the key the binary wrote, the way an
    // operator configures it: the document's own `verificationKeyEnvironment`.
    let serving = serve_with_key(
        &fixture,
        &serde_json::to_string(&capable["verificationKeyEnvironment"]).unwrap(),
    );
    assert_eq!(
        serving.line("mutations"),
        Some("mounted-per-collection-command-grant")
    );

    let token = capable["token"]
        .as_str()
        .expect("a minted token")
        .to_owned();
    let plan = bootstrap(
        serving.port,
        &token,
        &[
            format!("public.{}", fixture.tasks()),
            format!("public.{}", fixture.notes()),
        ],
    );
    let tasks = plan.table(&fixture.tasks());
    let notes = plan.table(&fixture.notes());

    let accepted = post_mutation(
        serving.port,
        &token,
        &plan,
        update(
            &plan,
            tasks,
            row_id(1),
            "title",
            "written by a minted grant",
        ),
    );
    assert_eq!(
        outcome_code(&accepted),
        MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32,
        "a grant minted by the CLI was refused"
    );
    assert_eq!(
        fixture.title_of(1).as_deref(),
        Some("written by a minted grant"),
        "the row is the evidence, not the status code"
    );

    // The same minted grant reaches nothing it did not name, so the round trip
    // carried the capability rather than merely carrying a valid signature.
    let refused = post_mutation(
        serving.port,
        &token,
        &plan,
        update(&plan, notes, row_id(1), "body", "widened past the mint"),
    );
    assert_eq!(
        outcome_code(&refused),
        MutationOutcomeCode::RejectedAuthorization as i32
    );
    assert_eq!(fixture.body_of(1).as_deref(), Some("note 1"));

    // And the same binary, rerun without `--command` against the same key file,
    // mints a grant this server accepts for reading and refuses the route to.
    let read_only = mint(&[]);
    assert_eq!(read_only["commands"], serde_json::json!([]));
    let read_token = read_only["token"].as_str().expect("a minted token");
    let read_plan = bootstrap(
        serving.port,
        read_token,
        &[format!("public.{}", fixture.tasks())],
    );
    let probe = post_mutation(
        serving.port,
        read_token,
        &read_plan,
        update(
            &read_plan,
            read_plan.table(&fixture.tasks()),
            row_id(1),
            "title",
            "widened by a read grant",
        ),
    );
    assert_eq!(
        probe.status, 404,
        "a grant the same binary minted without --command reached the route"
    );
    assert_eq!(
        fixture.title_of(1).as_deref(),
        Some("written by a minted grant")
    );

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}

/// Rewrites the signed payload of a token and reframes it under the original
/// signature, as a client widening its own capability would.
fn rewrite_payload(token: &str, from: &str, to: &str) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let (payload, signature) = token.split_once('.').expect("a framed grant");
    let decoded = String::from_utf8(URL_SAFE_NO_PAD.decode(payload).expect("base64url"))
        .expect("a JSON payload");
    let rewritten = decoded.replace(from, to);
    assert_ne!(rewritten, decoded, "the rewrite must change the payload");
    format!("{}.{signature}", URL_SAFE_NO_PAD.encode(rewritten))
}

/// Calque's role needs to own nothing. The tables and the publication belong
/// to another role, as an operator's would, and serve's role has only LOGIN,
/// REPLICATION, CREATE on the database for Calque's own schemas, and row
/// privileges on the tables. Before the publication exists, serve refuses
/// and prints the statement the tables' owner runs; once it does, serve
/// writes a row, and `calque destroy` keeps the publication it did not make.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_role_that_owns_no_table_serves_writes_and_destroys_without_dropping_the_publication() {
    let _exclusive = exclusive();
    let fixture = Fixture::least_privilege("leastpriv");

    let refused = serve_until_exit(&fixture);
    let statement = format!(
        "CREATE PUBLICATION \"{}\" FOR TABLE \"public\".\"{}\", \"public\".\"{}\";",
        fixture.publication(),
        fixture.notes(),
        fixture.tasks(),
    );
    assert!(
        refused.contains(&format!(
            "the PostgreSQL publication {} does not exist, and Calque's role cannot create it \
             because it does not own",
            fixture.publication()
        )) && refused.contains(&statement),
        "{refused}"
    );

    fixture.publish();
    let serving = serve(&fixture);
    let grant = fixture.grant(
        vec![capability(
            &fixture.tasks(),
            &[GrantCommandOperation::Update],
        )],
        1,
        unix_seconds(),
        600,
    );
    let plan = bootstrap(
        serving.port,
        &grant,
        &[
            format!("public.{}", fixture.tasks()),
            format!("public.{}", fixture.notes()),
        ],
    );
    let written = update(
        &plan,
        plan.table(&fixture.tasks()),
        row_id(0),
        "title",
        "written by a role that owns nothing",
    );
    assert_eq!(
        outcome_code(&post_mutation(serving.port, &grant, &plan, written)),
        MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32
    );
    assert_eq!(
        fixture.title_of(0).as_deref(),
        Some("written by a role that owns nothing")
    );
    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");

    let destroyed = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "destroy",
            "--state-dir",
            fixture.directory.path().join("state").to_str().unwrap(),
            "--database-url-env",
            DATABASE_ENV,
            "--yes",
        ])
        .env(DATABASE_ENV, &fixture.serve_url)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&destroyed.stdout);
    assert!(
        destroyed.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&destroyed.stderr)
    );
    assert!(
        stdout.contains(&format!(
            "Kept the publication {}, which another role owns.",
            fixture.publication()
        )),
        "{stdout}"
    );
    let mut owner = Client::connect(&fixture.owner_url, NoTls).unwrap();
    let mut count =
        |sql: &str, name: String| -> i64 { owner.query_one(sql, &[&name]).unwrap().get(0) };
    assert_eq!(
        count(
            "SELECT count(*) FROM pg_catalog.pg_publication WHERE pubname = $1",
            fixture.publication()
        ),
        1
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            fixture.slot()
        ),
        0
    );
    fixture.drop_source();
}

/// Runs `calque serve` for a start it must refuse, and returns its stderr.
fn serve_until_exit(fixture: &Fixture) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "serve",
            "--config",
            fixture.config().to_str().unwrap(),
            "--state-dir",
            fixture.directory.path().join("state").to_str().unwrap(),
            "--grant-audience",
            GRANT_AUDIENCE,
            "--grant-verification-key-env",
            GRANT_KEY_ENV,
            "--allow-origin",
            "http://127.0.0.1:5173",
            "--port",
            &reserved_port().to_string(),
        ])
        .env(DATABASE_ENV, &fixture.serve_url)
        .env(
            GRANT_KEY_ENV,
            fixture.issuer.verification_key_environment().unwrap(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            assert!(!status.success(), "serve started: {stderr}");
            return stderr;
        }
        if started.elapsed() > READY_TIMEOUT {
            let _ = child.kill();
            panic!("serve neither started nor refused within {READY_TIMEOUT:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
