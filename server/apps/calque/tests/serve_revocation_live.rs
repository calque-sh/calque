//! Revoking an authorization generation on a running `calque serve`, against a
//! live PostgreSQL 18, and the same deployment meeting many clients at once.
//!
//! The test plays the application backend: it holds the signing key, signs the
//! fence order, and signs every barrier draft Calque returns. Everything it
//! asserts is observed on the wire or in PostgreSQL, never a count the process
//! printed about itself.
//!
//! These tests are `#[ignore]`d and run through
//! `scripts/live-postgres-suite.sh`; without `TEST_DATABASE_URL` they fail
//! rather than silently pass.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Cursor as IoCursor, Read, Write as _},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use dataset_grant::{GrantCommandCapability, GrantCommandOperation};
use dataset_grant_issuer::GrantIssuer;
use postgres::{Client, NoTls};
use prost::Message as _;
use serde_json::Value;
use sync_protocol::{
    change::{
        self, cell_value::Kind, change_stream_frame, operation::Payload, CellValue,
        ChangeStreamFrame, ColumnValue, Operation, OperationKind, PrimaryKey, RevocationBarrier,
        RowPatch,
    },
    encode_primary_key,
    mutation::{
        AffectedRow, MutationBatchRequest, MutationBatchResponse, MutationEnvelope,
        MutationFootprint, MutationOutcomeCode,
    },
    verify_change_frame_body_sha256, verify_revocation_barrier, BatchFramesDigest, FrameDecoder,
    RevocationBarrierVerificationKey,
};

const GRANT_KEY_ENV: &str = "CALQUE_LIVE_REVOCATION_GRANT_KEY";
const GRANT_AUDIENCE: &str = "revocation.test";
const DATABASE_ENV: &str = "CALQUE_LIVE_REVOCATION_DATABASE_URL";
const READY_TIMEOUT: Duration = Duration::from_secs(300);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// A fenced generation's artifact is cut in the background; this bounds how
/// long a bootstrap of the new generation may keep answering 503.
const PUBLICATION_TIMEOUT: Duration = Duration::from_secs(120);
const ARTIFACT_PROFILE: &str = "chromium-native-artifact-v1";
const MUTATION_CONTENT_TYPE: &str = "application/vnd.calque.mutation.v1+protobuf";
const CHANGE_CONTENT_TYPE: &str = "application/vnd.calque.change.v1+protobuf";
const CROWD: usize = 24;
/// Long enough that a poll answering well inside it was woken, not timed out.
const PARKED_WAIT: Duration = Duration::from_secs(30);
/// Time for a poll sent from another thread to reach its wait.
const SETTLE: Duration = Duration::from_secs(2);
/// How long `calque serve` lets a write wait for a client that takes none of
/// it before it closes the connection.
const WRITE_STALL_BOUND: Duration = Duration::from_secs(30);

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

/// One source-wide collection with an update command, so a retired
/// generation's write can be refused as well as its reads.
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
            issuer: GrantIssuer::generate("live-revocation-key", GRANT_AUDIENCE)
                .unwrap()
                .0,
        }
    }

    fn tasks(&self) -> String {
        format!("tasks_{}", self.suffix)
    }

    fn relation(&self) -> String {
        format!("public.{}", self.tasks())
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

    /// A grant of `generation` that may update the collection.
    fn grant(&self, generation: u64) -> String {
        self.grant_for(generation, 600)
    }

    /// The same grant, expiring `lifetime_seconds` from now.
    fn grant_for(&self, generation: u64, lifetime_seconds: u64) -> String {
        self.issuer
            .issue_with_commands(
                "revocation-member".to_owned(),
                BTreeMap::new(),
                vec![GrantCommandCapability {
                    collection: self.tasks(),
                    operations: vec![GrantCommandOperation::Update],
                }],
                generation,
                unix_seconds(),
                lifetime_seconds,
            )
            .unwrap()
    }

    fn seed(&self) {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE TABLE {tasks} (id uuid NOT NULL PRIMARY KEY, title text NOT NULL);
                 ALTER TABLE {tasks} REPLICA IDENTITY FULL;
                 CREATE PUBLICATION {publication} FOR TABLE {tasks};",
                tasks = self.tasks(),
                publication = self.publication(),
            ))
            .unwrap();
        for row in 0..4 {
            self.insert(row, &format!("task {row}"));
        }
    }

    fn insert(&self, row: u64, title: &str) {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .execute(
                &format!("INSERT INTO {} (id, title) VALUES ($1, $2)", self.tasks()),
                &[&row_id(row), &title],
            )
            .unwrap();
    }

    /// Inserts `titles` in one transaction, from row `first` on.
    fn insert_together(&self, first: u64, titles: &[String]) {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        let mut transaction = client.transaction().unwrap();
        for (row, title) in (first..).zip(titles) {
            transaction
                .execute(
                    &format!("INSERT INTO {} (id, title) VALUES ($1, $2)", self.tasks()),
                    &[&row_id(row), title],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    fn title_of(&self, row: u64) -> Option<String> {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .query_opt(
                &format!("SELECT title FROM {} WHERE id = $1", self.tasks()),
                &[&row_id(row)],
            )
            .unwrap()
            .map(|row| row.get(0))
    }

    fn fenced_generation(&self) -> Option<i64> {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .query_opt(
                "SELECT generation FROM calque_control.authorization_fence WHERE source_id = $1",
                &[&self.source_id()],
            )
            .unwrap()
            .map(|row| row.get(0))
    }

    /// Writes the fence row directly, the way nothing but an operator can.
    fn overwrite_fenced_generation(&self, generation: i64) {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        let updated = client
            .execute(
                "UPDATE calque_control.authorization_fence SET generation = $2
                  WHERE source_id = $1",
                &[&self.source_id(), &generation],
            )
            .unwrap();
        assert_eq!(updated, 1);
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
             DELETE FROM calque_control.authorization_fence WHERE source_id = '{source}';
             DELETE FROM calque_control.revocation_barrier WHERE source_id = '{source}';
             DELETE FROM calque_control.authorization_retirement WHERE source_id = '{source}';
             DELETE FROM calque_control.authorization_scope_issued WHERE source_id = '{source}';
             DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {tasks};",
            slot = self.slot(),
            source = self.source_id(),
            publication = self.publication(),
            tasks = self.tasks(),
        ));
    }

    fn config(&self, data_version: u64) -> PathBuf {
        let document = serde_json::json!({
            "application": { "id": "revocation-ops", "version": "1.0.0" },
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
                "dataVersion": data_version,
            }],
            "collections": [{
                "id": self.tasks(),
                "source": self.source_id(),
                "kind": "table",
                "relation": { "schema": "public", "table": self.tasks() },
                "columns": [
                    { "name": "id", "postgresType": "uuid", "nullable": false },
                    { "name": "title", "postgresType": "text", "nullable": false },
                ],
                "primaryKey": ["id"],
                "scope": "all",
                "adapter": "direct-columnar",
                "encoding": "calque-columnar",
            }],
            "commands": [{
                "id": "edit-task",
                "kind": "crud",
                "collection": self.tasks(),
                "operations": ["update"],
                "writableColumns": ["title"],
            }],
        });
        let path = self
            .directory
            .path()
            .join(format!("calque.config.v{data_version}.json"));
        std::fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        path
    }

    /// The public half of the application key, as a barrier receiver holds it.
    fn barrier_key(&self) -> RevocationBarrierVerificationKey {
        let environment: Value =
            serde_json::from_str(&self.issuer.verification_key_environment().unwrap()).unwrap();
        let public: [u8; 32] = URL_SAFE_NO_PAD
            .decode(environment["publicKey"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        RevocationBarrierVerificationKey::new(self.issuer.key_id(), public)
    }
}

fn row_id(row: u64) -> uuid::Uuid {
    let mut id = [0_u8; 16];
    id[0..8].copy_from_slice(&(row + 1).to_be_bytes());
    uuid::Uuid::from_bytes(id)
}

/// A `calque serve` child, killed when the guard drops.
struct Serving {
    child: Child,
    lines: Vec<String>,
    /// Every line serve writes to stderr. A thread reads the pipe from the
    /// start: once it is full, each of serve's threads blocks on its next log
    /// line, delivery's included.
    stderr: Option<std::sync::mpsc::Receiver<String>>,
    port: u16,
    stopped: bool,
}

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
            if let Some(stderr) = &self.stderr {
                let logged = stderr.try_iter().collect::<Vec<_>>();
                let last = &logged[logged.len().saturating_sub(40)..];
                eprintln!("serve's last stderr lines:\n{}", last.join("\n"));
            }
        }
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// `calque serve` on `port` over the same state directory every time, with
/// the manifest at `data_version`, reading its source from `source_url`.
fn serve_command(fixture: &Fixture, data_version: u64, source_url: &str, port: u16) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_calque"));
    command
        .args([
            "serve",
            "--config",
            fixture.config(data_version).to_str().unwrap(),
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
        .env(DATABASE_ENV, source_url)
        .env(
            GRANT_KEY_ENV,
            fixture.issuer.verification_key_environment().unwrap(),
        );
    command
}

/// `calque serve` over the same state directory every time, with the
/// manifest at `data_version`.
fn serve(fixture: &Fixture, data_version: u64) -> Serving {
    serve_with(fixture, data_version, &[])
}

/// `serve`, with `args` added to the command line.
fn serve_with(fixture: &Fixture, data_version: u64, args: &[&str]) -> Serving {
    let port = reserved_port();
    let started = Instant::now();
    let mut child = serve_command(fixture, data_version, &database_url(), port)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = read_lines(child.stderr.take().unwrap());

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
        let stderr = stderr.iter().collect::<Vec<_>>();
        panic!("serve exited before readiness.\nstdout: {lines:#?}\nstderr: {stderr:#?}");
    }
    Serving {
        child,
        lines,
        stderr: Some(stderr),
        port,
        stopped: false,
    }
}

/// Reads `pipe` to its end on a thread of its own, sending each line.
fn read_lines(pipe: impl Read + Send + 'static) -> std::sync::mpsc::Receiver<String> {
    let (sender, lines) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    lines
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
    HttpResponse {
        status,
        body: raw[split + 4..].to_vec(),
    }
}

fn post_json(port: u16, path: &str, body: &Value) -> HttpResponse {
    http(
        port,
        "POST",
        path,
        &[("content-type", "application/json".to_owned())],
        Some(serde_json::to_vec(body).unwrap().as_slice()),
    )
}

/// What one bootstrap plan gives this test.
struct Plan {
    plan_id: String,
    scope_hash: String,
    scope_token: Vec<u8>,
    activation_cursor: String,
    /// The cursor the plan's artifact was cut at, where its catch-up starts.
    snapshot_cursor: String,
    table_id: String,
    id_column: u32,
    title_column: u32,
    /// Path and capability query of every artifact object the plan names.
    object_paths: Vec<String>,
}

fn object_paths(value: &Value, paths: &mut Vec<String>) {
    match value {
        Value::String(text) => {
            if let Some(start) = text.find("/v1/artifact-objects/") {
                paths.push(text[start..].to_owned());
            }
        }
        Value::Array(items) => items.iter().for_each(|item| object_paths(item, paths)),
        Value::Object(fields) => fields.values().for_each(|item| object_paths(item, paths)),
        _ => {}
    }
}

impl Plan {
    fn parse(value: &Value) -> Self {
        let table = &value["artifacts"][0]["runtime"]["tables"][0];
        let column = |name: &str| {
            table["columns"]
                .as_array()
                .unwrap()
                .iter()
                .find(|column| column["name"] == name)
                .and_then(|column| column["columnId"].as_u64())
                .and_then(|id| u32::try_from(id).ok())
                .unwrap_or_else(|| panic!("the plan names column {name}: {table}"))
        };
        let mut paths = Vec::new();
        object_paths(&value["artifacts"], &mut paths);
        Self {
            plan_id: value["planId"].as_str().unwrap().to_owned(),
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
            snapshot_cursor: value["artifacts"][0]["snapshotCursor"]
                .as_str()
                .unwrap()
                .to_owned(),
            table_id: table["tableId"].as_str().unwrap().to_owned(),
            id_column: column("id"),
            title_column: column("title"),
            object_paths: paths,
        }
    }
}

fn bootstrap_response(port: u16, grant: &str, relation: &str) -> HttpResponse {
    let document = serde_json::json!({
        "protocolVersion": 1,
        "requiredTables": [relation],
        "cachedArtifactHints": [],
        "acceptedEncodings": ["zstd", "identity"],
        "memoryBudgetBytes": 1_073_741_824_u64,
        "clientHeapReserveBytes": 67_108_864_u64,
        "clientInstanceId": "revocation-live",
        "deviceId": "revocation-live-device",
    });
    http(
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
    )
}

fn bootstrap(port: u16, grant: &str, relation: &str) -> Plan {
    let response = bootstrap_response(port, grant, relation);
    assert_eq!(response.status, 200, "{}", response.text());
    Plan::parse(&response.json())
}

fn poll(port: u16, plan: &Plan, from: &str) -> HttpResponse {
    poll_waiting(port, plan, from, Duration::ZERO)
}

/// A poll that, finding nothing past `from`, waits up to `wait` for a change.
fn poll_waiting(port: u16, plan: &Plan, from: &str, wait: Duration) -> HttpResponse {
    http(
        port,
        "POST",
        "/v1/changes",
        &[("content-type", CHANGE_CONTENT_TYPE.to_owned())],
        Some(live_request(plan, from, wait).encode_to_vec().as_slice()),
    )
}

/// The request a live poll from `from` sends, waiting up to `wait`.
fn live_request(plan: &Plan, from: &str, wait: Duration) -> change::ChangeRequest {
    change::ChangeRequest {
        protocol_version: 1,
        from_cursor_exclusive: Some(change::Cursor {
            token: from.as_bytes().to_vec(),
        }),
        through_cursor_inclusive: None,
        tables: vec![change::TableSubscription {
            table_id: plan.table_id.clone(),
            shard_snapshot_cursor: None,
        }],
        authorization_scope_token: plan.scope_token.clone(),
        max_batch_bytes: 4 * 1024 * 1024,
        max_transactions: 512,
        long_poll_timeout_ms: u32::try_from(wait.as_millis()).unwrap(),
        client_instance_id: "revocation-live".to_owned(),
        device_id: "revocation-live-device".to_owned(),
        pending_mutation_ids: Vec::new(),
        accept_transaction_fragmentation: true,
        accept_revocation_barrier: true,
        plan_id: plan.plan_id.clone(),
        stream: false,
    }
}

/// The request for one response that stays open for `lifetime`, telling the
/// server of `pending` mutation ids.
fn stream_request(
    plan: &Plan,
    from: &str,
    lifetime: Duration,
    pending: &[String],
) -> change::ChangeRequest {
    change::ChangeRequest {
        pending_mutation_ids: pending.to_vec(),
        stream: true,
        ..live_request(plan, from, lifetime)
    }
}

/// One change batch as this test reads it.
#[derive(Debug, Default)]
struct Batch {
    /// The body kind of every frame, in delivery order.
    frames: Vec<&'static str>,
    /// The body digest of every frame before the end, in delivery order.
    digests: Vec<[u8; 32]>,
    /// Whether the end counted and digested exactly the frames before it.
    sealed: bool,
    /// Every title an inserted or updated row carried, in delivery order.
    titles: Vec<String>,
    /// Every mutation id the batch acknowledged.
    mutation_ids: Vec<String>,
    barrier: Option<RevocationBarrier>,
    from: String,
    through: String,
    up_to_date: bool,
    reached_fence: bool,
}

fn read_batch(plan: &Plan, response: &HttpResponse) -> Batch {
    assert_eq!(response.status, 200, "{}", response.text());
    let mut decoder =
        FrameDecoder::with_max_payload_bytes(IoCursor::new(&response.body), 4 * 1024 * 1024);
    let mut batch = Batch::default();
    while let Some(frame) = decoder.read_message::<ChangeStreamFrame>().unwrap() {
        absorb_frame(&mut batch, plan.title_column, frame);
    }
    batch
}

/// Adds one verified frame to `batch`.
fn absorb_frame(batch: &mut Batch, title_column: u32, frame: ChangeStreamFrame) {
    verify_change_frame_body_sha256(&frame).expect("every frame carries its frozen digest");
    let body_sha256: [u8; 32] = frame.body_sha256.as_slice().try_into().unwrap();
    match frame.body {
        Some(change_stream_frame::Body::BatchStart(start)) => {
            batch.digests.push(body_sha256);
            batch.frames.push("start");
            batch.from = String::from_utf8(start.from_cursor_exclusive.unwrap().token).unwrap();
        }
        Some(change_stream_frame::Body::TransactionFragment(fragment)) => {
            batch.digests.push(body_sha256);
            batch.frames.push("transaction");
            for operation in fragment.operations {
                let columns = match operation.payload {
                    Some(change::operation::Payload::Insert(full)) => full.columns,
                    Some(change::operation::Payload::Update(patch)) => patch.changed_columns,
                    _ => Vec::new(),
                };
                for column in columns {
                    if column.column_id != title_column {
                        continue;
                    }
                    if let Some(Kind::TextValue(title)) = column.value.and_then(|v| v.kind) {
                        batch.titles.push(title);
                    }
                }
            }
        }
        Some(change_stream_frame::Body::RevocationBarrier(barrier)) => {
            batch.digests.push(body_sha256);
            batch.frames.push("barrier");
            batch.barrier = Some(barrier);
        }
        Some(change_stream_frame::Body::BatchEnd(end)) => {
            batch.frames.push("end");
            let mut digest = BatchFramesDigest::new();
            for body_sha256 in &batch.digests {
                digest.push(body_sha256);
            }
            batch.sealed = end.frame_count == digest.frames()
                && end.frames_sha256 == digest.finish().as_slice();
            batch.mutation_ids.extend(end.acknowledged_mutation_ids);
            batch.through = String::from_utf8(end.through_cursor_inclusive.unwrap().token).unwrap();
            batch.up_to_date = end.currently_up_to_date;
            batch.reached_fence = end.reached_requested_fence;
        }
        Some(change_stream_frame::Body::Heartbeat(_)) => batch.frames.push("heartbeat"),
        Some(change_stream_frame::Body::Error(error)) => {
            panic!("change stream returned {error:?}")
        }
        None => batch.frames.push("empty"),
    }
}

/// What the reader of an open response saw.
#[derive(Debug)]
enum Streamed {
    Batch(Box<Batch>),
    /// The body ended after a complete batch.
    End,
    /// The response was not a 200, or its body broke off.
    Failed(String),
}

/// An open streamed response, read on a thread of its own so the test can
/// wait for its next batch, or its end, with a deadline.
struct OpenStream {
    received: std::sync::mpsc::Receiver<Streamed>,
    /// Signalled once the response head has arrived.
    opened: std::sync::mpsc::Receiver<()>,
    connection: TcpStream,
}

impl OpenStream {
    /// Closes the connection, as a client that leaves does.
    fn leave(&self) {
        let _ = self.connection.shutdown(std::net::Shutdown::Both);
    }

    fn next(&self, within: Duration) -> Option<Streamed> {
        self.received.recv_timeout(within).ok()
    }

    /// Waits for the response head. `calque serve` sends it once the stream
    /// holds its place among the open streams and has given its request
    /// permit back.
    fn opens_within(&self, within: Duration) {
        if self.opened.recv_timeout(within).is_ok() {
            return;
        }
        match self.next(Duration::ZERO) {
            Some(Streamed::Failed(failure)) => panic!("the stream failed: {failure}"),
            other => panic!("expected the stream to open within {within:?}, got {other:?}"),
        }
    }

    fn batch(&self, within: Duration) -> Batch {
        match self.next(within) {
            Some(Streamed::Batch(batch)) => *batch,
            Some(Streamed::Failed(failure)) => panic!("the stream failed: {failure}"),
            other => panic!("expected a batch within {within:?}, got {other:?}"),
        }
    }

    /// Reads batches until one carries `title`, checking that each starts
    /// where the previous one ended, and returns them all.
    fn batches_through_title(&self, from: &str, title: &str) -> Vec<Batch> {
        let deadline = Instant::now() + DELIVERY_TIMEOUT;
        let mut cursor = from.to_owned();
        let mut batches = Vec::new();
        loop {
            let batch = self.batch(deadline.saturating_duration_since(Instant::now()));
            assert_eq!(
                batch.from, cursor,
                "a streamed batch broke the chain: {batch:?}"
            );
            assert!(batch.sealed, "{batch:?}");
            assert_eq!(batch.frames.first(), Some(&"start"), "{batch:?}");
            assert_eq!(batch.frames.last(), Some(&"end"), "{batch:?}");
            cursor.clone_from(&batch.through);
            let found = batch.titles.iter().any(|delivered| delivered == title);
            batches.push(batch);
            if found {
                return batches;
            }
        }
    }

    fn ends_within(&self, within: Duration) {
        match self.next(within) {
            Some(Streamed::End) => {}
            Some(Streamed::Failed(failure)) => panic!("the stream failed: {failure}"),
            other => panic!("expected the body to end within {within:?}, got {other:?}"),
        }
    }
}

/// Sends `request` and reads its streamed body on a thread of its own.
fn open_stream(port: u16, title_column: u32, request: &change::ChangeRequest) -> OpenStream {
    let body = request.encode_to_vec();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    let head = format!(
        "POST /v1/changes HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\
         content-type: {CHANGE_CONTENT_TYPE}\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    let (open, opened) = std::sync::mpsc::channel();
    let connection = stream.try_clone().unwrap();
    std::thread::spawn(move || {
        let outcome = read_streamed(&mut BufReader::new(stream), title_column, &open, &sender);
        if let Err(failure) = outcome {
            let _ = sender.send(Streamed::Failed(failure));
        }
    });
    OpenStream {
        received,
        opened,
        connection,
    }
}

/// Reads a chunked 200 response, signalling `open` once its head has arrived,
/// then sending each batch as its `BatchEnd` arrives and `End` when the body
/// ends.
fn read_streamed(
    reader: &mut BufReader<TcpStream>,
    title_column: u32,
    open: &std::sync::mpsc::Sender<()>,
    sender: &std::sync::mpsc::Sender<Streamed>,
) -> Result<(), String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    if line.split_whitespace().nth(1) != Some("200") {
        return Err(format!("the stream was answered {}", line.trim()));
    }
    let mut chunked = false;
    let mut unbuffered = false;
    loop {
        line.clear();
        reader
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        let header = line.trim().to_ascii_lowercase();
        if header.is_empty() {
            break;
        }
        chunked |= header == "transfer-encoding: chunked";
        unbuffered |= header == "x-accel-buffering: no";
    }
    if !chunked || !unbuffered {
        return Err(format!(
            "a stream must be chunked and unbuffered: chunked={chunked} unbuffered={unbuffered}"
        ));
    }
    let _ = open.send(());
    let mut pending = Vec::new();
    let mut batch = Batch::default();
    loop {
        line.clear();
        reader
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        let size = usize::from_str_radix(line.trim(), 16)
            .map_err(|error| format!("bad chunk size {line:?}: {error}"))?;
        if size == 0 {
            if !pending.is_empty() || !batch.frames.is_empty() {
                return Err("the body ended inside a batch".to_owned());
            }
            let _ = sender.send(Streamed::End);
            return Ok(());
        }
        let mut chunk = vec![0; size + 2];
        reader
            .read_exact(&mut chunk)
            .map_err(|error| error.to_string())?;
        pending.extend_from_slice(&chunk[..size]);
        while pending.len() >= 4 {
            let length =
                usize::try_from(u32::from_be_bytes(pending[..4].try_into().unwrap())).unwrap();
            if pending.len() < 4 + length {
                break;
            }
            let frame = ChangeStreamFrame::decode(&pending[4..4 + length])
                .map_err(|error| error.to_string())?;
            pending.drain(..4 + length);
            let ends = matches!(frame.body, Some(change_stream_frame::Body::BatchEnd(_)));
            absorb_frame(&mut batch, title_column, frame);
            if ends {
                let _ = sender.send(Streamed::Batch(Box::new(std::mem::take(&mut batch))));
            }
        }
    }
}

/// Polls `plan` forward from `from` until a batch delivers `title`, and
/// returns the cursor that batch ended at.
fn wait_for_title(port: u16, plan: &Plan, from: &str, title: &str) -> String {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut cursor = from.to_owned();
    let (mut polls, mut moves, mut longest) = (0_u32, 0_u32, Duration::ZERO);
    loop {
        let started = Instant::now();
        let batch = read_batch(plan, &poll(port, plan, &cursor));
        polls += 1;
        longest = longest.max(started.elapsed());
        assert!(batch.barrier.is_none(), "an unexpected barrier: {batch:?}");
        moves += u32::from(batch.through != cursor);
        cursor = batch.through;
        if batch.titles.iter().any(|delivered| delivered == title) {
            return cursor;
        }
        assert!(
            Instant::now() < deadline,
            "{} was not delivered within {DELIVERY_TIMEOUT:?}: {moves} of {polls} polls moved the \
             cursor, and the longest took {longest:?}",
            shown(title)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `title` for a failure message: a large row's title is cut to its start.
fn shown(title: &str) -> String {
    if title.len() <= 64 {
        return title.to_owned();
    }
    let start = title.chars().take(16).collect::<String>();
    format!("{start}... ({} bytes)", title.len())
}

/// Long-polls `plan` forward from `from` until a batch delivers `title`, and
/// returns that batch. Every answer on the way must be a batch: a refusal
/// fails the test rather than being retried.
fn wait_parked_for_title(port: u16, plan: &Plan, from: &str, title: &str) -> Batch {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut cursor = from.to_owned();
    loop {
        let batch = read_batch(plan, &poll_waiting(port, plan, &cursor, PARKED_WAIT));
        if batch.titles.iter().any(|delivered| delivered == title) {
            return batch;
        }
        cursor = batch.through;
        assert!(
            Instant::now() < deadline,
            "{} was not delivered within {DELIVERY_TIMEOUT:?}",
            shown(title)
        );
    }
}

/// Polls until the endpoint says `plan` is at the durable head.
fn drain(port: u16, plan: &Plan, from: &str) -> String {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut cursor = from.to_owned();
    loop {
        let batch = read_batch(plan, &poll(port, plan, &cursor));
        cursor = batch.through;
        if batch.up_to_date {
            return cursor;
        }
        assert!(Instant::now() < deadline, "the plan never reached the head");
    }
}

/// Every title `plan` is delivered from `from` through the durable head.
fn titles_through_head(port: u16, plan: &Plan, from: &str) -> Vec<String> {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut cursor = from.to_owned();
    let mut titles = Vec::new();
    loop {
        let batch = read_batch(plan, &poll(port, plan, &cursor));
        titles.extend(batch.titles);
        cursor = batch.through;
        if batch.up_to_date {
            return titles;
        }
        assert!(Instant::now() < deadline, "the plan never reached the head");
    }
}

/// The value of the unlabelled sample `name` on the metrics listener.
fn sample(metrics_port: u16, name: &str) -> f64 {
    sampled(metrics_port, name)
        .unwrap_or_else(|scrape| panic!("the scrape has no {name}: {scrape}"))
}

/// A labelled series is left out of the scrape until its first observation.
fn sample_or_zero(metrics_port: u16, name: &str) -> f64 {
    sampled(metrics_port, name).unwrap_or(0.0)
}

fn sampled(metrics_port: u16, name: &str) -> Result<f64, String> {
    let scrape = http(metrics_port, "GET", "/metrics", &[], None);
    assert_eq!(scrape.status, 200, "{}", scrape.text());
    let text = scrape.text();
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
        .ok_or(text)
}

fn wait_for_sample(metrics_port: u16, name: &str, reached: impl Fn(f64) -> bool) {
    let deadline = Instant::now() + PUBLICATION_TIMEOUT;
    while !reached(sample(metrics_port, name)) {
        assert!(
            Instant::now() < deadline,
            "{name} did not reach its value within {PUBLICATION_TIMEOUT:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn fence(port: u16, order: &str) -> HttpResponse {
    post_json(
        port,
        "/v1/authorization/fence",
        &serde_json::json!({ "order": order }),
    )
}

/// The exact `RevocationBarrier` a draft describes, before it is signed.
fn barrier_from_draft(draft: &Value) -> RevocationBarrier {
    RevocationBarrier {
        barrier_cursor: Some(change::Cursor {
            token: draft["barrierCursor"].as_str().unwrap().as_bytes().to_vec(),
        }),
        revoked_authorization_generation: draft["revokedAuthorizationGeneration"].as_u64().unwrap(),
        effective_authorization_generation: draft["effectiveAuthorizationGeneration"]
            .as_u64()
            .unwrap(),
        revoked_authorization_scope_hash: hex::decode(
            draft["revokedAuthorizationScopeHash"].as_str().unwrap(),
        )
        .unwrap(),
        barrier_id: draft["barrierId"].as_str().unwrap().to_owned(),
        issued_at_unix_micros: draft["issuedAtUnixMicros"].as_u64().unwrap(),
        signing_key_id: String::new(),
        signature: Vec::new(),
    }
}

/// Signs every draft with `issuer`, the way an application backend would.
fn signed_barriers(issuer: &GrantIssuer, drafts: &[Value]) -> Value {
    let barriers = drafts
        .iter()
        .map(|draft| {
            let mut barrier = barrier_from_draft(draft);
            issuer.sign_revocation_barrier(&mut barrier).unwrap();
            serde_json::json!({
                "barrierId": barrier.barrier_id,
                "signingKeyId": barrier.signing_key_id,
                "signature": URL_SAFE_NO_PAD.encode(&barrier.signature),
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({ "barriers": barriers })
}

fn attach(port: u16, body: &Value) -> HttpResponse {
    post_json(port, "/v1/authorization/barriers", body)
}

fn rename(plan: &Plan, grant: &str, port: u16, row: u64, title: &str) -> HttpResponse {
    let mutation_id = format!("mutation-{}", uuid::Uuid::new_v4());
    rename_as(plan, grant, port, row, title, &mutation_id)
}

/// `rename` under a mutation id the caller chose.
fn rename_as(
    plan: &Plan,
    grant: &str,
    port: u16,
    row: u64,
    title: &str,
    mutation_id: &str,
) -> HttpResponse {
    http(
        port,
        "POST",
        "/v1/mutations",
        &[
            ("authorization", format!("Bearer {grant}")),
            ("content-type", MUTATION_CONTENT_TYPE.to_owned()),
        ],
        Some(rename_request(plan, row, title, mutation_id).as_slice()),
    )
}

/// The encoded mutation batch that renames `row`.
fn rename_request(plan: &Plan, row: u64, title: &str, mutation_id: &str) -> Vec<u8> {
    let id = row_id(row);
    let key = encode_primary_key(&PrimaryKey {
        codec_version: 1,
        parts: vec![ColumnValue {
            column_id: plan.id_column,
            value: Some(CellValue {
                kind: Some(Kind::Uuid16(id.as_bytes().to_vec())),
            }),
        }],
    })
    .unwrap();
    let operation = Operation {
        table_id: plan.table_id.clone(),
        kind: OperationKind::UpdatePatch as i32,
        primary_key: key.clone(),
        payload: Some(Payload::Update(RowPatch {
            changed_columns: vec![ColumnValue {
                column_id: plan.title_column,
                value: Some(CellValue {
                    kind: Some(Kind::TextValue(title.to_owned())),
                }),
            }],
        })),
    };
    let request = MutationBatchRequest {
        protocol_version: 1,
        client_instance_id: "revocation-live".to_owned(),
        device_id: "revocation-live-device".to_owned(),
        authorization_scope_token: plan.scope_token.clone(),
        mutations: vec![MutationEnvelope {
            mutation_id: mutation_id.to_owned(),
            command_name: format!("crud:{}", plan.table_id),
            command_version: 1,
            canonical_input: operation.encode_to_vec(),
            base_cursor: Some(sync_protocol::mutation::Cursor {
                token: plan.activation_cursor.as_bytes().to_vec(),
            }),
            created_at_unix_micros: unix_micros(),
            expected_versions: Vec::new(),
            declared_footprint: Some(MutationFootprint {
                rows: vec![AffectedRow {
                    table_id: plan.table_id.clone(),
                    primary_key: key,
                }],
                table_ids: vec![plan.table_id.clone()],
            }),
        }],
    };
    request.encode_to_vec()
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

/// Bootstraps a grant of the generation in force until its artifact is
/// published, and says whether it saw the documented 503 on the way.
fn bootstrap_after_publication(port: u16, grant: &str, relation: &str) -> (Plan, bool) {
    let deadline = Instant::now() + PUBLICATION_TIMEOUT;
    let mut preparing = false;
    loop {
        let response = bootstrap_response(port, grant, relation);
        match response.status {
            200 => return (Plan::parse(&response.json()), preparing),
            503 => {
                assert!(
                    response.text().contains("ARTIFACT_PREPARING"),
                    "a 503 that is not the documented one: {}",
                    response.text()
                );
                preparing = true;
            }
            status => panic!("bootstrap answered {status}: {}", response.text()),
        }
        assert!(
            Instant::now() < deadline,
            "the fenced generation's artifact was not published within {PUBLICATION_TIMEOUT:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The whole revocation, end to end on one deployment: the application fences
/// generation 1, signs the one barrier its plans' scope needs, and every
/// surface of generation 1 is refused while generation 2 is served -- and
/// still is after a restart on the manifest that says 1.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_fence_retires_a_generation_in_order_and_survives_a_restart() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("fence");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve(&fixture, 1);
    let port = serving.port;
    assert_eq!(serving.line("authorization_generation"), Some("1"));
    assert_eq!(fixture.fenced_generation(), Some(1));

    // `retired` stops polling before the fence; `witness` keeps up, so the
    // test knows which rows are canonical without asking the process.
    let grant_one = fixture.grant(1);
    let retired = bootstrap(port, &grant_one, &relation);
    let witness = bootstrap(port, &grant_one, &relation);
    assert!(!retired.object_paths.is_empty(), "a plan names its objects");
    assert_eq!(
        http(port, "GET", &retired.object_paths[0], &[], None).status,
        200
    );
    let retired_from = drain(port, &retired, &retired.activation_cursor);
    fixture.insert(10, "before the fence");
    wait_for_title(
        port,
        &witness,
        &witness.activation_cursor,
        "before the fence",
    );

    // An order the application's key did not sign moves nothing, even under
    // the application's key id.
    let impostor = GrantIssuer::generate(fixture.issuer.key_id(), GRANT_AUDIENCE)
        .unwrap()
        .0;
    let forged = fence(
        port,
        &impostor
            .issue_fence_order(1, 2, unix_seconds(), 60)
            .unwrap(),
    );
    assert_eq!(forged.status, 401, "{}", forged.text());
    assert_eq!(fixture.fenced_generation(), Some(1));
    assert_eq!(
        http(port, "GET", &retired.object_paths[0], &[], None).status,
        200
    );

    // The fence: durable before it answers, and one draft for the scope both
    // live plans hold.
    let order = fixture
        .issuer
        .issue_fence_order(1, 2, unix_seconds(), 60)
        .unwrap();
    let fenced = fence(port, &order);
    assert_eq!(fenced.status, 200, "{}", fenced.text());
    assert_eq!(fixture.fenced_generation(), Some(2));
    let fenced = fenced.json();
    assert_eq!(fenced["revokedGeneration"], 1);
    assert_eq!(fenced["effectiveGeneration"], 2);
    let drafts = fenced["drafts"].as_array().unwrap().clone();
    assert_eq!(retired.scope_hash, witness.scope_hash);
    assert_eq!(drafts.len(), 1, "{drafts:?}");
    assert_eq!(
        format!(
            "sha256:{}",
            drafts[0]["revokedAuthorizationScopeHash"].as_str().unwrap()
        ),
        retired.scope_hash
    );
    let barrier_cursor = drafts[0]["barrierCursor"].as_str().unwrap().to_owned();

    // A row committed after the fence. Generation 1 must never see it.
    fixture.insert(11, "after the fence");

    // Every surface of generation 1 is refused before a barrier is signed.
    assert_eq!(bootstrap_response(port, &grant_one, &relation).status, 401);
    let stale_write = rename(&retired, &grant_one, port, 0, "written by generation 1");
    assert_eq!(stale_write.status, 401, "{}", stale_write.text());
    assert_eq!(fixture.title_of(0).as_deref(), Some("task 0"));
    assert_eq!(
        http(port, "GET", &retired.object_paths[0], &[], None).status,
        404
    );
    // Unsigned, the retired plan is capped at the barrier cursor and told
    // nothing about the barrier yet.
    let unsigned = read_batch(&retired, &poll(port, &retired, &retired_from));
    assert!(unsigned.barrier.is_none(), "{unsigned:?}");
    assert_eq!(unsigned.through, barrier_cursor);
    assert!(!unsigned
        .titles
        .iter()
        .any(|title| title == "after the fence"));

    // Generation 2 is served once its artifact is published, whether or not
    // the application has signed the barriers yet. Its snapshot may already
    // hold the row committed after the fence, so a row committed after
    // publication is what gives the test a canonical cursor past the barrier.
    let grant_two = fixture.grant(2);
    let (current, saw_preparing) = bootstrap_after_publication(port, &grant_two, &relation);
    println!("generation 2 bootstrap answered ARTIFACT_PREPARING first: {saw_preparing}");
    assert_ne!(current.scope_hash, retired.scope_hash);
    fixture.insert(12, "after publication");
    let past_barrier = wait_for_title(
        port,
        &current,
        &current.activation_cursor,
        "after publication",
    );
    // A retired plan that claims a position past the fence is refused before
    // any barrier exists, so the refusal is the fence's own and not the
    // change stream's placement of a barrier.
    let beyond_unsigned = poll(port, &retired, &past_barrier);
    assert_eq!(beyond_unsigned.status, 401, "{}", beyond_unsigned.text());

    // Signatures that are not the application's, or over a draft Calque did
    // not issue, attach nothing.
    let foreign = attach(port, &signed_barriers(&impostor, &drafts));
    assert_eq!(foreign.status, 401, "{}", foreign.text());
    let mut altered = drafts.clone();
    altered[0]["barrierId"] = Value::from("barrier-chosen-by-the-caller");
    let misaddressed = attach(port, &signed_barriers(&fixture.issuer, &altered));
    assert_eq!(misaddressed.status, 401, "{}", misaddressed.text());
    // A valid signature over the real draft, sent under another barrier id:
    // the application attaches the barrier it names, or nothing.
    let mut misnamed = signed_barriers(&fixture.issuer, &drafts);
    misnamed["barriers"][0]["barrierId"] = Value::from("barrier-chosen-by-the-caller");
    let misnamed = attach(port, &misnamed);
    assert_eq!(misnamed.status, 401, "{}", misnamed.text());
    assert_eq!(misnamed.json()["code"], "UNAUTHORIZED");
    // Still capped: neither row committed after the fence reaches it.
    let still_unsigned = read_batch(&retired, &poll(port, &retired, &retired_from));
    assert!(still_unsigned.barrier.is_none(), "{still_unsigned:?}");
    assert_eq!(still_unsigned.through, barrier_cursor);
    assert!(
        still_unsigned
            .titles
            .iter()
            .all(|title| title != "after the fence" && title != "after publication"),
        "{still_unsigned:?}"
    );

    let attached = attach(port, &signed_barriers(&fixture.issuer, &drafts));
    assert_eq!(attached.status, 200, "{}", attached.text());
    assert_eq!(attached.json()["attached"], 1);

    // In order: what was canonical before the fence, then the barrier, then
    // the end of the batch at the barrier cursor.
    let delivered = read_batch(&retired, &poll(port, &retired, &retired_from));
    assert_eq!(
        delivered.titles,
        vec!["before the fence".to_owned()],
        "{delivered:?}"
    );
    assert_eq!(
        delivered.frames.iter().rev().take(2).collect::<Vec<_>>(),
        vec![&"end", &"barrier"],
        "{delivered:?}"
    );
    assert_eq!(delivered.through, barrier_cursor);
    let barrier = delivered.barrier.expect("the signed barrier is delivered");
    verify_revocation_barrier(&barrier, &fixture.barrier_key())
        .expect("the delivered barrier verifies under the application's key");
    assert_eq!(barrier.revoked_authorization_generation, 1);
    assert_eq!(barrier.effective_authorization_generation, 2);
    assert_eq!(
        format!(
            "sha256:{}",
            hex::encode(&barrier.revoked_authorization_scope_hash)
        ),
        retired.scope_hash
    );
    // A client already standing on the barrier cursor gets the barrier alone.
    let standing = read_batch(&retired, &poll(port, &retired, &barrier_cursor));
    assert!(standing.titles.is_empty(), "{standing:?}");
    assert!(standing.barrier.is_some(), "{standing:?}");
    assert_eq!(standing.through, barrier_cursor);
    // A poll that would wait there gets it at once: the barrier moves no
    // cursor, and it is still the answer.
    let asked = Instant::now();
    let waiting = read_batch(
        &retired,
        &poll_waiting(port, &retired, &barrier_cursor, Duration::from_secs(20)),
    );
    assert!(
        asked.elapsed() < Duration::from_secs(10),
        "a poll standing on a signed barrier waited {:?}",
        asked.elapsed()
    );
    assert!(waiting.barrier.is_some(), "{waiting:?}");
    assert_eq!(waiting.through, barrier_cursor);
    // And one that claims a position past it is refused.
    let beyond = poll(port, &retired, &past_barrier);
    assert_eq!(beyond.status, 401, "{}", beyond.text());

    // A generation 2 grant does not revive a generation 1 plan's scope token;
    // on its own plan, the same write is accepted.
    let revived = rename(
        &retired,
        &grant_two,
        port,
        0,
        "written through a retired plan",
    );
    assert_eq!(revived.status, 401, "{}", revived.text());
    assert_eq!(fixture.title_of(0).as_deref(), Some("task 0"));
    assert!(accepted(&rename(
        &current,
        &grant_two,
        port,
        0,
        "generation 2 wrote this"
    )));
    assert_eq!(
        fixture.title_of(0).as_deref(),
        Some("generation 2 wrote this")
    );
    assert!(serving.shutdown(), "serve exits cleanly on SIGTERM");

    // A restart on the manifest that still says 1 serves 2: the fence and
    // its signed barrier are PostgreSQL rows, not process memory. Both plans
    // outlive the process that granted them: the retired one is still capped
    // and still given its barrier, and the current one keeps streaming.
    let restarted = serve(&fixture, 1);
    let port = restarted.port;
    assert_eq!(restarted.line("authorization_generation"), Some("2"));
    assert_eq!(restarted.line("artifact"), Some("published-existing"));
    assert_eq!(bootstrap_response(port, &grant_one, &relation).status, 401);
    let after_restart = read_batch(&retired, &poll(port, &retired, &retired_from));
    assert_eq!(
        after_restart.titles,
        vec!["before the fence".to_owned()],
        "{after_restart:?}"
    );
    assert!(after_restart.barrier.is_some(), "{after_restart:?}");
    assert_eq!(after_restart.through, barrier_cursor);
    let beyond = poll(port, &retired, &past_barrier);
    assert_eq!(beyond.status, 401, "{}", beyond.text());
    fixture.insert(13, "after the restart");
    wait_for_title(port, &current, &past_barrier, "after the restart");
    bootstrap(port, &grant_two, &relation);
    // The same order again is inert and answers the drafts the fence stored,
    // which the application can still sign while the retired plans are
    // unexpired. An order for a generation already retired is a conflict.
    let replayed = fence(port, &order);
    assert_eq!(replayed.status, 200, "{}", replayed.text());
    assert_eq!(replayed.json()["drafts"], Value::from(drafts.clone()));
    assert_eq!(fixture.fenced_generation(), Some(2));
    let signed_again = attach(port, &signed_barriers(&fixture.issuer, &drafts));
    assert_eq!(signed_again.status, 200, "{}", signed_again.text());
    assert_eq!(signed_again.json()["attached"], 1);
    assert_eq!(signed_again.json()["expired"], 0);
    let superseded = fence(
        port,
        &fixture
            .issuer
            .issue_fence_order(0, 1, unix_seconds(), 60)
            .unwrap(),
    );
    assert_eq!(superseded.status, 409, "{}", superseded.text());
    // A captured older order is refused before it reaches the fence row, so
    // replaying one cannot fail the deployment closed.
    assert_eq!(http(port, "GET", "/readyz", &[], None).status, 204);
    bootstrap(port, &grant_two, &relation);
    assert!(restarted.shutdown());

    // Raising the manifest's generation is a fence too, on the same state
    // directory.
    let raised = serve(&fixture, 3);
    assert_eq!(raised.line("authorization_generation"), Some("3"));
    assert_eq!(fixture.fenced_generation(), Some(3));
    assert_eq!(
        bootstrap_response(raised.port, &grant_two, &relation).status,
        401
    );
    let raised_plan = bootstrap(raised.port, &fixture.grant(3), &relation);
    // No order drew this fence, so it drafted no barrier, and a plan of
    // generation 2 is refused as expired rather than capped.
    let unbarriered = poll(raised.port, &current, &past_barrier);
    assert_eq!(unbarriered.status, 401, "{}", unbarriered.text());

    // A fence row raised past an order by anything but this process fails the
    // process closed: the order is refused, the plans it served lose their
    // objects, nothing more is bootstrapped, readiness drops, and a restart
    // comes up at the row's generation.
    fixture.overwrite_fenced_generation(5);
    let overtaken = fence(
        raised.port,
        &fixture
            .issuer
            .issue_fence_order(3, 4, unix_seconds(), 60)
            .unwrap(),
    );
    assert_eq!(overtaken.status, 409, "{}", overtaken.text());
    assert_eq!(fixture.fenced_generation(), Some(5));
    assert_eq!(
        http(raised.port, "GET", &raised_plan.object_paths[0], &[], None).status,
        404
    );
    let stale = bootstrap_response(raised.port, &fixture.grant(3), &relation);
    assert_eq!(stale.status, 503, "{}", stale.text());
    assert_eq!(
        bootstrap_response(raised.port, &fixture.grant(4), &relation).status,
        401
    );
    assert_eq!(http(raised.port, "GET", "/readyz", &[], None).status, 503);
    raised.shutdown();
    let overtaken = serve(&fixture, 3);
    assert_eq!(overtaken.line("authorization_generation"), Some("5"));
    bootstrap(overtaken.port, &fixture.grant(5), &relation);
    overtaken.shutdown();

    fixture.drop_source();
}

/// A state directory enrolled on one database is refused against another
/// before serve writes anything there. The fence is the first thing startup
/// writes, so neither its schema nor its table may appear on the database the
/// variable names.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn serve_against_another_database_refuses_before_writing_a_fence_there() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("other_database");
    fixture.seed();
    let enrolled = serve(&fixture, 1);
    assert_eq!(fixture.fenced_generation(), Some(1));
    enrolled.shutdown();

    let source_url = database_url();
    let mut admin = Client::connect(&source_url, NoTls).unwrap();
    let identity = admin
        .query_one(
            "SELECT current_database(), control.system_identifier::text,
                    checkpoint.timeline_id::bigint, checkpoint.prev_timeline_id::bigint
               FROM pg_catalog.pg_control_system() AS control
              CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint",
            &[],
        )
        .unwrap();
    let source_database: String = identity.get(0);
    let describe = |database: &str| {
        format!(
            "database {database} on PostgreSQL system {} timeline {} (previous {})",
            identity.get::<_, String>(1),
            identity.get::<_, i64>(2),
            identity.get::<_, i64>(3),
        )
    };
    let other_database = format!("calque_other_{}", uuid::Uuid::new_v4().simple());
    admin
        .batch_execute(&format!("CREATE DATABASE {other_database}"))
        .unwrap();
    let (base, _) = source_url
        .rsplit_once('/')
        .expect("database URL has a path");
    let other_url = format!("{base}/{other_database}");

    let output = serve_command(&fixture, 1, &other_url, reserved_port())
        .output()
        .unwrap();
    let written = Client::connect(&other_url, NoTls)
        .unwrap()
        .query_one(
            "SELECT to_regnamespace('calque_control')::text,
                    to_regclass('calque_control.authorization_fence')::text",
            &[],
        )
        .unwrap();
    let written: (Option<String>, Option<String>) = (written.get(0), written.get(1));
    admin
        .batch_execute(&format!("DROP DATABASE {other_database} WITH (FORCE)"))
        .unwrap();
    fixture.drop_source();

    assert_eq!(
        written,
        (None, None),
        "serve wrote calque_control to a database it had not verified"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    let state_dir = fixture
        .directory
        .path()
        .join("state")
        .canonicalize()
        .unwrap();
    let state_dir = state_dir.display();
    assert_eq!(
        stderr
            .lines()
            .filter(|line| line.starts_with("calque: "))
            .collect::<Vec<_>>(),
        [format!(
            "calque: connected {}, but {state_dir} belongs to {}. Calque cannot reuse state from another PostgreSQL source. \
             Point {DATABASE_ENV} at the persisted source and rerun serve, or run calque destroy --state-dir {state_dir} --yes --local-state-only \
             and then rerun serve; PostgreSQL objects on the original source will remain.",
            describe(&other_database),
            describe(&source_database),
        )]
    );
}

/// A publication altered while serve runs stops CDC at the next reconnect,
/// as a start that meets it refuses it. The stop's log line names the
/// publication, and serve exits with the refusal and the remedy that start
/// would print: restore the definition, then rerun serve.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_publication_altered_while_serving_stops_cdc_and_exits_naming_its_remedy() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("drift");
    fixture.seed();
    let mut served = serve(&fixture, 1);
    let stderr_lines = served.stderr.take().unwrap();

    let mut admin = Client::connect(&database_url(), NoTls).unwrap();
    admin
        .batch_execute(&format!(
            "ALTER PUBLICATION {} SET TABLE {} WHERE (title <> '')",
            fixture.publication(),
            fixture.relation(),
        ))
        .unwrap();
    let terminated: Vec<bool> = admin
        .query(
            "SELECT pg_terminate_backend(active_pid, 5000) FROM pg_replication_slots
              WHERE slot_name = $1 AND active_pid IS NOT NULL",
            &[&fixture.slot()],
        )
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(terminated, [true], "the slot had no walsender to terminate");

    let drift = format!(
        "the PostgreSQL publication {} no longer publishes exactly the enrolled tables, \
         for insert, update, delete and truncate, without publish_via_partition_root, and without \
         row filters or column lists.",
        fixture.publication(),
    );
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut logged = Vec::new();
    let stop = loop {
        let Ok(line) =
            stderr_lines.recv_timeout(deadline.saturating_duration_since(Instant::now()))
        else {
            panic!("CDC did not stop within {DELIVERY_TIMEOUT:?}: {logged:#?}");
        };
        if line.contains("CDC runtime stopped without reconnecting") {
            break line;
        }
        logged.push(line);
    };
    assert!(
        stop.contains(&format!("persistent CDC failed: {drift}")),
        "{stop}"
    );

    let exited_cleanly = served.shutdown();
    let exit: Vec<String> = stderr_lines.iter().collect();
    fixture.drop_source();

    assert!(!exited_cleanly, "{exit:#?}");
    assert_eq!(
        exit.iter()
            .map(String::as_str)
            .filter(|line| line.starts_with("calque: "))
            .collect::<Vec<_>>(),
        [format!(
            "calque: {drift} Restore that definition, then rerun serve."
        )]
    );
}

/// Clients that arrive together, the way every client of a restarted process
/// does: each bootstrap is planned rather than refused, each live poll parks,
/// and one commit wakes them all. A newer poll of a plan wakes the older one,
/// which answers at once, and is woken in turn by the next.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn clients_arriving_together_are_all_planned_parked_and_woken() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("crowd");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve(&fixture, 1);
    let port = serving.port;
    let grant = fixture.grant(1);

    let start = std::sync::Barrier::new(CROWD);
    let plans = std::thread::scope(|scope| {
        let bootstraps = (0..CROWD)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    bootstrap(port, &grant, &relation)
                })
            })
            .collect::<Vec<_>>();
        bootstraps
            .into_iter()
            .map(|bootstrap| bootstrap.join().unwrap())
            .collect::<Vec<_>>()
    });

    let heads = plans
        .iter()
        .map(|plan| drain(port, plan, &plan.activation_cursor))
        .collect::<Vec<_>>();
    let woken = std::thread::scope(|scope| {
        let polls = plans
            .iter()
            .zip(&heads)
            .map(|(plan, head)| {
                scope.spawn(move || wait_parked_for_title(port, plan, head, "seen by every client"))
            })
            .collect::<Vec<_>>();
        std::thread::sleep(SETTLE);
        fixture.insert(20, "seen by every client");
        polls
            .into_iter()
            .map(|poll| poll.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(woken.len(), CROWD);

    let plan = &plans[0];
    let head = woken[0].through.clone();
    let timed_poll = || {
        let started = Instant::now();
        let response = poll_waiting(port, plan, &head, PARKED_WAIT);
        (read_batch(plan, &response), started.elapsed())
    };
    std::thread::scope(|scope| {
        let first = scope.spawn(timed_poll);
        std::thread::sleep(SETTLE);
        let second = scope.spawn(timed_poll);
        let (first, first_waited) = first.join().unwrap();
        assert!(first.titles.is_empty(), "{first:?}");
        assert!(
            first_waited < PARKED_WAIT / 2,
            "the older poll waited {first_waited:?} after a newer one parked"
        );
        std::thread::sleep(SETTLE);
        let third =
            scope.spawn(|| wait_parked_for_title(port, plan, &head, "seen by the newest poll"));
        let (second, second_waited) = second.join().unwrap();
        assert!(second.titles.is_empty(), "{second:?}");
        assert!(
            second_waited < PARKED_WAIT / 2,
            "the second poll waited {second_waited:?} after a third one parked"
        );
        std::thread::sleep(SETTLE);
        fixture.insert(21, "seen by the newest poll");
        third.join().unwrap();
    });

    serving.shutdown();
    fixture.drop_source();
}

/// One response carries a batch per change, each starting where the last
/// ended. A receipt that becomes canonical while the stream waits is
/// acknowledged in its next batch, a newer request of the plan ends the older
/// stream, and a stream lives no longer than its wait.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_stream_carries_each_change_until_a_newer_request_or_its_wait_ends_it() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("stream");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve(&fixture, 1);
    let port = serving.port;
    let grant = fixture.grant(1);
    let plan = bootstrap(port, &grant, &relation);
    let head = drain(port, &plan, &plan.activation_cursor);

    let older = open_stream(
        port,
        plan.title_column,
        &stream_request(&plan, &head, PARKED_WAIT, &[]),
    );
    std::thread::sleep(SETTLE);
    let mut cursor = head;
    let mut titles = Vec::new();
    for row in 0..3 {
        let title = format!("streamed {row}");
        fixture.insert(30 + row, &title);
        for batch in older.batches_through_title(&cursor, &title) {
            titles.extend(batch.titles);
            cursor = batch.through;
        }
    }
    assert_eq!(titles, ["streamed 0", "streamed 1", "streamed 2"]);

    let mutation_id = format!("mutation-{}", uuid::Uuid::new_v4());
    let newer = open_stream(
        port,
        plan.title_column,
        &stream_request(
            &plan,
            &cursor,
            PARKED_WAIT,
            std::slice::from_ref(&mutation_id),
        ),
    );
    older.ends_within(PARKED_WAIT / 2);
    assert!(accepted(&rename_as(
        &plan,
        &grant,
        port,
        1,
        "renamed while streaming",
        &mutation_id
    )));
    let acknowledged = newer.batches_through_title(&cursor, "renamed while streaming");
    let last = acknowledged.last().unwrap();
    assert_eq!(last.mutation_ids, [mutation_id], "{last:?}");
    cursor.clone_from(&last.through);

    let lifetime = Duration::from_secs(4);
    let opened = Instant::now();
    let brief = open_stream(
        port,
        plan.title_column,
        &stream_request(&plan, &cursor, lifetime, &[]),
    );
    newer.ends_within(PARKED_WAIT / 2);
    fixture.insert(40, "inside a brief stream");
    let delivered = brief.batches_through_title(&cursor, "inside a brief stream");
    cursor.clone_from(&delivered.last().unwrap().through);
    brief.ends_within(lifetime + SETTLE);
    assert!(
        opened.elapsed() >= lifetime - Duration::from_millis(500),
        "the stream ended {:?} into a {lifetime:?} wait",
        opened.elapsed()
    );

    // A stream whose wait passes with no change still carries one batch,
    // since a client reads a body without one as broken.
    let quiet = Duration::from_secs(2);
    let idle = open_stream(
        port,
        plan.title_column,
        &stream_request(&plan, &cursor, quiet, &[]),
    );
    let batch = idle.batch(quiet + SETTLE);
    assert!(batch.sealed, "{batch:?}");
    assert_eq!(
        (&batch.from, &batch.through),
        (&cursor, &cursor),
        "{batch:?}"
    );
    assert!(batch.titles.is_empty(), "{batch:?}");
    idle.ends_within(SETTLE);

    serving.shutdown();
    fixture.drop_source();
}

/// A fenced catch-up asked for as a stream is answered with its batches back
/// to back, each starting where the last ended, and the response ends after
/// the batch that reaches the fence, long before its wait would end it, also
/// when that batch is the first.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_fenced_stream_carries_its_catch_up_and_ends_at_the_fence() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("catchup");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve(&fixture, 1);
    let port = serving.port;
    let grant = fixture.grant(1);
    let first = bootstrap(port, &grant, &relation);
    let titles = (0..3)
        .map(|row| format!("caught up {row}"))
        .collect::<Vec<_>>();
    for (row, title) in (50..).zip(&titles) {
        fixture.insert(row, title);
    }
    wait_for_title(port, &first, &first.activation_cursor, "caught up 2");
    // The artifact is not cut again, so this plan's catch-up runs from the
    // first plan's snapshot through the three inserts. A write after the
    // plan moves the head past its fence.
    let plan = bootstrap(port, &grant, &relation);
    assert_eq!(plan.snapshot_cursor, first.snapshot_cursor);
    assert_ne!(plan.activation_cursor, plan.snapshot_cursor);
    fixture.insert(60, "after the fence");
    wait_for_title(port, &plan, &plan.activation_cursor, "after the fence");

    let mut request = stream_request(&plan, &plan.snapshot_cursor, PARKED_WAIT, &[]);
    request.through_cursor_inclusive = Some(change::Cursor {
        token: plan.activation_cursor.as_bytes().to_vec(),
    });
    request.tables[0].shard_snapshot_cursor = Some(change::Cursor {
        token: plan.snapshot_cursor.as_bytes().to_vec(),
    });
    request.max_transactions = 1;
    let opened = Instant::now();
    let stream = open_stream(port, plan.title_column, &request);
    let mut batches = stream.batches_through_title(&plan.snapshot_cursor, "caught up 2");
    loop {
        match stream.next(PARKED_WAIT / 2) {
            Some(Streamed::Batch(batch)) => {
                assert_eq!(batch.from, batches.last().unwrap().through, "{batch:?}");
                batches.push(*batch);
            }
            Some(Streamed::End) => break,
            other => panic!("expected a batch or the end of the body, got {other:?}"),
        }
    }
    assert!(
        opened.elapsed() < PARKED_WAIT / 2,
        "the fenced stream ended {:?} into a {PARKED_WAIT:?} wait",
        opened.elapsed()
    );
    assert!(batches.len() >= titles.len(), "{batches:?}");
    let last = batches.last().unwrap();
    assert!(last.reached_fence, "{last:?}");
    assert_eq!(last.through, plan.activation_cursor);
    assert!(batches[..batches.len() - 1]
        .iter()
        .all(|batch| !batch.reached_fence));
    let delivered = batches
        .iter()
        .flat_map(|batch| batch.titles.clone())
        .collect::<Vec<_>>();
    assert_eq!(delivered, titles);

    // A catch-up that fits one batch is a body of that one batch.
    request.max_transactions = 512;
    let opened = Instant::now();
    let stream = open_stream(port, plan.title_column, &request);
    let only = stream.batch(PARKED_WAIT / 2);
    assert!(only.reached_fence, "{only:?}");
    assert_eq!(only.titles, titles);
    stream.ends_within(PARKED_WAIT / 2);
    assert!(
        opened.elapsed() < PARKED_WAIT / 2,
        "the one-batch fenced stream ended {:?} into a {PARKED_WAIT:?} wait",
        opened.elapsed()
    );

    serving.shutdown();
    fixture.drop_source();
}

/// A stream of a fenced generation is written nothing past the fence. Once
/// the barrier is signed it ends on the barrier; a stream that did not
/// negotiate the barrier ends on the refusal of its next build instead, and
/// its next request meets that refusal.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_fence_ends_a_stream_on_its_barrier_or_on_its_refused_build() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("streamfence");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve(&fixture, 1);
    let port = serving.port;
    let grant = fixture.grant(1);
    let ordered = bootstrap(port, &grant, &relation);
    let blind = bootstrap(port, &grant, &relation);
    let ordered_head = drain(port, &ordered, &ordered.activation_cursor);
    let blind_head = drain(port, &blind, &blind.activation_cursor);

    let ordered_stream = open_stream(
        port,
        ordered.title_column,
        &stream_request(&ordered, &ordered_head, PARKED_WAIT, &[]),
    );
    let blind_stream = open_stream(
        port,
        blind.title_column,
        &change::ChangeRequest {
            accept_revocation_barrier: false,
            ..stream_request(&blind, &blind_head, PARKED_WAIT, &[])
        },
    );
    std::thread::sleep(SETTLE);
    fixture.insert(10, "before the fence");
    ordered_stream.batches_through_title(&ordered_head, "before the fence");
    let blind_through = blind_stream
        .batches_through_title(&blind_head, "before the fence")
        .pop()
        .unwrap()
        .through;

    let fenced = fence(
        port,
        &fixture
            .issuer
            .issue_fence_order(1, 2, unix_seconds(), 60)
            .unwrap(),
    );
    assert_eq!(fenced.status, 200, "{}", fenced.text());
    let drafts = fenced.json()["drafts"].as_array().unwrap().clone();
    let barrier_cursor = drafts[0]["barrierCursor"].as_str().unwrap().to_owned();
    fixture.insert(11, "after the fence");
    // Anything either stream is written before the barrier is signed stops
    // at the barrier cursor and carries nothing committed after the fence.
    for stream in [&ordered_stream, &blind_stream] {
        while let Some(streamed) = stream.next(SETTLE) {
            let Streamed::Batch(batch) = streamed else {
                panic!("a stream ended before its barrier was signed: {streamed:?}");
            };
            assert_eq!(batch.through, barrier_cursor, "{batch:?}");
            assert!(batch.barrier.is_none(), "{batch:?}");
            assert!(batch.titles.is_empty(), "{batch:?}");
        }
    }

    let attached = attach(port, &signed_barriers(&fixture.issuer, &drafts));
    assert_eq!(attached.status, 200, "{}", attached.text());
    let delivered = ordered_stream.batch(PARKED_WAIT / 2);
    assert_eq!(
        delivered.frames.iter().rev().take(2).collect::<Vec<_>>(),
        vec![&"end", &"barrier"],
        "{delivered:?}"
    );
    assert!(delivered.titles.is_empty(), "{delivered:?}");
    assert_eq!(delivered.through, barrier_cursor);
    let barrier = delivered.barrier.expect("the signed barrier is delivered");
    verify_revocation_barrier(&barrier, &fixture.barrier_key())
        .expect("the delivered barrier verifies under the application's key");
    ordered_stream.ends_within(PARKED_WAIT / 2);
    blind_stream.ends_within(PARKED_WAIT / 2);
    // A stream opened on the barrier's cursor after the barrier is signed
    // is answered with the barrier at once and ends after it.
    let reopened = open_stream(
        port,
        ordered.title_column,
        &stream_request(&ordered, &barrier_cursor, PARKED_WAIT, &[]),
    );
    let repeated = reopened.batch(PARKED_WAIT / 2);
    assert_eq!(repeated.through, barrier_cursor, "{repeated:?}");
    assert!(repeated.barrier.is_some(), "{repeated:?}");
    reopened.ends_within(SETTLE);
    let refused = http(
        port,
        "POST",
        "/v1/changes",
        &[("content-type", CHANGE_CONTENT_TYPE.to_owned())],
        Some(
            change::ChangeRequest {
                accept_revocation_barrier: false,
                ..live_request(&blind, &blind_through, Duration::ZERO)
            }
            .encode_to_vec()
            .as_slice(),
        ),
    );
    assert_eq!(refused.status, 401, "{}", refused.text());

    serving.shutdown();
    fixture.drop_source();
}

/// A stream lives no longer than the grant its plan was issued under.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_stream_ends_when_its_grant_expires() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("streamexpiry");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve(&fixture, 1);
    let port = serving.port;
    let lifetime = Duration::from_secs(8);
    let issued = Instant::now();
    let grant = fixture.grant_for(1, lifetime.as_secs());
    let plan = bootstrap(port, &grant, &relation);
    let head = drain(port, &plan, &plan.activation_cursor);

    let stream = open_stream(
        port,
        plan.title_column,
        &stream_request(&plan, &head, PARKED_WAIT, &[]),
    );
    fixture.insert(10, "before expiry");
    let through = stream
        .batches_through_title(&head, "before expiry")
        .pop()
        .unwrap()
        .through;
    stream.ends_within(PARKED_WAIT / 2);
    // The grant's expiry is on a whole-second clock, so the stream may end up
    // to a second before `lifetime` has passed here, and never long after it.
    let ended = issued.elapsed();
    assert!(
        ended < lifetime + SETTLE,
        "the stream outlived its {lifetime:?} grant by {:?}",
        ended.saturating_sub(lifetime)
    );
    assert_eq!(poll(port, &plan, &through).status, 401);

    serving.shutdown();
    fixture.drop_source();
}

/// Open streams are bounded on their own: past the bound a stream is refused
/// as retryable before anything is built, and one that ends or whose client
/// leaves frees its place. An open stream holds no request permit, so with one
/// permit and two streams open, a bootstrap and a poll are still served.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn open_streams_have_their_own_bound_and_hold_no_request_permit() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("openstreams");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve_with(
        &fixture,
        1,
        &["--max-inflight-requests", "1", "--max-open-streams", "2"],
    );
    let port = serving.port;
    let grant = fixture.grant(1);
    let plans = (0..4)
        .map(|_| bootstrap(port, &grant, &relation))
        .collect::<Vec<_>>();
    let heads = plans
        .iter()
        .map(|plan| drain(port, plan, &plan.activation_cursor))
        .collect::<Vec<_>>();

    // Long enough to outlast the second stream's build and the third
    // request's admission on a loaded host.
    let brief = Duration::from_secs(10);
    let first = open_stream(
        port,
        plans[0].title_column,
        &stream_request(&plans[0], &heads[0], brief, &[]),
    );
    // Until it opens, the first stream's build holds the only request permit,
    // and the second would be refused as an overloaded request.
    first.opens_within(DELIVERY_TIMEOUT);
    let second = open_stream(
        port,
        plans[1].title_column,
        &stream_request(&plans[1], &heads[1], PARKED_WAIT, &[]),
    );
    second.opens_within(DELIVERY_TIMEOUT);

    let refused = http(
        port,
        "POST",
        "/v1/changes",
        &[("content-type", CHANGE_CONTENT_TYPE.to_owned())],
        Some(
            stream_request(&plans[2], &heads[2], PARKED_WAIT, &[])
                .encode_to_vec()
                .as_slice(),
        ),
    );
    assert_eq!(refused.status, 503, "{}", refused.text());
    let mut decoder = FrameDecoder::with_max_payload_bytes(IoCursor::new(&refused.body), 1 << 20);
    let frame = decoder
        .read_message::<ChangeStreamFrame>()
        .unwrap()
        .unwrap();
    let Some(change_stream_frame::Body::Error(error)) = frame.body else {
        panic!("expected an error frame, got {frame:?}");
    };
    assert_eq!(
        error.code(),
        change::StreamErrorCode::InternalRetryable,
        "{error:?}"
    );
    assert_eq!(error.retry_after_ms, 1_000, "{error:?}");

    bootstrap(port, &grant, &relation);
    assert!(read_batch(&plans[2], &poll(port, &plans[2], &heads[2])).up_to_date);

    fixture.insert(50, "seen by both streams");
    first.batches_through_title(&heads[0], "seen by both streams");
    second.batches_through_title(&heads[1], "seen by both streams");
    first.ends_within(brief + SETTLE);

    let mut left = TcpStream::connect(("127.0.0.1", port)).unwrap();
    left.set_read_timeout(Some(DELIVERY_TIMEOUT)).unwrap();
    let body = stream_request(&plans[2], &heads[2], PARKED_WAIT, &[]).encode_to_vec();
    write!(
        left,
        "POST /v1/changes HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\
         content-type: {CHANGE_CONTENT_TYPE}\r\ncontent-length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    left.write_all(&body).unwrap();
    let mut status = String::new();
    BufReader::new(&left).read_line(&mut status).unwrap();
    assert_eq!(status.split_whitespace().nth(1), Some("200"), "{status}");
    drop(left);
    std::thread::sleep(SETTLE);

    let third = open_stream(
        port,
        plans[3].title_column,
        &stream_request(&plans[3], &heads[3], PARKED_WAIT, &[]),
    );
    third.batches_through_title(&heads[3], "seen by both streams");

    serving.shutdown();
    fixture.drop_source();
}

/// A fence waits for every change build in flight, so nothing built under
/// the generation it retires is sent once it has answered.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_fence_waits_for_the_change_builds_in_flight() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("fencewait");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve_with(&fixture, 1, &["--change-tail-bytes", "0"]);
    let port = serving.port;
    let plan = bootstrap(port, &fixture.grant(1), &relation);
    let head = drain(port, &plan, &plan.activation_cursor);

    // A build reads the log's records and the fence only its head. Each build
    // that reaches the held table waits out its reader's lock timeout. The
    // plan's room holds all 80 of these small batches at once, so every build
    // is in flight from the start, and 80 builds over the log's 8 readers keep
    // some in flight for about 10 s.
    let small = change::ChangeRequest {
        max_batch_bytes: 64 * 1024,
        ..live_request(&plan, &head, Duration::ZERO)
    }
    .encode_to_vec();
    let mut held = TableHeld::hold("calque_control.canonical_log_record");
    std::thread::scope(|scope| {
        let polls = (0..80)
            .map(|_| {
                scope.spawn(|| {
                    http(
                        port,
                        "POST",
                        "/v1/changes",
                        &[("content-type", CHANGE_CONTENT_TYPE.to_owned())],
                        Some(small.as_slice()),
                    )
                    .status
                })
            })
            .collect::<Vec<_>>();
        held.wait_for_a_reader();
        let order = fixture
            .issuer
            .issue_fence_order(1, 2, unix_seconds(), 60)
            .unwrap();
        let fenced = scope.spawn(move || fence(port, &order));
        std::thread::sleep(SETTLE);
        assert!(
            !fenced.is_finished(),
            "the fence answered while builds were in flight"
        );
        assert_eq!(fixture.fenced_generation(), Some(1));
        drop(held);
        for polled in polls {
            let status = polled.join().unwrap();
            assert!(matches!(status, 200 | 503), "a poll was answered {status}");
        }
        let fenced = fenced.join().unwrap();
        assert_eq!(fenced.status, 200, "{}", fenced.text());
    });
    assert_eq!(fixture.fenced_generation(), Some(2));

    serving.shutdown();
    fixture.drop_source();
}

/// An open transaction holding `table`'s exclusive lock, so every statement
/// that reads the table waits until the guard drops.
struct TableHeld {
    client: Client,
    table: &'static str,
}

impl TableHeld {
    fn hold(table: &'static str) -> Self {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
            ))
            .unwrap();
        Self { client, table }
    }

    /// Returns once a statement waits for the lock.
    fn wait_for_a_reader(&mut self) {
        let deadline = Instant::now() + DELIVERY_TIMEOUT;
        loop {
            let waiting: i64 = self
                .client
                .query_one(
                    "SELECT count(*) FROM pg_locks
                      WHERE relation = $1::text::regclass AND NOT granted",
                    &[&self.table],
                )
                .unwrap()
                .get(0);
            if waiting > 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "nothing waited for {} within {DELIVERY_TIMEOUT:?}",
                self.table
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for TableHeld {
    fn drop(&mut self) {
        let _ = self.client.batch_execute("ROLLBACK");
    }
}

/// Inserts rows of 256 KiB titles, 14 MiB in transactions of 2 MiB: far more
/// than loopback's socket buffers hold, and less than one batch may carry.
/// Returns the titles in commit order.
fn insert_large_rows(fixture: &Fixture) -> Vec<String> {
    let titles = (0..56)
        .map(|row| format!("{row:04}{}", "x".repeat(256 * 1024)))
        .collect::<Vec<_>>();
    for (first, together) in (100..).step_by(8).zip(titles.chunks(8)) {
        fixture.insert_together(first, together);
    }
    titles
}

/// A one-batch request from `from` for the largest batch the protocol allows.
fn largest_batch(plan: &Plan, from: &str) -> change::ChangeRequest {
    change::ChangeRequest {
        max_batch_bytes: 16 * 1024 * 1024,
        ..live_request(plan, from, Duration::ZERO)
    }
}

/// Sends `request` on a connection of its own and reads none of the answer.
fn send_unread(port: u16, request: &change::ChangeRequest) -> TcpStream {
    let body = request.encode_to_vec();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "POST /v1/changes HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\
         content-type: {CHANGE_CONTENT_TYPE}\r\ncontent-length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
    stream
}

/// A stream whose client leaves while its build waits for its plan's room
/// gives its place among the open streams back then, not once the room frees.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_stream_whose_client_leaves_while_its_build_waits_frees_its_place() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("streamleft");
    fixture.seed();
    let relation = fixture.relation();
    // Every build reads the log, and one stream may be open.
    let serving = serve_with(
        &fixture,
        1,
        &["--change-tail-bytes", "0", "--max-open-streams", "1"],
    );
    let port = serving.port;
    let grant = fixture.grant(1);
    let leaving = bootstrap(port, &grant, &relation);
    let staying = bootstrap(port, &grant, &relation);
    let leaving_head = drain(port, &leaving, &leaving.activation_cursor);
    let staying_head = drain(port, &staying, &staying.activation_cursor);

    let left = open_stream(
        port,
        leaving.title_column,
        &stream_request(&leaving, &leaving_head, PARKED_WAIT, &[]),
    );
    let titles = insert_large_rows(&fixture);
    left.batches_through_title(&leaving_head, titles.last().unwrap());
    // An unread poll of the same plan fills the plan's room until the stall
    // bound closes it, so the stream's next build waits for room.
    let _unread = send_unread(port, &largest_batch(&leaving, &leaving_head));
    std::thread::sleep(SETTLE);
    fixture.insert(200, "past the plan's room");
    if let Some(sent) = left.next(SETTLE) {
        panic!("the stream's build found room: {sent:?}");
    }
    left.leave();
    std::thread::sleep(SETTLE);

    let stayed = open_stream(
        port,
        staying.title_column,
        &stream_request(&staying, &staying_head, PARKED_WAIT, &[]),
    );
    stayed.batches_through_title(&staying_head, "past the plan's room");

    serving.shutdown();
    fixture.drop_source();
}

/// A batch built from the log holds its plan's room until it is written.
/// Another build of the plan waits for that room without holding up a fence,
/// while other plans' builds go ahead; a client that reads none of its batch
/// holds it only until the write stall bound closes its connection.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn an_unread_batch_holds_its_plans_room_until_the_stall_bound_closes_it() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("planroom");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve_with(&fixture, 1, &["--change-tail-bytes", "0"]);
    let port = serving.port;
    let grant = fixture.grant(1);
    let stalled = bootstrap(port, &grant, &relation);
    let other = bootstrap(port, &grant, &relation);
    let stalled_head = drain(port, &stalled, &stalled.activation_cursor);
    let other_head = drain(port, &other, &other.activation_cursor);

    let titles = insert_large_rows(&fixture);
    wait_for_title(port, &other, &other_head, titles.last().unwrap());
    let send = |request: change::ChangeRequest| {
        let started = Instant::now();
        let response = http(
            port,
            "POST",
            "/v1/changes",
            &[("content-type", CHANGE_CONTENT_TYPE.to_owned())],
            Some(request.encode_to_vec().as_slice()),
        );
        (response, started.elapsed())
    };

    let mut unread = send_unread(port, &largest_batch(&stalled, &stalled_head));
    std::thread::sleep(SETTLE);
    std::thread::scope(|scope| {
        let waiting = scope.spawn(|| send(largest_batch(&stalled, &stalled_head)));
        std::thread::sleep(SETTLE);
        assert!(
            !waiting.is_finished(),
            "a second batch of the plan was built while its first was unwritten"
        );
        let (answered, took) = send(largest_batch(&other, &other_head));
        assert_eq!(read_batch(&other, &answered).titles, titles);
        assert!(
            took < WRITE_STALL_BOUND / 2,
            "another plan's build waited {took:?}"
        );
        // A build waiting for room holds no side of the fence.
        let started = Instant::now();
        let fenced = fence(
            port,
            &fixture
                .issuer
                .issue_fence_order(1, 2, unix_seconds(), 60)
                .unwrap(),
        );
        assert_eq!(fenced.status, 200, "{}", fenced.text());
        assert!(
            started.elapsed() < WRITE_STALL_BOUND / 2,
            "the fence waited {:?} for a build waiting for room",
            started.elapsed()
        );
        assert!(!waiting.is_finished());

        let (answered, waited) = waiting.join().unwrap();
        assert_eq!(read_batch(&stalled, &answered).titles, titles);
        assert!(
            waited > WRITE_STALL_BOUND / 2 && waited < WRITE_STALL_BOUND * 2,
            "the plan's build waited {waited:?}"
        );
        unread.set_read_timeout(Some(SETTLE)).unwrap();
        let mut received = Vec::new();
        let ended = unread.read_to_end(&mut received);
        assert!(
            !matches!(&ended, Err(error) if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            )),
            "the stalled connection is still open"
        );
        assert!(
            received.len() < answered.body.len(),
            "the stalled client was sent its whole batch"
        );
    });

    serving.shutdown();
    fixture.drop_source();
}

/// Once the log passes its bound, a re-cut publishes a newer artifact: a
/// later plan is handed it and catches up from it, and a plan made before
/// keeps its cut, its objects and every change after it.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_re_cut_hands_later_plans_a_newer_cut_and_keeps_live_plans_whole() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("recut");
    fixture.seed();
    let relation = fixture.relation();
    let metrics_port = reserved_port();
    let metrics_address = format!("127.0.0.1:{metrics_port}");
    let serving = serve_with(
        &fixture,
        1,
        &[
            "--recut-tail-ratio",
            "0.000001",
            "--recut-min-interval",
            "0",
            "--metrics-bind-address",
            &metrics_address,
        ],
    );
    let port = serving.port;
    let grant = fixture.grant(1);
    let before = bootstrap(port, &grant, &relation);
    drain(port, &before, &before.activation_cursor);
    assert_eq!(sample(metrics_port, "calque_artifact_recuts_total"), 0.0);
    // No process holds the plan: its retention lease alone keeps the log
    // behind the newer cut.
    let mut control = Client::connect(&database_url(), NoTls).unwrap();
    let leases: i64 = control
        .query_one(
            "SELECT count(*) FROM calque_control.canonical_log_plan_retention_lease
              WHERE source_id = $1 AND plan_id = $2",
            &[&fixture.source_id(), &before.plan_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(leases, 1);

    let tail = (10..20)
        .map(|row| format!("tail {row}"))
        .collect::<Vec<_>>();
    fixture.insert_together(10, &tail);
    wait_for_sample(metrics_port, "calque_artifact_recuts_total", |recuts| {
        recuts >= 1.0
    });
    // The pass after the re-cut measures from the newer cut, after trimming.
    wait_for_sample(
        metrics_port,
        "calque_canonical_log_tail_entries",
        |entries| entries == 0.0,
    );
    assert!(sample(metrics_port, "calque_canonical_log_retained_entries") >= 1.0);

    let after = bootstrap(port, &grant, &relation);
    assert_ne!(after.snapshot_cursor, before.snapshot_cursor);
    let without_query = |plan: &Plan| {
        plan.object_paths
            .iter()
            .map(|path| path.split('?').next().unwrap().to_owned())
            .collect::<BTreeSet<_>>()
    };
    assert_ne!(without_query(&after), without_query(&before));
    for path in before.object_paths.iter().chain(&after.object_paths) {
        assert_eq!(http(port, "GET", path, &[], None).status, 200, "{path}");
    }
    assert!(
        titles_through_head(port, &after, &after.snapshot_cursor).is_empty(),
        "the newer cut holds the tail"
    );
    assert_eq!(
        titles_through_head(port, &before, &before.snapshot_cursor),
        tail
    );

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}

/// An object store that cannot delete a retired cut's objects is reported
/// on every pass, and the log is still trimmed behind the newer cut.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_store_that_cannot_delete_a_retired_cut_does_not_stop_the_trim() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("recutdelete");
    fixture.seed();
    let relation = fixture.relation();
    let metrics_port = reserved_port();
    let metrics_address = format!("127.0.0.1:{metrics_port}");
    let serving = serve_with(
        &fixture,
        1,
        &[
            "--recut-tail-ratio",
            "0.000001",
            "--recut-min-interval",
            "0",
            "--metrics-bind-address",
            &metrics_address,
        ],
    );
    let port = serving.port;
    // The plan pins the first cut and holds the log until its grant expires,
    // which leaves time to make the store refuse deletes after the re-cut.
    let lifetime = Duration::from_secs(20);
    let issued = Instant::now();
    let before = bootstrap(port, &fixture.grant_for(1, lifetime.as_secs()), &relation);
    drain(port, &before, &before.activation_cursor);
    let tail = (10..20)
        .map(|row| format!("tail {row}"))
        .collect::<Vec<_>>();
    fixture.insert_together(10, &tail);
    wait_for_sample(metrics_port, "calque_artifact_recuts_total", |recuts| {
        recuts >= 1.0
    });
    wait_for_sample(
        metrics_port,
        "calque_artifact_recut_in_progress",
        |running| running == 0.0,
    );
    assert!(sample(metrics_port, "calque_canonical_log_retained_entries") >= 1.0);

    let objects = fixture.directory.path().join("state/objects/objects");
    let refusing = ReadOnlyDirectories::under(&objects);
    assert!(
        issued.elapsed() < lifetime,
        "the re-cut took longer than the plan's grant"
    );
    let delete_errors = "calque_errors_total{class=\"artifact_catalog\"}";
    let deadline = Instant::now() + PUBLICATION_TIMEOUT;
    while sample_or_zero(metrics_port, delete_errors) < 2.0
        || sample(metrics_port, "calque_canonical_log_retained_entries") != 0.0
    {
        assert!(
            Instant::now() < deadline,
            "the log was not trimmed past a cut the store could not delete: {} retained, {} errors",
            sample(metrics_port, "calque_canonical_log_retained_entries"),
            sample_or_zero(metrics_port, delete_errors),
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    drop(refusing);
    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}

/// Makes every directory under a root read-only, so no file in it can be
/// removed, and restores them when dropped.
struct ReadOnlyDirectories(Vec<PathBuf>);

impl ReadOnlyDirectories {
    fn under(root: &std::path::Path) -> Self {
        use std::os::unix::fs::PermissionsExt as _;
        let directories = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_dir())
            .collect::<Vec<_>>();
        assert!(
            !directories.is_empty(),
            "{} holds no objects",
            root.display()
        );
        for directory in &directories {
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o555)).unwrap();
        }
        Self(directories)
    }
}

impl Drop for ReadOnlyDirectories {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt as _;
        for directory in &self.0 {
            let _ = std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o755));
        }
    }
}

/// A write authorized under the shared fence lock waits for the writer
/// connection, and its client goes away. The blocking task that runs the
/// write holds the lock, so the fence waits for the commit instead of reading
/// its head and answering first, and every write of the retired generation
/// commits before the fence answers.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn a_write_whose_client_left_commits_before_the_fence_answers() {
    let _exclusive = exclusive();
    let fixture = Fixture::new("dropped_write");
    fixture.seed();
    let relation = fixture.relation();
    let serving = serve(&fixture, 1);
    let port = serving.port;
    let grant = fixture.grant(1);
    let plan = bootstrap(port, &grant, &relation);

    // A transaction of the application's holds row 0, so the write waits on
    // PostgreSQL with the writer connection, for less than the writer's one
    // second lock timeout.
    let mut holder = Client::connect(&database_url(), NoTls).unwrap();
    let mut holding = holder.transaction().unwrap();
    holding
        .execute(
            &format!("SELECT 1 FROM {} WHERE id = $1 FOR UPDATE", fixture.tasks()),
            &[&row_id(0)],
        )
        .unwrap();
    let title = "written by a client that left";
    let body = rename_request(
        &plan,
        0,
        title,
        &format!("mutation-{}", uuid::Uuid::new_v4()),
    );
    let order = fixture
        .issuer
        .issue_fence_order(1, 2, unix_seconds(), 60)
        .unwrap();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let sent = Instant::now();
    write!(
        stream,
        "POST /v1/mutations HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\
         authorization: Bearer {grant}\r\ncontent-type: {MUTATION_CONTENT_TYPE}\r\n\
         content-length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    // Still unanswered when its client leaves: the write is waiting.
    stream.set_nonblocking(true).unwrap();
    let waiting = stream.read(&mut [0_u8; 1]);
    assert!(
        waiting
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock),
        "the write answered before its client left: {waiting:?}"
    );
    drop(stream);
    std::thread::sleep(Duration::from_millis(100));

    let fencing = std::thread::spawn({
        let tasks = fixture.tasks();
        move || {
            let fenced = fence(port, &order);
            let mut client = Client::connect(&database_url(), NoTls).unwrap();
            let title: String = client
                .query_one(
                    &format!("SELECT title FROM {tasks} WHERE id = $1"),
                    &[&row_id(0)],
                )
                .unwrap()
                .get(0);
            (fenced, title)
        }
    });
    // The row is released once the fence answers, or before the write's lock
    // wait times out.
    let release_by = sent + Duration::from_millis(800);
    while !fencing.is_finished() && Instant::now() < release_by {
        std::thread::sleep(Duration::from_millis(5));
    }
    let answered_while_the_write_waited = fencing.is_finished();
    holding.rollback().unwrap();
    let (fenced, title_when_fenced) = fencing.join().unwrap();
    println!(
        "the fence answered {:?} after the write was sent",
        sent.elapsed()
    );
    assert_eq!(fenced.status, 200, "{}", fenced.text());
    assert_eq!(fixture.fenced_generation(), Some(2));
    let deadline = Instant::now() + SETTLE;
    while fixture.title_of(0).as_deref() != Some(title) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        (
            answered_while_the_write_waited,
            title_when_fenced.as_str(),
            fixture.title_of(0).as_deref(),
        ),
        (false, title, Some(title)),
        "the fence answered before the retired generation's write committed"
    );

    assert!(serving.shutdown(), "serve must exit cleanly on SIGTERM");
    fixture.drop_source();
}
