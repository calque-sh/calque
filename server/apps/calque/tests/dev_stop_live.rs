//! `calque dev` stops gracefully when nothing is left to stop it: when the
//! reader of its stderr goes away, as it does when the launcher that started
//! it dies, and on SIGHUP. Either way it releases its source lease, so the
//! next start takes the lease at once instead of waiting it out. A `dev`
//! whose PostgreSQL stops answering still stops on SIGTERM. A running `dev`
//! also answers a bootstrap that presents any bearer but its own with 401,
//! serves the scrape only on the metrics port it is given, and under
//! `--tenant-column` serves its bearer one tenant's rows. A state directory
//! deleted and made anew at the same path is refused with the `calque destroy`
//! that removes the old one's enrollment, and starts once it has run.
//!
//! These tests are `#[ignore]`d and run through
//! `scripts/live-postgres-suite.sh`; without `TEST_DATABASE_URL` they fail
//! rather than silently pass.

use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Read as _, Write as _},
    net::{Shutdown, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, ChildStderr, Command, ExitStatus, Output, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime},
};

use calque_engine::{LocalNames, StateStore};
use postgres::{Client, NoTls};

const DATABASE_ENV: &str = "CALQUE_LIVE_DEV_STOP_DATABASE_URL";
const READY_TIMEOUT: Duration = Duration::from_secs(300);
/// A stop that took longer than one source lease TTL (10 s) would release
/// the lease no sooner than letting it expire does.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `dev` gives a stop before it exits without finishing it.
const STOP_BOUND: Duration = Duration::from_secs(10);

/// One `calque dev` at a time from this binary: each drives a fenced canonical
/// writer and a replication slot against the one shared PostgreSQL.
static ONE_DEV_AT_A_TIME: Mutex<()> = Mutex::new(());

fn exclusive() -> std::sync::MutexGuard<'static, ()> {
    ONE_DEV_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL is required; run through scripts/live-postgres-suite.sh")
}

fn reserved_port() -> u16 {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    port
}

/// A disposable table for `dev` to serve, and the state directory it serves
/// it from. Dropping it removes what `dev` created in PostgreSQL.
struct Fixture {
    table: String,
    directory: tempfile::TempDir,
}

impl Fixture {
    fn create() -> Self {
        let table = format!("dev_stop_{}", uuid::Uuid::new_v4().simple());
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (id uuid PRIMARY KEY, title text NOT NULL);
                 INSERT INTO public.{table}
                 SELECT md5('dev-stop:' || n)::uuid, 'row ' || n FROM generate_series(1, 3) n;"
            ))
            .unwrap();
        Self {
            table,
            directory: tempfile::tempdir().unwrap(),
        }
    }

    /// Tenant 1's and tenant 2's rows in a table keyed by `tenant_id` first,
    /// and a table every tenant shares, `<table>_shared`.
    fn partitioned() -> Self {
        let table = format!("dev_tenants_{}", uuid::Uuid::new_v4().simple());
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (
                     tenant_id int8, id int8, title text NOT NULL, PRIMARY KEY (tenant_id, id));
                 INSERT INTO public.{table} VALUES (1, 1, 'tenant-1-only'), (2, 1, 'tenant-2-only');
                 CREATE TABLE public.{table}_shared (id int8 PRIMARY KEY, name text NOT NULL);
                 INSERT INTO public.{table}_shared VALUES (1, 'shared-row');"
            ))
            .unwrap();
        Self {
            table,
            directory: tempfile::tempdir().unwrap(),
        }
    }

    fn state(&self) -> PathBuf {
        self.directory.path().join("state")
    }

    fn bearer(&self) -> String {
        let secrets: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self.state().join("secrets.json")).unwrap())
                .unwrap();
        secrets["bearerToken"].as_str().unwrap().to_owned()
    }

    fn names(&self) -> LocalNames {
        StateStore::open_existing(self.state())
            .unwrap()
            .read_state()
            .unwrap()
            .names
    }

    /// The source lease row: phase, owner and expiry.
    fn lease(&self) -> (String, Option<String>, Option<SystemTime>) {
        let mut client = Client::connect(&database_url(), NoTls).unwrap();
        let row = client
            .query_one(
                "SELECT lease_phase, owner_id, expires_at
                   FROM calque_control.source_fence_lease WHERE source_id = $1",
                &[&self.names().source_id.as_str()],
            )
            .unwrap();
        (row.get(0), row.get(1), row.get(2))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let Ok(mut client) = Client::connect(&database_url(), NoTls) else {
            return;
        };
        if let Ok(store) = StateStore::open_existing(self.state()) {
            if let Ok(state) = store.read_state() {
                let names = state.names;
                let _ = client.batch_execute(&format!(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                         WHERE slot_name = '{slot}' AND active_pid IS NOT NULL;
                     SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                         WHERE slot_name = '{slot}';
                     DELETE FROM calque_control.source_fence_lease WHERE source_id = '{source}';
                     DELETE FROM calque_control.source_slot_enrollment WHERE source_id = '{source}';
                     DROP PUBLICATION IF EXISTS {publication};",
                    slot = names.slot_name,
                    source = names.source_id.as_str(),
                    publication = names.publication_name,
                ));
            }
        }
        let _ = client.batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{table}, public.{table}_shared",
            table = self.table
        ));
    }
}

/// A `calque dev` child, killed when the guard drops if it has not exited.
struct Dev {
    child: Child,
    port: u16,
    /// What `dev` printed up to and including its readiness line.
    lines: Vec<String>,
    /// Set to make the stderr reader close its end of the pipe.
    stderr_closing: Arc<AtomicBool>,
    stderr_reader: Option<JoinHandle<()>>,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
}

impl Dev {
    /// Starts `dev` on the fixture's table and returns once it reports
    /// readiness and answers `/readyz` with 204, so its stop signals are
    /// being listened for.
    fn start(fixture: &Fixture) -> Self {
        Self::start_with(fixture, &[])
    }

    /// [`Dev::start`], with `extra` arguments after the ones every test
    /// passes.
    fn start_with(fixture: &Fixture, extra: &[&str]) -> Self {
        Self::launch(fixture, extra, &database_url())
    }

    /// [`Dev::start`], connecting to PostgreSQL through `proxy`.
    fn start_through(fixture: &Fixture, proxy: &Proxy) -> Self {
        Self::launch(fixture, &[], &proxy.url)
    }

    fn launch(fixture: &Fixture, extra: &[&str], database_url: &str) -> Self {
        let port = reserved_port();
        let mut child = dev_command(fixture, port, database_url)
            .args(extra)
            // The stop on a closed stderr needs a log line to meet it, and
            // the replication client's idle loop writes one every 500 ms at
            // info.
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr_closing = Arc::new(AtomicBool::new(false));
        let stderr_tail = Arc::new(Mutex::new(VecDeque::new()));
        let stderr_reader = read_stderr(
            child.stderr.take().unwrap(),
            Arc::clone(&stderr_closing),
            Arc::clone(&stderr_tail),
        );
        let mut dev = Self {
            child,
            port,
            lines: Vec::new(),
            stderr_closing,
            stderr_reader: Some(stderr_reader),
            stderr_tail,
        };

        let started = Instant::now();
        let mut stdout = BufReader::new(dev.child.stdout.take().unwrap());
        let mut lines = Vec::new();
        loop {
            let mut line = String::new();
            if stdout.read_line(&mut line).unwrap() == 0 {
                panic!(
                    "dev exited before readiness: {:?}\nstdout: {lines:#?}\nstderr: {:#?}",
                    dev.child.wait().unwrap(),
                    dev.stderr()
                );
            }
            let ready = line.starts_with("Calque dev is ready at");
            lines.push(line.trim_end().to_owned());
            if ready {
                break;
            }
            assert!(
                started.elapsed() < READY_TIMEOUT,
                "dev did not report readiness within {READY_TIMEOUT:?}: {lines:#?}"
            );
        }
        dev.lines = lines;
        thread::spawn(move || {
            let _ = std::io::copy(&mut stdout, &mut std::io::sink());
        });
        while readyz(port) != Some(204) {
            assert!(
                started.elapsed() < READY_TIMEOUT,
                "dev did not answer /readyz with 204 within {READY_TIMEOUT:?}: {:#?}",
                dev.stderr()
            );
            thread::sleep(Duration::from_millis(100));
        }
        dev
    }

    /// Closes the reading end of `dev`'s stderr, once the reader has taken
    /// the line in flight.
    fn close_stderr(&mut self) {
        self.stderr_closing.store(true, Ordering::SeqCst);
        self.stderr_reader.take().unwrap().join().unwrap();
    }

    fn signal(&self, signal: &str) {
        let status = Command::new("kill")
            .args([signal, &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success(), "kill {signal} failed");
    }

    /// The exit status, and how long it took, if `dev` exits within
    /// `bound`.
    fn exit_within(&mut self, bound: Duration) -> (ExitStatus, Duration) {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return (status, started.elapsed());
            }
            assert!(
                started.elapsed() < bound,
                "dev did not exit within {bound:?}; /readyz {:?}; stderr: {:#?}",
                readyz(self.port),
                self.stderr()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn stderr(&self) -> Vec<String> {
        self.stderr_tail.lock().unwrap().iter().cloned().collect()
    }

    /// Waits for a stderr line containing `text`, which the reader thread
    /// may still be taking in after `dev` has exited.
    fn says(&self, text: &str) -> bool {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(5) {
            if self.stderr().iter().any(|line| line.contains(text)) {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }
}

/// `calque dev` on the fixture's table and state directory.
fn dev_command(fixture: &Fixture, port: u16, database_url: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_calque"));
    command
        .args([
            "dev",
            "--table",
            &format!("public.{}", fixture.table),
            "--acknowledge-disposable-source",
            "--state-dir",
            fixture.state().to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--database-url-env",
            DATABASE_ENV,
        ])
        .env(DATABASE_ENV, database_url);
    command
}

/// Runs `command` to its exit, failing the test if it runs past `bound`.
fn output_within(mut command: Command, bound: Duration) -> Output {
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    let (done, finished) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(child.wait_with_output());
    });
    match finished.recv_timeout(bound) {
        Ok(output) => output.unwrap(),
        Err(_) => {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
            panic!("the command did not exit within {bound:?}");
        }
    }
}

impl Drop for Dev {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// A loopback TCP proxy in front of the suite's PostgreSQL, which a test can
/// make stop answering. Dropping it closes every connection it holds.
struct Proxy {
    /// The suite's connection string, through the proxy.
    url: String,
    shared: Arc<ProxyShared>,
}

#[derive(Default)]
struct ProxyShared {
    /// Set once the proxy stops answering. No byte crosses it from then on,
    /// and a connection it accepts is held open and never answered.
    silent: AtomicBool,
    /// Both ends of each connection the proxy forwarded.
    forwarded: Mutex<Vec<(TcpStream, TcpStream)>>,
    /// The connections accepted once the proxy was silent.
    unanswered: Mutex<Vec<TcpStream>>,
}

impl Proxy {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let (url, upstream) = through_port(&database_url(), listener.local_addr().unwrap().port());
        let shared = Arc::new(ProxyShared::default());
        let accepting = Arc::clone(&shared);
        thread::spawn(move || {
            for client in listener.incoming().flatten() {
                if accepting.silent.load(Ordering::SeqCst) {
                    accepting.unanswered.lock().unwrap().push(client);
                    continue;
                }
                let Ok(server) = TcpStream::connect(&upstream) else {
                    continue;
                };
                forward(&client, &server, &accepting);
                forward(&server, &client, &accepting);
                accepting.forwarded.lock().unwrap().push((client, server));
            }
        });
        Self { url, shared }
    }

    /// Stops answering and leaves every connection open, as a PostgreSQL
    /// that froze would: what is sent on one never arrives, and nothing comes
    /// back.
    fn freeze(&self) {
        self.shared.silent.store(true, Ordering::SeqCst);
    }

    /// Closes every connection, then accepts new ones and never answers
    /// them, as Docker's port proxy does while its container is unreachable.
    fn drop_connections(&self) {
        self.freeze();
        for (client, server) in self.shared.forwarded.lock().unwrap().iter() {
            let _ = client.shutdown(Shutdown::Both);
            let _ = server.shutdown(Shutdown::Both);
        }
    }

    fn unanswered(&self) -> usize {
        self.shared.unanswered.lock().unwrap().len()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.drop_connections();
        for stream in self.shared.unanswered.lock().unwrap().iter() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

/// Copies what arrives on `from` to `to` until either closes, and drops it
/// once the proxy is silent.
fn forward(from: &TcpStream, to: &TcpStream, shared: &Arc<ProxyShared>) {
    let mut from = from.try_clone().unwrap();
    let mut to = to.try_clone().unwrap();
    let shared = Arc::clone(shared);
    thread::spawn(move || {
        let mut buffer = [0; 16 * 1024];
        while let Ok(read @ 1..) = from.read(&mut buffer) {
            if !shared.silent.load(Ordering::SeqCst) && to.write_all(&buffer[..read]).is_err() {
                break;
            }
        }
        if !shared.silent.load(Ordering::SeqCst) {
            let _ = to.shutdown(Shutdown::Write);
        }
    });
}

/// `url` with its host and port replaced by the loopback `port`, and the
/// host and port it replaced.
fn through_port(url: &str, port: u16) -> (String, String) {
    let (scheme, rest) = url.split_once("://").unwrap();
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, path) = rest.split_at(end);
    let (user, address) = match authority.rsplit_once('@') {
        Some((user, address)) => (format!("{user}@"), address),
        None => (String::new(), authority),
    };
    let upstream = if address.contains(':') {
        address.to_owned()
    } else {
        format!("{address}:5432")
    };
    (format!("{scheme}://{user}127.0.0.1:{port}{path}"), upstream)
}

/// Reads `stderr` a line at a time, keeping the last 200, until it ends or
/// `close` is set; then it drops the pipe's reading end.
fn read_stderr(
    stderr: ChildStderr,
    close: Arc<AtomicBool>,
    tail: Arc<Mutex<VecDeque<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut stderr = BufReader::new(stderr);
        let mut line = String::new();
        while !close.load(Ordering::SeqCst) {
            line.clear();
            if !matches!(stderr.read_line(&mut line), Ok(read) if read > 0) {
                break;
            }
            let mut tail = tail.lock().unwrap();
            if tail.len() == 200 {
                tail.pop_front();
            }
            tail.push_back(line.trim_end().to_owned());
        }
    })
}

/// The status `/readyz` answers with, or `None` when nothing answers.
fn readyz(port: u16) -> Option<u16> {
    get(port, "/readyz", &format!("127.0.0.1:{port}")).map(|(status, _)| status)
}

/// The status and body a `GET` of `path` sent as `Host: host` answers with,
/// or `None` when nothing answers.
fn get(port: u16, path: &str, host: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n"
    )
    .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let status = response.split_whitespace().nth(1)?.parse().ok()?;
    let body = response.split_once("\r\n\r\n")?.1.to_owned();
    Some((status, body))
}

/// A launcher that dies leaves `dev` writing its log to a pipe nobody reads.
/// The first line that meets the closed pipe stops it as Ctrl-C does: it
/// exits 0 and leaves the lease row unowned.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_stops_and_releases_its_lease_when_nothing_reads_its_stderr() {
    let _one = exclusive();
    let fixture = Fixture::create();
    let mut dev = Dev::start(&fixture);
    let held = fixture.lease();
    assert_eq!(held.0, "active", "{held:?}");

    dev.close_stderr();
    let (status, took) = dev.exit_within(STOP_TIMEOUT);
    assert!(status.success(), "{status} after {took:?}");
    assert_eq!(fixture.lease(), ("unowned".to_owned(), None, None));
}

/// The names a state directory gives the source derive from its path, so one
/// made anew where a deleted one was meets the enrollment that one left in
/// PostgreSQL. `dev` names the `calque destroy` that removes it, and starts
/// once it has run.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_names_the_destroy_that_clears_a_deleted_state_directorys_enrollment() {
    let _one = exclusive();
    let fixture = Fixture::create();
    let mut dev = Dev::start(&fixture);
    let names = fixture.names();
    dev.signal("-INT");
    let (status, took) = dev.exit_within(STOP_TIMEOUT);
    assert!(status.success(), "{status} after {took:?}");
    std::fs::remove_dir_all(fixture.state()).unwrap();

    let refused = output_within(
        dev_command(&fixture, reserved_port(), &database_url()),
        Duration::from_secs(60),
    );
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success(), "{stderr}");
    let state_dir = fixture.state().to_str().unwrap().to_owned();
    assert!(
        stderr.contains(&format!(
            "calque: this source was enrolled by another state directory at the same path. Run calque destroy --state-dir {state_dir} --database-url-env {DATABASE_ENV} --yes, then rerun dev"
        )),
        "{stderr}"
    );

    let mut destroy = Command::new(env!("CARGO_BIN_EXE_calque"));
    destroy
        .args([
            "destroy",
            "--state-dir",
            &state_dir,
            "--database-url-env",
            DATABASE_ENV,
            "--yes",
        ])
        .env(DATABASE_ENV, database_url());
    let destroyed = output_within(destroy, Duration::from_secs(60));
    assert!(
        destroyed.status.success(),
        "{}",
        String::from_utf8_lossy(&destroyed.stderr)
    );

    let _dev = Dev::start(&fixture);
    assert_eq!(fixture.names(), names);
}

/// SIGHUP, which closing a terminal sends, stops `dev` as Ctrl-C does.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_stops_and_releases_its_lease_on_sighup() {
    let _one = exclusive();
    let fixture = Fixture::create();
    let mut dev = Dev::start(&fixture);
    let held = fixture.lease();
    assert_eq!(held.0, "active", "{held:?}");

    dev.signal("-HUP");
    let (status, took) = dev.exit_within(STOP_TIMEOUT);
    assert!(status.success(), "{status} after {took:?}");
    assert_eq!(fixture.lease(), ("unowned".to_owned(), None, None));
    assert!(
        dev.says("stopping: received SIGHUP, so the terminal that started calque dev closed"),
        "{:#?}",
        dev.stderr()
    );
    assert!(dev.says("stopped"), "{:#?}", dev.stderr());
}

/// A PostgreSQL that closes every connection and then accepts new ones
/// without answering leaves change capture reconnecting. Each attempt gives up
/// once its connect timeout runs out, so SIGTERM still stops `dev` cleanly.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_stops_on_sigterm_while_its_postgres_accepts_connections_and_never_answers() {
    let _one = exclusive();
    let fixture = Fixture::create();
    let proxy = Proxy::start();
    let mut dev = Dev::start_through(&fixture, &proxy);

    proxy.drop_connections();
    let dropped = Instant::now();
    while proxy.unanswered() == 0 {
        assert!(
            dropped.elapsed() < READY_TIMEOUT,
            "dev did not reconnect within {READY_TIMEOUT:?}: {:#?}",
            dev.stderr()
        );
        thread::sleep(Duration::from_millis(20));
    }
    // Past the first reconnect's backoff, so change capture is in a connect
    // when the signal arrives.
    thread::sleep(Duration::from_secs(1));
    dev.signal("-TERM");
    let (status, took) = dev.exit_within(STOP_TIMEOUT);
    assert!(
        status.success(),
        "{status} after {took:?}: {:#?}",
        dev.stderr()
    );
    assert!(
        dev.says("stopping: received SIGTERM"),
        "{:#?}",
        dev.stderr()
    );
    assert!(dev.says("stopped"), "{:#?}", dev.stderr());
}

/// A PostgreSQL that freezes with every connection open leaves the source's
/// requests unanswered. Each fails once its reply bound runs out, so `dev`
/// finishes its stop before `STOP_BOUND`, exits 1 and says the cleanup
/// failed; the watchdog behind `STOP_BOUND` never fires.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_stops_before_its_stop_bound_when_its_postgres_freezes() {
    let _one = exclusive();
    let fixture = Fixture::create();
    let proxy = Proxy::start();
    let mut dev = Dev::start_through(&fixture, &proxy);

    proxy.freeze();
    // Longer than a lease renewal's 3.3 s interval, so one is waiting on the
    // frozen control connection.
    thread::sleep(Duration::from_secs(5));
    dev.signal("-TERM");
    let (status, took) = dev.exit_within(STOP_BOUND + Duration::from_secs(10));
    assert_eq!(status.code(), Some(1), "{status} after {took:?}");
    assert!(took < STOP_BOUND, "{took:?}: {:#?}", dev.stderr());
    let exited = Instant::now();
    while dev.stderr().last().map(String::as_str)
        != Some("calque: persistent CDC session cleanup failed")
    {
        assert!(
            exited.elapsed() < Duration::from_secs(2),
            "{:#?}",
            dev.stderr()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// Without `--metrics-port` nothing serves the scrape, and `dev` says so;
/// with it, the scrape is served on that loopback port, to that port's Host
/// alone. The HTTP listener answers `/metrics` as a path it does not route
/// either way.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_serves_the_scrape_on_its_metrics_port_alone() {
    let _one = exclusive();
    let fixture = Fixture::create();
    let mut dev = Dev::start(&fixture);
    let http_host = format!("127.0.0.1:{}", dev.port);
    assert!(
        dev.lines
            .iter()
            .any(|line| line == "Metrics: off; pass --metrics-port to serve them"),
        "{:#?}",
        dev.lines
    );
    assert_eq!(
        get(dev.port, "/metrics", &http_host).map(|(status, _)| status),
        Some(404)
    );
    dev.signal("-TERM");
    let (status, took) = dev.exit_within(STOP_TIMEOUT);
    assert!(status.success(), "{status} after {took:?}");

    let metrics_port = reserved_port();
    let metrics_host = format!("127.0.0.1:{metrics_port}");
    let mut dev = Dev::start_with(&fixture, &["--metrics-port", &metrics_port.to_string()]);
    let http_host = format!("127.0.0.1:{}", dev.port);
    let announced = format!("Metrics: http://{metrics_host}/metrics");
    assert!(dev.lines.contains(&announced), "{:#?}", dev.lines);
    let (status, body) = get(metrics_port, "/metrics", &metrics_host).unwrap();
    assert_eq!(status, 200, "{body}");
    assert!(body.ends_with("# EOF\n"), "{body}");
    assert_eq!(
        get(metrics_port, "/metrics", &http_host).map(|(status, _)| status),
        Some(421)
    );
    assert_eq!(
        get(dev.port, "/metrics", &http_host).map(|(status, _)| status),
        Some(404)
    );
    dev.signal("-TERM");
    let (status, took) = dev.exit_within(STOP_TIMEOUT);
    assert!(status.success(), "{status} after {took:?}");
}

/// The status a `dev` bootstrap for `relation` answers when it presents
/// `bearer`, and the whole response.
fn bootstrap_status(port: u16, bearer: &str, relation: &str) -> (u16, String) {
    let body = serde_json::json!({
        "protocolVersion": 1,
        "requiredTables": [relation],
        "acceptedEncodings": ["zstd", "identity"],
        "memoryBudgetBytes": 1_073_741_824_u64,
        "clientHeapReserveBytes": 67_108_864_u64,
        "clientInstanceId": "dev-bearer-live",
        "deviceId": "dev-bearer-live-device",
    })
    .to_string();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    write!(
        stream,
        "POST /v1/bootstrap HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\
         authorization: Bearer {bearer}\r\ncontent-type: application/json\r\n\
         accept-encoding: zstd, identity\r\n\
         x-calque-artifact-profile: chromium-native-artifact-v1\r\n\
         content-length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {response:?}"));
    (status, response)
}

/// What a `GET` of `path` answers with: its status and its body's bytes, in
/// identity encoding.
fn get_bytes(port: u16, path: &str) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\
         accept-encoding: identity\r\n\r\n"
    )
    .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response head");
    let head = String::from_utf8_lossy(&response[..split]).to_ascii_lowercase();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(!head.contains("transfer-encoding: chunked"), "{head}");
    (status, response[split + 4..].to_vec())
}

fn holds(bytes: &[u8], text: &str) -> bool {
    bytes
        .windows(text.len())
        .any(|window| window == text.as_bytes())
}

/// Under `--tenant-column` and `--tenant`, `dev` serves the scope
/// partitioned by that column: its bearer's artifact holds the tenant's rows
/// and the shared table's, and none of another tenant's.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_serves_its_bearer_one_tenant_of_a_partitioned_scope() {
    let _one = exclusive();
    let fixture = Fixture::partitioned();
    let shared = format!("public.{}_shared", fixture.table);
    let dev = Dev::start_with(
        &fixture,
        &[
            "--table",
            &shared,
            "--tenant-column",
            "tenant_id",
            "--tenant",
            "1",
        ],
    );
    assert!(
        dev.lines
            .iter()
            .any(|line| line == "Scope: partitioned by tenant_id; the bearer reads tenant 1"),
        "{:#?}",
        dev.lines
    );
    let body = serde_json::json!({
        "protocolVersion": 1,
        "requiredTables": [format!("public.{}", fixture.table), shared],
        "acceptedEncodings": ["zstd", "identity"],
        "memoryBudgetBytes": 1_073_741_824_u64,
        "clientHeapReserveBytes": 67_108_864_u64,
        "clientInstanceId": "dev-tenants-live",
        "deviceId": "dev-tenants-live-device",
    })
    .to_string();
    let mut stream = TcpStream::connect(("127.0.0.1", dev.port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    write!(
        stream,
        "POST /v1/bootstrap HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\
         authorization: Bearer {bearer}\r\ncontent-type: application/json\r\n\
         accept-encoding: identity\r\n\
         x-calque-artifact-profile: chromium-native-artifact-v1\r\n\
         content-length: {length}\r\n\r\n{body}",
        port = dev.port,
        bearer = fixture.bearer(),
        length = body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (head, plan) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{response}");
    let plan: serde_json::Value = serde_json::from_str(plan).unwrap();
    let url = plan["artifacts"][0]["selectedRepresentation"]["downloadUrl"]
        .as_str()
        .unwrap();
    let path = &url[url.find("/v1/").unwrap()..];
    let (status, artifact) = get_bytes(dev.port, path);
    assert_eq!(status, 200);
    assert_eq!(&artifact[..8], b"CALQART1");
    assert!(holds(&artifact, "tenant-1-only"));
    assert!(holds(&artifact, "shared-row"));
    assert!(!holds(&artifact, "tenant-2-only"));
}

/// The shared bearer is the only credential `dev` accepts. One of the same
/// length and encoding with other bytes is refused over HTTP, and the bearer
/// from the state directory is accepted for the same request.
#[test]
#[ignore = "requires TEST_DATABASE_URL"]
fn dev_refuses_a_bootstrap_with_another_bearer() {
    let _one = exclusive();
    let fixture = Fixture::create();
    let dev = Dev::start(&fixture);
    let bearer = fixture.bearer();
    let relation = format!("public.{}", fixture.table);

    let other = "A".repeat(bearer.len());
    assert_ne!(other, bearer);
    let (status, refused) = bootstrap_status(dev.port, &other, &relation);
    assert_eq!(status, 401, "{refused}");
    let (status, planned) = bootstrap_status(dev.port, &bearer, &relation);
    assert_eq!(status, 200, "{planned}");
}
