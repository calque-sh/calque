//! `calque` reaches a PostgreSQL on another machine only over TLS that
//! verifies the server, as a managed PostgreSQL requires. A proxy that
//! terminates TLS in front of the suite's PostgreSQL stands in for one: it
//! answers the TLS request as PostgreSQL does, presents a certificate from a
//! CA the test makes, and records every session it forwards.
//!
//! These tests are `#[ignore]`d and run through
//! `scripts/live-postgres-suite.sh`; without `TEST_DATABASE_URL` they fail
//! rather than silently pass.

use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use calque_engine::StateStore;
use postgres::{Client, NoTls};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const DATABASE_ENV: &str = "CALQUE_LIVE_TLS_DATABASE_URL";
const READY_TIMEOUT: Duration = Duration::from_secs(300);
const CANCEL_REQUEST: u32 = 80_877_102;
const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;

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

/// The suite's URL is `postgresql://user:password@127.0.0.1:port/database`.
fn database_parts() -> (String, SocketAddr, String) {
    let url = database_url();
    let rest = url.split_once("://").unwrap().1;
    let (userinfo, rest) = rest.rsplit_once('@').unwrap();
    let (address, database) = rest.split_once('/').unwrap();
    let database = database.split('?').next().unwrap().to_owned();
    (userinfo.to_owned(), address.parse().unwrap(), database)
}

fn percent_encoded(path: &Path) -> String {
    path.to_str()
        .unwrap()
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn reserved_port() -> u16 {
    let reservation = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    port
}

/// A certificate authority, written where `sslrootcert` reads it.
struct Authority {
    issuer: rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
    root: PathBuf,
    _directory: tempfile::TempDir,
}

impl Authority {
    fn new(name: &str) -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let issuer =
            rcgen::CertifiedIssuer::self_signed(params, rcgen::KeyPair::generate().unwrap())
                .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("ca.pem");
        std::fs::write(&root, issuer.pem()).unwrap();
        Self {
            issuer,
            root,
            _directory: directory,
        }
    }

    /// The TLS configuration of a server whose certificate this authority
    /// signed for `name`.
    fn server(&self, name: &str) -> Arc<rustls::ServerConfig> {
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = rcgen::CertificateParams::new(vec![name.to_owned()])
            .unwrap()
            .signed_by(&key, &*self.issuer)
            .unwrap();
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone(), self.issuer.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
        .unwrap();
        config.alpn_protocols = vec![b"postgresql".to_vec()];
        Arc::new(config)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Session {
    /// A startup message read inside TLS. `replication` is whether it asked
    /// for a replication connection, which only libpq opens.
    Tls { replication: bool },
    /// A startup message read in plaintext: credentials and queries in the
    /// clear.
    Plaintext,
    /// A cancel request, which carries a backend key and nothing else.
    Cancel,
}

/// The stand-in for a remote PostgreSQL. With no TLS configuration it
/// answers the TLS request with `N`, as a server without TLS does.
struct TlsProxy {
    port: u16,
    sessions: Arc<Mutex<Vec<Session>>>,
    _runtime: tokio::runtime::Runtime,
}

impl TlsProxy {
    fn start(server: Option<Arc<rustls::ServerConfig>>) -> Self {
        let (_, upstream, _) = database_parts();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let v4 = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let port = v4.local_addr().unwrap().port();
        // `localhost` may resolve to ::1 first.
        let v6 = runtime.block_on(TcpListener::bind(("::1", port))).ok();
        for listener in std::iter::once(v4).chain(v6) {
            let server = server.clone();
            let sessions = Arc::clone(&sessions);
            runtime.spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let server = server.clone();
                    let sessions = Arc::clone(&sessions);
                    tokio::spawn(async move {
                        let _ = relay(client, upstream, server, sessions).await;
                    });
                }
            });
        }
        Self {
            port,
            sessions,
            _runtime: runtime,
        }
    }

    /// The suite's database through this proxy, addressed by host name and
    /// trusting `root`.
    fn url(&self, root: &Path) -> String {
        let (userinfo, _, database) = database_parts();
        format!(
            "postgresql://{userinfo}@localhost:{}/{database}?sslmode=verify-full&sslrootcert={}",
            self.port,
            percent_encoded(root)
        )
    }

    fn sessions(&self) -> Vec<Session> {
        self.sessions.lock().unwrap().clone()
    }
}

async fn relay(
    mut client: TcpStream,
    upstream: SocketAddr,
    server: Option<Arc<rustls::ServerConfig>>,
    sessions: Arc<Mutex<Vec<Session>>>,
) -> std::io::Result<()> {
    loop {
        let mut header = [0; 8];
        client.read_exact(&mut header).await?;
        let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        match u32::from_be_bytes(header[4..].try_into().unwrap()) {
            GSSENC_REQUEST => client.write_all(b"N").await?,
            SSL_REQUEST => {
                let Some(server) = server.clone() else {
                    client.write_all(b"N").await?;
                    continue;
                };
                client.write_all(b"S").await?;
                let mut tls = tokio_rustls::TlsAcceptor::from(server)
                    .accept(client)
                    .await?;
                let mut startup = vec![0; 4];
                tls.read_exact(&mut startup).await?;
                let length = u32::from_be_bytes(startup[..4].try_into().unwrap()) as usize;
                startup.resize(length, 0);
                tls.read_exact(&mut startup[4..]).await?;
                let session = if is_cancel(&startup) {
                    Session::Cancel
                } else {
                    Session::Tls {
                        replication: asks_for_replication(&startup),
                    }
                };
                return forward(tls, startup, session, upstream, &sessions).await;
            }
            code => {
                let mut startup = header.to_vec();
                startup.resize(length, 0);
                client.read_exact(&mut startup[8..]).await?;
                let session = if code == CANCEL_REQUEST {
                    Session::Cancel
                } else {
                    Session::Plaintext
                };
                return forward(client, startup, session, upstream, &sessions).await;
            }
        }
    }
}

fn is_cancel(startup: &[u8]) -> bool {
    startup.len() >= 8 && u32::from_be_bytes(startup[4..8].try_into().unwrap()) == CANCEL_REQUEST
}

/// A startup message is its length, the protocol version, then `key\0value\0`
/// pairs.
fn asks_for_replication(startup: &[u8]) -> bool {
    let fields = startup[8..].split(|byte| *byte == 0).collect::<Vec<_>>();
    fields
        .chunks(2)
        .any(|pair| pair[0] == b"replication" && pair.get(1) == Some(&b"database".as_slice()))
}

async fn forward(
    mut client: impl AsyncRead + AsyncWrite + Unpin,
    startup: Vec<u8>,
    session: Session,
    upstream: SocketAddr,
    sessions: &Mutex<Vec<Session>>,
) -> std::io::Result<()> {
    sessions.lock().unwrap().push(session);
    let mut server = TcpStream::connect(upstream).await?;
    server.write_all(&startup).await?;
    tokio::io::copy_bidirectional(&mut client, &mut server).await?;
    Ok(())
}

/// A table and publication `calque inspect` and `calque dev` can read, and
/// the state directory `dev` serves it from.
struct Fixture {
    table: String,
    directory: tempfile::TempDir,
}

impl Fixture {
    fn create() -> Self {
        let table = format!("tls_{}", uuid::Uuid::new_v4().simple());
        Client::connect(&database_url(), NoTls)
            .unwrap()
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (id uuid PRIMARY KEY, title text NOT NULL);
                 INSERT INTO public.{table}
                 SELECT md5('tls:' || n)::uuid, 'row ' || n FROM generate_series(1, 3) n;
                 CREATE PUBLICATION {table} FOR TABLE public.{table};"
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

    /// Runs `calque inspect` on the fixture's table through `database_url`.
    fn inspect(&self, database_url: &str) -> (bool, String) {
        let output = Command::new(env!("CARGO_BIN_EXE_calque"))
            .args([
                "inspect",
                "--source",
                "primary",
                "--publication",
                &self.table,
                "--table",
                &format!("public.{}", self.table),
            ])
            .env("CALQUE_DATABASE_URL", database_url)
            .output()
            .unwrap();
        (
            output.status.success(),
            String::from_utf8(output.stderr).unwrap(),
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let Ok(mut client) = Client::connect(&database_url(), NoTls) else {
            return;
        };
        if let Ok(state) = StateStore::open_existing(self.state()).and_then(|s| s.read_state()) {
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
        let _ = client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {table}; DROP TABLE IF EXISTS public.{table};",
            table = self.table
        ));
    }
}

/// A `calque dev` child, killed when dropped.
struct Dev(Child);

impl Dev {
    /// Starts `dev` on the fixture's table and returns once `/readyz`
    /// answers 204: its snapshot is built and its change stream is open.
    fn start(fixture: &Fixture, database_url: &str) -> Self {
        let port = reserved_port();
        let mut child = Command::new(env!("CARGO_BIN_EXE_calque"))
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
            .env(DATABASE_ENV, database_url)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let stderr_tail = Arc::new(Mutex::new(Vec::new()));
        let tail = Arc::clone(&stderr_tail);
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                tail.lock().unwrap().push(line);
            }
        });
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let started = Instant::now();
        loop {
            let mut line = String::new();
            if stdout.read_line(&mut line).unwrap() == 0 {
                panic!(
                    "dev exited before readiness: {:?}\nstderr: {:#?}",
                    child.wait().unwrap(),
                    stderr_tail.lock().unwrap()
                );
            }
            if line.starts_with("Calque dev is ready at") {
                break;
            }
            assert!(
                started.elapsed() < READY_TIMEOUT,
                "dev did not report readiness"
            );
        }
        thread::spawn(move || {
            let _ = std::io::copy(&mut stdout, &mut std::io::sink());
        });
        while readyz(port) != Some(204) {
            assert!(
                started.elapsed() < READY_TIMEOUT,
                "dev did not answer /readyz with 204: {:#?}",
                stderr_tail.lock().unwrap()
            );
            thread::sleep(Duration::from_millis(100));
        }
        Self(child)
    }
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn readyz(port: u16) -> Option<u16> {
    use std::io::{Read as _, Write as _};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .write_all(
            format!("GET /readyz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.split_whitespace().nth(1)?.parse().ok()
}

/// `dev` against a host name: every connection it opens, the replication one
/// libpq carries included, is TLS to a server its CA vouches for, and the
/// change stream acknowledges a commit made after readiness.
#[test]
#[ignore = "requires PostgreSQL; run through scripts/live-postgres-suite.sh"]
fn dev_serves_a_remote_postgres_over_verified_tls() {
    let _exclusive = exclusive();
    let authority = Authority::new("Calque test CA");
    let proxy = TlsProxy::start(Some(authority.server("localhost")));
    let fixture = Fixture::create();
    let _dev = Dev::start(&fixture, &proxy.url(&authority.root));

    let slot = StateStore::open_existing(fixture.state())
        .unwrap()
        .read_state()
        .unwrap()
        .names
        .slot_name;
    let mut client = Client::connect(&database_url(), NoTls).unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO public.{} VALUES (gen_random_uuid(), 'after readiness')",
            fixture.table
        ))
        .unwrap();
    let committed: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);
    let started = Instant::now();
    loop {
        let acknowledged: bool = client
            .query_one(
                "SELECT coalesce(confirmed_flush_lsn >= $1::text::pg_lsn, false)
                   FROM pg_replication_slots WHERE slot_name = $2",
                &[&committed, &slot],
            )
            .unwrap()
            .get(0);
        if acknowledged {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the change stream did not acknowledge {committed}"
        );
        thread::sleep(Duration::from_millis(100));
    }

    let sessions = proxy.sessions();
    assert!(
        sessions.contains(&Session::Tls { replication: true }),
        "{sessions:?}"
    );
    assert!(
        sessions.contains(&Session::Tls { replication: false }),
        "{sessions:?}"
    );
    assert!(!sessions.contains(&Session::Plaintext), "{sessions:?}");
}

/// A server whose certificate the URL's CA did not sign, a certificate for
/// another name, and a server that declines TLS are each refused before a
/// startup message, and so before a credential, is sent.
#[test]
#[ignore = "requires PostgreSQL; run through scripts/live-postgres-suite.sh"]
fn inspect_refuses_a_server_tls_does_not_vouch_for() {
    let authority = Authority::new("Calque test CA");
    let fixture = Fixture::create();

    let trusted = TlsProxy::start(Some(authority.server("localhost")));
    let (success, stderr) = fixture.inspect(&trusted.url(&authority.root));
    assert!(success, "{stderr}");
    let sessions = trusted.sessions();
    assert!(!sessions.is_empty(), "{stderr}");
    assert!(
        sessions
            .iter()
            .all(|session| *session == Session::Tls { replication: false }),
        "{sessions:?}"
    );

    let stranger = Authority::new("Another CA");
    let other_name = TlsProxy::start(Some(authority.server("db.invalid")));
    for (proxy, root) in [(&trusted, &stranger.root), (&other_name, &authority.root)] {
        let before = proxy.sessions().len();
        let (success, stderr) = fixture.inspect(&proxy.url(root));
        assert!(!success, "{stderr}");
        assert!(stderr.contains("certificate"), "{stderr}");
        assert_eq!(proxy.sessions().len(), before, "{stderr}");
    }

    let declines = TlsProxy::start(None);
    let (success, stderr) = fixture.inspect(&declines.url(&authority.root));
    assert!(!success, "{stderr}");
    assert_eq!(declines.sessions(), [], "{stderr}");
}
