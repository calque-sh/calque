//! `calque dev` refusals at startup, driven through the binary: each names
//! what it refers to and, where a command fixes it, that command.

use std::{
    fs::File,
    io::Read,
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Command, Output, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use calque_engine::{LocalNames, LocalState, StateStore};
use source_control::{QualifiedTable, SourceIdentity};
use sync_types::SourceEpoch;

/// A loopback listener standing in for PostgreSQL. It counts the connections
/// that open a PostgreSQL handshake and closes each one after its first
/// message. Other local processes probe new ports with HTTP or TLS, or connect
/// and send nothing; those are not counted.
fn counting_listener() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handshakes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&handshakes);
    thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let counter = Arc::clone(&counter);
            thread::spawn(move || {
                if opens_postgres_handshake(&mut stream) {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });
    (port, handshakes)
}

/// Whether the first message on `stream` is an SSLRequest, a GSSENCRequest or
/// a protocol 3 startup message.
fn opens_postgres_handshake(stream: &mut TcpStream) -> bool {
    let mut first = [0; 8];
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    if stream.read_exact(&mut first).is_err() {
        return false;
    }
    let code = u32::from_be_bytes([first[4], first[5], first[6], first[7]]);
    matches!(code, 80_877_103 | 80_877_104) || code >> 16 == 3
}

/// A loopback listener standing in for a PostgreSQL that accepts connections
/// and never answers, as Docker's port proxy does while its container is
/// unreachable. It counts the connections it is offered and holds each open.
fn silent_listener() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().flatten() {
            counter.fetch_add(1, Ordering::SeqCst);
            held.push(stream);
        }
    });
    (port, accepted)
}

/// A port nothing listens on once this returns; dev binds it before it reads
/// its state directory.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A state directory an earlier dev enrolled for `tables` in `public`.
fn enrolled_state(state: &Path, tables: &[&str]) {
    let store = StateStore::open(state).unwrap();
    let identity = SourceIdentity {
        system_identifier: "7612345678901234567".to_owned(),
        database_name: "app".to_owned(),
        timeline_id: 1,
        previous_timeline_id: 1,
    };
    let names = LocalNames::derive(store.root(), &identity).unwrap();
    let tables = tables
        .iter()
        .map(|table| QualifiedTable {
            schema: "public".to_owned(),
            table: (*table).to_owned(),
        })
        .collect();
    let state = LocalState::new(
        identity,
        SourceEpoch::new("dev-startup-test".to_owned()).unwrap(),
        names,
        tables,
    )
    .unwrap();
    store.write_state(&state).unwrap();
}

/// `calque dev` for `public.issues` on `state`, reading its connection string
/// from `database_url_env`.
fn dev(state: &Path, database_url_env: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_calque"));
    command.args([
        "dev",
        "--table",
        "public.issues",
        "--acknowledge-disposable-source",
        "--state-dir",
        state.to_str().unwrap(),
        "--port",
        &free_port().to_string(),
        "--database-url-env",
        database_url_env,
    ]);
    command
}

/// The lines the binary printed as its error.
fn refusal(output: &Output) -> Vec<String> {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    stderr
        .lines()
        .skip_while(|line| !line.starts_with("calque: "))
        .map(str::to_owned)
        .collect()
}

/// A state directory enrolled for other tables names the destroy command with
/// the variable dev read, since destroy's default may point at another source.
#[test]
fn a_state_enrolled_for_other_tables_names_destroy_with_the_variable_dev_read() {
    let (database_port, postgres_connections) = counting_listener();
    let workspace = tempfile::tempdir().unwrap();
    let state = workspace.path().join("state");
    enrolled_state(&state, &["projects"]);

    let output = dev(&state, "CALQUE_DEV_STARTUP_URL")
        .env(
            "CALQUE_DEV_STARTUP_URL",
            format!("postgresql://127.0.0.1:{database_port}/app"),
        )
        .output()
        .unwrap();

    let state = state.display();
    assert_eq!(
        refusal(&output),
        [format!(
            "calque: {state} is enrolled for [public.projects], but dev requested [public.issues]. \
             Calque will not change an existing enrollment in place. Rerun dev with the persisted table set, \
             or run calque destroy --state-dir {state} --database-url-env CALQUE_DEV_STARTUP_URL --yes and then rerun dev."
        )]
    );
    assert_eq!(postgres_connections.load(Ordering::SeqCst), 0);
}

/// A connection-string variable that is not set names the variable and both
/// ways to fix it, before dev creates any state.
#[test]
fn an_unset_database_variable_names_itself_and_the_flag_that_renames_it() {
    let workspace = tempfile::tempdir().unwrap();
    let state = workspace.path().join("state");

    let output = dev(&state, "CALQUE_DEV_STARTUP_UNSET_URL")
        .env_remove("CALQUE_DEV_STARTUP_UNSET_URL")
        .output()
        .unwrap();

    assert_eq!(
        refusal(&output),
        ["calque: environment variable CALQUE_DEV_STARTUP_UNSET_URL is not set; \
          set it to the source's PostgreSQL connection string, or name the variable that holds it with --database-url-env"]
    );
    assert!(
        !state.exists(),
        "the refusal must precede the state directory"
    );
}

/// A state directory dev cannot create names the path it failed on, which a
/// bare operating-system error does not.
#[test]
fn a_state_directory_dev_cannot_create_names_its_path() {
    let (database_port, postgres_connections) = counting_listener();
    let workspace = tempfile::tempdir().unwrap();
    let file = workspace.path().join("not-a-directory");
    std::fs::write(&file, b"").unwrap();
    let state = file.join("state");

    let output = dev(&state, "CALQUE_DEV_STARTUP_URL")
        .env(
            "CALQUE_DEV_STARTUP_URL",
            format!("postgresql://127.0.0.1:{database_port}/app"),
        )
        .output()
        .unwrap();

    assert_eq!(
        refusal(&output),
        [format!(
            "calque: local state I/O failed at {}: Not a directory (os error 20)",
            state.display()
        )]
    );
    assert_eq!(postgres_connections.load(Ordering::SeqCst), 0);
}

/// A state file dev cannot decode names its path before the reason, which the
/// decoder's message alone does not.
#[cfg(unix)]
#[test]
fn a_state_file_dev_cannot_decode_names_its_path() {
    use std::os::unix::fs::PermissionsExt;

    let (database_port, postgres_connections) = counting_listener();
    let workspace = tempfile::tempdir().unwrap();
    let state = workspace.path().join("state");
    let state_file = StateStore::open(&state).unwrap().state_path();
    std::fs::write(&state_file, b"{").unwrap();
    std::fs::set_permissions(&state_file, std::fs::Permissions::from_mode(0o600)).unwrap();

    let output = dev(&state, "CALQUE_DEV_STARTUP_URL")
        .env(
            "CALQUE_DEV_STARTUP_URL",
            format!("postgresql://127.0.0.1:{database_port}/app"),
        )
        .output()
        .unwrap();

    assert_eq!(
        refusal(&output),
        [format!(
            "calque: {}: local state JSON is invalid: EOF while parsing an object at line 1 column 1",
            state_file.display()
        )]
    );
    assert_eq!(postgres_connections.load(Ordering::SeqCst), 0);
}

/// A PostgreSQL that accepts the connection and never answers is refused once
/// the connection's 5 s timeout runs out, which covers the startup message as
/// well as the socket; dev does not wait on it forever.
#[test]
fn a_postgres_that_never_answers_is_refused_once_the_connect_timeout_runs_out() {
    let (database_port, postgres_connections) = silent_listener();
    let workspace = tempfile::tempdir().unwrap();
    let state = workspace.path().join("state");
    let stderr_path = workspace.path().join("stderr");
    let mut child = dev(&state, "CALQUE_DEV_STARTUP_URL")
        .env(
            "CALQUE_DEV_STARTUP_URL",
            format!("postgresql://127.0.0.1:{database_port}/app"),
        )
        .stdout(Stdio::null())
        .stderr(File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();

    let started = Instant::now();
    let bound = Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > bound {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "dev was still starting {bound:?} after it was pointed at a PostgreSQL that never answers: {}",
                std::fs::read_to_string(&stderr_path).unwrap()
            );
        }
        thread::sleep(Duration::from_millis(50));
    };

    let output = Output {
        status,
        stdout: Vec::new(),
        stderr: std::fs::read(&stderr_path).unwrap(),
    };
    assert_eq!(
        refusal(&output),
        ["calque: PostgreSQL source operation failed: error connecting to server: connection timed out"]
    );
    assert!(postgres_connections.load(Ordering::SeqCst) >= 1);
}
