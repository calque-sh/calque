//! A port another process holds, or a metrics listener that cannot exist,
//! stops `serve` and `dev` before either creates state or reaches PostgreSQL,
//! and the error names the address and the flag that moves it.

use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use dataset_grant_issuer::GrantIssuer;

/// Every S3 environment variable `serve`'s object store selector consults,
/// cleared so a developer's exports cannot stop `serve` before the listener.
const S3_ENVIRONMENT: &[&str] = &[
    "CALQUE_OBJECT_STORE",
    "CALQUE_S3_BUCKET",
    "CALQUE_S3_PREFIX",
    "CALQUE_S3_REGION",
    "CALQUE_S3_ENDPOINT",
    "CALQUE_S3_ACCESS_KEY",
    "CALQUE_S3_SECRET_KEY",
    "CALQUE_S3_SESSION_TOKEN",
    "CALQUE_S3_FORCE_PATH_STYLE",
];

/// A loopback listener standing in for PostgreSQL. It counts the connections
/// it is offered and closes each one at once.
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

/// A loopback port nothing listens on once this returns.
fn free_port() -> u16 {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    reservation.local_addr().unwrap().port()
}

/// Runs `serve` on `port` with `extra` arguments, against a PostgreSQL
/// stand-in, and returns its output, its state directory and the connections
/// that stand-in was offered.
fn serve(port: u16, extra: &[&str]) -> (Output, tempfile::TempDir, Arc<AtomicUsize>) {
    let (database_port, postgres_connections) = counting_listener();
    let workspace = tempfile::tempdir().unwrap();
    let issuer = GrantIssuer::generate("listen-address-key", "incident.test")
        .unwrap()
        .0;
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/config/serve-sourcewide-smoke.config.json");

    let mut command = Command::new(env!("CARGO_BIN_EXE_calque"));
    command
        .args([
            "serve",
            "--config",
            manifest.to_str().unwrap(),
            "--state-dir",
            workspace.path().join("state").to_str().unwrap(),
            "--grant-audience",
            "incident.test",
            "--grant-verification-key-env",
            "CALQUE_LISTEN_ADDRESS_GRANT_KEY",
            "--allow-origin",
            "http://127.0.0.1:5173",
            "--port",
            &port.to_string(),
        ])
        .args(extra)
        .env(
            "CALQUE_LISTEN_ADDRESS_GRANT_KEY",
            issuer.verification_key_environment().unwrap(),
        )
        .env(
            "SERVE_SMOKE_DATABASE_URL",
            format!("postgresql://127.0.0.1:{database_port}/app"),
        );
    for name in S3_ENVIRONMENT {
        command.env_remove(name);
    }
    (command.output().unwrap(), workspace, postgres_connections)
}

/// Runs `dev` on `port` with `extra` arguments, as [`serve`] does.
fn dev(port: u16, extra: &[&str]) -> (Output, tempfile::TempDir, Arc<AtomicUsize>) {
    let (database_port, postgres_connections) = counting_listener();
    let workspace = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "dev",
            "--table",
            "public.issues",
            "--acknowledge-disposable-source",
            "--state-dir",
            workspace.path().join("state").to_str().unwrap(),
            "--port",
            &port.to_string(),
        ])
        .args(extra)
        .env(
            "CALQUE_DATABASE_URL",
            format!("postgresql://127.0.0.1:{database_port}/app"),
        )
        .output()
        .unwrap();
    (output, workspace, postgres_connections)
}

/// The process failed with every one of `expected` in its stderr, before it
/// created its state directory or offered PostgreSQL a connection.
fn assert_refused_before_startup(
    (output, workspace, postgres_connections): (Output, tempfile::TempDir, Arc<AtomicUsize>),
    expected: &[&str],
) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    for fragment in expected {
        assert!(stderr.contains(fragment), "expected {fragment:?}: {stderr}");
    }
    let state: &Path = &workspace.path().join("state");
    assert!(
        !state.exists(),
        "the refusal must precede the state directory: {stderr}"
    );
    assert_eq!(
        postgres_connections.load(Ordering::SeqCst),
        0,
        "the refusal must precede any PostgreSQL connection: {stderr}"
    );
}

#[test]
fn serve_refuses_a_held_port_before_it_creates_state_or_connects() {
    let holder = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = holder.local_addr().unwrap().port();

    assert_refused_before_startup(
        serve(port, &[]),
        &[
            &format!("cannot listen on 127.0.0.1:{port}: "),
            "pass another --port",
        ],
    );
}

#[test]
fn dev_refuses_a_held_port_before_it_creates_state_or_connects() {
    let holder = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = holder.local_addr().unwrap().port();

    assert_refused_before_startup(
        dev(port, &[]),
        &[
            &format!("cannot listen on 127.0.0.1:{port}: "),
            "pass another --port",
        ],
    );
}

/// A metrics listener on the HTTP listener's port, on no port, on a wildcard
/// address nobody names a `Host` for, or on a port another process holds is
/// refused where the HTTP listener's own binding is checked: before state,
/// PostgreSQL or CDC.
#[test]
fn serve_refuses_a_metrics_listener_that_cannot_exist_before_it_creates_state_or_connects() {
    let port = free_port();
    let http = format!("127.0.0.1:{port}");
    assert_refused_before_startup(
        serve(port, &["--metrics-bind-address", &http]),
        &[&format!(
            "--metrics-bind-address {http} shares a port with the HTTP listener on {http}"
        )],
    );
    let wildcard = format!("0.0.0.0:{port}");
    assert_refused_before_startup(
        serve(
            port,
            &[
                "--metrics-bind-address",
                &wildcard,
                "--metrics-expected-host",
                "127.0.0.1:9",
            ],
        ),
        &[&format!(
            "--metrics-bind-address {wildcard} shares a port with the HTTP listener on {http}"
        )],
    );
    assert_refused_before_startup(
        serve(port, &["--metrics-bind-address", "127.0.0.1:0"]),
        &["--metrics-bind-address 127.0.0.1:0: HTTP port must be nonzero"],
    );
    let other = free_port();
    assert_refused_before_startup(
        serve(
            port,
            &["--metrics-bind-address", &format!("0.0.0.0:{other}")],
        ),
        &["is a wildcard address, so --metrics-expected-host has to name the Host"],
    );
    assert_refused_before_startup(
        serve(
            port,
            &[
                "--metrics-bind-address",
                &format!("127.0.0.1:{other}"),
                "--metrics-expected-host",
                "http://127.0.0.1",
            ],
        ),
        &["--metrics-expected-host http://127.0.0.1: "],
    );
    // Not a socket address at all: the argument parser refuses it.
    assert_refused_before_startup(
        serve(port, &["--metrics-bind-address", "localhost:8788"]),
        &["invalid value 'localhost:8788' for '--metrics-bind-address"],
    );

    let holder = TcpListener::bind("127.0.0.1:0").unwrap();
    let held = holder.local_addr().unwrap();
    assert_refused_before_startup(
        serve(port, &["--metrics-bind-address", &held.to_string()]),
        &[
            &format!("cannot listen on {held}: "),
            "pass another --metrics-bind-address",
        ],
    );
}

#[test]
fn dev_refuses_a_metrics_listener_that_cannot_exist_before_it_creates_state_or_connects() {
    let port = free_port();
    assert_refused_before_startup(
        dev(port, &["--metrics-port", &port.to_string()]),
        &[&format!(
            "--metrics-port {port} is also the HTTP listener's --port"
        )],
    );
    assert_refused_before_startup(
        dev(port, &["--metrics-port", "0"]),
        &["--metrics-port must be nonzero"],
    );

    let holder = TcpListener::bind("127.0.0.1:0").unwrap();
    let held = holder.local_addr().unwrap().port();
    assert_refused_before_startup(
        dev(port, &["--metrics-port", &held.to_string()]),
        &[
            &format!("cannot listen on 127.0.0.1:{held}: "),
            "pass another --metrics-port",
        ],
    );
}
