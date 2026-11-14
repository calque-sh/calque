use std::{net::TcpListener, path::PathBuf, process::Command};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/config")
        .join(name)
}

/// Runs `calque serve` against a fixture manifest on a reserved-then-freed port
/// and returns `(status success, stdout, stderr, port, state directory)`.
fn serve(manifest: &str) -> (bool, String, String, u16, tempfile::TempDir) {
    let state = tempfile::tempdir().unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);

    let output = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "serve",
            "--config",
            fixture(manifest).to_str().unwrap(),
            "--state-dir",
            state.path().to_str().unwrap(),
            "--grant-audience",
            "incident.test",
            "--grant-verification-key-env",
            "ABSENT_TEST_GRANT_KEY",
            "--allow-origin",
            "http://127.0.0.1:5173",
            "--port",
            &port.to_string(),
        ])
        .env_remove("ABSENT_TEST_GRANT_KEY")
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
        port,
        state,
    )
}

fn assert_nothing_started(port: u16, state: &tempfile::TempDir, stdout: &str) {
    assert!(!stdout.contains("ready"), "unexpected readiness: {stdout}");
    TcpListener::bind(("127.0.0.1", port))
        .expect("the refusal must happen before the listener is bound");
    assert!(
        !state.path().join("state.json").exists(),
        "the refusal must precede durable startup state"
    );
}

/// A source-wide manifest passes validation, and startup stops at the grant
/// key environment it is not given, before any state exists and before the
/// listener is bound.
#[test]
fn a_source_wide_scope_passes_validation_and_startup_continues() {
    let (success, stdout, stderr, port, state) = serve("serve-sourcewide-smoke.config.json");

    assert!(!success);
    assert!(
        stderr.contains("environment variable ABSENT_TEST_GRANT_KEY, which --grant-verification-key-env names, is not set"),
        "startup must reach the grant configuration: {stderr}"
    );
    assert_nothing_started(port, &state, &stdout);
}

/// Every S3 environment variable `serve`'s selector consults, so a test can
/// clear whatever the developer running it happens to have exported.
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

fn recovery_command(arguments: &[&str], object_store: &[(&str, &str)]) -> (bool, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_calque"));
    command.args(arguments);
    for name in S3_ENVIRONMENT {
        command.env_remove(name);
    }
    // Unset, so a build that resolved the object half after the connection
    // string would fail on that instead and the assertions below would not
    // hold by accident.
    command.env_remove("CALQUE_DATABASE_URL");
    for (name, value) in object_store {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

/// A complete S3 configuration, pointing at a port nothing listens on. Opening
/// a store contacts nothing, so a recovery command goes on to read the
/// connection string.
const COMPLETE_S3: &[(&str, &str)] = &[
    ("CALQUE_OBJECT_STORE", "s3"),
    ("CALQUE_S3_BUCKET", "calque-artifacts"),
    ("CALQUE_S3_PREFIX", "calque"),
    ("CALQUE_S3_REGION", "us-east-1"),
    ("CALQUE_S3_ENDPOINT", "http://127.0.0.1:9"),
    ("CALQUE_S3_ACCESS_KEY", "recovery-test-access-key"),
    ("CALQUE_S3_SECRET_KEY", "recovery-test-secret-key"),
    ("CALQUE_S3_FORCE_PATH_STYLE", "1"),
];

/// A path-style setting that is neither 1 nor 0 lists the two values.
#[test]
fn a_bad_force_path_style_value_lists_what_is_accepted() {
    let workspace = tempfile::tempdir().unwrap();
    let unit = workspace.path().join("unit");
    let state = workspace.path().join("state");
    let object_store = COMPLETE_S3
        .iter()
        .copied()
        .filter(|(name, _)| *name != "CALQUE_S3_FORCE_PATH_STYLE")
        .chain([("CALQUE_S3_FORCE_PATH_STYLE", "true")])
        .collect::<Vec<_>>();
    let (success, stderr) = recovery_command(
        &[
            "backup",
            "--output",
            unit.to_str().unwrap(),
            "--state-dir",
            state.to_str().unwrap(),
        ],
        &object_store,
    );
    assert!(!success, "{stderr}");
    assert!(
        stderr.contains("E_SERVE_OBJECT_STORE_INVALID CALQUE_S3_FORCE_PATH_STYLE=true is not accepted; set it to 1 for path-style bucket addressing or 0 for virtual-host style, or unset it to use path style exactly when CALQUE_S3_ENDPOINT is set"),
        "{stderr}"
    );
}

/// The S3 store is not a refusal, but a configuration `serve` would not start
/// on is refused first, before the connection string is read or any directory
/// exists. `restore` needs this most: its target directory has to stay absent.
#[test]
fn recovery_commands_resolve_the_object_half_before_anything_else() {
    let workspace = tempfile::tempdir().unwrap();
    let unit = workspace.path().join("unit");
    let state = workspace.path().join("state");
    let unit = unit.to_str().unwrap();
    let state = state.to_str().unwrap();
    let commands = [
        vec!["backup", "--output", unit, "--state-dir", state],
        vec!["restore", "--backup", unit, "--state-dir", state],
        vec!["verify-restore", "--backup", unit, "--state-dir", state],
    ];

    // Half-written: the selector says `s3` and nothing names a bucket, so the
    // commands give `serve`'s own refusal.
    for arguments in &commands {
        let (success, stderr) = recovery_command(arguments, &[("CALQUE_OBJECT_STORE", "s3")]);
        assert!(!success, "{arguments:?} must refuse: {stderr}");
        assert!(
            stderr.contains("E_SERVE_OBJECT_STORE_INCOMPLETE")
                && stderr.contains("CALQUE_S3_BUCKET"),
            "{arguments:?}: {stderr}"
        );
        assert!(
            !stderr.contains("CALQUE_DATABASE_URL"),
            "{arguments:?} read the connection string first: {stderr}"
        );
    }
    assert!(
        !workspace.path().join("unit").exists() && !workspace.path().join("state").exists(),
        "a refused recovery command must create nothing"
    );

    // Complete: the S3 store is opened, and the command gets on with its own
    // work -- here, discovering that no connection string is exported. Neither
    // the state directory a restore requires to be absent nor the provider
    // adapter's scratch directory beside it is left behind.
    for arguments in &commands {
        let (success, stderr) = recovery_command(arguments, COMPLETE_S3);
        assert!(!success, "{arguments:?}: {stderr}");
        assert!(
            !stderr.contains("E_RECOVERY_OBJECT_STORE_UNSUPPORTED")
                && !stderr.contains("E_SERVE_OBJECT_STORE"),
            "a configured S3 store is a store to read objects from, not a refusal: {stderr}"
        );
        assert!(
            stderr.contains("CALQUE_DATABASE_URL"),
            "{arguments:?}: {stderr}"
        );
        assert!(
            !workspace.path().join("state").exists()
                && !workspace.path().join("state.recovery-staging").exists(),
            "{arguments:?} left a directory where a restore requires none"
        );
    }

    // And the default store still needs no S3 configuration at all.
    let (success, stderr) = recovery_command(
        &["verify-restore", "--backup", unit, "--state-dir", state],
        &[],
    );
    assert!(!success);
    assert!(stderr.contains("CALQUE_DATABASE_URL"), "{stderr}");
}
