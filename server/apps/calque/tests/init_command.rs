//! `calque init` run through the binary: input that is not an inspection
//! document is refused under the name of where it came from, because in
//! `calque inspect ... | calque init --from-inspection -` a failed inspect
//! leaves init reading nothing at all.

use std::{
    io::Write,
    process::{Command, Stdio},
};

/// Runs `calque init` on `from` with `stdin` as its standard input, writing
/// into a fresh directory, and returns `(status success, stderr)`.
fn init(from: &str, stdin: &str) -> (bool, String) {
    let directory = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args(["init", "--from-inspection", from, "--application-id", "app"])
        .current_dir(directory.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

#[test]
fn empty_standard_input_is_refused_as_standard_input() {
    let (success, stderr) = init("-", "");
    assert!(!success);
    assert!(
        stderr.starts_with("calque: standard input is not a calque inspect document: "),
        "{stderr}"
    );
}

#[test]
fn a_file_that_is_not_an_inspection_is_refused_by_its_path() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inspection.json");
    std::fs::write(&path, "{\"tables\": 1}").unwrap();
    let (success, stderr) = init(path.to_str().unwrap(), "");
    assert!(!success);
    assert!(
        stderr.starts_with(&format!(
            "calque: {} is not a calque inspect document: ",
            path.display()
        )),
        "{stderr}"
    );
}

#[test]
fn the_earlier_flag_name_is_refused_with_the_current_one() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_calque"))
        .args([
            "init",
            "--from-inspection",
            "-",
            "--application-id",
            "app",
            "--connection-env",
            "APP_DATABASE_URL",
        ])
        .current_dir(directory.path())
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "calque: --connection-env is now --database-url-env\n"
    );
}
