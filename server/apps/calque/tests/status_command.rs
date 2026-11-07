//! `calque status` run through the binary: it names the source, its epoch,
//! its tables and its state directory, and prints none of the secrets the
//! directory holds.

use std::process::Command;

use calque_engine::{LocalNames, LocalState, StateStore};
use source_control::{QualifiedTable, SourceIdentity};
use sync_types::{SourceEpoch, SourceId};

#[test]
fn status_names_the_source_and_prints_no_secret() {
    let directory = tempfile::tempdir().unwrap();
    let state_dir = directory.path().join("state");
    let store = StateStore::open(&state_dir).unwrap();
    let state = LocalState::new(
        SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        },
        SourceEpoch::new("status-epoch").unwrap(),
        LocalNames {
            source_id: SourceId::new("status-source").unwrap(),
            publication_name: "status_publication".to_owned(),
            slot_name: "status_slot".to_owned(),
        },
        ["projects", "tasks"]
            .into_iter()
            .map(|table| QualifiedTable {
                schema: "public".to_owned(),
                table: table.to_owned(),
            })
            .collect(),
    )
    .unwrap();
    store.write_state(&state).unwrap();
    store.load_or_create_secrets().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_calque"))
        .arg("status")
        .arg("--state-dir")
        .arg(&state_dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        stdout,
        format!(
            "source: status-source\nepoch: status-epoch\ntables:\n  - public.projects\n  - public.tasks\nstate: {}\n",
            store.root().display()
        )
    );

    let secrets: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state_dir.join("secrets.json")).unwrap()).unwrap();
    for name in ["cursorSecret", "proofSecret", "planSecret", "bearerToken"] {
        let secret = secrets[name].as_str().unwrap();
        assert!(!secret.is_empty(), "{name}");
        assert!(!stdout.contains(secret), "status printed {name}");
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains(secret),
            "status wrote {name} to stderr"
        );
    }
}
