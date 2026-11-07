//! The checked-in packed artifacts under `spec/fixtures/packed-artifact`,
//! which the client's unpacker restores in its own tests. Each case is a
//! directory holding `artifact.bin` and the `packed.bin` [`pack`] makes of it;
//! `manifest.json` lists them.
//!
//! Regenerate after an intended change to the packer, the encoder or the
//! brotli crate:
//! `CALQUE_WRITE_PACKED_FIXTURE=1 cargo test -p direct-columnar --test packed_fixture`

use std::{fs, path::PathBuf};

use direct_columnar::pack::{pack, unpack, ColumnChoice, Packed, Transform};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[allow(dead_code)]
#[path = "../examples/emit_fixture.rs"]
mod emit_fixture;
#[allow(dead_code)]
#[path = "../examples/emit_key_fixture.rs"]
mod emit_key_fixture;
mod support;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn fixtures() -> PathBuf {
    root().join("spec/fixtures/packed-artifact")
}

fn locked_brotli() -> String {
    let lock = fs::read_to_string(root().join("Cargo.lock")).unwrap();
    let after = lock
        .split("name = \"brotli\"\nversion = \"")
        .nth(1)
        .expect("Cargo.lock locks the brotli crate");
    after[..after.find('"').unwrap()].to_owned()
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    /// The encoder behind the packer's size test: another version may choose
    /// other columns.
    size_test_encoder: String,
    cases: Vec<Case>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: &'static str,
    /// `packed`, or `stored` when the body is the artifact itself.
    body: &'static str,
    artifact_bytes: usize,
    artifact_sha256: String,
    packed_bytes: usize,
    packed_sha256: String,
    hoisted_parts: usize,
    transforms: Transforms,
    /// Foreign-key values found in no table, over all `fk` columns.
    escapes: u64,
}

#[derive(Serialize)]
struct Transforms {
    fk: usize,
    int: usize,
    sep: usize,
}

fn artifacts() -> Vec<(&'static str, Vec<u8>)> {
    let every_kind = support::every_kind();
    let smallest_first = support::smallest_first(&every_kind);
    vec![
        ("emit-fixture", emit_fixture::artifact().unwrap()),
        ("emit-key-fixture", emit_key_fixture::artifact().unwrap()),
        ("every-kind", every_kind),
        ("tables-smallest-first", smallest_first),
        ("stored", support::stored()),
    ]
}

fn applied(packed: &Packed, transform: Transform) -> usize {
    packed
        .columns
        .iter()
        .filter(|column| column.transform == Some(transform))
        .count()
}

fn choice<'a>(packed: &'a Packed, table: &str, column: &str) -> &'a ColumnChoice {
    packed
        .columns
        .iter()
        .find(|choice| choice.table == table && choice.column == column)
        .unwrap()
}

fn hoisted(packed: &Packed) -> Vec<(&str, &str, usize)> {
    packed
        .hoisted
        .iter()
        .map(|part| (part.table.as_str(), part.column.as_str(), part.part))
        .collect()
}

#[test]
fn the_checked_in_bodies_are_this_packer_s_and_restore_their_artifacts() {
    let write = std::env::var_os("CALQUE_WRITE_PACKED_FIXTURE").is_some();
    let mut cases = Vec::new();
    for (name, artifact) in artifacts() {
        let packed = pack(&artifact).unwrap();
        assert!(packed.body.len() <= artifact.len(), "{name}");
        assert_eq!(
            unpack(&packed.body, artifact.len() as u64).unwrap(),
            artifact,
            "{name}"
        );
        assert_eq!(packed.stored, packed.body == artifact, "{name}");

        let directory = fixtures().join(name);
        if write {
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join("artifact.bin"), &artifact).unwrap();
            fs::write(directory.join("packed.bin"), &packed.body).unwrap();
        }
        assert!(
            fs::read(directory.join("artifact.bin")).unwrap() == artifact,
            "{name}/artifact.bin is not the artifact this test generates"
        );
        assert!(
            fs::read(directory.join("packed.bin")).unwrap() == packed.body,
            "{name}/packed.bin is not the body the packer writes"
        );
        cases.push(Case {
            name,
            body: if packed.stored { "stored" } else { "packed" },
            artifact_bytes: artifact.len(),
            artifact_sha256: sha256(&artifact),
            packed_bytes: packed.body.len(),
            packed_sha256: sha256(&packed.body),
            hoisted_parts: packed.hoisted.len(),
            transforms: Transforms {
                fk: applied(&packed, Transform::Fk),
                int: applied(&packed, Transform::Int),
                sep: applied(&packed, Transform::Sep),
            },
            escapes: packed
                .columns
                .iter()
                .filter(|column| column.transform == Some(Transform::Fk))
                .filter_map(|column| column.escapes)
                .sum(),
        });
    }
    let manifest = Manifest {
        size_test_encoder: format!("brotli crate {}", locked_brotli()),
        cases,
    };
    if write {
        fs::write(
            fixtures().join("manifest.json"),
            format!("{}\n", serde_json::to_string_pretty(&manifest).unwrap()),
        )
        .unwrap();
    }
    // A bump of the brotli crate regenerates the fixture.
    let checked_in: serde_json::Value =
        serde_json::from_slice(&fs::read(fixtures().join("manifest.json")).unwrap()).unwrap();
    assert_eq!(checked_in, serde_json::to_value(&manifest).unwrap());
}

#[test]
fn the_encoder_s_fixtures_give_a_stored_body_and_a_packed_one() {
    let small = emit_fixture::artifact().unwrap();
    let packed = pack(&small).unwrap();
    assert!(packed.stored);
    assert_eq!(packed.body, small);
    // The size test still ran on the columns a transform fits.
    assert_eq!(
        choice(&packed, "items", "note").considered,
        Some(Transform::Sep)
    );
    assert_eq!(choice(&packed, "items", "note").transform, None);

    let keys = emit_key_fixture::artifact().unwrap();
    let packed = pack(&keys).unwrap();
    assert!(!packed.stored);
    assert!(packed.body.starts_with(b"CALQPAK1"));
    assert!(applied(&packed, Transform::Sep) > 0);
}

#[test]
fn every_kind_takes_each_transform_and_hoists_the_keys_that_follow() {
    let artifact = support::every_kind();
    let packed = pack(&artifact).unwrap();
    assert!(!packed.stored);
    // Both referenced tables follow items and links, so both keys are hoisted.
    assert_eq!(hoisted(&packed), [("owners", "id", 0), ("zones", "id", 0)]);

    let owner = choice(&packed, "items", "owner_id");
    assert_eq!(owner.transform, Some(Transform::Fk));
    assert_eq!(owner.referenced_table.as_deref(), Some("owners"));
    // 300 owners take two bytes a row, and one value is in no table.
    assert_eq!((owner.width, owner.escapes), (Some(2), Some(1)));
    assert_eq!(owner.stream_bytes, Some(200 * 2 + 16));
    let zone = choice(&packed, "items", "zone_id");
    assert_eq!(zone.transform, Some(Transform::Fk));
    assert_eq!(zone.referenced_table.as_deref(), Some("zones"));
    assert_eq!((zone.width, zone.escapes), (Some(1), Some(0)));
    assert_eq!(
        choice(&packed, "links", "zone_id").transform,
        Some(Transform::Fk)
    );

    // A frame-of-reference column, one stored in whole milliseconds with null
    // rows, and one without an encoding.
    for (table, column) in [("items", "position"), ("ticks", "at"), ("pair", "id")] {
        assert_eq!(
            choice(&packed, table, column).transform,
            Some(Transform::Int),
            "{table}.{column}"
        );
    }
    // The size test keeps a column whose stream is no smaller, and a value
    // 2^51 from zero is outside the transform.
    let jitter = choice(&packed, "ticks", "jitter");
    assert_eq!(
        (jitter.considered, jitter.transform),
        (Some(Transform::Int), None)
    );
    assert!(jitter.stream_q9_bytes >= jitter.stored_q9_bytes);
    assert_eq!(choice(&packed, "pair", "big").considered, None);

    // Text with empty values, with null rows, all null, and of an empty table.
    for (table, column) in [
        ("items", "title"),
        ("items", "note"),
        ("items", "ghost"),
        ("items", "meta"),
        ("nothing", "label"),
    ] {
        assert_eq!(
            choice(&packed, table, column).transform,
            Some(Transform::Sep),
            "{table}.{column}"
        );
    }
    for column in &packed.columns {
        if matches!(column.considered, Some(Transform::Int | Transform::Sep)) {
            assert_eq!(
                column.transform.is_some(),
                column.stream_q9_bytes < column.stored_q9_bytes,
                "{}.{}",
                column.table,
                column.column
            );
        }
    }

    // What the header says of the columns above.
    let header_length = u32::from_le_bytes(artifact[8..12].try_into().unwrap()) as usize;
    let header: serde_json::Value =
        serde_json::from_slice(&artifact[12..12 + header_length]).unwrap();
    let column = |table: usize, column: usize| &header["tables"][table]["columns"][column];
    assert_eq!(column(0, 3)["name"], "position");
    assert_eq!(column(0, 3)["encoding"]["kind"], "for");
    assert_eq!(column(4, 0)["name"], "id");
    assert!(column(4, 0)["encoding"].is_null());
    assert_eq!(column(5, 1)["name"], "at");
    assert_eq!(column(5, 1)["encoding"]["kind"], "for");
    // Both of its blocks are stored in milliseconds: scale 3.
    let (tables, payload_start) = support::parts(&artifact);
    let (headers, length) = tables[5][3];
    assert_eq!(length, 32);
    assert_eq!(artifact[payload_start + headers + 13], 3);
    assert_eq!(artifact[payload_start + headers + 16 + 13], 3);
    let kinds: std::collections::BTreeSet<&str> = packed
        .columns
        .iter()
        .map(|column| column.kind.as_str())
        .collect();
    assert_eq!(kinds.len(), 14);
}

#[test]
fn tables_smallest_first_meets_a_key_before_and_after_the_column_coded_against_it() {
    let every_kind = support::every_kind();
    let artifact = support::smallest_first(&every_kind);
    let (tables, _) = support::parts(&artifact);
    let sizes: Vec<usize> = tables
        .iter()
        .map(|parts| parts.iter().map(|&(_, length)| length).sum())
        .collect();
    // In the header's order: items, links, nothing, owners, pair, ticks,
    // zones. In the payload: nothing, pair, zones, links, owners, items, ticks.
    let order = support::smallest_first_order(&sizes);
    assert_eq!(order, [2, 4, 6, 1, 3, 0, 5]);
    let starts: Vec<usize> = order.iter().map(|&table| tables[table][0].0).collect();
    assert!(starts.is_sorted());
    assert_eq!(starts[0], 0);

    let packed = pack(&artifact).unwrap();
    assert!(!packed.stored);
    assert_eq!(
        unpack(&packed.body, artifact.len() as u64).unwrap(),
        artifact
    );
    // links precedes owners and follows zones, and items follows both.
    assert_eq!(hoisted(&packed), [("owners", "id", 0)]);
    for (table, column, referenced) in [
        ("links", "owner_id", "owners"),
        ("links", "zone_id", "zones"),
        ("items", "owner_id", "owners"),
        ("items", "zone_id", "zones"),
    ] {
        let choice = choice(&packed, table, column);
        assert_eq!(choice.transform, Some(Transform::Fk), "{table}.{column}");
        assert_eq!(choice.referenced_table.as_deref(), Some(referenced));
    }
    // The order of the payload changes no column's choice.
    let in_schema_order = pack(&every_kind).unwrap();
    assert_eq!(packed.columns, in_schema_order.columns);
}

#[test]
fn an_artifact_with_nothing_to_pack_is_its_own_body() {
    let artifact = support::stored();
    let packed = pack(&artifact).unwrap();
    assert!(packed.stored);
    assert_eq!(packed.body, artifact);
    assert!(packed.hoisted.is_empty());
    assert!(packed
        .columns
        .iter()
        .all(|column| column.considered.is_none()));
    assert_eq!(
        unpack(&packed.body, artifact.len() as u64).unwrap(),
        artifact
    );
}
