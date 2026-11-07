//! The checked-in packed-brotli-large-window object, which the client's
//! decoder worker decodes and unpacks in `columnar-decoder-worker.test.ts`. It
//! is what a publication makes of
//! `spec/fixtures/packed-artifact/every-kind/artifact.bin`.
//!
//! Regenerate after an intended packer or encoder change:
//! `CALQUE_WRITE_PACKED_BROTLI_FIXTURE=1 cargo test -p snapshot-pipeline --test packed_brotli_fixture`

use std::{fs, io::Read, path::PathBuf};

use artifact_contract::{PackedParameters, RepresentationEncoding, PACKED_BODY_VERSION};
use direct_columnar::pack::unpack;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use snapshot_pipeline::{
    encode_packed_brotli_large_window_controlled, pack_artifact_controlled, PreparedRepresentation,
    BROTLI_QUALITY,
};
use tokio_util::sync::CancellationToken;

const ARTIFACT: &str = "packed-artifact/every-kind/artifact.bin";
const BODY: &str = "packed-artifact/every-kind/packed.bin";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn fixtures() -> PathBuf {
    root().join("spec/fixtures")
}

fn locked_brotli() -> String {
    let lock = fs::read_to_string(root().join("Cargo.lock")).unwrap();
    let after = lock
        .split("name = \"brotli\"\nversion = \"")
        .nth(1)
        .expect("Cargo.lock locks the brotli crate");
    after[..after.find('"').unwrap()].to_owned()
}

/// The object a publication makes of `artifact`, and the directory that
/// holds it.
fn published(artifact: &[u8]) -> (tempfile::TempDir, PreparedRepresentation) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("decoded");
    fs::write(&path, artifact).unwrap();
    let length = artifact.len() as u64;
    let cancellation = CancellationToken::new();
    let packed = pack_artifact_controlled(&path, length, &cancellation)
        .unwrap()
        .expect("the packer transforms columns of this artifact");
    let (object, _chunks) = encode_packed_brotli_large_window_controlled(
        &packed,
        length,
        &hex::encode(Sha256::digest(artifact)),
        length,
        &cancellation,
    )
    .unwrap();
    (directory, object)
}

fn write_fixture(artifact: &[u8]) {
    let (_directory, object) = published(artifact);
    let manifest = object.manifest();
    assert_eq!(
        manifest.encoding,
        RepresentationEncoding::PackedBrotliLargeWindow
    );
    let target = fixtures().join("packed-brotli-large-window");
    fs::create_dir_all(&target).unwrap();
    // Written, not copied, so the file does not keep a temporary file's mode.
    fs::write(target.join("object.br"), fs::read(object.path()).unwrap()).unwrap();
    let record = json!({
        "artifact": ARTIFACT,
        "decodedBytes": artifact.len(),
        "decodedSha256": hex::encode(Sha256::digest(artifact)),
        "bodyVersion": manifest.packed.unwrap().version,
        "bodyBytes": manifest.packed.unwrap().body_bytes,
        "encodedBytes": manifest.encoded_bytes,
        "encodedSha256": manifest.encoded_sha256,
        "quality": BROTLI_QUALITY,
        "windowBits": manifest.brotli.unwrap().window_bits,
        "encoder": format!("brotli crate {}", locked_brotli()),
    });
    fs::write(
        target.join("manifest.json"),
        format!("{}\n", serde_json::to_string_pretty(&record).unwrap()),
    )
    .unwrap();
}

#[test]
fn the_checked_in_object_is_this_publication_s_and_restores_the_artifact() {
    let artifact = fs::read(fixtures().join(ARTIFACT)).unwrap();
    let body = fs::read(fixtures().join(BODY)).unwrap();
    if std::env::var_os("CALQUE_WRITE_PACKED_BROTLI_FIXTURE").is_some() {
        write_fixture(&artifact);
    }
    let target = fixtures().join("packed-brotli-large-window");
    let manifest: Value =
        serde_json::from_slice(&fs::read(target.join("manifest.json")).unwrap()).unwrap();
    let encoded = fs::read(target.join("object.br")).unwrap();
    assert_eq!(manifest["artifact"], ARTIFACT);
    assert_eq!(
        manifest["decodedBytes"].as_u64(),
        Some(artifact.len() as u64)
    );
    assert_eq!(
        manifest["decodedSha256"],
        hex::encode(Sha256::digest(&artifact))
    );
    assert_eq!(
        manifest["bodyVersion"].as_u64(),
        Some(u64::from(PACKED_BODY_VERSION))
    );
    assert_eq!(manifest["bodyBytes"].as_u64(), Some(body.len() as u64));
    assert_eq!(
        manifest["encodedBytes"].as_u64(),
        Some(encoded.len() as u64)
    );
    assert_eq!(
        manifest["encodedSha256"],
        hex::encode(Sha256::digest(&encoded))
    );
    assert_eq!(
        manifest["quality"].as_u64(),
        Some(u64::from(BROTLI_QUALITY))
    );
    assert_eq!(manifest["windowBits"].as_u64(), Some(17));
    // A bump of the encoder, of the quality or of the packer regenerates the
    // object.
    assert_eq!(
        manifest["encoder"],
        format!("brotli crate {}", locked_brotli())
    );
    assert_eq!([encoded[0], encoded[1] & 0x3f], [0x11, 17]);

    let mut decoded = Vec::new();
    brotli::Decompressor::new(encoded.as_slice(), 64 * 1024)
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(decoded, body);
    assert_eq!(unpack(&decoded, artifact.len() as u64).unwrap(), artifact);

    let (_directory, object) = published(&artifact);
    assert_eq!(fs::read(object.path()).unwrap(), encoded);
    assert_eq!(
        object.manifest().packed,
        Some(PackedParameters {
            version: PACKED_BODY_VERSION,
            body_bytes: body.len() as u64
        })
    );
}
