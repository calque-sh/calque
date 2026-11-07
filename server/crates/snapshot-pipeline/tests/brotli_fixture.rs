//! The checked-in brotli-large-window object, which the client's pinned
//! decoder module decodes in `columnar-decoder-worker.test.ts`.
//!
//! Regenerate after an intended encoder change:
//! `CALQUE_WRITE_BROTLI_FIXTURE=1 cargo test --release -p snapshot-pipeline --test brotli_fixture`

use std::{fs, io::Read, path::PathBuf};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use snapshot_pipeline::{encode_brotli_large_window_controlled, BROTLI_QUALITY};
use tokio_util::sync::CancellationToken;

const NOISE_BYTES: usize = 4096;
const FAR_BYTES: usize = 1 << 24;
const DECODED_SHA256: &str = "d632742b105e65bb19605812188613968bc26fb2dfac7c58a780626c8898138e";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn fixtures() -> PathBuf {
    root().join("spec/fixtures/brotli-large-window")
}

/// Noise, 2^24 zeros, then the same noise: the second copy is a match only a
/// window larger than a standard stream's can reach.
fn decoded() -> Vec<u8> {
    let mut state = 0x2545_f491_u32;
    let noise: Vec<u8> = (0..NOISE_BYTES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            (state & 0xff) as u8
        })
        .collect();
    let mut bytes = noise.clone();
    bytes.resize(NOISE_BYTES + FAR_BYTES, 0);
    bytes.extend_from_slice(&noise);
    bytes
}

fn locked_brotli() -> String {
    let lock = fs::read_to_string(root().join("Cargo.lock")).unwrap();
    let after = lock
        .split("name = \"brotli\"\nversion = \"")
        .nth(1)
        .expect("Cargo.lock locks the brotli crate");
    after[..after.find('"').unwrap()].to_owned()
}

fn write_fixture(decoded: &[u8]) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("decoded");
    fs::write(&path, decoded).unwrap();
    let (object, _chunks) = encode_brotli_large_window_controlled(
        &path,
        decoded.len() as u64,
        DECODED_SHA256,
        decoded.len() as u64,
        &CancellationToken::new(),
    )
    .unwrap();
    let manifest = object.manifest();
    fs::create_dir_all(fixtures()).unwrap();
    fs::copy(object.path(), fixtures().join("object.br")).unwrap();
    let record = json!({
        "decodedBytes": decoded.len(),
        "decodedSha256": DECODED_SHA256,
        "encodedBytes": manifest.encoded_bytes,
        "encodedSha256": manifest.encoded_sha256,
        "quality": BROTLI_QUALITY,
        "windowBits": manifest.brotli.unwrap().window_bits,
        "encoder": format!("brotli crate {}", locked_brotli()),
    });
    fs::write(
        fixtures().join("manifest.json"),
        format!("{}\n", serde_json::to_string_pretty(&record).unwrap()),
    )
    .unwrap();
}

#[test]
fn the_checked_in_object_is_this_encoder_s_and_reaches_past_a_standard_window() {
    let decoded = decoded();
    assert_eq!(hex::encode(Sha256::digest(&decoded)), DECODED_SHA256);
    if std::env::var_os("CALQUE_WRITE_BROTLI_FIXTURE").is_some() {
        write_fixture(&decoded);
    }
    let manifest: Value =
        serde_json::from_slice(&fs::read(fixtures().join("manifest.json")).unwrap()).unwrap();
    let encoded = fs::read(fixtures().join("object.br")).unwrap();
    assert_eq!(
        manifest["decodedBytes"].as_u64(),
        Some(decoded.len() as u64)
    );
    assert_eq!(manifest["decodedSha256"], DECODED_SHA256);
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
    assert_eq!(manifest["windowBits"].as_u64(), Some(25));
    // A bump of the encoder or of the quality regenerates the object.
    assert_eq!(
        manifest["encoder"],
        format!("brotli crate {}", locked_brotli())
    );
    assert_eq!([encoded[0], encoded[1] & 0x3f], [0x11, 25]);
    // Without a match across the zeros the stream holds both copies of the
    // noise, more than 8,000 bytes.
    assert!(encoded.len() < 6_000, "{} bytes", encoded.len());
    let mut round_trip = Vec::new();
    brotli::Decompressor::new(encoded.as_slice(), 64 * 1024)
        .read_to_end(&mut round_trip)
        .unwrap();
    assert_eq!(round_trip, decoded);
}
