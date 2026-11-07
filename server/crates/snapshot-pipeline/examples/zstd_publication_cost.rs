//! The cost of one publication's level-12 zstd encode with its verifier:
//! `zstd_publication_cost <decoded file>`, run under `/usr/bin/time -l`. It
//! prepares the file's representations as preparation does, which encodes
//! zstd with the published parameters and decodes it back against the file's
//! length, SHA-256 and chunk digests, and prints the zstd object's length and
//! how long the preparation took. The file's SHA-256 is read first and is
//! outside that time.

use std::{env, fs::File, io::Read, path::Path, process::ExitCode, time::Instant};

use artifact_contract::RepresentationEncoding;
use sha2::{Digest, Sha256};
use snapshot_pipeline::PreparedRepresentations;
use tokio_util::sync::CancellationToken;

const USAGE: &str = "usage: zstd_publication_cost <decoded file>";

fn main() -> ExitCode {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let [decoded] = arguments.as_slice() else {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    };
    match run(Path::new(decoded)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("zstd_publication_cost: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(decoded: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut hasher = Sha256::new();
    let mut file = File::open(decoded)?;
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut decoded_bytes = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        decoded_bytes += read as u64;
    }
    let decoded_sha256 = hex::encode(hasher.finalize());

    let started = Instant::now();
    let prepared = PreparedRepresentations::prepare_controlled(
        decoded,
        decoded_bytes,
        &decoded_sha256,
        decoded_bytes,
        &CancellationToken::new(),
    )?;
    let elapsed = started.elapsed();
    let zstd = prepared
        .manifests()
        .into_iter()
        .find(|manifest| manifest.encoding == RepresentationEncoding::Zstd)
        .ok_or("no zstd representation was prepared")?;
    println!(
        "decoded {decoded_bytes} bytes; zstd {} bytes, sha256 {}; prepared in {:.3} ms",
        zstd.encoded_bytes,
        zstd.encoded_sha256,
        elapsed.as_secs_f64() * 1e3,
    );
    Ok(())
}
