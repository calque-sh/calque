//! The packer as a command, for the benchmark's brotli harness.
//!
//! `pack_artifact pack <artifact file> <packed file>` writes the packed body
//! and prints what the packer did with each column. `pack_artifact unpack
//! <packed file> <decoded bytes> <artifact file>` writes the artifact a body
//! restores, which must be `<decoded bytes>` long. Each prints one JSON object
//! on success, and on failure one line on stderr with a non-zero status.

use std::{error::Error, ffi::OsString, fs, process::ExitCode};

use direct_columnar::pack::{pack, unpack, ColumnChoice, HoistedPart};
use serde::Serialize;
use sha2::{Digest, Sha256};

const USAGE: &str = "usage: pack_artifact pack <artifact file> <packed file> | \
                     unpack <packed file> <decoded bytes> <artifact file>";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PackReport {
    artifact_bytes: u64,
    artifact_sha256: String,
    packed_bytes: u64,
    packed_sha256: String,
    hoisted_parts: Vec<HoistedPart>,
    columns: Vec<ColumnChoice>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnpackReport {
    artifact_bytes: u64,
    artifact_sha256: String,
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn run(arguments: &[OsString]) -> Result<String, Box<dyn Error>> {
    let command = arguments.first().and_then(|command| command.to_str());
    match (command, &arguments[arguments.len().min(1)..]) {
        (Some("pack"), [artifact_file, packed_file]) => {
            let artifact = fs::read(artifact_file)?;
            let packed = pack(&artifact)?;
            fs::write(packed_file, &packed.body)?;
            Ok(serde_json::to_string(&PackReport {
                artifact_bytes: artifact.len() as u64,
                artifact_sha256: sha256(&artifact),
                packed_bytes: packed.body.len() as u64,
                packed_sha256: sha256(&packed.body),
                hoisted_parts: packed.hoisted,
                columns: packed.columns,
            })?)
        }
        (Some("unpack"), [packed_file, decoded_bytes, artifact_file]) => {
            let decoded_bytes: u64 = decoded_bytes
                .to_str()
                .and_then(|decoded_bytes| decoded_bytes.parse().ok())
                .ok_or("<decoded bytes> is not a byte count")?;
            let artifact = unpack(&fs::read(packed_file)?, decoded_bytes)?;
            fs::write(artifact_file, &artifact)?;
            Ok(serde_json::to_string(&UnpackReport {
                artifact_bytes: artifact.len() as u64,
                artifact_sha256: sha256(&artifact),
            })?)
        }
        _ => Err(USAGE.into()),
    }
}

fn main() -> ExitCode {
    match run(&std::env::args_os().skip(1).collect::<Vec<_>>()) {
        Ok(report) => {
            println!("{report}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("pack_artifact: {error}");
            ExitCode::FAILURE
        }
    }
}
