use std::{env, error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let spec_dir = manifest_dir.join("../../../spec");
    let protos = [
        spec_dir.join("change_stream.proto"),
        spec_dir.join("mutation.proto"),
    ];

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.compile_protos(&protos, &[spec_dir])?;

    Ok(())
}
