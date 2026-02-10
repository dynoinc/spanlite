fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_set_path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?)
        .join("proto/spanlite_descriptor_set.bin");
    println!("cargo:rerun-if-changed=proto/spanlite_descriptor_set.bin");

    tonic_prost_build::configure()
        .build_server(false)
        .file_descriptor_set_path(&descriptor_set_path)
        .skip_protoc_run()
        .compile_protos(
            &[
                "proto/google/spanner/v1/spanner.proto",
                "proto/google/rpc/status.proto",
                "proto/google/rpc/error_details.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}
