fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
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
