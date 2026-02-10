check:
    protoc \
        --proto_path=proto \
        --include_imports \
        --include_source_info \
        --descriptor_set_out=proto/spanlite_descriptor_set.bin \
        proto/google/spanner/v1/spanner.proto \
        proto/google/rpc/status.proto \
        proto/google/rpc/error_details.proto

    cargo fmt --all
    cargo fix --allow-dirty --allow-staged --all-targets --all-features
    cargo clippy --fix --allow-dirty --allow-staged --all-targets --all-features -- -D warnings
    cargo check --all-targets --all-features
    cargo test --all-targets --all-features
