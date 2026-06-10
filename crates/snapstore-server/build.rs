fn main() -> Result<(), Box<dyn std::error::Error>> {
    // No protoc on dev boxes or CI runners; use the vendored binary.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_build::configure()
        .build_server(true)
        .build_client(true) // in-process clients for tests
        .compile_protos(&["../../proto/snapshot_store.proto"], &["../../proto"])?;
    Ok(())
}
