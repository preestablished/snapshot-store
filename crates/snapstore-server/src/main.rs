// `forbid` would break tonic codegen includes; `deny` is enforced in lib.rs.
#![deny(unsafe_code)]

use snapstore_server::{build_server, config::load_config, metrics::Metrics};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── CLI: --config <path> ──────────────────────────────────────────────────
    let mut config_path: PathBuf = PathBuf::from("./config.toml");
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            config_path = PathBuf::from_str(&args[i + 1])?;
            i += 2;
        } else {
            i += 1;
        }
    }

    // ── Tracing (JSON to stderr) ──────────────────────────────────────────────
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // ── Config ────────────────────────────────────────────────────────────────
    let config = load_config(&config_path)?;
    tracing::info!(
        config_path = %config_path.display(),
        data_root = %config.data_root.display(),
        grpc_tcp = %config.grpc_tcp_addr,
        http = %config.http_addr,
        "snapstore-server starting"
    );

    // ── Metrics ───────────────────────────────────────────────────────────────
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(Metrics::new(&registry));

    // ── Startup + serve ───────────────────────────────────────────────────────
    build_server::run(config, metrics, registry).await?;

    Ok(())
}
