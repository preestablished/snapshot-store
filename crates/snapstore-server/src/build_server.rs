//! Startup + transport binding.
//!
//! Binds the same tonic service on:
//! - TCP at `config.grpc_tcp_addr`
//! - UDS at `config.resolved_uds_path()` (mode 0660)
//!
//! Also runs an HTTP server on `config.http_addr` for `/healthz` + `/metrics`.
//! Provides `serve_for_tests` for in-process integration tests.

use std::convert::Infallible;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::UnixListener;
use tokio::signal;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use crate::config::ServerConfig;
use crate::metrics::Metrics;
use crate::service::SnapshotStoreServer;
use crate::snapstore_proto::snapshot_store_server::SnapshotStoreServer as TonicSnapshotStoreServer;
use crate::startup::{run_startup, StartupError};

use hyper::{body::Incoming, Request as HyperRequest, Response as HyperResponse};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as AutoBuilder;

// ── Handle for in-process test servers ────────────────────────────────────────

/// A running in-process server that can be shut down.
pub struct ServerHandle {
    pub shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl std::fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerHandle").finish_non_exhaustive()
    }
}

impl ServerHandle {
    /// Signal the server to shut down.  Does not wait for it to complete.
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

// ── Main entry point ──────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("startup failed: {0}")]
    Startup(#[from] StartupError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tonic transport error: {0}")]
    Tonic(#[from] tonic::transport::Error),
    #[error("config error: {0}")]
    Config(#[from] crate::config::ConfigError),
}

/// Full production entry point: run startup, bind transports, serve until SIGTERM.
pub async fn run(
    config: ServerConfig,
    metrics: Arc<Metrics>,
    registry: prometheus::Registry,
) -> Result<(), RunError> {
    let state = run_startup(&config, &metrics)?;

    let store = Arc::new(state.store);
    let meta = Arc::new(state.meta);

    // Build tonic-health reporter + checker.
    let (mut health_reporter, health_svc) = tonic_health::server::health_reporter();
    // Start NOT_SERVING — we're already past startup at this point for
    // integration tests; for production, recovery already completed.
    health_reporter
        .set_service_status(
            "determinism.snapstore.v1.SnapshotStore",
            tonic_health::ServingStatus::NotServing,
        )
        .await;

    let tcp_addr = config.grpc_tcp_addr;
    let uds_path = config.resolved_uds_path();
    let http_addr = config.http_addr;

    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // ── TCP transport ─────────────────────────────────────────────────────────
    let tcp_shutdown = shutdown_tx.subscribe();
    let svc_tcp = SnapshotStoreServer {
        store: Arc::clone(&store),
        meta: Arc::clone(&meta),
        metrics: Arc::clone(&metrics),
    };
    // Use the primary health_svc for the TCP side.
    let tcp_handle = tokio::spawn(async move {
        let mut rx = tcp_shutdown;
        Server::builder()
            .add_service(health_svc)
            .add_service(TonicSnapshotStoreServer::new(svc_tcp))
            .serve_with_shutdown(tcp_addr, async move {
                let _ = rx.recv().await;
            })
            .await
            .expect("TCP gRPC serve failed");
    });

    // ── UDS transport ─────────────────────────────────────────────────────────
    // Remove stale socket file if present.
    if uds_path.exists() {
        fs::remove_file(&uds_path)?;
    }

    let uds_listener = UnixListener::bind(&uds_path)?;
    // Set socket file mode 0660.
    fs::set_permissions(&uds_path, fs::Permissions::from_mode(0o660))?;

    let uds_stream = UnixListenerStream::new(uds_listener);
    let uds_shutdown = shutdown_tx.subscribe();
    let svc_uds = SnapshotStoreServer {
        store: Arc::clone(&store),
        meta: Arc::clone(&meta),
        metrics: Arc::clone(&metrics),
    };
    let (_, health_svc_uds) = tonic_health::server::health_reporter();
    let uds_handle = tokio::spawn(async move {
        let mut rx = uds_shutdown;
        Server::builder()
            .add_service(health_svc_uds)
            .add_service(TonicSnapshotStoreServer::new(svc_uds))
            .serve_with_incoming_shutdown(uds_stream, async move {
                let _ = rx.recv().await;
            })
            .await
            .expect("UDS gRPC serve failed");
    });

    // ── HTTP: /healthz + /metrics ─────────────────────────────────────────────
    let registry = Arc::new(registry);
    let http_shutdown = shutdown_tx.subscribe();
    let http_handle = tokio::spawn(run_http(http_addr, registry, http_shutdown));

    // Mark SERVING now that all transports are bound.
    health_reporter
        .set_service_status(
            "determinism.snapstore.v1.SnapshotStore",
            tonic_health::ServingStatus::Serving,
        )
        .await;
    tracing::info!(
        tcp = %tcp_addr,
        uds = %uds_path.display(),
        http = %http_addr,
        "snapstore-server SERVING"
    );

    // ── Graceful shutdown on SIGTERM / SIGINT ─────────────────────────────────
    tokio::select! {
        _ = signal::ctrl_c() => { tracing::info!("SIGINT received"); }
        _ = async {
            let mut sig = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
            sig.recv().await;
        } => { tracing::info!("SIGTERM received"); }
    }

    health_reporter
        .set_service_status(
            "determinism.snapstore.v1.SnapshotStore",
            tonic_health::ServingStatus::NotServing,
        )
        .await;

    let _ = shutdown_tx.send(());
    let _ = tcp_handle.await;
    let _ = uds_handle.await;
    let _ = http_handle.await;

    tracing::info!("snapstore-server shutdown complete");
    Ok(())
}

// ── In-process test helper ────────────────────────────────────────────────────

/// Spin up a full server on a UDS in a temp dir for integration tests.
///
/// Returns a `(ServerHandle, uds_path)` — the handle can be dropped to initiate
/// graceful shutdown.  The caller must keep the `TempDir` alive for the duration
/// of the test.
pub async fn serve_for_tests(config: ServerConfig) -> Result<(ServerHandle, PathBuf), RunError> {
    let metrics = Arc::new(Metrics::new(&prometheus::Registry::new()));
    let registry = prometheus::Registry::new();
    serve_for_tests_with_metrics(config, metrics, registry).await
}

/// Like `serve_for_tests` but with a caller-supplied metrics registry.
pub async fn serve_for_tests_with_metrics(
    config: ServerConfig,
    metrics: Arc<Metrics>,
    registry: prometheus::Registry,
) -> Result<(ServerHandle, PathBuf), RunError> {
    let uds_path = config.resolved_uds_path();

    let state = run_startup(&config, &metrics)?;
    let store = Arc::new(state.store);
    let meta = Arc::new(state.meta);

    let (mut health_reporter, health_svc) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status(
            "determinism.snapstore.v1.SnapshotStore",
            tonic_health::ServingStatus::NotServing,
        )
        .await;

    let svc_impl = SnapshotStoreServer {
        store,
        meta,
        metrics: Arc::clone(&metrics),
    };

    // Remove stale socket.
    if uds_path.exists() {
        fs::remove_file(&uds_path)?;
    }

    let uds_listener = UnixListener::bind(&uds_path)?;
    fs::set_permissions(&uds_path, fs::Permissions::from_mode(0o660))?;
    let uds_stream = UnixListenerStream::new(uds_listener);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // HTTP server for /healthz + /metrics.
    let registry_arc = Arc::new(registry);

    let (bcast_tx, uds_rx) = tokio::sync::broadcast::channel::<()>(1);
    let http_rx = bcast_tx.subscribe();

    tokio::spawn(async move {
        let _ = shutdown_rx.await;
        let _ = bcast_tx.send(());
    });

    let path_clone = uds_path.clone();

    let http_addr = config.http_addr;
    tokio::spawn(run_http(http_addr, registry_arc, http_rx));

    tokio::spawn(async move {
        Server::builder()
            .add_service(health_svc)
            .add_service(TonicSnapshotStoreServer::new(svc_impl))
            .serve_with_incoming_shutdown(uds_stream, async move {
                let mut rx = uds_rx;
                let _ = rx.recv().await;
            })
            .await
            .expect("test UDS serve failed");
        tracing::debug!(uds = %path_clone.display(), "test server UDS task exited");
    });

    // Mark SERVING.
    health_reporter
        .set_service_status(
            "determinism.snapstore.v1.SnapshotStore",
            tonic_health::ServingStatus::Serving,
        )
        .await;

    Ok((ServerHandle { shutdown_tx }, uds_path))
}

// ── HTTP service (hyper) ──────────────────────────────────────────────────────

async fn run_http(
    addr: std::net::SocketAddr,
    registry: Arc<prometheus::Registry>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr = %addr, err = %e, "HTTP server failed to bind");
            return;
        }
    };

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, _) = match accept {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(err = %e, "HTTP accept error");
                        continue;
                    }
                };
                let reg = Arc::clone(&registry);
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = hyper::service::service_fn(move |req: HyperRequest<Incoming>| {
                        let reg = Arc::clone(&reg);
                        async move { http_handler(req, reg).await }
                    });
                    let _ = AutoBuilder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                });
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}

async fn http_handler(
    req: HyperRequest<Incoming>,
    registry: Arc<prometheus::Registry>,
) -> Result<HyperResponse<String>, Infallible> {
    let path = req.uri().path();
    match path {
        "/healthz" => Ok(HyperResponse::builder()
            .status(200)
            .header("content-type", "text/plain")
            .body("ok\n".to_owned())
            .unwrap()),
        "/metrics" => {
            use prometheus::Encoder;
            let encoder = prometheus::TextEncoder::new();
            let mut buf = Vec::new();
            let _ = encoder.encode(&registry.gather(), &mut buf);
            Ok(HyperResponse::builder()
                .status(200)
                .header("content-type", encoder.format_type())
                .body(String::from_utf8_lossy(&buf).into_owned())
                .unwrap())
        }
        _ => Ok(HyperResponse::builder()
            .status(404)
            .body("not found\n".to_owned())
            .unwrap()),
    }
}
