//! Transport selection and channel construction.
//!
//! Supports three modes:
//!
//! - `Uds(path)` — Unix-domain socket gRPC.
//! - `Tcp(addr)` — TCP gRPC.
//! - `Auto { uds_path, tcp_addr, page_channel_path }` — try UDS first; if the
//!   socket file is absent or the initial connect probe fails, fall back to TCP.
//!   The `page_channel_path` slot is reserved for the M5 page-channel arm
//!   (added in WI3) and is currently unused.

use std::path::PathBuf;

use tonic::transport::{Channel, Endpoint};

use crate::error::ClientError;

/// Transport configuration.
///
/// ## Auto mode
///
/// `Auto` attempts to connect over UDS first:
/// 1. Checks that the socket file exists at `uds_path`.
/// 2. Attempts a short connect probe.
/// 3. On any failure falls back to `tcp_addr`.
///
/// The `page_channel_path` field is present **now** so that WI3 only needs to
/// add the arm; until then its presence is documented and it is ignored at
/// runtime.
#[derive(Clone, Debug)]
pub enum Transport {
    /// UDS if the socket exists and connects, else TCP. `page_channel_path` is
    /// reserved for the M5 page-channel arm (WI3 adds the active code path).
    Auto {
        uds_path: PathBuf,
        tcp_addr: String,
        /// Reserved for WI3 (page-channel fast path). Currently unused.
        page_channel_path: Option<PathBuf>,
    },
    /// Explicit Unix-domain socket.
    Uds(PathBuf),
    /// Explicit TCP endpoint, e.g. `"http://127.0.0.1:7410"`.
    Tcp(String),
}

impl Transport {
    /// Establish a tonic `Channel` according to the transport configuration.
    pub async fn connect(&self) -> Result<Channel, ClientError> {
        match self {
            Transport::Uds(path) => connect_uds(path).await,
            Transport::Tcp(addr) => connect_tcp(addr).await,
            Transport::Auto {
                uds_path,
                tcp_addr,
                page_channel_path: _,
            } => {
                if uds_path.exists() {
                    match connect_uds(uds_path).await {
                        Ok(ch) => {
                            tracing::debug!(
                                path = %uds_path.display(),
                                "Auto: connected via UDS"
                            );
                            return Ok(ch);
                        }
                        Err(e) => {
                            tracing::debug!(
                                path = %uds_path.display(),
                                err = %e,
                                "Auto: UDS connect failed, falling back to TCP"
                            );
                        }
                    }
                } else {
                    tracing::debug!(
                        path = %uds_path.display(),
                        "Auto: UDS socket file absent, using TCP"
                    );
                }
                connect_tcp(tcp_addr).await
            }
        }
    }
}

/// Connect to a Unix-domain socket using the tonic 0.12 pattern:
/// `Endpoint::try_from("http://[::]:0")?.connect_with_connector(...)`.
async fn connect_uds(path: &std::path::Path) -> Result<Channel, ClientError> {
    let path = path.to_owned();
    let endpoint = Endpoint::try_from("http://[::]:0")
        .map_err(|e| ClientError::Transport(format!("bad endpoint URI: {e}")))?;

    endpoint
        .connect_with_connector(tower::service_fn(move |_uri: tonic::transport::Uri| {
            let p = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(&p).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|e| ClientError::Transport(format!("UDS connect error: {e}")))
}

/// Connect to a TCP gRPC endpoint.
async fn connect_tcp(addr: &str) -> Result<Channel, ClientError> {
    Endpoint::new(addr.to_owned())
        .map_err(|e| ClientError::Transport(format!("bad TCP endpoint: {e}")))?
        .connect()
        .await
        .map_err(|e| ClientError::Transport(format!("TCP connect error: {e}")))
}
