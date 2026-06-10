// `forbid` is too strong while tonic codegen is in the tree: include_proto
// expands to code we don't control. Manual code keeps the discipline via deny.
#![deny(unsafe_code)]
// ClientError must carry Vec<PageHash> for the MissingPages detail; boxing
// every variant would make the API more cumbersome without real benefit.
#![allow(clippy::result_large_err)]

/// Generated `determinism.snapstore.v1` types and service stubs.
///
/// Single re-export seam: when control-plane fulfils the
/// adopt-snapstore-proto-v1 request, this module body swaps to a re-export
/// of the published crate and nothing else changes (phase-2 plan, risk 2).
pub mod snapstore_proto {
    tonic::include_proto!("determinism.snapstore.v1");
}

pub mod client;
pub mod details;
pub mod error;
pub mod helpers;
pub mod retry;
pub mod transport;

/// Blocking facade over the async client.
///
/// Owns a `current_thread` tokio runtime and delegates each method to the
/// async `SnapstoreClient` via `block_on`. Intended for KVM vCPU worker loops
/// that are not tokio-native (sync-async bridge design note, decision d).
pub mod blocking;

pub use client::SnapstoreClient;
pub use error::ClientError;
pub use transport::Transport;

#[cfg(test)]
mod tests;
