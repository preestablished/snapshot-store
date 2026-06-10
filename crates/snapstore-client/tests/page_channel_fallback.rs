//! Page-channel fallback and cross-check tests (WI3 client half).
//!
//! Tests:
//! 1. `corrupt_cross_check`: server flips batch_blake3 → client surfaces
//!    `ClientError::BatchBlake3Mismatch`, NOT retried, NOT fallen back.
//! 2. `fallback_missing_socket`: Auto with a non-existent page_channel_path →
//!    put_pages silently uses gRPC, results identical to gRPC-only path.
//! 3. `live_channel_identical_to_grpc`: Auto with live page channel → results
//!    identical to gRPC path (same counts + has_pages = true).

#![cfg(target_os = "linux")]

use std::path::PathBuf;

use tempfile::TempDir;

use snapstore_client::{error::ClientError, transport::Transport, SnapstoreClient};
use snapstore_server::{
    build_server::serve_for_tests,
    config::{PageChannelConfig, ServerConfig},
};
use snapstore_types::PAGE_SIZE;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn rand_page(seed: u64, idx: usize) -> Vec<u8> {
    let mut p = vec![0u8; PAGE_SIZE];
    let v = seed
        .wrapping_add(idx as u64)
        .wrapping_mul(0x9e3779b97f4a7c15);
    p[0..8].copy_from_slice(&v.to_le_bytes());
    p[8..16].copy_from_slice(&seed.to_le_bytes());
    p
}

/// Start a server. Returns `(handle, uds_path, pc_path, tempdir)`.
async fn start_server(
    pc_path: Option<PathBuf>,
    corrupt_cross_check: bool,
) -> (
    snapstore_server::build_server::ServerHandle,
    PathBuf,
    TempDir,
) {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();

    let config = ServerConfig {
        data_root: data_root.clone(),
        grpc_tcp_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_uds_path: Some(data_root.join("snapstore.sock")),
        page_channel_path: pc_path,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: PageChannelConfig {
            ingest_queue_pages: None,
            corrupt_cross_check_for_test: if corrupt_cross_check {
                Some(true)
            } else {
                None
            },
        },
    };

    let (handle, uds_path) = serve_for_tests(config).await.expect("serve_for_tests");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    (handle, uds_path, dir)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// The corrupt-cross-check path: the server flips batch_blake3 → client must
/// surface `ClientError::BatchBlake3Mismatch`, must NOT retry, must NOT fall
/// back to gRPC.
#[tokio::test]
async fn corrupt_cross_check_surfaces_mismatch() {
    let dir = TempDir::new().unwrap();
    let pc_path = dir.path().join("pages.sock");

    let (_handle, uds_path, _dir) = start_server(Some(pc_path.clone()), true).await;

    let transport = Transport::Auto {
        uds_path: uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(), // unreachable, must not be used
        page_channel_path: Some(pc_path),
    };
    let client = SnapstoreClient::connect(transport).await.unwrap();

    let pages: Vec<(u64, Vec<u8>)> = (0..4)
        .map(|i| (i as u64, rand_page(0xBAD_C0DE, i)))
        .collect();

    let result = client.put_pages(pages).await;
    assert!(result.is_err(), "must fail with batch_blake3 mismatch");
    match result.unwrap_err() {
        ClientError::BatchBlake3Mismatch { .. } => {}
        other => panic!("expected BatchBlake3Mismatch, got: {other:?}"),
    }
}

/// Fallback: Auto with a page_channel_path that doesn't exist → put_pages
/// silently uses gRPC; results must match a pure-gRPC client.
#[tokio::test]
async fn fallback_missing_socket_uses_grpc() {
    let dir = TempDir::new().unwrap();
    let nonexistent_pc = dir.path().join("nonexistent.sock");

    let (_handle, uds_path, _dir) = start_server(None, false).await;

    // Client with missing page_channel_path.
    let transport = Transport::Auto {
        uds_path: uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(),
        page_channel_path: Some(nonexistent_pc),
    };
    let client = SnapstoreClient::connect(transport).await.unwrap();

    let pages: Vec<(u64, Vec<u8>)> = (0..16)
        .map(|i| (i as u64, rand_page(0x1234_5678, i)))
        .collect();

    // Should succeed via gRPC fallback.
    let (new_count, dedup_count) = client.put_pages(pages.clone()).await.unwrap();
    assert_eq!(new_count, 16, "all pages should be new via gRPC fallback");
    assert_eq!(dedup_count, 0, "no dedup on first put");

    // Second put: all dedup.
    let (new2, dedup2) = client.put_pages(pages).await.unwrap();
    assert_eq!(new2, 0);
    assert_eq!(dedup2, 16);
}

/// Live channel: Auto with a working page_channel_path → results identical to
/// gRPC path (same counts + subsequent has_pages = true).
#[tokio::test]
async fn live_channel_identical_to_grpc() {
    let dir = TempDir::new().unwrap();
    let pc_path = dir.path().join("pages.sock");

    let (_handle, uds_path, _dir) = start_server(Some(pc_path.clone()), false).await;

    // Client via page channel.
    let transport = Transport::Auto {
        uds_path: uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(),
        page_channel_path: Some(pc_path),
    };
    let channel_client = SnapstoreClient::connect(transport).await.unwrap();

    // Client via pure gRPC (no page channel).
    let grpc_transport = Transport::Uds(uds_path.clone());
    let grpc_client = SnapstoreClient::connect(grpc_transport).await.unwrap();

    let pages: Vec<(u64, Vec<u8>)> = (0..32)
        .map(|i| (i as u64, rand_page(0xABCD_EF01, i)))
        .collect();

    // Put via channel.
    let (ch_new, ch_dedup) = channel_client.put_pages(pages.clone()).await.unwrap();
    assert_eq!(ch_new, 32);
    assert_eq!(ch_dedup, 0);

    // Verify via gRPC has_pages.
    let hashes: Vec<snapstore_types::PageHash> = pages
        .iter()
        .map(|(_, d)| snapstore_types::PageHash::from_bytes(*blake3::hash(d).as_bytes()))
        .collect();
    let present = grpc_client.has_pages(hashes).await.unwrap();
    assert!(
        present.iter().all(|&p| p),
        "all pages should be present after page-channel put"
    );

    // Now put the same pages via gRPC → all dedup.
    let (grpc_new, grpc_dedup) = grpc_client.put_pages(pages).await.unwrap();
    assert_eq!(grpc_new, 0, "all dedup via gRPC");
    assert_eq!(grpc_dedup, 32);
}
