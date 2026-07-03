//! Page-channel fallback and restore fast-path tests.

#![cfg(target_os = "linux")]

use std::{
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::fs::PermissionsExt,
    },
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use nix::sys::socket::{
    accept, bind, listen, socket, AddressFamily, Backlog, SockFlag, SockType, UnixAddr,
};
use tempfile::TempDir;

use snapstore_client::{error::ClientError, transport::Transport, SnapstoreClient};
use snapstore_manifest::DeviceBlob;
use snapstore_server::{
    build_server::{serve_for_tests_with_metrics, ServerHandle},
    config::{PageChannelConfig, ServerConfig},
    metrics::Metrics,
};
use snapstore_types::{SnapshotRef, PAGE_SIZE};

struct TestServer {
    _handle: ServerHandle,
    uds_path: PathBuf,
    page_channel_path: Option<PathBuf>,
    metrics: Arc<Metrics>,
    _dir: TempDir,
}

static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

fn rand_page(seed: u64, idx: u64) -> Vec<u8> {
    let mut p = vec![0u8; PAGE_SIZE];
    let v = seed.wrapping_add(idx).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    p[0..8].copy_from_slice(&v.to_le_bytes());
    p[8..16].copy_from_slice(&seed.to_le_bytes());
    p[16..24].copy_from_slice(&idx.to_le_bytes());
    p
}

fn full_pages(n: usize, seed: u64) -> Vec<(u64, Vec<u8>)> {
    (0..n)
        .map(|i| (i as u64, rand_page(seed, i as u64)))
        .collect()
}

fn indexed_pages(seed: u64, indexes: &[u64]) -> Vec<(u64, Vec<u8>)> {
    indexes
        .iter()
        .map(|idx| (*idx, rand_page(seed, *idx)))
        .collect()
}

async fn start_server(page_channel: bool, corrupt_cross_check: bool) -> TestServer {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();
    let page_channel_path = page_channel.then(|| data_root.join("pages.sock"));

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(Metrics::new(&registry));

    let config = ServerConfig {
        data_root: data_root.clone(),
        grpc_tcp_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_uds_path: Some(data_root.join("snapstore.sock")),
        page_channel_path: page_channel_path.clone(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: PageChannelConfig {
            ingest_queue_pages: None,
            corrupt_cross_check_for_test: corrupt_cross_check.then_some(true),
        },
    };

    let (handle, uds_path) = serve_for_tests_with_metrics(config, Arc::clone(&metrics), registry)
        .await
        .expect("serve_for_tests_with_metrics");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    TestServer {
        _handle: handle,
        uds_path,
        page_channel_path,
        metrics,
        _dir: dir,
    }
}

async fn grpc_client(server: &TestServer) -> SnapstoreClient {
    SnapstoreClient::connect(Transport::Uds(server.uds_path.clone()))
        .await
        .expect("connect gRPC UDS client")
}

async fn channel_client(server: &TestServer) -> SnapstoreClient {
    SnapstoreClient::connect(Transport::Auto {
        uds_path: server.uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(),
        page_channel_path: server.page_channel_path.clone(),
    })
    .await
    .expect("connect Auto client")
}

async fn store_snapshot(
    client: &SnapstoreClient,
    parent: Option<&SnapshotRef>,
    guest_pages: u64,
    pages: Vec<(u64, Vec<u8>)>,
) -> SnapshotRef {
    client
        .put_snapshot_from_parts(parent, guest_pages * PAGE_SIZE as u64, pages, empty_blob())
        .await
        .expect("put_snapshot_from_parts")
}

fn get_batches(metrics: &Metrics) -> f64 {
    metrics
        .page_channel_batches
        .with_label_values(&["get"])
        .get()
}

fn assert_corrupt_contains(err: ClientError, expected: &str) {
    match err {
        ClientError::CorruptPayload(detail) => assert!(
            detail.context.contains(expected),
            "context {:?} did not contain {:?}",
            detail.context,
            expected
        ),
        other => panic!("expected CorruptPayload, got {other:?}"),
    }
}

fn status_code(err: &ClientError) -> Option<tonic::Code> {
    match err {
        ClientError::Status(status) => Some(status.code()),
        _ => None,
    }
}

fn start_closing_seqpacket_server(path: PathBuf) -> std::thread::JoinHandle<()> {
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let sock = socket(
        AddressFamily::Unix,
        SockType::SeqPacket,
        SockFlag::SOCK_CLOEXEC,
        None,
    )
    .expect("fake page-channel socket");
    let addr = UnixAddr::new(path.as_path()).expect("fake page-channel addr");
    bind(sock.as_raw_fd(), &addr).expect("fake page-channel bind");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660))
        .expect("fake page-channel permissions");
    listen(&sock, Backlog::new(1).unwrap()).expect("fake page-channel listen");

    std::thread::spawn(move || {
        if let Ok(raw_fd) = accept(sock.as_raw_fd()) {
            // SAFETY: accept returned a fresh fd owned by this thread.
            let conn = unsafe { OwnedFd::from_raw_fd(raw_fd) };
            drop(conn);
        }
        drop(sock);
    })
}

#[tokio::test]
async fn corrupt_cross_check_surfaces_mismatch() {
    let _guard = test_guard().await;
    let server = start_server(true, true).await;
    let client = channel_client(&server).await;

    let pages: Vec<(u64, Vec<u8>)> = (0..4)
        .map(|i| (i, rand_page(0xBAD_C0DE, i)))
        .collect();

    let result = client.put_pages(pages).await;
    assert!(result.is_err(), "must fail with batch_blake3 mismatch");
    match result.unwrap_err() {
        ClientError::BatchBlake3Mismatch { .. } => {}
        other => panic!("expected BatchBlake3Mismatch, got: {other:?}"),
    }
}

#[tokio::test]
async fn fallback_missing_socket_uses_grpc_for_put_pages() {
    let _guard = test_guard().await;
    let server = start_server(false, false).await;
    let missing_dir = TempDir::new().unwrap();
    let client = SnapstoreClient::connect(Transport::Auto {
        uds_path: server.uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(),
        page_channel_path: Some(missing_dir.path().join("nonexistent.sock")),
    })
    .await
    .unwrap();

    let pages = full_pages(16, 0x1234_5678);

    let (new_count, dedup_count) = client.put_pages(pages.clone()).await.unwrap();
    assert_eq!(new_count, 16);
    assert_eq!(dedup_count, 0);

    let (new2, dedup2) = client.put_pages(pages).await.unwrap();
    assert_eq!(new2, 0);
    assert_eq!(dedup2, 16);
}

#[tokio::test]
async fn live_channel_put_pages_identical_to_grpc() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let channel_client = channel_client(&server).await;
    let grpc_client = grpc_client(&server).await;

    let pages = full_pages(32, 0xABCD_EF01);

    let (ch_new, ch_dedup) = channel_client.put_pages(pages.clone()).await.unwrap();
    assert_eq!(ch_new, 32);
    assert_eq!(ch_dedup, 0);

    let hashes: Vec<snapstore_types::PageHash> = pages
        .iter()
        .map(|(_, d)| snapstore_types::PageHash::from_bytes(*blake3::hash(d).as_bytes()))
        .collect();
    let present = grpc_client.has_pages(hashes).await.unwrap();
    assert!(present.iter().all(|&p| p));

    let (grpc_new, grpc_dedup) = grpc_client.put_pages(pages).await.unwrap();
    assert_eq!(grpc_new, 0);
    assert_eq!(grpc_dedup, 32);
}

#[tokio::test]
async fn live_resolve_pages_uses_get_batch_and_matches_grpc() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let grpc = grpc_client(&server).await;
    let channel = channel_client(&server).await;

    let snap_ref = store_snapshot(&grpc, None, 16, full_pages(16, 0xCAFE)).await;
    let expected = grpc
        .resolve_pages(snap_ref.clone(), None, false)
        .await
        .expect("grpc resolve");

    let before = get_batches(&server.metrics);
    let actual = channel
        .resolve_pages(snap_ref, None, false)
        .await
        .expect("channel resolve");
    assert_eq!(actual, expected);
    assert!(
        get_batches(&server.metrics) > before,
        "GET_BATCH metric must increment"
    );
}

#[tokio::test]
async fn resolve_pages_hashes_only_does_not_use_get_batch() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let grpc = grpc_client(&server).await;
    let channel = channel_client(&server).await;

    let snap_ref = store_snapshot(&grpc, None, 8, full_pages(8, 0xABAB)).await;
    let before = get_batches(&server.metrics);
    let resolved = channel
        .resolve_pages(snap_ref, None, true)
        .await
        .expect("hashes-only resolve");

    assert_eq!(get_batches(&server.metrics), before);
    assert!(!resolved.is_empty());
    assert!(resolved.iter().all(|(_, _, payload)| payload.is_none()));
}

#[tokio::test]
async fn resolve_pages_missing_socket_falls_back_to_grpc_payloads() {
    let _guard = test_guard().await;
    let server = start_server(false, false).await;
    let missing_dir = TempDir::new().unwrap();
    let grpc = grpc_client(&server).await;
    let fallback = SnapstoreClient::connect(Transport::Auto {
        uds_path: server.uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(),
        page_channel_path: Some(missing_dir.path().join("missing-pages.sock")),
    })
    .await
    .expect("connect fallback client");

    let snap_ref = store_snapshot(&grpc, None, 12, full_pages(12, 0x5151)).await;
    let expected = grpc
        .resolve_pages(snap_ref.clone(), None, false)
        .await
        .expect("grpc resolve");
    let actual = fallback
        .resolve_pages(snap_ref, None, false)
        .await
        .expect("fallback resolve");
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn mode_b_resolve_pages_uses_get_batch_and_preserves_order() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let grpc = grpc_client(&server).await;
    let channel = channel_client(&server).await;

    let base_ref = store_snapshot(&grpc, None, 8, full_pages(8, 0x1111)).await;
    let delta_ref = store_snapshot(&grpc, Some(&base_ref), 8, indexed_pages(0x2222, &[1, 5])).await;

    let expected = grpc
        .resolve_pages(delta_ref.clone(), Some(base_ref.clone()), false)
        .await
        .expect("grpc mode B");
    let before = get_batches(&server.metrics);
    let actual = channel
        .resolve_pages(delta_ref, Some(base_ref), false)
        .await
        .expect("channel mode B");

    assert_eq!(actual, expected);
    assert_eq!(
        actual.iter().map(|(idx, _, _)| *idx).collect::<Vec<_>>(),
        vec![1, 5]
    );
    assert!(get_batches(&server.metrics) > before);
}

#[tokio::test]
async fn duplicate_page_hashes_are_preserved_as_separate_entries() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let grpc = grpc_client(&server).await;
    let channel = channel_client(&server).await;

    let repeated = rand_page(0xD00D, 0);
    let pages = vec![
        (0, repeated.clone()),
        (1, repeated.clone()),
        (2, rand_page(0xD00D, 2)),
    ];
    let snap_ref = store_snapshot(&grpc, None, 3, pages).await;

    let before = get_batches(&server.metrics);
    let resolved = channel
        .resolve_pages(snap_ref, None, false)
        .await
        .expect("channel duplicate resolve");

    assert!(get_batches(&server.metrics) > before);
    assert_eq!(resolved.len(), 3);
    assert_eq!(resolved[0].0, 0);
    assert_eq!(resolved[1].0, 1);
    assert_eq!(resolved[0].1, resolved[1].1);
    assert_eq!(
        resolved[0].2.as_ref().unwrap(),
        resolved[1].2.as_ref().unwrap()
    );
}

#[tokio::test]
async fn empty_mode_b_returns_empty_without_get_batch() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let grpc = grpc_client(&server).await;
    let channel = channel_client(&server).await;

    let snap_ref = store_snapshot(&grpc, None, 8, full_pages(8, 0x3333)).await;
    let before = get_batches(&server.metrics);
    let resolved = channel
        .resolve_pages(snap_ref.clone(), Some(snap_ref), false)
        .await
        .expect("empty mode B");

    assert!(resolved.is_empty());
    assert_eq!(get_batches(&server.metrics), before);
}

#[tokio::test]
async fn get_batch_not_found_after_resolve_hashes_is_consistency_error() {
    let _guard = test_guard().await;
    let grpc_server = start_server(false, false).await;
    let empty_page_channel_server = start_server(true, false).await;
    let grpc = grpc_client(&grpc_server).await;

    let snap_ref = store_snapshot(&grpc, None, 4, full_pages(4, 0x4444)).await;
    let mixed = SnapstoreClient::connect(Transport::Auto {
        uds_path: grpc_server.uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(),
        page_channel_path: empty_page_channel_server.page_channel_path.clone(),
    })
    .await
    .expect("connect mixed client");

    let err = mixed
        .resolve_pages(snap_ref, None, false)
        .await
        .expect_err("NotFound must not fall back to gRPC");
    assert_corrupt_contains(err, "NotFound");
}

#[tokio::test]
async fn connected_page_channel_close_falls_back_to_grpc_payloads() {
    let _guard = test_guard().await;
    let server = start_server(false, false).await;
    let grpc = grpc_client(&server).await;
    let snap_ref = store_snapshot(&grpc, None, 6, full_pages(6, 0x5555)).await;
    let expected = grpc
        .resolve_pages(snap_ref.clone(), None, false)
        .await
        .expect("grpc resolve");

    let fake_dir = TempDir::new().unwrap();
    let fake_path = fake_dir.path().join("closing-pages.sock");
    let fake_thread = start_closing_seqpacket_server(fake_path.clone());
    let fallback = SnapstoreClient::connect(Transport::Auto {
        uds_path: server.uds_path.clone(),
        tcp_addr: "http://127.0.0.1:1".into(),
        page_channel_path: Some(fake_path),
    })
    .await
    .expect("connect fallback client");

    let actual = fallback
        .resolve_pages(snap_ref, None, false)
        .await
        .expect("closed page channel should fall back");
    assert_eq!(actual, expected);
    fake_thread.join().expect("fake page-channel thread");
}

#[tokio::test(flavor = "multi_thread")]
async fn blocking_client_inherits_get_batch_fast_path() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let grpc = grpc_client(&server).await;
    let snap_ref = store_snapshot(&grpc, None, 10, full_pages(10, 0x6666)).await;
    let expected = grpc
        .resolve_pages(snap_ref.clone(), None, false)
        .await
        .expect("grpc resolve");

    let before = get_batches(&server.metrics);
    let uds_path = server.uds_path.clone();
    let page_channel_path = server.page_channel_path.clone();
    let actual = tokio::task::spawn_blocking(move || {
        let client = snapstore_client::blocking::SnapstoreClient::connect(Transport::Auto {
            uds_path,
            tcp_addr: "http://127.0.0.1:1".into(),
            page_channel_path,
        })
        .expect("blocking connect");
        client
            .resolve_pages(snap_ref, None, false)
            .expect("blocking resolve")
    })
    .await
    .expect("spawn_blocking");

    assert_eq!(actual, expected);
    assert!(get_batches(&server.metrics) > before);
}

#[tokio::test]
async fn live_channel_preserves_grpc_error_before_get_batch() {
    let _guard = test_guard().await;
    let server = start_server(true, false).await;
    let grpc = grpc_client(&server).await;
    let channel = channel_client(&server).await;

    let snap_ref = store_snapshot(&grpc, None, 4, full_pages(4, 0x7777)).await;
    let bad_baseline = SnapshotRef::from_bytes([0x42; 32]);
    let grpc_err = grpc
        .resolve_pages(snap_ref.clone(), Some(bad_baseline.clone()), false)
        .await
        .expect_err("grpc invalid baseline");

    let before = get_batches(&server.metrics);
    let channel_err = channel
        .resolve_pages(snap_ref, Some(bad_baseline), false)
        .await
        .expect_err("channel invalid baseline");

    assert_eq!(status_code(&channel_err), status_code(&grpc_err));
    assert_eq!(get_batches(&server.metrics), before);
}
