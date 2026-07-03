//! Integration tests for the SEQPACKET page channel (WI2 + WI3 server half).
//!
//! Tests:
//! 1. PUT/GET round-trip with random pages at several sizes.
//! 2. Dedup overlap: batches with identical pages yield correct counts.
//! 3. Unsealed-fd rejection → ERROR INVALID.
//! 4. FD-leak audit: 50 batches + error paths; fd count returns to baseline.
//! 5. OVERLOAD path with tiny ingest_queue_pages.
//! 6. Killed-client-mid-batch fd audit.

#![cfg(target_os = "linux")]

use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::path::PathBuf;

use nix::sys::memfd::{memfd_create, MemFdCreateFlag};
use tempfile::TempDir;

use snapstore_localpath::client::PageChannelClient;
use snapstore_localpath::linux::{create_sealed_put_memfd, recv_datagram, send_datagram};
use snapstore_localpath::proto::{
    decode_error, decode_hdr, encode_put_batch, ErrorCode, MsgKind, PUT_BATCH_MAX_PAGES,
};
use snapstore_server::{
    build_server::serve_for_tests,
    config::{PageChannelConfig, ServerConfig},
};
use snapstore_types::{PageHash, PAGE_SIZE};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn rand_page(seed: u64, idx: usize) -> Box<[u8; PAGE_SIZE]> {
    let mut p = Box::new([0u8; PAGE_SIZE]);
    let v = seed
        .wrapping_add(idx as u64)
        .wrapping_mul(0x9e3779b97f4a7c15);
    p[0..8].copy_from_slice(&v.to_le_bytes());
    p[8..16].copy_from_slice(&seed.to_le_bytes());
    p
}

/// Start a test server with the page channel enabled. Returns
/// `(handle, uds_path, pc_path, tempdir)`.
async fn start_server_with_pc(
    ingest_queue_pages: Option<u32>,
    corrupt_cross_check: bool,
) -> (
    snapstore_server::build_server::ServerHandle,
    PathBuf,
    PathBuf,
    TempDir,
) {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();
    let pc_path = data_root.join("pages.sock");

    let config = ServerConfig {
        data_root: data_root.clone(),
        grpc_tcp_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_uds_path: Some(data_root.join("snapstore.sock")),
        page_channel_path: Some(pc_path.clone()),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: PageChannelConfig {
            ingest_queue_pages,
            corrupt_cross_check_for_test: if corrupt_cross_check {
                Some(true)
            } else {
                None
            },
        },
        gc: Default::default(),
    };

    let (handle, uds_path) = serve_for_tests(config).await.expect("serve_for_tests");
    // Give the listener thread a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    (handle, uds_path, pc_path, dir)
}

/// Connect a PageChannelClient to the test server.
fn connect_pc(pc_path: &std::path::Path) -> PageChannelClient {
    PageChannelClient::connect(pc_path).expect("connect page channel")
}

/// Count open fds in /proc/self/fd.
fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// PUT/GET round-trip at several batch sizes; verifies bytes and dedup counts.
#[tokio::test]
async fn put_get_round_trip() {
    let (_handle, _uds_path, pc_path, _dir) = start_server_with_pc(None, false).await;

    // Batch sizes to test: small, medium, 1500 (GET_BATCH_MAX_PER_DATAGRAM),
    // large (> 1500 to exercise multi-datagram GET), 8192 (PUT_BATCH_MAX_PAGES).
    for n_pages in [1usize, 7, 100, 1500, 1501, 8192] {
        let pc = connect_pc(&pc_path);

        // Generate unique pages.
        let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..n_pages)
            .map(|i| rand_page(0xdeadbeef ^ (n_pages as u64 * 100), i))
            .collect();
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();

        // Chunk into PUT_BATCH_MAX_PAGES chunks and ingest.
        let mut total_new: u64 = 0;
        let mut total_deduped: u64 = 0;
        let all_hashes: Vec<PageHash> = page_refs
            .iter()
            .map(|p| PageHash::from_bytes(*blake3::hash(*p).as_bytes()))
            .collect();

        for chunk in page_refs.chunks(PUT_BATCH_MAX_PAGES as usize) {
            let outcome = pc.put_batch(chunk).expect("put_batch");
            total_new += outcome.pages_new as u64;
            total_deduped += outcome.pages_deduped as u64;
        }

        assert_eq!(
            total_new, n_pages as u64,
            "all pages should be new: n={n_pages}"
        );
        assert_eq!(
            total_deduped, 0,
            "no dedup expected on first PUT: n={n_pages}"
        );

        // GET round-trip: retrieve every page, check content.
        let reqs: Vec<(PageHash, u64)> = all_hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (*h, i as u64))
            .collect();

        // PageChannelClient::get_batch handles multi-datagram automatically.
        let got = pc.get_batch(&reqs).expect("get_batch");
        assert_eq!(got.len(), n_pages, "expected all pages back: n={n_pages}");
        // Sort by dst_slot to match input order.
        let mut got = got;
        got.sort_by_key(|(slot, _)| *slot);
        for (i, (slot, bytes)) in got.iter().enumerate() {
            assert_eq!(*slot, i as u64);
            assert_eq!(
                bytes.as_slice(),
                pages[i].as_ref(),
                "page {i} content mismatch: n={n_pages}"
            );
        }

        // PUT same pages again → all dedup.
        let mut second_new: u64 = 0;
        let mut second_dedup: u64 = 0;
        for chunk in page_refs.chunks(PUT_BATCH_MAX_PAGES as usize) {
            let outcome = pc.put_batch(chunk).expect("put_batch second");
            second_new += outcome.pages_new as u64;
            second_dedup += outcome.pages_deduped as u64;
        }
        assert_eq!(second_new, 0, "all pages dedup on second PUT: n={n_pages}");
        assert_eq!(
            second_dedup, n_pages as u64,
            "all pages counted as dedup: n={n_pages}"
        );
    }
}

/// Mixed-dedup batch: send half-new, half-duplicate pages.
#[tokio::test]
async fn dedup_overlap() {
    let (_handle, _uds_path, pc_path, _dir) = start_server_with_pc(None, false).await;
    let pc = connect_pc(&pc_path);

    let pages_a: Vec<Box<[u8; PAGE_SIZE]>> = (0..8).map(|i| rand_page(0xAABB, i)).collect();
    let refs_a: Vec<&[u8; PAGE_SIZE]> = pages_a.iter().map(|p| p.as_ref()).collect();

    // First PUT: all new.
    let out1 = pc.put_batch(&refs_a).unwrap();
    assert_eq!(out1.pages_new, 8);
    assert_eq!(out1.pages_deduped, 0);

    // Mix: 4 from pages_a (dup) + 4 new.
    let pages_b: Vec<Box<[u8; PAGE_SIZE]>> = (0..4).map(|i| rand_page(0xCCDD, i)).collect();
    let mut mixed: Vec<&[u8; PAGE_SIZE]> = refs_a[0..4].to_vec();
    mixed.extend(pages_b.iter().map(|p| p.as_ref()));

    let out2 = pc.put_batch(&mixed).unwrap();
    assert_eq!(out2.pages_new, 4, "4 new pages");
    assert_eq!(out2.pages_deduped, 4, "4 dedup pages");
}

/// Send a PUT_BATCH with an UNSEALED memfd — server must reply ERROR INVALID.
#[tokio::test]
async fn unsealed_fd_rejected() {
    let (_handle, _uds_path, pc_path, _dir) = start_server_with_pc(None, false).await;

    // Connect raw SEQPACKET socket (bypass PageChannelClient to send bad fd).
    use nix::sys::socket::{connect, socket, AddressFamily, SockFlag, SockType, UnixAddr};
    let sock = socket(
        AddressFamily::Unix,
        SockType::SeqPacket,
        SockFlag::SOCK_CLOEXEC,
        None,
    )
    .unwrap();
    let addr = UnixAddr::new(&pc_path).unwrap();
    connect(sock.as_raw_fd(), &addr).unwrap();

    // Create an UNSEALED memfd with one page.
    let fd = memfd_create(
        c"test-unsealed",
        MemFdCreateFlag::MFD_CLOEXEC | MemFdCreateFlag::MFD_ALLOW_SEALING,
    )
    .unwrap();
    let file = std::fs::File::from(fd);
    file.set_len(PAGE_SIZE as u64).unwrap();
    let fd: OwnedFd = OwnedFd::from(file);
    // Intentionally do NOT seal.

    let wire = encode_put_batch(1, 1);
    send_datagram(sock.as_fd(), &wire, Some(fd.as_fd())).unwrap();

    let dgram = recv_datagram(sock.as_fd()).unwrap().unwrap();
    let (hdr, body) = decode_hdr(&dgram.bytes).unwrap();
    assert_eq!(hdr.msg, MsgKind::Error, "expected ERROR response");
    let err = decode_error(&hdr, body).unwrap();
    assert_eq!(
        err.code,
        ErrorCode::Invalid,
        "expected INVALID for unsealed fd"
    );
}

/// Unknown-hash GET → server replies ERROR NOT_FOUND.
#[tokio::test]
async fn get_unknown_hash() {
    let (_handle, _uds_path, pc_path, _dir) = start_server_with_pc(None, false).await;
    let pc = connect_pc(&pc_path);

    let fake_hash = PageHash::from_bytes([0xde; 32]);
    let result = pc.get_batch(&[(fake_hash, 0)]);
    assert!(result.is_err());
    match result.unwrap_err() {
        snapstore_localpath::ChannelError::Peer {
            code: ErrorCode::NotFound,
            ..
        } => {}
        other => panic!("expected NotFound, got: {other:?}"),
    }
}

/// OVERLOAD path: send a 512-page batch against a server with `ingest_queue_pages = 64`.
#[tokio::test]
async fn overload_backpressure() {
    // 64 page budget; 512 page batch must trigger OVERLOAD.
    let (_handle, _uds_path, pc_path, _dir) = start_server_with_pc(Some(64), false).await;
    let pc = connect_pc(&pc_path);

    let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..512).map(|i| rand_page(0xFAFBFCFD, i)).collect();
    let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();

    let result = pc.put_batch(&refs);
    assert!(result.is_err());
    match result.unwrap_err() {
        snapstore_localpath::ChannelError::Peer {
            code: ErrorCode::Overload,
            ..
        } => {}
        other => panic!("expected Overload, got: {other:?}"),
    }
}

/// FD-leak audit: run 50 batches including error paths.
///
/// Uses a delta between "before the batches" and "after cleanup" rather than
/// an absolute baseline, so the test is robust against parallel test execution
/// where other tests may have fds open.
#[tokio::test]
async fn fd_leak_audit() {
    let (_handle, _uds_path, pc_path, _dir) = start_server_with_pc(None, false).await;

    // Small pause to let server startup fd churn settle.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let before = open_fd_count();

    for round in 0..50usize {
        let pc = connect_pc(&pc_path);

        // Alternate between valid PUT, valid GET, unsealed PUT (error path).
        if round % 3 == 2 {
            // Trigger an error: request a non-existent page.
            let _ = pc.get_batch(&[(PageHash::from_bytes([0xee; 32]), 0)]);
        } else {
            // Normal round-trip.
            let n = 4 + (round % 8);
            let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..n)
                .map(|i| rand_page(round as u64 * 100 + 0x1234, i))
                .collect();
            let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
            let _ = pc.put_batch(&refs);
            let hashes: Vec<PageHash> = pages
                .iter()
                .map(|p| PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()))
                .collect();
            let reqs: Vec<(PageHash, u64)> = hashes
                .iter()
                .enumerate()
                .map(|(i, h)| (*h, i as u64))
                .collect();
            let _ = pc.get_batch(&reqs);
        }
        // pc drops here — connection closed.
    }

    // Retry for up to 2 seconds for server connection threads to finish.
    let mut after = open_fd_count();
    for _ in 0..20 {
        let delta = (after as i64) - (before as i64);
        if delta <= 8 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        after = open_fd_count();
    }

    let delta = (after as i64) - (before as i64);
    assert!(
        delta <= 8,
        "fd leak detected: before={before}, after={after}, delta={delta}"
    );
}

/// Killed-client-mid-batch: connect, send PUT_BATCH header+fd, then drop the
/// socket without receiving a reply. Repeat 10 times; verify no fd leak.
#[tokio::test]
async fn killed_client_no_fd_leak() {
    let (_handle, _uds_path, pc_path, _dir) = start_server_with_pc(None, false).await;

    let baseline = open_fd_count();

    for i in 0..10usize {
        // Connect raw socket.
        use nix::sys::socket::{connect, socket, AddressFamily, SockFlag, SockType, UnixAddr};
        let sock = socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::SOCK_CLOEXEC,
            None,
        )
        .unwrap();
        let addr = UnixAddr::new(&pc_path).unwrap();
        connect(sock.as_raw_fd(), &addr).unwrap();

        // Create a valid sealed memfd with 1 page.
        let page = rand_page(0xABCD, i);
        let fd = create_sealed_put_memfd(&[page.as_ref()]).unwrap();

        let wire = encode_put_batch(i as u64, 1);
        send_datagram(sock.as_fd(), &wire, Some(fd.as_fd())).unwrap();
        // Drop fd (our copy); the server has received it via SCM_RIGHTS.
        drop(fd);
        // Drop sock without reading reply → server gets EPIPE / EOF.
        drop(sock);
    }

    // Give server connection threads enough time to finish processing and exit.
    // Each thread reads the page, ingests, tries to reply (fails because client
    // dropped), then exits and closes the fd.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut after = 0;
    for _ in 0..20 {
        after = open_fd_count();
        if after <= baseline + 4 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        after <= baseline + 4,
        "fd leak after killed-client: baseline={baseline}, after={after}"
    );
}
