//! Integration tests for snapstorectl: exercise every subcommand once against
//! a real in-process server on a temp directory + UDS.
//!
//! The test uses `#[tokio::test(flavor = "multi_thread")]` so that blocking
//! `Command::output()` calls (wrapped in `spawn_blocking`) do not starve the
//! tokio executor driving the in-process server's UDS handler.

use std::path::PathBuf;
use std::process::Command;

use snapstore_client::{
    client::SnapstoreClient as AsyncClient,
    snapstore_proto::{CreateNodeRequest, NodeStatus as ProtoNodeStatus},
    transport::Transport,
};
use snapstore_manifest::{DeviceBlob, Manifest, ManifestEntry};
use snapstore_server::{build_server::serve_for_tests, config::ServerConfig};
use snapstore_types::{PageHash, SnapshotRef};
use tempfile::TempDir;

// ── helpers ───────────────────────────────────────────────────────────────────

fn bin_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_snapstorectl"))
}

/// Spin up a test server in a fresh tempdir.
async fn start_server() -> (
    TempDir,
    PathBuf,
    snapstore_server::build_server::ServerHandle,
) {
    let dir = TempDir::new().expect("tempdir");
    let data_root = dir.path().to_owned();

    let cfg = ServerConfig {
        data_root: data_root.clone(),
        grpc_tcp_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_uds_path: Some(data_root.join("test.sock")),
        page_channel_path: None,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: Default::default(),
    };

    let (handle, uds_path) = serve_for_tests(cfg).await.expect("serve_for_tests");
    (dir, uds_path, handle)
}

/// Build a simple full manifest + upload pages, return snapshot_ref_hex, snap_ref.
async fn seed_snapshot(client: &AsyncClient, n_pages: usize) -> (String, SnapshotRef) {
    let pages: Vec<(u64, Vec<u8>)> = (0..n_pages)
        .map(|i| {
            let mut page = vec![0u8; 4096];
            page[0] = i as u8;
            page[1] = (i >> 8) as u8;
            (i as u64, page)
        })
        .collect();

    client.put_pages(pages.clone()).await.expect("put_pages");

    let entries: Vec<ManifestEntry> = pages
        .iter()
        .map(|(idx, data)| {
            let hash = blake3::hash(data);
            ManifestEntry {
                page_index: *idx,
                page_hash: PageHash::from_bytes(*hash.as_bytes()),
            }
        })
        .collect();

    let blob = DeviceBlob {
        format: 0,
        zstd: false,
        bytes: b"test-device".to_vec(),
        raw_len: 11,
    };
    let manifest = Manifest::new_full(n_pages as u64 * 4096, entries, blob).expect("manifest");
    let container = manifest.encode();
    let snap_ref = Manifest::snapshot_ref(&container);
    let snap_ref_hex: String = snap_ref
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    let stored = client
        .put_snapshot(container.clone())
        .await
        .expect("put_snapshot");
    assert_eq!(stored, snap_ref);

    (snap_ref_hex, snap_ref)
}

// ── the single integration test ────────────────────────────────────────────────

// Use multi_thread so that blocking `Command::output()` calls (wrapped in
// spawn_blocking) do not starve the tokio executor that drives the in-process
// server's UDS handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_subcommand() {
    // ── server setup ─────────────────────────────────────────────────────────
    let (dir, uds_path, handle) = start_server().await;

    let uds_str = uds_path.to_string_lossy().into_owned();
    let endpoint = format!("uds:{uds_str}");

    // Connect async client to seed data.
    let client = AsyncClient::connect(Transport::Uds(uds_path.clone()))
        .await
        .expect("async connect");

    // ── seed data ─────────────────────────────────────────────────────────────
    let (snap_ref_hex, snap_ref) = seed_snapshot(&client, 4).await;
    let exp = "test-exp";

    // Create root node.
    let root_req = CreateNodeRequest {
        experiment_id: exp.to_owned(),
        node_id: 0,
        parent_node_id: None,
        snapshot_ref: snap_ref.to_bytes().to_vec(),
        input_log_id: vec![],
        inline_input_log: vec![],
        status: ProtoNodeStatus::Frontier as i32,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: vec![],
    };
    let root_node = client
        .create_node(root_req)
        .await
        .expect("create root node");
    let root_id = root_node.node_id;

    // ── stats ─────────────────────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["stats", "--experiment", exp]).await;
    assert!(out.status.success(), "stats failed: {}", stderr(&out));

    // stats --json
    let out = ctl_async(&endpoint, &["--json", "stats"]).await;
    assert!(
        out.status.success(),
        "stats --json failed: {}",
        stderr(&out)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("stats json parse");
    assert!(parsed.get("store").is_some(), "stats json missing 'store'");

    // ── dump-manifest ─────────────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["dump-manifest", &snap_ref_hex]).await;
    assert!(
        out.status.success(),
        "dump-manifest failed: {}",
        stderr(&out)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("snapshot_ref"),
        "dump-manifest output missing snapshot_ref"
    );
    assert!(
        stdout.contains("entry_count"),
        "dump-manifest output missing entry_count"
    );

    // ── get-node ──────────────────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["get-node", exp, &root_id.to_string()]).await;
    assert!(out.status.success(), "get-node failed: {}", stderr(&out));

    // get-node --json
    let out = ctl_async(
        &endpoint,
        &["--json", "get-node", exp, &root_id.to_string()],
    )
    .await;
    assert!(
        out.status.success(),
        "get-node --json failed: {}",
        stderr(&out)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("get-node json");
    assert_eq!(parsed["node_id"], root_id);

    // ── query ─────────────────────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["query", exp, "--status", "frontier"]).await;
    assert!(out.status.success(), "query failed: {}", stderr(&out));

    let out = ctl_async(&endpoint, &["--json", "query", exp, "--limit", "10"]).await;
    assert!(
        out.status.success(),
        "query --json failed: {}",
        stderr(&out)
    );
    let nodes: serde_json::Value = serde_json::from_slice(&out.stdout).expect("query json");
    assert!(nodes.as_array().is_some(), "query json should be array");

    // ── kv put + get + delete ─────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["kv", "put", "mykey", "myvalue"]).await;
    assert!(out.status.success(), "kv put failed: {}", stderr(&out));

    let out = ctl_async(&endpoint, &["kv", "get", "mykey"]).await;
    assert!(out.status.success(), "kv get failed: {}", stderr(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("myvalue"), "kv get output missing value");

    let out = ctl_async(&endpoint, &["kv", "delete", "mykey"]).await;
    assert!(out.status.success(), "kv delete failed: {}", stderr(&out));

    // ── pin ───────────────────────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["pin", &snap_ref_hex, "--note", "test-pin"]).await;
    assert!(out.status.success(), "pin failed: {}", stderr(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("newly_pinned: true"),
        "pin should be newly pinned"
    );

    // ── unpin ─────────────────────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["unpin", &snap_ref_hex]).await;
    assert!(out.status.success(), "unpin failed: {}", stderr(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("was_pinned: true"),
        "unpin: was_pinned should be true"
    );

    // ── prune (prune a non-root node — create a child first) ──────────────────
    let (_, child_snap_ref) = seed_snapshot(&client, 4).await;
    let child_req = CreateNodeRequest {
        experiment_id: exp.to_owned(),
        node_id: 1,
        parent_node_id: Some(0),
        snapshot_ref: child_snap_ref.to_bytes().to_vec(),
        input_log_id: vec![],
        inline_input_log: vec![],
        status: ProtoNodeStatus::Frontier as i32,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: vec![],
    };
    client
        .create_node(child_req)
        .await
        .expect("create child node");

    let out = ctl_async(&endpoint, &["prune", exp, "1"]).await;
    assert!(out.status.success(), "prune failed: {}", stderr(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("pruned"), "prune should output count");

    // ── gc — must exit nonzero ─────────────────────────────────────────────────
    let out = ctl_async(&endpoint, &["gc"]).await;
    assert!(
        !out.status.success(),
        "gc should exit nonzero (unimplemented)"
    );

    // ── bench put-pages (small: 512 pages) ────────────────────────────────────
    let out = ctl_async(&endpoint, &["bench", "--pages", "512", "--msg-pages", "64"]).await;
    assert!(
        out.status.success(),
        "bench put-pages failed: {}",
        stderr(&out)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("MB/s"), "bench output should contain MB/s");

    // bench with --warm
    let out = ctl_async(
        &endpoint,
        &["bench", "--pages", "64", "--msg-pages", "16", "--warm"],
    )
    .await;
    assert!(
        out.status.success(),
        "bench --warm failed: {}",
        stderr(&out)
    );

    // ── fsck (offline, against the tempdir) ───────────────────────────────────
    let store_root = dir.path().join("store");
    let meta_db = dir.path().join("meta").join("tree.db");

    // Shut down server first so SQLite is not locked.
    handle.shutdown();
    // Give the server a brief moment to close files.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let out = ctl_no_endpoint_async(&[
        "fsck",
        "--store-root",
        store_root.to_str().unwrap(),
        "--meta-db",
        meta_db.to_str().unwrap(),
    ])
    .await;
    assert!(
        out.status.success(),
        "fsck should be clean, got stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        stderr(&out)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Should be valid JSON with no violations.
    let report: serde_json::Value = serde_json::from_str(&stdout).expect("fsck json");
    let violations = report["violations"].as_array().expect("violations array");
    assert!(
        violations.is_empty(),
        "expected no violations, got: {violations:?}"
    );

    // fsck --deep
    let out = ctl_no_endpoint_async(&[
        "fsck",
        "--store-root",
        store_root.to_str().unwrap(),
        "--meta-db",
        meta_db.to_str().unwrap(),
        "--deep",
    ])
    .await;
    assert!(out.status.success(), "fsck --deep failed: {}", stderr(&out));

    // dir is dropped at end of test — keeps TempDir alive throughout.
    drop(dir);
}

// ── command runners ────────────────────────────────────────────────────────────
//
// All use `spawn_blocking` so that waiting for the subprocess does not block
// the tokio executor thread (which also drives the in-process test server).

async fn ctl_async(endpoint: &str, args: &[&str]) -> std::process::Output {
    let bin = bin_path();
    let mut owned_args: Vec<String> = vec!["--endpoint".to_owned(), endpoint.to_owned()];
    owned_args.extend(args.iter().map(|s| s.to_string()));
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new(bin);
        for a in &owned_args {
            cmd.arg(a);
        }
        cmd.output().expect("spawn snapstorectl")
    })
    .await
    .expect("spawn_blocking")
}

async fn ctl_no_endpoint_async(args: &[&str]) -> std::process::Output {
    let bin = bin_path();
    let owned_args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new(bin);
        for a in &owned_args {
            cmd.arg(a);
        }
        cmd.output().expect("spawn snapstorectl")
    })
    .await
    .expect("spawn_blocking")
}

fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}
