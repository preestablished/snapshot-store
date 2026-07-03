//! Integration tests for `snapstore-server`.
//!
//! Spins the full service in-process on a UDS in a tempdir.
//! Tests: PutPages, PutSnapshot, GetSnapshot, ResolvePages, HasPages,
//! CreateNode, UpdateNodes, QueryNodes, KV CAS, Pin/Unpin/PruneSubtree/Stats,
//! TriggerGc, health, STORE_VERSION mismatch, unknown config key.

use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use snapstore_manifest::DeviceBlob;
use snapstore_server::{
    build_server::{serve_for_tests, ServerHandle},
    config::{load_config, ServerConfig},
    errors::{decode_current_generation, decode_missing_nodes, decode_missing_pages},
    snapstore_proto::{
        snapshot_store_client::SnapshotStoreClient, CreateNodeRequest, DeleteMetadataRequest,
        GetMetadataRequest, GetSnapshotRequest, NodeUpdate as ProtoNodeUpdate, PinRequest,
        PruneSubtreeRequest, PutMetadataRequest, PutPagesRequest, PutSnapshotRequest,
        QueryNodesRequest, ResolvePagesRequest, StatsRequest, TriggerGcRequest, UnpinRequest,
        UpdateNodesRequest,
    },
};
use snapstore_store::build::{build_delta_container, build_full_container};
use snapstore_types::PAGE_SIZE;

// ── Test helpers ─────────────────────────────────────────────────────────────

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

fn rand_pages(n: usize, seed: u64) -> Vec<[u8; PAGE_SIZE]> {
    (0..n)
        .map(|i| {
            let mut p = [0u8; PAGE_SIZE];
            let v = seed.wrapping_add(i as u64);
            p[0..8].copy_from_slice(&v.to_le_bytes());
            p
        })
        .collect()
}

async fn make_client(uds_path: PathBuf) -> SnapshotStoreClient<Channel> {
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_uri: tonic::transport::Uri| {
            let path = uds_path.clone();
            async move {
                let stream = UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("connect to UDS");
    SnapshotStoreClient::new(channel)
}

/// Create a tempdir, spawn a server, return (handle, client, tempdir).
async fn start_server() -> (ServerHandle, SnapshotStoreClient<Channel>, TempDir) {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();
    let grpc_tcp_addr = "127.0.0.1:0".parse().unwrap(); // unused in tests
    let http_addr = "127.0.0.1:0".parse().unwrap();

    let config = ServerConfig {
        data_root: data_root.clone(),
        grpc_tcp_addr,
        grpc_uds_path: Some(data_root.join("snapstore.sock")),
        page_channel_path: None,
        http_addr,
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: Default::default(),
        gc: Default::default(),
    };

    let (handle, uds_path) = serve_for_tests(config).await.expect("serve_for_tests");
    let client = make_client(uds_path).await;
    (handle, client, dir)
}

// ── (a) PutPages ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn put_pages_counts_and_hash() {
    let (_handle, mut client, _dir) = start_server().await;

    let pages_data: Vec<Vec<[u8; PAGE_SIZE]>> = (0..3).map(|i| rand_pages(100, i * 1000)).collect();

    // Hash pages locally in stream order.
    let mut local_hasher = blake3::Hasher::new();
    let mut all_pages_flat: Vec<Vec<u8>> = Vec::new();
    for batch in &pages_data {
        for p in batch {
            let hash = blake3::hash(p.as_ref());
            local_hasher.update(hash.as_bytes());
            all_pages_flat.push(p.to_vec());
        }
    }
    let local_batch_hash = local_hasher.finalize();

    let stream = tokio_stream::iter(pages_data.into_iter().map(|batch| PutPagesRequest {
        pages: batch.iter().map(|p| p.to_vec()).collect(),
    }));

    let resp = client.put_pages(stream).await.unwrap().into_inner();
    assert_eq!(resp.pages_new, 300, "all pages should be new");
    assert_eq!(resp.pages_deduped, 0);
    assert_eq!(
        resp.batch_blake3,
        local_batch_hash.as_bytes().to_vec(),
        "batch_blake3 must match locally computed"
    );

    // Second upload: all deduped (send in 100-page batches).
    let stream2 = tokio_stream::iter(
        all_pages_flat
            .chunks(100)
            .map(|chunk| PutPagesRequest {
                pages: chunk.to_vec(),
            })
            .collect::<Vec<_>>()
            .into_iter(),
    );
    let resp2 = client.put_pages(stream2).await.unwrap().into_inner();
    assert_eq!(resp2.pages_deduped, 300);
    assert_eq!(resp2.pages_new, 0);
}

// ── (b) PutSnapshot / GetSnapshot ────────────────────────────────────────────

#[tokio::test]
async fn put_and_get_snapshot_byte_identity() {
    let (_handle, mut client, _dir) = start_server().await;

    let pages = rand_pages(16, 42);
    let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let guest_ram = 16 * PAGE_SIZE as u64;
    let container = build_full_container(guest_ram, &page_pairs, empty_blob());

    // PutPages first.
    let stream = tokio_stream::iter(vec![PutPagesRequest {
        pages: pages.iter().map(|p| p.to_vec()).collect(),
    }]);
    client.put_pages(stream).await.unwrap();

    // PutSnapshot.
    let snap_resp = client
        .put_snapshot(PutSnapshotRequest {
            container: container.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let snap_ref = snap_resp.snapshot_ref.clone();
    assert_eq!(snap_ref.len(), 32);

    // GetSnapshot byte identity.
    let get_resp = client
        .get_snapshot(GetSnapshotRequest {
            snapshot_ref: snap_ref,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.container, container);
}

// ── (c) Missing pages → FAILED_PRECONDITION + MissingPages detail ─────────────

#[tokio::test]
async fn put_snapshot_missing_pages_detail() {
    let (_handle, mut client, _dir) = start_server().await;

    // Build container WITHOUT ingesting pages first.
    let pages = rand_pages(8, 123);
    let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container = build_full_container(8 * PAGE_SIZE as u64, &page_pairs, empty_blob());

    let err = client
        .put_snapshot(PutSnapshotRequest { container })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let detail = decode_missing_pages(&err).expect("MissingPages detail must be present");
    assert_eq!(detail.page_hashes.len(), 8);
    for h in &detail.page_hashes {
        assert_eq!(h.len(), 32);
    }
}

// ── (c) MissingNodes detail via bad UpdateNodes ───────────────────────────────

#[tokio::test]
async fn update_nodes_missing_nodes_detail() {
    let (_handle, mut client, _dir) = start_server().await;

    // Try to update a node that doesn't exist.
    let err = client
        .update_nodes(UpdateNodesRequest {
            experiment_id: "test-exp".to_string(),
            updates: vec![ProtoNodeUpdate {
                node_id: 9999,
                status: None,
                score: None,
                attrs: None,
                visit_count_delta: None,
                touch_visited: false,
                icount: None,
                virtual_ns: None,
            }],
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
    let detail = decode_missing_nodes(&err).expect("MissingNodes detail must be present");
    assert!(detail.node_ids.contains(&9999));
}

// ── (c) CurrentGeneration detail via CAS mismatch ────────────────────────────

#[tokio::test]
async fn put_metadata_cas_mismatch_detail() {
    let (_handle, mut client, _dir) = start_server().await;

    // Create with generation 0 (create-only).
    client
        .put_metadata(PutMetadataRequest {
            key: b"mykey".to_vec(),
            value: b"myval".to_vec(),
            expected_generation: Some(0),
        })
        .await
        .unwrap();

    // Try to create again with expected_generation=0 — should fail CAS.
    let err = client
        .put_metadata(PutMetadataRequest {
            key: b"mykey".to_vec(),
            value: b"other".to_vec(),
            expected_generation: Some(0),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let gen_detail =
        decode_current_generation(&err).expect("CurrentGeneration detail must be present");
    assert_eq!(gen_detail.generation, 1, "current generation should be 1");
}

// ── (d) CreateNode ────────────────────────────────────────────────────────────

async fn put_a_snapshot(client: &mut SnapshotStoreClient<Channel>) -> Vec<u8> {
    let pages = rand_pages(4, 777);
    let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container = build_full_container(4 * PAGE_SIZE as u64, &page_pairs, empty_blob());

    let stream = tokio_stream::iter(vec![PutPagesRequest {
        pages: pages.iter().map(|p| p.to_vec()).collect(),
    }]);
    client.put_pages(stream).await.unwrap();

    let resp = client
        .put_snapshot(PutSnapshotRequest { container })
        .await
        .unwrap()
        .into_inner();
    resp.snapshot_ref
}

#[tokio::test]
async fn create_node_unknown_snapshot_ref_not_found() {
    let (_handle, mut client, _dir) = start_server().await;

    let err = client
        .create_node(CreateNodeRequest {
            experiment_id: "exp1".to_string(),
            node_id: 0,
            parent_node_id: None,
            snapshot_ref: vec![0xAB; 32],
            input_log_id: vec![],
            inline_input_log: vec![],
            status: 0, // UNSPECIFIED → FRONTIER
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: vec![],
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn create_node_idempotent_and_already_exists() {
    let (_handle, mut client, _dir) = start_server().await;

    let snap_ref = put_a_snapshot(&mut client).await;

    let req = CreateNodeRequest {
        experiment_id: "exp-idem".to_string(),
        node_id: 0,
        parent_node_id: None,
        snapshot_ref: snap_ref.clone(),
        input_log_id: vec![],
        inline_input_log: vec![],
        status: 1, // FRONTIER
        score: None,
        icount: 10,
        virtual_ns: 100,
        attrs: vec![],
    };

    let r1 = client.create_node(req.clone()).await.unwrap().into_inner();
    let r2 = client.create_node(req.clone()).await.unwrap().into_inner();

    assert_eq!(
        r1.node.as_ref().unwrap().node_id,
        r2.node.as_ref().unwrap().node_id,
        "idempotent replay must return same row"
    );

    // Different snapshot_ref → ALREADY_EXISTS (immutable field conflict).
    // Build and upload a different snapshot.
    let pages2 = rand_pages(4, 888); // different seed → different pages/ref
    let page_pairs2: Vec<(u64, &[u8; PAGE_SIZE])> = pages2
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container2 = build_full_container(4 * PAGE_SIZE as u64, &page_pairs2, empty_blob());
    let stream2 = tokio_stream::iter(vec![PutPagesRequest {
        pages: pages2.iter().map(|p| p.to_vec()).collect(),
    }]);
    client.put_pages(stream2).await.unwrap();
    let snap_ref2 = client
        .put_snapshot(PutSnapshotRequest {
            container: container2,
        })
        .await
        .unwrap()
        .into_inner()
        .snapshot_ref;
    assert_ne!(snap_ref2, snap_ref, "second snapshot must differ");

    let mut req2 = req.clone();
    req2.snapshot_ref = snap_ref2;
    let err = client.create_node(req2).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::AlreadyExists);
}

// ── (e) ResolvePages ─────────────────────────────────────────────────────────

#[tokio::test]
async fn resolve_pages_mode_a_and_hashes_only() {
    let (_handle, mut client, _dir) = start_server().await;

    let n = 32usize;
    let pages = rand_pages(n, 55);
    let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container = build_full_container(n as u64 * PAGE_SIZE as u64, &page_pairs, empty_blob());

    let stream = tokio_stream::iter(vec![PutPagesRequest {
        pages: pages.iter().map(|p| p.to_vec()).collect(),
    }]);
    client.put_pages(stream).await.unwrap();

    let snap_ref = client
        .put_snapshot(PutSnapshotRequest { container })
        .await
        .unwrap()
        .into_inner()
        .snapshot_ref;

    // Mode A — full.
    use tokio_stream::StreamExt;
    let mut stream_resp = client
        .resolve_pages(ResolvePagesRequest {
            snapshot_ref: snap_ref.clone(),
            baseline_ref: vec![],
            hashes_only: false,
        })
        .await
        .unwrap()
        .into_inner();

    let mut resolved = Vec::new();
    while let Some(msg) = stream_resp.next().await {
        let msg = msg.unwrap();
        for p in msg.pages {
            resolved.push(p);
        }
    }
    assert_eq!(resolved.len(), n);
    // Ascending by page_index.
    for (i, p) in resolved.iter().enumerate() {
        assert_eq!(p.page_index, i as u64);
        assert!(!p.payload.is_empty(), "payload should be present");
    }

    // hashes_only = true.
    let mut stream_hashes = client
        .resolve_pages(ResolvePagesRequest {
            snapshot_ref: snap_ref.clone(),
            baseline_ref: vec![],
            hashes_only: true,
        })
        .await
        .unwrap()
        .into_inner();

    let mut hash_pages = Vec::new();
    while let Some(msg) = stream_hashes.next().await {
        let msg = msg.unwrap();
        for p in msg.pages {
            hash_pages.push(p);
        }
    }
    assert_eq!(hash_pages.len(), n);
    for p in &hash_pages {
        assert!(
            p.payload.is_empty(),
            "payload must be empty in hashes_only mode"
        );
        assert_eq!(p.page_hash.len(), 32);
    }
}

// ── (e) Mode B delta ─────────────────────────────────────────────────────────

#[tokio::test]
async fn resolve_pages_mode_b_delta() {
    let (_handle, mut client, _dir) = start_server().await;

    let n = 16usize;
    let pages_v0 = rand_pages(n, 100);
    let page_pairs_v0: Vec<(u64, &[u8; PAGE_SIZE])> = pages_v0
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container_v0 =
        build_full_container(n as u64 * PAGE_SIZE as u64, &page_pairs_v0, empty_blob());

    let stream = tokio_stream::iter(vec![PutPagesRequest {
        pages: pages_v0.iter().map(|p| p.to_vec()).collect(),
    }]);
    client.put_pages(stream).await.unwrap();

    let snap_ref_v0_bytes = client
        .put_snapshot(PutSnapshotRequest {
            container: container_v0,
        })
        .await
        .unwrap()
        .into_inner()
        .snapshot_ref;
    let snap_ref_v0 =
        snapstore_types::SnapshotRef::from_bytes(snap_ref_v0_bytes.as_slice().try_into().unwrap());

    // Delta: only pages 0..4 changed.
    let pages_delta: Vec<[u8; PAGE_SIZE]> = rand_pages(4, 999);
    let delta_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages_delta
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container_v1 = build_delta_container(
        &snap_ref_v0,
        n as u64 * PAGE_SIZE as u64,
        &delta_pairs,
        empty_blob(),
    );

    let stream2 = tokio_stream::iter(vec![PutPagesRequest {
        pages: pages_delta.iter().map(|p| p.to_vec()).collect(),
    }]);
    client.put_pages(stream2).await.unwrap();

    let snap_ref_v1 = client
        .put_snapshot(PutSnapshotRequest {
            container: container_v1,
        })
        .await
        .unwrap()
        .into_inner()
        .snapshot_ref;

    // Mode B: delta from v0 → v1.
    use tokio_stream::StreamExt;
    let mut stream_resp = client
        .resolve_pages(ResolvePagesRequest {
            snapshot_ref: snap_ref_v1,
            baseline_ref: snap_ref_v0_bytes,
            hashes_only: false,
        })
        .await
        .unwrap()
        .into_inner();

    let mut delta_pages = Vec::new();
    while let Some(msg) = stream_resp.next().await {
        for p in msg.unwrap().pages {
            delta_pages.push(p);
        }
    }
    // Only the 4 changed pages.
    assert_eq!(delta_pages.len(), 4);
}

// ── (f) QueryNodes cursor paging ─────────────────────────────────────────────

#[tokio::test]
async fn query_nodes_cursor_paging() {
    let (_handle, mut client, _dir) = start_server().await;

    let snap_ref = put_a_snapshot(&mut client).await;

    // Create 5 nodes.
    for i in 0u64..5 {
        let parent = if i == 0 { None } else { Some(i - 1) };
        client
            .create_node(CreateNodeRequest {
                experiment_id: "qn-exp".to_string(),
                node_id: i,
                parent_node_id: parent,
                snapshot_ref: snap_ref.clone(),
                input_log_id: vec![],
                inline_input_log: vec![],
                status: 1,
                score: None,
                icount: i * 10,
                virtual_ns: 0,
                attrs: vec![],
            })
            .await
            .unwrap();
    }

    // Query with a small limit to force paging.
    use tokio_stream::StreamExt;
    let mut stream = client
        .query_nodes(QueryNodesRequest {
            experiment_id: "qn-exp".to_string(),
            status: None,
            parent_node_id: None,
            min_depth: None,
            max_depth: None,
            created_after: None,
            updated_after: None,
            order: 1, // CREATED_AT
            limit: 2,
        })
        .await
        .unwrap()
        .into_inner();

    let mut all_nodes = Vec::new();
    while let Some(msg) = stream.next().await {
        let msg = msg.unwrap();
        all_nodes.extend(msg.nodes);
    }
    assert_eq!(
        all_nodes.len(),
        5,
        "should retrieve all 5 nodes across pages"
    );
}

// ── (g) KV CAS ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn kv_cas_create_and_mismatch() {
    let (_handle, mut client, _dir) = start_server().await;

    // Create-only (expected_generation = 0).
    let gen = client
        .put_metadata(PutMetadataRequest {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            expected_generation: Some(0),
        })
        .await
        .unwrap()
        .into_inner()
        .generation;
    assert_eq!(gen, 1);

    // Get.
    let g = client
        .get_metadata(GetMetadataRequest {
            key: b"k1".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(g.value, b"v1".to_vec());
    assert_eq!(g.generation, 1);

    // Match.
    let gen2 = client
        .put_metadata(PutMetadataRequest {
            key: b"k1".to_vec(),
            value: b"v2".to_vec(),
            expected_generation: Some(1),
        })
        .await
        .unwrap()
        .into_inner()
        .generation;
    assert_eq!(gen2, 2);

    // Mismatch.
    let err = client
        .put_metadata(PutMetadataRequest {
            key: b"k1".to_vec(),
            value: b"v3".to_vec(),
            expected_generation: Some(1), // wrong
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let detail = decode_current_generation(&err).expect("detail");
    assert_eq!(detail.generation, 2);

    // Delete.
    let d = client
        .delete_metadata(DeleteMetadataRequest {
            key: b"k1".to_vec(),
            expected_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(d.deleted);

    // Not found after delete.
    let nf = client
        .get_metadata(GetMetadataRequest {
            key: b"k1".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(nf.code(), tonic::Code::NotFound);
}

// ── (h) Pin/Unpin/PruneSubtree/Stats ─────────────────────────────────────────

#[tokio::test]
async fn pin_unpin_prune_stats() {
    let (_handle, mut client, _dir) = start_server().await;

    let snap_ref = put_a_snapshot(&mut client).await;

    // Pin.
    let pin_resp = client
        .pin(PinRequest {
            snapshot_ref: snap_ref.clone(),
            note: "test pin".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(pin_resp.newly_pinned);

    // Pin again — not newly pinned.
    let pin_resp2 = client
        .pin(PinRequest {
            snapshot_ref: snap_ref.clone(),
            note: "dup".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!pin_resp2.newly_pinned);

    // Stats: pins_total should be 1.
    let stats = client
        .stats(StatsRequest {
            experiment_id: "".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.store.as_ref().unwrap().pins_total, 1);
    assert_eq!(stats.store.as_ref().unwrap().manifests_total, 1);

    // Unpin.
    let unpin_resp = client
        .unpin(UnpinRequest {
            snapshot_ref: snap_ref,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(unpin_resp.was_pinned);

    // Create a node tree and prune.
    let snap2 = put_a_snapshot(&mut client).await;
    client
        .create_node(CreateNodeRequest {
            experiment_id: "exp-prune".to_string(),
            node_id: 0,
            parent_node_id: None,
            snapshot_ref: snap2.clone(),
            input_log_id: vec![],
            inline_input_log: vec![],
            status: 1,
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: vec![],
        })
        .await
        .unwrap();

    client
        .create_node(CreateNodeRequest {
            experiment_id: "exp-prune".to_string(),
            node_id: 1,
            parent_node_id: Some(0),
            snapshot_ref: snap2.clone(),
            input_log_id: vec![],
            inline_input_log: vec![],
            status: 1,
            score: None,
            icount: 1,
            virtual_ns: 0,
            attrs: vec![],
        })
        .await
        .unwrap();

    let prune = client
        .prune_subtree(PruneSubtreeRequest {
            experiment_id: "exp-prune".to_string(),
            node_id: 0,
            allow_root: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(prune.nodes_pruned, 2);

    // Stats for experiment.
    let stats2 = client
        .stats(StatsRequest {
            experiment_id: "exp-prune".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let exp = stats2.experiment.unwrap();
    assert_eq!(exp.nodes_pruned, 2);
    assert_eq!(exp.nodes_total, 2);
}

// ── (i) TriggerGc ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn trigger_gc_empty_store() {
    let (_handle, mut client, _dir) = start_server().await;

    let resp = client
        .trigger_gc(TriggerGcRequest {
            compact_aggressively: false,
            detach: false,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.started);
    assert!(!resp.already_running);
    assert_eq!(resp.nodes_reaped, 0);
    assert_eq!(resp.manifests_deleted, 0);
    assert_eq!(resp.pages_reclaimed, 0);
    assert_eq!(resp.bytes_reclaimed, 0);
    assert_eq!(resp.packs_compacted, 0);

    let stats = client
        .stats(StatsRequest {
            experiment_id: "".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.store.unwrap().gc_runs_total, 1);
}

async fn put_a_snapshot_seeded(client: &mut SnapshotStoreClient<Channel>, seed: u64) -> Vec<u8> {
    let pages = rand_pages(4, seed);
    let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container = build_full_container(4 * PAGE_SIZE as u64, &page_pairs, empty_blob());

    let stream = tokio_stream::iter(vec![PutPagesRequest {
        pages: pages.iter().map(|p| p.to_vec()).collect(),
    }]);
    client.put_pages(stream).await.unwrap();

    let resp = client
        .put_snapshot(PutSnapshotRequest { container })
        .await
        .unwrap()
        .into_inner();
    resp.snapshot_ref
}

#[tokio::test]
async fn trigger_gc_reclaims_pruned_manifest() {
    let (_handle, mut client, _dir) = start_server().await;

    // A rooted snapshot (node points at it) and an orphan one (never
    // attached to any node/pin) — the orphan is garbage from the first
    // cycle. Distinct seeds so the two snapshots content-address to
    // different refs.
    let rooted = put_a_snapshot_seeded(&mut client, 111).await;
    let orphan = put_a_snapshot_seeded(&mut client, 222).await;
    assert_ne!(rooted, orphan);

    client
        .create_node(CreateNodeRequest {
            experiment_id: "exp-gc".to_string(),
            node_id: 0,
            parent_node_id: None,
            snapshot_ref: rooted.clone(),
            input_log_id: vec![],
            inline_input_log: vec![],
            status: 1,
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: vec![],
        })
        .await
        .unwrap();

    let resp = client
        .trigger_gc(TriggerGcRequest {
            compact_aggressively: true,
            detach: false,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.started);
    assert!(!resp.already_running);
    assert_eq!(resp.manifests_deleted, 1, "the orphan manifest is swept");

    // The rooted snapshot must still resolve.
    client
        .get_snapshot(GetSnapshotRequest {
            snapshot_ref: rooted,
        })
        .await
        .expect("rooted snapshot must survive GC");

    // The orphan is gone.
    let err = client
        .get_snapshot(GetSnapshotRequest {
            snapshot_ref: orphan,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    let stats = client
        .stats(StatsRequest {
            experiment_id: "".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(stats.store.unwrap().gc_runs_total >= 1);
}

#[tokio::test]
async fn pin_unknown_ref_failed_precondition() {
    let (_handle, mut client, _dir) = start_server().await;

    let err = client
        .pin(PinRequest {
            snapshot_ref: vec![0xCD; 32],
            note: "".to_string(),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

// ── (j) Health: SERVING after serve_for_tests returns ────────────────────────

#[tokio::test]
async fn health_serving_after_startup() {
    let (handle, mut client, _dir) = start_server().await;

    // Check via a Stats call that the server is responsive (implies SERVING).
    let resp = client
        .stats(StatsRequest {
            experiment_id: "".to_string(),
        })
        .await;
    assert!(resp.is_ok(), "server should be responsive after startup");

    drop(handle); // initiate shutdown
}

// ── (k) STORE_VERSION mismatch refusal ───────────────────────────────────────

#[tokio::test]
async fn store_version_mismatch_refused() {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();

    // Write a wrong version.
    let store_dir = data_root.join("store");
    std::fs::create_dir_all(&store_dir).unwrap();
    std::fs::create_dir_all(&data_root).unwrap();
    std::fs::write(data_root.join("STORE_VERSION"), "2\n").unwrap();

    let config = ServerConfig {
        data_root: data_root.clone(),
        grpc_tcp_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_uds_path: Some(data_root.join("snapstore.sock")),
        page_channel_path: None,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: Default::default(),
        gc: Default::default(),
    };

    let result = serve_for_tests(config).await;
    assert!(result.is_err(), "must refuse STORE_VERSION mismatch");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("mismatch") || err.contains("STORE_VERSION"),
        "error should mention version mismatch: {err}"
    );
}

// ── (l) Unknown config key rejection ─────────────────────────────────────────

#[test]
fn unknown_config_key_rejection() {
    // Write a config with an unknown key.
    let dir = TempDir::new().unwrap();
    let cfg_path = dir.path().join("config.toml");
    std::fs::write(&cfg_path, "data_root = \"/tmp\"\nunknown_key = 42\n").unwrap();

    let result = load_config(&cfg_path);
    assert!(result.is_err(), "unknown key must be rejected");
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("unknown_key") || err_str.contains("unknown field"),
        "error must name the offending key: {err_str}"
    );
}
