//! M5 benchmark gates (plan 03 WI4 / gate S4), measured against a live
//! server with the page channel enabled. Run on the reference box in
//! release:
//!
//! ```bash
//! SNAPSTORE_BENCH_ROOT=/mnt/phase5-scratch \
//! SNAPSTORE_M5_BENCH_JSON=target/phase5-readiness/m5-transport/results.json \
//! cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture
//! ```
//!
//! Gates (05): PUT_BATCH dedup-warm at 1.5 GB/s and GET_BATCH warm at
//! 2.5 GB/s block at spec on any hardware (transport/CPU/page-cache bound);
//! the 16-client commit p99 row is fsync-bound and gates at the SATA floor
//! here, NVMe re-validation at M8.

#![cfg(target_os = "linux")]

use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use hyper_util::rt::TokioIo;
use serde::Serialize;
use snapstore_localpath::client::PageChannelClient;
use snapstore_manifest::DeviceBlob;
use snapstore_server::{
    build_server::serve_for_tests,
    config::ServerConfig,
    snapstore_proto::{
        snapshot_store_client::SnapshotStoreClient, CreateNodeRequest,
        NodeUpdate as ProtoNodeUpdate, PutSnapshotRequest, UpdateNodesRequest,
    },
};
use snapstore_types::{PageHash, PAGE_SIZE};
use tempfile::{Builder as TempBuilder, TempDir};
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

const BATCH_PAGES: usize = 8192; // 32 MiB per PUT_BATCH
const META_WARMUP_SAMPLES: usize = 50;
const META_COUNTED_SAMPLES: usize = 500;
const UPDATE_BATCH_NODES: u64 = 256;

#[derive(Serialize)]
struct M5BenchResults {
    put_batch_warm_1_stream_gbps: f64,
    put_batch_warm_sustained_gbps: f64,
    get_batch_warm_1_stream_gbps: f64,
    get_batch_warm_sustained_gbps: f64,
    commit_16x8mib_p50_ms: f64,
    commit_16x8mib_p99_ms: f64,
    commit_16x8mib_aggregate_gbps: f64,
    create_node_inline_log_p50_ms: f64,
    create_node_inline_log_p95_ms: f64,
    create_node_inline_log_p99_ms: f64,
    update_nodes_256_p50_ms: f64,
    update_nodes_256_p95_ms: f64,
    update_nodes_256_p99_ms: f64,
    samples: M5BenchSamples,
}

#[derive(Serialize)]
struct M5BenchSamples {
    commit_latencies_ms: Vec<f64>,
    create_node_inline_log_ms: Vec<f64>,
    update_nodes_256_ms: Vec<f64>,
}

fn page(client: u64, iter: u64, idx: u64) -> Box<[u8; PAGE_SIZE]> {
    let mut p = Box::new([0u8; PAGE_SIZE]);
    p[..8].copy_from_slice(&client.to_le_bytes());
    p[8..16].copy_from_slice(&iter.to_le_bytes());
    p[16..24].copy_from_slice(&idx.to_le_bytes());
    // Touch every cache line so the page is not trivially compressible work.
    for chunk in p.chunks_mut(64).skip(1) {
        chunk[0] = (idx as u8).wrapping_add(chunk.len() as u8);
    }
    p
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn bench_tempdir(prefix: &str) -> TempDir {
    let root = std::env::var_os("SNAPSTORE_BENCH_ROOT")
        .expect("SNAPSTORE_BENCH_ROOT is required for page_channel_perf measurements");
    TempBuilder::new()
        .prefix(prefix)
        .tempdir_in(root)
        .expect("create benchmark tempdir in SNAPSTORE_BENCH_ROOT")
}

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
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

async fn put_snapshot_for_meta(
    client: &mut SnapshotStoreClient<Channel>,
    pc_path: &Path,
) -> Vec<u8> {
    let pc_path = pc_path.to_path_buf();
    let container = tokio::task::spawn_blocking(move || {
        let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..16).map(|i| page(9000, 0, i)).collect();
        let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        PageChannelClient::connect(&pc_path)
            .expect("pc connect")
            .put_batch(&refs)
            .expect("put pages for meta rows");
        let indexed: Vec<(u64, &[u8; PAGE_SIZE])> = pages
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p.as_ref()))
            .collect();
        snapstore_client::helpers::build_snapshot_container(
            None,
            16 * PAGE_SIZE as u64,
            &indexed,
            empty_blob(),
        )
        .expect("snapshot container for meta rows")
    })
    .await
    .expect("build snapshot for meta rows");

    client
        .put_snapshot(PutSnapshotRequest { container })
        .await
        .expect("put snapshot for meta rows")
        .into_inner()
        .snapshot_ref
}

fn input_log_container(sample: usize) -> (Vec<u8>, Vec<u8>) {
    let mut payload = vec![0u8; 16 * 1024 - 56];
    for (chunk_idx, chunk) in payload.chunks_mut(8).enumerate() {
        let word = (sample as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .rotate_left((chunk_idx % 64) as u32)
            ^ (chunk_idx as u64).wrapping_mul(0xD6E8_FD50_9B54_AA2D);
        let bytes = word.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    let container = snapstore_client::helpers::build_input_log_container(1, &payload);
    debug_assert_eq!(container.len(), 16 * 1024);
    let log_id = snapstore_client::helpers::log_id_of(&container)
        .to_bytes()
        .to_vec();
    (container, log_id)
}

#[test]
fn input_log_container_uniqueness() {
    let mut seen = std::collections::HashSet::new();
    for sample in 0..(META_WARMUP_SAMPLES + META_COUNTED_SAMPLES) {
        let (container, log_id) = input_log_container(sample);
        assert_eq!(container.len(), 16 * 1024);
        assert!(seen.insert(log_id), "duplicate log_id at sample {sample}");
    }
}

async fn measure_meta_rows(uds_path: PathBuf, pc_path: PathBuf) -> (Vec<f64>, Vec<f64>) {
    let mut client = make_client(uds_path).await;
    let snap_ref = put_snapshot_for_meta(&mut client, &pc_path).await;

    client
        .create_node(CreateNodeRequest {
            experiment_id: "m5-create-node".to_string(),
            node_id: 0,
            parent_node_id: None,
            snapshot_ref: snap_ref.clone(),
            input_log_id: vec![],
            inline_input_log: vec![],
            status: 1,
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: vec![],
        })
        .await
        .expect("create-node bench root");

    let log_inputs: Vec<(Vec<u8>, Vec<u8>)> = (0..(META_WARMUP_SAMPLES + META_COUNTED_SAMPLES))
        .map(input_log_container)
        .collect();
    let mut create_samples = Vec::with_capacity(META_COUNTED_SAMPLES);
    for (i, (container, log_id)) in log_inputs.into_iter().enumerate() {
        let start = Instant::now();
        client
            .create_node(CreateNodeRequest {
                experiment_id: "m5-create-node".to_string(),
                node_id: i as u64 + 1,
                parent_node_id: Some(0),
                snapshot_ref: snap_ref.clone(),
                input_log_id: log_id,
                inline_input_log: container,
                status: 1,
                score: None,
                icount: i as u64,
                virtual_ns: i as u64,
                attrs: vec![],
            })
            .await
            .expect("timed CreateNode");
        if i >= META_WARMUP_SAMPLES {
            create_samples.push(start.elapsed().as_secs_f64() * 1e3);
        }
    }

    client
        .create_node(CreateNodeRequest {
            experiment_id: "m5-update-nodes".to_string(),
            node_id: 0,
            parent_node_id: None,
            snapshot_ref: snap_ref.clone(),
            input_log_id: vec![],
            inline_input_log: vec![],
            status: 1,
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: vec![],
        })
        .await
        .expect("update-nodes bench root");
    for node_id in 1..=UPDATE_BATCH_NODES {
        client
            .create_node(CreateNodeRequest {
                experiment_id: "m5-update-nodes".to_string(),
                node_id,
                parent_node_id: Some(0),
                snapshot_ref: snap_ref.clone(),
                input_log_id: vec![],
                inline_input_log: vec![],
                status: 1,
                score: None,
                icount: node_id,
                virtual_ns: node_id,
                attrs: vec![],
            })
            .await
            .expect("seed UpdateNodes node");
    }

    let mut update_samples = Vec::with_capacity(META_COUNTED_SAMPLES);
    for i in 0..(META_WARMUP_SAMPLES + META_COUNTED_SAMPLES) {
        let updates: Vec<ProtoNodeUpdate> = (1..=UPDATE_BATCH_NODES)
            .map(|node_id| ProtoNodeUpdate {
                node_id,
                status: None,
                score: Some(i as f64),
                attrs: None,
                visit_count_delta: Some(1),
                touch_visited: true,
                icount: Some(i as u64),
                virtual_ns: Some(i as u64),
            })
            .collect();
        let start = Instant::now();
        client
            .update_nodes(UpdateNodesRequest {
                experiment_id: "m5-update-nodes".to_string(),
                updates,
            })
            .await
            .expect("timed UpdateNodes");
        if i >= META_WARMUP_SAMPLES {
            update_samples.push(start.elapsed().as_secs_f64() * 1e3);
        }
    }

    (create_samples, update_samples)
}

fn write_json_if_requested(results: &M5BenchResults) {
    let Ok(path) = std::env::var("SNAPSTORE_M5_BENCH_JSON") else {
        return;
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create SNAPSTORE_M5_BENCH_JSON parent");
    }
    let bytes = serde_json::to_vec_pretty(results).expect("serialize M5 bench results");
    std::fs::write(&path, bytes).expect("write SNAPSTORE_M5_BENCH_JSON");
    println!("wrote {}", path.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "M5 gate measurement; run in release on the reference box"]
async fn m5_benchmarks() {
    let dir = bench_tempdir("snapstore-m5-");
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
        page_channel: Default::default(),
        gc: Default::default(),
    };
    let uds_path = data_root.join("snapstore.sock");
    let _handle = serve_for_tests(config).await.expect("serve");

    // ── PUT_BATCH dedup-warm (transport+hash bound; spec >= 1.5 GB/s) ────────
    let (warm_put_gbps, hashes) = tokio::task::spawn_blocking({
        let pc_path = pc_path.clone();
        move || {
            let client = PageChannelClient::connect(&pc_path).expect("pc connect");
            let pages: Vec<Box<[u8; PAGE_SIZE]>> =
                (0..BATCH_PAGES as u64).map(|i| page(0, 0, i)).collect();
            let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
            let hashes: Vec<PageHash> = pages
                .iter()
                .map(|p| PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()))
                .collect();

            // Cold pass stores them; everything after is dedup-warm.
            client.put_batch(&refs).expect("cold put");

            let iters = 24;
            let start = Instant::now();
            for _ in 0..iters {
                let out = client.put_batch(&refs).expect("warm put");
                assert_eq!(out.pages_new, 0, "warm pass must be all-dedup");
            }
            let secs = start.elapsed().as_secs_f64();
            let bytes = (iters * BATCH_PAGES * PAGE_SIZE) as f64;
            (bytes / secs / 1e9, hashes)
        }
    })
    .await
    .unwrap();

    // ── GET_BATCH warm (page-cache bound; spec >= 2.5 GB/s) ──────────────────
    let warm_get_gbps = tokio::task::spawn_blocking({
        let pc_path = pc_path.clone();
        let hashes = hashes.clone();
        move || {
            let client = PageChannelClient::connect(&pc_path).expect("pc connect");
            let reqs: Vec<(PageHash, u64)> = hashes
                .iter()
                .enumerate()
                .map(|(i, h)| (*h, i as u64))
                .collect();
            // Warm the page cache.
            let got = client.get_batch(&reqs).expect("warm-up get");
            assert_eq!(got.len(), BATCH_PAGES);

            let iters = 24;
            let start = Instant::now();
            for _ in 0..iters {
                let got = client.get_batch(&reqs).expect("get");
                assert_eq!(got.len(), BATCH_PAGES);
            }
            let secs = start.elapsed().as_secs_f64();
            let bytes = (iters * BATCH_PAGES * PAGE_SIZE) as f64;
            bytes / secs / 1e9
        }
    })
    .await
    .unwrap();

    // ── Sustained (multi-stream) warm PUT/GET: 4 clients in parallel — the
    //    "sustained ingest" gate is the store-wide rate, not one stream's
    //    round-trip latency ────────────────────────────────────────────────
    const STREAMS: u64 = 4;
    let mut put_tasks = Vec::new();
    let start = Instant::now();
    for s in 0..STREAMS {
        let pc_path = pc_path.clone();
        put_tasks.push(tokio::task::spawn_blocking(move || {
            let client = PageChannelClient::connect(&pc_path).expect("pc connect");
            let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..BATCH_PAGES as u64)
                .map(|i| page(100 + s, 0, i))
                .collect();
            let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
            client.put_batch(&refs).expect("cold put"); // store once
            let iters = 12;
            for _ in 0..iters {
                client.put_batch(&refs).expect("warm put");
            }
            iters * BATCH_PAGES * PAGE_SIZE
        }));
    }
    let mut total_bytes = 0usize;
    for t in put_tasks {
        total_bytes += t.await.unwrap();
    }
    let sustained_put_gbps = total_bytes as f64 / start.elapsed().as_secs_f64() / 1e9;

    let mut get_tasks = Vec::new();
    let start = Instant::now();
    for s in 0..STREAMS {
        let pc_path = pc_path.clone();
        get_tasks.push(tokio::task::spawn_blocking(move || {
            let client = PageChannelClient::connect(&pc_path).expect("pc connect");
            let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..BATCH_PAGES as u64)
                .map(|i| page(100 + s, 0, i))
                .collect();
            let reqs: Vec<(PageHash, u64)> = pages
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    (
                        PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()),
                        i as u64,
                    )
                })
                .collect();
            let iters = 12;
            for _ in 0..iters {
                let got = client.get_batch(&reqs).expect("get");
                assert_eq!(got.len(), BATCH_PAGES);
            }
            iters * BATCH_PAGES * PAGE_SIZE
        }));
    }
    let mut total_bytes = 0usize;
    for t in get_tasks {
        total_bytes += t.await.unwrap();
    }
    let sustained_get_gbps = total_bytes as f64 / start.elapsed().as_secs_f64() / 1e9;

    // ── 16 parallel clients, 8 MiB deltas (PUT_BATCH + PutSnapshot incl.
    //    group fsync): p99 commit < 40 ms (fsync floor) + aggregate ─────────
    const CLIENTS: u64 = 16;
    const COMMITS_PER_CLIENT: u64 = 12;
    const DELTA_PAGES: u64 = 2048; // 8 MiB

    let uds = uds_path.clone();
    let agg_start = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..CLIENTS {
        let pc_path = pc_path.clone();
        let uds = uds.clone();
        tasks.push(tokio::task::spawn_blocking(move || {
            let pc = PageChannelClient::connect(&pc_path).expect("pc connect");
            let grpc = snapstore_client::blocking::SnapstoreClient::connect(
                snapstore_client::Transport::Uds(uds),
            )
            .expect("grpc connect");

            let mut latencies = Vec::with_capacity(COMMITS_PER_CLIENT as usize);
            for iter in 0..COMMITS_PER_CLIENT {
                // Unique pages per (client, iter): cold ingest + real fsync.
                let pages: Vec<Box<[u8; PAGE_SIZE]>> =
                    (0..DELTA_PAGES).map(|i| page(c + 1, iter, i)).collect();
                let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
                let indexed: Vec<(u64, &[u8; PAGE_SIZE])> = pages
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (i as u64, p.as_ref()))
                    .collect();
                let container = snapstore_client::helpers::build_snapshot_container(
                    None,
                    DELTA_PAGES * PAGE_SIZE as u64,
                    &indexed,
                    snapstore_manifest::DeviceBlob {
                        format: 0,
                        zstd: false,
                        bytes: vec![],
                        raw_len: 0,
                    },
                )
                .expect("container");

                let start = Instant::now();
                pc.put_batch(&refs).expect("put_batch");
                grpc.put_snapshot(container).expect("put_snapshot");
                latencies.push(start.elapsed().as_secs_f64() * 1e3);
            }
            latencies
        }));
    }
    let mut all: Vec<f64> = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let agg_secs = agg_start.elapsed().as_secs_f64();
    all.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let agg_bytes = (CLIENTS * COMMITS_PER_CLIENT * DELTA_PAGES) as f64 * PAGE_SIZE as f64;
    let agg_gbps = agg_bytes / agg_secs / 1e9;

    let (mut create_node_samples, mut update_nodes_samples) =
        measure_meta_rows(uds_path, pc_path.clone()).await;
    create_node_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    update_nodes_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let results = M5BenchResults {
        put_batch_warm_1_stream_gbps: warm_put_gbps,
        put_batch_warm_sustained_gbps: sustained_put_gbps,
        get_batch_warm_1_stream_gbps: warm_get_gbps,
        get_batch_warm_sustained_gbps: sustained_get_gbps,
        commit_16x8mib_p50_ms: percentile(&all, 0.50),
        commit_16x8mib_p99_ms: percentile(&all, 0.99),
        commit_16x8mib_aggregate_gbps: agg_gbps,
        create_node_inline_log_p50_ms: percentile(&create_node_samples, 0.50),
        create_node_inline_log_p95_ms: percentile(&create_node_samples, 0.95),
        create_node_inline_log_p99_ms: percentile(&create_node_samples, 0.99),
        update_nodes_256_p50_ms: percentile(&update_nodes_samples, 0.50),
        update_nodes_256_p95_ms: percentile(&update_nodes_samples, 0.95),
        update_nodes_256_p99_ms: percentile(&update_nodes_samples, 0.99),
        samples: M5BenchSamples {
            commit_latencies_ms: all.clone(),
            create_node_inline_log_ms: create_node_samples.clone(),
            update_nodes_256_ms: update_nodes_samples.clone(),
        },
    };
    write_json_if_requested(&results);

    println!("== M5 benchmark results (gate S4) ==");
    println!(
        "PUT_BATCH dedup-warm (1 stream)  : {:.2} GB/s   (round-trip latency bound)",
        warm_put_gbps
    );
    println!(
        "PUT_BATCH dedup-warm (sustained) : {:.2} GB/s   (spec gate >= 1.5 GB/s)",
        sustained_put_gbps
    );
    println!(
        "GET_BATCH warm (1 stream)        : {:.2} GB/s   (round-trip latency bound)",
        warm_get_gbps
    );
    println!(
        "GET_BATCH warm (sustained)       : {:.2} GB/s   (spec gate >= 2.5 GB/s)",
        sustained_get_gbps
    );
    println!(
        "16-client commit     : p50 {:.1} ms  p99 {:.1} ms  (spec p99 < 40 ms; fsync-bound row)",
        percentile(&all, 0.50),
        percentile(&all, 0.99)
    );
    println!(
        "16-client aggregate  : {:.2} GB/s   (spec >= 1.2 GB/s; cold/disk-bound here)",
        agg_gbps
    );
    println!(
        "CreateNode + 16KiB log: p50 {:.3} ms  p95 {:.3} ms  p99 {:.3} ms   (spec p50 < 1.5 ms)",
        results.create_node_inline_log_p50_ms,
        results.create_node_inline_log_p95_ms,
        results.create_node_inline_log_p99_ms
    );
    println!(
        "UpdateNodes(256)      : p50 {:.3} ms  p95 {:.3} ms  p99 {:.3} ms   (spec p50 < 3 ms)",
        results.update_nodes_256_p50_ms,
        results.update_nodes_256_p95_ms,
        results.update_nodes_256_p99_ms
    );
}
