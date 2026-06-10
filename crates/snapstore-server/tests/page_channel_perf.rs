//! M5 benchmark gates (plan 03 WI4 / gate S4), measured against a live
//! server with the page channel enabled. Run on the reference box in
//! release:
//!
//! ```bash
//! cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture
//! ```
//!
//! Gates (05): PUT_BATCH dedup-warm at 1.5 GB/s and GET_BATCH warm at
//! 2.5 GB/s block at spec on any hardware (transport/CPU/page-cache bound);
//! the 16-client commit p99 row is fsync-bound and gates at the SATA floor
//! here, NVMe re-validation at M8.

#![cfg(target_os = "linux")]

use std::time::Instant;

use snapstore_localpath::client::PageChannelClient;
use snapstore_server::{build_server::serve_for_tests, config::ServerConfig};
use snapstore_types::{PageHash, PAGE_SIZE};
use tempfile::TempDir;

const BATCH_PAGES: usize = 8192; // 32 MiB per PUT_BATCH

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "M5 gate measurement; run in release on the reference box"]
async fn m5_benchmarks() {
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
        page_channel: Default::default(),
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
}
