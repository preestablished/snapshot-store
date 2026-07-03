//! M4 gRPC benchmarks — Gate S3 rows.
//!
//! Benches:
//!   * `put_pages_uds_dedup_warm` — transport+hash bound at >=600 MB/s spec.
//!     Pre-uploads 16 384 distinct pages; bench iterates streaming the SAME
//!     16 384 pages in 256-page messages over UDS (all dedup => no disk writes).
//!     Throughput measured as 16 384 * 4 096 bytes per iteration.
//!
//!   * `query_nodes_page_1000` — p50 < 4 ms spec.
//!     Seeds one experiment with 2 000 nodes (small attrs); bench measures a
//!     QueryNodes page of limit 1 000 over UDS (collect the stream).
//!
//! Run with:
//! ```text
//! cargo bench -p snapstore-server --bench grpc_bench -- \
//!     --warm-up-time 2 --measurement-time 8
//! ```

use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use snapstore_manifest::DeviceBlob;
use snapstore_server::{
    build_server::serve_for_tests,
    config::ServerConfig,
    snapstore_proto::{
        snapshot_store_client::SnapshotStoreClient as RawClient, CreateNodeRequest,
        PutPagesRequest, PutSnapshotRequest, QueryNodesRequest,
    },
};
use snapstore_store::build::build_full_container;
use snapstore_types::PAGE_SIZE;

// ── setup helpers ─────────────────────────────────────────────────────────────

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

/// Spin up a test server in a temp dir.
/// The TempDir is returned to keep the directory alive during the bench.
/// The ServerHandle is intentionally leaked (server lives for the process lifetime).
fn start_server(rt: &tokio::runtime::Runtime) -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();

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

    let uds_path = rt.block_on(async {
        let (handle, uds_path) = serve_for_tests(config).await.expect("serve_for_tests");
        // Leak the handle so the server keeps running during the benchmark.
        std::mem::forget(handle);
        uds_path
    });

    (dir, uds_path)
}

/// Build a raw gRPC channel over UDS.
async fn make_channel(uds_path: PathBuf) -> Channel {
    Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_uri: tonic::transport::Uri| {
            let p = uds_path.clone();
            async move {
                let stream = UnixStream::connect(&p).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("channel connect")
}

// ── Bench: put_pages_uds_dedup_warm ──────────────────────────────────────────

/// 16 384 pages * 4 096 bytes = 64 MiB per iteration.
const BENCH_PAGES: usize = 16_384;
/// gRPC message size: 256 pages * 4 096 = 1 MiB.
const MSG_PAGES: usize = 256;

fn bench_put_pages_dedup_warm(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let (_dir, uds_path) = start_server(&rt);

    // Generate BENCH_PAGES distinct pages (unique content by index).
    let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..BENCH_PAGES)
        .map(|i| {
            let mut p = Box::new([0u8; PAGE_SIZE]);
            let v = (i as u64).wrapping_mul(0x9e3779b97f4a7c15u64);
            p[0..8].copy_from_slice(&v.to_le_bytes());
            p
        })
        .collect();

    // Pre-upload all pages so the warm-run hits only dedup (no disk writes).
    rt.block_on(async {
        let mut client = RawClient::new(make_channel(uds_path.clone()).await);

        let msgs: Vec<PutPagesRequest> = pages
            .chunks(MSG_PAGES)
            .map(|chunk| PutPagesRequest {
                pages: chunk.iter().map(|p| p.to_vec()).collect(),
            })
            .collect();

        let stream = tokio_stream::iter(msgs);
        let resp = client
            .put_pages(stream)
            .await
            .expect("pre-upload put_pages");
        let r = resp.into_inner();
        assert_eq!(
            r.pages_new, BENCH_PAGES as u64,
            "pre-upload: all pages must be new"
        );
    });

    let total_bytes = (BENCH_PAGES * PAGE_SIZE) as u64;

    let mut group = c.benchmark_group("put_pages");
    group.throughput(Throughput::Bytes(total_bytes));
    group.sample_size(20);

    group.bench_function("uds_dedup_warm", |b| {
        // Build message list once — they are cloned per iteration.
        let msgs: Vec<PutPagesRequest> = pages
            .chunks(MSG_PAGES)
            .map(|chunk| PutPagesRequest {
                pages: chunk.iter().map(|p| p.to_vec()).collect(),
            })
            .collect();

        b.to_async(&rt).iter(|| {
            let msgs = msgs.clone();
            let uds = uds_path.clone();
            async move {
                let mut client = RawClient::new(make_channel(uds).await);
                let stream = tokio_stream::iter(msgs);
                let resp = client.put_pages(stream).await.expect("put_pages bench");
                let r = resp.into_inner();
                // All pages must be deduped (no disk writes).
                assert_eq!(r.pages_deduped, BENCH_PAGES as u64);
                criterion::black_box(r)
            }
        });
    });

    group.finish();
}

// ── Bench: query_nodes_page_1000 ─────────────────────────────────────────────

const QUERY_BENCH_NODES: usize = 2_000;
const QUERY_BENCH_LIMIT: u32 = 1_000;
const QUERY_BENCH_EXP: &str = "bench-exp";

fn bench_query_nodes_page_1000(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let (_dir, uds_path) = start_server(&rt);

    // Seed the experiment with QUERY_BENCH_NODES nodes, each sharing one snapshot.
    rt.block_on(async {
        let mut client = RawClient::new(make_channel(uds_path.clone()).await);

        // Upload a small snapshot.
        let n_pages = 4usize;
        let snap_pages: Vec<[u8; PAGE_SIZE]> = (0..n_pages)
            .map(|i| {
                let mut p = [0u8; PAGE_SIZE];
                let v = (i as u64).wrapping_mul(0x1234_5678_9abc_def0u64);
                p[0..8].copy_from_slice(&v.to_le_bytes());
                p
            })
            .collect();

        let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = snap_pages
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p))
            .collect();

        let put_stream = tokio_stream::iter(vec![PutPagesRequest {
            pages: snap_pages.iter().map(|p| p.to_vec()).collect(),
        }]);
        client
            .put_pages(put_stream)
            .await
            .expect("pre-upload pages");

        let container =
            build_full_container(n_pages as u64 * PAGE_SIZE as u64, &page_pairs, empty_blob());
        let snap_resp = client
            .put_snapshot(PutSnapshotRequest { container })
            .await
            .expect("put_snapshot")
            .into_inner();
        let snap_ref = snap_resp.snapshot_ref;

        for i in 0..QUERY_BENCH_NODES {
            client
                .create_node(CreateNodeRequest {
                    experiment_id: QUERY_BENCH_EXP.to_string(),
                    node_id: i as u64,
                    parent_node_id: if i == 0 { None } else { Some(i as u64 - 1) },
                    snapshot_ref: snap_ref.clone(),
                    input_log_id: vec![],
                    inline_input_log: vec![],
                    status: 1, // FRONTIER
                    score: None,
                    icount: i as u64,
                    virtual_ns: 0,
                    attrs: vec![],
                })
                .await
                .expect("create_node");
        }
    });

    let mut group = c.benchmark_group("query_nodes");
    group.sample_size(50);

    group.bench_function("page_1000", |b| {
        b.to_async(&rt).iter(|| {
            let uds = uds_path.clone();
            async move {
                use tokio_stream::StreamExt;

                let mut client = RawClient::new(make_channel(uds).await);
                let mut stream = client
                    .query_nodes(QueryNodesRequest {
                        experiment_id: QUERY_BENCH_EXP.to_string(),
                        status: None,
                        parent_node_id: None,
                        min_depth: None,
                        max_depth: None,
                        created_after: None,
                        updated_after: None,
                        order: 1, // CREATED_AT
                        limit: QUERY_BENCH_LIMIT,
                    })
                    .await
                    .expect("query_nodes")
                    .into_inner();

                let mut count = 0usize;
                while let Some(msg) = stream.next().await {
                    let msg = msg.expect("stream msg");
                    count += msg.nodes.len();
                    if count >= QUERY_BENCH_LIMIT as usize {
                        break;
                    }
                }
                criterion::black_box(count)
            }
        });
    });

    group.finish();
}

// ── Criterion entry point ─────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_put_pages_dedup_warm,
    bench_query_nodes_page_1000
);
criterion_main!(benches);
