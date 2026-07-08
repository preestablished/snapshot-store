//! Phase 5 M7 GC readiness benchmark.
//!
//! Run on qualified hardware:
//!
//! ```bash
//! SNAPSTORE_BENCH_ROOT=/mnt/phase5-scratch \
//! SNAPSTORE_GC_BENCH_JSON=target/phase5-readiness/m7-gc-benchmark/results.json \
//! cargo test -p snapstore-server --test gc_readiness_bench --release -- --ignored --nocapture
//! ```

#![cfg(target_os = "linux")]

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use serde::Serialize;
use snapstore_manifest::DeviceBlob;
use snapstore_meta::{CreateNodeParams, MetaDb};
use snapstore_server::{
    config::ServerConfig,
    gc::{run_gc_cycle, GcOpts, GcReport},
    metrics::Metrics,
    startup::run_startup,
};
use snapstore_store::{build::build_full_container, gc::GcHooks, SnapshotStore};
use snapstore_types::{ExperimentId, NodeId, NodeStatus, SnapshotRef, PAGE_SIZE};
use tempfile::{Builder as TempBuilder, TempDir};

const BRANCHING: u64 = 8;
const DEFAULT_SEED: u64 = 20260708;
const PHASE_IDLE: u8 = 0;
const PHASE_GRACE: u8 = 1;
const PHASE_RECLAIMING: u8 = 2;
const PHASE_STOP: u8 = 255;

#[derive(Clone, Debug, Serialize)]
struct BenchConfig {
    nodes: u64,
    target_physical_gb: f64,
    target_garbage_fraction: f64,
    ingest_target_mbps: f64,
    seed: u64,
    gc_mode: String,
    gc_opts: GcOptsJson,
    commit_workers: usize,
    commit_payload_mib: usize,
    idle_seconds: u64,
}

#[derive(Clone, Debug, Serialize)]
struct GcOptsJson {
    compact_threshold: f64,
    rotate_active_first: bool,
    tombstone_grace_cycles: u32,
}

#[derive(Debug, Serialize)]
struct BenchOutput {
    config: BenchConfig,
    population: PopulationJson,
    idle_commit: CommitSummary,
    grace_gc_run: GcRunJson,
    reclaiming_gc_run: ReclaimingGcRunJson,
    pass: PassJson,
    latency_csv: Option<String>,
}

#[derive(Debug, Serialize)]
struct PopulationJson {
    nodes_created: u64,
    pages_per_node: u64,
    physical_page_bytes_before_gc: u64,
    predicted_garbage_bytes: u64,
    pruned_subtrees: Vec<PrunedSubtreeJson>,
}

#[derive(Debug, Serialize)]
struct PrunedSubtreeJson {
    experiment_id: String,
    node_id: u64,
    nodes: u64,
    predicted_garbage_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
struct CommitSample {
    phase: String,
    latency_ms: f64,
    bytes: u64,
    ok: bool,
    error: String,
}

#[derive(Debug, Serialize)]
struct CommitSummary {
    duration_s: f64,
    throughput_mbps: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    errors: u64,
    samples: usize,
}

#[derive(Debug, Serialize)]
struct GcRunJson {
    duration_ms: u64,
    nodes_reaped: u64,
    pages_reclaimed: u64,
    bytes_reclaimed: u64,
    packs_compacted: u64,
    packs_deleted: u64,
}

#[derive(Debug, Serialize)]
struct ReclaimingGcRunJson {
    duration_ms: u64,
    nodes_reaped: u64,
    pages_reclaimed: u64,
    bytes_reclaimed: u64,
    packs_compacted: u64,
    packs_deleted: u64,
    ingest_mbps: f64,
    commit_p99_ms: f64,
}

#[derive(Debug, Serialize)]
struct PassJson {
    gc_under_60s: bool,
    reclaiming_cycle_reaped_nodes: bool,
    ingest_at_200_mbps: bool,
    p99_under_2x_idle: bool,
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn bench_config() -> BenchConfig {
    BenchConfig {
        nodes: env_u64("SNAPSTORE_GC_NODES", 100_000),
        target_physical_gb: env_f64("SNAPSTORE_GC_PHYSICAL_GB", 30.0),
        target_garbage_fraction: env_f64("SNAPSTORE_GC_GARBAGE_FRACTION", 0.50),
        ingest_target_mbps: env_f64("SNAPSTORE_GC_INGEST_MBPS", 200.0),
        seed: env_u64("SNAPSTORE_GC_SEED", DEFAULT_SEED),
        gc_mode: std::env::var("SNAPSTORE_GC_MODE")
            .unwrap_or_else(|_| "trigger_gc_aggressive".to_string()),
        gc_opts: GcOptsJson {
            compact_threshold: 0.9,
            rotate_active_first: true,
            tombstone_grace_cycles: 1,
        },
        commit_workers: env_usize("SNAPSTORE_GC_COMMIT_WORKERS", 4).max(1),
        commit_payload_mib: env_usize("SNAPSTORE_GC_COMMIT_PAYLOAD_MIB", 8).max(1),
        idle_seconds: env_u64("SNAPSTORE_GC_IDLE_SECONDS", 60).max(1),
    }
}

fn bench_tempdir(prefix: &str) -> TempDir {
    let root = std::env::var_os("SNAPSTORE_BENCH_ROOT")
        .expect("SNAPSTORE_BENCH_ROOT is required for gc_readiness_bench");
    TempBuilder::new()
        .prefix(prefix)
        .tempdir_in(root)
        .expect("create benchmark tempdir in SNAPSTORE_BENCH_ROOT")
}

fn open_state(root: &Path) -> (Arc<SnapshotStore>, Arc<MetaDb>) {
    let registry = prometheus::Registry::new();
    let metrics = Metrics::new(&registry);
    let config = ServerConfig {
        data_root: root.to_path_buf(),
        grpc_tcp_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_uds_path: Some(root.join("snapstore.sock")),
        page_channel_path: None,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: Default::default(),
        gc: Default::default(),
    };
    let state = run_startup(&config, &metrics).expect("run_startup");
    (Arc::new(state.store), Arc::new(state.meta))
}

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

fn phase_name(phase: u8) -> &'static str {
    match phase {
        PHASE_IDLE => "idle",
        PHASE_GRACE => "during_grace_gc",
        PHASE_RECLAIMING => "during_reclaiming_gc",
        PHASE_STOP => "stop",
        _ => "unknown",
    }
}

fn page(seed: u64, namespace: u64, seq: u64, idx: u64) -> Box<[u8; PAGE_SIZE]> {
    let mut p = Box::new([0u8; PAGE_SIZE]);
    let mut x = seed
        ^ namespace.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ seq.wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ idx.wrapping_mul(0x94d0_49bb_1331_11eb);
    for chunk in p.chunks_mut(8) {
        x ^= x >> 30;
        x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^= x >> 31;
        chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
    }
    p
}

fn commit_snapshot(
    store: &SnapshotStore,
    seed: u64,
    namespace: u64,
    seq: u64,
    pages: u64,
) -> Result<SnapshotRef, String> {
    let pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..pages)
        .map(|idx| page(seed, namespace, seq, idx))
        .collect();
    let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
    store.pages().ingest(&refs).map_err(|e| e.to_string())?;
    let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
        .iter()
        .enumerate()
        .map(|(idx, p)| (idx as u64, p.as_ref()))
        .collect();
    let container =
        build_full_container(pairs.len() as u64 * PAGE_SIZE as u64, &pairs, empty_blob());
    store.put_snapshot(&container).map_err(|e| e.to_string())
}

fn attach_node(
    store: &SnapshotStore,
    meta: &MetaDb,
    experiment_id: &ExperimentId,
    node_id: NodeId,
    parent_node_id: Option<NodeId>,
    snapshot_ref: SnapshotRef,
) -> Result<(), String> {
    let gate = store.commit_gate();
    store
        .register_live_ref(&gate, &snapshot_ref)
        .map_err(|e| e.to_string())?;
    meta.create_node(CreateNodeParams {
        experiment_id: experiment_id.clone(),
        node_id,
        parent_node_id,
        snapshot_ref,
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: node_id.0,
        virtual_ns: node_id.0,
        attrs: None,
    })
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn pages_per_node(config: &BenchConfig) -> u64 {
    let target_bytes = (config.target_physical_gb * 1024.0 * 1024.0 * 1024.0).max(PAGE_SIZE as f64);
    ((target_bytes / config.nodes.max(1) as f64) / PAGE_SIZE as f64)
        .floor()
        .max(1.0) as u64
}

fn count_subtree_nodes(root: u64, total_nodes: u64) -> u64 {
    if root >= total_nodes {
        return 0;
    }
    let mut count = 0;
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if node >= total_nodes {
            continue;
        }
        count += 1;
        let first_child = node.saturating_mul(BRANCHING).saturating_add(1);
        for child in first_child..first_child.saturating_add(BRANCHING) {
            if child < total_nodes {
                stack.push(child);
            }
        }
    }
    count
}

fn populate_and_prune(
    store: &SnapshotStore,
    meta: &MetaDb,
    config: &BenchConfig,
) -> PopulationJson {
    let experiment_id = ExperimentId::new("phase5-gc-tree").unwrap();
    let pages_per_node = pages_per_node(config);
    println!(
        "population: {} nodes, {} pages/node, target {:.2} GiB",
        config.nodes, pages_per_node, config.target_physical_gb
    );

    for node in 0..config.nodes {
        let snap_ref = commit_snapshot(store, config.seed, 1, node, pages_per_node)
            .unwrap_or_else(|e| panic!("commit population node {node}: {e}"));
        let parent = if node == 0 {
            None
        } else {
            Some(NodeId((node - 1) / BRANCHING))
        };
        attach_node(store, meta, &experiment_id, NodeId(node), parent, snap_ref)
            .unwrap_or_else(|e| panic!("attach population node {node}: {e}"));
        if node > 0 && node % 1000 == 0 {
            println!("population progress: {node}/{} nodes", config.nodes);
        }
    }

    let target_pruned_nodes = (config.nodes as f64 * config.target_garbage_fraction).round() as u64;
    let mut pruned_nodes = 0u64;
    let mut pruned_subtrees = Vec::new();
    for root in 1..=BRANCHING {
        if pruned_nodes >= target_pruned_nodes {
            break;
        }
        let subtree_nodes = count_subtree_nodes(root, config.nodes);
        if subtree_nodes == 0 {
            continue;
        }
        let reaped = meta
            .prune_subtree(experiment_id.clone(), NodeId(root), false)
            .unwrap_or_else(|e| panic!("prune subtree {root}: {e}"));
        assert_eq!(reaped, subtree_nodes, "tree model and DB prune must agree");
        pruned_nodes += subtree_nodes;
        pruned_subtrees.push(PrunedSubtreeJson {
            experiment_id: experiment_id.as_str().to_string(),
            node_id: root,
            nodes: subtree_nodes,
            predicted_garbage_bytes: subtree_nodes * pages_per_node * PAGE_SIZE as u64,
        });
    }

    PopulationJson {
        nodes_created: config.nodes,
        pages_per_node,
        physical_page_bytes_before_gc: config.nodes * pages_per_node * PAGE_SIZE as u64,
        predicted_garbage_bytes: pruned_nodes * pages_per_node * PAGE_SIZE as u64,
        pruned_subtrees,
    }
}

struct CommitterRun {
    phase: Arc<AtomicU8>,
    bytes_done: Arc<AtomicU64>,
    start: Instant,
    handles: Vec<std::thread::JoinHandle<Vec<CommitSample>>>,
}

impl CommitterRun {
    fn stop_and_join(self) -> (Duration, u64, Vec<CommitSample>) {
        self.phase.store(PHASE_STOP, Ordering::SeqCst);
        let duration = self.start.elapsed();
        let bytes_done = self.bytes_done.load(Ordering::SeqCst);
        let mut samples = Vec::new();
        for handle in self.handles {
            samples.extend(handle.join().expect("commit worker thread"));
        }
        (duration, bytes_done, samples)
    }
}

fn seed_commit_experiment(
    store: &SnapshotStore,
    meta: &MetaDb,
    config: &BenchConfig,
    experiment: &str,
    namespace: u64,
) -> ExperimentId {
    let experiment_id = ExperimentId::new(experiment).unwrap();
    let root_ref = commit_snapshot(store, config.seed, namespace, 0, 1)
        .unwrap_or_else(|e| panic!("commit workload root snapshot: {e}"));
    attach_node(store, meta, &experiment_id, NodeId::ROOT, None, root_ref)
        .unwrap_or_else(|e| panic!("commit workload root node: {e}"));
    experiment_id
}

fn spawn_committers(
    store: Arc<SnapshotStore>,
    meta: Arc<MetaDb>,
    config: &BenchConfig,
    experiment: &str,
    namespace: u64,
    initial_phase: u8,
) -> CommitterRun {
    let experiment_id = seed_commit_experiment(&store, &meta, config, experiment, namespace);
    let phase = Arc::new(AtomicU8::new(initial_phase));
    let bytes_reserved = Arc::new(AtomicU64::new(0));
    let bytes_done = Arc::new(AtomicU64::new(0));
    let next_node = Arc::new(AtomicU64::new(1));
    let start = Instant::now();
    let bytes_per_commit = (config.commit_payload_mib as u64) * 1024 * 1024;
    let pages_per_commit = (bytes_per_commit / PAGE_SIZE as u64).max(1);
    let target_bytes_per_sec = config.ingest_target_mbps * 1_000_000.0;

    let mut handles = Vec::with_capacity(config.commit_workers);
    for worker in 0..config.commit_workers {
        let store = Arc::clone(&store);
        let meta = Arc::clone(&meta);
        let experiment_id = experiment_id.clone();
        let phase = Arc::clone(&phase);
        let bytes_reserved = Arc::clone(&bytes_reserved);
        let bytes_done = Arc::clone(&bytes_done);
        let next_node = Arc::clone(&next_node);
        let seed = config.seed;
        let run_start = start;
        handles.push(std::thread::spawn(move || {
            let mut samples = Vec::new();
            loop {
                let phase_code = phase.load(Ordering::SeqCst);
                if phase_code == PHASE_STOP {
                    break;
                }
                let reserved = bytes_reserved
                    .fetch_add(bytes_per_commit, Ordering::SeqCst)
                    .saturating_add(bytes_per_commit);
                let target_elapsed =
                    Duration::from_secs_f64(reserved as f64 / target_bytes_per_sec);
                while run_start.elapsed() < target_elapsed {
                    if phase.load(Ordering::SeqCst) == PHASE_STOP {
                        return samples;
                    }
                    let remaining = target_elapsed.saturating_sub(run_start.elapsed());
                    std::thread::sleep(remaining.min(Duration::from_millis(10)));
                }

                let node_id = next_node.fetch_add(1, Ordering::SeqCst);
                let commit_start = Instant::now();
                let result = commit_snapshot(
                    &store,
                    seed,
                    namespace + worker as u64 + 1,
                    node_id,
                    pages_per_commit,
                )
                .and_then(|snap_ref| {
                    attach_node(
                        &store,
                        &meta,
                        &experiment_id,
                        NodeId(node_id),
                        Some(NodeId::ROOT),
                        snap_ref,
                    )
                });
                let latency_ms = commit_start.elapsed().as_secs_f64() * 1e3;
                match result {
                    Ok(()) => {
                        bytes_done.fetch_add(bytes_per_commit, Ordering::SeqCst);
                        samples.push(CommitSample {
                            phase: phase_name(phase_code).to_string(),
                            latency_ms,
                            bytes: bytes_per_commit,
                            ok: true,
                            error: String::new(),
                        });
                    }
                    Err(error) => samples.push(CommitSample {
                        phase: phase_name(phase_code).to_string(),
                        latency_ms,
                        bytes: bytes_per_commit,
                        ok: false,
                        error,
                    }),
                }
            }
            samples
        }));
    }

    CommitterRun {
        phase,
        bytes_done,
        start,
        handles,
    }
}

fn summarize_commits(
    duration: Duration,
    bytes_done: u64,
    samples: &[CommitSample],
) -> CommitSummary {
    let mut latencies: Vec<f64> = samples
        .iter()
        .filter(|s| s.ok)
        .map(|s| s.latency_ms)
        .collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let errors = samples.iter().filter(|s| !s.ok).count() as u64;
    CommitSummary {
        duration_s: duration.as_secs_f64(),
        throughput_mbps: bytes_done as f64 / duration.as_secs_f64().max(0.001) / 1_000_000.0,
        p50_ms: percentile_or_zero(&latencies, 0.50),
        p95_ms: percentile_or_zero(&latencies, 0.95),
        p99_ms: percentile_or_zero(&latencies, 0.99),
        errors,
        samples: samples.len(),
    }
}

fn percentile_or_zero(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn gc_opts(config: &BenchConfig) -> GcOpts {
    GcOpts {
        compact_threshold: config.gc_opts.compact_threshold,
        rotate_active_first: config.gc_opts.rotate_active_first,
        tombstone_grace_cycles: config.gc_opts.tombstone_grace_cycles,
    }
}

fn gc_run_json(report: &GcReport) -> GcRunJson {
    GcRunJson {
        duration_ms: report.duration_ms,
        nodes_reaped: report.nodes_reaped,
        pages_reclaimed: report.pages_reclaimed,
        bytes_reclaimed: report.bytes_reclaimed,
        packs_compacted: report.packs_compacted,
        packs_deleted: report.packs_deleted,
    }
}

fn reclaim_samples(samples: &[CommitSample]) -> Vec<f64> {
    let mut latencies: Vec<f64> = samples
        .iter()
        .filter(|s| s.ok && s.phase == "during_reclaiming_gc")
        .map(|s| s.latency_ms)
        .collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    latencies
}

fn reclaim_bytes(samples: &[CommitSample]) -> u64 {
    samples
        .iter()
        .filter(|s| s.ok && s.phase == "during_reclaiming_gc")
        .map(|s| s.bytes)
        .sum()
}

fn write_outputs(output: &BenchOutput, samples: &[CommitSample]) {
    let Ok(json_path) = std::env::var("SNAPSTORE_GC_BENCH_JSON") else {
        return;
    };
    let json_path = PathBuf::from(json_path);
    if let Some(parent) = json_path.parent() {
        std::fs::create_dir_all(parent).expect("create SNAPSTORE_GC_BENCH_JSON parent");
        let csv_path = parent.join("commit-latencies.csv");
        let mut csv = String::from("phase,latency_ms,bytes,ok,error\n");
        for sample in samples {
            csv.push_str(&format!(
                "{},{:.6},{},{},{}\n",
                sample.phase,
                sample.latency_ms,
                sample.bytes,
                sample.ok,
                sample.error.replace(',', ";")
            ));
        }
        std::fs::write(&csv_path, csv).expect("write commit latency CSV");
    }
    let bytes = serde_json::to_vec_pretty(output).expect("serialize GC benchmark JSON");
    std::fs::write(&json_path, bytes).expect("write SNAPSTORE_GC_BENCH_JSON");
    println!("wrote {}", json_path.display());
}

#[test]
#[ignore = "M7 GC readiness measurement; run in release on qualified hardware"]
fn gc_readiness_benchmark() {
    let config = bench_config();
    assert_eq!(
        config.gc_mode, "trigger_gc_aggressive",
        "only trigger_gc_aggressive is counted for Phase 5 readiness"
    );

    println!("== idle commit baseline ==");
    let idle_dir = bench_tempdir("snapstore-gc-idle-");
    let (idle_store, idle_meta) = open_state(idle_dir.path());
    let idle_run = spawn_committers(
        idle_store,
        idle_meta,
        &config,
        "phase5-idle-commits",
        10_000,
        PHASE_IDLE,
    );
    std::thread::sleep(Duration::from_secs(config.idle_seconds));
    let (idle_duration, idle_bytes, idle_samples) = idle_run.stop_and_join();
    let idle_summary = summarize_commits(idle_duration, idle_bytes, &idle_samples);
    println!(
        "idle: {:.1} MB/s, p99 {:.2} ms, samples {}, errors {}",
        idle_summary.throughput_mbps,
        idle_summary.p99_ms,
        idle_summary.samples,
        idle_summary.errors
    );

    println!("== populate and prune GC dataset ==");
    let gc_dir = bench_tempdir("snapstore-gc-reclaim-");
    let (store, meta) = open_state(gc_dir.path());
    let population = populate_and_prune(&store, &meta, &config);

    println!("== grace + reclaiming GC under commit load ==");
    let commit_run = spawn_committers(
        Arc::clone(&store),
        Arc::clone(&meta),
        &config,
        "phase5-gc-commits",
        20_000,
        PHASE_GRACE,
    );
    let opts = gc_opts(&config);
    let grace = run_gc_cycle(&store, &meta, &opts, &GcHooks::none()).expect("grace GC cycle");
    commit_run.phase.store(PHASE_RECLAIMING, Ordering::SeqCst);
    let reclaim_start = Instant::now();
    let reclaim =
        run_gc_cycle(&store, &meta, &opts, &GcHooks::none()).expect("reclaiming GC cycle");
    let reclaim_wall = reclaim_start.elapsed();
    let (gc_commit_duration, gc_commit_bytes, gc_samples) = commit_run.stop_and_join();
    let _gc_commit_summary = summarize_commits(gc_commit_duration, gc_commit_bytes, &gc_samples);

    let reclaim_latencies = reclaim_samples(&gc_samples);
    let reclaim_ingest_mbps =
        reclaim_bytes(&gc_samples) as f64 / reclaim_wall.as_secs_f64().max(0.001) / 1_000_000.0;
    let reclaim_p99 = percentile_or_zero(&reclaim_latencies, 0.99);

    let reclaiming_gc_run = ReclaimingGcRunJson {
        duration_ms: reclaim.duration_ms,
        nodes_reaped: reclaim.nodes_reaped,
        pages_reclaimed: reclaim.pages_reclaimed,
        bytes_reclaimed: reclaim.bytes_reclaimed,
        packs_compacted: reclaim.packs_compacted,
        packs_deleted: reclaim.packs_deleted,
        ingest_mbps: reclaim_ingest_mbps,
        commit_p99_ms: reclaim_p99,
    };
    let pass = PassJson {
        gc_under_60s: reclaim.duration_ms < 60_000,
        reclaiming_cycle_reaped_nodes: reclaim.nodes_reaped > 0,
        ingest_at_200_mbps: reclaim_ingest_mbps >= config.ingest_target_mbps,
        p99_under_2x_idle: idle_summary.p99_ms > 0.0 && reclaim_p99 < 2.0 * idle_summary.p99_ms,
    };

    let json_path = std::env::var("SNAPSTORE_GC_BENCH_JSON").ok();
    let latency_csv = json_path.as_ref().and_then(|p| {
        Path::new(p)
            .parent()
            .map(|parent| parent.join("commit-latencies.csv").display().to_string())
    });
    let output = BenchOutput {
        config,
        population,
        idle_commit: idle_summary,
        grace_gc_run: gc_run_json(&grace),
        reclaiming_gc_run,
        pass,
        latency_csv,
    };
    let mut all_samples = idle_samples;
    all_samples.extend(gc_samples);
    write_outputs(&output, &all_samples);

    println!(
        "reclaiming GC: {} ms, reaped {} nodes, reclaimed {:.2} GiB, ingest {:.1} MB/s, p99 {:.2}/{:.2} ms",
        output.reclaiming_gc_run.duration_ms,
        output.reclaiming_gc_run.nodes_reaped,
        output.reclaiming_gc_run.bytes_reclaimed as f64 / 1024.0 / 1024.0 / 1024.0,
        output.reclaiming_gc_run.ingest_mbps,
        output.reclaiming_gc_run.commit_p99_ms,
        output.idle_commit.p99_ms
    );
}
