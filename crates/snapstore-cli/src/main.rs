//! `snapstorectl` — thin CLI over the `snapstore-client` blocking facade.
//!
//! All subcommands use `snapstore_client::blocking::SnapstoreClient`; the CLI
//! has no async code of its own.

#![deny(unsafe_code)]

use std::path::PathBuf;
use std::process;

use clap::{Args, Parser, Subcommand};
use snapstore_client::{
    blocking::SnapstoreClient,
    snapstore_proto::{
        NodeStatus as ProtoNodeStatus, QueryNodesRequest, QueryOrder as ProtoQueryOrder,
    },
    transport::Transport,
};
use snapstore_manifest::Manifest;
use snapstore_types::SnapshotRef;

// ── CLI definition ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "snapstorectl", about = "snapstore CLI")]
struct Cli {
    /// gRPC endpoint.
    ///
    /// Forms: uds:<path>  |  tcp:<host:port>  |  auto:<uds-path>,<tcp-addr>
    /// Default: tries uds:./snapstore.sock then tcp:127.0.0.1:7410 (auto mode).
    #[arg(long, global = true)]
    endpoint: Option<String>,

    /// Emit machine-readable JSON output where applicable.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fetch store / experiment stats.
    Stats(StatsArgs),

    /// Decode and print a stored snapshot manifest.
    DumpManifest(DumpManifestArgs),

    /// Fetch a single node.
    GetNode(GetNodeArgs),

    /// Filtered node scan.
    Query(QueryArgs),

    /// Prune a subtree.
    Prune(PruneArgs),

    /// Pin a snapshot ref.
    Pin(PinArgs),

    /// Unpin a snapshot ref.
    Unpin(UnpinArgs),

    /// Key-value metadata subcommands.
    Kv(KvArgs),

    /// Benchmark: drive a PutPages stream with synthetic pages.
    Bench(BenchArgs),

    /// Offline integrity check (no server required).
    Fsck(FsckArgs),

    /// Trigger a GC cycle.
    Gc(GcArgs),
}

#[derive(Args)]
struct GcArgs {
    /// Compact aggressively: threshold 0.9 + rotate the active pack first.
    #[arg(long)]
    aggressive: bool,
    /// Fire-and-forget: return immediately, poll Stats for progress.
    #[arg(long)]
    detach: bool,
}

#[derive(Args)]
struct StatsArgs {
    /// Experiment ID to scope stats; omit for global stats.
    #[arg(long)]
    experiment: Option<String>,
}

#[derive(Args)]
struct DumpManifestArgs {
    /// 64-char hex snapshot ref.
    ref_hex: String,
}

#[derive(Args)]
struct GetNodeArgs {
    experiment: String,
    node_id: u64,
}

#[derive(Args)]
struct QueryArgs {
    experiment: String,

    /// Filter by node status.
    #[arg(long, value_name = "STATUS")]
    status: Option<NodeStatusArg>,

    /// Filter by parent node id.
    #[arg(long)]
    parent: Option<u64>,

    /// Cursor: only nodes created after this logical counter.
    #[arg(long)]
    created_after: Option<u64>,

    /// Cursor: only nodes updated after this logical counter.
    #[arg(long)]
    updated_after: Option<u64>,

    /// Maximum number of nodes to return.
    #[arg(long)]
    limit: Option<u32>,

    /// Sort order.
    #[arg(long, value_name = "ORDER")]
    order: Option<QueryOrderArg>,
}

#[derive(Clone, clap::ValueEnum)]
enum NodeStatusArg {
    Frontier,
    Expanded,
    Pruned,
    Goal,
}

#[derive(Clone, clap::ValueEnum)]
enum QueryOrderArg {
    Created,
    Updated,
    NodeId,
}

#[derive(Args)]
struct PruneArgs {
    experiment: String,
    node_id: u64,
    /// Allow pruning the root node.
    #[arg(long)]
    allow_root: bool,
}

#[derive(Args)]
struct PinArgs {
    /// 64-char hex snapshot ref.
    ref_hex: String,
    /// Human-readable note.
    #[arg(long, default_value = "")]
    note: String,
}

#[derive(Args)]
struct UnpinArgs {
    /// 64-char hex snapshot ref.
    ref_hex: String,
}

#[derive(Args)]
struct KvArgs {
    #[command(subcommand)]
    subcmd: KvSubcmd,
}

#[derive(Subcommand)]
enum KvSubcmd {
    /// Fetch a key.
    Get { key: String },
    /// Store a key-value pair.
    Put {
        key: String,
        value: String,
        /// CAS: value must match this generation; 0 = key must be absent.
        #[arg(long)]
        expected_generation: Option<u64>,
    },
    /// Delete a key.
    Delete {
        key: String,
        /// CAS guard: generation must match before deletion.
        #[arg(long)]
        expected_generation: Option<u64>,
    },
}

#[derive(Args)]
struct BenchArgs {
    /// Number of 4096-byte pages to upload.
    #[arg(long, default_value_t = 65536)]
    pages: usize,

    /// Pages per PutPages stream message.
    #[arg(long, default_value_t = 256)]
    msg_pages: usize,

    /// Upload the same set twice; report the second pass (dedup-warm).
    #[arg(long)]
    warm: bool,
}

#[derive(Args)]
struct FsckArgs {
    /// Root of the store directory (contains pages/, manifests/).
    #[arg(long)]
    store_root: PathBuf,

    /// Path to the SQLite meta database (tree.db).
    #[arg(long)]
    meta_db: PathBuf,

    /// Re-hash every page payload and pack body.
    #[arg(long)]
    deep: bool,
}

// ── endpoint parsing ───────────────────────────────────────────────────────────

fn parse_transport(endpoint: Option<&str>) -> Result<Transport, String> {
    match endpoint {
        None => Ok(Transport::Auto {
            uds_path: PathBuf::from("./snapstore.sock"),
            tcp_addr: "http://127.0.0.1:7410".to_owned(),
            page_channel_path: None,
        }),
        Some(s) => {
            if let Some(path) = s.strip_prefix("uds:") {
                Ok(Transport::Uds(PathBuf::from(path)))
            } else if let Some(addr) = s.strip_prefix("tcp:") {
                // Normalise to http:// if not already a URI.
                let addr = if addr.starts_with("http://") || addr.starts_with("https://") {
                    addr.to_owned()
                } else {
                    format!("http://{addr}")
                };
                Ok(Transport::Tcp(addr))
            } else if let Some(rest) = s.strip_prefix("auto:") {
                let mut parts = rest.splitn(2, ',');
                let uds = parts
                    .next()
                    .ok_or_else(|| format!("auto: expected <uds-path>,<tcp-addr> in {s}"))?;
                let tcp = parts
                    .next()
                    .ok_or_else(|| format!("auto: expected <uds-path>,<tcp-addr> in {s}"))?;
                let tcp = if tcp.starts_with("http://") || tcp.starts_with("https://") {
                    tcp.to_owned()
                } else {
                    format!("http://{tcp}")
                };
                Ok(Transport::Auto {
                    uds_path: PathBuf::from(uds),
                    tcp_addr: tcp,
                    page_channel_path: None,
                })
            } else {
                Err(format!(
                    "unknown endpoint scheme {s:?}; expected uds:<path>, tcp:<addr>, or auto:<uds>,<tcp>"
                ))
            }
        }
    }
}

// ── hex helpers ────────────────────────────────────────────────────────────────

fn parse_hex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!(
            "expected 64 hex chars, got {} chars: {s:?}",
            s.len()
        ));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = nibble(chunk[0]).ok_or_else(|| {
            format!(
                "invalid hex char at position {}: {}",
                i * 2,
                chunk[0] as char
            )
        })?;
        let lo = nibble(chunk[1]).ok_or_else(|| {
            format!(
                "invalid hex char at position {}: {}",
                i * 2 + 1,
                chunk[1] as char
            )
        })?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex_from_bytes(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ── main ───────────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    let transport = match parse_transport(cli.endpoint.as_deref()) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: {e}");
            process::exit(2);
        }
    };

    let exit_code = match run_command(cli.command, transport, cli.json) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: {e}");
            1
        }
    };

    process::exit(exit_code);
}

fn run_command(cmd: Commands, transport: Transport, json: bool) -> Result<(), String> {
    match cmd {
        Commands::Gc(args) => {
            let client = connect(transport)?;
            run_gc(&client, args)
        }

        Commands::Fsck(args) => {
            let report = snapstore_crash::fsck::fsck(&args.store_root, &args.meta_db, args.deep);
            let output =
                serde_json::to_string_pretty(&report).map_err(|e| format!("serialise: {e}"))?;
            println!("{output}");
            if !report.ok() {
                process::exit(1);
            }
            Ok(())
        }

        Commands::Bench(args) => run_bench(transport, &args),

        // Subcommands below need a live server connection.
        Commands::Stats(args) => {
            let client = connect(transport)?;
            run_stats(&client, args, json)
        }

        Commands::DumpManifest(args) => {
            let client = connect(transport)?;
            run_dump_manifest(&client, &args)
        }

        Commands::GetNode(args) => {
            let client = connect(transport)?;
            run_get_node(&client, args, json)
        }

        Commands::Query(args) => {
            let client = connect(transport)?;
            run_query(&client, args, json)
        }

        Commands::Prune(args) => {
            let client = connect(transport)?;
            run_prune(&client, args)
        }

        Commands::Pin(args) => {
            let client = connect(transport)?;
            run_pin(&client, args)
        }

        Commands::Unpin(args) => {
            let client = connect(transport)?;
            run_unpin(&client, args)
        }

        Commands::Kv(args) => {
            let client = connect(transport)?;
            run_kv(&client, args)
        }
    }
}

fn connect(transport: Transport) -> Result<SnapstoreClient, String> {
    SnapstoreClient::connect(transport).map_err(|e| format!("connect: {e}"))
}

// ── stats ──────────────────────────────────────────────────────────────────────

fn run_stats(client: &SnapstoreClient, args: StatsArgs, json: bool) -> Result<(), String> {
    let resp = client
        .stats(args.experiment)
        .map_err(|e| format!("stats: {e}"))?;

    if json {
        // Print a simple JSON representation.
        let store = &resp.store;
        let store_json = match store {
            Some(s) => serde_json::json!({
                "unique_pages": s.unique_pages,
                "physical_page_bytes": s.physical_page_bytes,
                "manifests_total": s.manifests_total,
                "logical_page_bytes": s.logical_page_bytes,
                "dedup_ratio": s.dedup_ratio,
                "experiments_total": s.experiments_total,
                "nodes_total": s.nodes_total,
                "pins_total": s.pins_total,
                "tombstones_total": s.tombstones_total,
                "logical_counter": s.logical_counter,
                "gc_runs_total": s.gc_runs_total,
                "gc_pages_reclaimed_total": s.gc_pages_reclaimed_total,
                "gc_bytes_reclaimed_total": s.gc_bytes_reclaimed_total,
                "gc_last_finished_logical_counter": s.gc_last_finished_logical_counter,
            }),
            None => serde_json::json!(null),
        };
        let exp_json = resp.experiment.as_ref().map(|e| {
            serde_json::json!({
                "experiment_id": e.experiment_id,
                "nodes_total": e.nodes_total,
                "nodes_frontier": e.nodes_frontier,
                "nodes_expanded": e.nodes_expanded,
                "nodes_pruned": e.nodes_pruned,
                "nodes_goal": e.nodes_goal,
                "max_depth": e.max_depth,
                "input_logs_total": e.input_logs_total,
                "input_log_bytes": e.input_log_bytes,
            })
        });
        let out = serde_json::json!({ "store": store_json, "experiment": exp_json });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        println!("=== Store Stats ===");
        if let Some(s) = &resp.store {
            println!("  unique_pages           : {}", s.unique_pages);
            println!("  physical_page_bytes    : {}", s.physical_page_bytes);
            println!("  manifests_total        : {}", s.manifests_total);
            println!("  logical_page_bytes     : {}", s.logical_page_bytes);
            println!("  dedup_ratio            : {:.4}", s.dedup_ratio);
            println!("  experiments_total      : {}", s.experiments_total);
            println!("  nodes_total            : {}", s.nodes_total);
            println!("  pins_total             : {}", s.pins_total);
            println!("  logical_counter        : {}", s.logical_counter);
        }
        if let Some(e) = &resp.experiment {
            println!("\n=== Experiment: {} ===", e.experiment_id);
            println!("  nodes_total    : {}", e.nodes_total);
            println!("  nodes_frontier : {}", e.nodes_frontier);
            println!("  nodes_expanded : {}", e.nodes_expanded);
            println!("  nodes_pruned   : {}", e.nodes_pruned);
            println!("  nodes_goal     : {}", e.nodes_goal);
            println!("  max_depth      : {}", e.max_depth);
        }
    }
    Ok(())
}

// ── dump-manifest ──────────────────────────────────────────────────────────────

fn run_dump_manifest(client: &SnapstoreClient, args: &DumpManifestArgs) -> Result<(), String> {
    let ref_bytes = parse_hex32(&args.ref_hex)?;
    let snapshot_ref = SnapshotRef::from_bytes(ref_bytes);

    let container = client
        .get_snapshot(snapshot_ref)
        .map_err(|e| format!("get_snapshot: {e}"))?;

    let manifest = Manifest::decode(&container).map_err(|e| format!("decode manifest: {e}"))?;

    println!("snapshot_ref   : {}", args.ref_hex);
    println!("version        : {}", manifest.version);
    println!(
        "type           : {}",
        if manifest.delta { "DELTA" } else { "FULL" }
    );
    println!(
        "parent         : {}",
        manifest
            .parent
            .as_ref()
            .map(|r| hex_from_bytes(&r.to_bytes()))
            .unwrap_or_else(|| "none".to_owned())
    );
    println!("guest_ram_bytes: {}", manifest.guest_ram_bytes);
    println!("entry_count    : {}", manifest.entries.len());
    println!(
        "device_blob    : format={} len={} zstd={} raw_len={}",
        manifest.device_blob.format,
        manifest.device_blob.bytes.len(),
        manifest.device_blob.zstd,
        manifest.device_blob.raw_len
    );

    // First/last few entries.
    let n = manifest.entries.len();
    let show = 3usize.min(n);
    if show > 0 {
        println!("\n--- first {show} entries ---");
        for e in manifest.entries.iter().take(show) {
            println!(
                "  page_index={} hash={}",
                e.page_index,
                hex_from_bytes(e.page_hash.as_bytes())
            );
        }
        if n > show * 2 {
            println!("  ... ({} more) ...", n - show * 2);
        }
        if n > show {
            println!("--- last {show} entries ---");
            for e in manifest
                .entries
                .iter()
                .rev()
                .take(show)
                .collect::<Vec<_>>()
                .iter()
                .rev()
            {
                println!(
                    "  page_index={} hash={}",
                    e.page_index,
                    hex_from_bytes(e.page_hash.as_bytes())
                );
            }
        }
    }

    Ok(())
}

// ── get-node ───────────────────────────────────────────────────────────────────

fn run_get_node(client: &SnapstoreClient, args: GetNodeArgs, json: bool) -> Result<(), String> {
    let node = client
        .get_node(args.experiment, args.node_id)
        .map_err(|e| format!("get_node: {e}"))?;

    if json {
        let out = node_meta_to_json(&node);
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        print_node_meta(&node);
    }
    Ok(())
}

// ── query ──────────────────────────────────────────────────────────────────────

fn run_query(client: &SnapstoreClient, args: QueryArgs, json: bool) -> Result<(), String> {
    let status_filter = args.status.as_ref().map(|s| match s {
        NodeStatusArg::Frontier => ProtoNodeStatus::Frontier as i32,
        NodeStatusArg::Expanded => ProtoNodeStatus::Expanded as i32,
        NodeStatusArg::Pruned => ProtoNodeStatus::Pruned as i32,
        NodeStatusArg::Goal => ProtoNodeStatus::Goal as i32,
    });

    let order = match &args.order {
        None => ProtoQueryOrder::Unspecified as i32,
        Some(QueryOrderArg::Created) => ProtoQueryOrder::CreatedAt as i32,
        Some(QueryOrderArg::Updated) => ProtoQueryOrder::UpdatedAt as i32,
        Some(QueryOrderArg::NodeId) => ProtoQueryOrder::NodeId as i32,
    };

    let req = QueryNodesRequest {
        experiment_id: args.experiment,
        status: status_filter,
        parent_node_id: args.parent,
        min_depth: None,
        max_depth: None,
        created_after: args.created_after,
        updated_after: args.updated_after,
        order,
        limit: args.limit.unwrap_or(0),
    };

    let nodes = client
        .query_nodes(req)
        .map_err(|e| format!("query_nodes: {e}"))?;

    if json {
        let arr: Vec<_> = nodes.iter().map(node_meta_to_json).collect();
        println!("{}", serde_json::to_string_pretty(&arr).unwrap());
    } else {
        println!(
            "{:<8}  {:<10}  {:<10}  {:<10}  {:<12}  {:<12}",
            "node_id", "status", "parent", "depth", "created_at", "updated_at"
        );
        for n in &nodes {
            println!(
                "{:<8}  {:<10}  {:<10}  {:<10}  {:<12}  {:<12}",
                n.node_id,
                node_status_name(n.status),
                n.parent_node_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                n.depth,
                n.created_at,
                n.updated_at,
            );
        }
        println!("({} nodes)", nodes.len());
    }
    Ok(())
}

// ── prune ──────────────────────────────────────────────────────────────────────

fn run_prune(client: &SnapstoreClient, args: PruneArgs) -> Result<(), String> {
    let pruned = client
        .prune_subtree(args.experiment, args.node_id, args.allow_root)
        .map_err(|e| format!("prune_subtree: {e}"))?;
    println!("pruned {pruned} nodes");
    Ok(())
}

// ── pin / unpin ────────────────────────────────────────────────────────────────

fn run_pin(client: &SnapstoreClient, args: PinArgs) -> Result<(), String> {
    let ref_bytes = parse_hex32(&args.ref_hex)?;
    let snapshot_ref = SnapshotRef::from_bytes(ref_bytes);
    let newly = client
        .pin(snapshot_ref, args.note)
        .map_err(|e| format!("pin: {e}"))?;
    println!("newly_pinned: {newly}");
    Ok(())
}

fn run_unpin(client: &SnapstoreClient, args: UnpinArgs) -> Result<(), String> {
    let ref_bytes = parse_hex32(&args.ref_hex)?;
    let snapshot_ref = SnapshotRef::from_bytes(ref_bytes);
    let was = client
        .unpin(snapshot_ref)
        .map_err(|e| format!("unpin: {e}"))?;
    println!("was_pinned: {was}");
    Ok(())
}

// ── gc ─────────────────────────────────────────────────────────────────────────

/// Exit 0 on success, 2 when the server reports `already_running`
/// (script-friendly distinct code — the caller can retry later rather than
/// treating it as a hard failure).
fn run_gc(client: &SnapstoreClient, args: GcArgs) -> Result<(), String> {
    let resp = client
        .trigger_gc(args.aggressive, args.detach)
        .map_err(|e| format!("trigger_gc: {e}"))?;

    if resp.already_running {
        eprintln!("gc: already running");
        process::exit(2);
    }

    if args.detach {
        println!("gc: started (detached)");
    } else {
        println!(
            "gc: nodes_reaped={} manifests_deleted={} pages_reclaimed={} bytes_reclaimed={} packs_compacted={} duration_ms={}",
            resp.nodes_reaped,
            resp.manifests_deleted,
            resp.pages_reclaimed,
            resp.bytes_reclaimed,
            resp.packs_compacted,
            resp.duration_ms,
        );
    }
    Ok(())
}

// ── kv ─────────────────────────────────────────────────────────────────────────

fn run_kv(client: &SnapstoreClient, args: KvArgs) -> Result<(), String> {
    match args.subcmd {
        KvSubcmd::Get { key } => {
            let (value, gen) = client
                .get_metadata(key.into_bytes())
                .map_err(|e| format!("get_metadata: {e}"))?;
            let val_str = String::from_utf8_lossy(&value);
            println!("value      : {val_str}");
            println!("generation : {gen}");
        }
        KvSubcmd::Put {
            key,
            value,
            expected_generation,
        } => {
            let gen = client
                .put_metadata(key.into_bytes(), value.into_bytes(), expected_generation)
                .map_err(|e| format!("put_metadata: {e}"))?;
            println!("generation: {gen}");
        }
        KvSubcmd::Delete {
            key,
            expected_generation,
        } => {
            let deleted = client
                .delete_metadata(key.into_bytes(), expected_generation)
                .map_err(|e| format!("delete_metadata: {e}"))?;
            println!("deleted: {deleted}");
        }
    }
    Ok(())
}

// ── bench put-pages ────────────────────────────────────────────────────────────

fn run_bench(transport: Transport, args: &BenchArgs) -> Result<(), String> {
    // Build synthetic pages: deterministic content, unique per index.
    let pages = make_bench_pages(args.pages);
    let msg_pages = args.msg_pages.max(1);

    let client = connect(transport)?;

    // Optionally do a warm-up pass (uploads same data twice; report second pass).
    if args.warm {
        let _ = send_pages_in_chunks(&client, &pages, msg_pages)
            .map_err(|e| format!("bench warm-up pass: {e}"))?;
    }

    let start = std::time::Instant::now();
    let (new, deduped) =
        send_pages_in_chunks(&client, &pages, msg_pages).map_err(|e| format!("bench: {e}"))?;
    let elapsed = start.elapsed();

    let total_bytes = args.pages as u64 * 4096;
    let mb_s = total_bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;

    println!(
        "pages={} msg_pages={} warm={} elapsed={:.3}s MB/s={:.1} pages_new={} pages_deduped={}",
        args.pages,
        msg_pages,
        args.warm,
        elapsed.as_secs_f64(),
        mb_s,
        new,
        deduped
    );
    Ok(())
}

/// Build deterministic 4096-byte pages.  Each page content is derived from its
/// index so every page is unique (good for cold benchmarks).
fn make_bench_pages(n: usize) -> Vec<(u64, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let mut page = vec![0u8; 4096];
            // Fill with a simple pattern derived from the index.
            let seed = i as u64;
            for (j, b) in page.iter_mut().enumerate() {
                *b = ((seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(j as u64))
                    & 0xff) as u8;
            }
            (i as u64, page)
        })
        .collect()
}

/// Send pages in chunks of `msg_pages`, collecting total (new, deduped).
fn send_pages_in_chunks(
    client: &SnapstoreClient,
    pages: &[(u64, Vec<u8>)],
    msg_pages: usize,
) -> Result<(u64, u64), String> {
    let mut total_new = 0u64;
    let mut total_deduped = 0u64;
    for chunk in pages.chunks(msg_pages) {
        let batch: Vec<(u64, Vec<u8>)> = chunk.to_vec();
        let (n, d) = client
            .put_pages(batch)
            .map_err(|e| format!("put_pages: {e}"))?;
        total_new += n;
        total_deduped += d;
    }
    Ok((total_new, total_deduped))
}

// ── display helpers ────────────────────────────────────────────────────────────

fn node_status_name(status: i32) -> &'static str {
    match ProtoNodeStatus::try_from(status) {
        Ok(ProtoNodeStatus::Unspecified) => "unspecified",
        Ok(ProtoNodeStatus::Frontier) => "frontier",
        Ok(ProtoNodeStatus::Expanded) => "expanded",
        Ok(ProtoNodeStatus::Pruned) => "pruned",
        Ok(ProtoNodeStatus::Goal) => "goal",
        Err(_) => "unknown",
    }
}

fn node_meta_to_json(n: &snapstore_client::snapstore_proto::NodeMeta) -> serde_json::Value {
    serde_json::json!({
        "experiment_id": n.experiment_id,
        "node_id": n.node_id,
        "parent_node_id": n.parent_node_id,
        "depth": n.depth,
        "snapshot_ref": hex_from_bytes(&n.snapshot_ref),
        "input_log_id": hex_from_bytes(&n.input_log_id),
        "status": node_status_name(n.status),
        "score": n.score,
        "visit_count": n.visit_count,
        "icount": n.icount,
        "virtual_ns": n.virtual_ns,
        "created_at": n.created_at,
        "updated_at": n.updated_at,
        "last_visited_at": n.last_visited_at,
    })
}

fn print_node_meta(n: &snapstore_client::snapstore_proto::NodeMeta) {
    println!("experiment_id : {}", n.experiment_id);
    println!("node_id       : {}", n.node_id);
    println!(
        "parent_node_id: {}",
        n.parent_node_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_owned())
    );
    println!("depth         : {}", n.depth);
    println!("status        : {}", node_status_name(n.status));
    println!("snapshot_ref  : {}", hex_from_bytes(&n.snapshot_ref));
    println!(
        "input_log_id  : {}",
        if n.input_log_id.is_empty() {
            "none".to_owned()
        } else {
            hex_from_bytes(&n.input_log_id)
        }
    );
    println!("visit_count   : {}", n.visit_count);
    println!("created_at    : {}", n.created_at);
    println!("updated_at    : {}", n.updated_at);
    if let Some(s) = n.score {
        println!("score         : {s:.6}");
    }
}
