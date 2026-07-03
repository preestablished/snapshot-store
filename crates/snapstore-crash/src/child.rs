// ── child — seeded synthetic workload ────────────────────────────────────────
//!
//! Run by the parent process via `std::process::Command`.  Opens a
//! `SnapshotStore` + `MetaDb` on a scratch directory and executes a
//! deterministic sequence of operations driven by a seeded PRNG.
//!
//! All acknowledged operations are appended to `<scratch>/oracle.journal`
//! opened with `O_SYNC` **after** the library call returns `Ok`.
//! "Journal line present" == "the caller observed success".

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use snapstore_manifest::input_log::InputLogContainer;
use snapstore_manifest::DeviceBlob;
use snapstore_meta::{CreateNodeParams, MetaDb, NodeUpdate};
use snapstore_store::build::{build_delta_container, build_full_container};
use snapstore_store::SnapshotStore;
use snapstore_types::{ExperimentId, LogId, NodeId, NodeStatus, SnapshotRef, PAGE_SIZE};

// ── Scenario ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Scenario {
    Default,
    SqliteBatch,
    /// Full-stack mode: the real `snapstore-server` binary is the kill target.
    /// The child process is NOT used for the workload in this scenario; the
    /// parent drives the server via gRPC.  This variant is accepted by the CLI
    /// so `--scenario full-stack` routes correctly through `run_cycles`.
    FullStack,
}

impl std::str::FromStr for Scenario {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "default" => Ok(Scenario::Default),
            "sqlite-batch" => Ok(Scenario::SqliteBatch),
            "full-stack" => Ok(Scenario::FullStack),
            _ => Err(format!("unknown scenario: {s}")),
        }
    }
}

// ── Journal ───────────────────────────────────────────────────────────────────

/// An O_SYNC journal writer.
struct Journal {
    writer: BufWriter<std::fs::File>,
}

impl Journal {
    fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .custom_flags(libc::O_SYNC)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Append a journal line and flush.  Called ONLY after the op returned Ok.
    fn record(&mut self, op: &str, key: &str, step: u64) -> std::io::Result<()> {
        writeln!(self.writer, "{op}\t{key}\t{step}")?;
        self.writer.flush()
    }

    /// Append a journal line whose third field is an arbitrary string rather
    /// than a plain step counter (used by `gc_done`, whose third field is
    /// `reaped=<exp:node,...>` per 05-crash-harness.md §1 — re-deriving
    /// grace-cycle arithmetic during journal replay would be fragile, so the
    /// reaped-subtree facts travel in the journal line itself).  Called ONLY
    /// after the op returned Ok.
    fn record_raw(&mut self, op: &str, key: &str, rest: &str) -> std::io::Result<()> {
        writeln!(self.writer, "{op}\t{key}\t{rest}")?;
        self.writer.flush()
    }
}

// ── Constants ─────────────────────────────────────────────────────────────────

const PAGES: usize = 8; // pages per FULL snapshot
const GUEST_RAM_BYTES: u64 = PAGES as u64 * PAGE_SIZE as u64;

// ── run_child ─────────────────────────────────────────────────────────────────

/// Entry point for the child process.
///
/// `force_gc`: when true (armed by the parent for `--failpoint gc-*` matrix
/// runs), the `Default` scenario forces `gc` ops early and repeatedly so an
/// armed GC failpoint is guaranteed to be hit within the op budget (05
/// §2) — mirrors how the sqlite-batch scenario forces its own path.
pub fn run_child(scratch: &Path, seed: u64, ops: u64, scenario: Scenario, force_gc: bool) {
    // Arm failpoints from the environment before touching anything.
    #[cfg(feature = "failpoints")]
    let _scenario_guard = fail::FailScenario::setup();

    let db_path = scratch.join("meta").join("tree.db");
    let store_root = scratch.join("store");

    let store = SnapshotStore::open(&store_root).expect("child: open store");
    let meta = MetaDb::open(&db_path).expect("child: open meta");

    let mut journal = Journal::open(&scratch.join("oracle.journal")).expect("child: open journal");
    let mut rng = StdRng::seed_from_u64(seed);

    match scenario {
        Scenario::Default => run_default(&store, &meta, &mut rng, &mut journal, ops, force_gc),
        Scenario::SqliteBatch => run_sqlite_batch(&store, &meta, &mut rng, &mut journal, ops),
        Scenario::FullStack => {
            // The full-stack scenario does not use a child process for the
            // workload; the parent drives the server directly via gRPC.
            // If somehow invoked as a child subcommand, just exit immediately.
            eprintln!(
                "snapstore-crash child: full-stack scenario does not use a child workload process"
            );
        }
    }
}

// ── Default scenario ──────────────────────────────────────────────────────────

fn run_default(
    store: &SnapshotStore,
    meta: &MetaDb,
    rng: &mut StdRng,
    journal: &mut Journal,
    ops: u64,
    force_gc: bool,
) {
    let exp_a = ExperimentId::new("exp-A").unwrap();
    let exp_b = ExperimentId::new("exp-B").unwrap();

    // Node history: (experiment_id_char, node_id) pairs
    let mut node_ids: Vec<(bool, NodeId)> = Vec::new(); // true = exp-A

    let mut prev_ref: Option<SnapshotRef> = None;

    // Committed snapshot refs, for `pin` op candidates.
    let mut snap_refs: Vec<SnapshotRef> = Vec::new();
    // Currently-pinned refs (per this child's view), for `unpin` op candidates.
    let mut pinned_refs: Vec<SnapshotRef> = Vec::new();
    // Local GC cycle counter for the `gc_done` journal line.
    let mut gc_cycle: u64 = 0;

    for step in 0..ops {
        let exp_a_turn = step % 2 == 0;
        let exp = if exp_a_turn { &exp_a } else { &exp_b };

        // ── ingest + put_snapshot ─────────────────────────────────────────────

        // Generate deterministic pages.
        let pages: Vec<[u8; PAGE_SIZE]> = (0..PAGES)
            .map(|i| make_page(seed_from(step, i as u64)))
            .collect();

        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().collect();
        if store.pages().ingest(&page_refs).is_err() {
            continue;
        }

        let container = match prev_ref.clone() {
            None => {
                // FULL
                let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (i as u64, p))
                    .collect();
                build_full_container(GUEST_RAM_BYTES, &page_pairs, empty_blob())
            }
            Some(ref parent) => {
                // DELTA — change a few pages
                let changed: Vec<(u64, &[u8; PAGE_SIZE])> = {
                    let n = (rng.gen::<usize>() % PAGES) + 1;
                    pages
                        .iter()
                        .enumerate()
                        .take(n)
                        .map(|(i, p)| (i as u64, p))
                        .collect()
                };
                build_delta_container(parent, GUEST_RAM_BYTES, &changed, empty_blob())
            }
        };

        match store.put_snapshot(&container) {
            Ok(snap_ref) => {
                let key = hex_ref(&snap_ref);
                if step == 0 {
                    journal.record("put_snapshot_full", &key, step).ok();
                } else {
                    journal.record("put_snapshot_delta", &key, step).ok();
                }
                snap_refs.push(snap_ref.clone());
                prev_ref = Some(snap_ref);
            }
            Err(_) => {
                prev_ref = None;
                continue;
            }
        }

        let snap_ref = prev_ref.clone().unwrap();

        // ── put_input_log ────────────────────────────────────────────────────

        let payload = format!("step={step} seed={}", rng.gen::<u64>()).into_bytes();
        let log_container = InputLogContainer::encode(1, &payload);
        let log_id =
            LogId::from_bytes(*blake3::hash(&log_container[..log_container.len() - 32]).as_bytes());

        if meta.put_input_log(log_id, &log_container).is_ok() {
            journal
                .record("put_input_log", &hex_log_id(&log_id), step)
                .ok();
        }

        // ── create_node ──────────────────────────────────────────────────────

        let node_id = NodeId(step + 1);
        let parent_node_id = if node_ids.is_empty() {
            // First node — create root (node_id=0) first.
            let root_params = CreateNodeParams {
                experiment_id: exp.clone(),
                node_id: NodeId(0),
                parent_node_id: None,
                snapshot_ref: snap_ref.clone(),
                input_log_id: None,
                inline_log_container: None,
                status: NodeStatus::Frontier,
                score: None,
                icount: 0,
                virtual_ns: 0,
                attrs: None,
            };
            if meta.create_node(root_params).is_ok() {
                journal
                    .record("create_node", &format!("{}/0", exp.as_str()), step)
                    .ok();
                // Node linkage for journal-replay reachability (05 §3 item
                // 1): key = exp/node/parent ("-" = none), rest = the node's
                // snapshot-ref hex. Lets recovery compute reaped-subtree
                // CLOSURES from `gc_done` roots (descendant node rows are
                // reaped along with the listed subtree root) without
                // re-deriving anything from meta.
                journal
                    .record_raw(
                        "node_edge",
                        &format!("{}/0/-", exp.as_str()),
                        &hex_ref(&snap_ref),
                    )
                    .ok();
                node_ids.push((exp_a_turn, NodeId(0)));
            }
            Some(NodeId(0))
        } else {
            // Pick a random existing node as parent (from same experiment if possible).
            let same: Vec<_> = node_ids.iter().filter(|(a, _)| *a == exp_a_turn).collect();
            if same.is_empty() {
                Some(NodeId(0))
            } else {
                let idx = rng.gen::<usize>() % same.len();
                Some(same[idx].1)
            }
        };

        let params = CreateNodeParams {
            experiment_id: exp.clone(),
            node_id,
            parent_node_id,
            snapshot_ref: snap_ref.clone(),
            input_log_id: Some(log_id),
            inline_log_container: None,
            status: NodeStatus::Frontier,
            score: Some(rng.gen::<f64>()),
            icount: rng.gen::<u64>() % 1_000_000,
            virtual_ns: rng.gen::<u64>() % 1_000_000_000,
            attrs: None,
        };
        if let Ok(row) = meta.create_node(params) {
            journal
                .record(
                    "create_node",
                    &format!("{}/{}", exp.as_str(), row.node_id.0),
                    step,
                )
                .ok();
            let parent_str = parent_node_id.map_or("-".to_string(), |p| p.0.to_string());
            journal
                .record_raw(
                    "node_edge",
                    &format!("{}/{}/{}", exp.as_str(), row.node_id.0, parent_str),
                    &hex_ref(&snap_ref),
                )
                .ok();
            node_ids.push((exp_a_turn, node_id));
        }

        // ── batch update_nodes every ~8 steps ────────────────────────────────

        if step > 0 && step % 8 == 0 && node_ids.len() >= 4 {
            let batch_id = step;
            let ids_to_update: Vec<NodeId> = node_ids
                .iter()
                .filter(|(a, _)| *a == exp_a_turn)
                .take(4)
                .map(|(_, id)| *id)
                .collect();

            let updates: Vec<NodeUpdate> = ids_to_update
                .iter()
                .map(|&id| {
                    let mut u = NodeUpdate::new(id);
                    u.attrs = Some(batch_id.to_le_bytes().to_vec());
                    u.visit_count_delta = 1;
                    u
                })
                .collect();

            if let Ok(updated_at) = meta.update_nodes(exp.clone(), updates) {
                journal
                    .record("update_nodes", &format!("{batch_id}@{updated_at}"), step)
                    .ok();
            }
        }

        // ── KV CAS checkpoint every ~16 steps ────────────────────────────────

        if step % 16 == 0 {
            let key = b"checkpoint".to_vec();
            let val = format!("step={step}").into_bytes();
            // Unconditional upsert
            if let Ok(gen) = meta.put_metadata(key.clone(), val, None) {
                journal
                    .record("put_metadata", &format!("checkpoint@{gen}"), step)
                    .ok();
            }
        }

        // ── Prune a leaf subtree occasionally ────────────────────────────────

        if step > 0 && step % 32 == 0 {
            // Pick a leaf node (one without children in our local list).
            let leaf_candidates: Vec<_> = node_ids
                .iter()
                .filter(|(a, id)| {
                    *a == exp_a_turn
                        && !id.is_root()
                        && !node_ids.iter().any(|(_, other)| {
                            // Rough approximation: no other node is immediately after it
                            other.0 == id.0 + 1
                        })
                })
                .collect();

            if let Some((_, leaf_id)) = leaf_candidates.last() {
                if meta.prune_subtree(exp.clone(), *leaf_id, false).is_ok() {
                    // Journaled so recovery can mark this subtree's refs
                    // INDETERMINATE when a kill lands inside a GC cycle
                    // (destruction may have happened without the `gc_done`
                    // ack ever reaching the journal — see gc_start below).
                    journal
                        .record("prune", &format!("{}/{}", exp.as_str(), leaf_id.0), step)
                        .ok();
                }
            }
        }

        // ── pin / unpin every ~16 steps ──────────────────────────────────────
        //
        // Appended after the existing op arms rather than interleaved with
        // them, so old seeds' pre-existing RNG-stream draws (ingest/delta
        // sizing, node-parent selection, etc.) stay reproducible — this
        // extension only adds NEW draws at the tail of each step, it never
        // reorders or removes an existing arm (05 §1).  Adding these draws
        // does perturb which value later draws in the SAME step observe from
        // `rng` on steps where a pin/unpin/gc happens; that is an accepted,
        // documented deviation, not silent seed corruption.
        if step % 16 == 0 && !snap_refs.is_empty() {
            // Occasionally unpin a previously-pinned ref instead of pinning.
            if !pinned_refs.is_empty() && rng.gen::<u32>() % 3 == 0 {
                let idx = rng.gen::<usize>() % pinned_refs.len();
                let target = pinned_refs[idx].clone();
                if meta.unpin(&target).unwrap_or(false) {
                    journal.record("unpin", &hex_ref(&target), step).ok();
                    pinned_refs.remove(idx);
                }
            } else {
                let idx = rng.gen::<usize>() % snap_refs.len();
                let target = snap_refs[idx].clone();
                if meta.pin(target.clone(), None).is_ok() {
                    journal.record("pin", &hex_ref(&target), step).ok();
                    if !pinned_refs.iter().any(|r| r == &target) {
                        pinned_refs.push(target);
                    }
                }
            }
        }

        // ── gc every ~24 steps (or forced, for the gc-* failpoint matrix) ────
        let due_for_gc = if force_gc {
            // Force early and repeatedly so an armed `gc-*` failpoint is
            // guaranteed to be reached within the op budget (05 §2).
            step % 2 == 0
        } else {
            step > 0 && step % 24 == 0
        };
        if due_for_gc {
            if force_gc {
                // The pack-sweep failpoints (gc-compact-copy, gc-compact-seal,
                // gc-index-repoint, gc-pack-unlink) only fire when a sealed
                // pack below the fence falls under the compaction threshold.
                // The normal workload's packs are ~100% live (every page is
                // referenced by a node's manifest chain), so those boundaries
                // would never be reached. Ingest garbage pages (never
                // referenced by any manifest — dead at mark time) so the pack
                // is mostly dead, and rotate-first below so it is sweepable
                // THIS cycle.
                let garbage: Vec<[u8; PAGE_SIZE]> = (0..24)
                    .map(|i| make_page(seed_from(step ^ 0xdead_beef_dead_beef, i)))
                    .collect();
                let garbage_refs: Vec<&[u8; PAGE_SIZE]> = garbage.iter().collect();
                let _ = store.pages().ingest(&garbage_refs);
            }
            let opts = snapstore_server::gc::GcOpts {
                compact_threshold: 0.5,
                // Forced mode sweeps pre-cycle data immediately (see above);
                // the normal workload keeps the production default (false).
                rotate_active_first: force_gc,
                tombstone_grace_cycles: 1,
            };
            let hooks = snapstore_store::gc::GcHooks::none();
            // Intent line BEFORE the cycle: a `gc_start` without a matching
            // `gc_done` tells recovery the kill landed inside GC, so pruned
            // subtrees may have been reaped without their `gc_done` ack ever
            // being journaled (destruction is durable before the journal
            // write). Recovery treats journaled-pruned refs as INDETERMINATE
            // in that case: allowed to be gone, never required to be.
            journal
                .record("gc_start", &(gc_cycle + 1).to_string(), step)
                .ok();
            if let Ok(report) = snapstore_server::gc::run_gc_cycle(store, meta, &opts, &hooks) {
                gc_cycle += 1;
                let reaped: String = report
                    .reaped_subtrees
                    .iter()
                    .map(|(exp_id, node_id)| format!("{exp_id}:{node_id}"))
                    .collect::<Vec<_>>()
                    .join(",");
                journal
                    .record_raw(
                        "gc_done",
                        &gc_cycle.to_string(),
                        &format!("reaped={reaped}"),
                    )
                    .ok();
            }
        }
    }
}

// ── SQLite batch scenario ─────────────────────────────────────────────────────

fn run_sqlite_batch(
    store: &SnapshotStore,
    meta: &MetaDb,
    rng: &mut StdRng,
    journal: &mut Journal,
    ops: u64,
) {
    let exp = ExperimentId::new("batch-exp").unwrap();

    // Anchor every node to one real stored manifest: deep fsck verifies that
    // node snapshot_refs resolve, so an all-zero placeholder ref would be
    // (correctly) reported as MissingManifest after every recovery.
    let pages: Vec<[u8; PAGE_SIZE]> = (0..PAGES)
        .map(|i| {
            let mut p = [0u8; PAGE_SIZE];
            p[0] = i as u8;
            p[1] = 0xb5; // batch-scenario marker
            p
        })
        .collect();
    let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().collect();
    store
        .pages()
        .ingest(&page_refs)
        .expect("child: ingest anchor pages");
    let indexed: Vec<(u64, &[u8; PAGE_SIZE])> = pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p))
        .collect();
    let container = snapstore_store::build::build_full_container(
        GUEST_RAM_BYTES,
        &indexed,
        DeviceBlob {
            format: 0,
            zstd: false,
            bytes: vec![],
            raw_len: 0,
        },
    );
    let anchor_ref = store
        .put_snapshot(&container)
        .expect("child: anchor snapshot");
    let dummy_ref = anchor_ref;

    // Create root.
    let root = CreateNodeParams {
        experiment_id: exp.clone(),
        node_id: NodeId(0),
        parent_node_id: None,
        snapshot_ref: dummy_ref.clone(),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    };
    let _ = meta.create_node(root);

    let mut node_ids: Vec<NodeId> = vec![NodeId(0)];

    // Pre-create 256 leaf nodes for the batch.
    for i in 1..=256u64 {
        let params = CreateNodeParams {
            experiment_id: exp.clone(),
            node_id: NodeId(i),
            parent_node_id: Some(NodeId(0)),
            snapshot_ref: dummy_ref.clone(),
            input_log_id: None,
            inline_log_container: None,
            status: NodeStatus::Frontier,
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: None,
        };
        if meta.create_node(params).is_ok() {
            node_ids.push(NodeId(i));
        }
    }

    // Hammer 256-update batches.
    for step in 0..ops {
        let batch_marker = rng.gen::<u64>();
        let updates: Vec<NodeUpdate> = node_ids
            .iter()
            .take(256)
            .map(|&id| {
                let mut u = NodeUpdate::new(id);
                u.attrs = Some(batch_marker.to_le_bytes().to_vec());
                u.visit_count_delta = 1;
                u
            })
            .collect();

        if let Ok(updated_at) = meta.update_nodes(exp.clone(), updates) {
            journal
                .record(
                    "batch_update",
                    &format!("{batch_marker}@{updated_at}"),
                    step,
                )
                .ok();
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_page(seed: u64) -> [u8; PAGE_SIZE] {
    let mut p = [0u8; PAGE_SIZE];
    let seed_bytes = seed.to_le_bytes();
    for (i, b) in p.iter_mut().enumerate() {
        *b = seed_bytes[i % 8].wrapping_add(i as u8);
    }
    p
}

fn seed_from(a: u64, b: u64) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(&a.to_le_bytes());
    h.update(&b.to_le_bytes());
    let out = h.finalize();
    u64::from_le_bytes(out.as_bytes()[0..8].try_into().unwrap())
}

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

fn hex_ref(r: &SnapshotRef) -> String {
    r.to_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_log_id(l: &LogId) -> String {
    l.to_bytes().iter().map(|b| format!("{b:02x}")).collect()
}
