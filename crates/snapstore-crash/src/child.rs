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
}

impl std::str::FromStr for Scenario {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "default" => Ok(Scenario::Default),
            "sqlite-batch" => Ok(Scenario::SqliteBatch),
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
}

// ── Constants ─────────────────────────────────────────────────────────────────

const PAGES: usize = 8; // pages per FULL snapshot
const GUEST_RAM_BYTES: u64 = PAGES as u64 * PAGE_SIZE as u64;

// ── run_child ─────────────────────────────────────────────────────────────────

/// Entry point for the child process.
pub fn run_child(scratch: &Path, seed: u64, ops: u64, scenario: Scenario) {
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
        Scenario::Default => run_default(&store, &meta, &mut rng, &mut journal, ops),
        Scenario::SqliteBatch => run_sqlite_batch(&store, &meta, &mut rng, &mut journal, ops),
    }
}

// ── Default scenario ──────────────────────────────────────────────────────────

fn run_default(
    store: &SnapshotStore,
    meta: &MetaDb,
    rng: &mut StdRng,
    journal: &mut Journal,
    ops: u64,
) {
    let exp_a = ExperimentId::new("exp-A").unwrap();
    let exp_b = ExperimentId::new("exp-B").unwrap();

    // Node history: (experiment_id_char, node_id) pairs
    let mut node_ids: Vec<(bool, NodeId)> = Vec::new(); // true = exp-A

    let mut prev_ref: Option<SnapshotRef> = None;

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
                let _ = meta.prune_subtree(exp.clone(), *leaf_id, false);
                // Not journaled — prune success is not part of recovery invariants tested here.
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
