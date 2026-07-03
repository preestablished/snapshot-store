// ── gc_fixture — joint restore-after-GC verification artifact ─────────────────
//!
//! `populate-gc-fixture` (06 §3): a self-contained seeded generator (no
//! proptest, mirrors `child.rs`'s `StdRng` approach) that builds a store with
//! fork-tree snapshot histories across several experiments, prunes a sample
//! of non-root subtrees, pins a sample of survivors, and writes the model's
//! expected-reachable ref set to disk. Does NOT run GC itself — the
//! bridge-side flow triggers `TriggerGc` against a scratch server on the
//! populated data root and restores every ref in
//! `expected-surviving-refs.txt`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

use snapstore_manifest::{DeviceBlob, Manifest};
use snapstore_meta::{CreateNodeParams, MetaDb};
use snapstore_store::build::{build_delta_container, build_full_container};
use snapstore_store::SnapshotStore;
use snapstore_types::{ExperimentId, NodeId, NodeStatus, SnapshotRef, PAGE_SIZE};

// ── Options / summary ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct GcFixtureOpts {
    pub dir: PathBuf,
    pub seed: u64,
    pub nodes: u64,
    pub pruned_subtrees: u64,
}

#[derive(Debug, Clone, Default)]
pub struct GcFixtureSummary {
    pub nodes_created: u64,
    pub subtrees_pruned: u64,
    pub refs_pinned: u64,
    pub surviving_refs: u64,
}

// ── Constants ─────────────────────────────────────────────────────────────────

/// Number of experiments the fork trees are spread across (>= 3, per 06 §3).
const NUM_EXPERIMENTS: u64 = 4;
/// Shared page pool for dedup-heavy content: every snapshot's pages are drawn
/// from this pool, so the same page hash recurs across many manifests.
const PAGE_POOL_SIZE: usize = 256;
/// A snapshot has between MIN_PAGES and MAX_PAGES entries.
const MIN_PAGES: u64 = 4;
const MAX_PAGES: u64 = 16;
/// A sample of surviving refs get pinned.
const PIN_SAMPLE: usize = 20;

// ── populate_gc_fixture ──────────────────────────────────────────────────────

pub fn populate_gc_fixture(opts: &GcFixtureOpts) -> Result<GcFixtureSummary, String> {
    std::fs::create_dir_all(&opts.dir).map_err(|e| format!("create dir: {e}"))?;
    let store_root = opts.dir.join("store");
    let meta_db_path = opts.dir.join("meta").join("tree.db");
    std::fs::create_dir_all(&store_root).map_err(|e| format!("create store dir: {e}"))?;
    std::fs::create_dir_all(opts.dir.join("meta")).map_err(|e| format!("create meta dir: {e}"))?;

    let store = SnapshotStore::open(&store_root).map_err(|e| format!("store open: {e}"))?;
    let meta = MetaDb::open(&meta_db_path).map_err(|e| format!("meta open: {e}"))?;

    let mut rng = StdRng::seed_from_u64(opts.seed);

    // Shared dedup-heavy page pool.
    let pool: Vec<[u8; PAGE_SIZE]> = (0..PAGE_POOL_SIZE)
        .map(|i| pool_page(opts.seed, i as u64))
        .collect();

    // ── Build fork trees across NUM_EXPERIMENTS experiments ──────────────────

    let nodes_per_exp = opts.nodes.div_ceil(NUM_EXPERIMENTS).max(1);

    // (experiment_id, node_id, snapshot_ref)
    let mut all_nodes: Vec<(ExperimentId, NodeId, SnapshotRef)> = Vec::new();
    // Per-experiment: node_id -> snapshot_ref, for parent lookups.
    let mut nodes_created: u64 = 0;

    for exp_idx in 0..NUM_EXPERIMENTS {
        let exp = ExperimentId::new(format!("gcfix-{exp_idx}"))
            .map_err(|e| format!("bad experiment id: {e:?}"))?;

        let mut exp_nodes: Vec<(NodeId, SnapshotRef)> = Vec::new();

        for local_idx in 0..nodes_per_exp {
            let node_id = NodeId(local_idx);
            // All snapshots in one chain must share guest_ram_bytes
            // (put_snapshot enforces ParentRamMismatch), so every snapshot in
            // the fixture uses a fixed MAX_PAGES-page geometry; the DELTA
            // "small pages" knob is how many entries change, not the RAM size.
            let guest_ram_bytes = MAX_PAGES * PAGE_SIZE as u64;

            let (container, parent_node) = if exp_nodes.is_empty() {
                // Root: FULL snapshot covering every page index.
                let pages: Vec<(u64, [u8; PAGE_SIZE])> = (0..MAX_PAGES)
                    .map(|i| (i, pool[rng.gen_range(0..pool.len())]))
                    .collect();
                let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|(_, p)| p).collect();
                store
                    .pages()
                    .ingest(&refs)
                    .map_err(|e| format!("ingest: {e}"))?;
                let pairs: Vec<(u64, &[u8; PAGE_SIZE])> =
                    pages.iter().map(|(i, p)| (*i, p)).collect();
                (
                    build_full_container(guest_ram_bytes, &pairs, empty_blob()),
                    None,
                )
            } else {
                // Fork: pick a random existing node in this experiment as parent.
                let parent_idx = rng.gen_range(0..exp_nodes.len());
                let (parent_id, parent_ref) = exp_nodes[parent_idx].clone();

                let n_changed = rng.gen_range(MIN_PAGES..=MAX_PAGES);
                let pages: Vec<(u64, [u8; PAGE_SIZE])> = (0..n_changed)
                    .map(|i| (i, pool[rng.gen_range(0..pool.len())]))
                    .collect();
                let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|(_, p)| p).collect();
                store
                    .pages()
                    .ingest(&refs)
                    .map_err(|e| format!("ingest: {e}"))?;
                let pairs: Vec<(u64, &[u8; PAGE_SIZE])> =
                    pages.iter().map(|(i, p)| (*i, p)).collect();
                (
                    build_delta_container(&parent_ref, guest_ram_bytes, &pairs, empty_blob()),
                    Some(parent_id),
                )
            };

            let snap_ref = store
                .put_snapshot(&container)
                .map_err(|e| format!("put_snapshot: {e}"))?;

            meta.create_node(CreateNodeParams {
                experiment_id: exp.clone(),
                node_id,
                parent_node_id: parent_node,
                snapshot_ref: snap_ref.clone(),
                input_log_id: None,
                inline_log_container: None,
                status: NodeStatus::Frontier,
                score: Some(rng.gen::<f64>()),
                icount: 0,
                virtual_ns: 0,
                attrs: None,
            })
            .map_err(|e| format!("create_node: {e}"))?;

            exp_nodes.push((node_id, snap_ref.clone()));
            all_nodes.push((exp.clone(), node_id, snap_ref));
            nodes_created += 1;
        }
    }

    // ── Prune >= opts.pruned_subtrees non-root subtrees ───────────────────────
    //
    // Leaves only (nodes with no recorded child), so pruning one node never
    // implicitly prunes another chosen target — each pruned node is exactly
    // one subtree root, keeping the "pruned" bookkeeping exact.
    let mut has_child: HashSet<(String, u64)> = HashSet::new();
    // We don't track parent explicitly here beyond fork selection above, so
    // recompute children by re-querying meta.
    for exp_idx in 0..NUM_EXPERIMENTS {
        let exp = ExperimentId::new(format!("gcfix-{exp_idx}")).unwrap();
        let mut filter = snapstore_meta::QueryFilter::new(exp.clone());
        filter.limit = Some((nodes_per_exp + 1) as u32);
        if let Ok(rows) = meta.query_nodes(filter) {
            for r in &rows {
                if let Some(p) = r.parent_node_id {
                    has_child.insert((exp.as_str().to_string(), p.0));
                }
            }
        }
    }

    let leaf_candidates: Vec<&(ExperimentId, NodeId, SnapshotRef)> = all_nodes
        .iter()
        .filter(|(exp, nid, _)| {
            !nid.is_root() && !has_child.contains(&(exp.as_str().to_string(), nid.0))
        })
        .collect();

    let mut pruned: HashSet<(String, u64)> = HashSet::new();
    let target_prunes = (opts.pruned_subtrees as usize).min(leaf_candidates.len());
    let mut candidate_idxs: Vec<usize> = (0..leaf_candidates.len()).collect();
    // Fisher-Yates partial shuffle for a random sample without replacement.
    for i in 0..target_prunes {
        let j = i + rng.gen_range(0..(candidate_idxs.len() - i));
        candidate_idxs.swap(i, j);
    }
    let mut subtrees_pruned: u64 = 0;
    for &idx in &candidate_idxs[..target_prunes] {
        let (exp, nid, _) = leaf_candidates[idx];
        if meta.prune_subtree(exp.clone(), *nid, false).is_ok() {
            pruned.insert((exp.as_str().to_string(), nid.0));
            subtrees_pruned += 1;
        }
    }

    // ── Pin a sample of surviving refs ────────────────────────────────────────

    let surviving_nodes: Vec<&(ExperimentId, NodeId, SnapshotRef)> = all_nodes
        .iter()
        .filter(|(exp, nid, _)| !pruned.contains(&(exp.as_str().to_string(), nid.0)))
        .collect();

    let pin_count = PIN_SAMPLE.min(surviving_nodes.len());
    let mut pin_idxs: Vec<usize> = (0..surviving_nodes.len()).collect();
    for i in 0..pin_count {
        let j = i + rng.gen_range(0..(pin_idxs.len() - i));
        pin_idxs.swap(i, j);
    }
    let mut refs_pinned: u64 = 0;
    for &idx in &pin_idxs[..pin_count] {
        let (_, _, snap_ref) = surviving_nodes[idx];
        if meta.pin(snap_ref.clone(), None).is_ok() {
            refs_pinned += 1;
        }
    }

    // ── Compute the expected surviving-ref set ────────────────────────────────
    //
    // The model: every non-pruned node's snapshot_ref, plus every pinned ref,
    // plus their full ancestor chains (walked via get_snapshot + Manifest
    // decode — the same reachability definition `SnapshotStore::gc_mark`
    // uses). This is a SAFETY set (must-resolve), not an exact post-GC
    // physical snapshot — grace-cycle timing means a fresh store's first GC
    // cycle may not reap anything yet (space-leak tolerance is intentional
    // elsewhere in the gate); these refs must resolve regardless.
    let mut survivor_roots: Vec<SnapshotRef> =
        surviving_nodes.iter().map(|(_, _, r)| r.clone()).collect();
    let pins = meta.list_pins().map_err(|e| format!("list_pins: {e}"))?;
    for p in &pins {
        survivor_roots.push(p.snapshot_ref.clone());
    }

    let mut reachable: HashSet<[u8; 32]> = HashSet::new();
    for root in &survivor_roots {
        let mut cursor = root.clone();
        loop {
            if reachable.contains(&cursor.to_bytes()) {
                break;
            }
            let bytes = store
                .get_snapshot(&cursor)
                .map_err(|e| format!("get_snapshot during model walk: {e}"))?;
            let m = Manifest::decode(&bytes).map_err(|e| format!("decode manifest: {e:?}"))?;
            reachable.insert(cursor.to_bytes());
            if m.delta {
                cursor = m.parent.expect("delta must have parent");
            } else {
                break;
            }
        }
    }

    // ── Write expected-surviving-refs.txt ─────────────────────────────────────

    let mut hex_refs: Vec<String> = reachable.iter().map(crate::fsck::hex_from_32).collect();
    hex_refs.sort();
    hex_refs.dedup();
    let refs_path = opts.dir.join("expected-surviving-refs.txt");
    std::fs::write(&refs_path, hex_refs.join("\n") + "\n")
        .map_err(|e| format!("write refs file: {e}"))?;

    // ── Write fixture-manifest.json ────────────────────────────────────────────

    let manifest = FixtureManifest {
        seed: opts.seed,
        params: FixtureParams {
            nodes: opts.nodes,
            pruned_subtrees: opts.pruned_subtrees,
            num_experiments: NUM_EXPERIMENTS,
        },
        git_rev: git_rev(),
        counts: FixtureCounts {
            nodes_created,
            subtrees_pruned,
            refs_pinned,
            surviving_refs: hex_refs.len() as u64,
        },
        unix_timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    };
    let manifest_path = opts.dir.join("fixture-manifest.json");
    let json = serde_json::to_string_pretty(&manifest).map_err(|e| e.to_string())?;
    std::fs::write(&manifest_path, json).map_err(|e| format!("write manifest: {e}"))?;

    Ok(GcFixtureSummary {
        nodes_created,
        subtrees_pruned,
        refs_pinned,
        surviving_refs: hex_refs.len() as u64,
    })
}

// ── Fixture manifest model ───────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct FixtureManifest {
    seed: u64,
    params: FixtureParams,
    git_rev: String,
    counts: FixtureCounts,
    unix_timestamp: u64,
}

#[derive(Debug, Serialize)]
struct FixtureParams {
    nodes: u64,
    pruned_subtrees: u64,
    num_experiments: u64,
}

#[derive(Debug, Serialize)]
struct FixtureCounts {
    nodes_created: u64,
    subtrees_pruned: u64,
    refs_pinned: u64,
    surviving_refs: u64,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

/// Deterministic pool page: distinct per (seed, index), stable across a run.
fn pool_page(seed: u64, idx: u64) -> [u8; PAGE_SIZE] {
    let mut h = blake3::Hasher::new();
    h.update(&seed.to_le_bytes());
    h.update(&idx.to_le_bytes());
    let digest = h.finalize();
    let mut p = [0u8; PAGE_SIZE];
    let bytes = digest.as_bytes();
    for (i, b) in p.iter_mut().enumerate() {
        *b = bytes[i % 32].wrapping_add(i as u8);
    }
    p
}

/// Current git revision, via `git rev-parse HEAD` in the crate's directory,
/// falling back to the `GIT_REV` env var, then `"unknown"`.
fn git_rev() -> String {
    if let Ok(out) = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_dir())
        .output()
    {
        if out.status.success() {
            if let Ok(s) = String::from_utf8(out.stdout) {
                let s = s.trim().to_string();
                if !s.is_empty() {
                    return s;
                }
            }
        }
    }
    std::env::var("GIT_REV").unwrap_or_else(|_| "unknown".to_string())
}

fn repo_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}
