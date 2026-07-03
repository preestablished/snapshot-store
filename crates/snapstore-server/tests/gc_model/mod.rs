//! Model + op-tape executor for the M7 GC property suite.
//!
//! Spec: `.agents/plans/phase3-m7-gc-exit-gate/04-property-suite.md`.
//!
//! The model is a refcount-free oracle: after every op it recomputes the
//! reachable set brute-force (roots = all un-reaped node refs + pins; walk
//! parent chains; union page hashes).  The executor drives a real
//! `SnapshotStore` + `MetaDb` + `run_gc_cycle` on TempDirs, entirely
//! in-process, with controlled interleaving via `GcHooks`.
//!
//! Legal-outcome rule (04 §3): an acked `PutPages` whose pages were never
//! referenced by a committed manifest MAY be collected.  The executor
//! therefore always (re-)ingests a commit's pages at commit time, and on
//! `PutError::MissingPages` re-ingests the missing pages and retries once
//! (mirroring a real client); the model never counts `PutPagesOnly` pages
//! as protected.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use proptest::prelude::*;
use tempfile::TempDir;

use snapstore_manifest::{DeviceBlob, Manifest};
use snapstore_meta::{CreateNodeParams, MetaDb};
use snapstore_server::gc::{run_gc_cycle, GcOpts};
use snapstore_store::build::{build_delta_container, build_full_container};
use snapstore_store::gc::GcHooks;
use snapstore_store::gc::GcPoint;
use snapstore_store::{PutError, SnapshotStore, StoreOpts};
use snapstore_types::{ExperimentId, NodeId, NodeStatus, PageHash, SnapshotRef, PAGE_SIZE};

// ── Page helpers ──────────────────────────────────────────────────────────────

/// Distinct 4 KiB page stamped with `tag`.
pub fn page(tag: u64) -> [u8; PAGE_SIZE] {
    let mut p = [0u8; PAGE_SIZE];
    p[..8].copy_from_slice(&tag.to_le_bytes());
    p
}

pub fn page_hash(tag: u64) -> PageHash {
    PageHash::from_bytes(*blake3::hash(&page(tag)).as_bytes())
}

pub fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

// ── Op alphabet (04 §1) ───────────────────────────────────────────────────────

/// Page-content generator: ~30% of pages reuse a previously generated tag
/// (dedup pressure across siblings), the rest are fresh.
#[derive(Clone, Debug)]
pub enum PageGen {
    Fresh,
    Reuse(u8),
}

/// The `GcPoint` kinds the interleave alphabet may pin an op to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PointSel {
    AfterCopy,
    BeforeFinalize,
    BeforeManifestSweep,
}

/// Ops executed from inside the GC cycle via the `GcHooks` callback.
#[derive(Clone, Debug)]
pub enum InterleaveOp {
    CommitDelta {
        parent_sel: u8,
        dirty: Vec<(u8, PageGen)>,
    },
    CommitOrphan {
        pages: Vec<PageGen>,
    },
    Pin {
        sel: u8,
    },
    /// Race B replay: an orphan manifest staged earlier in the tape gets
    /// its `create_node` inside the hook.  NOT_FOUND is a legal outcome
    /// (GC may already have doomed it) — see `apply_hook_outcomes`.
    CreateNodeLate {
        sel: u8,
    },
}

/// One entry of the generated op tape.  Selectors are u8 indices resolved
/// modulo the model's live lists at execution time; ops whose target list
/// is empty are skipped (keeps every tape valid and shrinkable).
#[derive(Clone, Debug)]
pub enum Op {
    CommitFull {
        pages: Vec<PageGen>,
    },
    CommitDelta {
        parent_sel: u8,
        dirty: Vec<(u8, PageGen)>,
    },
    CommitOrphan {
        pages: Vec<PageGen>,
    },
    PutPagesOnly {
        pages: Vec<PageGen>,
    },
    Pin {
        sel: u8,
    },
    Unpin {
        sel: u8,
    },
    Prune {
        node_sel: u8,
    },
    Gc {
        aggressive: bool,
        interleave: Vec<(InterleaveOp, PointSel)>,
    },
    Read {
        sel: u8,
    },
}

fn page_gen() -> impl Strategy<Value = PageGen> {
    prop_oneof![
        7 => Just(PageGen::Fresh),
        3 => any::<u8>().prop_map(PageGen::Reuse),
    ]
}

/// Guest-sized page vector: 8–32 pages (runtime budget, 04 §7).
fn guest_pages() -> impl Strategy<Value = Vec<PageGen>> {
    prop::collection::vec(page_gen(), 8..=32)
}

fn dirty_pages() -> impl Strategy<Value = Vec<(u8, PageGen)>> {
    prop::collection::vec((any::<u8>(), page_gen()), 1..=8)
}

fn interleave_op() -> impl Strategy<Value = InterleaveOp> {
    prop_oneof![
        3 => (any::<u8>(), dirty_pages())
            .prop_map(|(parent_sel, dirty)| InterleaveOp::CommitDelta { parent_sel, dirty }),
        2 => prop::collection::vec(page_gen(), 8..=16)
            .prop_map(|pages| InterleaveOp::CommitOrphan { pages }),
        2 => any::<u8>().prop_map(|sel| InterleaveOp::Pin { sel }),
        2 => any::<u8>().prop_map(|sel| InterleaveOp::CreateNodeLate { sel }),
    ]
}

fn point_sel() -> impl Strategy<Value = PointSel> {
    prop_oneof![
        Just(PointSel::AfterCopy),
        Just(PointSel::BeforeFinalize),
        Just(PointSel::BeforeManifestSweep),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => guest_pages().prop_map(|pages| Op::CommitFull { pages }),
        5 => (any::<u8>(), dirty_pages())
            .prop_map(|(parent_sel, dirty)| Op::CommitDelta { parent_sel, dirty }),
        2 => guest_pages().prop_map(|pages| Op::CommitOrphan { pages }),
        2 => prop::collection::vec(page_gen(), 1..=8)
            .prop_map(|pages| Op::PutPagesOnly { pages }),
        2 => any::<u8>().prop_map(|sel| Op::Pin { sel }),
        1 => any::<u8>().prop_map(|sel| Op::Unpin { sel }),
        2 => any::<u8>().prop_map(|node_sel| Op::Prune { node_sel }),
        3 => (any::<bool>(), prop::collection::vec((interleave_op(), point_sel()), 0..=3))
            .prop_map(|(aggressive, interleave)| Op::Gc { aggressive, interleave }),
        2 => any::<u8>().prop_map(|sel| Op::Read { sel }),
    ]
}

/// The generated value: an op tape of up to `max_len` ops.
pub fn ops_strategy(max_len: usize) -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op(), 1..=max_len)
}

// ── Model (04 §2) ─────────────────────────────────────────────────────────────

/// `(page_index, page_hash, content_tag)` rows of a flattened page table.
pub type ExpectedFlat = Vec<(u64, PageHash, u64)>;

#[derive(Clone, Debug)]
pub struct ModelManifest {
    pub parent: Option<[u8; 32]>,
    /// (page_index, hash) — raw per-manifest entries (mark semantics:
    /// shadowed pages are conservatively live).
    pub entries: Vec<(u64, PageHash)>,
    pub guest_pages: u64,
}

#[derive(Clone, Debug)]
pub struct ModelNode {
    pub exp: String,
    pub node_id: u64,
    pub parent_node: Option<u64>,
    pub reff: [u8; 32],
    pub pruned: bool,
}

#[derive(Clone, Debug)]
pub struct ModelTombstone {
    pub exp: String,
    pub node_id: u64,
    /// Number of GC cycles this tombstone has survived.  Mirrors the
    /// `created_at <= last_fence_counter` horizon: a tombstone that
    /// existed when a cycle ran is reaped by the next cycle (grace 1),
    /// or immediately (grace 0).
    pub seen_cycles: u32,
}

#[derive(Default)]
pub struct Model {
    /// All generated page tags, for `PageGen::Reuse` resolution.
    pub tags: Vec<u64>,
    pub next_tag: u64,
    /// hash -> tag for every page ever handed to the store.
    pub content: HashMap<PageHash, u64>,
    /// Every manifest ever acked by put_snapshot (never pruned from the
    /// model — only walked from live roots).
    pub manifests: HashMap<[u8; 32], ModelManifest>,
    /// Un-reaped node rows (pruned-but-unreaped included).
    pub nodes: Vec<ModelNode>,
    pub pins: HashSet<[u8; 32]>,
    /// Selector domain for Pin/CreateNodeLate: orphan manifests the model
    /// optimistically believes may still exist.  A failed pin/create of
    /// one is a legal outcome and drops it.
    pub orphans: Vec<[u8; 32]>,
    pub tombstones: Vec<ModelTombstone>,
    pub next_exp: u64,
    pub next_node_id: HashMap<String, u64>,
}

impl Model {
    pub fn resolve_tags(&mut self, gens: &[PageGen]) -> Vec<u64> {
        gens.iter()
            .map(|g| match g {
                PageGen::Reuse(k) if !self.tags.is_empty() => {
                    self.tags[*k as usize % self.tags.len()]
                }
                _ => {
                    let t = self.next_tag;
                    self.next_tag += 1;
                    self.tags.push(t);
                    t
                }
            })
            .collect()
    }

    /// Record page content for `tags`, returning (hash, tag) pairs.
    pub fn record_content(&mut self, tags: &[u64]) -> Vec<(PageHash, u64)> {
        tags.iter()
            .map(|t| {
                let h = page_hash(*t);
                self.content.insert(h, *t);
                (h, *t)
            })
            .collect()
    }

    /// Distinct root refs: every un-reaped node ref plus every pin.
    pub fn root_refs(&self) -> HashSet<[u8; 32]> {
        let mut roots: HashSet<[u8; 32]> = self.nodes.iter().map(|n| n.reff).collect();
        roots.extend(self.pins.iter().copied());
        roots
    }

    /// Brute-force reachability: (manifest refs, page hashes).
    pub fn reachable(&self) -> (HashSet<[u8; 32]>, HashSet<PageHash>) {
        let mut ms: HashSet<[u8; 32]> = HashSet::new();
        let mut ps: HashSet<PageHash> = HashSet::new();
        for root in self.root_refs() {
            let mut cursor = root;
            loop {
                if !ms.insert(cursor) {
                    break;
                }
                let m = self
                    .manifests
                    .get(&cursor)
                    .unwrap_or_else(|| panic!("model: unknown manifest in chain"));
                for (_, h) in &m.entries {
                    ps.insert(*h);
                }
                match m.parent {
                    Some(p) => cursor = p,
                    None => break,
                }
            }
        }
        (ms, ps)
    }

    /// Expected flattened page table for `r` (ascending page index).
    pub fn expected_flat(&self, r: &[u8; 32]) -> ExpectedFlat {
        let mut chain: Vec<&ModelManifest> = Vec::new();
        let mut cursor = *r;
        loop {
            let m = self.manifests.get(&cursor).expect("model: chain manifest");
            chain.push(m);
            match m.parent {
                Some(p) => cursor = p,
                None => break,
            }
        }
        let mut flat: BTreeMap<u64, PageHash> = BTreeMap::new();
        for m in chain.iter().rev() {
            for (idx, h) in &m.entries {
                flat.insert(*idx, *h);
            }
        }
        flat.into_iter()
            .map(|(idx, h)| (idx, h, *self.content.get(&h).expect("model: page content")))
            .collect()
    }

    /// Mirror of the orchestrator's tombstone reap (run_gc_cycle step 1).
    /// grace 0: reap everything; grace >= 1: reap tombstones that existed
    /// before the previous cycle ran (seen_cycles >= 1); survivors age.
    pub fn apply_reap(&mut self, grace_cycles: u32) {
        let eligible: Vec<(String, u64)> = self
            .tombstones
            .iter()
            .filter(|t| grace_cycles == 0 || t.seen_cycles >= 1)
            .map(|t| (t.exp.clone(), t.node_id))
            .collect();
        for (exp, node_id) in &eligible {
            // BFS the subtree among current rows (all pruned by
            // construction — prune marks the whole subtree).
            let mut doomed: HashSet<u64> = HashSet::new();
            doomed.insert(*node_id);
            loop {
                let before = doomed.len();
                for n in self.nodes.iter().filter(|n| n.exp == *exp) {
                    if let Some(p) = n.parent_node {
                        if doomed.contains(&p) {
                            doomed.insert(n.node_id);
                        }
                    }
                }
                if doomed.len() == before {
                    break;
                }
            }
            self.nodes
                .retain(|n| !(n.exp == *exp && doomed.contains(&n.node_id)));
        }
        self.tombstones.retain(|t| {
            !eligible
                .iter()
                .any(|(e, id)| t.exp == *e && t.node_id == *id)
        });
        for t in &mut self.tombstones {
            t.seen_cycles += 1;
        }
    }

    /// Non-pruned nodes (valid delta parents / prune targets).
    pub fn live_nodes(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| !n.pruned)
            .map(|(i, _)| i)
            .collect()
    }

    pub fn alloc_exp(&mut self) -> String {
        let e = format!("exp-{}", self.next_exp);
        self.next_exp += 1;
        self.next_node_id.insert(e.clone(), 1);
        e
    }

    pub fn alloc_node_id(&mut self, exp: &str) -> u64 {
        let c = self.next_node_id.entry(exp.to_string()).or_insert(1);
        let id = *c;
        *c += 1;
        id
    }
}

// ── Interleave preparation ────────────────────────────────────────────────────

enum Action {
    Commit {
        container: Vec<u8>,
        pages: Vec<(PageHash, u64)>,
        /// Some((exp, node_id, parent_node_id)) => also create_node.
        node: Option<(String, u64, Option<u64>)>,
        reff: [u8; 32],
        manifest: ModelManifest,
    },
    PinRef {
        reff: [u8; 32],
        /// true iff the ref is an orphan (rejection is a legal outcome);
        /// node refs are protected roots and must never be rejected.
        may_reject: bool,
    },
    LateNode {
        reff: [u8; 32],
        exp: String,
    },
}

#[derive(Debug)]
enum HookOutcome {
    CommitOk,
    CommitErr(String),
    PinOk,
    PinRejected(String),
    NodeOk,
    NodeRejected(String),
}

struct PreparedEntry {
    point: PointSel,
    fired: AtomicBool,
    action: Action,
}

fn run_action(
    store: &SnapshotStore,
    meta: &MetaDb,
    action: &Action,
    content: &HashMap<PageHash, u64>,
) -> HookOutcome {
    match action {
        Action::Commit {
            container,
            pages,
            node,
            reff,
            ..
        } => {
            let bufs: Vec<[u8; PAGE_SIZE]> = pages.iter().map(|(_, t)| page(*t)).collect();
            let refs: Vec<&[u8; PAGE_SIZE]> = bufs.iter().collect();
            if !refs.is_empty() {
                if let Err(e) = store.pages().ingest(&refs) {
                    return HookOutcome::CommitErr(format!("interleave ingest: {e}"));
                }
            }
            match put_with_retry(store, container, content) {
                Ok(r) => {
                    debug_assert_eq!(r.to_bytes(), *reff);
                    if let Some((exp, node_id, parent)) = node {
                        // Mirror the server create_node handler: gate read
                        // lock across register -> create_node.
                        let gate = store.commit_gate();
                        if let Err(e) = store.register_live_ref(&gate, &r) {
                            return HookOutcome::CommitErr(format!(
                                "interleave register before create_node: {e}"
                            ));
                        }
                        let res = meta.create_node(CreateNodeParams {
                            experiment_id: ExperimentId::new(exp.clone()).unwrap(),
                            node_id: NodeId(*node_id),
                            parent_node_id: parent.map(NodeId),
                            snapshot_ref: r,
                            input_log_id: None,
                            inline_log_container: None,
                            status: NodeStatus::Frontier,
                            score: None,
                            icount: 0,
                            virtual_ns: 0,
                            attrs: None,
                        });
                        drop(gate);
                        if let Err(e) = res {
                            return HookOutcome::CommitErr(format!("interleave create_node: {e}"));
                        }
                    }
                    HookOutcome::CommitOk
                }
                Err(e) => HookOutcome::CommitErr(e),
            }
        }
        Action::PinRef { reff, .. } => {
            let r = SnapshotRef::from_bytes(*reff);
            // Mirror the server pin handler: gate read lock across
            // register -> meta.pin.
            let gate = store.commit_gate();
            match store.register_live_ref(&gate, &r) {
                Ok(()) => match meta.pin(r, None) {
                    Ok(()) => HookOutcome::PinOk,
                    Err(e) => HookOutcome::PinRejected(format!("meta.pin: {e}")),
                },
                Err(e) => HookOutcome::PinRejected(format!("register: {e}")),
            }
        }
        Action::LateNode { reff, exp } => {
            let r = SnapshotRef::from_bytes(*reff);
            let gate = store.commit_gate();
            match store.register_live_ref(&gate, &r) {
                Ok(()) => {
                    let res = meta.create_node(CreateNodeParams {
                        experiment_id: ExperimentId::new(exp.clone()).unwrap(),
                        node_id: NodeId(0),
                        parent_node_id: None,
                        snapshot_ref: r,
                        input_log_id: None,
                        inline_log_container: None,
                        status: NodeStatus::Frontier,
                        score: None,
                        icount: 0,
                        virtual_ns: 0,
                        attrs: None,
                    });
                    drop(gate);
                    match res {
                        Ok(_) => HookOutcome::NodeOk,
                        Err(e) => HookOutcome::NodeRejected(format!("create_node: {e}")),
                    }
                }
                // Legal: GC already doomed the orphan (Race B, both
                // outcomes legal — 04 §1 CreateNodeLate).
                Err(e) => HookOutcome::NodeRejected(format!("register: {e}")),
            }
        }
    }
}

/// put_snapshot with the real-client MissingPages recovery: re-ingest the
/// missing pages (content known to the model) and retry once.
pub fn put_with_retry(
    store: &SnapshotStore,
    container: &[u8],
    content: &HashMap<PageHash, u64>,
) -> Result<SnapshotRef, String> {
    match store.put_snapshot(container) {
        Ok(r) => Ok(r),
        Err(PutError::MissingPages(missing)) => {
            let bufs: Vec<[u8; PAGE_SIZE]> = missing
                .iter()
                .map(|h| {
                    content
                        .get(h)
                        .map(|t| page(*t))
                        .ok_or_else(|| format!("missing page {h:?} has no model content"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let refs: Vec<&[u8; PAGE_SIZE]> = bufs.iter().collect();
            store
                .pages()
                .ingest(&refs)
                .map_err(|e| format!("re-ingest: {e}"))?;
            store
                .put_snapshot(container)
                .map_err(|e| format!("put_snapshot retry after re-ingest: {e}"))
        }
        Err(e) => Err(format!("put_snapshot: {e}")),
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

pub struct TapeExec {
    pub _dir: TempDir,
    pub store: Arc<SnapshotStore>,
    pub meta: MetaDb,
    pub model: Model,
}

/// Aggressive/exactness opts (04 §3): threshold 1.01 compacts even
/// 100%-live packs (1.0 < 1.01), so every quiescent aggressive cycle
/// rewrites ALL pre-fence data — intended; with rotate-first this makes
/// physical state exactly equal to the reachable set.
pub fn aggressive_opts() -> GcOpts {
    GcOpts {
        compact_threshold: 1.01,
        rotate_active_first: true,
        tombstone_grace_cycles: 0,
    }
}

impl TapeExec {
    pub fn new() -> Result<Self, String> {
        let dir = TempDir::new().map_err(|e| format!("tempdir: {e}"))?;
        let mut opts = StoreOpts::default();
        // Tiny packs (~16 records each) so sweeps see many sealed packs.
        opts.pagestore.max_pack_bytes = 16 * 4133;
        let store = SnapshotStore::open_with_options(&dir.path().join("store"), opts)
            .map_err(|e| format!("store open: {e}"))?;
        let meta = MetaDb::open(&dir.path().join("meta/tree.db"))
            .map_err(|e| format!("meta open: {e}"))?;
        Ok(Self {
            _dir: dir,
            store: Arc::new(store),
            meta,
            model: Model::default(),
        })
    }

    fn ingest_tags(&self, tags: &[u64]) -> Result<(), String> {
        let bufs: Vec<[u8; PAGE_SIZE]> = tags.iter().map(|t| page(*t)).collect();
        let refs: Vec<&[u8; PAGE_SIZE]> = bufs.iter().collect();
        if !refs.is_empty() {
            self.store
                .pages()
                .ingest(&refs)
                .map_err(|e| format!("ingest: {e}"))?;
        }
        Ok(())
    }

    /// FULL container from `tags` (page i = tags[i]).
    fn full_container(tags: &[u64]) -> Vec<u8> {
        let bufs: Vec<[u8; PAGE_SIZE]> = tags.iter().map(|t| page(*t)).collect();
        let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = bufs
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p))
            .collect();
        build_full_container(tags.len() as u64 * PAGE_SIZE as u64, &pairs, empty_blob())
    }

    fn delta_container(parent: &[u8; 32], guest_pages: u64, dirty: &[(u64, u64)]) -> Vec<u8> {
        let bufs: Vec<[u8; PAGE_SIZE]> = dirty.iter().map(|(_, t)| page(*t)).collect();
        let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = dirty
            .iter()
            .zip(bufs.iter())
            .map(|((idx, _), p)| (*idx, p))
            .collect();
        build_delta_container(
            &SnapshotRef::from_bytes(*parent),
            guest_pages * PAGE_SIZE as u64,
            &pairs,
            empty_blob(),
        )
    }

    /// Resolve dirty selectors to sorted, deduped (page_index, tag) pairs.
    fn resolve_dirty(&mut self, guest_pages: u64, dirty: &[(u8, PageGen)]) -> Vec<(u64, u64)> {
        let gens: Vec<PageGen> = dirty.iter().map(|(_, g)| g.clone()).collect();
        let tags = self.model.resolve_tags(&gens);
        let mut by_idx: BTreeMap<u64, u64> = BTreeMap::new();
        for ((sel, _), t) in dirty.iter().zip(tags) {
            by_idx.insert(*sel as u64 % guest_pages, t);
        }
        by_idx.into_iter().collect()
    }

    pub fn commit_full(&mut self, gens: &[PageGen]) -> Result<[u8; 32], String> {
        let tags = self.model.resolve_tags(gens);
        self.model.record_content(&tags);
        self.ingest_tags(&tags)?;
        let container = Self::full_container(&tags);
        let r = put_with_retry(&self.store, &container, &self.model.content)?;
        let reff = r.to_bytes();
        self.model.manifests.insert(
            reff,
            ModelManifest {
                parent: None,
                entries: tags
                    .iter()
                    .enumerate()
                    .map(|(i, t)| (i as u64, page_hash(*t)))
                    .collect(),
                guest_pages: tags.len() as u64,
            },
        );
        Ok(reff)
    }

    fn create_root_node(&mut self, reff: [u8; 32]) -> Result<(), String> {
        let exp = self.model.alloc_exp();
        self.meta
            .create_node(CreateNodeParams {
                experiment_id: ExperimentId::new(exp.clone()).unwrap(),
                node_id: NodeId(0),
                parent_node_id: None,
                snapshot_ref: SnapshotRef::from_bytes(reff),
                input_log_id: None,
                inline_log_container: None,
                status: NodeStatus::Frontier,
                score: None,
                icount: 0,
                virtual_ns: 0,
                attrs: None,
            })
            .map_err(|e| format!("create_node root: {e}"))?;
        self.model.nodes.push(ModelNode {
            exp,
            node_id: 0,
            parent_node: None,
            reff,
            pruned: false,
        });
        Ok(())
    }

    pub fn apply_op(&mut self, op: &Op) -> Result<(), String> {
        match op {
            Op::CommitFull { pages } => {
                let reff = self.commit_full(pages)?;
                self.create_root_node(reff)
            }
            Op::CommitDelta { parent_sel, dirty } => {
                let live = self.model.live_nodes();
                if live.is_empty() {
                    return Ok(()); // no valid parent — skip
                }
                let pi = live[*parent_sel as usize % live.len()];
                let (p_exp, p_node_id, p_ref) = {
                    let p = &self.model.nodes[pi];
                    (p.exp.clone(), p.node_id, p.reff)
                };
                let guest_pages = self.model.manifests[&p_ref].guest_pages;
                let dirty_resolved = self.resolve_dirty(guest_pages, dirty);
                let tags: Vec<u64> = dirty_resolved.iter().map(|(_, t)| *t).collect();
                self.model.record_content(&tags);
                self.ingest_tags(&tags)?;
                let container = Self::delta_container(&p_ref, guest_pages, &dirty_resolved);
                let r = put_with_retry(&self.store, &container, &self.model.content)?;
                let reff = r.to_bytes();
                self.model.manifests.insert(
                    reff,
                    ModelManifest {
                        parent: Some(p_ref),
                        entries: dirty_resolved
                            .iter()
                            .map(|(idx, t)| (*idx, page_hash(*t)))
                            .collect(),
                        guest_pages,
                    },
                );
                let node_id = self.model.alloc_node_id(&p_exp);
                self.meta
                    .create_node(CreateNodeParams {
                        experiment_id: ExperimentId::new(p_exp.clone()).unwrap(),
                        node_id: NodeId(node_id),
                        parent_node_id: Some(NodeId(p_node_id)),
                        snapshot_ref: r,
                        input_log_id: None,
                        inline_log_container: None,
                        status: NodeStatus::Frontier,
                        score: None,
                        icount: 0,
                        virtual_ns: 0,
                        attrs: None,
                    })
                    .map_err(|e| format!("create_node delta: {e}"))?;
                self.model.nodes.push(ModelNode {
                    exp: p_exp,
                    node_id,
                    parent_node: Some(p_node_id),
                    reff,
                    pruned: false,
                });
                Ok(())
            }
            Op::CommitOrphan { pages } => {
                let reff = self.commit_full(pages)?;
                if !self.model.orphans.contains(&reff) {
                    self.model.orphans.push(reff);
                }
                Ok(())
            }
            Op::PutPagesOnly { pages } => {
                // Legal-outcome rule: these pages are NOT protected — the
                // model records content (for re-ingest) but nothing else.
                let tags = self.model.resolve_tags(pages);
                self.model.record_content(&tags);
                self.ingest_tags(&tags)
            }
            Op::Pin { sel } => {
                // Domain: node refs (protected) + orphans (may be gone).
                let mut domain: Vec<([u8; 32], bool)> =
                    self.model.nodes.iter().map(|n| (n.reff, false)).collect();
                domain.extend(self.model.orphans.iter().map(|r| (*r, true)));
                if domain.is_empty() {
                    return Ok(());
                }
                let (reff, may_reject) = domain[*sel as usize % domain.len()];
                let r = SnapshotRef::from_bytes(reff);
                let gate = self.store.commit_gate();
                match self.store.register_live_ref(&gate, &r) {
                    Ok(()) => {
                        self.meta.pin(r, None).map_err(|e| format!("pin: {e}"))?;
                        drop(gate);
                        self.model.pins.insert(reff);
                        Ok(())
                    }
                    Err(e) if may_reject => {
                        // Legal: the orphan was collected by an earlier GC.
                        drop(gate);
                        self.model.orphans.retain(|x| *x != reff);
                        let _ = e;
                        Ok(())
                    }
                    Err(e) => Err(format!("pin of protected node ref rejected: {e}")),
                }
            }
            Op::Unpin { sel } => {
                let pins: Vec<[u8; 32]> = self.model.pins.iter().copied().collect();
                if pins.is_empty() {
                    return Ok(());
                }
                let reff = pins[*sel as usize % pins.len()];
                self.meta
                    .unpin(&SnapshotRef::from_bytes(reff))
                    .map_err(|e| format!("unpin: {e}"))?;
                self.model.pins.remove(&reff);
                Ok(())
            }
            Op::Prune { node_sel } => {
                let live = self.model.live_nodes();
                if live.is_empty() {
                    return Ok(());
                }
                let ni = live[*node_sel as usize % live.len()];
                let (exp, node_id) = {
                    let n = &self.model.nodes[ni];
                    (n.exp.clone(), n.node_id)
                };
                self.meta
                    .prune_subtree(
                        ExperimentId::new(exp.clone()).unwrap(),
                        NodeId(node_id),
                        true,
                    )
                    .map_err(|e| format!("prune_subtree: {e}"))?;
                // Model: mark the whole subtree pruned; tombstone the root.
                let mut doomed: HashSet<u64> = HashSet::new();
                doomed.insert(node_id);
                loop {
                    let before = doomed.len();
                    for n in self.model.nodes.iter().filter(|n| n.exp == exp) {
                        if let Some(p) = n.parent_node {
                            if doomed.contains(&p) {
                                doomed.insert(n.node_id);
                            }
                        }
                    }
                    if doomed.len() == before {
                        break;
                    }
                }
                for n in self.model.nodes.iter_mut() {
                    if n.exp == exp && doomed.contains(&n.node_id) {
                        n.pruned = true;
                    }
                }
                self.model.tombstones.push(ModelTombstone {
                    exp,
                    node_id,
                    seen_cycles: 0,
                });
                Ok(())
            }
            Op::Gc {
                aggressive,
                interleave,
            } => self.do_gc(*aggressive, interleave),
            Op::Read { sel } => {
                if self.model.nodes.is_empty() {
                    return Ok(());
                }
                let reff = self.model.nodes[*sel as usize % self.model.nodes.len()].reff;
                verify_ref(&self.store, &reff, &self.model.expected_flat(&reff))
            }
        }
    }

    /// Execute a `Gc` op: model reap, prepared interleave via GcHooks, an
    /// R2 reader thread when the interleave is nonempty, run_gc_cycle,
    /// then outcome application + model GC semantics.
    pub fn do_gc(
        &mut self,
        aggressive: bool,
        interleave: &[(InterleaveOp, PointSel)],
    ) -> Result<(), String> {
        let opts = if aggressive {
            aggressive_opts()
        } else {
            GcOpts::default()
        };

        // Model mirrors run_gc_cycle step 1 (reap) before anything else.
        self.model.apply_reap(opts.tombstone_grace_cycles);

        // Prepare interleave entries against the post-reap model.
        let mut entries: Vec<PreparedEntry> = Vec::new();
        for (iop, point) in interleave {
            let action = match iop {
                InterleaveOp::CommitDelta { parent_sel, dirty } => {
                    let live = self.model.live_nodes();
                    if live.is_empty() {
                        continue;
                    }
                    let pi = live[*parent_sel as usize % live.len()];
                    let (p_exp, p_node_id, p_ref) = {
                        let p = &self.model.nodes[pi];
                        (p.exp.clone(), p.node_id, p.reff)
                    };
                    let guest_pages = self.model.manifests[&p_ref].guest_pages;
                    let dirty_resolved = self.resolve_dirty(guest_pages, dirty);
                    let tags: Vec<u64> = dirty_resolved.iter().map(|(_, t)| *t).collect();
                    let pages = self.model.record_content(&tags);
                    let container = Self::delta_container(&p_ref, guest_pages, &dirty_resolved);
                    let reff = Manifest::snapshot_ref(&container).to_bytes();
                    let node_id = self.model.alloc_node_id(&p_exp);
                    Action::Commit {
                        container,
                        pages,
                        node: Some((p_exp, node_id, Some(p_node_id))),
                        reff,
                        manifest: ModelManifest {
                            parent: Some(p_ref),
                            entries: dirty_resolved
                                .iter()
                                .map(|(idx, t)| (*idx, page_hash(*t)))
                                .collect(),
                            guest_pages,
                        },
                    }
                }
                InterleaveOp::CommitOrphan { pages } => {
                    let tags = self.model.resolve_tags(pages);
                    let recorded = self.model.record_content(&tags);
                    let container = Self::full_container(&tags);
                    let reff = Manifest::snapshot_ref(&container).to_bytes();
                    Action::Commit {
                        container,
                        pages: recorded,
                        node: None,
                        reff,
                        manifest: ModelManifest {
                            parent: None,
                            entries: tags
                                .iter()
                                .enumerate()
                                .map(|(i, t)| (i as u64, page_hash(*t)))
                                .collect(),
                            guest_pages: tags.len() as u64,
                        },
                    }
                }
                InterleaveOp::Pin { sel } => {
                    let mut domain: Vec<([u8; 32], bool)> =
                        self.model.nodes.iter().map(|n| (n.reff, false)).collect();
                    domain.extend(self.model.orphans.iter().map(|r| (*r, true)));
                    if domain.is_empty() {
                        continue;
                    }
                    let (reff, may_reject) = domain[*sel as usize % domain.len()];
                    Action::PinRef { reff, may_reject }
                }
                InterleaveOp::CreateNodeLate { sel } => {
                    if self.model.orphans.is_empty() {
                        continue;
                    }
                    let reff = self.model.orphans[*sel as usize % self.model.orphans.len()];
                    let exp = self.model.alloc_exp();
                    Action::LateNode { reff, exp }
                }
            };
            entries.push(PreparedEntry {
                point: *point,
                fired: AtomicBool::new(false),
                action,
            });
        }

        let entries = Arc::new(entries);
        let n_entries = entries.len();
        let outcomes: Arc<Mutex<Vec<Option<HookOutcome>>>> =
            Arc::new(Mutex::new((0..n_entries).map(|_| None).collect()));
        let content_snapshot: Arc<HashMap<PageHash, u64>> = Arc::new(self.model.content.clone());
        let r2_active = !entries.is_empty();

        // R2 reader thread: byte-verifies a snapshot of currently-reachable
        // refs until the cycle ends.  Any error/mismatch fails the case.
        let stop = Arc::new(AtomicBool::new(false));
        let reader = if r2_active {
            let expected: Vec<([u8; 32], ExpectedFlat)> = self
                .model
                .root_refs()
                .into_iter()
                .map(|r| (r, self.model.expected_flat(&r)))
                .collect();
            let store = Arc::clone(&self.store);
            let stop = Arc::clone(&stop);
            Some(std::thread::spawn(move || -> Result<(), String> {
                while !stop.load(Ordering::Relaxed) {
                    for (reff, flat) in &expected {
                        verify_ref(&store, reff, flat)
                            .map_err(|e| format!("R2 reader during GC: {e}"))?;
                    }
                    if expected.is_empty() {
                        std::thread::sleep(std::time::Duration::from_micros(200));
                    }
                }
                Ok(())
            }))
        } else {
            None
        };

        let hooks = {
            let entries = Arc::clone(&entries);
            let outcomes = Arc::clone(&outcomes);
            let store = Arc::clone(&self.store);
            let meta = self.meta.clone();
            let content = Arc::clone(&content_snapshot);
            GcHooks::with_callback(move |pt| {
                let kind = match pt {
                    GcPoint::AfterCopy(_) => PointSel::AfterCopy,
                    GcPoint::BeforeFinalize(_) => PointSel::BeforeFinalize,
                    GcPoint::BeforeManifestSweep => PointSel::BeforeManifestSweep,
                    GcPoint::AfterRepoint(_) => {
                        // Widen the R2 race window (04 §4).
                        if r2_active {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        return;
                    }
                    _ => return,
                };
                for (i, e) in entries.iter().enumerate() {
                    if e.point == kind && !e.fired.swap(true, Ordering::SeqCst) {
                        let out = run_action(&store, &meta, &e.action, &content);
                        outcomes.lock().unwrap()[i] = Some(out);
                    }
                }
            })
        };

        let cycle = run_gc_cycle(&self.store, &self.meta, &opts, &hooks);
        // Release the closure's Arc clones so try_unwrap below succeeds.
        drop(hooks);

        stop.store(true, Ordering::Relaxed);
        let reader_result = reader.map(|h| h.join().expect("reader thread panicked"));

        cycle.map_err(|e| format!("run_gc_cycle: {e}"))?;
        if let Some(Err(e)) = reader_result {
            return Err(e);
        }

        // Apply hook outcomes to the model.
        let outcomes = Arc::try_unwrap(outcomes)
            .map_err(|_| "outcomes still shared".to_string())?
            .into_inner()
            .unwrap();
        let entries = Arc::try_unwrap(entries).map_err(|_| "entries still shared".to_string())?;
        let mut just_committed: HashSet<[u8; 32]> = HashSet::new();
        for (e, out) in entries.into_iter().zip(outcomes) {
            let Some(out) = out else { continue }; // point never fired
            match (e.action, out) {
                (
                    Action::Commit {
                        node,
                        reff,
                        manifest,
                        pages,
                        ..
                    },
                    HookOutcome::CommitOk,
                ) => {
                    for (h, t) in pages {
                        self.model.content.insert(h, t);
                    }
                    self.model.manifests.insert(reff, manifest);
                    just_committed.insert(reff);
                    match node {
                        Some((exp, node_id, parent)) => self.model.nodes.push(ModelNode {
                            exp,
                            node_id,
                            parent_node: parent,
                            reff,
                            pruned: false,
                        }),
                        None => {
                            if !self.model.orphans.contains(&reff) {
                                self.model.orphans.push(reff);
                            }
                        }
                    }
                }
                (Action::Commit { .. }, HookOutcome::CommitErr(e)) => {
                    return Err(format!("interleaved commit failed (engine bug?): {e}"));
                }
                (Action::PinRef { reff, .. }, HookOutcome::PinOk) => {
                    self.model.pins.insert(reff);
                }
                (Action::PinRef { reff, may_reject }, HookOutcome::PinRejected(e)) => {
                    if !may_reject {
                        return Err(format!(
                            "interleaved pin of protected node ref rejected: {e}"
                        ));
                    }
                    self.model.orphans.retain(|x| *x != reff);
                }
                (Action::LateNode { reff, exp }, HookOutcome::NodeOk) => {
                    self.model.orphans.retain(|x| *x != reff);
                    self.model.nodes.push(ModelNode {
                        exp,
                        node_id: 0,
                        parent_node: None,
                        reff,
                        pruned: false,
                    });
                }
                (Action::LateNode { reff, .. }, HookOutcome::NodeRejected(reason)) => {
                    // Legal Race B outcome: GC won; the model drops it too.
                    let _ = reason;
                    self.model.orphans.retain(|x| *x != reff);
                }
                (_, out) => return Err(format!("outcome/action mismatch: {out:?}")),
            }
        }

        // Model GC visible semantics: unreachable manifests are swept, so
        // un-pinned orphans are gone — EXCEPT ones committed during this
        // cycle (their registration marked them live; collected next cycle).
        let (reach_m, _) = self.model.reachable();
        self.model
            .orphans
            .retain(|r| reach_m.contains(r) || just_committed.contains(r));
        Ok(())
    }
}

// ── Property checks (04 §4) ───────────────────────────────────────────────────

/// Byte-verify one ref against the model's expected flattened page table.
pub fn verify_ref(
    store: &SnapshotStore,
    reff: &[u8; 32],
    expected: &[(u64, PageHash, u64)],
) -> Result<(), String> {
    let r = SnapshotRef::from_bytes(*reff);
    store
        .get_snapshot(&r)
        .map_err(|e| format!("get_snapshot({}): {e}", hex8(reff)))?;
    let resolved: Vec<(u64, PageHash, Option<bytes::Bytes>)> = store
        .resolve_pages(&r, None, false)
        .map_err(|e| format!("resolve_pages({}): {e}", hex8(reff)))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("resolve_pages({}) iteration: {e}", hex8(reff)))?;
    if resolved.len() != expected.len() {
        return Err(format!(
            "resolve_pages({}) length {} != expected {}",
            hex8(reff),
            resolved.len(),
            expected.len()
        ));
    }
    for ((idx, h, payload), (e_idx, e_h, e_tag)) in resolved.iter().zip(expected) {
        if idx != e_idx || h != e_h {
            return Err(format!(
                "resolve_pages({}): entry (idx {idx}) != expected (idx {e_idx})",
                hex8(reff)
            ));
        }
        let payload = payload
            .as_ref()
            .ok_or_else(|| format!("resolve_pages({}): missing payload idx {idx}", hex8(reff)))?;
        if payload.as_ref() != page(*e_tag).as_ref() {
            return Err(format!(
                "resolve_pages({}): payload bytes differ at idx {idx} (tag {e_tag})",
                hex8(reff)
            ));
        }
    }
    Ok(())
}

/// prop_gc_safety_r1: every model-reachable root ref get_snapshots and
/// resolves byte-identically.  Because resolve_pages walks the full chain
/// and reads every page, this also proves physical ⊇ reachable for both
/// manifests and pages (the safety-only assertion for default GcOpts).
pub fn check_safety_r1(store: &SnapshotStore, model: &Model) -> Result<(), String> {
    for reff in model.root_refs() {
        verify_ref(store, &reff, &model.expected_flat(&reff))?;
    }
    Ok(())
}

/// prop_gc_completeness: after a quiescent aggressive Gc, physical state
/// equals the model's reachable set exactly, and meta has no tombstones
/// or pruned rows left.
pub fn check_completeness(
    store: &SnapshotStore,
    meta: &MetaDb,
    model: &Model,
) -> Result<(), String> {
    let (reach_m, reach_p) = model.reachable();

    let unique = store.pages().unique_pages();
    if unique != reach_p.len() as u64 {
        return Err(format!(
            "completeness: unique_pages {} != model reachable pages {}",
            unique,
            reach_p.len()
        ));
    }

    let physical_m: HashSet<[u8; 32]> = store
        .list_manifest_refs()
        .map_err(|e| format!("list_manifest_refs: {e}"))?
        .into_iter()
        .map(|r| r.to_bytes())
        .collect();
    if physical_m != reach_m {
        return Err(format!(
            "completeness: physical manifests ({}) != model reachable manifests ({})",
            physical_m.len(),
            reach_m.len()
        ));
    }

    let tombs = meta
        .list_tombstones(u64::MAX)
        .map_err(|e| format!("list_tombstones: {e}"))?;
    if !tombs.is_empty() {
        return Err(format!(
            "completeness: {} tombstone(s) left after grace-0 cycle",
            tombs.len()
        ));
    }

    let roots: HashSet<[u8; 32]> = meta
        .gc_root_refs()
        .map_err(|e| format!("gc_root_refs: {e}"))?
        .into_iter()
        .map(|r| r.to_bytes())
        .collect();
    let model_roots = model.root_refs();
    if roots != model_roots {
        return Err(format!(
            "completeness: meta gc_root_refs ({}) != model roots ({}) — pruned rows not reaped?",
            roots.len(),
            model_roots.len()
        ));
    }

    let stats = meta.stats(None).map_err(|e| format!("meta stats: {e}"))?;
    if stats.total_nodes != model.nodes.len() as u64 {
        return Err(format!(
            "completeness: meta total_nodes {} != model {}",
            stats.total_nodes,
            model.nodes.len()
        ));
    }
    Ok(())
}

fn hex8(r: &[u8; 32]) -> String {
    r[..4].iter().map(|b| format!("{b:02x}")).collect()
}

// ── R2 retry-path exerciser ───────────────────────────────────────────────────

/// Drive the R2 repoint→unlink race until the read-retry path fires.
///
/// A reader thread tight-loops byte-verified resolves of ONE multi-pack
/// ref while the main thread runs aggressive GC cycles (threshold 1.01
/// rewrites every live pack each cycle, so every cycle repoints + unlinks
/// every pack).  Returns the observed `GC_READ_RETRIES` delta; any reader
/// error (an R2 violation) fails.  Bounded by `max_cycles` — in practice
/// the retry fires within a few dozen cycles.
pub fn exercise_r2_retry(max_cycles: u32) -> Result<u64, String> {
    use snapstore_pagestore::ingest::GC_READ_RETRIES;

    let mut ex = TapeExec::new()?;
    // 48 pages ≈ 3 packs at 16 records/pack.
    ex.apply_op(&Op::CommitFull {
        pages: vec![PageGen::Fresh; 48],
    })?;
    let reff = ex.model.nodes[0].reff;
    let expected = ex.model.expected_flat(&reff);

    let before = GC_READ_RETRIES.load(Ordering::Relaxed);
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let store = Arc::clone(&ex.store);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || -> Result<(), String> {
            while !stop.load(Ordering::Relaxed) {
                verify_ref(&store, &reff, &expected)
                    .map_err(|e| format!("R2 exerciser reader: {e}"))?;
            }
            Ok(())
        })
    };

    let mut result = Ok(());
    for _ in 0..max_cycles {
        if let Err(e) = run_gc_cycle(&ex.store, &ex.meta, &aggressive_opts(), &GcHooks::none()) {
            result = Err(format!("exerciser run_gc_cycle: {e}"));
            break;
        }
        if GC_READ_RETRIES.load(Ordering::Relaxed) > before {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    reader.join().expect("exerciser reader panicked")?;
    result?;
    Ok(GC_READ_RETRIES.load(Ordering::Relaxed) - before)
}
