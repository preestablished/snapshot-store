//! M7 GC orchestrator (D3, `.agents/plans/phase3-m7-gc-exit-gate/00-overview.md`).
//!
//! `snapstore-store::gc` owns the mechanics (mark walk, pack sweep +
//! compaction, manifest sweep) operating on caller-supplied roots;
//! `snapstore-store` has no visibility into `snapstore-meta`.  This module
//! composes both: it supplies the root set (`gc_root_refs`), reaps
//! tombstoned subtrees, and persists cycle state (`gc_state`).
//!
//! Cycle: reap tombstones -> optional rotate -> fence + mark -> sweep
//! packs -> sweep manifests -> persist `gc_state`.  See
//! `.agents/plans/phase3-m7-gc-exit-gate/02-gc-engine.md` §3.

use std::collections::HashSet;
use std::time::Instant;

use parking_lot::Mutex;

use snapstore_meta::{GcStateRow, MetaDb, MetaError};
use snapstore_store::gc::{GcHooks, GcPoint, LiveSet, Sabotage, SweepReport};
use snapstore_store::{GcEpochError, SnapshotStore, StoreError};

// ── Options / report / error ────────────────────────────────────────────────

/// Per-cycle options.  `GcOpts::default()` matches the `[gc]` config
/// defaults (compact_threshold 0.5, rotate_active_first false,
/// tombstone_grace_cycles 1).
#[derive(Debug, Clone)]
pub struct GcOpts {
    pub compact_threshold: f64,
    pub rotate_active_first: bool,
    pub tombstone_grace_cycles: u32,
}

impl Default for GcOpts {
    fn default() -> Self {
        Self {
            compact_threshold: 0.5,
            rotate_active_first: false,
            tombstone_grace_cycles: 1,
        }
    }
}

/// Counts and timing reported back to the RPC handler / auto-trigger.
#[derive(Debug, Default, Clone)]
pub struct GcReport {
    pub nodes_reaped: u64,
    pub manifests_deleted: u64,
    pub pages_reclaimed: u64,
    pub bytes_reclaimed: u64,
    pub packs_compacted: u64,
    pub packs_deleted: u64,
    pub missing_root_manifests: u64,
    pub duration_ms: u64,
}

/// Errors from `run_gc_cycle` / `GcRunner::run`.
#[derive(Debug)]
pub enum GcError {
    /// R4: a cycle is already running (either the caller's `GcRunner`
    /// latch, or the store's own epoch latch as a backstop).
    AlreadyRunning,
    Store(StoreError),
    Meta(MetaError),
}

impl std::fmt::Display for GcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcError::AlreadyRunning => write!(f, "a GC cycle is already running"),
            GcError::Store(e) => write!(f, "store error: {e}"),
            GcError::Meta(e) => write!(f, "meta error: {e}"),
        }
    }
}

impl std::error::Error for GcError {}

impl From<StoreError> for GcError {
    fn from(e: StoreError) -> Self {
        GcError::Store(e)
    }
}

impl From<MetaError> for GcError {
    fn from(e: MetaError) -> Self {
        GcError::Meta(e)
    }
}

// ── Cycle-scope latch ────────────────────────────────────────────────────────

/// Owns the cycle-scope latch for **one** store/meta pair.
///
/// A module-level `static Mutex<()>` would be wrong: multiple
/// `SnapshotStore`/`MetaDb` instances can exist in one process (every test
/// spins its own), and a static latch would serialize GC across all of
/// them.  The server holds one `GcRunner` per `SnapshotStoreServer`
/// instance instead (review finding A7).
///
/// R4 (never self-concurrent) has two layers: this latch (cheap, catches
/// the common RPC-vs-auto-trigger race before touching the store) and
/// `SnapshotStore::begin_gc_epoch`'s own epoch latch (the backstop — see
/// 02-gc-engine.md §2).
pub struct GcRunner {
    latch: Mutex<()>,
}

impl GcRunner {
    pub fn new() -> Self {
        Self {
            latch: Mutex::new(()),
        }
    }

    /// Run one GC cycle, refusing if another is already in flight for this
    /// runner.  Synchronous; callers on an async runtime must wrap this in
    /// `spawn_blocking`.
    pub fn run(
        &self,
        store: &SnapshotStore,
        meta: &MetaDb,
        opts: &GcOpts,
        hooks: &GcHooks,
    ) -> Result<GcReport, GcError> {
        let _guard = self.latch.try_lock().ok_or(GcError::AlreadyRunning)?;
        run_gc_cycle(store, meta, opts, hooks)
    }

    /// Try to acquire the cycle-scope latch without running a cycle.  Used
    /// by [`run_and_record`] so the `gc_running` gauge covers exactly the
    /// span the latch is held.
    fn try_acquire(&self) -> Option<parking_lot::MutexGuard<'_, ()>> {
        self.latch.try_lock()
    }
}

impl Default for GcRunner {
    fn default() -> Self {
        Self::new()
    }
}

// ── Cycle body ───────────────────────────────────────────────────────────────

/// Guard that ends the GC epoch on every exit path (success, `?`
/// early-return, or panic-unwind) once `begin_gc_epoch` has succeeded.
struct EpochGuard<'a>(&'a SnapshotStore);

impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        self.0.end_gc_epoch();
    }
}

/// Run one GC cycle: reap tombstones, optionally rotate the active pack,
/// fence + mark from the meta root set, sweep packs then manifests,
/// persist `gc_state`.
///
/// Callers that need R4 protection across concurrent triggers should go
/// through `GcRunner::run` instead; this free function has no latch of its
/// own (unit tests call it directly against a single store).
pub fn run_gc_cycle(
    store: &SnapshotStore,
    meta: &MetaDb,
    opts: &GcOpts,
    hooks: &GcHooks,
) -> Result<GcReport, GcError> {
    let start = Instant::now();
    let mut report = GcReport::default();

    // 1. Reap tombstones. Horizon: with grace_cycles == 0, reap everything;
    // otherwise only tombstones that existed before the previous cycle's
    // fence (last_fence_counter), so a subtree pruned mid-cycle survives at
    // least one full cycle before its rows are dropped.
    let prior_state = meta.gc_state()?;
    let horizon = if opts.tombstone_grace_cycles == 0 {
        u64::MAX
    } else {
        prior_state.last_fence_counter
    };
    for t in meta.list_tombstones(horizon)? {
        report.nodes_reaped += meta.reap_tombstone(&t.experiment_id, t.node_id)?;
    }

    // 2. Optionally rotate the active pack so all pre-cycle data becomes
    // sweepable this cycle.
    if opts.rotate_active_first {
        store
            .pages()
            .rotate_active()
            .map_err(|e| GcError::Store(StoreError::from(e)))?;
    }

    // 3. Fence + roots. The root-snapshot closure runs inside
    // begin_gc_epoch's write-lock hold (R3): no manifest can publish
    // between the root read and the fence record.
    let sabotage_drop_pins = hooks.sabotage() == Some(Sabotage::DropPinsFromRoots);
    let (fence, roots) = store
        .begin_gc_epoch(|| -> Result<Vec<snapstore_types::SnapshotRef>, MetaError> {
            let mut roots = meta.gc_root_refs()?;
            if sabotage_drop_pins {
                // Negative-proof mode (04 §5): drop pins from the root set
                // to prove the property suite catches an R1 safety break.
                // Never reachable in production (Sabotage is only
                // constructible under the gc-test-hooks feature).
                let pinned: HashSet<[u8; 32]> = meta
                    .list_pins()?
                    .into_iter()
                    .map(|p| p.snapshot_ref.to_bytes())
                    .collect();
                roots.retain(|r| !pinned.contains(&r.to_bytes()));
            }
            Ok(roots)
        })
        .map_err(|e| match e {
            GcEpochError::AlreadyRunning => GcError::AlreadyRunning,
            GcEpochError::Roots(me) => GcError::Meta(me),
        })?;
    // From here on every exit path (including `?`) must end the epoch.
    let _epoch_guard = EpochGuard(store);

    // Logical counter observed at (approximately) the fence instant, for
    // the next cycle's reap horizon. Meta has its own counter independent
    // of the store's fence_pack; reading it immediately after the fence
    // succeeds is the closest available approximation.
    let fence_logical_counter = meta.stats(None)?.logical_counter;

    // 4. Mark. BeforeMark is the one GcPoint the engine never fires itself
    // (the mark walk is orchestrated here, not in snapstore-store::gc) —
    // fire it explicitly so the property suite can inject at this point.
    hooks.fire_point(GcPoint::BeforeMark);
    let mut live = LiveSet::default();
    let mut sweep = SweepReport::default();
    store.gc_mark(&roots, &mut live, &mut sweep)?;

    // 5. Sweep packs, then manifests.
    store.gc_sweep_packs(
        &mut live,
        fence.fence_pack,
        opts.compact_threshold,
        hooks,
        &mut sweep,
    )?;
    store.gc_sweep_manifests(&mut live, hooks, &mut sweep)?;

    let finished_logical_counter = meta.stats(None)?.logical_counter;

    // 6. Persist gc_state.
    meta.set_gc_state(GcStateRow {
        cycles_total: prior_state.cycles_total + 1,
        last_fence_counter: fence_logical_counter,
        last_finished_at: finished_logical_counter,
        last_freed_bytes: sweep.bytes_reclaimed,
    })?;

    report.manifests_deleted = sweep.manifests_deleted;
    report.pages_reclaimed = sweep.pages_reclaimed;
    report.bytes_reclaimed = sweep.bytes_reclaimed;
    report.packs_compacted = sweep.packs_compacted;
    report.packs_deleted = sweep.packs_deleted;
    report.missing_root_manifests = sweep.missing_root_manifests;
    report.duration_ms = start.elapsed().as_millis() as u64;

    // _epoch_guard drops here, calling end_gc_epoch on the success path
    // (error paths hit it via `?` unwinding through the guard's scope).
    Ok(report)
}

/// Run one cycle through `runner`, updating the `gc_running` gauge for
/// exactly the span the latch is held and recording the completed-cycle
/// metrics on success.  Shared by the `TriggerGc` RPC handler and the
/// watermark auto-trigger task so both paths stay consistent.
pub fn run_and_record(
    runner: &GcRunner,
    store: &SnapshotStore,
    meta: &MetaDb,
    opts: &GcOpts,
    hooks: &GcHooks,
    metrics: &crate::metrics::Metrics,
) -> Result<GcReport, GcError> {
    let _guard = match runner.try_acquire() {
        Some(g) => g,
        None => return Err(GcError::AlreadyRunning),
    };
    metrics.gc_running.set(1);
    let result = run_gc_cycle(store, meta, opts, hooks);
    metrics.gc_running.set(0);
    if let Ok(report) = &result {
        metrics.record_gc_cycle(report);
    }
    result
}
