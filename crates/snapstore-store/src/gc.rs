//! M7 GC mechanics: mark walk, pack sweep + compaction, manifest sweep.
//!
//! This module is deliberately **meta-free**: roots are supplied by the
//! caller (the server-side orchestrator composes `MetaDb` + this).  The
//! design, invariants (R1–R5), and the race protocol implemented here are
//! specified in `.agents/plans/phase3-m7-gc-exit-gate/02-gc-engine.md`.
//!
//! Ordering contract (R2), enforced per pack:
//! copy live records → fsync new pack + sidecar → (under the sweep gate,
//! after draining late roots) repoint live index entries + remove dead
//! ones → invalidate cached handle → unlink old pack.  A reader racing
//! the unlink retries the index probe once (`read_sealed_with_retry`).

use std::collections::HashSet;

use snapstore_manifest::Manifest;
use snapstore_types::{PackId, PageHash, PageLoc, SnapshotRef};

use crate::{SnapshotStore, StoreError};

/// Byte size of one pack record: 37-byte header + 4096-byte payload.
const RECORD_BYTES: u64 = 37 + 4096;

// ── Hooks (property-suite instrumentation + negative proofs) ─────────────────

/// Named points inside a GC cycle where the property suite injects
/// concurrent operations (controlled interleaving).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcPoint {
    BeforeMark,
    BeforePackSweep(PackId),
    AfterCopy(PackId),
    BeforeFinalize(PackId),
    AfterRepoint(PackId),
    BeforeManifestSweep,
}

/// Deliberately-broken GC modes for the negative proofs (04-property-suite
/// §5).  The enum is always defined so `GcHooks` has one shape, but the
/// only way to *set* one is `GcHooks::sabotaged`, which exists only under
/// the `gc-test-hooks` feature — release builds cannot construct it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sabotage {
    /// Orchestrator-side: drop pins from the root set (safety/R1 break).
    DropPinsFromRoots,
    /// Skip the late-roots drain in finalize (safety/R1 break, Race A/B).
    SkipLateRootsDrain,
    /// Unlink the old pack before repointing the index (R2 break).
    UnlinkBeforeRepoint,
    /// Leave dead index entries in place (completeness break).
    SkipIndexRemoveOfDead,
}

/// Cycle instrumentation.  `GcHooks::none()` in production paths.
#[derive(Default)]
pub struct GcHooks {
    at: Option<Box<dyn Fn(GcPoint) + Send + Sync>>,
    sabotage: Option<Sabotage>,
}

impl GcHooks {
    /// No instrumentation (the only constructor available in release).
    pub fn none() -> Self {
        Self::default()
    }

    /// Interleaving callback, invoked at each `GcPoint`.
    #[cfg(any(test, feature = "gc-test-hooks"))]
    pub fn with_callback(cb: impl Fn(GcPoint) + Send + Sync + 'static) -> Self {
        Self {
            at: Some(Box::new(cb)),
            sabotage: None,
        }
    }

    /// Deliberately-broken GC for negative proofs.  Test-only by
    /// construction: not compiled in release builds.
    #[cfg(any(test, feature = "gc-test-hooks"))]
    pub fn sabotaged(s: Sabotage) -> Self {
        Self {
            at: None,
            sabotage: Some(s),
        }
    }

    #[cfg(any(test, feature = "gc-test-hooks"))]
    pub fn with_callback_and_sabotage(
        cb: impl Fn(GcPoint) + Send + Sync + 'static,
        s: Sabotage,
    ) -> Self {
        Self {
            at: Some(Box::new(cb)),
            sabotage: Some(s),
        }
    }

    fn fire(&self, p: GcPoint) {
        if let Some(cb) = &self.at {
            cb(p);
        }
    }

    /// Orchestrator-facing: fire a `GcPoint` callback from outside this
    /// module.  Every point except `BeforeMark` is fired internally by the
    /// sweep methods below; `BeforeMark` is fired by the caller (the mark
    /// walk itself is orchestrated in `snapstore-server/src/gc.rs`, not
    /// here).  Always available (not feature-gated): `GcHooks::none()` has
    /// no callback installed, so this is a no-op in production.
    pub fn fire_point(&self, p: GcPoint) {
        self.fire(p);
    }

    fn is(&self, s: Sabotage) -> bool {
        self.sabotage == Some(s)
    }

    /// Orchestrator-side query (DropPinsFromRoots is applied where roots
    /// are gathered, not in this module).
    pub fn sabotage(&self) -> Option<Sabotage> {
        self.sabotage
    }
}

// ── Live set ──────────────────────────────────────────────────────────────────

/// The mark state: pages named anywhere in a live manifest chain, and the
/// (memoized) set of live manifest refs.
#[derive(Default)]
pub struct LiveSet {
    pub pages: HashSet<PageHash>,
    pub manifests: HashSet<[u8; 32]>,
}

/// Per-cycle counters reported back to the orchestrator.
#[derive(Debug, Default, Clone)]
pub struct SweepReport {
    pub pages_reclaimed: u64,
    pub bytes_reclaimed: u64,
    pub packs_compacted: u64,
    pub packs_deleted: u64,
    pub manifests_deleted: u64,
    /// Roots whose manifest was missing at mark time (recorded, skipped —
    /// startup reconciliation owns dangling refs; GC must not invent policy).
    pub missing_root_manifests: u64,
}

// ── Mechanics ─────────────────────────────────────────────────────────────────

impl SnapshotStore {
    /// Mark phase: walk each root's manifest chain, inserting every page
    /// hash into `live.pages` and every visited manifest into
    /// `live.manifests`.  Memoized — chains share ancestors heavily.
    /// Runs outside any lock; late arrivals are caught by the finalize
    /// drains.
    pub fn gc_mark(
        &self,
        roots: &[SnapshotRef],
        live: &mut LiveSet,
        report: &mut SweepReport,
    ) -> Result<(), StoreError> {
        const MAX_CHAIN: usize = 4096;
        for root in roots {
            let mut cursor = root.clone();
            let mut depth = 0usize;
            loop {
                if depth >= MAX_CHAIN {
                    return Err(StoreError::ChainDepthExceeded);
                }
                if live.manifests.contains(&cursor.to_bytes()) {
                    break; // shared ancestor already walked
                }
                let bytes = match self.read_manifest_bytes(&cursor) {
                    Ok(b) => b,
                    Err(StoreError::NotFound) if depth == 0 => {
                        // Dangling root ref: record and skip (safety test
                        // names this in the report; policy is reconciliation's).
                        report.missing_root_manifests += 1;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                let m = Manifest::decode(&bytes)?;
                live.manifests.insert(cursor.to_bytes());
                for e in &m.entries {
                    live.pages.insert(e.page_hash);
                }
                if m.delta {
                    cursor = m.parent.expect("delta must have parent");
                    depth += 1;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Sweep + compact every sealed pack below the fence.
    ///
    /// `live` grows as late roots are drained; the caller passes the same
    /// set to `gc_sweep_manifests` afterwards.
    pub fn gc_sweep_packs(
        &self,
        live: &mut LiveSet,
        fence_pack: PackId,
        compact_threshold: f64,
        hooks: &GcHooks,
        report: &mut SweepReport,
    ) -> Result<(), StoreError> {
        let packs: Vec<PackId> = self
            .pages()
            .sealed_pack_ids()?
            .into_iter()
            .filter(|p| *p < fence_pack)
            .collect();

        for p in packs {
            hooks.fire(GcPoint::BeforePackSweep(p));
            self.gc_sweep_one_pack(p, live, compact_threshold, hooks, report)?;
        }
        Ok(())
    }

    fn gc_sweep_one_pack(
        &self,
        p: PackId,
        live: &mut LiveSet,
        compact_threshold: f64,
        hooks: &GcHooks,
        report: &mut SweepReport,
    ) -> Result<(), StoreError> {
        // Index view: records something still points at.  Records in the
        // pack file with no index entry are dead by definition and need no
        // index maintenance.
        let index_entries: Vec<(PageHash, PageLoc)> = self.pages().pack_entries(p);
        // Space view: all records physically in the pack.
        let total_records = self.pages().scan_pack(p)?.len() as u64;

        if total_records == 0 && index_entries.is_empty() {
            // Empty pack: no copy, no repoint — finalize straight to unlink.
            self.finalize_and_unlink(p, &[], &[], live, hooks, report)?;
            report.packs_deleted += 1;
            return Ok(());
        }

        let (live_recs, dead_recs): (Vec<_>, Vec<_>) = index_entries
            .into_iter()
            .partition(|(h, _)| live.pages.contains(h));

        // Explicit-zero branch first; then the threshold comparison
        // (never rely on NaN comparisons).
        let liveness = if total_records == 0 {
            0.0
        } else {
            live_recs.len() as f64 / total_records as f64
        };
        if liveness >= compact_threshold {
            return Ok(()); // healthy pack; dead bytes wait for a later cycle
        }

        // Copy live records into a fresh GC pack (ids allocated above the
        // fence, so this cycle never sweeps its own output).
        let mut copied: Vec<(PageHash, PageLoc)> = Vec::with_capacity(live_recs.len());
        if !live_recs.is_empty() {
            copied.extend(self.gc_copy_records(&live_recs)?);
        }
        hooks.fire(GcPoint::AfterCopy(p));

        // Finalize under the sweep gate with the straggler drain, then
        // invalidate + unlink.
        self.finalize_and_unlink(p, &copied, &dead_recs, live, hooks, report)?;

        report.packs_compacted += u64::from(!copied.is_empty());
        report.packs_deleted += 1;
        report.pages_reclaimed += dead_recs.len() as u64
            + total_records.saturating_sub(copied.len() as u64 + dead_recs.len() as u64);
        report.bytes_reclaimed += total_records.saturating_sub(copied.len() as u64) * RECORD_BYTES;
        Ok(())
    }

    /// Copy `records` (which currently live in some old pack) into a fresh
    /// GC pack; returns their new locations.  Durable on return.
    fn gc_copy_records(
        &self,
        records: &[(PageHash, PageLoc)],
    ) -> Result<Vec<(PageHash, PageLoc)>, StoreError> {
        let mut w = self.pages().create_gc_pack()?;
        let mut staged: Vec<PageHash> = Vec::with_capacity(records.len());
        for (h, loc) in records {
            let payload = self.pages().read_record(loc.pack, loc.offset, h)?;
            crate::fail_point!("gc-compact-copy");
            let buf: &[u8; snapstore_types::PAGE_SIZE] = payload
                .as_ref()
                .try_into()
                .map_err(|_| StoreError::MissingPage(*h))?;
            w.append(h, buf)?;
            staged.push(*h);
        }
        let (new_pack, offsets) = w.seal_and_publish()?;
        Ok(offsets
            .into_iter()
            .map(|(h, off)| {
                (
                    h,
                    PageLoc {
                        pack: new_pack,
                        offset: off,
                    },
                )
            })
            .collect())
    }

    /// The per-pack finalize instant: under the sweep gate, drain late
    /// roots until quiescent, then repoint copied entries and remove dead
    /// ones; after releasing the gate, invalidate the handle cache and
    /// unlink the pack.
    fn finalize_and_unlink(
        &self,
        p: PackId,
        copied: &[(PageHash, PageLoc)],
        dead: &[(PageHash, PageLoc)],
        live: &mut LiveSet,
        hooks: &GcHooks,
        report: &mut SweepReport,
    ) -> Result<(), StoreError> {
        hooks.fire(GcPoint::BeforeFinalize(p));

        // Straggler loop: converges because late_roots only grows via
        // gate-read holders, the drain empties it, and the write lock
        // blocks new registrations during the emptiness check.
        let mut copied_all: Vec<(PageHash, PageLoc)> = copied.to_vec();
        loop {
            let gate = self.sweep_gate();
            let stragglers = if hooks.is(Sabotage::SkipLateRootsDrain) {
                Vec::new()
            } else {
                self.drain_late_roots(&gate)
            };
            if stragglers.is_empty() {
                // Still holding the gate: the irreversible steps.
                if hooks.is(Sabotage::UnlinkBeforeRepoint) {
                    // Negative-proof mode: violate R2 on purpose.
                    self.pages().invalidate_pack_handle(p);
                    self.pages().delete_pack(p)?;
                }
                let mid = copied_all.len() / 2;
                for (i, (h, new_loc)) in copied_all.iter().enumerate() {
                    if i == mid {
                        crate::fail_point!("gc-index-repoint");
                    }
                    self.pages().repoint_if_in_pack(h, p, *new_loc);
                }
                if !hooks.is(Sabotage::SkipIndexRemoveOfDead) {
                    for (h, _) in dead {
                        // Guard: a straggler may have made this hash live
                        // after partitioning — never remove a live hash.
                        if !live.pages.contains(h) {
                            self.pages().remove_if_in_pack(h, p);
                        }
                    }
                }
                drop(gate);
                break;
            }
            drop(gate);

            // Walk stragglers' closures; copy any record of p that just
            // became live and wasn't copied yet.
            self.gc_mark(&stragglers, live, report)?;
            let already: HashSet<PageHash> = copied_all.iter().map(|(h, _)| *h).collect();
            let newly_live: Vec<(PageHash, PageLoc)> = dead
                .iter()
                .filter(|(h, _)| live.pages.contains(h) && !already.contains(h))
                .cloned()
                .collect();
            if !newly_live.is_empty() {
                copied_all.extend(self.gc_copy_records(&newly_live)?);
            }
        }

        hooks.fire(GcPoint::AfterRepoint(p));

        if !hooks.is(Sabotage::UnlinkBeforeRepoint) {
            self.pages().invalidate_pack_handle(p);
            self.pages().delete_pack(p)?;
        }
        Ok(())
    }

    /// Manifest sweep: delete every stored manifest not in `live.manifests`.
    /// Unlinks happen **under the sweep gate**, batched, with a re-drain
    /// before each batch (review finding A3 — releasing the lock before
    /// unlinking reopens Race B via idempotent re-put + CreateNode).
    pub fn gc_sweep_manifests(
        &self,
        live: &mut LiveSet,
        hooks: &GcHooks,
        report: &mut SweepReport,
    ) -> Result<(), StoreError> {
        hooks.fire(GcPoint::BeforeManifestSweep);

        // Candidate list computed outside the lock (O(manifests) dir walk).
        let mut candidates: Vec<SnapshotRef> = self
            .list_manifest_refs()?
            .into_iter()
            .filter(|r| !live.manifests.contains(&r.to_bytes()))
            .collect();

        const BATCH: usize = 256;
        while !candidates.is_empty() {
            let gate = self.sweep_gate();
            let stragglers = if hooks.is(Sabotage::SkipLateRootsDrain) {
                Vec::new()
            } else {
                self.drain_late_roots(&gate)
            };
            if !stragglers.is_empty() {
                // Walking closures may read manifests; cheap enough to do
                // under the gate (bounded by straggler count), and required
                // so the subtraction below is race-free.
                self.gc_mark(&stragglers, live, report)?;
                candidates.retain(|r| !live.manifests.contains(&r.to_bytes()));
            }
            let batch: Vec<SnapshotRef> = candidates.drain(..candidates.len().min(BATCH)).collect();
            for r in &batch {
                if live.manifests.contains(&r.to_bytes()) {
                    continue;
                }
                if self.delete_manifest(r)? {
                    report.manifests_deleted += 1;
                }
            }
            drop(gate);
        }
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::*;
    use crate::{GcEpochError, SnapshotStore, StoreOpts};
    use snapstore_manifest::DeviceBlob;
    use snapstore_types::PAGE_SIZE;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn empty_blob() -> DeviceBlob {
        DeviceBlob {
            format: 0,
            zstd: false,
            bytes: vec![],
            raw_len: 0,
        }
    }

    /// Distinct 4 KiB page stamped with `tag`.
    fn page(tag: u64) -> [u8; PAGE_SIZE] {
        let mut p = [0u8; PAGE_SIZE];
        p[..8].copy_from_slice(&tag.to_le_bytes());
        p
    }

    /// Tiny packs so sweeps see multiple sealed packs.
    fn small_pack_store(dir: &TempDir) -> SnapshotStore {
        let mut opts = StoreOpts::default();
        opts.pagestore.max_pack_bytes = 16 * 4133; // ~16 records per pack
        SnapshotStore::open_with_options(dir.path(), opts).unwrap()
    }

    /// Commit a FULL snapshot of `n` pages tagged from `base`.
    fn commit_full(store: &SnapshotStore, base: u64, n: u64) -> SnapshotRef {
        let pages: Vec<[u8; PAGE_SIZE]> = (0..n).map(|i| page(base + i)).collect();
        let refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().collect();
        store.pages().ingest(&refs).unwrap();
        let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p))
            .collect();
        let c = build_full_container(n * PAGE_SIZE as u64, &pairs, empty_blob());
        store.put_snapshot(&c).unwrap()
    }

    /// Run a full quiescent, aggressive cycle with `roots`.
    fn run_cycle(store: &SnapshotStore, roots: Vec<SnapshotRef>, hooks: &GcHooks) -> SweepReport {
        let (fence, roots) = store
            .begin_gc_epoch(|| Ok::<_, std::convert::Infallible>(roots))
            .map_err(|_| "epoch")
            .unwrap();
        let mut live = LiveSet::default();
        let mut report = SweepReport::default();
        store.gc_mark(&roots, &mut live, &mut report).unwrap();
        store
            .gc_sweep_packs(&mut live, fence.fence_pack, 1.01, hooks, &mut report)
            .unwrap();
        store
            .gc_sweep_manifests(&mut live, hooks, &mut report)
            .unwrap();
        store.end_gc_epoch();
        report
    }

    /// Quiescent aggressive GC: physical state == reachable set exactly
    /// (completeness), and every survivor still resolves byte-identically
    /// (safety R1).
    #[test]
    fn quiescent_gc_exact_and_safe() {
        #[cfg(feature = "failpoints")]
        let _fp = crate::tests::fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = small_pack_store(&dir);

        let keep = commit_full(&store, 0, 40);
        let drop_ = commit_full(&store, 1000, 40);
        let keep2 = commit_full(&store, 2000, 8);

        // Rotate so all data is below the fence.
        store.pages().rotate_active().unwrap();

        let before = store.pages().unique_pages();
        assert_eq!(before, 88);

        let report = run_cycle(&store, vec![keep.clone(), keep2.clone()], &GcHooks::none());

        // Completeness: exactly the 48 reachable pages remain indexed.
        assert_eq!(store.pages().unique_pages(), 48, "physical == reachable");
        assert_eq!(report.manifests_deleted, 1);
        assert!(!store.has_manifest(&drop_));

        // Safety R1: survivors resolve with correct bytes.
        for (r, n, base) in [(&keep, 40u64, 0u64), (&keep2, 8, 2000)] {
            let resolved: Vec<_> = store
                .resolve_pages(r, None, false)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(resolved.len(), n as usize);
            for (idx, _, payload) in resolved {
                assert_eq!(payload.unwrap().as_ref(), page(base + idx).as_ref());
            }
        }

        // Reopen: nothing resurrects, everything still resolves.
        drop(store);
        let store2 = small_pack_store(&dir);
        assert_eq!(store2.pages().unique_pages(), 48);
        assert!(store2.get_snapshot(&keep).is_ok());
        assert!(matches!(
            store2.get_snapshot(&drop_),
            Err(StoreError::NotFound)
        ));
    }

    /// Straggler protection (Race A/B): a commit landing at BeforeFinalize
    /// whose pages sit in the pack being swept must survive, and its
    /// manifest must survive the manifest sweep.
    #[test]
    fn straggler_commit_survives_finalize() {
        #[cfg(feature = "failpoints")]
        let _fp = crate::tests::fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = Arc::new(small_pack_store(&dir));

        // Orphan pages + manifest: not in the root set.
        let orphan = commit_full(&store, 5000, 12);
        let rooted = commit_full(&store, 6000, 12);
        store.pages().rotate_active().unwrap();

        // At BeforeFinalize of each pack, an interleaved client re-puts the
        // orphan container (idempotent early-return path → registration).
        let orphan_pages: Vec<[u8; PAGE_SIZE]> = (0..12).map(|i| page(5000 + i)).collect();
        let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = orphan_pages
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p))
            .collect();
        let orphan_container = build_full_container(12 * PAGE_SIZE as u64, &pairs, empty_blob());

        let s2 = Arc::clone(&store);
        let fired = std::sync::atomic::AtomicBool::new(false);
        let hooks = GcHooks::with_callback(move |pt| {
            if matches!(pt, GcPoint::BeforeFinalize(_))
                && !fired.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                // Re-put must either protect the orphan or fail loudly;
                // at BeforeFinalize nothing is dropped yet, so it protects.
                s2.put_snapshot(&orphan_container).unwrap();
            }
        });

        let report = run_cycle(&store, vec![rooted.clone()], &hooks);

        // The orphan was re-registered mid-sweep: manifest + pages survive.
        assert!(store.has_manifest(&orphan), "straggler manifest survives");
        let resolved: Vec<_> = store
            .resolve_pages(&orphan, None, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(resolved.len(), 12);
        for (idx, _, payload) in resolved {
            assert_eq!(payload.unwrap().as_ref(), page(5000 + idx).as_ref());
        }
        assert_eq!(report.manifests_deleted, 0);
    }

    /// R4: a second begin_gc_epoch while one is active is refused.
    #[test]
    fn epoch_latch_refuses_concurrent() {
        #[cfg(feature = "failpoints")]
        let _fp = crate::tests::fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = small_pack_store(&dir);

        let (_f, _r) = store
            .begin_gc_epoch(|| Ok::<_, std::convert::Infallible>(()))
            .map_err(|_| "first epoch must begin")
            .unwrap();
        assert!(matches!(
            store.begin_gc_epoch(|| Ok::<_, std::convert::Infallible>(())),
            Err(GcEpochError::AlreadyRunning)
        ));
        store.end_gc_epoch();
        // After ending, a new epoch begins fine.
        let again = store.begin_gc_epoch(|| Ok::<_, std::convert::Infallible>(()));
        assert!(again.is_ok());
        store.end_gc_epoch();
    }
}
