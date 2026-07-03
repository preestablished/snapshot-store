# WI2 — GC Engine

The cycle: reap → fence → mark → sweep (packs, then manifests) → persist
state. Mechanics live in `snapstore-store` (new module `src/gc.rs`, plus
epoch state on `SnapshotStore`); orchestration in
`snapstore-server/src/gc.rs` (composes `MetaDb` + `SnapshotStore`) — D3.

## 1. Why there are two commit races, and the protocol that closes them

The design doc's fence rule ("manifests committed at-or-after the fence are
unconditionally live; sweep never touches packs >= fence_pack") assumes a
post-fence manifest's pages live in post-fence packs. **False here**: a
client may `PutPages` long before `PutSnapshot`, so a post-fence manifest
can reference unmarked pages sitting in pre-fence packs. Two concrete races:

- **Race A (put_snapshot vs pack sweep):** put_snapshot's presence check
  (`contains_batch`, lib.rs:368-378) sees a page → sweep drops that page
  and removes its index entry → manifest publishes referencing a dropped
  page → R1 violation on the next read.
- **Race B (create_node / pin vs manifest sweep):** a manifest committed
  *before* the fence with no node row is garbage by the root rule; a
  concurrent `CreateNode` (or `Pin`) validates `has_manifest` → sweep
  unlinks the manifest → the node/pin row lands dangling.

Protocol (D5), three parts. **Revised after adversarial review** (see
`07-review-log.md`): registration alone is NOT protection — sweep progress
is irreversible per pack, so a late root arriving after its pages' pack was
finalized must be *rejected*, not just recorded. The mechanism is a gated,
**validating** registration.

1. **Widen the gate read lock** in `put_snapshot` to cover steps **2–6**:
   acquire `gc_commit_gate.read()` **before** the delta parent check
   (step 2, lib.rs:355-365) and the `contains_batch` presence check
   (step 3) — not after them as today (lib.rs:389). The group-commit
   barrier (step 4) stays inside the read lock (no deadlock: the flusher
   never touches the gate; verified lock order gate → active → shard is
   consistent everywhere). Cost: the fence write acquisition now waits for
   in-flight commits including their fdatasync — acceptable, once per
   cycle plus once per finalized pack/manifest batch.
2. **Gated, validating live-ref registration** on `SnapshotStore`:

   ```rust
   /// Some(..) while a GC cycle is running.
   gc_epoch: Mutex<Option<GcEpochState>>,   // GcEpochState { late_roots: Vec<SnapshotRef>, fence_pack: PackId }

   /// MUST be called while holding gc_commit_gate.read() (take a guard
   /// param or expose a combined `with_commit_gate(|reg| ...)` API so the
   /// requirement is unforgeable). Steps:
   ///   1. If an epoch is active, append `r` to late_roots.
   ///   2. Validate r's full dependency closure: walk the manifest chain
   ///      (every .spm present + decodes) and contains_batch every chain
   ///      manifest's page hashes; any miss → Err (chain broken / pages
   ///      collected).
   /// Registration precedes validation, so there is no TOCTOU: finalize
   /// removes index entries / unlinks manifests only under gate.write()
   /// after draining late_roots — a ref registered before the drain gets
   /// marked; one registered after observes the post-finalize state and
   /// its validation fails cleanly.
   pub fn register_live_ref(&self, r: &SnapshotRef) -> Result<(), StoreError>;
   ```

   Callers, all under `gc_commit_gate.read()`:
   - `put_snapshot` itself: after the manifest is durable, register the
     new ref (cheap: its own chain was just validated by steps 2–3; for a
     delta, the parent-chain closure must be validated too — the parent
     may be a pre-fence orphan whose pages a finalized pack already
     dropped). **Including the idempotent early-return path**
     (lib.rs:404-406) — a blind-retry re-put of a doomed manifest must
     register or fail, never silently Ok.
   - the **server** `create_node` handler: hold the gate read lock across
     register → `has_manifest` → `meta.create_node` (service.rs:502).
     Failure → NOT_FOUND.
   - the **server** `pin` handler: same shape (service.rs:870-900 — note
     it currently has NO manifest validation at all; this adds it).
     Failure → FAILED_PRECONDITION. This also fixes dangling-pin creation
     (fsck `DanglingPin`), which the crash-harness invariant (05 §3)
     depends on.

   Holding the gate across the meta write closes the epoch-install race:
   a CreateNode in flight across `begin_gc_epoch` either commits its row
   before the fence write acquisition (visible to the root snapshot) or
   starts after epoch install (registration live).
3. **Finalize-under-write with straggler drain** (per pack, and per
   manifest-unlink batch) — §5/§6 below. Straggler mark-walks must ingest
   the full closure of drained refs (chain manifests into `visited`,
   chain pages into `mark`).

## 2. Epoch / fence API on SnapshotStore

```rust
/// Begin a GC epoch. Takes gc_commit_gate.write() for the fence instant:
/// inside the write lock, (a) records fence_pack = pages.active_pack_id(),
/// (b) installs GcEpochState, (c) runs `snapshot_roots` (the meta root-set
/// read, supplied by the orchestrator as a closure so snapstore-store
/// stays meta-free). Returns the roots + fence. Errors if an epoch is
/// already active (R4: never self-concurrent).
pub fn begin_gc_epoch<R, E>(&self, snapshot_roots: impl FnOnce() -> Result<R, E>) -> Result<(GcFence, R), GcError>;

/// End the epoch (always — success or failure paths; use a guard type).
pub fn end_gc_epoch(&self);

/// Drain late roots accumulated since the last drain. MUST be called while
/// holding gc_commit_gate.write() (enforce: this is a private method of
/// the sweep, or take a proof token). Returns refs to mark-walk.
fn drain_late_roots(&self) -> Vec<SnapshotRef>;
```

R3 satisfied: no manifest can publish between the root snapshot and the
fence record because both happen inside one write-lock hold and commits
hold the read lock across presence-check→publish.

R4: the epoch latch alone is NOT enough — reap (step 1) and rotate
(step 2) run before `begin_gc_epoch`, so two racing triggers (RPC +
watermark) would double-reap/double-rotate. `run_gc_cycle` takes a
cycle-scope `try_lock` mutex at entry (owned by the orchestrator; the
loser returns `AlreadyRunning` immediately); `begin_gc_epoch` stays as the
store-level backstop. No `mark-<epoch>.state` resume file — a crashed GC
is simply discarded (ARCHITECTURE R4 allows this; extra copies reclaimed
next cycle).

Gate type: switch `gc_commit_gate` to `parking_lot::RwLock<()>` (already a
workspace dependency via pagestore). A GC panic while holding the write
lock must not poison the gate — with `std::sync::RwLock`, poisoning would
permanently error every subsequent `put_snapshot` (lib.rs:389-392 maps
poison to error) until restart. The failpoint runs panic on purpose;
parking_lot has no poisoning. Adjust put_snapshot's error mapping.

## 3. Cycle structure (orchestrator, snapstore-server/src/gc.rs)

```rust
pub struct GcOpts {
    pub compact_threshold: f64,      // default 0.5; aggressive 0.9; tests 1.0+
    pub rotate_active_first: bool,   // aggressive/tests: true
    pub tombstone_grace_cycles: u32, // default 1; tests 0
}
pub struct GcReport { nodes_reaped, manifests_deleted, pages_reclaimed,
                      bytes_reclaimed, packs_compacted, duration_ms, .. }

pub fn run_gc_cycle(store: &SnapshotStore, meta: &MetaDb, opts: &GcOpts,
                    hooks: &GcHooks) -> Result<GcReport, GcError>
```

Steps:

1. **Reap tombstones.** Horizon: with `grace_cycles = 1`, reap tombstones
   with `created_at <= gc_state.last_fence_counter` (i.e. they existed
   before the previous cycle began); with 0, all. One
   `meta.reap_tombstone` txn per subtree (01 §4).
2. **Optionally rotate** the active pack (`opts.rotate_active_first`).
3. **Fence + roots:** `store.begin_gc_epoch(|| meta.gc_root_refs())`.
4. **Mark** (§4) from those roots.
5. **Sweep packs** (§5), then **sweep manifests** (§6).
6. Persist `gc_state` (cycles_total+1, fence counter =
   meta logical counter observed at step 3, freed bytes), update metrics,
   `end_gc_epoch` (guard ensures this on error paths too).

## 4. Mark

Inputs: root refs. State: `mark: HashSet<PageHash>` (hashbrown, same memory
envelope as the page index), `visited: HashSet<[u8;32]>` of manifest refs.

For each root ref: walk the manifest chain (`get_snapshot` → decode →
insert every `entries[].page_hash` into `mark`, add ref to `visited`,
follow `parent` until FULL or already-visited; memoize — chains share
ancestors heavily; depth cap 4096 like resolve_pages, lib.rs:481). A root
whose manifest is **missing** is recorded in the report
(`missing_root_manifests`) and skipped — startup reconciliation owns
dangling refs, GC must not invent policy. Do NOT use `resolve_pages`
(it flattens; mark wants raw per-manifest entries — shadowed pages are
conservatively live, ARCHITECTURE §4.2).

Mark runs **outside** any lock — commits proceed concurrently; late
arrivals are caught by the late-roots drains.

## 5. Pack sweep + compaction

For each sealed pack `p < fence_pack` (snapshot the list once,
`sealed_pack_ids()` filtered):

1. `records = scan sidecar / index.entries_for_pack(p)` — use the
   **index** view, not the raw pack scan: records whose index entry
   already points elsewhere (earlier compaction, duplicate) are dead by
   definition. `live = records ∩ mark` (plus everything if `p >= fence`,
   excluded already).
2. If the pack has zero index records → skip straight to finalize/unlink
   (explicit branch; do NOT rely on `NaN >= threshold` being false).
   Else `liveness = live_bytes / total_record_bytes` (record = 4133
   bytes, pack.rs:9-19; total from scan or sidecar count). If
   `liveness >= compact_threshold` → leave the pack alone (dead bytes wait
   for a later cycle), continue. Note for tests: threshold 1.01 compacts
   even 100%-live packs (1.0 < 1.01) — intended, but it means the
   quiescent property rewrites all pre-fence data each cycle; say so in
   the suite comments (04 §3).
3. **Copy:** `w = pages().create_gc_pack()`; for each live record
   `read_record(p, off, hash)` → `w.append(hash, payload)`.
   Failpoint `gc-compact-copy` inside the loop.
4. **Publish:** `w.seal_and_publish()` — fsync pack, write sidecar
   (failpoint `gc-compact-seal` between them). New copies durable; index
   still points at `p`. (Crash here: extra copies only — R4.)
5. **Finalize under gate write, with straggler drain:**

   ```
   loop {
       let _w = gc_commit_gate.write();
       let stragglers = drain_late_roots();
       if stragglers.is_empty() { break /* holding _w */ }
       drop(_w);
       mark-walk stragglers' FULL closures (chain manifests → visited,
           chain pages → mark);
       for any newly-live record of p not yet copied: copy+publish it
           (a small follow-up gc pack per straggler round — but skip pack
           creation entirely when the straggler round adds no records of
           p, to avoid empty packs);
   }
   // still holding the write lock:
   for each live record: index.repoint_if_in_pack(hash, p, new_loc);
   for each dead record: index.remove_if_in_pack(hash, p);
   // failpoint `gc-index-repoint` in the middle of this loop;
   drop write lock.
   ```

   Convergence: `late_roots` only grows via commits, each drain empties
   it, and the write lock blocks new commits during the empty-check — the
   loop exits after finitely many rounds (in practice 0–1).
6. **Invalidate + unlink:** `invalidate_pack_handle(p)`; `delete_pack(p)`
   (failpoint `gc-pack-unlink` inside). R2 ordering proof: every live
   entry was repointed durably-backed *before* unlink; a racing reader
   holding the old `PageLoc` hits ENOENT → `read_sealed_with_retry`
   re-probes → new loc (or `None` for a genuinely dead page, which no live
   manifest references). An `Arc<File>` already cloned out keeps the
   unlinked inode alive for in-flight preads — the retry is for
   *new* opens only.

Accounting: pages_reclaimed += dead records; bytes_reclaimed += dead ×
4133 + whole-pack overhead when applicable.

**Reads during GC:** nothing else needed — the retry path exists
(ingest.rs:566-604). The property suite (04) proves it.

## 6. Manifest sweep

Delete every stored manifest not in `visited` (mark's memoized set).
**Unlinks happen under the gate write lock, in batches** — the earlier
draft released the lock before unlinking, which reopened Race B via the
idempotent re-put + CreateNode window (review finding; see
`07-review-log.md`).

1. Compute `candidates = list_manifest_refs() - visited` **outside** any
   lock (lib.rs:573 walks the shard dirs — holding the write lock across
   that walk would stall all commits for O(manifests) I/O).
2. Loop over `candidates` in batches (e.g. 256):
   - take `gc_commit_gate.write()`;
   - drain late_roots; mark-walk their closures (chains → `visited`,
     pages → `mark`); if the drain was non-empty, subtract the newly
     visited refs from all remaining candidates;
   - `delete_manifest` each ref in the batch still not in `visited`
     (idempotent — Ok(false) if already gone);
   - release, next batch.

   Registrations are serialized with the gate (§1.2), so a
   `register_live_ref` lands either before a batch's drain (ref joins
   `visited`, survives) or after that batch's unlinks (its validation
   observes the missing manifest and fails the caller cleanly).
3. `visited` includes every post-fence manifest by construction: every
   `put_snapshot` — including the exists-early-return path — registers
   under the read lock (§1.2). Keep the belt-and-braces debug assert:
   before each batch's unlinks, no candidate is younger than the fence
   (track refs registered this epoch in GcEpochState for the check).

## 7. Failpoints (all behind the existing `failpoints` feature)

| Name | Site |
|---|---|
| `gc-compact-copy` | inside the copy loop, per record |
| `gc-compact-seal` | between gc-pack fsync and sidecar write |
| `gc-index-repoint` | mid-repoint loop (some entries moved, some not) |
| `gc-pack-unlink` | before the .idx unlink (which precedes the .spk unlink — order rationale in 01 §2) |
| `gc-manifest-unlink` | before a manifest unlink |
| `gc-reap-txn` | inside reap_tombstone, before COMMIT (needs the new snapstore-meta failpoints feature — 01 §4) |

Use the local `fail_point!` macro pattern (store lib.rs:36-41). Add all six
to the crash-harness matrix list (05).

## 8. Test hooks (feature `gc-test-hooks` on snapstore-store + snapstore-server)

The property suite needs deterministic interleaving and breakable guards:

```rust
#[cfg(any(test, feature = "gc-test-hooks"))]
pub struct GcHooks {
    /// Called at named points: BeforeMark, BeforePackSweep(PackId),
    /// AfterCopy(PackId), BeforeFinalize(PackId), AfterRepoint(PackId),
    /// BeforeManifestSweep, ... The suite injects concurrent commits here.
    pub at: Option<Box<dyn Fn(GcPoint) + Send + Sync>>,
    /// Deliberately-broken modes for negative proofs (04 §5). NEVER
    /// compiled in release (feature-gated; assert in build.rs or CI grep).
    pub sabotage: Option<Sabotage>,
}
pub enum Sabotage { DropPinsFromRoots, SkipLateRootsDrain, UnlinkBeforeRepoint, SkipIndexRemoveOfDead }
```

Default (no feature): `GcHooks` is a zero-sized no-op struct so
`run_gc_cycle`'s signature stays uniform.

## 9. Unit tests (in-crate, beyond the property suite)

- Fence excludes packs >= fence_pack; rotate-first makes all prior data
  sweepable.
- Straggler loop: hook injects a commit at `BeforeFinalize` referencing a
  doomed page → page survives, manifest survives.
- Race B replay: hook injects register_live_ref+create_node-shaped access at
  `BeforeManifestSweep` → manifest survives.
- Crashed-GC recovery: kill after publish (step 4) → reopen → both copies
  readable, next cycle reclaims (also covered by 05 at process level).
- `end_gc_epoch` runs on error paths (guard test).
