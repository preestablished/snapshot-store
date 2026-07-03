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

Protocol (D5), three parts:

1. **Widen the gate read lock** in `put_snapshot` to cover steps 3–6:
   acquire `gc_commit_gate.read()` **before** the `contains_batch` presence
   check (currently step 5, after it — lib.rs:389). The group-commit
   barrier (step 4) stays inside the read lock. Cost: the fence write
   acquisition now waits for in-flight commits including their fdatasync —
   acceptable, it happens once per cycle plus once per finalized pack.
2. **Late-roots registration.** `SnapshotStore` gains an epoch-scoped set:

   ```rust
   /// Some(..) while a GC cycle is running. note_live_ref() appends; the
   /// sweep drains under the gate write lock.
   gc_epoch: Mutex<Option<GcEpochState>>,   // GcEpochState { late_roots: Vec<SnapshotRef>, fence_pack: PackId }

   pub fn note_live_ref(&self, r: &SnapshotRef);  // no-op when no epoch
   ```

   Callers: `put_snapshot` itself (after the manifest is durable, still
   under the read lock); the **server** before `create_node`'s
   `has_manifest` validation (service.rs:502) and before `pin`
   (service.rs — Pin handler). Registering before validation means: if the
   manifest is still there, it is now protected; if already swept, the
   validation fails NOT_FOUND — correct either way.
3. **Finalize-under-write with straggler drain** (per pack, and once for
   the manifest sweep) — §5 below.

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

R4: the `gc_epoch` Mutex<Option<..>> doubles as the "one GC at a time"
latch; `TriggerGc` returns `already_running` when occupied. No
`mark-<epoch>.state` resume file — a crashed GC is simply discarded
(ARCHITECTURE R4 allows this; extra copies reclaimed next cycle).

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
2. `liveness = live_bytes / total_record_bytes` (record = 4133 bytes,
   pack.rs:9-19; total from scan or sidecar count). If
   `liveness >= compact_threshold` → leave the pack alone (dead bytes wait
   for a later cycle), continue.
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
       mark-walk stragglers (may add hashes to `mark`);
       for any newly-live record of p not yet copied: copy+publish it
           (append to a fresh gc pack or reopen — simplest: a small
           follow-up gc pack per straggler round);
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

Delete every stored manifest not in `visited` (mark's memoized set),
using the same finalize discipline: take gate write, drain/walk
stragglers (their chains join `visited`), then compute
`doomed = list_manifest_refs() - visited`, **release the write lock**, and
`delete_manifest` each doomed ref (unlink order is crash-safe: a manifest
deleted twice is idempotent, one deleted-then-referenced is impossible
because anything referenced got into `visited`/late-roots before its
referencing row committed — Race B protocol). `list_manifest_refs`
(lib.rs:573) walks the shard dirs; fine off the hot path.

Subtle: `visited` must also include every ref in the late-roots drains
*and* every manifest committed after the fence. Post-fence manifests all
passed through `note_live_ref` in put_snapshot (§1.2), so they are in the
drains by construction. Assert this in a debug check: after the final
drain, re-list manifests and verify none is younger than the fence yet
doomed (belt-and-braces; cheap).

## 7. Failpoints (all behind the existing `failpoints` feature)

| Name | Site |
|---|---|
| `gc-compact-copy` | inside the copy loop, per record |
| `gc-compact-seal` | between gc-pack fsync and sidecar write |
| `gc-index-repoint` | mid-repoint loop (some entries moved, some not) |
| `gc-pack-unlink` | between .spk unlink and .idx unlink |
| `gc-manifest-unlink` | before a manifest unlink |
| `gc-reap-txn` | inside reap_tombstone, before COMMIT |

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
- Race B replay: hook injects note_live_ref+create_node-shaped access at
  `BeforeManifestSweep` → manifest survives.
- Crashed-GC recovery: kill after publish (step 4) → reopen → both copies
  readable, next cycle reclaims (also covered by 05 at process level).
- `end_gc_epoch` runs on error paths (guard test).
