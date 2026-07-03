# WI1 — Storage Surfaces

New low-level APIs GC needs. All are small additions to existing types; no
on-disk format changes. Each item lists the anchor it extends.

## 1. `snapstore-pagestore` — `ShardedIndex` (src/index.rs)

`insert`/`insert_batch` are first-writer-wins (`entry().or_insert`,
index.rs:53/60) — correct for ingest dedup, unusable for GC. Add:

```rust
/// Repoint `hash` to `new_loc` iff its current location's pack matches
/// `expected_pack`. Returns true if repointed. Used by compaction: only
/// entries still pointing at the pack being compacted are moved (a
/// concurrent re-ingest may already have written a fresh copy elsewhere —
/// content-addressed, either copy is valid, keep the newer).
pub fn repoint_if_in_pack(&self, hash: &PageHash, expected_pack: PackId, new_loc: PageLoc) -> bool;

/// Remove `hash` iff its current location's pack matches `expected_pack`.
/// Returns true if removed. Used by sweep for dropped (garbage) records.
pub fn remove_if_in_pack(&self, hash: &PageHash, expected_pack: PackId) -> bool;

/// All (hash, loc) entries currently pointing into `pack`.
/// O(index) full-shard scan — same cost class as `write_sidecar`
/// (index.rs:131), acceptable: GC-only, off the hot path.
pub fn entries_for_pack(&self, pack: PackId) -> Vec<(PageHash, PageLoc)>;
```

Both mutators take the shard **write** lock (existing sharding: first hash
byte, index.rs:202). The `expected_pack` guard makes compaction idempotent
under crash-retry and safe against concurrent ingest of the same hash.

Unit tests: repoint respects guard; remove respects guard; concurrent
insert-vs-repoint of same hash never loses the entry.

## 2. `snapstore-pagestore` — `PageStore` (src/ingest.rs)

`discover_packs`/`pack_path`/`sidecar_path` are private (ingest.rs:696/686/691).
Add public GC surfaces:

```rust
/// Sealed pack ids (everything discovered on disk except the active pack),
/// ascending. Snapshot at call time.
pub fn sealed_pack_ids(&self) -> Result<Vec<PackId>, StoreError>;

/// Current active pack id (brief `active` lock).
pub fn active_pack_id(&self) -> PackId;

/// Seal the active pack (flush + seal_no_sync + sidecar, exactly the
/// rotation path at ingest.rs:297-326) and open a fresh one. Returns the
/// new active pack id. Used by aggressive GC so the fence covers all data
/// written so far. Must be a no-op-ish cheap call when the active pack is
/// empty (skip rotation if zero records — avoids pack-id churn).
pub fn rotate_active(&self) -> Result<PackId, StoreError>;

/// All (offset, hash) records in a sealed pack, in file order.
/// Thin wrapper over PackReader::open + scan (pack.rs:325/556).
pub fn scan_pack(&self, pack: PackId) -> Result<Vec<(u64, PageHash)>, StoreError>;

/// Read one record's payload from a sealed pack at a known offset,
/// verifying hash (wrapper over read_cache.get_or_open +
/// PackReader::read_at_from_file, pack.rs:398). Compaction's copy read.
pub fn read_record(&self, pack: PackId, offset: u64, expected: &PageHash) -> Result<bytes::Bytes, StoreError>;

/// Allocate a fresh pack id for a GC compaction destination and return a
/// writer for it. Id allocation takes the `active` lock briefly and bumps
/// the same monotonic counter rotation uses (so ids never collide with the
/// active pack or future rotations). The pack is NOT the active ingest
/// pack; the caller owns the writer exclusively.
pub fn create_gc_pack(&self) -> Result<GcPackWriter, StoreError>;

/// Publish a sealed GC pack: fsync it, write its sidecar, register its
/// records... (see 02 for the exact repoint sequence — publication itself
/// only makes the pack durable + discoverable; index repoint is separate).
/// Implemented as methods on GcPackWriter: append(hash, payload) -> offset,
/// seal_and_publish(self) -> (PackId, Vec<(PageHash, u64)>).
pub struct GcPackWriter { /* wraps PackWriter + PageStore backrefs */ }

/// Unlink a sealed pack file and its sidecar, in the R2-mandated order:
/// caller must have already repointed/removed every index entry for it and
/// called invalidate_pack_handle. Steps: unlink .spk, unlink .idx, fsync
/// pages dir. Failpoints: `gc-pack-unlink` before the .spk unlink.
pub fn delete_pack(&self, pack: PackId) -> Result<(), StoreError>;
```

Recovery interaction (must-check): `PageStore::open` treats the
highest-numbered **unsealed** pack as active (ingest.rs:165-190). A GC pack
that crashed before sealing is unsealed and higher-numbered than the ingest
active pack — recovery would adopt it as active. That is **safe** (it
contains valid records; its copies are duplicates; index load order:
sidecars of sealed packs + rebuild of the adopted pack) but assert in a
crash-harness invariant that recovery converges (05). Duplicate-copy
resolution: `insert_batch` first-writer-wins keeps whichever loaded first —
both copies durable, either fine (R4: "a crashed GC leaves only extra
copies").

`IngestOutcome`/dedup behavior is untouched.

Unit tests: rotate_active skips empty pack; create_gc_pack id > active id;
delete_pack + reopen never resurrects entries (sidecar gone); scan_pack
matches ingest outcomes.

## 3. `snapstore-store` — `SnapshotStore` (src/lib.rs)

```rust
/// Delete a stored manifest: unlink manifests/<shard>/<hex>.spm, fsync the
/// shard dir, remove the flatten-cache entry, decrement manifests_total and
/// logical_page_bytes (read guest_ram_bytes from the header first — reuse
/// read_guest_ram_bytes, lib.rs:870). Failpoint `gc-manifest-unlink` before
/// the unlink. Returns Ok(false) if already absent (idempotent).
pub fn delete_manifest(&self, r: &SnapshotRef) -> Result<bool, StoreError>;
```

Requires `FlattenCache::remove(&mut self, r: &SnapshotRef)` (struct at
lib.rs:230-260, currently get/insert only).

Doc fix: `has_manifest`'s "permanent guarantee — no TOCTOU" comment
(lib.rs:550-553) becomes false once GC sweeps manifests. Rewrite the comment:
the guarantee holds only for refs protected as GC roots; the server's
create_node path relies on the late-roots protocol (02 §4) for correctness,
not on this comment.

The gate/epoch/late-roots API on SnapshotStore is specified in 02 §2 (it is
engine, not surface).

## 4. `snapstore-meta` — `MetaDb` (src/lib.rs, actor.rs, pool.rs)

New read-pool queries (no actor round-trip):

```rust
/// All distinct snapshot_refs that are GC roots: every nodes.snapshot_ref
/// (ALL statuses — PRUNED-but-unreaped rows are conservatively live) plus
/// every pins.snapshot_ref. Single read transaction = point-in-time root
/// set (WAL snapshot isolation). Called under the fence (02 §3).
pub fn gc_root_refs(&self) -> Result<Vec<SnapshotRef>, MetaError>;

/// Tombstoned subtree roots with created_at <= horizon (logical counter).
pub fn list_tombstones(&self, horizon: u64) -> Result<Vec<TombstoneRow>, MetaError>;
```

New actor commands (writer, one txn each — mirror `prune_subtree`'s
recursive-CTE pattern at actor.rs:808-856):

```rust
/// Reap one tombstoned subtree: recursive CTE from (experiment_id, node_id)
/// over PRUNED rows, DELETE those node rows, DELETE input_logs no longer
/// referenced by any node, DELETE the tombstone row. One transaction.
/// Returns rows deleted. Skips (returns 0) if the root row is missing
/// (already reaped — idempotent under crash-retry).
pub fn reap_tombstone(&self, experiment_id: &ExperimentId, node_id: NodeId) -> Result<u64, MetaError>;

/// Persisted GC cycle state (see migration below).
pub fn gc_state(&self) -> Result<GcStateRow, MetaError>;
pub fn set_gc_state(&self, s: GcStateRow) -> Result<(), MetaError>;
```

CTE caution: the reap CTE must descend only through rows that are PRUNED
**and** belong to the tombstoned subtree — a non-PRUNED descendant would
indicate a prune/reap bug; abort that subtree's txn with an error rather
than deleting live rows (defense in depth for R1).

`GcStateRow { cycles_total: u64, last_fence_counter: u64, last_finished_at: u64, last_freed_bytes: u64 }`.

Migration `002_gc_state.sql`: `CREATE TABLE gc_state (id INTEGER PRIMARY KEY
CHECK (id = 1), cycles_total INTEGER NOT NULL, last_fence_counter INTEGER
NOT NULL, last_finished_at INTEGER NOT NULL, last_freed_bytes INTEGER NOT
NULL); INSERT INTO gc_state VALUES (1, 0, 0, 0, 0);`
**DDL + seed row in ONE transaction** — the phase-2 crash harness found
torn meta-DB init when they were split (bd memory
`phase-2-crash-harness-found-two-real-recovery`). Follow the existing
migration runner pattern (schema.rs:74-83, single BEGIN…COMMIT per
migration); bump expected schema_version handling accordingly
(schema.rs:60-64 refuses future versions — verify the version constant
lands with the migration).

Unit tests: reap idempotency; reap refuses non-PRUNED descendant; root refs
include pins and all node statuses; migration applies on an existing
phase-2 DB (open old fixture → migrated → gc_state readable).
