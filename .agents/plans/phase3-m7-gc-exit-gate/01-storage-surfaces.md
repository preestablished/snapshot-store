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
/// writer for it. REQUIRES A REFACTOR, not a pure addition: no shared id
/// counter exists today — rotation computes `PackId(old.0 + 1)` inline
/// (ingest.rs:310) and open() uses `pack_ids.last()+1`. Introduce
/// `next_pack_id` in ActiveState, initialized by open() to
/// max(discovered)+1, and rewrite the rotation path (ingest.rs:297-326)
/// to consume it. Otherwise a GC pack id G > active id A collides when
/// ingest rotation eventually reaches G (create-on-existing-file clobber).
/// The pack is NOT the active ingest pack; the caller owns the writer.
pub fn create_gc_pack(&self) -> Result<GcPackWriter, StoreError>;

/// Implemented as methods on GcPackWriter:
///   append(hash, payload) -> offset
///   seal_and_publish(self) -> (PackId, Vec<(PageHash, u64)>)
/// seal_and_publish fsyncs the pack, then writes the sidecar FROM THE
/// WRITER'S OWN RECORD LIST — NEVER via ShardedIndex::write_sidecar
/// (index.rs:131 builds sidecars by scanning the index for entries in
/// that pack; at publish time the index still points at the OLD pack, so
/// it would emit a CRC-valid EMPTY sidecar. Crash after old-pack unlink →
/// reopen loads the empty sidecar "successfully", rebuild fallback never
/// fires, every compacted page unreachable — silent R1/R2 violation).
/// Add a crash-harness assertion: sealed GC pack sidecar entry count ==
/// pack record count (05 §3).
pub struct GcPackWriter { /* wraps PackWriter + PageStore backrefs */ }

/// Unlink a sealed pack and its sidecar. Caller must have already
/// repointed/removed every index entry for it and called
/// invalidate_pack_handle. Order: unlink `.idx` FIRST, then `.spk`, then
/// fsync pages dir. Rationale: discover_packs keys on .spk files
/// (ingest.rs:696-718), so crashing between the unlinks must not leave an
/// orphan .idx whose pack id is no longer discovered (it would leak
/// forever); .idx-first instead leaves a sealed pack with a missing
/// sidecar, which open() rebuilds cleanly (ingest.rs:197-202).
/// Failpoint `gc-pack-unlink` before the .idx unlink.
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
(lib.rs:550-553) becomes false once GC sweeps manifests — and the same
claim is duplicated at the create_node call site (service.rs:1015-1018);
rewrite both. The guarantee holds only for refs protected via
`register_live_ref` under the commit gate (02 §1.2).

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
/// TombstoneRow is NEW (only PinRow/StatsRow exist today, lib.rs:38):
/// { experiment_id: ExperimentId, node_id: NodeId, created_at: u64 } —
/// mirrors the tombstones table (001_initial.sql:57-61).
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
`phase-2-crash-harness-found-two-real-recovery`).

**The existing runner CANNOT apply a second migration — restructure it.**
`run_migration` (schema.rs:40-84) short-circuits `Some(_) => return Ok(())`
for any existing DB (schema.rs:66) and only ever runs `MIGRATION_001` on
first open; "following the pattern" would silently skip 002 on every
phase-2 store. Rewrite into a version-stepping loop: read
`schema_version`; while `v < SUPPORTED_VERSION`, apply migration v+1
(DDL + seed + `UPDATE meta SET schema_version = v+1` + `_migrations` row,
all in ONE transaction per step); bump `SUPPORTED_VERSION` to 2 (schema.rs:7);
keep the future-version refusal (schema.rs:60-64). First-open path applies
001 then steps to current the same way (or ships 001 already at v2 — either
is fine as long as both paths converge; test both).

Failpoints plumbing: `snapstore-meta` has NO `failpoints` feature, no
`fail` dep, no macro today (Cargo.toml: types/rusqlite/thiserror/crossbeam/
blake3/uuid only) — the `gc-reap-txn` failpoint (02 §7) needs all three
added, mirroring snapstore-store's shim (store lib.rs:36-41), plus
`"snapstore-meta/failpoints"` added to snapstore-crash's `failpoints`
feature list and snapstore-meta added to the ci.yaml:35/37 failpoints
clippy/test lines.

Unit tests: reap idempotency; reap refuses non-PRUNED descendant; root refs
include pins and all node statuses; migration steps an existing v1
phase-2 DB to v2 (open old fixture → migrated → gc_state readable) AND
first-open lands at v2 directly.
