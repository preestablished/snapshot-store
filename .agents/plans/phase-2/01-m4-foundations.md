# M4 foundations — spec alignment (manifest v2, meta v2, types)

Everything the gRPC surface stands on, converged to the normative docs
(`~/.agents/projects/determinism/docs/snapshot-store/` API.md §2–§3,
ARCHITECTURE.md §5). No proto/tonic dependency anywhere in this document —
all of it proceeds while the control-plane proto request is in flight.

## Work item 0 — types extension

`crates/snapstore-types/src/lib.rs` gains (alongside the existing `PageHash`,
`PackId`, `PageLoc`, `SnapshotRef`, `PAGE_SIZE`):

```rust
pub struct LogId(pub [u8; 32]);          // BLAKE3 of input-log container sans footer
pub struct NodeId(pub u64);              // caller-assigned; unique per experiment; root = 0
pub struct ExperimentId(pub String);     // caller-chosen, validated 1..=128 UTF-8 bytes
#[repr(u8)]
pub enum NodeStatus { Frontier = 0, Expanded = 1, Pruned = 2, Goal = 3 }
```

Same derive set as the existing newtypes; `ExperimentId` validates length on
construction; `NodeStatus` has fallible `from_u8` (DB/proto decode path). Keep
the crate proto-free (the optional `proto` feature stays as-is; the stale
`NodeMeta` re-export is removed when the proto request lands — tracked in 02).

**AC:** unit tests for bounds/round-trips; `cargo clippy -D warnings` clean.

## Work item 1 — manifest v2: the `.spm` container

Rewrite `crates/snapstore-manifest` to API.md §2 byte-precisely. This replaces
the phase-1 model (`icount`/`virtual_ns`/`MemoryMap` regions/`devices`) — those
concepts move into the opaque device blob and the node row; the manifest is
pages + device blob only.

### Model

```rust
pub struct Manifest {
    pub version: u16,                      // = 1
    pub delta: bool,                       // header flags bit0
    pub parent: Option<SnapshotRef>,       // Some iff delta
    pub guest_ram_bytes: u64,              // multiple of 4096
    pub entries: Vec<ManifestEntry>,       // sorted ascending by page_index, unique
    pub device_blob: DeviceBlob,           // { format: u32, zstd: bool, bytes: Vec<u8>, raw_len: u64 }
}
pub struct ManifestEntry { pub page_index: u64, pub page_hash: PageHash }

impl Manifest {
    pub fn encode(&self) -> Vec<u8>;                          // canonical bytes + footer
    pub fn decode(buf: &[u8]) -> Result<Self, ManifestError>; // full strict validation
    pub fn snapshot_ref(buf: &[u8]) -> SnapshotRef;           // blake3(buf[..len-32])
}
pub fn flatten(chain: &[&Manifest]) -> Result<Vec<ManifestEntry>, FlattenError>; // child-first
```

### Encoding (canonical, one valid encoding per snapshot)

Exactly API.md §2: 96-byte header (`SPSMAN01`, version u16=1, flags u16
{bit0 DELTA, bit1 DEV_ZSTD, rest MUST be 0}, header_len u32=96,
parent_manifest_hash [32] (all-zero iff !DELTA), guest_ram_bytes u64,
page_size u64=4096, entry_count u64, device_blob_len u64,
device_blob_raw_len u64, device_blob_format u32, reserved u32=0); entry table
(entry_count × 40 B); device blob; 32-byte BLAKE3 footer over everything
before it. All integers LE.

### Validation in `decode` (each failure a distinct `ManifestError` variant)

- magic / version / unknown flag bits / header_len ≠ 96 / reserved ≠ 0
- `page_size != 4096` rejected (v1 readers reject ≠ 4096 — API.md §2);
  `guest_ram_bytes` not a multiple of 4096 rejected
- entries sorted strictly ascending by `page_index`, no duplicates
- FULL (`!delta`): `entry_count == guest_ram_bytes/4096` and indices run
  `0..N-1` contiguously; `parent_manifest_hash` all-zero
- DELTA: parent hash non-zero; entries within `0..guest_ram_bytes/4096`
- footer equals recomputed BLAKE3 of canonical bytes; no trailing bytes
- DEV_ZSTD: blob decompresses to exactly `device_blob_raw_len` (result
  discarded — storage keeps the container byte-identical)

Parent-exists / pages-present checks are **store-level** (WI3), not codec-level
— the codec stays pure (`no I/O beyond &[u8]`, fuzzable).

### Flatten

Pure merge over sorted entry arrays, chain passed child-first; child entries
shadow parent entries with equal `page_index`; result must cover every index
`0..guest_ram_bytes/4096` (gap ⇒ `FlattenError::Coverage` — corruption signal,
P0 at call sites). Also: a delta-only merge variant for ResolvePages Mode B
(entries the child chain adds/changes relative to a given ancestor).

### Tests

- proptest round-trip: ∀ generated manifests `decode(encode(m)) == m`, ref stable
- canonicality: shuffled-entry inputs are rejected (sort is *validated*, not
  silently fixed — one valid encoding means encode-side sorting happens in the
  builder, decode-side order violations are errors)
- flatten correctness vs a naive reference implementation (proptest, chains ≤ 64,
  including shadowing and coverage-gap cases)
- strictness matrix: every validation rule has a negative test
- **new golden vector** (fixed manifest → exact bytes → exact ref), replacing
  the phase-1 vector in the same commit, with a commit message explaining the
  deliberate format change
- `cargo fuzz` target on `Manifest::decode` (carried from the upstream M2 AC;
  wire into nightly CI for 10 min, not PR CI)

**AC:** all of the above green; `PROPTEST_CASES=4096` deep run green.
**BM (criterion):** flatten of a 64-deep chain of 2k-entry deltas < 2 ms warm.

## Work item 2 — input-log container codec

Small module (in `snapstore-manifest` or a `container` module in
`snapstore-types` — implementer's choice, it's ~100 lines): the API.md §3
wrapper (`SILG` magic, container_version u16=1, `flags` u16=0 (reserved,
reject nonzero), `inner_format_version` u32, `reserved` u32=0 (reject
nonzero), `payload_len` u64, opaque payload, BLAKE3 footer = `log_id`).
Validate magic/version/flags/reserved/lengths/footer; expose
`inner_version()` and `log_id()`. The store never parses the payload.
Proptest round-trip + strictness tests.

## Work item 3 — store façade rework (`snapstore-store`)

Rework `SnapshotStore` (currently `commit(&GuestImage, Option<&MetaDb>)` /
`resolve`) into the server-side snapshot path:

- `put_snapshot(container: &[u8]) -> Result<SnapshotRef, PutError>` —
  full API.md §2 validation: codec decode (WI1); parent resolves to a stored
  manifest (`PutError::UnknownParent` ⇒ FAILED_PRECONDITION) and has identical
  `guest_ram_bytes` (`PutError::ParentRamMismatch` ⇒ INVALID_ARGUMENT — API.md
  §2 notes only the *missing* case as FAILED_PRECONDITION; everything else
  defaults to INVALID_ARGUMENT); every `page_hash` present **and durable**
  (`PutError::MissingPages(Vec<PageHash>)` listing exactly the gaps ⇒
  FAILED_PRECONDITION detail). **Durability barrier = group commit**, not a
  naive per-call `PageStore::sync()`: concurrent `put_snapshot`s coalesce on a
  sequence-numbered flush (a caller needing durability up to ingest-seq N
  waits on the next barrier; one fdatasync pass serves every waiter at ≤ that
  seq). Rationale: `sync()` takes the active-pack lock and fdatasyncs every
  dirty pack — 16 concurrent committers (gate S4 p99 < 40 ms) through
  per-call syncs would serialize into an fsync storm, re-creating the problem
  phase 1 removed for a 2.25× gain (commit 0d8ef62). The per-entry
  `synced`-bit from ARCHITECTURE.md §3 step 7 stays deferred; group commit
  gives the same contract. Also: take a read lock on a **`gc_commit_gate`
  `RwLock` stub** around the commit (no-op until M7's mark fence / M9's
  backup consistency point take the write side — ARCHITECTURE.md §4.5 R3;
  one line now vs hot-path surgery under M7 pressure).
  Then write the container byte-identical to
  `manifests/<first-byte-hex>/<hex>.spm` via `tmp/` + fsync + rename + dir
  fsync (keep the existing atomic-write discipline).
- `get_snapshot(ref) -> stored container bytes` (byte-identical; verify footer
  on read, reject corrupt).
- `resolve_pages(ref, baseline: Option<SnapshotRef>, hashes_only: bool)` —
  Mode A (flatten full chain) / Mode B (delta vs ancestor baseline; error if
  baseline is not in the parent chain), streaming-friendly iterator of
  `(page_index, PageHash, Option<Bytes>)` ascending by index, payloads read
  from the pagestore.
- **Flatten LRU cache** keyed by `SnapshotRef` (`flatten_cache_entries`,
  default 1024) — sibling restores hit this constantly (ARCHITECTURE.md §7.3).
- `has_pages(&[PageHash]) -> Vec<bool>`.
- The phase-1 `GuestImage` commit path moves to a test/bench helper that
  *builds containers* and calls `put_snapshot` (the synthetic e2e driver and
  benches need it; production clients build containers worker-side).
- **Remove the `commit(…, Some(&MetaDb))` auto-register coupling** — node rows
  are created only via CreateNode (orchestrator), never by the snapshot path
  (INTEGRATION.md §2.1). The phase-1 M2↔M3 integration test is replaced by the
  e2e test in 02.

**AC:** commit→resolve byte-identity on `.spm` containers; multi-epoch
delta-chain test (FULL root + 64 deltas: resolve Mode A equals synthetic guest
state; Mode B against each ancestor equals the recomputed diff); missing-page
commit returns exactly the gap list; unknown/mismatched parent rejected;
corrupt stored manifest rejected on read; reopen test still green.
**BM:** PutSnapshot p50 < 3 ms with pages already present (already-paged path,
2k-entry delta).

## Work item 4 — meta v2 (`snapstore-meta` rewrite)

Replace the phase-1 `snapshots` schema/API with ARCHITECTURE.md §5.3–§5.4
verbatim. This is the largest single item in the plan.

### Schema (schema_version = 1, new `meta/tree.db`)

Tables exactly per §5.3: `meta` (schema_version, store_uuid, logical_counter),
`nodes` (composite PK `(experiment_id, node_id)`, parent FK, depth, refs,
status, scores, counters, `attrs` blob), `kv_metadata` (generation CAS),
`input_logs` (inline content ≤ `input_log_max_bytes`), `pins`, `tombstones` —
with the five §5.3 indexes. Migrations table discipline carried over from the
phase-1 crate. The phase-1 `snapshots` table is **dropped, not migrated**
(synthetic test data only; no production stores exist). `node_id` stored as
i64 bit-cast of u64; round-trip property-tested; never compared by SQL
ordering.

### Concurrency model (§5.2)

- **Writer actor**: one dedicated blocking thread owning the sole write
  connection; commands arrive on a bounded `crossbeam-channel` carrying oneshot
  reply senders; the actor drains up to `write_batch_max` (256) commands into
  one `BEGIN IMMEDIATE … COMMIT`. Startup re-derives the counter as
  `max(persisted, max(created_at), max(updated_at)) + 1`.
- **Logical-counter granularity (decision, made here deliberately):** the
  counter advances **per command** within the batch txn (flushed once per
  txn), not per txn. ARCHITECTURE.md §5.3's "per txn" wording would give up to
  256 CreateNodes an identical `created_at`, and an exclusive
  `QueryNodes(created_after=…, limit=N)` cursor **silently skips rows** when a
  page boundary splits a same-counter group — breaking the orchestrator's
  warm-start contract (INTEGRATION.md §2.3, "never misses or double-counts").
  Per-command counters keep the cursor sound with no wire change; a whole
  `UpdateNodes` batch is one command and still gets one `updated_at` (matches
  API.md §1.4's response). File the upstream doc issue amending §5.3. The
  cursor test below must cover a batch split across page boundaries.
- **Read pool**: 4 connections, `PRAGMA query_only=ON`, used via
  `spawn_blocking` from the (future) server; expose a sync facade so [04]'s
  harness and tests can drive it without tokio.
- Pragmas everywhere: `journal_mode=WAL`, `synchronous=FULL` (writer),
  `foreign_keys=ON`, `wal_autocheckpoint=4000`, `mmap_size=268435456`,
  `busy_timeout=5000`.

### Operations (the full RPC-backing set)

- `create_node` — validates parent exists & not PRUNED, root rules
  (node_id 0 ⇔ parent unset; root uniqueness), `input_log_id` exists (or
  inline container stored in the same txn), depth = parent.depth + 1.
  **Idempotency** per API.md §1.4: PK conflict ⇒ re-read, compare immutable
  fields (parent, snapshot_ref, input_log_id) ⇒ return stored row or
  `AlreadyExists`. Note: the API.md §1.4 rule that `snapshot_ref` "must
  resolve to a stored manifest" (⇒ `NOT_FOUND`, a P0 commit-ordering signal
  per INTEGRATION.md §6) **cannot live in this crate** — meta has no manifest
  visibility. It is owned by the server layer (02 WI2); don't silently drop it.
- `update_nodes` — bulk partial updates, one txn, all-or-nothing; any unknown
  id rolls back the whole batch and reports the missing ids; deltas add
  (`visit_count_delta`), `touch_visited` stamps the txn counter; returns the
  txn's `updated_at` counter.
- `get_node`, `get_children`, `get_path` (recursive CTE, root-first, optional
  inline log containers), `query_nodes` (conjunctive filters, three orderings,
  `created_after`/`updated_after` exclusive cursors, limit) — §5.4 SQL.
- `put_input_log` — container validated (WI2), `INSERT OR IGNORE` by `log_id`,
  size cap from config; `get_input_log` returns byte-identical content.
- KV: `put_metadata` (CAS: unset / 0=create-only / N=generation match —
  `UPDATE … WHERE key=? AND generation=?`, `changes()==0` ⇒ CAS failure
  carrying the current generation, 0 = absent), `get_metadata`,
  `delete_metadata` (optional CAS). Key 1..=512 bytes; value ≤ 16 MiB.
- `pin` / `unpin`; `prune_subtree` (one txn: recursive-CTE subtree collect,
  `status=PRUNED` ∀, tombstone row for the subtree root; root protected unless
  `allow_root`); `stats` (per-experiment and global, §5.4 + node/page counts
  joined in by the server layer).

### Tests

- CreateNode idempotency property: replaying any prefix of a synthetic
  experiment's CreateNode stream, duplicates included, any interleaving ⇒
  byte-identical tree; key reuse with different content ⇒ AlreadyExists, zero
  rows changed.
- Multi-experiment isolation: two interleaved synthetic experiments sharing
  page content never observe each other's nodes via any tree query;
  per-experiment stats match per-driver bookkeeping.
- KV CAS contention: concurrent writers hammering one key ⇒ exactly one winner
  per generation; create-only and delete-CAS paths; value-cap rejection.
- UpdateNodes atomicity: one bad id ⇒ zero rows changed.
- QueryNodes cursor: `created_after` paging under concurrent writes — no gaps,
  no duplicates (interleaving test through the actor).
- GetPath on a deep chain; prune-subtree leaves tombstone + PRUNED rows.
- Property test: lineage invariants on random trees (carried from phase 1,
  re-targeted at the new schema).

**AC:** all tests green; 1M-node synthetic tree (branching ~8): GetPath(depth
5k) < 40 ms p99; sustained ≥ 5k node-mutations/s through the actor.
**BM (criterion):** CreateNode + inline 16 KiB log p50 < 1.5 ms;
UpdateNodes(256) p50 < 3 ms; PutMetadata (64 KiB) p50 < 2 ms.
(Kill -9 batch-atomicity ×200 belongs to the M6 harness — see 04 WI4.)

## Work item 5 — failpoint hooks (enabler for 04)

Add the `fail` crate behind a `failpoints` feature (off by default, never in
release builds) and place named failpoints at every fsync/rename boundary in
`snapstore-pagestore` and `snapstore-store` while the commit-path code is
being touched in WI3 (cheaper now than retrofitting):
`pack-append`, `pack-fdatasync`, `sidecar-write`, `sidecar-fsync`,
`pack-rotate-seal`, `manifest-tmp-write`, `manifest-fsync`, `manifest-rename`,
`manifest-dirsync`. The meta-txn kill points need no code hook (the harness
kills around SQLite commits by timing/statement count). Full matrix and
semantics in 04.

**AC:** `cargo build --workspace` unaffected without the feature; with the
feature, a smoke test triggers one failpoint and observes the configured panic.

## Work item 6 — pagestore read path (gates ResolvePages + GET_BATCH)

Review finding, missing from the original plan: the as-built read path cannot
meet any read gate. `PageStore::get`
(`crates/snapstore-pagestore/src/ingest.rs:295`) opens the pack file **per
page read**, and `PackReader::open` (`pack.rs:309`) validates by scanning the
entire pack body (`count_records_in_file`) on every open — worst case a ~1 GiB
scan per 4 KiB page. GET_BATCH at 2.5 GB/s warm is ~610k pages/s, and M4's
`ResolvePages` Mode A streams payloads through the same call.

- Cached pack readers: LRU of open `File` handles per ARCHITECTURE.md §6
  (cap 256); sealed packs are immutable ⇒ no revalidation on open (their
  sidecars were verified at startup) — validate-on-open survives only for the
  unsealed-pack recovery path.
- Batched reads: `pread`/`preadv` straight from the cached handle at `PageLoc`
  offsets; no per-read seek-scan. Multi-page lookups (GET_BATCH, ResolvePages)
  sort by `(pack, offset)` to keep reads sequential per pack.
- Eviction hook left for M7: GC compaction must be able to invalidate a pack's
  cached handle before unlink (R2's repoint-then-unlink makes one retry
  sufficient — keep the retry-on-ENOENT probe in the read path now).

**AC:** read correctness suite unchanged and green; criterion bench:
single-thread warm random `get` ≥ 500k pages/s; 8-thread ≥ 2.5 GB/s warm from
page cache (pre-gate for S4's GET_BATCH number — measured at the library
layer, before channel overhead).

## Dependencies and ordering

```
WI0 types ──► WI1 manifest v2 ──► WI3 store façade ──► (02)
         └──► WI2 log container ──► WI4 meta v2 ─────► (02)
WI6 read path ───────────────────────────────────────► (02 ResolvePages, 03 GET_BATCH)
WI5 failpoints rides with WI3 (same files)
```

WI1+WI3, WI2+WI4, and WI6 are three parallel streams (different crates/
modules, different beads issues). Nothing here touches tokio/tonic.
