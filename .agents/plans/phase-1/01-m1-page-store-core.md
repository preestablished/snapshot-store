# M1 — Page store core

**Crates:** `snapstore-pagestore` (new), `snapstore-testgen` (new), `snapstore-types` (extend)
**Depends on:** nothing (first milestone)
**Unblocks:** M2, M3, and Phase 1 gate G1 (≥1.5 GB/s fast-path ingest)

## Scope

An on-disk, content-addressed page store: guest memory pages go in, page
references come out, and the same pages can be read back. Append-only 1 GiB
pack files, a sharded in-memory index with per-pack persisted segments, and a
batched ingest pipeline. All testing against `snapstore-testgen` synthetic
data — no hypervisor, no real guest.

Out of scope for M1: manifests, snapshot refs, commit/resolve (M2), any
network/proto surface, compression of the fast path (see "Fast path vs cold
path"), garbage collection.

## Work item 0 — unbreak the workspace + shared types (`snapstore-types`)

**First: vendor the proto stub.** A dangling workspace path dep breaks *all*
cargo invocations, including `-p` builds of unrelated crates (verified — see
00-overview, risk 1). Create `vendor/determinism-proto`: a minimal crate with
a `snapstore` feature exposing `snapstore::v1::{PutSnapshotRequest, NodeMeta}`
matching the request spec, and point the workspace dep at it. Retired by a
one-line path flip when control-plane fulfills the request. Acceptance:
`cargo build --workspace --all-targets` green from a clean checkout.

Then extend `snapstore-types` with the core value types. **Make the
`determinism-proto` dependency optional behind a `proto` feature (default
off)** — the `NodeMeta` re-export moves behind that feature — so new crates
don't inherit the proto dep and the stub's surface stays frozen.

```rust
pub const PAGE_SIZE: usize = 4096;

/// BLAKE3 hash of a page's contents.
pub struct PageHash(pub [u8; 32]);

/// Identifies a sealed or active pack file within a store.
pub struct PackId(pub u32);

/// Where a page lives: which pack, at what byte offset.
pub struct PageLoc { pub pack: PackId, pub offset: u64 }
```

`SnapshotRef` stays as-is. Add `Serialize`-free, hand-written `to_bytes` /
`from_bytes` helpers where needed (determinism rule: no serde in encoded
formats).

Acceptance: `cargo test -p snapstore-types` with default features; crate
builds with and without `proto`.

## Work item 1 — synthetic-guest generator (`snapstore-testgen`)

A deterministic, seeded generator producing page streams that statistically
resemble guest memory. Needed by every M1/M2 test and the G1 benchmark, and
later reusable by other repos. Pure library, zero I/O.

Design:

```rust
pub struct GuestProfile {
    pub total_pages: usize,        // e.g. 1 GiB guest = 262_144 pages
    pub zero_fraction: f64,        // fully-zero pages (typical guests: 30–60%)
    pub text_fraction: f64,        // low-entropy "code/data"-like pages
    pub random_fraction: f64,      // high-entropy pages
    pub dirty_rate: f64,           // fraction of pages mutated per epoch
}

pub struct SyntheticGuest { /* seeded ChaCha/xoshiro RNG, page table */ }

impl SyntheticGuest {
    pub fn new(seed: u64, profile: GuestProfile) -> Self;
    /// Full current memory image as (page_index, &[u8; PAGE_SIZE]) iterator.
    pub fn pages(&self) -> impl Iterator<Item = (u64, &[u8])>;
    /// Advance one epoch: mutate `dirty_rate` of pages; returns dirty set.
    pub fn step_epoch(&mut self) -> Vec<u64>;
}
```

Requirements:
- Same seed + profile ⇒ byte-identical page streams (assert in a test).
- Epochs model successive snapshots of one guest: dedup ratio across epochs is
  controlled by `dirty_rate`, which is what makes M2 commit/resolve tests and
  the dedup-hit ingest path realistic.
- Provide three named profiles: `GuestProfile::idle_linux()`,
  `GuestProfile::busy_workload()`, and `GuestProfile::all_unique()` — the
  last generates pairwise-distinct page contents (zero dedup hits), which is
  what the G1 cold-ingest gate measures (see WI5; a zero-fraction profile
  would inflate or distort the cold number via dedup).

Acceptance: determinism test; distribution test (zero/text/random fractions
within tolerance); `step_epoch` dirty-set size matches rate.

## Work item 2 — pack file format (`snapstore-pagestore::pack`)

Append-only pack files capped at 1 GiB, named `pack-{:08x}.spk`.

Layout:

```
[header: magic "SPK1" | format_version u32 | pack_id u32 | created_unix u64]
[record]*
[footer (on seal): magic "SPKF" | record_count u64 | body_blake3 [32]]
```

Record:

```
[page_hash [32] | flags u8 | len u32 | payload [len]]
```

- `flags`: bit 0 = payload is raw 4 KiB page (fast path); bit 1 = reserved for
  compressed payload (cold path, later phase). M1 writes raw only.
- The footer is fixed-size and located by seeking from EOF, **never** by
  forward scan — records start with an arbitrary 32-byte hash, so a hash
  beginning with the footer magic would mis-parse. Validate footer magic +
  `record_count`; a forward scan that disagrees with the footer means
  corruption.
- `body_blake3` is maintained **incrementally during append** — `seal()` must
  not re-read 1 GiB to hash it (that read would land inside the G1 timed
  region on every rotation).
- A pack is **active** (being appended) or **sealed** (footer written, then
  immutable). Rotation happens when the next record would cross 1 GiB.
  `seal()` fdatasyncs record data *before* writing the footer — a footer's
  presence is a durability claim about everything before it.
- Crash story for M1, scanning an unsealed pack on open: for each record,
  bounds-check `len` and verify `blake3(payload) == page_hash`; truncate at
  the first failure. Length checks alone are insufficient — out-of-order
  block writeback can leave a structurally complete record with garbage
  payload, and serving wrong bytes for a hash breaks content addressing.
  Sealed packs are footer-trusted; full scrub is a later phase.

API: `PackWriter::append(hash, payload) -> u64 /*offset*/`, `PackWriter::seal()`,
`PackReader::read_at(offset) -> (PageHash, Bytes)`, `PackReader::scan()`.

Acceptance: write/seal/reopen/read round-trip; rotation at the 1 GiB
boundary; torn-write recovery tests — (a) truncate mid-record, (b) corrupt
payload bytes of the final record without changing length — reopen, verify
the bad record is dropped and everything before it survives.

## Work item 3 — sharded index (`snapstore-pagestore::index`)

Maps `PageHash → PageLoc`. Sharded 256 ways by the hash's first byte; each
shard is an independent `RwLock<HashMap<PageHash, PageLoc>>` so concurrent
ingest threads don't serialize on one lock.

Persistence: when a pack seals, write a sidecar `pack-{:08x}.idx` (sorted
`(hash, offset)` pairs + CRC). The index is always reconstructible from packs
alone (`snapstore-pagestore` exposes `rebuild_index()` for that).

Open-time rules (a crash during rotation — footer written, sidecar and/or
next pack not yet created — must not lose index entries):
- Every pack **without** a valid footer is scanned record-by-record (with the
  WI2 hash verification); all but the highest-numbered such pack are then
  sealed.
- Every sealed pack whose sidecar is **missing or invalid** (CRC failure) is
  rescanned and its sidecar regenerated — never silently skipped.
- Remaining sealed packs load from their sidecars.

Acceptance: lookup hit/miss correctness under concurrent insert (loom or
stress test); open-time rebuild equals pre-crash state; sidecar *corruption*
and sidecar *absence* both fall back to pack scan; crash-during-rotation test
(sealed pack present, sidecar deleted, no successor pack) reopens with zero
lost entries.

## Work item 4 — ingest pipeline (`snapstore-pagestore::ingest`)

The G1-critical path. Batch-oriented API on the top-level store type:

```rust
pub struct PageStore { /* dir, active pack, sealed packs, index */ }

pub struct IngestOutcome {
    pub hash: PageHash,       // M2 builds manifests from these
    pub loc: PageLoc,
    pub newly_written: bool,  // false = dedup hit; feeds M3's `new_pages`
}

impl PageStore {
    pub fn open(dir: &Path, opts: StoreOptions) -> Result<Self>;
    /// Ingest a batch. Returns one outcome per input page, in input order.
    pub fn ingest(&self, pages: &[(&[u8; PAGE_SIZE])]) -> Result<Vec<IngestOutcome>>;
    pub fn get(&self, hash: &PageHash) -> Result<Option<Bytes>>;
    /// Durability barrier — see "Durability contract" below.
    pub fn sync(&self) -> Result<()>;
}
```

(`ingest` returning hashes and dedup outcomes is load-bearing: M2's commit
builds the manifest from the returned hashes without re-hashing the image,
and M3's `new_pages` column is computed from `newly_written` counts.)

Fast-path design (what gets us to 1.5 GB/s):
1. **Hash** the batch with BLAKE3 (rayon parallel over pages; BLAKE3 alone
   does multiple GB/s/core — hashing must not be the bottleneck). Hashing
   happens **outside** any lock.
2. **Batch-local dedup** first (hash map over the batch's own hashes), then
   **probe** the sharded index. Without the batch-local pass, duplicates
   within one batch all miss the probe and all get appended — a zero-heavy
   first batch would append thousands of copies of the zero page.
3. **Append** misses to the active pack via large buffered writes: stage
   records into a 4–8 MiB write buffer, flush with single `write` calls.
   No per-page syscalls. Append scheme: a single mutex over the active-pack
   writer, held per batch-flush (hashing and dedup already happened outside
   it). If the writer lock proves to be the G1 bottleneck, escalate to
   offset-reservation — but that interacts with truncate-at-first-bad-record
   recovery (holes), so don't start there.
4. **Publish** index entries only after the bytes are written (readers never
   see a loc that can't be read).

Concurrency guarantee (stated precisely — this is *not* linearizability):
after `ingest` returns, every returned `PageLoc` reads back the input bytes,
and `get(hash)` succeeds for every input page. Under a concurrent race the
same page may be appended twice (wasted bytes, reclaimed by later-phase
compaction); the index keeps the first published location.

Durability contract (decide now, it shapes everything): `ingest` returns when
data is in the page cache. `PageStore::sync()` is the durability barrier and
must cover the rotation case: it fdatasyncs **every pack that received
appends since the last sync** (a batch can span a rotation, landing pages in
a pack that sealed before `sync` was called), and fsyncs the **store
directory** for any files created since the last sync (on ext4/xfs a freshly
created pack can vanish after a crash even if its data was fdatasync'd).
M2's `commit` calls `sync()` before returning a snapshot ref. The G1
benchmark measures the ingest fast path as defined — buffered appends without
per-batch fsync — plus a separately reported `ingest+sync` number for
honesty.

Acceptance: ingest→get round-trip for every synthetic profile; dedup returns
the same location for identical pages (across batches *and* within one
batch); the concurrency guarantee above holds under N-thread stress;
`sync()` covers a batch that spans a pack rotation (test forces rotation
mid-batch, then syncs, then verifies both packs' durability calls happened —
fault-injection via a write-layer trait or just call-recording).

## Work item 5 — G1 benchmark harness (`benches/ingest.rs`)

Criterion benchmark, `cargo bench -p snapstore-pagestore`:

- `ingest_fastpath_cold`: fresh store, `all_unique` profile (zero dedup hits
  — this exercises the true append path; a zero-fraction profile would
  inflate the number via dedup), 4 GiB per iteration, report GB/s of input
  bytes (this is the ≥1.5 GB/s gate number).
- `ingest_fastpath_realistic`: `busy_workload` profile, informational.
- `ingest_fastpath_warm`: second epoch with `dirty_rate = 0.1` — measures the
  dedup-dominated path.
- `ingest_plus_sync`: cold ingest including `sync()`, reported for context.

Methodology (pinned, or the gate isn't a measurement):
- Synthetic input is generated **outside** the timed region (pre-built page
  buffers via `iter_batched`) — testgen's high-entropy generation is itself
  expensive and must not pollute the number.
- Fresh store directory per iteration, deleted afterward; 4 GiB/iteration at
  default Criterion sample counts would otherwise want hundreds of GiB.
  Explicit `sample_size(10)` and a fixed `measurement_time`.
- Gate statistic: Criterion's reported **median** throughput.
- Dirty-page throttling: at ≥1.5 GB/s of buffered writes the kernel's
  `vm.dirty_*` limits engage within seconds and the writer degrades to
  device writeback speed. The gate is defined as a **burst** number: per-
  iteration volume (4 GiB) must sit below the reference box's dirty-bytes
  threshold (record `vm.dirty_ratio`/`dirty_bytes` alongside the result),
  with `sync()` + cleanup between iterations un-timed. Sustained-rate
  behavior is the storage hierarchy's problem, not Phase 1's.
- Store on a local NVMe path, not tmpfs — tmpfs would measure memcpy; the
  contract is page-cache writes against the real target filesystem.

Document the reference machine (the Intel box) in the bench header; gate
sign-off happens there, CI tracks regressions only.

Acceptance: bench runs green; on the reference machine
`ingest_fastpath_cold ≥ 1.5 GB/s` median.

## Dependencies (crates.io)

`blake3` (rayon feature), `rayon`, `parking_lot`, `crc32fast`, `bytes`,
`thiserror`, `tempfile` (dev), `criterion` (dev), `rand`+`rand_chacha`
(testgen). No serde, no tokio — the store is synchronous; async wrapping is
the server's problem in a later phase.

## Suggested execution order

```
WI0 (stub + types) ──► WI2 (pack) ──► WI3 (index) ──► WI4 (ingest) ──► WI5 (bench)
WI1 (testgen)  [parallel with WI2/WI3; must land before WI4's tests]
```

The vendored stub is the very first commit of the milestone — nothing builds
reliably until the workspace loads.
