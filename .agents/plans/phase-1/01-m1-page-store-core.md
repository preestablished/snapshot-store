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

## Work item 0 — shared types (`snapstore-types`)

Extend `snapstore-types` with the core value types. **Make the existing
`determinism-proto` dependency optional behind a `proto` feature (default
off)** so downstream crates don't inherit the dangling proto dep
(see 00-overview, risk 1).

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
- Provide two named profiles: `GuestProfile::idle_linux()` and
  `GuestProfile::busy_workload()` so benches and tests share vocabulary.

Acceptance: determinism test; distribution test (zero/text/random fractions
within tolerance); `step_epoch` dirty-set size matches rate.

## Work item 2 — pack file format (`snapstore-pagestore::pack`)

Append-only pack files capped at 1 GiB, named `pack-{:08x}.spk`.

Layout:

```
[header: magic "SPK1" | format_version u32 | pack_id u32 | created_unix u64]
[record]*
[footer (on seal): magic "SPKF" | record_count u64 | index_crc u32 | body_blake3 [32]]
```

Record:

```
[page_hash [32] | flags u8 | len u32 | payload [len]]
```

- `flags`: bit 0 = payload is raw 4 KiB page (fast path); bit 1 = reserved for
  compressed payload (cold path, later phase). M1 writes raw only.
- A pack is **active** (being appended) or **sealed** (footer written, then
  immutable). Rotation happens when the next record would cross 1 GiB.
- Crash story for M1: an active pack without a footer is scanned on open;
  a torn final record (length mismatch / short read) is truncated. Sealed
  packs are trusted via footer. Full scrub/verify is a later phase.

API: `PackWriter::append(hash, payload) -> u64 /*offset*/`, `PackWriter::seal()`,
`PackReader::read_at(offset) -> (PageHash, Bytes)`, `PackReader::scan()`.

Acceptance: write/seal/reopen/read round-trip; rotation at the 1 GiB
boundary; torn-write recovery test (truncate mid-record, reopen, verify).

## Work item 3 — sharded index (`snapstore-pagestore::index`)

Maps `PageHash → PageLoc`. Sharded 256 ways by the hash's first byte; each
shard is an independent `RwLock<HashMap<PageHash, PageLoc>>` so concurrent
ingest threads don't serialize on one lock.

Persistence: when a pack seals, write a sidecar `pack-{:08x}.idx` (sorted
`(hash, offset)` pairs + CRC). On store open, load all sidecars, then scan the
single active pack to rebuild its in-memory-only entries. The index is always
reconstructible from packs alone (`snapstore-pagestore` exposes
`rebuild_index()` for that).

Acceptance: lookup hit/miss correctness under concurrent insert (loom or
stress test); open-time rebuild equals pre-crash state; sidecar corruption
falls back to pack scan.

## Work item 4 — ingest pipeline (`snapstore-pagestore::ingest`)

The G1-critical path. Batch-oriented API on the top-level store type:

```rust
pub struct PageStore { /* dir, active pack, sealed packs, index */ }

impl PageStore {
    pub fn open(dir: &Path, opts: StoreOptions) -> Result<Self>;
    /// Ingest a batch. Returns one PageLoc per input page (dedup-aware).
    pub fn ingest(&self, pages: &[(&[u8; PAGE_SIZE])]) -> Result<Vec<PageLoc>>;
    pub fn get(&self, hash: &PageHash) -> Result<Option<Bytes>>;
}
```

Fast-path design (what gets us to 1.5 GB/s):
1. **Hash** the batch with BLAKE3 (rayon parallel over pages; BLAKE3 alone
   does multiple GB/s/core — hashing must not be the bottleneck).
2. **Dedup probe** against the sharded index; already-present pages return
   their existing `PageLoc` and cost no I/O.
3. **Append** misses to the active pack via large buffered writes: stage
   records into a 4–8 MiB write buffer, flush with single `write` calls.
   No per-page syscalls.
4. **Publish** index entries only after the bytes are written (readers never
   see a loc that can't be read).

Durability contract (decide now, it shapes everything): `ingest` returns when
data is in the page cache; an explicit `PageStore::sync()` (fdatasync on the
active pack) provides the durability barrier. M2's `commit` calls `sync()`
before returning a snapshot ref. The G1 benchmark measures the ingest fast
path as defined — buffered appends without per-batch fsync — plus a separately
reported `ingest+sync` number for honesty.

Acceptance: ingest→get round-trip for every synthetic profile; dedup returns
identical `PageLoc` for identical pages; concurrent ingest from N threads is
linearizable (no lost pages, no duplicate appends of the same hash beyond the
acceptable race window — document the chosen semantics: double-append of the
same page is allowed, index keeps the first).

## Work item 5 — G1 benchmark harness (`benches/ingest.rs`)

Criterion benchmark, `cargo bench -p snapstore-pagestore`:

- `ingest_fastpath_cold`: fresh store, `busy_workload` profile, 4 GiB of
  unique pages, report GB/s (this is the ≥1.5 GB/s gate number).
- `ingest_fastpath_warm`: second epoch with `dirty_rate = 0.1` — measures the
  dedup-dominated path.
- `ingest_plus_sync`: cold ingest including `sync()`, reported for context.

Output throughput in GB/s explicitly (custom Criterion `Throughput::Bytes`).
Document the reference machine (the Intel box) in the bench header; gate
sign-off happens there, CI tracks regressions only.

Acceptance: bench runs green; on the reference machine
`ingest_fastpath_cold ≥ 1.5 GB/s`.

## Dependencies (crates.io)

`blake3` (rayon feature), `rayon`, `parking_lot`, `crc32fast`, `bytes`,
`thiserror`, `tempfile` (dev), `criterion` (dev), `rand`+`rand_chacha`
(testgen). No serde, no tokio — the store is synchronous; async wrapping is
the server's problem in a later phase.

## Suggested execution order

```
WI0 (types) ──► WI2 (pack) ──► WI3 (index) ──► WI4 (ingest) ──► WI5 (bench)
WI1 (testgen)  [parallel with WI2/WI3; must land before WI4's tests]
```
