# M2 — Manifest codec + snapshot commit/resolve

**Crates:** `snapstore-manifest` (rewrite), `snapstore-store` (new)
**Depends on:** M1 (`PageStore::ingest`/`get` API stable)
**Parallel with:** M3
**Unblocks:** Phase 1 gate G2 (manifest round-trip property tests green)

## Scope

A snapshot is a manifest: a complete, self-describing record of one guest
state — which pages, in what order, plus opaque device/vcpu state blobs. M2
delivers (a) the manifest data model, (b) a canonical binary codec for it,
and (c) `commit`/`resolve` on top of the M1 page store. Resolve of a
committed snapshot must reproduce the input byte-for-byte.

Out of scope: lineage *queries* (M3 owns the DB; M2 only records `parent` in
the manifest), incremental/delta manifests (the page store dedup already
gives storage-level deltas; manifest-level deltas are a later optimization),
proto wire format.

## Work item 1 — manifest model (`snapstore-manifest`)

Replace the Phase 0 stub:

```rust
pub struct Manifest {
    pub version: u32,                  // SNAPSHOT_MANIFEST_VERSION = 1
    pub parent: Option<SnapshotRef>,   // lineage edge; None for roots
    pub icount: u64,                   // retired-instruction count at capture
    pub virtual_ns: u64,               // virtual clock at capture
    pub memory: MemoryMap,
    pub devices: Vec<DeviceState>,
}

pub struct MemoryMap {
    pub page_size: u32,                       // 4096 in Phase 1
    pub regions: Vec<MemoryRegion>,           // sorted by gpa, non-overlapping
}

pub struct MemoryRegion {
    pub gpa: u64,                             // guest-physical base
    pub pages: Vec<PageHash>,                 // one hash per page, in order
}

pub struct DeviceState {
    pub kind: String,                         // e.g. "vcpu0", "detclock"
    pub blob: Vec<u8>,                        // opaque to the store
}
```

Notes:
- `icount`/`virtual_ns` are carried opaquely; the hypervisor owns their
  meaning. They're in the manifest because the snapshot ref must cover them
  (two snapshots at different icounts must never collide).
- `devices` is ordered `Vec`, not a map — canonical encoding needs a defined
  order. Constructor sorts by `kind` and rejects duplicates.
- Invariants enforced at construction (`Manifest::new` validates): regions
  sorted/non-overlapping, page_size power of two, device kinds unique. The
  decoder re-validates, so a decoded manifest is always a valid manifest.

## Work item 2 — canonical codec

Hand-rolled binary encoding. **Not serde**: the encoding is a hash input
(`SnapshotRef = blake3(encoded manifest)`), so it must be canonical and
stable across compiler/library versions.

Rules:
- All integers little-endian fixed-width. Collections as `u32 count` +
  elements. `Option` as `u8` tag. Strings as `u32 len` + UTF-8 bytes.
- A field-order change is a format change ⇒ bump `SNAPSHOT_MANIFEST_VERSION`.
- Decoder is strict: trailing bytes, bad tags, or invariant violations are
  errors (no lenient mode — lenient decoding breaks ref stability).
- `encode(&self) -> Vec<u8>`, `decode(&[u8]) -> Result<Manifest, DecodeError>`,
  `Manifest::compute_ref(&self) -> SnapshotRef` (= blake3 of encode).

Property tests (proptest, this is gate G2):
- **Round-trip identity**: ∀ valid manifest `m`, `decode(encode(m)) == m`.
- **Ref stability**: `encode` is a pure function — equal manifests ⇒ equal
  bytes ⇒ equal refs; any single-field mutation ⇒ different ref.
- **Strictness**: ∀ `m`, appending a byte to `encode(m)` fails decode;
  truncating fails decode.
- A `proptest` `Strategy` for `Manifest` lives in the crate behind a
  `test-strategies` feature so M3 and integration tests reuse it.

Plus one **golden vector test**: a fixed manifest checked against a
hex-encoded expected byte string committed to the repo. This is what actually
catches accidental format drift across refactors — property tests alone
won't.

## Work item 3 — commit/resolve (`snapstore-store`)

New crate composing `snapstore-pagestore` + `snapstore-manifest`:

```rust
pub struct SnapshotStore { pages: PageStore, /* manifest storage */ }

impl SnapshotStore {
    pub fn open(dir: &Path) -> Result<Self>;

    /// Ingest all pages, durably persist, store the manifest, return its ref.
    pub fn commit(&self, guest: &GuestImage) -> Result<SnapshotRef>;

    /// Load the manifest for a ref.
    pub fn resolve(&self, r: &SnapshotRef) -> Result<Manifest>;

    /// Stream the full memory image of a snapshot back out.
    pub fn read_memory(&self, m: &Manifest)
        -> impl Iterator<Item = Result<(u64 /*gpa*/, Bytes)>>;
}
```

`GuestImage` is a borrow-view input type (regions of `&[u8]` + device blobs +
icount/virtual_ns) so the hypervisor can later hand over guest memory without
copying; M1's testgen grows a `as_guest_image()` adapter.

Commit sequence (ordering is the correctness story):
1. `pages.ingest()` every region's pages (batched).
2. `pages.sync()` — durability barrier (see M1 durability contract).
3. Build `Manifest` from returned hashes + metadata; `encode`; compute ref.
4. Store the encoded manifest, keyed by ref.
5. Return ref. A ref is never observable before steps 1–4 complete, so a
   resolvable ref always resolves to fully-durable pages.

Manifest storage for Phase 1: a `manifests/` subdirectory, one file per ref
(`manifests/{hex}.smf`), written via temp-file + rename for atomicity.
Manifests are small (a 4 GiB guest ≈ 32 MiB of hashes; typical regions far
less); pack-storing manifests is a later optimization and must not block G2.

Idempotency: committing identical state twice yields the identical ref and is
a near-no-op (all pages dedup, manifest file already exists).

Integration tests (synthetic, via testgen):
- commit → resolve → `read_memory` is byte-identical to the source guest for
  `idle_linux` and `busy_workload` profiles.
- Multi-epoch: commit epoch 0, `step_epoch`, commit epoch 1 with
  `parent = ref0` → both resolve correctly; store size growth ≈ dirty pages
  only (assert dedup actually worked, with tolerance).
- Reopen the store from disk, resolve both refs, verify bytes again.
- Same-state-twice ⇒ same ref.

## Suggested execution order

```
WI1 (model) ──► WI2 (codec + property tests) ──► WI3 (commit/resolve)
```

WI1+WI2 are pure in-crate work and can start the moment M1's `PageHash` type
exists (before the rest of M1 is done); WI3 needs M1 complete.
