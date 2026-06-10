# M5 — fast path: SEQPACKET page channel with memfd fd-passing

Bulk page bytes for co-located hypervisor workers, per API.md §4. Control
stays on gRPC; only page payloads ride this channel. Depends on 02 (server
wiring, client transport selection). The numbers here gate MAP.md principle 2
— treat BM misses as release blockers, not soft targets (hardware caveats in
05).

**Platform:** Linux-only (`SOCK_SEQPACKET`, `memfd_create`, `SCM_RIGHTS`).
The crate compiles to a stub returning `Unsupported` on other targets so the
workspace stays green on the darwin dev machine; all acceptance runs happen on
the Intel reference box.

## Work item 1 — `crates/snapstore-localpath`: protocol layer

Wire types exactly per API.md §4, `repr(C)`, little-endian, packed as written:

- `PcHdr { magic: 0x50434831 "PCH1", msg: u16, flags: u16, seq: u64, count: u32, reserved: u32 }`
- msg 1 `PUT_BATCH` (client→server, fd = memfd, count ≤ 8192 pages ⇒ ≤ 32 MiB)
- msg 2 `PUT_BATCH_OK` body `PutOkBody { pages_new: u32, pages_deduped: u32, batch_blake3: [u8; 32] }`
  — `batch_blake3` = BLAKE3 over concatenated per-page hashes in memfd order
  (the full hash list would blow the 64 KiB datagram cap)
- msg 3 `GET_BATCH` (count × `GetReq { page_hash: [u8; 32], dst_slot: u64 }`,
  count ≤ 1500/datagram; client sends multiple datagrams for larger sets, seq
  orders them)
- msg 4 `GET_BATCH_DATA` (fd = memfd, page for request[i] at offset i*4096;
  `dst_slot` echoed metadata, server never interprets it)
- msg 5 `ERROR` body `ErrBody { code: u32 /* 1 NOT_FOUND, 2 INVALID, 3 OVERLOAD */, detail_len: u32, utf8… }`

Encode/decode functions are pure over byte slices → unit-testable on any OS
(size/layout asserted with `const` checks against the spec offsets); the
sendmsg/recvmsg + `SCM_RIGHTS` ancillary plumbing (via `nix`) is the
Linux-only half.

**AC:** codec round-trip + layout-constant tests (host-portable); a
loopback socketpair test sending each message type with an fd (Linux).

## Work item 2 — server half

Wired into `snapstore-server` startup: listener on `page_channel_path`
(`/run/snapstore/pages.sock`, mode 0660), one task per connection.

- **PUT_BATCH**: receive fd → validate memfd size == count*4096 → mmap
  readonly → rayon batch-hash (same pool as gRPC ingest) → dedup probe →
  enqueue novel pages to the pack writer → reply `PUT_BATCH_OK` with
  new/deduped counts + cross-check hash → close fd after the batch is
  queued+indexed (never earlier; never leak on error paths — RAII fd guard).
  Zero pages short-circuit as in the existing ingest path.
- **GET_BATCH**: collect datagrams, look up each hash (index probe + pack
  pread, or flatten-cache-backed resolve), create memfd sized count*4096,
  write pages at i*4096, seal size (`F_SEAL_GROW|F_SEAL_SHRINK`), send fd.
  Unknown hash ⇒ `ERROR NOT_FOUND` naming the hash in detail.
- **Backpressure**: bounded ingest queue (`ingest_queue_pages`); when full,
  reply `ERROR OVERLOAD` (client backs off and retries — content-idempotent).
- Malformed datagram / bad magic / fd-count mismatch ⇒ `ERROR INVALID` and
  connection close (protocol state is per-message, so close is safe).
- Metrics: per-direction bytes/batches, `snapstore_page_channel_clients`,
  cross-check-mismatch counter (never expected to move).

**AC:** PUT/GET round-trip property test (random batches, sizes 1..8192,
random dedup overlap) — bytes out == bytes in, counts correct; a killed client
mid-batch leaks nothing (server-side fd audit: `/proc/self/fd` count before ==
after, looped); OVERLOAD path exercised with a tiny queue.

## Work item 3 — client half + auto-selection

In `snapstore-localpath` (client struct) + `snapstore-client` (selection):

- Client creates memfds, writes pages, sends `PUT_BATCH`; computes per-page
  hashes itself (it builds the manifest anyway) and **cross-checks**
  `batch_blake3` — mismatch is a P0 determinism bug: typed fatal error +
  metric, never retried silently (API.md §4: "Mismatch = P0").
- `GET_BATCH` fan-out (≤1500/datagram), reassembly by seq, scatter via
  `dst_slot`.
- Auto-selection order in `SnapstoreClient` (API.md §4 rules): page channel if
  `page_channel_path` exists and connects → UDS gRPC → TCP. Page bytes only;
  every operation keeps its pure-gRPC equivalent (fallback on channel error,
  with a warn log + metric).

**AC:** unit test for the corrupted-cross-check path (flip a byte in a stored
page via test hook ⇒ client surfaces P0 error + server metric increments);
fallback test (no socket ⇒ gRPC path used, results identical).

## Work item 4 — benchmarks (M5 gate)

Criterion/`snapstorectl bench` on the reference box (methodology + hardware
caveats in 05):

- `PUT_BATCH` ingest ≥ **1.5 GB/s** sustained — measured dedup-warm
  (transport+hash bound) against the spec number; cold disk-bound variant
  recorded against the SATA ceiling for the record.
- `GET_BATCH` ≥ **2.5 GB/s** warm (page cache).
- **16 parallel clients** each committing 8 MiB deltas (2,048 pages):
  p99 commit (PUT_BATCH + PutSnapshot incl. fsync) < 40 ms, aggregate
  ≥ 1.2 GB/s — same hardware caveat; this is the number hypervisor M5
  switch-over cares about.

## Dependencies and ordering

```
(02 server wiring) ──► WI1 protocol ──► WI2 server half ──► WI4 BMs
(02 client)        ──────────────────► WI3 client half ──► WI4
```

WI1 can start as soon as 02 WI2's server skeleton exists (it's mostly
self-contained); WI2/WI3 parallelize across server/client once WI1's codec is
stable. Hypervisor M4 does **not** wait for M5 (it integrates on gRPC and
switches to the fast path when M5 lands — phase doc note).
