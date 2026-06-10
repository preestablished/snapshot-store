# M6 — durability: crash-injection harness

Proves the commit-ordering contract (pages → manifest → node row,
ARCHITECTURE.md §3/§8) survives `kill -9` at every boundary. This is phase
exit gate 3. Library-mode harness needs only 01; full-stack mode needs 02;
fully parallel with 03.

Phase 1 already covers in-process torn-tail/rotation/sync-spans-rotation cases
inside `snapstore-pagestore`; M6 generalizes to **child-process SIGKILL**
against the whole stack with targeted failpoints.

**Platform:** the harness runs on Linux (reference box / Linux CI) — fsync
semantics and `/proc` are what we're testing. It should *build* everywhere;
kill/fd assertions are `cfg(target_os = "linux")`.

## Work item 1 — failpoint instrumentation

(Hooks land with 01 WI5 while the commit path is being rewritten; this WI
finalizes the matrix.) `fail` crate behind a workspace `failpoints` feature,
compiled only in the harness profile. Named points at every ordering boundary:

| # | Failpoint | Boundary it brackets |
|---|---|---|
| 1 | `pack-append` | record bytes written, not yet synced |
| 2 | `pack-fdatasync` | before/after batch fdatasync |
| 3 | `pack-rotate-seal` | mid-rotation (seal old / open new) |
| 4 | `sidecar-write` | `.idx` sidecar bytes written |
| 5 | `sidecar-fsync` | sidecar durable vs not |
| 6 | `manifest-tmp-write` | `.spm` staged in `tmp/` |
| 7 | `manifest-fsync` | staged file synced |
| 8 | `manifest-rename` | rename into `manifests/` done, dir not synced |
| 9 | `manifest-dirsync` | full PutSnapshot durability point |

Each point supports `panic`/`exit` actions (configured via `FAILPOINTS` env in
the child). The SQLite node-row boundary is exercised by **timed kills**
around meta-actor txns (SQLite's own WAL atomicity is the contract; we verify
batch all-or-nothing, not its internals).

## Work item 2 — deep fsck (library + `snapstorectl fsck`)

The invariant checker the harness (and operators) run after every recovery.
Library function in `snapstore-store` (or a small `fsck` module crate-local to
the CLI if dependency direction is cleaner), exposed as
`snapstorectl fsck [--deep]` (offline, direct store access):

All checks are written against the **as-built** pack/sidecar format
(`pack.rs`: `SPK1` header, 37-byte record headers hash+flags+len, `SPKF`
footer with record count + body BLAKE3; `index.rs`: `.idx` sidecars with
CRC32 trailer) — NOT against ARCHITECTURE.md §2.1–2.2's `SPPACK01`/`CREC`/
`.sppx` layout, which the code deliberately diverges from (00-overview
risk 6). There is no per-record `rec_magic` or crc32c in the as-built format;
per-record integrity comes from the stored BLAKE3 hash itself.

- **Shallow** (ARCHITECTURE.md §8 step 5 + container checks): every node
  row's `snapshot_ref` resolves to a manifest; every pinned ref resolves;
  every manifest entry resolves to an indexed page; every `input_log_id`
  referenced by a node exists; `.idx` sidecar CRC32s and `.spm` manifest
  footers verify; sealed-pack `SPKF` footers present with matching record
  counts.
- **Deep**: additionally re-read every pack record and verify
  BLAKE3(payload) == the record's stored `page_hash`; recompute each sealed
  pack's body BLAKE3 against its `SPKF` footer; re-hash input-log container
  footers.

Output: machine-readable report (counts + first N violations), nonzero exit on
any violation.

**AC:** seeded-corruption tests — flip a payload byte / truncate a sidecar /
zero a manifest footer / delete a referenced `.spm` → fsck reports exactly
that violation class.

## Work item 3 — the harness (`crates/snapstore-crash`)

Parent/child architecture per the upstream plan:

- **Child** (bin): opens a store on a scratch dir and runs a scripted, seeded
  workload through the **library** APIs (pagestore + store façade + meta
  actor): synthetic exploration loop — ingest delta pages, put input log, put
  snapshot, create node, batched update-nodes, occasional KV CAS checkpoint,
  prune. Before reporting any op as acknowledged, the child appends
  `(op, key/ref, result)` to an **oracle journal** on the scratch fs written
  with O_SYNC append — the journal is what "the client observed success"
  means.
- **Parent** (test runner): spawns the child and kills it with SIGKILL either
  (a) at a randomized, seeded delay/op-count, or (b) at a named failpoint from
  WI1's matrix (env-configured). Then: reopen the store (normal startup
  recovery), run **fsck --deep**, and check the invariants:
  1. Every node row's `snapshot_ref` resolves; every manifest entry resolves
     to a page whose stored bytes hash to its key (deep fsck).
  2. Every `PutSnapshot` the journal records as acknowledged is durable; an
     unacknowledged one is wholly absent **or** fully valid — never partially
     visible (a `.spm` with a bad footer must have been removed by recovery).
  3. Same for CreateNode/UpdateNodes batches: acknowledged ⇒ present;
     unacknowledged batch ⇒ wholly present or wholly absent.
  4. Logical counter monotonicity across the restart.
- Every run is reproducible from `(workload_seed, kill_spec)`; failures print
  the repro command line.

**AC:** harness runs N randomized cycles + the failpoint matrix from one
command (`cargo run -p snapstore-crash -- --cycles N --seed S
--matrix-passes K`; `--matrix-passes 0` skips the matrix — same interface 05
S5 invokes).

## Work item 4 — SQLite batch-atomicity kill loop

The upstream M3 AC deferred to this harness: kill -9 during a 256-update
`UpdateNodes` batch ⇒ on restart the batch is wholly present or wholly absent
— loop ×200. Implemented as a dedicated harness scenario (child hammers
update batches; parent kills at random; invariant 3 checked each cycle).

## Work item 5 — full-stack mode + CI wiring

- **Full-stack scenario** (after 02): child = the real `snapstore-server`
  process driven by `snapstore-client` from the parent; kills hit the server.
  Verifies the same invariants through the public API (and that a client's
  blind-retry after the restart converges — ties to INTEGRATION.md §6's
  "clients simply retry in-flight ops").
- **CI infrastructure** (review finding: current CI is fmt+build+test only —
  none of the plan's CI commitments have a home yet; this WI owns building
  them, not just "wiring"):
  - **Immediately** (first commit of phase 2, independent of the harness):
    add `cargo clippy --workspace --all-targets -- -D warnings` to PR CI
    (claimed in the phase-1 sign-off, never enforced), and pin the
    control-plane checkout rev in `ci.yaml` (00 risk 2).
  - PR job: library-mode smoke (~25 randomized cycles + failpoint matrix ×1),
    required.
  - Nightly job: **1,000 randomized cycles with zero invariant violations** +
    failpoint matrix (9 boundaries × kill) **×50 each** (upstream M6 AC),
    plus the 10-minute manifest fuzz run (01 WI1 — net-new: no `fuzz/` dir
    exists; the phase-1 M2 fuzz AC was never delivered) and the perf-regression
    smoke. **Decision needed before building**: GitHub-hosted runner vs
    self-hosted on the reference box — budget the wall-clock of 1,000
    cycles + deep fsck per cycle empirically (a 6-hour job is fine nightly,
    a 20-hour one isn't) and pick accordingly; kill/fd semantics require
    Linux either way.

**AC (milestone):** nightly job green: 1,000 cycles, zero violations; matrix
×50 green; PR smoke wired and required.

## Dependencies and ordering

```
(01 WI3+WI5) ──► WI1 matrix ──► WI3 harness ──► WI4 sqlite loop ──► WI5 CI
(01 WI3)     ──► WI2 deep fsck ──┘                    (02) ──► WI5 full-stack
```
