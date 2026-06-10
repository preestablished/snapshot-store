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
| 4 | `sidecar-write` | `.sppx` bytes written |
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

- **Shallow** (ARCHITECTURE.md §8 step 5 + sidecar footers): every node row's
  `snapshot_ref` resolves to a manifest; every pinned ref resolves; every
  manifest entry resolves to an indexed page; every `input_log_id` referenced
  by a node exists; sidecar and manifest footers verify.
- **Deep**: additionally re-read every pack record — `rec_magic`, crc32c of
  payload, BLAKE3(payload) == `page_hash` in the record header; input-log
  container footers re-hashed.

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
command (`cargo run -p snapstore-crash -- --cycles N --seed S [--matrix]`).

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
- **CI**: PR job = library-mode smoke (~25 randomized cycles + one pass of the
  failpoint matrix ×1); nightly = **1,000 randomized cycles with zero
  invariant violations** + failpoint matrix (9 boundaries × kill) **×50
  each** (upstream M6 AC). Nightly runs on the Linux runner / reference box.

**AC (milestone):** nightly job green: 1,000 cycles, zero violations; matrix
×50 green; PR smoke wired and required.

## Dependencies and ordering

```
(01 WI3+WI5) ──► WI1 matrix ──► WI3 harness ──► WI4 sqlite loop ──► WI5 CI
(01 WI3)     ──► WI2 deep fsck ──┘                    (02) ──► WI5 full-stack
```
