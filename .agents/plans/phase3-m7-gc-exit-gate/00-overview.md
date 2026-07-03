# M7 GC Plan — Overview

Plan for `.agents/requests/phase3-m7-gc-exit-gate/` (rom-operator-bridge,
2026-07-03). Implements IMPLEMENTATION-PLAN.md §M7 — mark-and-sweep GC with
pack compaction — with the model-based property suite as the centerpiece
(Phase 3 exit-gate item 4). Tracking bead: `snapstore-z5o` (claimed,
description corrected).

Written for a coding agent with no prior context. Every work item cites
file:line anchors verified on `main` at `9b2e55a` (2026-07-03).

## Files in this plan

| File | Contents |
|---|---|
| `01-storage-surfaces.md` | WI1 — new low-level APIs: index repoint/remove, pack enumeration/scan/delete, manifest delete, meta reap commands |
| `02-gc-engine.md` | WI2 — the GC cycle: epoch/fence, late-roots protocol, mark, tombstone reaping, pack compaction, manifest sweep, gc_state persistence, failpoints |
| `03-server-wiring.md` | WI3 — proto fields, TriggerGc RPC, `gc_*` metrics, `[gc]` config, watermark auto-trigger, CLI/client |
| `04-property-suite.md` | WI4 — model-based proptest suite (the gate), op-sequence generator, oracle, three invariants, negative proofs, seeded runner |
| `05-crash-harness.md` | WI5 — kills inside GC: new failpoints, scenario ops, recovery invariants |
| `06-ci-and-evidence.md` | WI6 — CI wiring + case counts, evidence.json discipline, joint-verification artifact, handback (`04-resolution.md`), bead hygiene |

## Verified current state (gap analysis, 2026-07-03)

What exists (the real head start — note this **corrects both the bead's
original overstatement and one detail in the request**):

- `gc_commit_gate: RwLock<()>` — read side taken in `put_snapshot` step 5
  (`crates/snapstore-store/src/lib.rs:277`, `:389-392`). Write side (mark
  fence) does not exist anywhere.
- **The R2 read-retry path already exists**, contrary to the request's
  "no GC read-path invalidate hooks": `read_sealed_with_retry`
  (`crates/snapstore-pagestore/src/ingest.rs:566-604`) does
  ENOENT → invalidate cached handle → re-probe index → retry-once, wired
  into the `get_batch` hot loop (`ingest.rs:448-458`) and the group-open
  failure path (`ingest.rs:464-471`). `invalidate_pack_handle`
  (`ingest.rs:648`) exists "Called by M7 GC before unlinking". These fix
  the compaction ordering contract (see 02) but have **never been
  exercised against a real repoint** — the property suite and crash
  harness are what prove them.
- Pins and tombstones: stored, honored, surfaced in stats
  (`snapstore-meta`: `pin`/`unpin`/`list_pins` lib.rs:285/299/399;
  `prune_subtree` flips subtree to PRUNED + inserts tombstone row,
  actor.rs:808-856; `tombstones_count` in `StatsRow`).
- `PackReader::scan()` (`pack.rs:556`) enumerates all `(offset, hash)`
  records of a pack — the compaction read primitive.
- `TriggerGc` RPC stub returns UNIMPLEMENTED (`service.rs:1032-1037`);
  proto messages both empty; `StoreStats` reserves `gc_runs_total = 11`,
  `gc_pages_reclaimed_total = 12` (hardcoded 0 at service.rs:1023-1024).
- Crash harness (`snapstore-crash`) with seeded op-sequence child,
  SIGKILL parent, 9-failpoint matrix (`harness.rs:24-34`), deep fsck with
  `MissingPage`/`DanglingPin` violations — ready-made GC-safety oracles.
- proptest precedent in `snapstore-manifest` with a reusable
  `test-strategies` feature (lib.rs:684-793) and a brute-force reference
  model pattern (`proptest_flatten_vs_reference`, lib.rs:1552).

What does NOT exist (all new work):

- Mark fence / epoch, root-set snapshot, mark walk.
- `ShardedIndex` repoint/remove (insert is first-writer-wins
  `entry().or_insert`, index.rs:53 — unusable for repoint).
- Pack enumeration (`discover_packs` is private, ingest.rs:696),
  compaction destination-pack allocation, pack unlink.
- Manifest delete (incl. flatten-cache invalidation — `FlattenCache` has
  no remove, store lib.rs:230-260 — and counter decrements).
- Meta commands: list tombstones, reap subtree rows, orphan input_log
  deletion, node/root-set enumeration across experiments.
- Any `[gc]` config (config structs are `deny_unknown_fields`), `gc_*`
  metrics, background-task infrastructure in the server.
- The model-based op-sequence generator (neither `snapstore-crash`'s
  child ops nor `snapstore-testgen` page profiles provide fork-tree /
  pin / GC-interleaving machinery).

## Decisions (state these in 04-resolution.md on handback)

| # | Decision | Choice | Why |
|---|---|---|---|
| D1 | Property-suite location | `crates/snapstore-server/tests/gc_properties.rs` (+ `gc-test-hooks` cargo feature on snapstore-store) | The suite needs SnapshotStore + MetaDb composed; snapstore-server is the only crate that already depends on both, and `serve_for_tests` lives there for RPC-level assertions. No new crate to wire into CI. |
| D2 | TriggerGc semantics | Synchronous by default; `detach=true` for fire-and-forget. Response carries reclaim counts when synchronous. | The property harness, joint restore-after-GC verification, and CLI all want completion + counts; API.md's fire-and-forget shape is preserved via `detach`. Additive fields on the empty vendored proto messages (canonical until `snapstore-8qx`); mirror to control-plane at adoption. |
| D3 | GC engine layering | Mechanics (mark walk, sweep, compaction) in `snapstore-store::gc` operating on caller-supplied roots; orchestration (reap → fence → mark → sweep, meta access) in `snapstore-server/src/gc.rs` | snapstore-store must own the gate/epoch/late-roots state but does not depend on snapstore-meta; the server composes both. Keeps the dependency graph acyclic. |
| D4 | Compaction destination | Dedicated GC-owned packs (fresh PackIds above the fence), NOT the active ingest pack | Normal `ingest()` dedups against the index so it can never re-copy an already-indexed page; and sharing the active pack couples GC to the single pack-writer. GC packs get ids > fence so they are never swept in their own cycle. |
| D5 | Commit-race protection | Gate read lock widened to cover presence-check→publish; epoch-scoped `note_live_ref` late-roots set; sweep finalizes per pack under brief gate write with a drain-late-roots straggler loop | Closes the two real races (put_snapshot presence-check vs sweep drop; CreateNode/Pin of a pre-fence orphan manifest vs manifest sweep). Full protocol in 02. |
| D6 | Benchmark bar (100k-node/30 GB, <60s, p99<2×) | Deferred past the gate as its own bead, `bd dep add` against `snapstore-z5o` | Per request 02 §Explicitly Deferrable; it is a `BM:` item, not `AC:`. |
| D7 | CI lanes | No new runner lane. PR: property suite at ≥500 cases in `ci.yaml` rust job. Nightly: ≥10k-case job in `nightly.yaml` with explicit logged seed. | nightly.yaml's own header argues hosted runners suffice; case counts map onto the existing split. |

## Work-item sequencing

```
WI1 storage surfaces ──► WI2 gc engine ──► WI3 server wiring ──► WI6 CI + evidence
                              │
                              ├──► WI4 property suite  (needs WI2 hooks; drives WI2 fixes)
                              └──► WI5 crash harness   (needs WI2 failpoints)
```

WI4 and WI5 are parallel once WI2 lands. Expect WI4 to find bugs in WI2 —
that is its job; budget iteration time. Gate-required subset =
WI1+WI2+WI3(minus auto-trigger polish)+WI4+WI5+WI6.

## Out of scope

- The M7 `BM:` benchmark (D6 — deferred bead).
- `gc_*` dashboards beyond basic counters (Phase 6); basic counters DO land.
- M9 backup consistency-point use of the gate write side (M9 reuses what
  WI2 builds).
- Any change to the **deployed production snapstore-server on this host**
  (rom-bridge-o73 runtime). All testing on scratch instances; if the
  deployed instance ever needs the new binary, the bridge side owns the
  restart choreography (request 01 §Deployment Cautions). Nothing in this
  plan requires an on-disk format change to existing data: the meta
  migration (02) is additive and applied on open.

## Invariant references (cite in test names/comments)

- **R1 safety**: never delete a page/manifest reachable from a live
  manifest chain rooted at a non-deleted node or pin.
- **R2 read correctness**: index entry always points at a durable copy;
  reader retry-once is sufficient because a pack is unlinked only after
  every live record is repointed.
- **R3 fence**: commits and mark-root snapshot serialize on
  `gc_commit_gate` (commits read, fence write).
- **R4**: GC never self-concurrent; crashed GC leaves only extra copies.
- **R5**: pins are roots, period.
