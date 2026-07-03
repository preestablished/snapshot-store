# WI5 — Crash Harness Extension (kills inside GC)

Extends `crates/snapstore-crash`. Recovery invariant (plan AC): **recovery
never loses reachable data; at worst it leaks space reclaimed by the next
cycle.**

## 1. Child workload (src/child.rs)

Extend the `Default` scenario op mix (child.rs:115-325):

- `pin` / `unpin` ops (~every 16 steps, random known ref; journal
  `pin\t<refhex>` only after Ok — pins change the root set, so recovery
  checks need them).
- `gc` op (~every 24 steps): call `run_gc_cycle` **in-process** (child
  already opens SnapshotStore + MetaDb directly; add snapstore-server as a
  dependency for the orchestrator fn, or move `run_gc_cycle` to a location
  both can use — if the dependency feels heavy, re-export it from
  snapstore-store behind the orchestration closure from 02 §2). Journal
  `gc_done\t<cycle>` after Ok. Use default GcOpts (threshold 0.5, grace 1)
  — production shape, not the exactness shape. The `gc_done` journal line
  must carry the facts recovery needs, NOT leave them to be re-derived:
  `gc_done\t<cycle>\treaped=<exp:node,...>` (the reaped subtree roots) —
  re-deriving grace arithmetic (tombstone created_at vs previous fence
  counter) during journal replay would be fragile and was flagged in
  review.
- Keep op counts/seeds derived from the existing `StdRng::seed_from_u64`
  stream so old seeds stay reproducible (append new ops to the dispatch
  table; do not reorder existing arms).

The parent's random-sleep SIGKILL (harness.rs:216-237) now lands inside GC
cycles naturally; the failpoint matrix targets exact boundaries.

## 2. Failpoint matrix (harness.rs:24-34)

Append the six new failpoints from 02 §7 to `FAILPOINTS`:
`gc-compact-copy`, `gc-compact-seal`, `gc-index-repoint`,
`gc-pack-unlink`, `gc-manifest-unlink`, `gc-reap-txn`.

The existing matrix runner arms one failpoint per child via
`FAILPOINTS=<name>=panic` env (harness.rs:277-279) — unchanged; the new
names just need the child workload to actually reach GC (guarantee: when a
`--failpoint gc-*` is requested, the child scenario forces a GC op early
and repeatedly so the failpoint is hit within the op budget; mirror how
sqlite-batch forces its path).

## 3. Recovery + invariants (harness.rs:306-529)

After kill → reopen → fsck --deep → journal replay, add checks:

1. Every journaled (acknowledged) snapshot still resolves with correct
   bytes **unless** a journaled `gc_done` line lists its subtree as
   reaped (reachability per the journal's own root-set replay:
   nodes + pins − the reaped sets carried in `gc_done` lines — no grace
   arithmetic re-derivation).
2. Every journaled pin's ref resolves (R5) — fsck's `DanglingPin` and
   `MissingPage` violations (fsck.rs:4-13) already catch the on-disk
   side; the journal check catches "acknowledged then lost". (This
   invariant is only sound because the Pin handler now validates under
   the gate — 03 §7; without that fix, dangling pins were creatable and
   this check would flake.)
3. **Space-leak tolerance:** do NOT assert exact physical == reachable
   after recovery. Instead: run one full in-process GC cycle
   post-recovery, then assert no fsck violations and all
   journal-reachable refs still resolve (the "next cycle reclaims"
   clause). A leak counter (pre-GC unique_pages − post-GC) goes into the
   cycle summary for the evidence tables.
4. GC-pack adoption sanity (from 01 §2): if the kill landed between
   gc-pack creation and seal, reopen must succeed and either adopt or
   seal the orphan pack; assert reopen + fsck green (this is the runt/
   unsealed-pack window the phase-2 harness memory warns about).
5. Sidecar-integrity assertion (from 01 §2's GcPackWriter caution): for
   every sealed pack with a sidecar present, sidecar entry count ==
   pack record count — catches the empty-sidecar failure mode where
   `load_sidecar` succeeds with 0 entries and the rebuild fallback never
   fires. Cheap to add inside fsck's pack pass.

## 4. Matrix + cycle targets (gate AC 3)

- PR smoke (`ci.yaml` crash-smoke job): existing
  `--cycles 25 --matrix-passes 1` now covers 15 failpoints (9 + 6) — keep
  as-is; wall-clock impact is minutes.
- Nightly (`nightly.yaml` crash-suite): existing
  `--cycles 1000 --matrix-passes 50` — with the matrix now ×15, verify the
  120-min timeout still holds (phase-2 measured ~6 min for 9 boundaries;
  linear scaling says ~10 min — fine).
- Evidence run (06): one recorded `--cycles 1000 --matrix-passes 50` with
  a fixed seed on the reference box, per-failpoint pass table.

## 5. Deliberate non-goals

- No FullStack (gRPC server kill) GC scenario in the gate: the release
  server binary has no failpoints and random-timing kills of a
  short-lived GC cycle add flake, not coverage. The in-process
  default-scenario coverage plus the property suite's RPC smoke is the
  gate bar. File a follow-up bead if the bridge review asks for it.
