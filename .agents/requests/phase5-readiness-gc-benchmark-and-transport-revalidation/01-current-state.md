# Current State (Evidence-Based)

Repo `main` at `3b665a7`, clean tree, assessed 2026-07-07.

## What Is Proven

- **M7 GC correctness — done and independently verified.** The full `AC:`
  scope of IMPLEMENTATION-PLAN.md §M7 landed (`3a539c7`, `e4511cf`,
  `0de6573`): epoch-fenced mark with commit-gate write side, tombstone
  reaping, pack compaction with index repoint and retry-on-race reads,
  manifest sweep, `TriggerGc`, watermark auto-trigger, `gc_*` metrics.
  Property suite: 10k cases (seed 20260703), 7 passed / 0 failed, 21
  read-retry-race hits, 4 permanent negative proofs; crash matrix 1000
  cycles + 15 failpoints × 50 with zero invariant failures; five real
  engine bugs found and fixed by the suites. Evidence root:
  `target/m7-acceptance-20260703T063635Z/`. Independent verification by
  rom-operator-bridge:
  `.agents/requests/phase3-m7-gc-exit-gate/05-verification.md` — "Phase 3
  exit-gate item 4 is green," including the joint restore-after-GC
  criterion (900/900 surviving refs resolve).
- **M5 fast path — functionally done**, SEQPACKET/memfd page channel
  (`37aeb4e`, `9cba59e`, `7902e28`), crash harness green (M6).

## What Was Deferred, With Receipts

1. **`snapstore-feb` (P2) — the M7 `BM:` benchmark bar.** The M7 resolution
   split performance out of the correctness gate. The plan's bar: GC of a
   100k-node tree (~30 GB, 50% garbage) in **<60s under concurrent
   200 MB/s ingest, with p99 commit latency during GC < 2× idle**
   (IMPLEMENTATION-PLAN.md §M7). No measurement of any kind exists yet.
2. **`snapstore-28z` (P1) — M8-entry benchmark re-validation.** M5's
   benchmarks missed their bars on the SATA reference box: **PUT_BATCH
   ingest 0.89 GB/s vs the ≥1.5 GB/s sustained bar, GET_BATCH warm
   0.64 GB/s vs ≥2.5 GB/s** (`docs/bench-baseline.md`). The recorded
   attribution is **CPU/memory-bus saturation** (double-hashing, BLAKE3
   verify, memfd overhead on dedup-warm rows) — explicitly *not* disk.
   The bead defers re-measurement to NVMe-class hardware, **alongside the
   fsync-bound rows** the baseline also defers: the 16-client × 8 MiB
   concurrent-commit row (p99 measured ~1.0 s against a <40 ms bar,
   aggregate ≥1.2 GB/s spec — the single most soak-representative row)
   and the CreateNode/UpdateNodes p50 rows. Unresolved either way.
3. **`snapstore-nn4` (P2) — flaky `page_channel_fallback` test**, failing
   in ~30–50% of runs per the bead ("metrics-count assertions racing the
   fallback path"; the bead notes it was "verified present at `e4511cf`
   without M7 changes" and is not GC-related). On the fast-path fallback
   the search loop will lean on. Not hardware-gated; actionable today.

## Why This Lands In The Phase 5 Blast Radius

- `phase-5-closed-loop.md` entry requirements: "Phase 3's snapshot-store GC
  (long runs churn the tree)". Exit gate 5: "sustained expansions/sec keeps
  all Intel-box worker slots >80% busy over a 4-hour soak."
- The orchestrator's M6 first-integration milestone runs "real
  snapshot-store + real hypervisor on the Intel box" — the first time this
  store sees fork-heavy churn from a real search, with GC required to keep
  pace in the background.
- The plan's own risk register names exactly the failure modes the
  benchmark exists to bound: R5 ("GC compaction I/O trashes commit
  latency") and R8 ("disk fills before GC reclaims").

## Open Beads Snapshot (for orientation)

7 open, 0 in progress: `28z` (P1, transport re-validation), `675` (P2, M8
epic — gated on hypervisor joint milestone), `feb` (P2, GC benchmark),
`8qx` (P2, vendored proto swap — gated on control-plane), `nn4` (P2, flaky
test — ready), `pov` (P2, beads Dolt remote repair — infra), `agz` (P3,
M9 — Phase 8). This request covers `feb` + `28z` + `nn4` and nothing else.
