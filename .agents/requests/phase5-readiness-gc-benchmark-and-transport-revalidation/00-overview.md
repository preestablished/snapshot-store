# Request: Prove GC + Transport Performance Before Phase 5 Leans On Them

## Who Is Asking

The `exploration-orchestrator` project, as the owner of Phase 5's critical
path (the select→fork→act→score→commit loop and its 4-hour soak gate). Filed
2026-07-07 on behalf of the determinism phase plan
(`~/.agents/projects/determinism/phases/phase-5-closed-loop.md`).

## Why snapshot-store, Why Now

Phase 3's snapshot-store obligation (M7 GC correctness) is **done and
independently verified** — `.agents/requests/phase3-m7-gc-exit-gate/`
resolved 2026-07-03, property suites green, five real engine bugs caught and
fixed. Nothing in this request re-litigates that.

What remains is the half of M7 the resolution explicitly deferred: the
**benchmark bar** (`snapstore-feb`), plus the M5 transport shortfall recorded
at M8-entry (`snapstore-28z`). Both matter to us specifically:

- Phase 5's entry requirements name "Phase 3's snapshot-store GC (long runs
  churn the tree)", and its exit gate 5 is a **4-hour soak keeping all
  Intel-box worker slots >80% busy**. The property suite proved GC is
  *correct*; only the benchmark proves GC *keeps up* — 100k-node tree, ~30 GB,
  50% garbage, collected in <60s **under concurrent 200 MB/s ingest with p99
  commit latency < 2× idle** (IMPLEMENTATION-PLAN.md §M7 `BM:`). If that bar
  fails, the long search stalls or fills disk mid-soak — the plan's own R5/R8
  risks — and we find out during our gate run instead of now.
- The M5 benchmarks missed their bars on the SATA reference box
  (`snapstore-28z`, P1): PUT_BATCH ingest 0.89 vs ≥1.5 GB/s, GET_BATCH warm
  0.64 vs ≥2.5 GB/s, and — deferred with them — the 16-client concurrent-
  commit row that measured p99 ~1.0 s against a <40 ms bar. Fork-heavy
  search is concurrent commits; if the shortfall is a real hardware
  ceiling, fine — but that needs to be *known*, not assumed, before the soak.
- `snapstore-nn4`: the `page_channel_fallback` test flakes in ~30–50% of
  runs and sits on the exact transport path the search loop exercises. A
  flaky test on a load-bearing path is either a test bug or a real race;
  a real race there would warrant P0 treatment.

This is the last quiet window: once Phase 4 closes, the orchestrator's M6
first-integration run starts hitting this store for real.

## The Ask In One Paragraph

Run the deferred M7 GC benchmark and the M5 transport re-validation on
NVMe-class hardware (first confirming whether the Intel lab box qualifies —
if no qualifying box exists, say so in the resolution and this request
converts into a hardware escalation with your measured numbers attached), fix
or root-cause the `page_channel_fallback` flake, and record the results with
the same durable-evidence discipline as the M7 acceptance
(`target/m7-acceptance-20260703T063635Z/`). Pass/fail against the plan's
stated bars; a *measured, explained* miss with a bottleneck analysis is an
acceptable outcome — an unmeasured assumption is not.

## Files In This Request

| File | Contents |
|---|---|
| `01-current-state.md` | Evidence-based state: what M7/M5 proved, what was deferred, the three open items |
| `02-requested-work.md` | The ask, suggested sequencing, acceptance criteria, out of scope |
| `03-verification-offer.md` | The churn workload and soak rehearsal the orchestrator provides |
