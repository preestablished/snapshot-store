# Requested Work

## What We Need (Behavioral)

Before the orchestrator's M6 first-integration run, the store's two deferred
performance questions are answered with measurements, and the one flaky test
on the fast path is fixed or root-caused:

1. **Hardware preflight (first, cheap).** Identify the target box and
   record it by hostname. Note the ambiguity you'll need to resolve: the
   SATA reference box in `docs/bench-baseline.md` is itself an Intel
   (i5-8400) machine — determine whether the box Phase 5's soak will run
   on ("the Intel box" in the phase docs) is that same machine or a
   different more capable one, and say which in the evidence. Record disk
   class, sequential/random throughput baseline, CPU headroom, and free
   disk (the GC benchmark needs ~60+ GB working space). If no
   operator-attested reference box is reachable, stop here and write
   `04-resolution.md` with the preflight numbers, file a P1 bead for the
   blocked benchmarks, and flag the operator (Matt) directly — the hardware
   escalation is then a program-plan decision, and that outcome closes this
   request honestly.
2. **`snapstore-feb` — the M7 GC benchmark bar**, run on the qualifying
   box: 100k-node tree, ~30 GB, 50% garbage → full GC in <60s **while
   ingesting 200 MB/s**, p99 commit latency during GC < 2× the idle p99
   (IMPLEMENTATION-PLAN.md §M7 `BM:`). **Building the harness is in
   scope** — no GC benchmark exists today; build on the existing benches
   (`snapstore-pagestore/benches/{ingest,read_path}.rs`,
   `snapstore-store/benches/put_snapshot.rs`) for the tree generator and
   ingest driver, and record an idle-baseline run first so the "2× idle"
   denominator is durable. Report pass/fail per bar. A miss is acceptable
   **if** the resolution includes a bottleneck analysis (CPU/memory-bus vs
   disk vs lock contention — note the M5 analysis already attributed its
   misses to CPU/memory-bus, not disk) and a stated risk position for a
   4-hour Phase 5 soak (e.g. "GC keeps pace up to N MB/s sustained
   ingest").
3. **`snapstore-28z` — M5 benchmark re-validation** on the same box: the
   deferred rows from `docs/bench-baseline.md`, with their correct bars —
   PUT_BATCH ingest ≥1.5 GB/s sustained; GET_BATCH warm ≥2.5 GB/s;
   **16 parallel clients × 8 MiB deltas: p99 commit <40 ms, aggregate
   ≥1.2 GB/s** (the row most predictive of soak behavior — it measured
   p99 ~1.0 s on the reference box); and the fsync-bound
   CreateNode/UpdateNodes p50 rows the baseline defers to the same reference
   pass. Either the bars pass, or the shortfall is attributed (hardware
   ceiling vs code) with profile evidence. Close the bead only if all its
   deferred rows are measured; otherwise re-scope it explicitly.
4. **`snapstore-nn4` — fix the `page_channel_fallback` flake.** The bead
   records ~30–50% failure, "metrics-count assertions racing the fallback
   path," present before M7's changes. Determine whether it is a
   test-harness race or a real fallback-path race. If real, a correctness
   race on this load-bearing path warrants P0 and jumps ahead of items
   2–3.

## Suggested Sequencing (Yours To Overrule)

`nn4` first (no hardware dependency, and if it is a real race you want it
fixed before benchmarking the same path) → hardware preflight → `28z`
transport re-run (shorter, calibrates the box) → `feb` GC benchmark →
resolution write-up. Items 2–3 share box setup; do them in one lab session.

## Acceptance Criteria

1. A durable evidence root (`target/phase5-readiness-<timestamp>/`,
   same discipline as `target/m7-acceptance-20260703T063635Z/`) containing:
   hardware preflight record (hostname, disk class, baselines, free disk),
   the idle-baseline run, benchmark configs, raw numbers, and pass/fail
   against each plan bar.
2. `docs/bench-baseline.md` updated with a new hardware section and the
   measured rows — the beads name that file as the canonical record; the
   evidence root must not fork from it.
3. `snapstore-feb` and `snapstore-28z` beads closed or re-scoped with the
   measured numbers in their close reasons; `snapstore-nn4` closed with the
   root cause named (test bug vs real race).
4. `page_channel_fallback` green across 50 consecutive runs (the flake was
   ~30–50%, so 50 clean runs bounds it well below noise).
5. A one-paragraph risk statement in the resolution: can GC + transport
   sustain a 4-hour fork-heavy soak on this hardware, yes/no/conditional.

## Out Of Scope For This Request

- M8 (`snapstore-675`) — the joint hypervisor determinism-regression
  milestone; it has its own entry criteria and epic.
- M9 (`snapstore-agz`) — Phase 8 operability/backup.
- `snapstore-8qx` (vendored proto swap) — gated on control-plane publishing
  the crate; tracked separately.
- Any GC/transport *code* changes beyond what items 2–4 force. If a bar
  fails for a code reason, file the fix as its own bead and link it here;
  don't grow this request into a rewrite.
