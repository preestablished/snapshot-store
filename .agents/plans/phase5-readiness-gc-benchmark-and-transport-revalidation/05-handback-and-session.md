# WI5 - Resolution, Beads, and Session Close

This work item is complete only when the evidence is durable, the request has a
resolution, the relevant beads are closed or explicitly rescoped, and all git
changes are pushed.

## Request Resolution

Write:

```text
.agents/requests/phase5-readiness-gc-benchmark-and-transport-revalidation/04-resolution.md
```

Use this shape:

```markdown
# Resolution: Phase 5 Readiness GC Benchmark and Transport Revalidation

## Summary

<one paragraph: pass/fail/conditional>

## Commits

| SHA | Contents |
|---|---|

## Evidence

Evidence root: `target/phase5-readiness-<UTC>/`

| Area | Result |
|---|---|
| Hardware preflight | ... |
| `snapstore-nn4` | ... |
| `snapstore-28z` | ... |
| `snapstore-feb` | ... |

## Per-Bar Results

| Bar | Target | Measured | Status |
|---|---:|---:|---|

## Bead Disposition

| Bead | Disposition |
|---|---|

## Phase 5 Soak Risk

<one paragraph: yes/no/conditional, with any required throttling or hardware caveat>
```

If WI2 dead-ends because no qualifying hardware is reachable, the same
`04-resolution.md` carries the preflight numbers and escalation. In that case,
do not fabricate benchmark pass/fail rows.

## `docs/bench-baseline.md`

Add one new section with the qualified host and date. Do not edit the old
2026-06-10 SATA results except to link forward to the new revalidation section.

Required contents:

| Contents | Source |
|---|---|
| hostname, CPU, RAM, disk, kernel, rustc | WI2 hardware files |
| fio baseline | WI2 fio JSON |
| M5 transport table | WI3 results JSON |
| M7 GC benchmark table | WI4 results JSON |
| bottleneck analysis | WI3/WI4 profile evidence |
| Phase 5 soak posture | final risk paragraph |

## Bead Updates

Use close reasons with measured numbers:

```bash
bd close snapstore-nn4 --reason="page_channel_fallback root cause: <test race or real race>; 50 consecutive runs green; evidence <path>"
bd close snapstore-28z --reason="NVMe revalidation on <host>: PUT <x> GB/s, GET <y> GB/s, 16x8MiB p99 <z> ms agg <a> GB/s, CreateNode p50 <b> ms, UpdateNodes p50 <c> ms; evidence <path>"
bd close snapstore-feb --reason="M7 BM on <host>: 100k nodes, <gb> GB, <garbage>% garbage, GC <s>s under <mbps> MB/s ingest, p99 <during>/<idle> ms; evidence <path>"
```

If a benchmark misses but is measured and explained, either close with the miss
and risk posture if the request owner accepts that outcome, or rescope/file a
follow-up bead for the optimization. Do not close a bead as passed when a named
row is unmeasured.

Possible follow-up beads:

| Condition | New bead |
|---|---|
| No qualifying hardware | P1 hardware escalation for Phase 5 readiness benchmarks |
| Real page-channel race | P0 transport correctness bug |
| Transport row misses due to code | P1/P2 optimization with profile evidence |
| GC cannot keep pace | P1 GC pacing/optimization or orchestrator throttling handoff |

## Quality Gates

Run the relevant local gates before committing:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1
cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture
```

The full GC benchmark is hardware/lab gated; run it only on the qualified box
with `SNAPSTORE_BENCH_ROOT` set. If the local machine is not qualified, record
that the full lab run was not performed locally and point to the lab evidence.

## Commit and Push

Follow the repo protocol:

```bash
git status --short --branch
git add <changed files>
git commit -m "Record phase5 readiness GC and transport evidence"
git pull --rebase
bd dolt push
git push
git status --short --branch
```

`bd dolt push` is currently tracked by `snapstore-pov` and may fail with
`no common ancestor`. Record the failure if it persists, but still complete the
normal `git push` unless a real git conflict blocks it.
