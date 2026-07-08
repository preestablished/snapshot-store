# Phase 5 Readiness Plan - GC Benchmark and Transport Revalidation

Plan for `.agents/requests/phase5-readiness-gc-benchmark-and-transport-revalidation/`,
filed 2026-07-07 by `exploration-orchestrator`.

Written for a coding agent starting from the current repo state. The request is
not asking for new GC correctness work; M7 correctness already passed. This plan
covers the deferred benchmark and transport evidence, plus the flaky
page-channel fallback test on the same load-bearing path.

Tracking beads:

| Bead | Purpose | Plan file |
|---|---|---|
| `snapstore-nn4` | Fix/root-cause `page_channel_fallback` flake | `01-page-channel-flake.md` |
| `snapstore-28z` | Re-run deferred M5 transport rows on qualifying hardware | `03-transport-revalidation.md` |
| `snapstore-feb` | Run M7 GC benchmark bar under concurrent ingest | `04-gc-benchmark.md` |

Plan artifact bead: `snapstore-8mi`.

## Current State

Important anchors verified on `main` during planning:

| Surface | Anchor |
|---|---|
| Existing M5 perf harness | `crates/snapstore-server/tests/page_channel_perf.rs:42` |
| Existing M5 result table and hardware posture | `docs/bench-baseline.md:39` |
| Flaky fallback metric assertions | `crates/snapstore-client/tests/page_channel_fallback.rs:260`, `:330`, `:357`, `:460` |
| Page-channel server increments GET metric after sending the reply | `crates/snapstore-server/src/page_channel.rs:583` |
| GC cycle API and report fields | `crates/snapstore-server/src/gc.rs:28`, `:44`, `:170` |
| `TriggerGc` RPC semantics | `crates/snapstore-server/src/service.rs:1077` |
| Store commit path and group-commit barrier | `crates/snapstore-store/src/lib.rs:405` |
| Test/bench manifest builders | `crates/snapstore-store/src/lib.rs:1026` |
| Meta create/prune APIs | `crates/snapstore-meta/src/lib.rs:190`, `:322` |
| Existing M7 evidence script precedent | `scripts/m7-evidence.sh` |

## Sequencing

1. **WI1 - `snapstore-nn4` flake first.** It has no hardware dependency and
   sits on the transport path used by the benchmarks. If it is a real race,
   file/raise a P0 and do not trust performance numbers until the race is
   addressed.
2. **WI2 - hardware preflight and evidence scaffold.** Establish the exact host,
   filesystem, disk class, free space, and baseline I/O before any benchmark
   number can count.
3. **WI3 - `snapstore-28z` transport revalidation.** Extend the existing ignored
   perf test only as needed for durable evidence, then run it on the same
   filesystem selected by WI2.
4. **WI4 - `snapstore-feb` GC benchmark.** Add the large one-off harness, run an
   idle commit-latency baseline, then run GC while sustaining 200 MB/s ingest.
5. **WI5 - handback.** Update `docs/bench-baseline.md`, write
   `.agents/requests/.../04-resolution.md`, close or rescope beads with measured
   numbers, and push.

## Output Shape

The implementation should produce one evidence root:

```text
target/phase5-readiness-<UTC>/
  evidence.json
  hardware/
  logs/
  raw/
  m5-transport/
  m7-gc-benchmark/
  flake/
```

`docs/bench-baseline.md` is the canonical human-readable benchmark record.
`evidence.json` is the durable machine-readable record. They must agree; do not
leave raw evidence in `target/` without updating the doc.

## Pass/Fail Rules

The request accepts a measured and explained miss. It does not accept an
unmeasured assumption.

| Row | Pass bar | If it misses |
|---|---:|---|
| `page_channel_fallback` | 50 consecutive green runs | Name root cause; if real transport race, file P0 and pause perf work |
| PUT_BATCH warm sustained | `>= 1.5 GB/s` | Attribute with profile evidence |
| GET_BATCH warm sustained | `>= 2.5 GB/s` | Attribute with profile evidence |
| 16 clients x 8 MiB commits | p99 `< 40 ms`, aggregate `>= 1.2 GB/s` | Attribute disk/fsync vs CPU/memory vs code |
| CreateNode / UpdateNodes p50 | `< 1.5 ms` / `< 3 ms` on qualifying hardware | Attribute SQLite/fsync/actor bottleneck |
| M7 GC | 100k nodes, ~30 GB physical, ~50% garbage, full GC `< 60 s` under 200 MB/s ingest | State max sustainable ingest or bottleneck |
| Commit latency during GC | p99 `< 2 x idle p99` | State soak risk and mitigation |

## Non-Goals

Do not expand this into M8 (`snapstore-675`), M9 (`snapstore-agz`), or the
vendored proto swap (`snapstore-8qx`). If a benchmark miss requires code
optimization beyond local harness/test fixes, file a separate bead and link it
from the resolution.
