# Resolution: Phase 5 Readiness GC Benchmark and Transport Revalidation

## Summary

Local work fixed the `page_channel_fallback` flake, added evidence-grade M5 and
M7 benchmark harnesses, and produced a Phase 5 evidence root with hardware
preflight. The local host did not qualify for counted transport or GC bars:
`SNAPSTORE_BENCH_ROOT` resolved to the SATA-backed root filesystem on
`infra-control`, and no operator attestation identified this machine as the
actual Phase 5 soak host. The M5 and M7 performance questions remain open until
they are run on a qualified NVMe-class soak host.

## Commits

| SHA | Contents |
|---|---|
| this commit | Flake fix, M5/M7 benchmark harnesses, Phase 5 evidence script, baseline/resolution docs |

## Evidence

Evidence root: `target/phase5-readiness-20260708T180021Z/`

| Area | Result |
|---|---|
| Hardware preflight | `qualified=false`; host `infra-control`; scratch mount `/` on `/dev/mapper/ubuntu--vg--1-ubuntu--lv`; disk class SATA; 905 GiB free; fio skipped locally with `RUN_FIO=0`; soak-host attestation unset |
| `snapstore-nn4` | Fixed as a test observability race; 50 consecutive `page_channel_fallback` runs green |
| `snapstore-28z` | Harness updated, JSON output added, `SNAPSTORE_BENCH_ROOT` enforced; counted rows not run because hardware did not qualify |
| `snapstore-feb` | New ignored `gc_readiness_bench` harness added and smoke-run at tiny scale; full 100k-node/30 GB/200 MB/s bar not run because hardware did not qualify |

## Per-Bar Results

| Bar | Target | Measured | Status |
|---|---:|---:|---|
| `page_channel_fallback` | 50 green runs | 50 runs, 0 failures | pass |
| PUT_BATCH warm sustained | >= 1.5 GB/s | not run | hardware blocked |
| GET_BATCH warm sustained | >= 2.5 GB/s | not run | hardware blocked |
| 16 clients x 8 MiB p99 | < 40 ms | not run | hardware blocked |
| 16 clients aggregate | >= 1.2 GB/s | not run | hardware blocked |
| CreateNode + inline log p50 | < 1.5 ms | not run | hardware blocked |
| UpdateNodes(256) p50 | < 3 ms | not run | hardware blocked |
| M7 reclaiming GC | < 60 s under 200 MB/s ingest | not run | hardware blocked |
| Commit p99 during reclaiming GC | < 2 x idle p99 | not run | hardware blocked |

## Bead Disposition

| Bead | Disposition |
|---|---|
| `snapstore-nn4` | Closed: root cause was a test-harness metric race; evidence `target/phase5-readiness-20260708T180021Z/flake/postfix-50x-summary.txt` |
| `snapstore-28z` | Leave open / hardware-blocked. All deferred rows still need a qualified NVMe-class soak-host run. |
| `snapstore-feb` | Leave open / hardware-blocked. Full 100k-node M7 benchmark still needs a qualified NVMe-class soak-host run. |
| `snapstore-ba6` | New P1 hardware escalation for a qualified NVMe-class Phase 5 benchmark host. `snapstore-28z` and `snapstore-feb` now depend on it. |

## Phase 5 Soak Risk

Phase 5 should not treat GC or transport throughput as proven on the soak host
from this local run. The flake on the page-channel fallback path is fixed, and
the harnesses are ready, but the actual throughput and GC pacing risk remains
conditional on a qualifying hardware session with operator attestation and fio
baselines.

## Local Verification

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets -- -D warnings` | pass |
| `cargo test --workspace` | pass |
| `cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1` | pass |
| `cargo test -p snapstore-server --test page_channel_perf --test gc_readiness_bench --release --no-run` | pass |
| `RUN_FLAKE_50X=1 RUN_FIO=0 RUN_M5=0 RUN_M7_GC=0 scripts/phase5-readiness-evidence.sh` | pass; wrote evidence root above |
| scaled `gc_readiness_bench` smoke (`20` nodes, `0.001` GiB, `1` second idle) | pass; control-flow only, not acceptance evidence |

## Sync Notes

`bd dolt pull` failed on this branch because the Dolt remote needs an explicit
branch. `bd` auto-push also reported the known `snapstore-pov` remote
divergence: `push to origin/main: Error 1105: unknown push error; no common
ancestor`.
