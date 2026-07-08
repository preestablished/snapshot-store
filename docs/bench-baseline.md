# Bench baseline — reference hardware record

Hardware rule (plan 05, G1 precedent): transport-/CPU-/page-cache-bound
rows gate at spec values on any hardware; disk- and fsync-bound rows gate
at the fio/G1-derived floor of this machine, with spec values re-validated
on NVMe-class hardware before M8 sign-off.

Phase 5 readiness note (2026-07-08): local preflight evidence on
`infra-control` did **not** supersede that M8/NVMe deferral. The selected
scratch root resolved to the existing SATA-backed root filesystem and no
operator attestation identified it as the actual Phase 5 soak host. See
`target/phase5-readiness-20260708T180021Z/evidence.json`.

## Machine identity (the Intel/SATA reference box)

| | |
|---|---|
| CPU | Intel Core i5-8400 @ 2.80 GHz (6 cores) |
| RAM | 31 GiB |
| Disk | Samsung SSD 860 (SATA, non-rotational), LVM volume `ubuntu--vg--1-ubuntu--lv` |
| Kernel | Linux 6.8.0-124-generic |
| rustc | 1.96.0 (ac68faa20 2026-05-25) |
| vm.dirty_ratio / background / expire | 20 / 10 / 3000 |

Phase-1 anchor: G1 ingest measured ~461 MiB/s on this box (gate lowered
from the NVMe-class 1.5 GB/s to 400 MiB/s, commit bd3139b). The
disk-bound floor for phase-2 rows is G1-consistent: **≥ 400 MiB/s** for
sustained cold ingest; fsync-bound latency rows record actuals against
the SATA flush floor.

All measurements 2026-06-10, release builds, this machine.

## Gate S3 — M4 benchmarks

| BM | Spec target | Gate here | Measured | Status |
|---|---|---|---|---|
| PutPages over UDS gRPC, 256-page msgs, dedup-warm | ≥ 600 MB/s | spec as-is (transport+hash bound) | ~670 MB/s median (630–706) | **MET** |
| PutPages cold (disk-bound) | — | informational vs G1 (~450 MiB/s ceiling) | covered by 16-client cold row below | recorded |
| QueryNodes page of 1,000 over UDS | p50 < 4 ms | spec as-is | ~3.3 ms median (3.2–3.5) | **MET** |
| PutSnapshot already-paged (2k-entry delta) | p50 < 3 ms | fsync-bound: record actual | 369 µs (idempotent fast path) | **MET** |
| flatten 64-deep × 2k-entry chain (warm) | < 2 ms | spec as-is (pure CPU) | 1.13 ms median | **MET** |
| library-layer warm read, 8 threads | ≥ 2.5 GB/s | spec as-is (page-cache bound) | see GET_BATCH analysis below | see S4 note |
| CreateNode + 16 KiB inline log / UpdateNodes(256) | p50 < 1.5 ms / < 3 ms | fsync-bound: record actual | e2e sustains the mixed op stream at ~33 steps/s/experiment incl. all fsyncs; dedicated criterion rows deferred to the M8 NVMe pass | recorded |

## Gate S4 — M5 benchmarks

| BM | Spec target | Gate here | Measured | Status |
|---|---|---|---|---|
| PUT_BATCH dedup-warm, single stream | (round-trip latency bound) | informational | 0.90 GB/s | recorded |
| PUT_BATCH dedup-warm, sustained (4 streams) | ≥ 1.5 GB/s | spec | 0.89 GB/s | **MISS at spec — see analysis** |
| GET_BATCH warm, sustained (4 streams) | ≥ 2.5 GB/s | spec | 0.64 GB/s | **MISS at spec — see analysis** |
| PUT_BATCH cold sustained (disk-bound) | (NVMe number) | ≥ 400 MiB/s (G1-consistent) | 16-client cold aggregate 190 MB/s — but that row includes per-commit PutSnapshot manifest fsyncs, not pure streaming ingest; pure-ingest cold remains G1's ~461 MiB/s | recorded |
| 16 clients × 8 MiB deltas, p99 commit incl. fsync | < 40 ms | fsync-bound: record actual = floor | p50 ~650 ms, p99 ~1.0 s (each wave writes 128 MiB cold + group fsync on SATA) | **floor recorded; NVMe at M8** |
| 16 clients aggregate ingest (dedup-warm) | ≥ 1.2 GB/s | spec | bounded by the same sustained-warm ceiling: 0.89 GB/s | **MISS at spec — see analysis** |

### Analysis of the transport-row misses (decided posture, not a deferral by neglect)

The transport-bound rows were optimized to the hardware ceiling: vectored
`pwritev` from caller pages into the memfd (zero staging copy), server-side
`mmap` of the seal-verified memfd (zero receive copy), single-pread record
reads with zero-copy payload slices, rayon-parallel hashing on both halves,
per-pack handle reuse, and pipelined GET datagrams. Gains: PUT 0.51 →
0.90 GB/s, GET 0.38 → 0.64 GB/s. Throughput no longer scales with extra
streams, i.e. the box itself — a 2017 6-core desktop with dual-channel
DDR4 — is saturated (every 32 MiB batch is hashed twice, BLAKE3-verified
per record on GET, and crosses the memory bus several times; the per-batch
memfd create/fault/teardown adds a fixed kernel cost spec-class hardware
absorbs).

The 1.5 / 2.5 GB/s numbers assume the NVMe-class reference box
(ARCHITECTURE §7.1) — which this machine also is not for CPU/memory
purposes, exactly the situation of the phase-1 G1 precedent (1.5 GB/s
spec → 400 MiB/s gate on this box). Posture: actuals recorded here as
this machine's measured ceiling; **spec-value validation of the
transport rows moves to the M8 entry items on NVMe-class hardware,
alongside the fsync-bound rows**. M5 sign-off on this box carries the
two rows as open hardware-bound risks, explicitly flagged to the program
(beads follow-up filed), not silently passed.

## Phase 5 Readiness Preflight - infra-control, 2026-07-08

Evidence root: `target/phase5-readiness-20260708T180021Z/`

| | |
|---|---|
| Host | `infra-control` |
| Scratch root | `target/phase5-local-scratch` |
| Mount | `/` on `/dev/mapper/ubuntu--vg--1-ubuntu--lv`, ext4 |
| Disk class | SATA (`lsblk` transport), not NVMe-class |
| Free space | 905 GiB reported by `df`; 971,442,429,952 bytes in `evidence.json` |
| Fio baseline | Not run in this local preflight (`RUN_FIO=0`) |
| Soak-host attestation | Unset (`actual_soak_host=UNSET`, `same_as_i5_sata_reference=UNSET`) |
| rustc/kernel | Recorded under `hardware/rustc.txt` and `hardware/kernel.txt` |

The Phase 5 local run fixed and verified the `page_channel_fallback`
observability flake, and added evidence-grade harness support for the M5 and
M7 rows, but it did not run the hardware-gated benchmark bars. The evidence
qualification is therefore `qualified=false`: the selected mount is SATA and
there is no proof this is the actual Phase 5 soak host.

| Area | Target | Measured | Status | Evidence |
|---|---:|---:|---|---|
| `page_channel_fallback` | 50 consecutive green runs | 50 runs, 0 failures | **MET** | `flake/postfix-50x-summary.txt` |
| PUT_BATCH warm sustained | ≥ 1.5 GB/s | not run | hardware blocked | `m5-transport/not-run.txt` |
| GET_BATCH warm sustained | ≥ 2.5 GB/s | not run | hardware blocked | `m5-transport/not-run.txt` |
| 16 clients × 8 MiB p99 / aggregate | < 40 ms / ≥ 1.2 GB/s | not run | hardware blocked | `m5-transport/not-run.txt` |
| CreateNode / UpdateNodes p50 | < 1.5 ms / < 3 ms | not run | hardware blocked | `m5-transport/not-run.txt` |
| M7 GC reclaiming cycle | < 60 s under 200 MB/s ingest, p99 < 2× idle | not run | hardware blocked | `m7-gc-benchmark/not-run.txt` |

Harness changes landed for the eventual qualified run:
`scripts/phase5-readiness-evidence.sh` records hardware evidence and assembles
`evidence.json`; `page_channel_perf` now requires `SNAPSTORE_BENCH_ROOT` and
can write `SNAPSTORE_M5_BENCH_JSON`; `gc_readiness_bench` is an ignored
release test for the 100k-node GC bar and can write `SNAPSTORE_GC_BENCH_JSON`.
Run those on a qualifying NVMe-class soak host before closing
`snapstore-28z` or `snapstore-feb`.

## Gate S5 — crash suite (for the record)

1,000 randomized kill cycles + failpoint matrix (9 boundaries) × 50
(450 kills) + SQLite batch-atomicity × 200 + full-stack server-process
scenario × 10: zero invariant violations, zero fsck violations.
~226 s wall for the 1,000-cycle + matrix run.

## Gate S1/S2 (for the record)

- S1: e2e 10k steps × 2 concurrent experiments through the public API:
  PASSED (~5 min, release).
- S2: PROPTEST_CASES=4096 manifest suite green; new golden vector
  committed with the format change; fuzz target 4.4 M execs/16 s clean
  locally, 10-minute run wired into nightly CI.
