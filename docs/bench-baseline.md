# Bench baseline — reference hardware record

Hardware rule (plan 05, G1 precedent): transport-/CPU-/page-cache-bound
rows gate at spec values on any hardware; disk- and fsync-bound rows gate
at the fio/G1-derived floor of this machine, with spec values re-validated
on NVMe-class hardware before M8 sign-off.

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

## Gate S3 — M4 benchmarks

| BM | Spec target | Gate here | Measured | Status |
|---|---|---|---|---|
| PutPages over UDS gRPC, 256-page msgs, dedup-warm | ≥ 600 MB/s | spec as-is (transport+hash bound) | _pending_ | |
| PutPages cold (disk-bound) | — | informational vs G1 (~450 MiB/s ceiling) | _pending_ | |
| QueryNodes page of 1,000 over UDS | p50 < 4 ms | spec as-is | _pending_ | |
| PutSnapshot already-paged (2k-entry delta) | p50 < 3 ms | fsync-bound: record actual | _pending_ | |
| CreateNode + 16 KiB inline log | p50 < 1.5 ms | fsync-bound: record actual | _pending_ | |
| UpdateNodes(256) | p50 < 3 ms | fsync-bound: record actual | _pending_ | |
| flatten 64-deep × 2k-entry chain (warm) | < 2 ms | spec as-is (pure CPU) | _pending_ | |
| library-layer warm read, 8 threads | ≥ 2.5 GB/s | spec as-is (page-cache bound) | _pending_ | |

## Gate S4 — M5 benchmarks

| BM | Spec target | Gate here | Measured | Status |
|---|---|---|---|---|
| PUT_BATCH dedup-warm | ≥ 1.5 GB/s sustained | spec as-is | _pending_ | |
| PUT_BATCH cold sustained | (NVMe number) | ≥ 400 MiB/s (G1-consistent) | _pending_ | |
| GET_BATCH warm (page cache) | ≥ 2.5 GB/s | spec as-is | _pending_ | |
| 16 clients × 8 MiB deltas, p99 commit incl. fsync | < 40 ms | fsync-bound: record actual | _pending_ | |
| 16 clients aggregate ingest (dedup-warm) | ≥ 1.2 GB/s | spec as-is | _pending_ | |

NVMe re-validation of every fsync-/disk-bound row is an M8 entry item.
