# WI2 - Hardware Preflight and Evidence Scaffold

No benchmark result counts until it is tied to an explicit host, filesystem, and
disk class. The request calls out an ambiguity: `docs/bench-baseline.md` names
an Intel i5-8400 SATA reference box, while Phase 5 also says "the Intel box".
Resolve whether those are the same machine.

## Evidence Root

Create a reusable script, `scripts/phase5-readiness-evidence.sh`, following the
style of `scripts/m7-evidence.sh`. It should create:

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

The script should support:

| Env var | Meaning | Default |
|---|---|---|
| `SNAPSTORE_BENCH_ROOT` | Parent directory for all scratch stores and fio files | required |
| `PHASE5_EVIDENCE_ROOT` | Existing evidence root to append to | new timestamp |
| `RUN_M5` | Run M5 transport revalidation | `0` |
| `RUN_M7_GC` | Run M7 GC benchmark | `0` |
| `RUN_FLAKE_50X` | Run 50x fallback verification | `0` |

Use `tempfile::Builder::tempdir_in` in Rust harnesses so every large benchmark
uses `SNAPSTORE_BENCH_ROOT`; `TempDir::new()` may land on `/tmp` or another
filesystem and must not be used for counted measurements.

## Preflight Record

Under `hardware/`, record:

| File | Command or source |
|---|---|
| `hostname.txt` | `hostname --fqdn || hostname` |
| `git.txt` | `git rev-parse HEAD`, `git status --short --branch` |
| `kernel.txt` | `uname -a` |
| `rustc.txt` | `rustc -Vv` |
| `cpu.txt` | `lscpu` |
| `memory.txt` | `free -h` and `/proc/meminfo` |
| `mount.txt` | `findmnt -T "$SNAPSTORE_BENCH_ROOT"` |
| `df.txt` | `df -h "$SNAPSTORE_BENCH_ROOT"` |
| `lsblk.json` | `lsblk -J -o NAME,MODEL,SERIAL,TRAN,ROTA,TYPE,SIZE,MOUNTPOINT,FSTYPE` |
| `dirty-vm.txt` | `sysctl vm.dirty_ratio vm.dirty_background_ratio vm.dirty_expire_centisecs vm.dirty_bytes vm.dirty_background_bytes` |
| `fio-*.json` | Sequential and random read/write baselines |

Recommended fio commands:

```bash
fio --name=phase5-seqwrite --directory "$SNAPSTORE_BENCH_ROOT" --rw=write \
  --bs=1M --size=8G --iodepth=32 --numjobs=1 --direct=1 --runtime=60 \
  --time_based --group_reporting --output-format=json \
  --output "$EVIDENCE_ROOT/hardware/fio-seqwrite.json"

fio --name=phase5-seqread --directory "$SNAPSTORE_BENCH_ROOT" --rw=read \
  --bs=1M --size=8G --iodepth=32 --numjobs=1 --direct=1 --runtime=60 \
  --time_based --group_reporting --output-format=json \
  --output "$EVIDENCE_ROOT/hardware/fio-seqread.json"

fio --name=phase5-randrw --directory "$SNAPSTORE_BENCH_ROOT" --rw=randrw \
  --rwmixread=70 --bs=4k --size=4G --iodepth=64 --numjobs=4 --direct=1 \
  --runtime=60 --time_based --group_reporting --output-format=json \
  --output "$EVIDENCE_ROOT/hardware/fio-randrw.json"
```

If `fio` is missing and cannot be installed by the operator, record that fact in
`hardware/fio-unavailable.txt` and use conservative `dd`/`sync` numbers as
fallback evidence. Mark benchmark qualification as inconclusive unless disk
class is clear from `lsblk`.

## Qualification Gate

A qualifying run must have:

| Requirement | Minimum evidence |
|---|---|
| NVMe-class target | `lsblk` shows `TRAN=nvme`, or operator-provided proof that the selected mount is the intended Phase 5 NVMe device |
| Free space | at least 70 GiB free at `SNAPSTORE_BENCH_ROOT`; 100 GiB preferred |
| CPU headroom | enough idle cores to run server, committer workers, and GC; record `lscpu` and load average |
| Disk baseline | fio sequential and random results recorded |
| Host identity | hostname and whether this is the Phase 5 soak host |

If no qualifying box is reachable, stop after WI2. Write
`.agents/requests/phase5-readiness-gc-benchmark-and-transport-revalidation/04-resolution.md`
with the preflight data, file a P1 bead for blocked benchmarks, flag Matt, and
do not close `snapstore-feb` or `snapstore-28z` as passed.

## `evidence.json`

Assemble one JSON file with:

| Field | Contents |
|---|---|
| `request` | request directory path |
| `git_rev` | `git rev-parse HEAD` |
| `git_status_clean` | boolean plus raw status |
| `host` | hostname, kernel, rustc |
| `bench_root` | absolute `SNAPSTORE_BENCH_ROOT` and mount info |
| `hardware_qualified` | boolean and reason |
| `flake` | root cause and 50x status, if run |
| `m5_transport` | measured rows and pass/fail, if run |
| `m7_gc` | measured rows and pass/fail, if run |
| `risk_statement` | final yes/no/conditional soak posture |

Keep raw command logs in `logs/`; keep parsed summaries in `evidence.json`.
