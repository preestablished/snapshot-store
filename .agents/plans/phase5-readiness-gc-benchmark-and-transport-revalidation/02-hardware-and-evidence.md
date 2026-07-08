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
| `cpu-governor.txt` | `cpupower frequency-info` if available, plus `/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor` |
| `thermal.txt` | `sensors` if available, plus any CPU throttle lines from `dmesg` |
| `memory.txt` | `free -h` and `/proc/meminfo` |
| `mount.txt` | `findmnt -T "$SNAPSTORE_BENCH_ROOT"` |
| `df.txt` | `df -h "$SNAPSTORE_BENCH_ROOT"` |
| `lsblk.json` | `lsblk -J -o NAME,MODEL,SERIAL,TRAN,ROTA,TYPE,SIZE,MOUNTPOINT,FSTYPE` |
| `dirty-vm.txt` | `sysctl vm.dirty_ratio vm.dirty_background_ratio vm.dirty_expire_centisecs vm.dirty_bytes vm.dirty_background_bytes` |
| `phase5-host-attestation.txt` | Operator statement: actual Phase 5 soak host or surrogate, and whether it is the i5-8400/SATA reference box |
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
| CPU headroom | enough idle cores to run server, committer workers, and GC; record `lscpu`, load average, CPU governor, and thermal/throttle state |
| Disk baseline | fio sequential and random results recorded |
| Host identity | hostname, `phase5_soak_host`, `same_as_i5_sata_reference`, and whether this run is on the actual soak host or a surrogate |

If no qualifying box is reachable, stop after WI2. Write
`.agents/requests/phase5-readiness-gc-benchmark-and-transport-revalidation/04-resolution.md`
with the preflight data, file a P1 bead for blocked benchmarks, flag Matt, and
do not close `snapstore-feb` or `snapstore-28z` as passed.

If the operator confirms that the Phase 5 soak host is the i5-8400/SATA
reference box from `docs/bench-baseline.md`, stop after WI2 and file the
hardware escalation. The existing M5 misses were attributed to CPU/memory-bus
saturation as well as storage class, so an NVMe scratch mount on a surrogate is
not equivalent to proving the actual soak host.

## `evidence.json`

Assemble one JSON file with a stable top-level shape:

```json
{
  "run_id": "phase5-readiness-<UTC>",
  "request": ".agents/requests/phase5-readiness-gc-benchmark-and-transport-revalidation",
  "started_at": "<UTC RFC3339>",
  "finished_at": "<UTC RFC3339>",
  "git": {
    "rev": "<sha>",
    "status_clean": false,
    "status_short": "..."
  },
  "host": {
    "hostname": "...",
    "phase5_soak_host": "...",
    "same_as_i5_sata_reference": false,
    "actual_soak_host": true,
    "operator_attestation": "hardware/phase5-host-attestation.txt",
    "kernel": "...",
    "rustc": "..."
  },
  "bench_root": {
    "path": "...",
    "mount": "hardware/mount.txt",
    "free_bytes": 0
  },
  "hardware_qualification": {
    "qualified": false,
    "reason": "...",
    "disk_class": "...",
    "cpu_governor": "...",
    "thermal_or_throttle_notes": "..."
  },
  "commands": [
    {"id": "m5_transport", "argv": "...", "env": {}, "log": "m5-transport/page_channel_perf.log"}
  ],
  "artifacts": [
    {"path": "hardware/fio-seqwrite.json", "sha256": "..."}
  ],
  "bar_results": [
    {"id": "put_batch_warm_sustained", "target": ">= 1.5", "measured": 0.0, "unit": "GB/s", "status": "pass|fail|not_run", "attribution": "", "evidence_path": ""}
  ],
  "flake": {},
  "m5_transport": {},
  "m7_gc": {},
  "risk_statement": ""
}
```

The per-harness JSON files from WI3 and WI4 may contain richer raw data, but
the top-level `bar_results[]` array is the acceptance table. Include exact
commands and relevant env vars for every counted run. Include SHA-256 checksums
for raw artifacts that support pass/fail claims.

Keep raw command logs in `logs/`; keep parsed summaries in `evidence.json`.
