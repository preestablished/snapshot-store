#!/usr/bin/env bash
# Phase 5 readiness evidence runner.
#
# Produces target/phase5-readiness-<UTC>/ with hardware preflight files,
# optional M5/M7/flake runs, and a top-level evidence.json.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${SNAPSTORE_BENCH_ROOT:-}" ]]; then
  echo "SNAPSTORE_BENCH_ROOT is required" >&2
  exit 2
fi
mkdir -p "$SNAPSTORE_BENCH_ROOT"
SNAPSTORE_BENCH_ROOT="$(cd "$SNAPSTORE_BENCH_ROOT" && pwd -P)"
export SNAPSTORE_BENCH_ROOT

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
export PHASE5_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
EVIDENCE_ROOT="${PHASE5_EVIDENCE_ROOT:-target/phase5-readiness-${STAMP}}"
RUN_M5="${RUN_M5:-0}"
RUN_M7_GC="${RUN_M7_GC:-0}"
RUN_FLAKE_50X="${RUN_FLAKE_50X:-0}"
RUN_FIO="${RUN_FIO:-1}"

mkdir -p \
  "$EVIDENCE_ROOT/hardware" \
  "$EVIDENCE_ROOT/logs" \
  "$EVIDENCE_ROOT/raw" \
  "$EVIDENCE_ROOT/m5-transport" \
  "$EVIDENCE_ROOT/m7-gc-benchmark" \
  "$EVIDENCE_ROOT/flake"

echo "== Phase 5 readiness evidence -> $EVIDENCE_ROOT"

run_capture() {
  local out="$1"
  shift
  set +e
  "$@" >"$out" 2>&1
  local status=$?
  set -e
  return "$status"
}

run_shell_capture() {
  local out="$1"
  shift
  set +e
  bash -lc "$*" >"$out" 2>&1
  local status=$?
  set -e
  return "$status"
}

echo "== [1/5] hardware preflight"
run_capture "$EVIDENCE_ROOT/hardware/hostname.txt" hostname --fqdn || hostname >"$EVIDENCE_ROOT/hardware/hostname.txt"
{
  git rev-parse HEAD
  git status --short --branch
} >"$EVIDENCE_ROOT/hardware/git.txt"
run_capture "$EVIDENCE_ROOT/hardware/kernel.txt" uname -a || true
run_capture "$EVIDENCE_ROOT/hardware/rustc.txt" rustc -Vv || true
run_capture "$EVIDENCE_ROOT/hardware/cpu.txt" lscpu || true
{
  if command -v cpupower >/dev/null 2>&1; then
    cpupower frequency-info || true
  else
    echo "cpupower unavailable"
  fi
  for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [[ -e "$f" ]] && printf '%s: %s\n' "$f" "$(cat "$f")"
  done
} >"$EVIDENCE_ROOT/hardware/cpu-governor.txt" 2>&1
{
  if command -v sensors >/dev/null 2>&1; then
    sensors || true
  else
    echo "sensors unavailable"
  fi
  dmesg 2>/dev/null | grep -Ei 'thrott|thermal' || true
} >"$EVIDENCE_ROOT/hardware/thermal.txt"
{
  free -h || true
  cat /proc/meminfo || true
} >"$EVIDENCE_ROOT/hardware/memory.txt"
run_capture "$EVIDENCE_ROOT/hardware/mount.txt" findmnt -T "$SNAPSTORE_BENCH_ROOT" || true
run_capture "$EVIDENCE_ROOT/hardware/df.txt" df -h "$SNAPSTORE_BENCH_ROOT" || true
run_capture "$EVIDENCE_ROOT/hardware/lsblk.json" lsblk -J -o NAME,MODEL,SERIAL,TRAN,ROTA,TYPE,SIZE,MOUNTPOINT,FSTYPE || true
run_capture "$EVIDENCE_ROOT/hardware/dirty-vm.txt" sysctl vm.dirty_ratio vm.dirty_background_ratio vm.dirty_expire_centisecs vm.dirty_bytes vm.dirty_background_bytes || true
cat >"$EVIDENCE_ROOT/hardware/phase5-host-attestation.txt" <<EOF
phase5_soak_host=${PHASE5_SOAK_HOST:-UNSET}
actual_soak_host=${PHASE5_ACTUAL_SOAK_HOST:-UNSET}
same_as_i5_sata_reference=${PHASE5_SAME_AS_I5_SATA_REFERENCE:-UNSET}
operator_attestation=${PHASE5_OPERATOR_ATTESTATION:-UNSET}
EOF

if [[ "$RUN_FIO" == "1" ]]; then
  if command -v fio >/dev/null 2>&1; then
    echo "== fio baselines"
    fio --name=phase5-seqwrite --directory "$SNAPSTORE_BENCH_ROOT" --rw=write \
      --bs=1M --size="${PHASE5_FIO_SEQ_SIZE:-8G}" --iodepth=32 --numjobs=1 --direct=1 \
      --runtime="${PHASE5_FIO_RUNTIME:-60}" --time_based --group_reporting --output-format=json \
      --output "$EVIDENCE_ROOT/hardware/fio-seqwrite.json" || true
    fio --name=phase5-seqread --directory "$SNAPSTORE_BENCH_ROOT" --rw=read \
      --bs=1M --size="${PHASE5_FIO_SEQ_SIZE:-8G}" --iodepth=32 --numjobs=1 --direct=1 \
      --runtime="${PHASE5_FIO_RUNTIME:-60}" --time_based --group_reporting --output-format=json \
      --output "$EVIDENCE_ROOT/hardware/fio-seqread.json" || true
    fio --name=phase5-randrw --directory "$SNAPSTORE_BENCH_ROOT" --rw=randrw \
      --rwmixread=70 --bs=4k --size="${PHASE5_FIO_RAND_SIZE:-4G}" --iodepth=64 --numjobs=4 --direct=1 \
      --runtime="${PHASE5_FIO_RUNTIME:-60}" --time_based --group_reporting --output-format=json \
      --output "$EVIDENCE_ROOT/hardware/fio-randrw.json" || true
  else
    {
      echo "fio unavailable"
      echo "dd write fallback:"
      dd if=/dev/zero of="$SNAPSTORE_BENCH_ROOT/phase5-dd-fallback.tmp" bs=1M count=1024 oflag=direct conv=fdatasync || true
      rm -f "$SNAPSTORE_BENCH_ROOT/phase5-dd-fallback.tmp"
    } >"$EVIDENCE_ROOT/hardware/fio-unavailable.txt" 2>&1
  fi
else
  echo "RUN_FIO=0" >"$EVIDENCE_ROOT/hardware/fio-skipped.txt"
fi

if [[ "$RUN_FLAKE_50X" == "1" ]]; then
  echo "== [2/5] page_channel_fallback 50x"
  cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1 \
    2>&1 | tee "$EVIDENCE_ROOT/flake/page_channel_fallback.log"
  : >"$EVIDENCE_ROOT/flake/postfix-50x.log"
  for i in $(seq 1 50); do
    cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1 \
      >>"$EVIDENCE_ROOT/flake/postfix-50x.log" 2>&1
  done
  printf 'runs=50\nfailures=0\ncommand=cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1\n' \
    >"$EVIDENCE_ROOT/flake/postfix-50x-summary.txt"
  echo "Root cause: test observability race. The client could receive GET_BATCH_DATA before the server-side GET_BATCH metric increment became visible; positive metric assertions now poll for the increment." \
    >"$EVIDENCE_ROOT/flake/root-cause.txt"
else
  echo "RUN_FLAKE_50X=0" >"$EVIDENCE_ROOT/flake/not-run.txt"
fi

if [[ "$RUN_M5" == "1" ]]; then
  echo "== [3/5] M5 transport revalidation"
  if command -v pidstat >/dev/null 2>&1; then
    pidstat -durh 1 >"$EVIDENCE_ROOT/m5-transport/pidstat.log" 2>&1 &
    PIDSTAT_PID=$!
  else
    echo "pidstat unavailable" >"$EVIDENCE_ROOT/m5-transport/pidstat-unavailable.txt"
    PIDSTAT_PID=""
  fi
  if command -v iostat >/dev/null 2>&1; then
    iostat -xz 1 >"$EVIDENCE_ROOT/m5-transport/iostat.log" 2>&1 &
    IOSTAT_PID=$!
  else
    echo "iostat unavailable" >"$EVIDENCE_ROOT/m5-transport/iostat-unavailable.txt"
    IOSTAT_PID=""
  fi
  set +e
  SNAPSTORE_BENCH_ROOT="$SNAPSTORE_BENCH_ROOT" \
  SNAPSTORE_M5_BENCH_JSON="$EVIDENCE_ROOT/m5-transport/results.json" \
  cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture \
    2>&1 | tee "$EVIDENCE_ROOT/m5-transport/page_channel_perf.log"
  M5_STATUS=${PIPESTATUS[0]}
  set -e
  [[ -n "${PIDSTAT_PID:-}" ]] && kill "$PIDSTAT_PID" 2>/dev/null || true
  [[ -n "${IOSTAT_PID:-}" ]] && kill "$IOSTAT_PID" 2>/dev/null || true
  SNAPSTORE_BENCH_ROOT="$SNAPSTORE_BENCH_ROOT" \
  cargo bench -p snapstore-pagestore --bench read_path -- \
    --warm-up-time 2 --measurement-time 8 \
    2>&1 | tee "$EVIDENCE_ROOT/m5-transport/read_path.log" || true
  [[ "$M5_STATUS" -eq 0 ]]
else
  echo "RUN_M5=0" >"$EVIDENCE_ROOT/m5-transport/not-run.txt"
fi

if [[ "$RUN_M7_GC" == "1" ]]; then
  echo "== [4/5] M7 GC readiness benchmark"
  set +e
  SNAPSTORE_BENCH_ROOT="$SNAPSTORE_BENCH_ROOT" \
  SNAPSTORE_GC_BENCH_JSON="$EVIDENCE_ROOT/m7-gc-benchmark/results.json" \
  cargo test -p snapstore-server --test gc_readiness_bench --release -- --ignored --nocapture \
    2>&1 | tee "$EVIDENCE_ROOT/m7-gc-benchmark/gc_readiness_bench.log"
  M7_STATUS=${PIPESTATUS[0]}
  set -e
  [[ "$M7_STATUS" -eq 0 ]]
else
  echo "RUN_M7_GC=0" >"$EVIDENCE_ROOT/m7-gc-benchmark/not-run.txt"
fi

echo "== [5/5] assembling evidence.json"
python3 - "$EVIDENCE_ROOT" "$SNAPSTORE_BENCH_ROOT" <<'PY'
import hashlib, json, os, platform, shutil, subprocess, sys
from pathlib import Path

root = Path(sys.argv[1])
bench_root = Path(sys.argv[2])

def read(path, default=""):
    try:
        return Path(path).read_text(errors="replace").strip()
    except FileNotFoundError:
        return default

def sh(*args):
    return subprocess.run(args, capture_output=True, text=True).stdout.strip()

def load_json(path):
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return None

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def artifact_list():
    out = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "evidence.json":
            out.append({"path": str(path.relative_to(root)), "sha256": sha256(path)})
    return out

def bar(id, target, measured, unit, ok, evidence_path, attribution=""):
    if measured is None:
        status = "not_run"
    else:
        status = "pass" if ok else "fail"
    return {
        "id": id,
        "target": target,
        "measured": measured,
        "unit": unit,
        "status": status,
        "attribution": attribution,
        "evidence_path": evidence_path,
    }

m5 = load_json(root / "m5-transport" / "results.json")
m7 = load_json(root / "m7-gc-benchmark" / "results.json")
flake_summary = read(root / "flake" / "postfix-50x-summary.txt")
attestation = read(root / "hardware" / "phase5-host-attestation.txt")
att = {}
for line in attestation.splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        att[k] = v

stat = os.statvfs(bench_root)
free_bytes = stat.f_bavail * stat.f_frsize
lsblk = load_json(root / "hardware" / "lsblk.json") or {}
transports = []
for dev in lsblk.get("blockdevices", []):
    tran = dev.get("tran")
    if tran:
        transports.append(tran)
disk_class = "nvme" if "nvme" in transports else (",".join(sorted(set(transports))) or "unknown")
qualified = disk_class == "nvme" and free_bytes >= 70 * 1024**3 and att.get("actual_soak_host") == "true"
qualification_reason = "qualified" if qualified else "requires NVMe-class disk, >=70 GiB free, and actual_soak_host=true attestation"

bars = []
bars.append(bar("page_channel_fallback_50x", "50 green runs", 50 if "failures=0" in flake_summary else None, "runs", "failures=0" in flake_summary, "flake/postfix-50x-summary.txt"))
if m5:
    bars.extend([
        bar("put_batch_warm_sustained", ">= 1.5", m5.get("put_batch_warm_sustained_gbps"), "GB/s", m5.get("put_batch_warm_sustained_gbps", 0) >= 1.5, "m5-transport/results.json"),
        bar("get_batch_warm_sustained", ">= 2.5", m5.get("get_batch_warm_sustained_gbps"), "GB/s", m5.get("get_batch_warm_sustained_gbps", 0) >= 2.5, "m5-transport/results.json"),
        bar("commit_16x8mib_p99", "< 40", m5.get("commit_16x8mib_p99_ms"), "ms", m5.get("commit_16x8mib_p99_ms", 1e9) < 40, "m5-transport/results.json"),
        bar("commit_16x8mib_aggregate", ">= 1.2", m5.get("commit_16x8mib_aggregate_gbps"), "GB/s", m5.get("commit_16x8mib_aggregate_gbps", 0) >= 1.2, "m5-transport/results.json"),
        bar("create_node_inline_log_p50", "< 1.5", m5.get("create_node_inline_log_p50_ms"), "ms", m5.get("create_node_inline_log_p50_ms", 1e9) < 1.5, "m5-transport/results.json"),
        bar("update_nodes_256_p50", "< 3", m5.get("update_nodes_256_p50_ms"), "ms", m5.get("update_nodes_256_p50_ms", 1e9) < 3, "m5-transport/results.json"),
    ])
else:
    for name in ["put_batch_warm_sustained", "get_batch_warm_sustained", "commit_16x8mib_p99", "commit_16x8mib_aggregate", "create_node_inline_log_p50", "update_nodes_256_p50"]:
        bars.append(bar(name, "", None, "", False, "m5-transport/not-run.txt"))
if m7:
    reclaim = m7.get("reclaiming_gc_run", {})
    idle = m7.get("idle_commit", {})
    bars.extend([
        bar("m7_gc_reclaiming_duration", "< 60000", reclaim.get("duration_ms"), "ms", reclaim.get("duration_ms", 1e18) < 60000, "m7-gc-benchmark/results.json"),
        bar("m7_gc_nodes_reaped", "> 0", reclaim.get("nodes_reaped"), "nodes", reclaim.get("nodes_reaped", 0) > 0, "m7-gc-benchmark/results.json"),
        bar("m7_gc_ingest_during_gc", ">= target", reclaim.get("ingest_mbps"), "MB/s", reclaim.get("ingest_mbps", 0) >= m7.get("config", {}).get("ingest_target_mbps", 200), "m7-gc-benchmark/results.json"),
        bar("m7_gc_commit_p99_vs_idle", "< 2x idle", reclaim.get("commit_p99_ms"), "ms", idle.get("p99_ms", 0) > 0 and reclaim.get("commit_p99_ms", 1e18) < 2 * idle.get("p99_ms", 0), "m7-gc-benchmark/results.json"),
    ])
else:
    for name in ["m7_gc_reclaiming_duration", "m7_gc_nodes_reaped", "m7_gc_ingest_during_gc", "m7_gc_commit_p99_vs_idle"]:
        bars.append(bar(name, "", None, "", False, "m7-gc-benchmark/not-run.txt"))

evidence = {
    "run_id": root.name,
    "request": ".agents/requests/phase5-readiness-gc-benchmark-and-transport-revalidation",
    "started_at": os.environ.get("PHASE5_STARTED_AT", ""),
    "finished_at": sh("date", "-u", "+%Y-%m-%dT%H:%M:%SZ"),
    "git": {
        "rev": sh("git", "rev-parse", "HEAD"),
        "status_clean": sh("git", "status", "--porcelain") == "",
        "status_short": sh("git", "status", "--short", "--branch"),
    },
    "host": {
        "hostname": read(root / "hardware" / "hostname.txt") or platform.node(),
        "phase5_soak_host": att.get("phase5_soak_host", "UNSET"),
        "same_as_i5_sata_reference": att.get("same_as_i5_sata_reference", "UNSET"),
        "actual_soak_host": att.get("actual_soak_host", "UNSET"),
        "operator_attestation": "hardware/phase5-host-attestation.txt",
        "kernel": read(root / "hardware" / "kernel.txt"),
        "rustc": read(root / "hardware" / "rustc.txt"),
    },
    "bench_root": {
        "path": str(bench_root),
        "mount": "hardware/mount.txt",
        "free_bytes": free_bytes,
    },
    "hardware_qualification": {
        "qualified": qualified,
        "reason": qualification_reason,
        "disk_class": disk_class,
        "cpu_governor": read(root / "hardware" / "cpu-governor.txt"),
        "thermal_or_throttle_notes": read(root / "hardware" / "thermal.txt"),
    },
    "commands": [
        {"id": "flake_50x", "argv": "cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1", "env": {}, "log": "flake/postfix-50x.log"},
        {"id": "m5_transport", "argv": "cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture", "env": {"SNAPSTORE_BENCH_ROOT": str(bench_root)}, "log": "m5-transport/page_channel_perf.log"},
        {"id": "m7_gc", "argv": "cargo test -p snapstore-server --test gc_readiness_bench --release -- --ignored --nocapture", "env": {"SNAPSTORE_BENCH_ROOT": str(bench_root)}, "log": "m7-gc-benchmark/gc_readiness_bench.log"},
    ],
    "artifacts": artifact_list(),
    "bar_results": bars,
    "flake": {
        "summary": flake_summary,
        "root_cause": read(root / "flake" / "root-cause.txt"),
    },
    "m5_transport": m5 or {},
    "m7_gc": m7 or {},
    "risk_statement": "",
}
(root / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
print(f"wrote {root / 'evidence.json'}")
PY

echo "== done: $EVIDENCE_ROOT"
