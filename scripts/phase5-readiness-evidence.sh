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
run_capture "$EVIDENCE_ROOT/hardware/mount.json" findmnt -J -T "$SNAPSTORE_BENCH_ROOT" -o SOURCE,SOURCES,FSTYPE,MAJ:MIN,TARGET,FSROOT || true
run_capture "$EVIDENCE_ROOT/hardware/df.txt" df -h "$SNAPSTORE_BENCH_ROOT" || true
run_capture "$EVIDENCE_ROOT/hardware/lsblk.json" lsblk -J -o NAME,KNAME,PATH,PKNAME,MAJ:MIN,MODEL,SERIAL,TRAN,ROTA,TYPE,SIZE,MOUNTPOINTS,MOUNTPOINT,FSTYPE || true
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
python3 scripts/phase5_readiness_evidence.py "$EVIDENCE_ROOT" "$SNAPSTORE_BENCH_ROOT"

echo "== done: $EVIDENCE_ROOT"
