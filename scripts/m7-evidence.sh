#!/usr/bin/env bash
# M7 GC acceptance evidence runner.
#
# Produces target/m7-acceptance-<UTC>/evidence.json plus raw logs, in the
# shape guest-sdk's Ms4 acceptance used (git rev, host/kernel, per-case
# tables).  Re-runnable by the bridge side verbatim:
#
#   GC_PROP_SEED=<seed> CRASH_SEED=<seed> scripts/m7-evidence.sh
#
# Defaults below are the recorded acceptance seeds.
set -euo pipefail

cd "$(dirname "$0")/.."

export GC_PROP_CASES="${GC_PROP_CASES:-10000}"
export GC_PROP_SEED="${GC_PROP_SEED:-20260703}"
export CRASH_CYCLES="${CRASH_CYCLES:-1000}"
export CRASH_SEED="${CRASH_SEED:-20260703}"
export MATRIX_PASSES="${MATRIX_PASSES:-50}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="target/m7-acceptance-${STAMP}"
mkdir -p "$OUT"

echo "== M7 evidence run -> $OUT"

# ── 1. Property suite (deep, seeded) ─────────────────────────────────────────
echo "== [1/3] property suite: ${GC_PROP_CASES} cases, seed ${GC_PROP_SEED}"
PROP_LOG="$OUT/gc-properties.log"
set +e
( GC_PROP_CASES="$GC_PROP_CASES" GC_PROP_SEED="$GC_PROP_SEED" \
  cargo test -p snapstore-server --test gc_properties \
    --features snapstore-server/gc-test-hooks --release -- --nocapture \
  ) 2>&1 | tee "$PROP_LOG"
PROP_STATUS=${PIPESTATUS[0]}
set -e

# ── 2. Negative proofs are part of the same suite; scrape the table ─────────
grep -h "NEGATIVE-PROOF" "$PROP_LOG" > "$OUT/negative-proofs.txt" || true

# ── 3. Crash matrix (extended with the six gc-* failpoints) ─────────────────
echo "== [2/3] crash suite: ${CRASH_CYCLES} cycles, matrix x${MATRIX_PASSES}, seed ${CRASH_SEED}"
CRASH_LOG="$OUT/crash-suite.log"
set +e
( cargo run --release -p snapstore-crash --features failpoints -- \
    run --cycles "$CRASH_CYCLES" --seed "$CRASH_SEED" --matrix-passes "$MATRIX_PASSES" \
  ) 2>&1 | tee "$CRASH_LOG"
CRASH_STATUS=${PIPESTATUS[0]}
set -e

# ── evidence.json ─────────────────────────────────────────────────────────────
echo "== [3/3] assembling evidence.json"
python3 - "$OUT" <<'EOF'
import json, os, platform, re, subprocess, sys

out = sys.argv[1]
def sh(*args):
    return subprocess.run(args, capture_output=True, text=True).stdout.strip()

prop_log = open(os.path.join(out, "gc-properties.log")).read()
crash_log = open(os.path.join(out, "crash-suite.log")).read()

seed_line = next((l for l in prop_log.splitlines() if "GC_PROP_SEED=" in l), "")
retries = next((l for l in prop_log.splitlines() if "GC_READ_RETRIES" in l), "")
prop_results = re.findall(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed", prop_log)
crash_done = next((l for l in crash_log.splitlines() if l.startswith("DONE")), "")
negatives = [l.strip() for l in prop_log.splitlines() if "NEGATIVE-PROOF" in l]

evidence = {
    "milestone": "snapshot-store M7 GC (Phase 3 exit-gate item 4)",
    "git_rev": sh("git", "rev-parse", "HEAD"),
    "git_status_clean": sh("git", "status", "--porcelain") == "",
    "host": platform.node(),
    "kernel": sh("uname", "-a"),
    "rustc": sh("rustc", "--version"),
    "property_suite": {
        "cases": os.environ.get("GC_PROP_CASES"),
        "seed_line": seed_line,
        "results": [{"status": s, "passed": int(p), "failed": int(f)} for s, p, f in prop_results],
        "r2_retry_counter_line": retries,
    },
    "negative_proofs": negatives,
    "crash_suite": {
        "summary_line": crash_done,
        "seed": os.environ.get("CRASH_SEED"),
        "cycles": os.environ.get("CRASH_CYCLES"),
        "matrix_passes": os.environ.get("MATRIX_PASSES"),
    },
}
path = os.path.join(out, "evidence.json")
json.dump(evidence, open(path, "w"), indent=2)
print(f"wrote {path}")
EOF

echo "== done: $OUT"
exit $(( PROP_STATUS + CRASH_STATUS ))
