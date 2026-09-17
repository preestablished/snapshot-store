#!/usr/bin/env bash
# Pin the READY root snapshot so GC never reclaims it.
# See docs/ops/ready-root-pin.md. Prints only newly_pinned and the first
# 8 hex chars of the ref; the full ref stays private.
set -euo pipefail

for var in SNAPSTORE_GRPC_UDS_PATH BRIDGE_REAL_SNAPSHOT_REF BRIDGE_WORKLOAD_IMAGE_REF REFWORK_EMU_VERSION; do
  val="${!var:-}"
  # Empty, or a literal "<placeholder>" copied from the hypervisor runbook.
  if [[ -z "$val" || "$val" =~ ^\<.*\>$ ]]; then
    echo "pin-ready-root: $var is unset or a placeholder" >&2
    exit 2
  fi
done

if [[ ! "$BRIDGE_REAL_SNAPSHOT_REF" =~ ^[0-9a-fA-F]{64}$ ]]; then
  echo "pin-ready-root: BRIDGE_REAL_SNAPSHOT_REF is not 64 hex chars" >&2
  exit 2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
snapstorectl="${SNAPSTORECTL:-$here/../../target/release/snapstorectl}"

note="ready-root image=${BRIDGE_WORKLOAD_IMAGE_REF} emu=${REFWORK_EMU_VERSION} date=$(date -u +%F) by=snapstore-plan-C"

out="$("$snapstorectl" --endpoint "uds:${SNAPSTORE_GRPC_UDS_PATH}" pin "$BRIDGE_REAL_SNAPSHOT_REF" --note "$note")"
newly="$(sed -n 's/^newly_pinned: *//p' <<<"$out")"
if [[ "$newly" != "true" && "$newly" != "false" ]]; then
  echo "pin-ready-root: unexpected snapstorectl output" >&2
  exit 1
fi
echo "newly_pinned=${newly} ref=${BRIDGE_REAL_SNAPSHOT_REF:0:8}"
