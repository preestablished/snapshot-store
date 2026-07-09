# WI3 - Restore Wiring, FULL Cadence, And Performance Runs

This work connects the fake-testable harness to the real cross-repo execution
path, then runs the hardware-gated rows and the M8 acceptance session.

## Baseline-Delta Restore

The store side already supports baseline-delta restore:

| Surface | Anchor |
|---|---|
| Store mode B | `crates/snapstore-store/src/lib.rs:546` documents baseline mode |
| Store validation | `crates/snapstore-store/src/lib.rs:591` rejects non-ancestor baselines |
| Server request parsing | `crates/snapstore-server/src/service.rs:306` parses `baseline_ref` |
| Client API | `crates/snapstore-client/src/client.rs:251` accepts `baseline_ref` |
| Blocking client | `crates/snapstore-client/src/blocking.rs:56` accepts `baseline_ref` |

The hypervisor gap is the caller and the restore model:

```text
../determinism-hypervisor/crates/dh-worker/src/restore_engine.rs:162
store.resolve_pages(snapshot_ref, None, false)
```

Do not implement this as "pass `baseline_ref` into the current fresh-slot full
restore." `ResolvePages(snapshot, baseline_ref=parent)` returns only delta
pages, while the current `restore_engine` writes into a fresh slot and requires
all RAM pages to be covered. That would leave holes.

Implement a baseline-resident restore path:

| Step | Requirement |
|---|---|
| Baseline materialization | The slot already contains the parent snapshot RAM, or the worker first restores/materializes the parent into a reusable baseline cache/lease |
| Baseline validation | Verify the resident baseline identity matches the `baseline_ref` used in `ResolvePages`; stale or unknown baseline falls back to full restore |
| Delta resolve | Call `resolve_pages(child_ref, Some(parent_ref), false)` and apply only returned pages over the resident baseline RAM |
| Coverage accounting | Full-coverage checks stay for full restore; delta mode checks that every returned page is in range and unique, not that every page is returned |
| Evidence | Record `restore_mode=baseline_delta`, `baseline_ref`, page count returned, and fallback reason if full restore was used |

Preserve full restore for bootstrap, cold restore, FULL-manifest rollover, and
explicit diagnostic modes. The M8 smoke must prove the delta path ran, either
through a metric, a trace field, or an evidence row marking
`restore_mode=baseline_delta`.

## FULL-Manifest Cadence

INTEGRATION.md §2.1(f) requires a FULL manifest whenever delta `chain_depth`
would exceed `max_delta_chain` (default 64). Add or verify worker-side metadata
that tracks:

| Field | Purpose |
|---|---|
| `snapshot_ref` | Current ref |
| `parent_ref` | Direct parent ref, if delta |
| `chain_depth` | Number of deltas since the last FULL root |
| `manifest_kind` | FULL or DELTA |
| `max_delta_chain` | Config value used by the worker |

Smoke requirements:

| Scenario | Expected result |
|---|---|
| Root snapshot | FULL, `chain_depth=0` |
| Child below threshold | DELTA, `chain_depth=parent+1` |
| Child at threshold rollover | FULL, `chain_depth=0`, no parent |
| Restore from child with parent available | baseline-delta restore used |
| Restore from FULL rollover | full restore allowed and recorded |

Keep this smoke small enough for ordinary CI if it can be faked; the live KVM
smoke can stay ignored/operator-run if the worker path requires `/dev/kvm`.

## Joint Session Preflight

Before the full 1000x run, produce a session manifest:

| Field | Required content |
|---|---|
| snapshot-store rev | `git rev-parse HEAD` and clean/dirty status |
| determinism-hypervisor rev | `git -C ../determinism-hypervisor rev-parse HEAD` and clean/dirty status |
| control-plane / guest-sdk revs | because both repos path-depend on them in CI |
| guest image identity | Linux or nanokernel fixture path, hashes, and config |
| host identity | hostname, kernel, CPU, RAM, disk class, mount, fio result |
| runner slot cores | `DH_M7_ACCEPT_SLOT_CORES` or equivalent |
| bridge/runtime window | explicit statement that no long `RunWithFrameCapture` streams share the box |

Use the Phase 5 script's hardware capture logic as the model. It already records
hostname, git, kernel, rustc, CPU, memory, mount, `lsblk`, dirty VM settings,
fio, and qualification at `scripts/phase5-readiness-evidence.sh:78` and
`scripts/phase5_readiness_evidence.py:706`.

## Phase 5 Hardware Rows

The M8 run should not proceed as a final acceptance while the request's
hardware-bound predecessors are unresolved. Current state:

| Row | Current evidence |
|---|---|
| M5 transport | `docs/bench-baseline.md:103` says not run in the Phase 5 local preflight |
| M7 GC readiness | `docs/bench-baseline.md:108` says not run in the Phase 5 local preflight |
| Qualification | `docs/bench-baseline.md:95` says the 2026-07-08 run was hardware-unqualified |

On a qualified NVMe-class soak host, run:

```bash
SNAPSTORE_BENCH_ROOT=/path/on/nvme \
PHASE5_ACTUAL_SOAK_HOST=true \
RUN_FIO=1 RUN_M5=1 RUN_M7_GC=1 RUN_FLAKE_50X=1 \
scripts/phase5-readiness-evidence.sh
```

Then update `docs/bench-baseline.md` with the measured M5/M7 rows before
closing the M8 gated lane. If the hardware cannot be provided, record a hardware
escalation and do not claim M8 benchmark compliance.

The M8 snapstore data root must be under the same qualified NVMe mount. The
existing hypervisor M7 harness uses ordinary tempdirs for the store; M8 must add
`M8_STORE_ROOT` or an equivalent configuration and fail evidence qualification
if the actual store root resolves outside the qualified mount.

## M8 Full Run

Default command shape should extend the M7 ignored acceptance:

```bash
DH_M7_ACCEPT_GUEST=linux \
DH_M7_ACCEPT_JOBS=1000 \
DH_M7_ACCEPT_SLOT_CORES=2-5 \
DH_M7_ACCEPT_ALLOW_SKIP=0 \
DH_M9_ALLOW_SKIP=0 \
M8_STORE_ROOT=/path/on/qualified-nvme/m8-store \
M8_EVIDENCE_ROOT=target/m8-joint-fork-integrity-<UTC> \
cargo test -p dh-worker --test m7_fork_verify --release \
  m8_accept_1000_forks_ref_identity \
  -- --ignored --nocapture --test-threads=1
```

The exact test name may differ, but the evidence contract must not:

| Measurement | Pass bar |
|---|---|
| Child count | 1000 original children and 1000 replay commits |
| Ref identity | 1000/1000 `replay_ref == original_ref` |
| Replay | 1000/1000 VerifyReplay Done with no Divergence |
| Store root | `M8_STORE_ROOT` is on the qualified NVMe mount and recorded in evidence |
| Shared pages | Aggregate `shared_page_ratio >= 0.94` |
| Fork-to-commit latency | p50/p99 compared to ARCHITECTURE §7.1 commit target |
| Restore latency | full and delta restore p50/p99 compared to ARCHITECTURE §7.1 restore targets |

Measured misses are not automatically fatal if they are attributed and accepted
by the phases track. Unmeasured rows are fatal to closeout.

## Bench-Baseline Update

Append an M8 section to `docs/bench-baseline.md` in the same hardware record as
the qualifying Phase 5 rows. Include:

| Row | Required columns |
|---|---|
| M8 host/session | host, disk, runner, revs, evidence root |
| Fork-to-commit | p50, p95, p99, max, target, status |
| Restore full | p50, p95, p99, max, target, status |
| Restore baseline-delta | p50, p95, p99, max, target, status |
| Shared pages | min, p50, p95, aggregate, target >= 94%, status |
| Ref identity | `1000/1000`, status |
| Semantic negative | red-run link, status |

Do not overwrite the earlier SATA/local preflight notes; the contrast matters.
