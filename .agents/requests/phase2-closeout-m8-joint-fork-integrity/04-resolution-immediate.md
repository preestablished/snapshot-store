# Immediate Resolution / Status

Implementation session: 2026-07-09 on
`phase2-closeout-m8-joint-fork-integrity`.

This file records the immediate tracker and fake-testable tooling work. It is
not the full M8 closeout; the 1000x joint run, NVMe benchmark rows, and
cross-repo required checks remain open.

## Tracker Reconciliation

`bd dolt pull` succeeds in the current checkout, and the request's named beads
remain absent from this beads database:

- `snapstore-675`
- `snapstore-pov`
- `snapstore-28z`
- `snapstore-feb`
- `snapstore-nn4`

Replacement beads were created:

| Bead | Purpose | State |
|---|---|---|
| `snapshot-store-orm` | M8 joint fork-integrity closeout epic/replacement tracker | open |
| `snapshot-store-m0u` | Qualified Phase 5 hardware rows predecessor | in progress |
| `snapshot-store-gy9` | Replay-commit ref identity harness/evidence work | closed |
| `snapshot-store-8p9` | Baseline-resident restore and FULL-cadence smoke | closed |
| `snapshot-store-4ua` | 1000x joint fork ref-identity acceptance | open |
| `snapshot-store-2dl` | Required M8 regression in both repos | in progress |
| `snapshot-store-4fm` | Beads dependency-table repair for this graph | closed |

## Dependency Edge Status

The requested dependency graph is now tracker-enforced. The embedded Dolt db was
missing the `wisp_dependencies` compatibility table required by `bd dep`; it was
recreated from the pre-rebootstrap backup schema, after which `bd dep add/list`
worked again. `snapshot-store-4fm` is closed.

Installed edges:

- `snapshot-store-8p9` depends on `snapshot-store-gy9`
- `snapshot-store-4ua` depends on `snapshot-store-m0u`
- `snapshot-store-4ua` depends on `snapshot-store-8p9`
- `snapshot-store-2dl` depends on `snapshot-store-8p9`
- `snapshot-store-orm` depends on `snapshot-store-gy9`,
  `snapshot-store-m0u`, `snapshot-store-4ua`, `snapshot-store-2dl`, and
  `snapshot-store-8p9`

Graph validation:

```bash
bd dep list snapshot-store-8p9 snapshot-store-4ua snapshot-store-2dl snapshot-store-orm
bd ready      # only snapshot-store-m0u is ready
bd blocked    # snapshot-store-4ua and snapshot-store-orm are blocked as intended
bd dep cycles # no dependency cycles
```

`bd preflight --check` is not useful in this checkout because its configured
checks are Go defaults (`go test -short ./...`, `golangci-lint run ./...`) and
fail before reaching project-specific Rust validation.

## `snapstore-pov` Status

The request described a beads Dolt remote divergence under `snapstore-pov`.
That condition is not reproducible in this checkout so far: `bd dolt pull`
succeeds, the named `snapstore-pov` bead is absent, and `bd dolt push` also
succeeds after creating/updating the replacement M8 beads. No owner force-push
decision is needed for this checkout.

## Tooling Landed

The snapshot-store side now has a host-runnable M8 evidence validator:

- `scripts/m8_joint_fork_integrity_evidence.py`
- `scripts/m8_joint_fork_integrity_evidence_test.py`
- `scripts/m8_joint_fork_integrity_fake_harness.py`
- `scripts/m8_joint_fork_integrity_fake_harness_test.py`

The validator enforces the plan's schema requirements for:

- `schema_version`, `run_kind`, `expected_child_count`, and request identity
- exact repository rev/dirty-state fields
- store-root qualification for full acceptance
- required M8 pass/fail bars
- typed child JSONL rows
- replay-commit ref equality for positive runs
- committed replay-ref mismatch for semantic-negative runs
- contiguous child indices and at least one baseline-delta row in positive runs
- optional `row_source` values (`fresh`/`resumed`) and optional top-level
  resume counts when live evidence includes them
- optional `latency_ms` summaries with typed p50/p95/p99/max stats when live
  evidence includes them

CI now runs the evidence test suites:

- `python3 scripts/phase5_readiness_evidence_test.py`
- `python3 scripts/m8_joint_fork_integrity_evidence_test.py`
- `python3 scripts/m8_joint_fork_integrity_fake_harness_test.py`

Local validation passed with:

```bash
DD_TRACE_ENABLED=false DD_CIVISIBILITY_ENABLED=false python3 scripts/m8_joint_fork_integrity_evidence_test.py
DD_TRACE_ENABLED=false DD_CIVISIBILITY_ENABLED=false python3 scripts/m8_joint_fork_integrity_fake_harness_test.py
DD_TRACE_ENABLED=false DD_CIVISIBILITY_ENABLED=false python3 scripts/phase5_readiness_evidence_test.py
```

The fake harness writes validator-valid `run_kind=fake` evidence with
`evidence.json`, `child-ref-table.jsonl`, and `child-ref-table.csv`. Its tests
cover FULL cadence rows, baseline-delta rows, shared-page aggregate accounting,
resume from a partial child table, and a committed semantic-negative
`ref_mismatch` row. This is host-only scaffolding; it is not live KVM/NVMe M8
acceptance evidence.

## Hypervisor Replay-Commit Progress

The sibling `determinism-hypervisor` branch
`m8-snapshot-store-replay-commit` now adds an explicit M8 replay-commit harness
path in `crates/dh-worker/tests/m7_fork_verify.rs`.

The new ignored gate is:

```bash
DH_M9_ALLOW_SKIP=0 DH_M7_ACCEPT_GUEST=linux \
DH_M7_ACCEPT_SLOT_CORES=2-5 DH_M7_ACCEPT_JOBS=1000 \
  cargo test -p dh-worker --test m7_fork_verify --release \
    m8_accept_1000_seeded_forks_replay_commit_ref_identity \
    -- --ignored --nocapture --test-threads=1
```

The gate forks the original child, validates its DHILOG lineage, runs
`VerifyReplay`, then restores the root snapshot, re-drives the same deterministic
child, calls `TakeSnapshot`, and requires the replay commit to match the
original snapshot ref, state hash, input log id, counters, frame metadata, and
Linux PVBLK proof metadata. Slot id is allowed to differ.

The hypervisor harness now honors:

- `M8_STORE_ROOT` for the actual snapstore data root
- `M8_EVIDENCE_ROOT` for live run artifacts
- `M8_EVIDENCE_RESUME=1` to resume a positive live run from an existing
  `child-ref-table.jsonl`
- `M8_STORE_ROOT_QUALIFIED=1` and `M8_STORE_ROOT_DISK_CLASS=<class>` for
  evidence store-root metadata

The M8 gate writes live `child-ref-table.jsonl` rows incrementally, writes
`child-ref-table.csv` and `evidence.json` at finish, and computes
`shared_page_ratio` from store-visible root/child manifest page hashes. It now
probes each child snapshot through `RestoreSnapshot(baseline=root_ref)`, records
positive rows as `restore_mode=baseline_delta`, and keeps the separate
replay-root restore timing as `timing_ms.replay_restore`. It records row-level
timing for fork, original run/commit, baseline-delta restore, replay restore,
replay, and replay commit, then writes `latency_ms` summaries for
p50/p95/p99/max evidence.
`m8_fork_commit_p99` turns green when every row has measured fork-to-original
commit timing; `m8_restore_delta_p99` turns green only when baseline-delta
restore timing is present. The positive live path can resume only from a valid
contiguous child-index prefix for the same seeded job universe; resumed rows
are rewritten with `row_source=resumed`, fresh rows are emitted with
`row_source=fresh`, and `evidence.json` includes resume counts. If
`${M8_EVIDENCE_ROOT}/semantic-negative/evidence.json` exists, the positive
summary links it and uses its red result for
`semantic_negative.actual_red_result`.

The hypervisor branch also adds a separate live nanokernel semantic-negative
gate:

```bash
cargo test -p dh-worker --test m7_fork_verify \
  m8_accept_semantic_negative_replay_commit_ref_mismatch \
  -- --ignored --nocapture --test-threads=1
```

That gate mutates the first pad input after restoring the root snapshot,
commits the replay with `TakeSnapshot`, requires the replay snapshot ref to
differ from the original child ref, and writes `run_kind=semantic_negative`
evidence under `${M8_EVIDENCE_ROOT:-target/m8-joint-fork-integrity-live}/semantic-negative`.

The hypervisor worker now also supports the M8 baseline/FULL restore wiring:

- `RestoreSnapshotRequest.baseline` is an opt-in ancestor ref. When present,
  the worker materializes that baseline into the allocated slot, then overlays
  `ResolvePages(snapshot, baseline_ref=baseline)` before applying child
  device/vCPU state.
- `TakeSnapshot` computes the parent chain depth and emits a FULL manifest
  instead of another DELTA when the next delta would exceed
  `max_delta_chain` (`64` by default).
- A service smoke with `max_delta_chain=1` proves FULL -> DELTA -> FULL
  rollover and restores the DELTA snapshot with `baseline=FULL`.

## CI Permanence Progress

Bounded M8 ref-identity workflow lanes have been added in both repos:

- `determinism-hypervisor` CI now runs `M8 bounded replay-commit ref identity`
  in the self-hosted `kvm-intel` PR lane with `DH_M7_ACCEPT_JOBS=8`.
- `determinism-hypervisor` `nightly-drift` now has `m8-ref-identity-100`,
  defaulting to 100 children and uploading the M8 evidence root.
- `snapshot-store` CI now has `m8-ref-identity-bounded`, a self-hosted
  cross-repo job that checks out the current snapshot-store SHA as the sibling
  dependency and runs the bounded M8 hypervisor test against it.

The snapshot-store job is temporarily pinned to the sibling hypervisor branch
`m8-snapshot-store-replay-commit`, overridable with repository variable
`M8_HYPERVISOR_REF`, until that branch lands on the hypervisor default branch.
Remaining external closeout for `snapshot-store-2dl`: observe green GitHub runs,
record branch-protection/required-check status, and record phases-track sign-off
for bounded required CI plus operator-run 1000x full acceptance.

CI root-cause note: GitHub rejected the latest M8 workflow pushes before
scheduling jobs because the new bounded M8 jobs used `${{ runner.temp }}` in
job-level `env`, where the `runner` context is unavailable. The workflows now
use `${{ github.workspace }}/m8-{store,evidence}-${{ github.run_id }}` instead.
Local workflow validation passed with:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@latest \
  -ignore 'label "kvm-intel" is unknown' .github/workflows/ci.yaml
go run github.com/rhysd/actionlint/cmd/actionlint@latest \
  -ignore 'label "kvm-intel" is unknown' .github/workflows/nightly-drift.yaml
```

Hypervisor local validation passed:

```bash
rustfmt --edition 2021 --check crates/dh-worker/src/restore_engine.rs crates/dh-worker/src/service.rs crates/dh-worker/tests/restore_engine.rs crates/dh-worker/tests/m7_fork_verify.rs crates/dh-worker/tests/common/mod.rs crates/dh-worker/tests/m6_full_api_uds.rs crates/dh-worker/tests/linux_worker_api.rs crates/dh-worker/src/m9_handoff.rs crates/dh-worker/tests/m5_frame_scheduling.rs crates/dh-worker/tests/capture_engine_real_image.rs crates/dh-worker/tests/play_perf_smoke.rs crates/dh-worker/tests/frame_capture_stream.rs crates/dh-worker/tests/m4_transparency.rs
cargo test -p dh-worker take_snapshot_rolls_full_manifest_and_restore_accepts_baseline_delta -- --nocapture
cargo test -p dh-worker --test restore_engine delta_chain_restore_materializes_the_full_state -- --nocapture
cargo test -p dh-worker --test m7_fork_verify replay_commit_matcher_allows_slot_drift_but_rejects_ref_drift
cargo test -p dh-worker --test m7_fork_verify m8_resume
cargo test -p dh-worker --test m7_fork_verify m8_semantic_negative_link_reads_red_result
cargo test -p dh-worker --test m7_fork_verify --no-run
cargo test -p dh-worker --no-run
python3 - <<'PY'  # YAML parse check for touched workflows
import yaml
for path in [".github/workflows/ci.yaml", ".github/workflows/nightly-drift.yaml"]:
    with open(path, "r", encoding="utf-8") as fh:
        yaml.safe_load(fh)
PY
git diff --check
```

## Remaining Immediate Work

- Confirm the new M8 workflow lanes in GitHub, record required-check status,
  and capture bounded-CI/full-acceptance sign-off for `snapshot-store-2dl`.
- Run the hardware-gated Phase 5 rows and the 1000x M8 acceptance on a
  qualified NVMe-class store root.

## Hardware Escalation Progress

`snapshot-store-m0u` is now claimed. A fresh local hardware-availability
preflight was captured at
`target/phase5-readiness-m0u-20260709-local/evidence.json` with:

```bash
SNAPSTORE_BENCH_ROOT=target/phase5-m0u-local-scratch \
RUN_FIO=0 RUN_M5=0 RUN_M7_GC=0 RUN_FLAKE_50X=0 \
PHASE5_EVIDENCE_ROOT=target/phase5-readiness-m0u-20260709-local \
scripts/phase5-readiness-evidence.sh
```

The evidence remains unqualified: `hardware_qualification.qualified=false`,
`disk_class=sata`, backing device `/dev/sda`, no actual soak-host attestation,
and the fio/M5/M7 rows were intentionally not run. This is a current blocker
record, not acceptance evidence. The next required action is to run the full
Phase 5 readiness command on an NVMe-class soak host with `RUN_FIO=1`,
`RUN_M5=1`, `RUN_M7_GC=1`, and `PHASE5_ACTUAL_SOAK_HOST=true`.
