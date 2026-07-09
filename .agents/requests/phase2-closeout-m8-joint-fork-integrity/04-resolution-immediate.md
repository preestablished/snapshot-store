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
| `snapshot-store-m0u` | Qualified Phase 5 hardware rows predecessor | open |
| `snapshot-store-gy9` | Replay-commit ref identity harness/evidence work | in progress |
| `snapshot-store-8p9` | Baseline-resident restore and FULL-cadence smoke | open |
| `snapshot-store-4ua` | 1000x joint fork ref-identity acceptance | open |
| `snapshot-store-2dl` | Required M8 regression in both repos | open |
| `snapshot-store-4fm` | Beads dependency-table repair for this graph | open |

## Dependency Edge Status

The requested dependency graph cannot currently be tracker-enforced because
`bd dep add` and `bd dep list` fail in embedded mode with:

```text
Error 1146: table not found: wisp_dependencies
```

The attempted edge was:

```bash
bd dep add snapshot-store-8p9 snapshot-store-gy9
```

The same backend failure occurs for:

```bash
bd dep list snapshot-store-8p9
```

This tracker defect is filed as `snapshot-store-4fm`. The intended edges are
recorded in `snapshot-store-orm` notes and in
`.agents/plans/phase2-closeout-m8-joint-fork-integrity/01-tracker-and-entry.md`:

- `snapshot-store-8p9` depends on `snapshot-store-gy9`
- `snapshot-store-4ua` depends on `snapshot-store-m0u`
- `snapshot-store-4ua` depends on `snapshot-store-8p9`
- `snapshot-store-2dl` depends on `snapshot-store-8p9`
- `snapshot-store-orm` depends on the child beads

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
- `M8_STORE_ROOT_QUALIFIED=1` and `M8_STORE_ROOT_DISK_CLASS=<class>` for
  evidence store-root metadata

The M8 gate writes live `child-ref-table.jsonl` rows incrementally, writes
`child-ref-table.csv` and `evidence.json` at finish, and computes
`shared_page_ratio` from store-visible root/child manifest page hashes. The
positive live `evidence.json` is intentionally marked partial until the
baseline-delta smoke is aggregated into full acceptance evidence,
semantic-negative aggregation is included in the full acceptance evidence, and
latency bars are implemented.

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

Hypervisor local validation passed:

```bash
rustfmt --edition 2021 --check crates/dh-worker/src/restore_engine.rs crates/dh-worker/src/service.rs crates/dh-worker/tests/restore_engine.rs crates/dh-worker/tests/m7_fork_verify.rs crates/dh-worker/tests/common/mod.rs crates/dh-worker/tests/m6_full_api_uds.rs crates/dh-worker/tests/linux_worker_api.rs crates/dh-worker/src/m9_handoff.rs crates/dh-worker/tests/m5_frame_scheduling.rs crates/dh-worker/tests/capture_engine_real_image.rs crates/dh-worker/tests/play_perf_smoke.rs crates/dh-worker/tests/frame_capture_stream.rs crates/dh-worker/tests/m4_transparency.rs
cargo test -p dh-worker take_snapshot_rolls_full_manifest_and_restore_accepts_baseline_delta -- --nocapture
cargo test -p dh-worker --test restore_engine delta_chain_restore_materializes_the_full_state -- --nocapture
cargo test -p dh-worker --test m7_fork_verify replay_commit_matcher_allows_slot_drift_but_rejects_ref_drift
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

- Repair or migrate the beads dependency table so the intended graph can be
  represented with real `bd dep` edges.
- Confirm the new M8 workflow lanes in GitHub, record required-check status,
  and capture bounded-CI/full-acceptance sign-off for `snapshot-store-2dl`.
- Integrate live M8 resumability, baseline-delta smoke aggregation,
  semantic-negative aggregation into the full acceptance evidence, and latency
  bars around the new replay-commit evidence path.
- Run the hardware-gated Phase 5 rows and the 1000x M8 acceptance on a
  qualified NVMe-class store root.
