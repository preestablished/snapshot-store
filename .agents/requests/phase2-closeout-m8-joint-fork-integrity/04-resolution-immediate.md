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

Hypervisor local validation passed:

```bash
rustfmt --edition 2021 --check crates/dh-worker/tests/m7_fork_verify.rs
cargo test -p dh-worker --test m7_fork_verify replay_commit_matcher_allows_slot_drift_but_rejects_ref_drift
cargo test -p dh-worker --test m7_fork_verify --no-run
git diff --check
```

## Remaining Immediate Work

- Repair or migrate the beads dependency table so the intended graph can be
  represented with real `bd dep` edges.
- Wire baseline-resident delta restore and FULL-manifest cadence before any
  full M8 run.
- Integrate live hypervisor evidence emission, resumability/shared-page
  accounting, and semantic-negative red-run output around the new replay-commit
  path.
- Run the hardware-gated Phase 5 rows and the 1000x M8 acceptance on a
  qualified NVMe-class store root.
