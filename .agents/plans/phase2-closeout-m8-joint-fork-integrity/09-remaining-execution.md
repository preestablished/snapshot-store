# Remaining Execution Plan (2026-07-11)

This file is the current handoff for the next coding agent. Earlier plan files
explain the design, but several of their implementation steps are complete.
Start here and do not reopen closed beads unless current verification fails.

## 1. Recover State And Protect Existing Work

Run sequentially because the embedded beads backend takes an exclusive lock:

```bash
bd prime
git status --short --branch
bd dolt pull
bd show snapshot-store-m0u
bd show snapshot-store-2dl
bd show snapshot-store-4ua
bd show snapshot-store-orm
bd ready
bd blocked
```

Expected state: `m0u` and `2dl` are in progress; `4ua` is blocked by `m0u`;
`orm` is blocked by the remaining children; `gy9` and `8p9` are closed. Inspect
the latest request resolution and current commits before changing code. The
snapshot-store and hypervisor implementations already contain M8 work; preserve
unrelated or newer changes in both repositories.

## 2. Finish `snapshot-store-2dl`: Prove CI Permanence

The workflow definitions exist, but definition is not proof. Resolve runner
access so snapshot-store can schedule a job with labels
`self-hosted, kvm-intel`. Prefer granting this repository access to the existing
runner group; do not weaken the job onto an unqualified hosted runner.

After access is fixed:

1. Make the snapshot-store workflow consume the hypervisor default branch once
   the M8 hypervisor commits are verified and merged there. Confirm this with
   `git branch --contains <m8-commit>` and the remote default-branch SHA; do not
   infer it merely from a local worktree containing the code. Remove the
   temporary branch default;
   retain `M8_HYPERVISOR_REF` only as an explicit diagnostic override if useful.
2. Run the bounded snapshot-store job against the exact snapshot-store SHA and
   run/confirm the hypervisor bounded and nightly lanes.
3. Download and validate the evidence with
   `python3 scripts/m8_joint_fork_integrity_evidence.py <artifact-root>`.
4. Record workflow URLs, artifact IDs, exact repository SHAs, runner name/group,
   check names, and branch-protection required-check status in the bead and
   request resolution.
5. Obtain and record phases-track sign-off for bounded required CI plus an
   operator-run full 1000-child gate. Without that sign-off, do not describe the
   literal full-in-CI requirement as satisfied.

If runner-group administration is unavailable, record the owner, requested
change, timestamp, and failing/queued run URL in `2dl`; keep the bead open.

## 3. Finish `snapshot-store-m0u`: Qualified Predecessor Rows

Coordinate a window on the operator-attested Intel/SATA reference host. The
2026-07-10 policy permits this SATA host; the missing requirements are the
attestation and counted fio/M5/M7 runs, not NVMe.

Use a large local store root on that host and a new timestamped evidence root:

```bash
SNAPSTORE_BENCH_ROOT=/absolute/path/on/reference-host \
PHASE5_EVIDENCE_ROOT=target/phase5-readiness-<UTC> \
PHASE5_ACTUAL_SOAK_HOST=true \
PHASE5_SAME_AS_I5_SATA_REFERENCE=true \
RUN_FIO=1 RUN_M5=1 RUN_M7_GC=1 RUN_FLAKE_50X=1 \
scripts/phase5-readiness-evidence.sh
```

Before accepting the result, inspect `evidence.json` and raw artifacts. Require
`hardware_qualification.qualified=true`, successful fio output, counted M5 and
M7 rows, clean repository identity, and no silent skipped bar. Record measured
misses honestly under the reference-host floor policy. Append the qualified
rows to `docs/bench-baseline.md`, update/close `m0u`, run `bd dolt push`, and
confirm `4ua` becomes ready.

## 4. Run The Semantic Negative And Full 1000-Child Acceptance

Schedule through the bridge/runtime owner. Avoid concurrent long frame-capture
streams and preserve the same qualified store root and clean repository/image
identities for the complete session.

In the hypervisor repository, first run the FULL-cadence smoke and write its
success marker into the parent evidence root. Then run the semantic negative
into the same root that the positive run will link, followed by the positive
gate. Use the exact command names present in the current sibling checkout;
the expected shape is:

```bash
mkdir -p target/m8-joint-fork-integrity-<UTC>
cargo test -p dh-worker --lib \
  take_snapshot_rolls_full_manifest_and_restore_accepts_baseline_delta \
  -- --nocapture --test-threads=1
touch target/m8-joint-fork-integrity-<UTC>/full-cadence-smoke.ok

M8_STORE_ROOT=/absolute/path/on/reference-host \
M8_EVIDENCE_ROOT=target/m8-joint-fork-integrity-<UTC> \
M8_STORE_ROOT_QUALIFIED=1 \
M8_STORE_ROOT_DISK_CLASS=sata \
cargo test -p dh-worker --test m7_fork_verify --release \
  m8_accept_semantic_negative_replay_commit_ref_mismatch \
  -- --ignored --nocapture --test-threads=1

DH_M9_ALLOW_SKIP=0 DH_M7_ACCEPT_GUEST=linux \
DH_M7_ACCEPT_SLOT_CORES=2-5 DH_M7_ACCEPT_JOBS=1000 \
M8_STORE_ROOT=/absolute/path/on/reference-host \
M8_EVIDENCE_ROOT=target/m8-joint-fork-integrity-<UTC> \
M8_STORE_ROOT_QUALIFIED=1 \
M8_STORE_ROOT_DISK_CLASS=sata \
cargo test -p dh-worker --test m7_fork_verify --release \
  m8_accept_1000_seeded_forks_replay_commit_ref_identity \
  -- --ignored --nocapture --test-threads=1
```

If interrupted, preserve the evidence root and resume only after confirming the
same snapshot-store SHA, hypervisor SHA, guest image, store root, job universe,
and configuration. Set `M8_EVIDENCE_RESUME=1` for the resumed positive run.
Never splice rows from different identities into one accepted table.

Validate the finished evidence with the snapshot-store validator. Acceptance
requires exactly 1000 contiguous child rows, 1000/1000 original/replay ref
equality, replay completion, at least one baseline-delta restore, FULL-cadence
smoke evidence, aggregate shared-page ratio at least 0.94, qualified store root,
measured fork-to-commit and restore latency summaries, and a linked committed
semantic-negative ref mismatch.

## 5. Benchmark, Closeout, And Durable Handback

Append the M8 fork-to-commit, baseline-delta restore, and sibling-sharing rows
to the same qualified-host section of `docs/bench-baseline.md`. Compare latency
actuals to ARCHITECTURE section 7.1 and explain any accepted reference-host
floor or deviation.

Write `.agents/requests/phase2-closeout-m8-joint-fork-integrity/05-resolution.md`
with:

- exact SHAs and clean/dirty state for both repositories and guest image;
- qualified host/store-root identity and Phase 5 predecessor evidence root;
- M8 evidence root/artifact URLs, 1000/1000 result, sharing ratio, and latency
  percentiles;
- semantic-negative command and committed ref-mismatch result;
- bounded CI workflow/check URLs, branch-protection status, and deviation
  sign-off;
- closed beads and any linked defects.

Run relevant unit/workflow checks in both changed repositories. At minimum in
snapshot-store:

```bash
DD_TRACE_ENABLED=false DD_CIVISIBILITY_ENABLED=false \
  python3 scripts/m8_joint_fork_integrity_evidence_test.py
DD_TRACE_ENABLED=false DD_CIVISIBILITY_ENABLED=false \
  python3 scripts/m8_joint_fork_integrity_fake_harness_test.py
python3 scripts/m8_joint_fork_integrity_evidence.py <full-evidence-root>
git diff --check
```

Close `2dl` only after live green checks and sign-off are recorded. Close
`4ua` only after the qualified full evidence and benchmark rows pass. Close
`orm` only when all children and the final resolution are complete. File beads
for real residual defects rather than hiding them in prose.

Finish each changed repository according to its current `AGENTS.md` and
`bd prime`: pull/rebase safely, push beads, commit intentionally, push Git, and
verify `git status` reports the branch up to date with its remote. Do not
force-push beads or Git history.
