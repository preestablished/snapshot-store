# WI1 - Tracker Reconciliation And Entry State

The first implementer action is to reconcile the request's bead assumptions
against the current beads database. Do this before editing code or creating M8
evidence.

## Revalidate Beads State

Run the first pass as read-only and sequentially; the embedded Dolt backend can
lock if `bd` commands are parallelized.

```bash
bd prime
bd dolt pull
bd list --all
bd search snapstore-675
bd search snapstore-pov
bd search snapstore-28z
bd search snapstore-feb
bd search snapstore-nn4
```

Current planning observation: `bd dolt pull` succeeds and the request's named
beads are absent. That means the old `snapstore-pov` divergence is not
currently reproducible in this checkout. After graph edits are ready, run
`bd preflight` and `bd dolt push`. If that push succeeds, do not perform owner
escalation for `pov`; record it as "request condition already resolved or not
present in this bead db." If it fails with the "no common ancestor" failure
described by the request, stop tracker mutation and get the owner decision the
request requires. Do not force-push beads.

## Rebuild Or Import The M8 Bead Graph

If the named beads are restored by a later `bd dolt pull`, update the existing
records. If they remain absent, create replacement beads in the current db.
Prefer stable, descriptive titles over trying to recreate old opaque IDs.

If replacement beads are needed, create them with concrete descriptions and
then add dependency edges. Example command shape:

```bash
bd create --title="M8 joint fork-integrity closeout" --description="Correct stale M8 blocker, coordinate Phase 2 joint fork-integrity closeout, and track child work for harness, qualified run, CI permanence, and handback." --type=task --priority=0
bd create --title="M8 predecessor: qualified Phase 5 hardware rows" --description="Run or record accepted escalation for the NVMe-class M5 transport and M7 GC rows that gate M8 benchmark closeout." --type=task --priority=0
bd create --title="M8 harness: replay-commit ref identity" --description="Inventory the hypervisor M7 harness, add replay-commit ref capture, fake-backed tests, shared-page accounting, resumable child table, and semantic ref-divergence negative." --type=task --priority=0
bd create --title="M8 smoke: baseline-resident restore and FULL cadence" --description="Wire baseline-resident delta restore and max_delta_chain FULL-manifest cadence, then prove them with a small smoke before the full session." --type=task --priority=0
bd create --title="M8 acceptance: 1000x joint fork ref identity" --description="Run the coordinated 1000-child joint acceptance on qualified hardware, record refs, replay commits, shared-page ratio, and latency rows." --type=task --priority=0
bd create --title="M8 permanence: required regression in both repos" --description="Install required bounded M8 ref-identity checks in snapshot-store and determinism-hypervisor, with deviation sign-off if full 1000x remains operator-run." --type=task --priority=0

# Replace the placeholders below with the IDs emitted by the create commands.
EPIC=REPLACE_WITH_M8_EPIC_ID
HW=REPLACE_WITH_PHASE5_HARDWARE_ID
HARNESS=REPLACE_WITH_M8_HARNESS_ID
SMOKE=REPLACE_WITH_M8_SMOKE_ID
JOINT=REPLACE_WITH_M8_JOINT_RUN_ID
CI=REPLACE_WITH_M8_CI_ID
bd dep add "$SMOKE" "$HARNESS"
bd dep add "$JOINT" "$HW"
bd dep add "$JOINT" "$SMOKE"
bd dep add "$CI" "$SMOKE"
bd dep add "$EPIC" "$HARNESS"
bd dep add "$EPIC" "$HW"
bd dep add "$EPIC" "$JOINT"
bd dep add "$EPIC" "$CI"
bd preflight
bd dolt push
```

Recommended graph semantics:

| Bead purpose | Gate | Notes |
|---|---|---|
| M8 tracker correction / epic | immediate | Records that M8 is Phase 2 debt and the hypervisor M4-M7 blocker is satisfied |
| M8 harness + fake-backed tests | immediate | No hardware or guest prerequisite; can run with fake store/hypervisor components |
| M8 restore and FULL-cadence smoke | after harness | Cross-repo code path, small child count |
| Qualified Phase 5 hardware rows | hardware gate | Must cover the unresolved `28z`/`feb` equivalent rows on NVMe-class soak host |
| M8 joint 1000x acceptance + BM rows | blocked by hardware rows and smoke | Produces the evidence root and `docs/bench-baseline.md` rows |
| M8 CI permanence + cross-repo handback | blocked by smoke; full run evidence needed for final close | Required in both repos |

The joint 1000x acceptance depends on the qualified Phase 5 rows and on the
restore/FULL-cadence smoke. The harness bead does not depend on the hardware
rows. Push only after `bd preflight` or an equivalent graph audit shows the
dependencies match this shape.

## Correct The Stale Blocker

The request says `snapstore-675` described M8 as blocked on
determinism-hypervisor M4-M7. The current plan should record that this is stale,
but only after verifying the sibling repo's tracker or evidence files in the
implementation session.

Minimum evidence to cite:

| Hypervisor prerequisite | Evidence surface to verify |
|---|---|
| M4 fork transparency / frozen-parent reproducibility | Hypervisor bead `-a6s` if present, or matching resolution/evidence in sibling repo |
| Tier-A CoW fork through real store | Hypervisor bead `-9e4` if present, plus real-store path deps in `../determinism-hypervisor/Cargo.toml:44` |
| M7 1000x VerifyReplay | `../determinism-hypervisor/crates/dh-worker/tests/m7_fork_verify.rs:1557` and historical evidence/bead `-cw2` if present |
| Linux M7 fork VerifyReplay + nightly 100-child canary | `../determinism-hypervisor/.github/workflows/nightly-drift.yaml:154` and bead `-4s9.29` if present |

If the old bead is absent, put this correction into the new M8 epic and the
request resolution instead of pretending an unavailable record was edited.

## Entry Conditions

Immediate lane entry:

| Condition | Required state |
|---|---|
| Beads writable | `bd dolt pull` and `bd dolt push` both succeed, or the failure is recorded with owner escalation |
| Harness scope known | Hypervisor M7 harness inventory completed and host repo chosen |
| Store APIs available | Existing store/client `resolve_pages` baseline path confirmed; no new store API invented |

Gated lane entry:

| Condition | Required state |
|---|---|
| Phase 5 hardware rows | M5 transport and M7 GC rows measured on qualified NVMe-class soak host, or hardware escalation recorded and accepted |
| Flake | `page_channel_fallback` remains green in the current checkout |
| Restore smoke | Baseline-delta restore and FULL cadence have a small-count proof |
| Session window | Hypervisor, bridge/runtime owner, and store operator agree on the shared box window |

## Drift Handling

The implementation plan must not blindly follow obsolete request facts. If a
requested bead, blocker, or evidence root is missing, record the discrepancy in
the active bead and in `.agents/requests/phase2-closeout-m8-joint-fork-integrity/04-resolution-immediate.md`.
The acceptable outcomes are "edited existing record" or "replacement record
created because the named record is absent"; silent omission is not acceptable.
