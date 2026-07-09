# Review Resolution

Two independent subagent reviews were run on the initial draft:

| Reviewer | Focus |
|---|---|
| Franklin | Technical correctness: restore semantics, replay ref capture, semantic negative, shared-page measurement, CI freshness |
| Locke | Operability/completeness: tracker graph, hardware gate, CI permanence, evidence schema, closeout |

Both reviewers identified the same two blockers: the draft did not force a real
replay `PutSnapshot` ref, and it treated baseline-delta restore like a simple
parameter change even though the current hypervisor restore writes into a fresh
slot. Both findings were accepted.

## Changes Folded In

| Area | Change |
|---|---|
| Replay ref identity | Added a mandatory replay-commit path in `02-harness-and-inventory.md`; M8 must either extend VerifyReplay to commit/report a replay snapshot ref or have the harness re-drive the burst and call `TakeSnapshot`. |
| Baseline-delta restore | Rewrote `03-wiring-and-performance.md` to require baseline-resident RAM, baseline identity validation, delta-page application, mode-specific coverage checks, and full-restore fallback. |
| Semantic negative | Tightened the negative to require a committed `replay_ref != original_ref`; VerifyReplay divergence alone is only diagnostic. |
| Shared-page measurement | Renamed `dedup_ratio` to `shared_page_ratio`, defined the denominator, and made manifest page-hash comparison the primary measurement unless new counters are added. |
| Hardware root | Added `M8_STORE_ROOT` / equivalent and evidence qualification for the actual snapstore data root on the qualified NVMe mount. |
| CI permanence | Required fresh bounded M8 evidence for the exact snapshot-store SHA in a required check; scheduled-only runs are supplemental, and evidence-validator-only compliance requires signed deviation. |
| Beads | Split read-only bead revalidation from graph mutation and added concrete replacement bead/dependency commands. |
| Evidence schema | Added mandatory typed schema/validator fields and pass/fail bars. |
| Closeout | Added a per-repo closeout checklist and an explicit requirement to publish the plan artifact. |

## Remaining Intentional Flexibility

The plan still leaves two implementation choices open because the right answer
depends on code touched by the implementer:

| Choice | Constraint |
|---|---|
| Extend VerifyReplay vs harness-driven replay commit | Either is acceptable only if a real replay `PutSnapshot` ref is committed and compared. |
| Direct snapshot-store KVM job vs fresh evidence-validator required check | Direct KVM is preferred. Validator-only compliance needs fresh exact-SHA evidence and phases-track deviation sign-off. |
