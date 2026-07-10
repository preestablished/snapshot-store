# Operability Review

Reviewer: subagent Locke, 2026-07-09. Scope: tracker sequencing, current-state
drift, Phase 5 hardware gate, CI permanence, evidence schema, and closeout
completeness.

## Findings

| Severity | Finding | Resolution |
|---|---|---|
| Critical | The plan required `replay_ref == original_ref` but did not define how `replay_ref` is obtained. | Accepted. The plan now requires an explicit replay-commit path and fake tests for missing replay refs. |
| Critical | Baseline-delta restore needed baseline RAM materialization; a naive parameter change would fail. | Accepted. The restore section now requires a resident baseline lease/cache, validation, delta application, and mode-specific coverage checks. |
| High | The M8 run was not tied to the qualified reference data root; the existing M7 harness uses ordinary tempdirs. | Accepted. `03-wiring-and-performance.md` now requires `M8_STORE_ROOT` or equivalent under the qualified reference mount and evidence failure if it is not qualified. |
| High | Snapshot-store CI permanence allowed scheduled-only or vague validator shapes. | Accepted. `04-ci-and-evidence.md` now requires a required check and exact-SHA fresh evidence, with scheduled-only called supplemental. |
| Medium | WI1 pushed too early and replacement bead creation was vague. | Accepted. `01-tracker-and-entry.md` now separates read-only revalidation from graph mutation and provides concrete `bd create` / `bd dep add` shape. |
| Medium | Evidence was prose rather than an implementable schema. | Accepted. `04-ci-and-evidence.md` now requires a typed validator/schema with `schema_version`, `run_kind`, `expected_child_count`, `store_root`, bars, and deviations. |
| Medium | Cross-repo closeout ordering was unclear. | Accepted. `05-closeout-and-handback.md` now includes a per-repo closeout checklist. |
| Low | The plan artifact was untracked during review. | Accepted. `05-closeout-and-handback.md` now says the plan must be committed or otherwise published before implementation handoff; this planning session will stage/commit it. |

No files were edited by the reviewer.
