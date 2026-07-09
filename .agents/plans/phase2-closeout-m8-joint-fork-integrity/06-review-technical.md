# Technical Review

Reviewer: subagent Franklin, 2026-07-09. Scope: technical correctness of the
M8 harness, ref-identity assertion, shared-page measurement, semantic
negative, and current code anchors.

## Findings

| Severity | Finding | Resolution |
|---|---|---|
| High | Baseline-delta restore was underspecified. Passing `baseline_ref` into the current fresh-slot restore would be wrong because Mode B returns only delta pages while `restore_engine` requires full RAM coverage. | Accepted. `03-wiring-and-performance.md` now requires a baseline-resident restore path, baseline validation, delta-only coverage rules, and full-restore fallback. |
| High | The plan required `replay_ref` but did not force a replayed `PutSnapshot` ref to exist. Current `VerifyReplay` reports state hash only. | Accepted. `02-harness-and-inventory.md` now requires an explicit replay-commit path and fake tests that fail if only state hash exists. |
| Medium | The semantic negative could pass with VerifyReplay divergence only, which does not prove the M8 ref-identity gate trips. | Accepted. The negative must now commit a replay ref and prove `replay_ref != original_ref`; VerifyReplay divergence is diagnostic only. |
| Medium | Per-child dedup counters are not currently exposed, and `dedup_ratio` conflicts with the store-wide stat name. | Accepted. The plan now uses `shared_page_ratio`, defines the denominator, and makes parent/child manifest page-hash comparison the primary measurement. |
| Medium | Snapshot-store CI permanence was too loose; stale evidence validation is not a live regression for the current SHA. | Accepted. `04-ci-and-evidence.md` now requires a required check with fresh evidence for the exact snapshot-store SHA, or signed deviation. |

No files were edited by the reviewer.
