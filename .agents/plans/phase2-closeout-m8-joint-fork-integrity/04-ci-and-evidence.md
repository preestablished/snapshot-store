# WI4 - CI Permanence And Evidence Contract

M8 is not only an operator-run acceptance. The implementation must leave a
permanent determinism regression in both snapshot-store and
determinism-hypervisor CI, with any bounded/full split made explicit.

## CI Shape

The hypervisor already has the closest precedent:

| Lane | Anchor | Meaning |
|---|---|---|
| KVM PR lane | `../determinism-hypervisor/.github/workflows/ci.yaml:87` | Self-hosted `kvm-intel` job for live KVM tests |
| M7 nightly canary | `../determinism-hypervisor/.github/workflows/nightly-drift.yaml:118` | 100-child M7 VerifyReplay canary |
| Linux M7 nightly canary | `../determinism-hypervisor/.github/workflows/nightly-drift.yaml:154` | Linux fixture 100-child canary |

Required M8 permanence:

| Repo | Required lane |
|---|---|
| determinism-hypervisor | Extend the existing `nightly-drift` M7 canary to run the M8 ref-identity variant with bounded child count, default 100 |
| snapshot-store | Add a required check that either runs the bounded M8 ref-identity variant against this exact snapshot-store SHA or validates fresh paired evidence generated against this exact SHA |

The full 1000-child run may remain operator-dispatched if it is too expensive
for required PR CI. If so, record this as a deviation from the literal
"permanent in CI" wording, with:

| Field | Required value |
|---|---|
| Required check | Name and URL of bounded job in each repo |
| Full acceptance | Command and workflow/manual runbook for 1000-child acceptance |
| Sign-off | Phases-track approval that bounded required + operator full satisfies M8 permanence |
| Failure policy | P0 on any ref-identity or replay divergence |

Do not silently call an operator-only 1000x command "CI".

## Snapshot-Store CI Options

Pick one after implementation reality is clear:

| Option | When to choose | Notes |
|---|---|---|
| Direct cross-repo KVM job | Snapshot-store has access to the same self-hosted `kvm-intel` labels | Strongest mirror of hypervisor gate; required check name should be stable, e.g. `m8-ref-identity-bounded`; check out control-plane, guest-sdk, determinism-hypervisor, and snapshot-store as siblings |
| Evidence-validator job with signed deviation | Snapshot-store cannot run live KVM | The paired hypervisor workflow must upload fresh `evidence.json`; snapshot-store CI validates schema, refs, `shared_page_ratio`, semantic-negative result, and exact snapshot-store SHA |
| Host-only fake harness | Only as supplement | Useful for PR speed, not sufficient for M8 permanence by itself |

Scheduled-only snapshot-store runs are supplements, not compliance. If direct
KVM is unavailable for snapshot-store, the evidence-validator job must be
required and must fail on stale evidence, missing child rows, failed bars,
missing branch-protection check name, or repo SHA mismatch. Freshness must be
defined mechanically: same commit SHA, same branch/PR context, and an evidence
`finished_at` from the current workflow run or paired workflow run.

## Evidence JSON Schema

Commit a typed validator or JSON schema for M8 evidence. Top-level
`evidence.json` should include:

| Field | Required content |
|---|---|
| `schema_version` | Integer, starting at `1` |
| `request` | `.agents/requests/phase2-closeout-m8-joint-fork-integrity` |
| `run_kind` | `fake`, `bounded_ci`, `full_acceptance`, or `semantic_negative` |
| `expected_child_count` | Configured count; `1000` for full acceptance |
| `run_id` | Evidence root name |
| `started_at`, `finished_at` | UTC timestamps |
| `repos` | snapshot-store, determinism-hypervisor, control-plane, guest-sdk revs and dirty status |
| `host` | hostname, kernel, CPU, RAM, disk class, mount, `/dev/kvm`, runner labels |
| `guest` | guest kind, image hashes, machine config hash |
| `store_root` | Absolute path, resolved mount, disk class, and qualification result |
| `config` | jobs, slot cores, max delta chain, restore mode, child batch size |
| `child_table` | path to JSONL and CSV |
| `bars` | machine-readable pass/fail bars |
| `commands` | exact commands and env used |
| `artifacts` | relative paths to logs/raw data |
| `semantic_negative` | command, mutated input description, expected red result, actual red result |
| `deviations` | Array of deviations, each with sign-off owner/date/link or empty |

Required bars:

| Bar | Predicate |
|---|---|
| `m8_command_status` | command status `0` |
| `m8_child_count` | `1000` for full acceptance; configured value for bounded CI |
| `m8_ref_identity` | all child rows have equal original/replay refs |
| `m8_replay_done` | all child rows reached VerifyReplay Done without Divergence |
| `m8_shared_page_ratio_aggregate` | `>= 0.94` |
| `m8_restore_delta_used` | true for child hot path rows |
| `m8_full_manifest_cadence` | rollover smoke passed |
| `m8_semantic_negative_red` | true |
| `m8_store_root_qualified` | true for full acceptance and benchmark rows |
| `m8_fork_commit_p99` | compared to ARCHITECTURE §7.1 target or recorded miss with sign-off |
| `m8_restore_delta_p99` | compared to ARCHITECTURE §7.1 target or recorded miss with sign-off |

Child JSONL rows must be typed and validator-enforced:

| Field | Type | Required |
|---|---|---|
| `child_index` | integer | yes |
| `seed_hex` | 32-byte hex string | yes |
| `original_ref_hex` | 32-byte hex string | yes |
| `replay_ref_hex` | 32-byte hex string | yes for positive runs |
| `input_log_id_hex` | 32-byte hex string | yes |
| `state_hash_original_hex` | 32-byte hex string | yes |
| `state_hash_replay_hex` | 32-byte hex string | yes for positive runs |
| `restore_mode` | enum: `baseline_delta`, `full` | yes |
| `baseline_ref_hex` | 32-byte hex string or null | yes |
| `manifest_kind` | enum: `FULL`, `DELTA` | yes |
| `chain_depth` | integer | yes |
| `dirty_pages` | integer or null | yes |
| `shared_page_ratio` | float in `[0,1]` | yes |
| `timing_ms` | object of numeric timing fields | yes |
| `result` | enum: `pass`, `ref_mismatch`, `state_mismatch`, `replay_divergence`, `error` | yes |

Use the Phase 5 evidence assembler as a style precedent, but do not overload
`phase5_readiness_evidence.py` if that would blur request identities. A new
`scripts/m8_joint_fork_integrity_evidence.py` plus tests is the preferred
implementation.

## Artifact Retention

Keep raw evidence in `target/` for local runs, but the closeout must copy or
upload enough durable artifacts for review:

| Artifact | Durable destination |
|---|---|
| `evidence.json` | Request resolution link and CI artifact |
| `child-ref-table.jsonl` | CI artifact and local evidence root |
| Command logs | CI artifact or request `evidence/` summary if small |
| Bench rows | `docs/bench-baseline.md` |
| Semantic negative summary | Request resolution and evidence root |

Do not commit large raw payloads or guest images into the repo. Commit concise
human-readable summaries, schema/tests, CI workflow changes, and benchmark rows.
