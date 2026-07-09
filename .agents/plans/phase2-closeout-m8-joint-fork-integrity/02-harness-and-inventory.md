# WI2 - Harness Inventory And M8 Ref-Identity Tooling

This work is intentionally ungated by the Phase 5 NVMe hardware rows. It should
produce a fake-testable harness and evidence model before anyone schedules the
full guest session.

## Inventory First

Start in the hypervisor repo and answer these questions in the implementing
bead notes:

| Question | Current anchor |
|---|---|
| Does the M7 harness already drive the real store? | `../determinism-hypervisor/crates/dh-worker/tests/m7_fork_verify.rs:171` spawns a snapstore server; `../determinism-hypervisor/Cargo.toml:44` path-deps `snapstore-client` |
| Where are child refs and logs collected? | `ChildRecord` at `m7_fork_verify.rs:94`; `snapshot_record` at `:596`; `fetch_log_payload` at `:1007` |
| Where are children run in batches? | `run_child_batch` at `m7_fork_verify.rs:923`; full 1000x loop at `:1590` |
| Where is replay verified? | `verify_child` at `m7_fork_verify.rs:1264`; `verify_batch` at `:1369` |
| Does replay currently return a committed ref? | No. `VerifyDone` has no snapshot field in `../determinism-hypervisor/proto/hypervisor.proto:357`, and M7 compares only state hash |
| Where should evidence be emitted? | No M8 evidence root exists yet; Phase 5 precedent is `scripts/phase5-readiness-evidence.sh:20` and `scripts/phase5_readiness_evidence.py:678` |

Default decision: extend the hypervisor M7 acceptance harness because it already
owns fork, run, log, replay, and KVM runner setup. Add store-side evidence and
assertions around it. A store-hosted harness is acceptable only if the inventory
finds that reusing M7 would make the store assertions indirect or fragile.

## M8 Assertion Model

For each child index `i`, the harness must persist this row:

| Field | Meaning |
|---|---|
| `child_index` | 0-based child ordinal in the 1000x universe |
| `seed` | Entropy seed used for the child fork |
| `original_ref` | Snapshot ref returned by the original child `TakeSnapshot` / store `PutSnapshot` |
| `input_log_id` | Stored DHILOG/input-log id for the child burst |
| `replay_ref` | Snapshot ref returned by the explicit M8 replay-commit path |
| `state_hash_original` | State hash captured by original child snapshot |
| `state_hash_replay` | State hash returned by replay execution |
| `restore_mode` | `baseline_delta` or `full`; M8 requires baseline-delta for the hot path |
| `dirty_pages` | Dirty page count reported by the worker if available |
| `pages_shipped` | Pages sent to the store for this child, if instrumentation exposes it |
| `pages_new` | Pages newly stored during this child commit, if instrumentation exposes it |
| `shared_page_ratio` | Sibling sharing ratio, with denominator defined below |
| `timing_ms` | Fork, run, original commit, restore, replay, replay commit |

The acceptance assertion is strict:

```text
for every child row:
  replay_ref == original_ref
  state_hash_replay == state_hash_original
  VerifyReplay reports Done and no Divergence
```

The `PutSnapshot` ref equality is not replaceable with state hash equality. A
state hash proves guest identity; the returned ref proves store bit identity of
the manifest/device blob/page hash set.

## Replay-Commit Path

M8 must add a path that actually commits the replayed child state back to
snapshot-store and exposes the returned ref. The current M7 path is insufficient:
`VerifyReplay` streams progress and `Done.end_state_hash`, but no
`PutSnapshot` ref.

Choose one implementation and test it directly:

| Option | Requirements |
|---|---|
| Extend VerifyReplay | Add an opt-in request field such as `commit_replay_snapshot`; at replay END, seal the replayed state through the same snapshot path and include `replay_snapshot_ref` in `VerifyDone` or a terminal M8 event |
| Harness-driven replay commit | Restore the base snapshot, re-drive the recorded child burst through the worker, call `TakeSnapshot`, and compare that returned ref to the original child ref |

The second option avoids proto churn if the harness can drive the same events
deterministically. The first option is cleaner if the replay engine already has
the exact restored slot at END and can call the worker snapshot path without
duplicating harness control flow. Either way, a fake-backed test must fail when
only state-hash equality is available and `replay_ref` is absent.

## Resumability

Persist the child table incrementally as JSONL under the evidence root:

```text
target/m8-joint-fork-integrity-<UTC>/
  evidence.json
  child-ref-table.jsonl
  child-ref-table.csv
  logs/
  raw/
  hypervisor/
  snapstore/
  hardware/
```

The harness should resume from completed rows by child index and original ref.
Content addressing makes re-runs idempotent, but the evidence must distinguish
"reused completed row" from "freshly re-executed row". A resumed run passes only
if every row was produced by the same store rev, hypervisor rev, guest image,
and run configuration, or if the resume file explicitly records the boundary
and the operator accepts the split.

## Fake-Backed Tests

Before the live KVM session, add tests that do not need the reference box:

| Test | Required behavior |
|---|---|
| Ref equality pass | Fake store returns identical original/replay refs for every child; harness passes and writes complete evidence |
| Ref divergence fail | One fake child returns a different replay ref; harness fails and names the child |
| Missing replay ref fail | Fake replay reports state-hash success but no replay commit ref; harness fails |
| State divergence fail | Replay state hash differs even if ref is equal; harness fails and names both identities |
| Resume | First run stops mid-table; second run skips completed rows and finishes with one coherent evidence file |
| Shared-page accounting | Known parent/child page hashes or instrumentation counters produce the expected aggregate sibling sharing |

Keep these tests in the host repo chosen by the inventory. If the live harness
stays in the hypervisor repo, a small store-side parser/validator test may live
in snapshot-store, but do not duplicate the execution harness.

## Semantic Corruption Negative

The negative test must mutate deterministic input before sealing or drive a
modified input burst, then prove the gate turns red because the replayed child
returns a different ref/state. Do not flip raw manifest or pack bytes after the
fact. Raw bit flips are caught by container/footer/checksum validation and do
not prove the M8 determinism regression.

Recommended shape:

1. Run child `k` with seed/input burst `B` and record `original_ref`.
2. Re-execute the M8 replay-commit path using a deliberately changed burst `B'`.
3. Assert the harness reports the child as failed because it committed
   `replay_ref != original_ref`. VerifyReplay `Divergence` is useful diagnostic
   evidence, but it is not sufficient for the required negative by itself.
4. Persist the red-run evidence under `semantic-negative/` and link it from
   `evidence.json`.

## Shared-Page Measurement

The sharing number must be measured from store-visible facts, not inferred only
from the hypervisor dirty log. Use the evidence field name
`shared_page_ratio` to avoid confusion with the store-wide `dedup_ratio` stat.
Acceptable sources, in order:

| Source | Notes |
|---|---|
| Page hash comparison between parent and child manifests | Primary measurement unless new per-child counters are added; compare non-zero page hashes over the guest RAM page universe |
| Store PutPages/PutSnapshot metrics or response counters | Good supplement if the worker exposes pages shipped/new/deduped per child |
| Dirty page log | Useful diagnostic, but not enough by itself because M8 measures store sibling sharing |

Definition:

```text
shared_page_ratio(child) =
  count(non_zero_pages where child_page_hash == parent_page_hash)
  / count(non_zero_pages in the child guest RAM universe)
```

Aggregate pass bar: sibling `shared_page_ratio >= 0.94`, with per-child min,
p50, p95, and aggregate values recorded. If a real guest workload produces
legitimate lower sharing, stop and get phases-track sign-off rather than
weakening the bar locally.
