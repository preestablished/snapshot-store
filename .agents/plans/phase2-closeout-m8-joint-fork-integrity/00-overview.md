# M8 Joint Fork-Integrity Closeout Plan

Plan for `.agents/requests/phase2-closeout-m8-joint-fork-integrity/`, drafted
2026-07-09 on branch `phase2-closeout-m8-joint-fork-integrity` at
`11646c4`.

This is a handoff plan for another implementation agent. It does not implement
M8. It converts the July 7 request into current-state-aware work items and
keeps the original sequencing rule: guest-free tracker and harness work can
start immediately; the full joint run waits for the Phase 5 hardware-bound
predecessors to be resolved or explicitly escalated.

Tracking bead for this plan artifact: `snapshot-store-dgj`.

## Files In This Plan

| File | Contents |
|---|---|
| `01-tracker-and-entry.md` | Reconcile stale request assumptions, rebuild/verify bead graph, and record entry conditions |
| `02-harness-and-inventory.md` | Inventory and extend the hypervisor M7 harness for M8 ref-identity, shared-page measurement, resumability, and semantic negative tests |
| `03-wiring-and-performance.md` | Complete baseline-delta restore, FULL-manifest cadence, smoke tests, joint run, and benchmark rows |
| `04-ci-and-evidence.md` | Required checks in both repos, evidence schema, artifacts, and deviation handling |
| `05-closeout-and-handback.md` | Resolution files, bead closeout, cross-repo handback, and session hygiene |
| `06-review-technical.md` | Technical-correctness subagent review |
| `07-review-operability.md` | Operability/completeness subagent review |
| `08-review-resolution.md` | Accepted findings and changes folded into this plan |

## Current State Verified During Planning

The request's July 7 state has drifted. Use the state below, not the filing text,
as the starting point:

| Surface | Current fact |
|---|---|
| Local snapshot-store head | `11646c4` on `phase2-closeout-m8-joint-fork-integrity`, same commit as `main` / `origin/main` |
| Beads db | `bd dolt pull` succeeds; `bd list --all` shows only the earlier closed work plus this plan bead; request beads `snapstore-675`, `snapstore-pov`, `snapstore-28z`, `snapstore-feb`, and `snapstore-nn4` are absent from the current db |
| Phase 5 plan | `.agents/plans/phase5-readiness-gc-benchmark-and-transport-revalidation/` now exists |
| Phase 5 evidence | `target/phase5-readiness-20260708T180021Z/evidence.json` exists and is marked hardware-unqualified; M5/M7 hardware-gated runs were not run |
| Bench baseline | `docs/bench-baseline.md` records the Phase 5 local preflight and says counted reference-host transport/GC rows still block before closing `snapstore-28z` / `snapstore-feb` |
| Store resolve API | `SnapshotStore::resolve_pages` supports `baseline: Option<&SnapshotRef>` and `hashes_only` modes at `crates/snapstore-store/src/lib.rs:553` |
| Store gRPC/client path | `ResolvePagesRequest.baseline_ref` is parsed and forwarded at `crates/snapstore-server/src/service.rs:306`; client exposes it at `crates/snapstore-client/src/client.rs:251` and blocking wrapper at `crates/snapstore-client/src/blocking.rs:56` |
| Hypervisor restore gap | `../determinism-hypervisor/crates/dh-worker/src/restore_engine.rs:162` still calls `resolve_pages(snapshot_ref, None, false)` into a full-coverage fresh-slot restore path |
| Replay ref gap | `VerifyReplay` returns `Done{total_icount,end_state_hash}` but no committed snapshot ref in `../determinism-hypervisor/proto/hypervisor.proto:357`; M8 must add an explicit replay-commit path |
| Hypervisor M7 harness | `../determinism-hypervisor/crates/dh-worker/tests/m7_fork_verify.rs:1557` has the ignored 1000-fork VerifyReplay acceptance; it forks in batches, validates DHILOG lineage, and verifies every child |
| Hypervisor CI precedent | `../determinism-hypervisor/.github/workflows/nightly-drift.yaml:118` runs a 100-child M7 canary on `kvm-intel`; full 1000-child acceptance remains operator-run |

Normative anchors:

| Source | Requirement |
|---|---|
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/IMPLEMENTATION-PLAN.md:140` | M8 is the joint hypervisor integration + determinism regression milestone |
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/IMPLEMENTATION-PLAN.md:145` | Worker must use FULL-manifest cadence and baseline-delta restore |
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/IMPLEMENTATION-PLAN.md:147` | Fork one guest 1000x, restore + re-execute each child, require identical returned refs |
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/IMPLEMENTATION-PLAN.md:150` | Regression becomes permanent in both repos' CI |
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/IMPLEMENTATION-PLAN.md:152` | BM rows: fork-to-commit, restore latencies, sibling dedup/shared-page ratio >= 94%, recorded in `docs/bench-baseline.md` |
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/INTEGRATION.md:69` | Hot-loop restore uses `ResolvePages(S, baseline_ref=A, hashes_only)` for delta top-up |
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/INTEGRATION.md:105` | Worker emits FULL manifests when delta `chain_depth` would exceed `max_delta_chain` |
| `/home/infra-admin/.agents/projects/determinism/docs/snapshot-store/ARCHITECTURE.md:477` | §7.1 p50/p99 targets for commit, restore, metadata, and query operations |

## Work Item Sequence

`WI1` tracker reconciliation and entry-state verification happens first. It
must make the current bead situation explicit before anyone edits milestone
records.

`WI2` harness inventory and fake-testable M8 harness additions can proceed
without the NVMe hardware gate. It should extend the hypervisor M7 harness
unless the inventory proves a smaller store-hosted harness is safer.

`WI3` restore/FULL-cadence wiring smoke is the first cross-repo code path and
should land before the full 1000-child session.

`WI4` qualified Phase 5 hardware rows and the M8 joint run share the same
reference box session. Do not run M8 numbers on a SATA root and call them the
gate.

`WI5` CI permanence lands in both repos after the smoke and before closeout.
The bounded check may be smaller than 1000 children, but any non-CI full 1000x
shape is a recorded deviation requiring sign-off.

## Pass/Fail Rules

The milestone passes only when all of these are true:

| Requirement | Pass bar |
|---|---|
| Tracker | Bead graph exists in the current db, with real dependency edges for hardware-gated versus immediate work |
| Harness | Fake-backed tests prove replay-commit ref capture, ref-table persistence, ref-equality assertion, shared-page accounting, and semantic-corruption negative behavior |
| Wiring | Hypervisor restore exercises a baseline-resident delta mode; worker tracks FULL cadence by chain depth |
| Joint run | 1000/1000 children restore, re-execute, and return the same `PutSnapshot` ref as the original child |
| Benchmarks | fork-to-commit and restore latency rows are compared to ARCHITECTURE §7.1; sibling `shared_page_ratio` is >= 94%; actuals land in `docs/bench-baseline.md` |
| CI | Both repos have linked required regression lanes using fresh evidence for the exact snapshot-store SHA; any smaller permanent lane versus full 1000x is explicitly approved and recorded |

## Non-Goals

Do not expand this plan into the vendored-proto swap (`snapstore-8qx` in the
request), M9, or hypervisor OOM/capture-engine proof work. If the M8 run finds
a hypervisor defect, file/link it in the hypervisor tracker and keep this plan
focused on store-side M8 closeout.
