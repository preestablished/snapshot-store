# M8 Joint Fork-Integrity Closeout Plan

Plan for `.agents/requests/phase2-closeout-m8-joint-fork-integrity/`, originally
drafted 2026-07-09 and refreshed 2026-07-11 on `main` at `9f263bd`.

This is a handoff plan for another implementation agent. The tracker repair,
fake-backed tooling, cross-repo replay-commit harness, baseline/FULL wiring,
and workflow definitions have already landed. Do not reimplement those work
items. The live hardware and CI closeout remains. `09-remaining-execution.md`
is the authoritative starting point; files `01` through `05` preserve design
and acceptance detail needed when diagnosing failures.

Tracking beads for the plan artifacts: closed original `snapshot-store-dgj`
and refresh `snapshot-store-7i0`.

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
| `09-remaining-execution.md` | Current, dependency-ordered implementation handoff for unfinished work |
| `10-review-technical-refresh.md` | 2026-07-11 technical review of the refreshed plan |
| `11-review-operability-refresh.md` | 2026-07-11 operability review of the refreshed plan |
| `12-review-resolution-refresh.md` | Accepted fresh-review findings and resulting edits |

## Current State Verified During Planning

The request's July 7 state and the original July 9 plan have drifted. Use the
state below and `09-remaining-execution.md`, not the filing text, as the
starting point:

| Surface | Current fact |
|---|---|
| Local snapshot-store head | `9f263bd` on `main`; the implementation merge is locally 16 commits ahead of `origin/main` at refresh time and must be pushed during this planning session |
| Beads db | Replacement graph is installed: `orm` epic; closed `gy9` harness and `8p9` smoke; in-progress `m0u` hardware and `2dl` CI permanence; open/blocked `4ua` full acceptance |
| Phase 5 plan | `.agents/plans/phase5-readiness-gc-benchmark-and-transport-revalidation/` now exists |
| Phase 5 evidence | `target/phase5-readiness-20260708T180021Z/evidence.json` exists and is marked hardware-unqualified; M5/M7 hardware-gated runs were not run |
| Bench baseline | `docs/bench-baseline.md` records the Phase 5 local preflight and says counted reference-host transport/GC rows still block before closing `snapstore-28z` / `snapstore-feb` |
| Store resolve API | `SnapshotStore::resolve_pages` supports `baseline: Option<&SnapshotRef>` and `hashes_only` modes at `crates/snapstore-store/src/lib.rs:553` |
| Store gRPC/client path | `ResolvePagesRequest.baseline_ref` is parsed and forwarded at `crates/snapstore-server/src/service.rs:306`; client exposes it at `crates/snapstore-client/src/client.rs:251` and blocking wrapper at `crates/snapstore-client/src/blocking.rs:56` |
| Store M8 tooling | Evidence validator, fake/resume harness, semantic-negative tests, and CI unit-test wiring exist under `scripts/m8_joint_fork_integrity_*` and `.github/workflows/ci.yaml` |
| Hypervisor implementation | Sibling `main` contains the replay-commit harness, baseline-resident restore, FULL-cadence rollover, resumability, semantic negative, and bounded/nightly workflow definitions |
| CI blocker | Snapshot-store's bounded job was queued because no `kvm-intel` runner was visible to this repo; it still needs a green live run and required-check/branch-protection evidence |
| Hardware blocker | `m0u` has only an unqualified local preflight. The operator-attested Intel/SATA host now qualifies by policy, but fio, counted M5, and counted M7 rows must run |
| Full acceptance | `4ua` remains blocked on `m0u`; no qualified 1000/1000 live evidence or final M8 benchmark rows exist |

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

Follow `09-remaining-execution.md`: first revalidate and claim the active beads;
then unblock and prove bounded CI; then qualify the attested reference host;
then run semantic-negative and resumable 1000-child acceptance on the same
qualified store root; finally record benchmark rows, deviation sign-off,
tracker closure, commits, and pushes. The CI runner and lab scheduling work can
proceed in parallel, but `4ua` remains tracker-blocked until `m0u` closes.

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
