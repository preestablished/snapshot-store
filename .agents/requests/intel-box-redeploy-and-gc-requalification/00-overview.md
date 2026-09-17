# Request: Intel-Box Redeploy and GC/Transport Requalification

## Who Is Asking

The operator, via the 2026-09-14 "fix what remains" plan suite (this is plan C;
plan directory
`~/.agents/projects/snapshot-store/plans/intel-box-redeploy-and-gc-requalification/`).
Consumers: determinism-hypervisor plan B (worker stack redeploy),
reference-workload plan A (corpus capture), exploration-orchestrator plan D
(M6 real-substrate integration).

## Why snapshot-store, Why Now

- The Intel-box worker stack plans A and D need had no documented, stable
  `snapstore-server` deployment: the hypervisor handoff writes a config with
  ephemeral ports and no page channel.
- The READY root snapshot the handoff produces is referenced by no node and no
  pin, so the first GC cycle would delete it.
- The Phase-5 GC/transport bars were last counted on 2026-07-11 at commit
  `3218d6b` and missed; nothing has been measured at the current commit.
- Issue tracking moved to the `bn` hub on 2026-09-14; none of the July-plan
  ids (`snapstore-feb`, `snapstore-28z`, `snapstore-8qx`, `snapstore-agz`,
  `snapstore-nn4`) exist there. They are cited as history only.

## The Ask In One Paragraph

Ship a committed overlay config template, unit example, and deploy runbook;
bring `snapstore-server` up on the Intel box from that overlay with health
checks and a restart drill; pin the READY root and prove it survives an
aggressive GC cycle (and cover that property in CI); rerun the Phase-5
evidence script at HEAD on the storage class the data root actually uses and
disposition every bar as `met`, `miss (tuned)`, or `miss (accepted floor)`.

## Issues Filed (hub project `snapshot-store`)

| Id | Title | Priority |
|---|---|---|
| `snapshot-store-ee8q` | Intel-box snapstore deployment: config overlay, unit, health checks | P1 |
| `snapshot-store-vb3a` | Pin READY root after handoff; pinned-orphan-survives test | P0 |
| `snapshot-store-fga8` | Phase-5 requalification run at HEAD on Intel box | P1 |
| `snapshot-store-tvjc` | Bounded pagestore/GC tuning (only if bars miss) | P2 |
| `snapshot-store-7hec` | Request handoff pin in dh-m9-ready-handoff (plan B) | P2 |
| `snapshot-store-h2yv` | Advisory flock on `<data_root>/LOCK` in startup.rs | P1 |

Dependencies: `vb3a` and `fga8` are blocked by `ee8q`; `tvjc` by `fga8`.

## Out Of Scope

Proto/schema changes, M9 backup, the hypervisor handoff code, the
orchestrator's root-node creation and attrs write, and any change to the
`STORE_VERSION` on-disk format.

## Files In This Request

| File | Contents |
|---|---|
| `00-overview.md` | This file |
| `04-resolution.md` | Per-bar table, storage-class decision, accepted floors, pin/identity record, issue disposition |
