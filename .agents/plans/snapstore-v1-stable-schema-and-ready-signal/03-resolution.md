# Resolution — snapstore/v1 stable schema + owner-ready signal

Executed 2026-07-16. Both work items discharged on the **default signal-only
path**; the ball is now in control-plane's court.

## WI1 — schema derivation outcome

- **Source of truth confirmed:** `proto/snapshot_store.proto` (455 lines,
  canonical vendored copy) is the stable v1 schema content. All 21 RPCs
  (5 pages/snapshots, 2 input logs, 6 tree, 3 metadata KV, 5 lifecycle)
  mapped against `crates/snapstore-server/src/service.rs` — every handler
  present, streaming directions match (PutPages client-stream at
  `service.rs:128`; ResolvePages/QueryNodes server-stream via the
  `ResolvePagesStream`/`QueryNodesStream` types at `:295`/`:678`; rest
  unary). TriggerGc implemented at `:1084` (M7 shipped); the vendored
  `// UNIMPLEMENTED until M7` comment is stale doc-only drift. No
  server↔proto divergence exists (tonic codegen makes the trait follow the
  file).
- **Divergence disposition:** all 13 API.md §1 ↔ code divergences from the
  plan table confirmed as doc drift; **code wins** for every one. Doc-drift
  bead **snapshot-store-wz8** (p2, docs) filed with the full list plus the
  stale TriggerGc comment. API.md untouched in this unit; owner proto file
  untouched (owner SHA stability).
- **Stability review:** enum zeros are `*_UNSPECIFIED` (NodeStatus,
  QueryOrder); `git log --follow -p proto/snapshot_store.proto` shows zero
  released-then-removed fields (2 commits total: b8aa175 vendor, e4511cf M7
  additive) — no `reserved` needed; deliberate absences (no ReleaseSnapshot /
  ResolveArtifact / ListNodes) reaffirmed as owner intent and recorded in the
  signal; error-detail messages (MissingPages, MissingNodes,
  CurrentGeneration) freeze with the file.
- **Owner SHA pinned:** `a582bee5abfd0f1bd078e645f2eaa9576e3f966f` —
  verified equal to `origin/main` before the signal was written (immutable).

## Path taken: DEFAULT signal-only

No control-plane proto landing, no comparator run, no control-plane gate
suite — per the plan's decision point these are the successor's Release A
steps (running the comparator now against the 12-line placeholder would
rightly fail). This unit's control-plane footprint is the signal file alone.
Land-now branch not taken (no control-plane concurrence sought — the recorded
protocol sequences signal → successor → Release A).

Gates that ran: owner-side verification only (RPC mapping, streaming
direction check, proto history audit). Snapshot-store has no code change in
this unit — plan/doc/bead only — so no build/test gates were owed here.
Stating plainly per house verification rules: the descriptor comparator,
buf lint/breaking, and control-plane workspace build/test did NOT run in
this unit; they are owed by control-plane's promotion-execution successor.

## Signal delivery

- File: `control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/05-snapstore-owner-ready-signal.md`
- Control-plane commit: `e71864d`, pushed to origin/main (direct commit per
  their request-dir convention for doc-only deliveries; index 05 was free —
  the sibling archive-ref handshake plan had not taken it).
- Carries every owner-side evidence-manifest field plus explicit v1
  stability approval. Live consumer check recorded in the signal:
  hypervisor consumes via path deps on the snapstore crates (checked at
  `b4358a7…`), orchestrator has no snapstore dependency (checked at
  `ffe93f2…`) — API.md §5's "they vendor the file" is not realized; folded
  into the signal's re-verify-live caveat.

## Bead transitions (live DB, after `bd dolt pull`)

- `snapstore-8qx`: **absent** from the live DB (`bd show` → not found), the
  plan's predicted likely case per the phase-2 prefix-teardown precedent.
  Closure-record bead **snapshot-store-98o** created citing the delivery
  evidence and closed immediately; `8qx` survives only in documents.
- Successor **snapshot-store-bxg** (p2, impl) filed: adopt frozen
  snapstore/v1 from determinism-proto post-`T_freeze`; blocked externally on
  control-plane's consumer-handback signal, not on local work.
- Doc drift: **snapshot-store-wz8** (see WI1).

## Reciprocal expectation

Control-plane's next move (theirs alone): file
`phase?-snapstore-v1-promotion-execution/` per the playbook — criteria 2–3 of
`02-requested-work.md`, two-release `T_stage`/`T_freeze` sequence, `proto/`
landing from the owner SHA, comparator, freeze-ledger flip, facade
migration, consumer handback. Our later half (snapshot-store-bxg) triggers
on their handback signal per playbook `:168-179`. The ball is in
control-plane's court.
