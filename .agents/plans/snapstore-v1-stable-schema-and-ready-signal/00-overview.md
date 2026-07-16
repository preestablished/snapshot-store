# snapstore/v1 Stable Schema + Owner-Ready Signal

Plan for discharging `snapstore-8qx`'s currently-actionable half: author the
stable `determinism.snapstore.v1` proto schema (owner side of the vdev→v1
promotion) and deliver the owner-authored stable-schema ready-signal to
control-plane. That signal is the sole trigger control-plane is on record
waiting for before filing its `phase?-snapstore-v1-promotion-execution/`
successor (`control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/04-playbook-resolution.md:60-69`:
"File the successor only after snapshot-store sends an owner-authored
stable-schema ready signal").

Written for a fresh coding agent with zero conversation context. Plan files:

| File | Work item |
|---|---|
| `01-author-stable-schema.md` | Derive the stable v1 schema from the as-built service surface; land it as the vdev candidate content in control-plane's proto tree |
| `02-ready-signal-and-handback.md` | Deliver the owner-ready signal into control-plane's request dir, handle the bead, record the resolution |

## Goal (definition of done)

1. The stable `determinism.snapstore.v1` schema is finalized and reviewed
   OWNER-SIDE, matching the as-built snapshot-store service, with an
   immutable owner SHA recorded. On the DEFAULT (signal-only) path the
   control-plane `proto/` landing — replacing the 12-line placeholder,
   descriptor-verified — transfers to the promotion successor (their
   criterion 2); it happens in this unit only on the land-now branch with
   control-plane's recorded concurrence. Family stays vdev/breaking-ignored
   either way (no promotion performed here).
2. The owner-authored ready-signal file is committed in
   `control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/`,
   carrying every owner-side field of the playbook's evidence manifest plus an
   explicit v1 stability approval.
3. `snapstore-8qx` is closed with `-r` citing the delivery evidence (or its
   closure-record replacement bead per `02-` step 6 if the legacy-prefix
   bead is absent — the likely case), with a successor bead filed for the
   post-`T_freeze` re-pin adoption (see beads handling below).
4. A resolution note in this plan dir records commits, signal location, and
   the reciprocal expectation.

## Grounding (verified 2026-07-16, tolerances noted)

| Claim | Verified anchor |
|---|---|
| Reciprocal handshake text ("whichever side is ready first leaves the ready-signal in the other's request dir"), listed separately from M9 | `snapshot-store/.agents/requests/phase2-closeout-m8-joint-fork-integrity/02-requested-work.md:77-83` (cited as ~77–84; tolerance ±1 line). M9 (`agz`) is a separate bullet at `:84` |
| `8qx` was doubly blocked: control-plane proto-freeze request + this repo authoring its real `snapstore/v1` upstream | same dir `01-current-state.md:66-71` (cited as ~67–71) |
| First blocker cleared: playbook + dry-run resolved 2026-07-11; successor waits ONLY on the owner-authored ready signal; criteria 2 & 3 deferred to `phase?-snapstore-v1-promotion-execution/` | `control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/04-playbook-resolution.md:1-3,54-69` (cited lines 64–69 confirmed) |
| Playbook: preconditions, evidence manifest, two-release (`T_stage`/`T_freeze`) sequence, comparator command, consumer-handback template | `control-plane/docs/vdev-promotion-playbook.md` (esp. `:22-47` manifest+preconditions, `:76-77` owner-authored commit into `proto/`, `:80-84` comparator, `:168-179` handback template) |
| Current placeholder state | `control-plane/proto/determinism/snapstore/v1/snapshot_store.proto` — 12 lines (`wc -l`): `NodeMeta{experiment_id,node_id,parent_id,snapshot_ref,input_log_id}`, `PutSnapshotRequest{manifest}`, empty `service SnapshotStore {}` — matches control-plane's "12-line placeholder" |
| buf posture | `control-plane/buf.yaml:41` — `proto/determinism/snapstore/v1` in `breaking.ignore`; `:33` — `snapshot_store.proto` in `lint SERVICE_SUFFIX ignore_only` |
| Freeze ledger | `control-plane/docs/proto-freeze-policy.md:19` — `determinism.snapstore.v1` pre-release; promotion must flip ledger + remove ignore in the same freeze change (`:33-36`) |
| Handwritten facade | `control-plane/crates/determinism-proto/src/lib.rs:73-90` — `snapstore` feature exposes hand-rolled `NodeMeta`/`PutSnapshotRequest`; no packaged snapstore proto under `crates/determinism-proto/proto/`. Facade migration is Release-A/successor work, untouched here |
| Owner doc | `/Users/punk1290/.agents/projects/determinism/docs/snapshot-store/API.md` §1 (gRPC surface), §5 (versioning: "until then this repo's copy is canonical and the orchestrator/hypervisor vendor it") |
| Contract ownership | `/Users/punk1290/.agents/projects/determinism/docs/MAP.md:146` — "Snapshot manifest, page store, tree/lineage API…, metadata KV → `snapshot-store`"; `:153` — "proto repo layout → `control-plane`" |
| As-built schema | `snapshot-store/proto/snapshot_store.proto` (455 lines, header comment: canonical vendored copy) compiled by `crates/snapstore-client/build.rs:7` and `crates/snapstore-server/build.rs:7` |

**Unconfirmed / verify at execution time:**

- **"proto crate only grows" phrasing** was NOT found verbatim in MAP.md. The
  nearest standing rule found: "released fields are never broken"
  (`control-plane/.agents/requests/phase4-proto-freeze-tag-and-breaking-gate/00-overview.md:28`).
  Treat both as: no breaking change to any *frozen* package; vdev families may
  be reshaped until frozen (explicit in `docs/proto-freeze-policy.md:33-34`).
- **Bead state.** `.beads/` is embedded Dolt — file inspection is not
  authoritative and `bd` was deliberately not run during planning. Verify with
  `bd show snapstore-8qx` (after sync) at execution start; its recorded scope
  is historically "vendored-proto swap" (see beads handling).
- **API.md ↔ code divergences** (enumerated in `01-`) were verified
  proto-vs-doc; per-RPC spot-checks against `snapstore-server/src/service.rs`
  happen at execution.

## What this plan is NOT

- **Not M9** (`snapstore-agz`, Phase 8) — untouched.
- **Not promotion execution.** No `T_stage`/`T_freeze` tags, no version bumps,
  no `buf.yaml` ignore removal, no freeze-ledger flip, no
  `determinism-proto` codegen/facade migration, no re-pin. All of that is
  control-plane's `phase?-snapstore-v1-promotion-execution/` successor per the
  playbook's two-release sequence.
- **No breaking change to any released (frozen) proto.** Only the
  `snapstore/v1` family content changes, and it is pre-release/breaking-ignored.

## Authority

- **snapshot-store owns the `snapstore/v1` contract** (MAP.md contract table);
  the schema content decisions here are ours, and the ready-signal must carry
  our explicit v1 stability approval (playbook `:45-47`).
- **control-plane owns the proto repo process** (layout, buf gates, PR review,
  tags, freeze ledger). Default delivery is **signal-only** (the recorded
  protocol sequences signal → successor → Release A; see `01-`'s decision
  point) — the schema is finalized owner-side and the signal cites the
  immutable owner SHA; the optional early control-plane PR is a branch taken
  only with their recorded concurrence.

## Beads handling

- Local DB state uncertain: start with `bd dolt pull` (or documented sync),
  then `bd show snapstore-8qx`. Run all `bd` commands serially.
- `8qx`'s historical description is the *vendored-proto swap* (adopting the
  published crate) — which cannot complete until control-plane's `T_freeze`.
  This unit discharges the owner-side half. On delivery: close `8qx` with
  `-r` citing the schema commit + signal location, and file a successor bead
  ("Adopt frozen snapstore/v1 from determinism-proto after T_freeze; re-pin
  and delete vendored proto") blocked on nothing locally but documented as
  waiting on control-plane's consumer-handback signal. If `bd show` reveals
  the bead text is materially broader/narrower than assumed, record the
  correction in a bead comment before closing (documentary-blocker lesson
  from the phase-2 closeout request).

## Sequencing

1. `01-` — schema derivation + owner-side finalization (control-plane PR +
   gates only on the land-now branch).
2. `02-` — ready-signal (cites the immutable owner SHA; the merged
   control-plane SHA only on the land-now branch), bead close, resolution
   note.

## Execution / session close

Per house practice (CLAUDE.md): quality gates on changed code, issue updates,
then in **each** touched repo: `git pull --rebase`, `bd dolt push` (where
beads-tracked), `git push`, `git status` up-to-date-with-origin. Two repos are
touched (snapshot-store: plan/resolution + bead; control-plane: the signal
file alone on the default path — plus the proto commit only on the land-now
branch). Verify repo context (`pwd`, `git remote -v`) before each commit.
