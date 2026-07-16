# WI1 — Author the stable `determinism.snapstore.v1` schema

Owner-side deliverable: the stable v1 schema content, derived from the
**as-built** service surface, landed in control-plane's proto tree as the
vdev→stable candidate (family stays vdev/breaking-ignored; promotion itself is
the successor's).

## Source of truth: as-built beats doc

Two candidate sources disagree; the rule is **code wins for as-built**:

1. `snapshot-store/proto/snapshot_store.proto` — the canonical vendored copy
   (its own header says so), compiled into both `snapstore-client` and
   `snapstore-server` via `build.rs`, i.e. the wire contract the running
   service actually speaks.
2. `/Users/punk1290/.agents/projects/determinism/docs/snapshot-store/API.md`
   §1 — the owner doc's proto sketch, which has drifted.

### Known API.md ↔ code divergences (verified proto-vs-doc; spot-check each against `crates/snapstore-server/src/service.rs` before finalizing)

| Surface | API.md §1 | As-built proto |
|---|---|---|
| `NodeStatus` | `FRONTIER = 0` (no UNSPECIFIED) | `NODE_STATUS_UNSPECIFIED = 0`, `FRONTIER = 1` … |
| `NodeMeta` | `progress_score`/`novelty_score`/`expand_count`; fields 8–15 numbered accordingly | single `optional double score = 8`, `icount = 10`, `virtual_ns = 11`; different 9–14 numbering |
| `PutPagesResponse` | `pages_received`, `hashes` (per-page 32 B list) | `pages_new`/`pages_deduped`/`batch_blake3` (cross-check hash only) |
| `PutSnapshotRequest` | field named `manifest` | field named `container` |
| `ResolvePagesResponse` | nested `PageEntry`, ≤256/msg | top-level `ResolvedPage`, ≤512/msg |
| `PutInputLogResponse` | `log_id` only | adds `newly_stored` |
| `CreateNodeRequest` | `input_log_container = 10`, progress/novelty fields | `inline_input_log = 6`, `icount`/`virtual_ns`, different numbering |
| `GetPath` | `include_input_logs` + parallel arrays | `include_logs` + `PathElement{node, input_log_container}` |
| `QueryNodes` | repeated `statuses`, min/max progress/novelty, nested `OrderBy` | single `optional status`, `parent_node_id`, top-level `QueryOrder` (incl. `NODE_ID`) |
| Metadata `key` | `string` | `bytes` |
| `DeleteMetadataResponse` | empty | `deleted` bool |
| `Pin`/`Unpin` | `reason`; empty responses | `note`; `newly_pinned`/`was_pinned` |
| `Stats` | flat `StatsResponse` (fields 1–18) | `StoreStats` + `ExperimentStats` submessages |
| `TriggerGc` | `started` only | adds `detach`, `already_running`, and 6 completion-report fields |

Actions:

- **Map every RPC** (21 total: 5 pages/snapshots, 2 input logs, 6 tree,
  3 metadata KV, 5 lifecycle — Stats and TriggerGc are inside the lifecycle
  five, not additional) from the
  as-built proto against `service.rs` handlers — confirm each is implemented
  and its streaming direction matches. The RPC *set* agrees between doc and
  code; only message shapes drifted.
- **Any divergence where code and doc disagree: code wins** for the v1 schema.
  If a spot-check finds the *server* diverging from the vendored proto itself
  (not expected — tonic codegen makes that hard), stop and fix in
  snapshot-store first; that would be a real bug, not doc drift.
- **File one doc-drift bead** in snapshot-store (`-l docs`, p2) listing the
  table above, to bring API.md §1 in line with the frozen shape. Do not edit
  API.md as a side effect of this unit.
- **[Land-now BRANCH only — on the default path this transfers to the
  successor's landing commit]** Refresh stale comments in the CONTROL-PLANE
  COPY ONLY (comments do not
  affect the descriptor): e.g. drop the vendored `// UNIMPLEMENTED until M7`
  on `TriggerGc` (M7 shipped; `service.rs` implements it) and describe the
  family as owner-authored v1 candidate content. **The owner vendored file
  is untouched in this unit** — editing it would change the owner SHA
  mid-flight and contradict `02-`'s no-code-change claim. (If the owner
  copy's stale comments bother anyone, fold them into the doc-drift bead.)

## Stability review (this is the "stable" in stable schema)

Before declaring v1-freezable, do an owner pass over the as-built shape —
after `T_freeze`, released fields are never broken:

- Field numbering has room to grow in every message (proto3: additive only
  post-freeze). Confirm no reserved-number needs (e.g. reserve nothing unless
  a field was already removed historically — check `git log -p proto/` for
  ever-released-then-removed fields; none known).
- Enum zero values are `*_UNSPECIFIED` (buf STANDARD) — as-built complies for
  `NodeStatus`/`QueryOrder`.
- Confirm the deliberate absences (no `ReleaseSnapshot`, no `ResolveArtifact`,
  no `ListNodes` — API.md §1) remain owner intent. Record that intent in the
  ready-signal + resolution note (on the default path there is no file to
  carry a new header comment — the owner copy is untouched and no control
  copy is written); the schema header comment lands with the successor's
  landing commit (or this unit's PR on the land-now branch).
- Error-detail messages (`MissingPages`, `MissingNodes`, `CurrentGeneration`)
  are part of the contract — they ship in the same file and freeze with it.

## Land in control-plane (vdev candidate content — NOT promotion)

Per the playbook (`docs/vdev-promotion-playbook.md:76-77`), Release A step 1
brings the schema "from the exact owner SHA into the root `proto/` path
through an owner-authored commit". This unit supplies exactly that
owner-authored commit, ahead of the successor, while the family remains
vdev — safe because `proto/determinism/snapstore/v1` is in `buf.yaml`
`breaking.ignore` (`:41`) and pre-release in the ledger.

**Decision point — DEFAULT IS SIGNAL-ONLY.** The recorded protocol sequences
signal → successor → Release A: control-plane's own request makes placeholder
replacement part of criterion 2 (`02-requested-work.md:46-52`), which
`04-playbook-resolution.md:60-62` assigns "wholly" to the future successor,
and the successor is filed only AFTER the ready signal (`:67-68`). Landing
the schema first would invert that and soft-deadlock: the signal would wait
on a control-plane-reviewed merge while control-plane's recorded trigger to
engage at all is the signal. So: **default = finalize the owner-side schema,
deliver the ready-signal citing the immutable owner SHA + root/file (the
playbook's Release A step 1 explicitly accepts "documented owner approval"
in lieu of an owner-pushed commit); the successor performs the `proto/`
landing.** Land-now is the BRANCH, taken only if control-plane review
affirmatively accepts an early PR (recorded concurrence before merge).
Record which path was taken in the resolution. If signal-only is taken, the
"Land in control-plane" steps below transfer to the successor and this
unit's control-plane footprint is the signal file alone.

Steps:

1. Branch in `control-plane` (their PR flow; never commit to their `main`
   directly).
2. Replace the placeholder body of
   `proto/determinism/snapstore/v1/snapshot_store.proto` with the stable
   schema (same `package determinism.snapstore.v1;`, same file path — the
   lint ignore at `buf.yaml:33` keys on this path).
3. Touch **nothing else**: no `buf.yaml` change, no `docs/proto-freeze-policy.md`
   change, no `crates/determinism-proto` change (the handwritten `snapstore`
   facade at `src/lib.rs:73-90` stays; it is superseded during Release A by
   the successor), no version/tag changes.
4. Run the descriptor comparator to prove owner↔control copies are
   semantically identical (cwd: the control-plane repo root — the relative
   `--control-root proto` resolves there):

   ```bash
   cargo run -p proto-descriptor-eq -- \
     --owner-root /Users/punk1290/git/preestablished/snapshot-store/proto \
     --owner-file snapshot_store.proto \
     --control-root proto \
     --control-file determinism/snapstore/v1/snapshot_store.proto
   ```

   (Owner file carries no path prefix; the comparator maps the explicitly
   named root file. Record the invocation + output.)
5. Full standing gate suite in control-plane (8 of the 9 commands at
   `04-playbook-resolution.md:19-27` — `scripts/dry-run-vdev-promotion.sh`
   is deliberately omitted: this unit performs no promotion):

   ```bash
   cargo fmt --all -- --check
   cargo build --workspace --all-features
   cargo test --workspace --all-features
   buf lint
   scripts/buf-breaking-against.sh          # green: family is ignored
   scripts/check-buf-breaking-self-test.sh
   scripts/check-proto-descriptor-eq.sh
   scripts/check-proto-version.sh           # no version change made
   ```

6. Open the PR (owner-authored). Merge authority, pre-decided per the
   observatory-plan precedent: merge once control-plane CI is green
   (`gh pr merge --squash`) — do not stall waiting for a human reviewer.
   PR body: state this is the owner-authored vdev candidate content for the
   snapstore promotion, cite the playbook and the owner SHA, and state that
   promotion (two-release staging/freeze) remains the successor's.

## Acceptance

Both paths:

- Every as-built RPC/message mapped; divergences dispositioned (code wins);
  doc-drift bead filed in snapshot-store.
- Owner SHA pinned and recorded for the signal — the ready-signal in `02-`
  requires it.

DEFAULT (signal-only) path — nothing further; the comparator, control-plane
gate suite, and `proto/` landing are the successor's Release A steps and
running the comparator now against the 12-line placeholder would rightly
fail.

Land-now BRANCH only (with control-plane's recorded concurrence):

- Comparator passes owner-copy ↔ control-plane-copy.
- `buf lint` and `scripts/buf-breaking-against.sh` green; workspace build +
  tests green (all features — proves the untouched `snapstore` facade and
  every other feature still compile).
- Playbook schema-review posture satisfied: family still pre-release in the
  ledger, still breaking-ignored, no placeholder-era consumer broken.
- Control-plane PR merged; capture the merged commit SHA for the signal.
