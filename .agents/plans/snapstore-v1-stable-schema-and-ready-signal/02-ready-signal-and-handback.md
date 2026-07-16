# WI2 — Owner-Ready Signal, Bead Close, Handback

Deliver the owner-authored stable-schema ready-signal into control-plane's
request dir, discharge `snapstore-8qx`, and record the handback. This file is
executable only after `01-` has produced an immutable owner commit SHA (and,
on the land-now BRANCH only, a merged control-plane schema commit — the
default signal-only path requires no control-plane commit).

## Why the signal goes where it goes

The handshake text (mirrored on both sides so the texts agree):
`snapshot-store/.agents/requests/phase2-closeout-m8-joint-fork-integrity/02-requested-work.md:77-83`
— "whichever side is ready first … leaves the ready-signal in the other's
request dir", and control-plane's receiving side is
`phase4-snapstore-promotion-and-vdev-playbook/`. Control-plane's playbook
half resolved 2026-07-11 (`04-playbook-resolution.md`), and its resolution
states it will "file the successor only after snapshot-store sends an
owner-authored stable-schema ready signal" (`:69`). We are now the ready
side; the signal lands in **their** request dir.

## Sibling-plan coordination (read first)

A parallel control-plane plan exists:
`control-plane/.agents/plans/archive-ref-spec-answer-and-handshake/`. Its
handshake step writes control-plane's OUTBOUND playbook-ready signal —
`05-` or `06-handshake-signal-sent.md` in this same control-plane request
dir, and `07-controlplane-playbook-ready-signal.md` into snapshot-store's
`phase2-closeout-m8-joint-fork-integrity/` dir. Those files are the
OPPOSITE direction from the owner-ready signal this plan owes: **the two
signals are complementary and BOTH are required.** If you find either of
the sibling's files already present, they neither satisfy nor block this
plan's delivery — do not skip WI2 because a "handshake" file exists.
Re-derive the next free index at write time; never renumber the sibling's
files.

## The signal file

Create (next free index in that dir — currently 00–04 are used, with two 04-*
files; use 05 unless the dir has changed or the sibling took it — then 06):

`control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/05-snapstore-owner-ready-signal.md`

Content requirements — the playbook's evidence manifest
(`docs/vdev-promotion-playbook.md:22-37`) names the owner-side fields the
promotion executor must be able to fill from this signal, and `:45-47`
requires both a ready signal **and** explicit v1 stability approval. Include:

```text
# snapstore/v1 owner-ready signal (stable schema)

signal type:               owner-authored stable-schema ready signal
                           (the trigger named in 04-playbook-resolution.md:69)
family/package:            determinism.snapstore.v1
owner repository:          snapshot-store (path/URL as pinned by consumers)
owner commit (SHA):        <immutable snapshot-store SHA containing the schema
                           — must be PUSHED to origin before the signal is
                           written (an unpushed SHA is not immutable)>
owner canonical proto root/file:  proto/ , snapshot_store.proto
control-plane candidate commit:   <merged control-plane SHA from 01-, or
                                  "not landed — successor copies from owner SHA">
descriptor comparator:     <command + result from 01- step 4 on the land-now
                           branch; on the default path: "deferred to the
                           successor's Release A — no control-plane copy
                           exists yet">
v1 stability approval:     snapshot-store approves freezing this schema as
                           determinism.snapstore.v1; post-freeze evolution is
                           additive-only per docs/proto-freeze-policy.md.
known consumers:           snapstore-client, snapstore-server (in-repo,
                           build.rs codegen of the vendored copy);
                           determinism-hypervisor and exploration-orchestrator
                           vendor the file per API.md §5 — verify their
                           current pin SHAs live, do not trust this snapshot.
bead:                      snapstore-8qx (closed on this delivery; successor
                           bead <id> tracks the post-T_freeze re-pin)
requested next step:       control-plane files phase?-snapstore-v1-promotion-
                           execution/ per the playbook (criteria 2–3 of
                           02-requested-work.md); two-release staging/freeze
                           sequence mandatory.
```

Notes:

- Fill every value with **live-verified** data at execution time (playbook
  `:41-43`: "do not reuse … from a request snapshot").
- The consumer list feeds the manifest's "known consumers and their checked
  SHAs" line — check the hypervisor/orchestrator vendored copies actually
  exist before naming them, and record the SHAs you checked.
- Commit the signal through control-plane's normal flow (it is a request-dir
  doc, not code; a direct PR/commit per their request-dir convention).

## Bead handling (serial `bd`, snapshot-store repo)

(Note on "comment" steps: `bd` has no documented comment subcommand in the
house conventions — where a step says to comment on a bead, fold the record
into the close `-r` text or the successor bead's `-d` instead.)

1. Sync first (`bd dolt pull` or the repo's documented sync), then
   `bd show snapstore-8qx` — verify live scope text (planning could not:
   embedded Dolt, files not authoritative).
2. File the successor bead for the half that stays open on our side:

   ```bash
   SWAP=$(bd create "Adopt frozen snapstore/v1 from determinism-proto (post-T_freeze re-pin)" \
     -d "When control-plane's phase?-snapstore-v1-promotion-execution delivers T_freeze + consumer-handback signal (tag, family path, re-pin instructions per playbook handback template), swap snapstore-client/-server from vendored proto/snapshot_store.proto to the published determinism-proto crate and delete the vendored copy per its header note. Blocked externally on control-plane's signal, not on local work." \
     -p 2 -l impl -t task --silent)
   ```

3. Comment on `8qx` with the correction/record: owner-side half discharged
   (schema commit SHA(s), signal path), swap half moved to `$SWAP`.
4. Close: `bd close snapstore-8qx -r "Stable determinism.snapstore.v1 schema authored (owner SHA <sha>; control-plane landing deferred to successor per signal — or candidate <sha/PR> on the land-now branch) and owner-ready signal delivered to control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/05-snapstore-owner-ready-signal.md. Post-T_freeze re-pin tracked in <SWAP id>."`
5. If `bd show` reveals `8qx` scope materially different from the assumed
   "vendored-proto swap" text, adjust: the invariant is that closing it must
   not orphan the re-pin work — the successor bead is the safety net either
   way.
6. **Absent-bead branch (the LIKELY case):** the phase-2 reconciliation
   record (`04-resolution-immediate.md:12-31` in the same request dir)
   shows the five legacy `snapstore-*`-prefix beads it names (8qx was not
   among the five, but the same teardown applies) were absent from the live DB
   after the teardown, with replacements created under the current
   `snapshot-store-*` prefix. If `bd show snapstore-8qx` errors (bead not
   found), do NOT stall: create a closure-record bead under the current
   prefix citing the delivery evidence (schema SHA, signal path), close it
   immediately with the same `-r` text, and note in the resolution that
   `8qx` survives only in documents, per that reconciliation precedent.

## Reciprocal — what is deliberately NOT ours

- Filing `phase?-snapstore-v1-promotion-execution/` is **control-plane's next
  move** (their resolution `:60-69` owns criteria 2–3). Do not file it, do not
  nag beyond the requested-next-step line in the signal.
- The eventual re-pin landing is ours but *later* (successor bead), triggered
  by their consumer-handback signal (playbook `:168-179`).

## Resolution note

Write `03-resolution.md` **in this plan dir** recording: schema derivation
outcome (divergence dispositions + doc-drift bead id), which path executed
(default signal-only, or the land-now branch with its control-plane
commit/PR), gate outputs where they ran (comparator, buf, build/test —
actual results, per verification house rules: report what ran, not what was
assumed; on the default path state plainly that the control-plane gates are
the successor's),
signal file path + commit, bead transitions (`8qx` closed, successor id), and
the explicit statement that the ball is now in control-plane's court.

## Session close

Both repos: quality gates (control-plane's suite already ran in `01-`;
snapshot-store has no code change — plan/doc/bead only), then per house
practice in each touched repo: `git pull --rebase`, `bd dolt push`
(snapshot-store), `git push`, `git status` shows up-to-date with origin.
Verify `pwd`/`git remote -v` before each commit — two repos are in play.
