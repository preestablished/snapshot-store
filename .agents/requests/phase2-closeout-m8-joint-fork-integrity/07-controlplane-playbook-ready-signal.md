# control-plane → snapshot-store: vdev promotion playbook READY

Dated 2026-07-16. This file exists in this dir per the reciprocal handshake
mirrored in `02-requested-work.md` ("Out Of Scope" section: "whichever side
is ready first (their playbook, or this repo's authored schema) leaves the
ready-signal in the other's request dir"). Control-plane's side has been
ready since 2026-07-11; this signal was left unsent at the time (recorded in
`control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/04-playbook-resolution.md`)
and is delivered now to complete the handshake on the record.

## What is ready

- Promotion playbook + rehearsed dry run, landed 2026-07-11 at control-plane
  commit `5a3b4f9`.
- Entry point: `control-plane/docs/vdev-promotion-playbook.md`; rehearsal
  transcript: `control-plane/docs/vdev-promotion-dry-run.md`.
- Standing CI descriptor comparator runs on every control-plane PR.
- Resolution of record:
  `control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/04-playbook-resolution.md`.

## Handshake state

snapshot-store's reverse-direction signal has already arrived: the
owner-authored stable-schema ready signal was delivered 2026-07-16 as
`control-plane/.agents/requests/phase4-snapstore-promotion-and-vdev-playbook/05-snapstore-owner-ready-signal.md`
(owner SHA `a582bee5abfd0f1bd078e645f2eaa9576e3f966f`, v1 stability
approved). Both directions of the handshake are now on the record; the two
signals are complementary, per your plan
`snapstore-v1-stable-schema-and-ready-signal` — neither side double-sends.

## Control-plane's next move

With the owner-ready signal received, control-plane files the successor
request (`phase?-snapstore-v1-promotion-execution/`) using the playbook's
mandatory two-release staging/freeze sequence (criteria 2–3 of the original
`02-requested-work.md` in control-plane's request dir). Nothing is required
from snapshot-store until that successor delivers the T_freeze +
consumer-handback signal (your post-T_freeze re-pin is tracked owner-side in
`snapshot-store-bxg`).

## Status quo until the successor runs

`determinism.snapstore.v1` remains a placeholder in control-plane's tree,
its path is still ignored by Buf breaking, it remains in the pre-release
ledger, and no `proto-v*` tag has been created (all per
`04-playbook-resolution.md`).
