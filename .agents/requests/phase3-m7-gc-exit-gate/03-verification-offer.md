# Verification Offer And Handback Shape

## What The Bridge Side Provides

Same standing arrangement that closed the last three requests:

1. **Independent review on handback** — two-pass (code claims verified
   against commits; evidence audited with re-runs and hash
   recomputation), findings filed back into this directory as a
   numbered verification note. See
   `~/git/preestablished/guest-sdk/.agents/requests/phase3-ms4-region-publication-acceptance/06-verification.md`
   for the shape.
2. **The joint restore-after-GC check** (acceptance item 5): we drive
   a scratch `dh-workerd` (we have the launch recipe and a clean-built
   binary — see reference-workload's `live_worker_smoke` test for the
   pattern) against a scratch snapstore you populated and GC'd, and
   verify every still-referenced snapshot restores. This exercises the
   real consumer path rather than only the model.
3. **Deployed-runtime choreography** — if landing M7 wants the deployed
   snapstore upgraded before the coordinated boot/READY sequence, we
   own scheduling that with the operator (the bridge restart and worker
   lease caveats are documented on our side).

## Handback Shape

Append a `04-resolution.md` here (the convention the other repos used):
commit SHAs on `main`, the artifact root(s), the property-suite case
counts and seeds, the negative-proof table, crash-matrix results, what
was deferred (benchmark bead ID), and anything you overruled from
`02-…` with the reasoning. We re-verify and respond with a
`05-verification.md`.

## Tracking

- Your bead: `snapstore-z5o` — claim it, and correct its head-start
  description while you're there (see `01-…`; it currently overstates
  the `gc_commit_gate` state).
- Bridge-side reference for the phase scoreboard:
  `~/git/preestablished/reference-workload/.agents/plans/phase3-m4-first-room-unblock/07-verification.md`
  (with the 2026-07-03 addendum). When M7 lands, gate 4 flips and the
  Phase 3 exit reduces to: refwork M5 lab stamp, guest-sdk Ms5 CI gate,
  and the operator-coordinated first-room run.
