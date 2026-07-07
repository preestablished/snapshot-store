# What The Orchestrator Provides For Verification

The exploration-orchestrator's M1–M4 stack (pure core, fakes, scheduler,
experiment runner) is done and reviewed — resolution at
`../exploration-orchestrator/.agents/requests/phase5-entry-m3-m4-runner-on-fakes/04-resolution.md`
(written at `084892f`; the dual-review findings were subsequently applied
at `bf5b7b3`, the current `main`). That gives us a realistic load
generator you can borrow *before* Phase 5 formally opens.

## Standing Offer

1. **A representative churn profile.** The fakes-based experiment runner
   produces the snapshot-op *mix* a real search generates — fork-heavy
   expansion, subtree pruning as the frontier moves, checkpoint commits.
   We commit to adding the small piece of instrumentation needed to record
   **op-mix ratios (fork:prune:read:commit) and tree shape over time**
   from an M4 grid-world run and hand that over. Note the fakes run in
   virtual time, so they cannot give you meaningful ops/sec — wall-clock
   rates come only from the M6 rehearsal in item 2. Ask and we'll generate
   the profile against whatever tree size you want.
2. **A joint rehearsal at M6.** When the orchestrator's M6
   first-integration milestone opens (real store + real hypervisor on the
   Intel box), we will run a bounded search with your `gc_*` metrics
   scraped and report GC pacing observed under genuine load back into this
   request directory — the empirical follow-up to the synthetic benchmark.
3. **Early warning both directions.** If your benchmark shows GC cannot
   keep pace at some ingest rate, we build the expansion throttling into
   the orchestrator's configuration (orch-sched's slot-lease/backpressure
   machinery is the place to hang it) rather than discovering the limit
   mid-soak; tell us the number.

## Handback Shape

Same convention as `phase3-m7-gc-exit-gate/` in this repo: append
`04-resolution.md` here containing the git SHAs, the evidence-root path,
a per-bar pass/fail table, the bead dispositions, and the risk paragraph
(acceptance item 5); we re-verify and respond with `05-verification.md`.
If the hardware preflight dead-ends, `04-resolution.md` carries the
preflight numbers and the escalation instead — that also counts as
resolved.

## Contact / Tracking

- Orchestrator-side context: `exploration-orchestrator/.agents/plans/phase5-entry-m3-m4-runner-on-fakes/`
  (evidence at the orchestrator repo root, `exploration-orchestrator/evidence/phase5-m3-m4/`).
- Store-side beads this request covers: `snapstore-feb`, `snapstore-28z`,
  `snapstore-nn4`.
- Cross-repo precedent for the request/handback pattern:
  `.agents/requests/phase3-m7-gc-exit-gate/` in this repo (00–05 tell the
  whole story including the independent verification format).
