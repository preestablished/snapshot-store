# Request: Close Out M8 — Phase 2's Joint Fork-Integrity Milestone, Overdue Behind A Stale Blocker

## Who Is Asking

The phases track, round 2 (2026-07-07), on behalf of the Phase 5
first-integration consumer (exploration-orchestrator M6) and the shared
determinism-regression gate both this repo and determinism-hypervisor
are supposed to carry in CI.

## Standing Relative To Round 1 — Read This First

Round-1 (`phase5-readiness-gc-benchmark-and-transport-revalidation/`) is
unexecuted. Sequencing, defended rather than assumed: M8 being overdue
Phase-2 debt argues for urgency — the standing rule says determinism
regressions are P0 in every phase — but round-1 still runs first for
the hardware-bound tail: `28z` is literally titled "M8-entry" in its
own bead, the M8 BM rows reuse round-1's NVMe bring-up and
`docs/bench-baseline.md` section, and `nn4` (a flaky transport test)
must not sit under a new determinism regression. What that does NOT
justify is idling the guest-free work: the immediate lane *and the
harness build (item 3)* are ungated here. Filed now so (a) the stale
blocker is corrected today, and (b) round-1's lab session is planned
knowing M8 is the same box's next customer.

## Why snapshot-store, Why Now

- **The M8 epic's stated blocker is stale — and M8 is *overdue*, not
  early.** `snapstore-675` says "After determinism-hypervisor M4-M7."
  Satisfied: the hypervisor closed its M4 fork-transparency acceptance
  (`-a6s`), Tier-A CoW fork (`-9e4`), the M7 1000× fork acceptance
  (`-cw2`, "fork 1000x harness ... VerifyReplay all"), and the Linux M7
  fork VerifyReplay + nightly 100-child canary (`-4s9.29`, closed
  2026-06-20). Sharper still: the phase matrix puts snapshot-store M8
  in **Phase 2** ("M4–M6, M8"), and `phase-2-fork-and-replay.md` lists
  it as the Phase-2 joint close-out. This is late Phase-2 debt that
  went invisible behind a stale bead note — not Phase-5 work being
  pulled forward.
- **M8 is the store's half of a shared obligation.** Plan §M8: fork one
  guest 1000× through the store, restore + re-execute each child, and
  assert every `PutSnapshot` returns a ref identical to the original
  child's — content-address as bit-identity — then install that as a
  permanent regression in **both repos' CI**. Plus the §M8 BM rows:
  fork→commit / restore latencies vs ARCHITECTURE §7.1 and sibling dedup
  ≥94% shared pages, recorded in `docs/bench-baseline.md`.
- **Phase 5 stands on it.** The orchestrator's M6 first-integration run
  is "real snapshot-store + real hypervisor on the Intel box"; round-1
  proves the store keeps up *alone*, M8 proves it keeps up *under the
  real hypervisor with bit-identity* — which is what the 4-hour soak
  gate actually exercises.

## The Ask In One Paragraph

Today: un-block `snapstore-675` in bd with the stale-blocker correction
recorded (cite the hypervisor bead closures), and fix `snapstore-pov`
(the beads Dolt remote divergence — with the owner's explicit decision,
no force-push) so M8's evidence/bead updates can actually push. Then,
after round-1 resolves: build the 1000×-fork ref-equality harness and
dedup measurement tooling (preparable without the guest), run the joint
acceptance in a coordinated session with the hypervisor (worker wired to
`snapstore-client` per INTEGRATION.md §1–2 — FULL-manifest cadence,
baseline-delta restore), install the regression as a required check in
both CIs, and record the §M8 BM rows on the operator-attested reference box in
`docs/bench-baseline.md`.

## Files In This Request

| File | Contents |
|---|---|
| `01-current-state.md` | Evidence: the stale blocker, what §M8 owes, what exists |
| `02-requested-work.md` | Entry conditions, the ask, acceptance criteria, out of scope |
| `03-verification-offer.md` | Joint-session choreography with the hypervisor; handback |
