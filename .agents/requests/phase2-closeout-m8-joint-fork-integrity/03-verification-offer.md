# Joint-Session Choreography And Handback

## With determinism-hypervisor

Their half of the joint acceptance is coordination, not new milestones
(fork/restore/replay complete). Choreography:

- the wiring smoke (item 4) and the 1000× run (item 5) happen in one
  scheduled session; their round-2 request
  (`phase4-oom-fix-and-capture-engine-proving/`) shares lab logistics —
  combine calendars, not scopes — and a mirror note goes in their
  request dir so their executor knows the session exists;
- **the bridge is in this choreography too**: the worker is
  production-deployed serving live Play sessions (bridge owns the
  restart window, their `72o` caveat), and the prior OOM killed
  snapstore as collateral on the shared box. Session hygiene: schedule
  through the bridge's window, and no long `RunWithFrameCapture`
  streams on the box during the 1000× session until the leak fix
  lands;
- the required-check installation lands in both CIs in the same window
  so neither repo carries an un-mirrored gate;
- hypervisor-side defects surfaced by the run are filed in their
  tracker and linked here; store-side ones get beads here.

## Phases-Track Verification

On your resolution we will:

1. re-run the harness's fake-backed tests including the
   deliberate-corruption negative from a clean checkout;
2. audit the 1000× evidence (child count, per-child ref table,
   hostnames, revs) and re-verify one sampled child's ref by
   re-executing it;
3. confirm both CI checks exist and that the corruption demo turned
   one red.

## Handback Shape

Immediate items (1–2) can be resolved early: append a short
`04-resolution-immediate.md` when `675` is un-blocked and `pov`
repaired — don't sit on cheap corrections while the gated tail waits.
The full resolution follows as `05-resolution.md` (or continue
numbering) with SHAs, evidence roots, BM rows, CI links; we respond
with a verification file.

## Contact / Tracking

- Beads: `675` (the epic), `pov` (side-fix), round-1's `28z`/`feb`/`nn4`
  (gate), `8qx` (parked).
- Plan authority: IMPLEMENTATION-PLAN §M8; INTEGRATION.md §1–2;
  ARCHITECTURE §7.1.
- Shared record: `docs/bench-baseline.md` — one hardware section for
  round-1 and M8 rows alike.
