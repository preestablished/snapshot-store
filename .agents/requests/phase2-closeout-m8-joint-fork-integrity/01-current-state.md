# Current State (Evidence-Based)

Repo `main` at `3a8056c` (the round-1 filing commit on `3b665a7`),
clean tree, assessed 2026-07-07. Beads: 7 open, 0 in progress, all
`bd ready` — no *blocking* dep edges (feb carries one satisfied edge);
every "blocked" below is documentary (bead-description text), not
tracker-enforced.

## Round-1 Status

Unexecuted: no `phase5-readiness-*` plan dir, no
`target/phase5-readiness-*` evidence root, `28z`/`feb`/`nn4` untouched.

## The Stale Blocker

`snapstore-675` (`[EPIC]` M8 joint hypervisor-integration; note it is
already OPEN/ready in bd — the "blocker" is description text, so the
correction is a description/comment edit, not a state change):
description says "After determinism-hypervisor M4-M7." Hypervisor
reality (their tracker): `-a6s` M4 ACCEPT fork transparency +
frozen-parent reproducibility — closed; `-9e4` Tier-A CoW fork (incl.
"identical ref through the real store") — closed; `-cw2` M7 ACCEPT
fork-1000× harness, VerifyReplay all — closed; `-4s9.29` Linux M7 fork
VerifyReplay + nightly 100-child canary — closed 2026-06-20. The
prerequisite is met in full; only the bead text never caught up. And
per the phase matrix (store M8 in **P2**, the Phase-2 joint close-out
with hypervisor M7), the milestone is overdue, not early.

## What §M8 Owes (Plan + INTEGRATION.md)

1. **Joint 1000×-fork determinism regression**: fork one guest 1000×
   through the store; restore + re-execute each child; every
   `PutSnapshot` of a re-executed child returns a ref identical to the
   original child's (content-address ⇒ bit-identity); installed as a
   permanent required check in both repos' CI.
2. **BM rows**: fork→commit and restore latencies vs ARCHITECTURE §7.1;
   sibling dedup ≥94% shared pages; recorded in
   `docs/bench-baseline.md` (the same file round-1's rows land in —
   shared hardware record, one box, one baseline).
3. **Wiring — narrower than it looks.** The hypervisor workspace
   *already* path-deps `snapstore-client`/`-server`, and their M4/M7
   acceptances ran against the real store (`-9e4`: identical ref
   through the real store; `-4s9.29`: cross-slot identical child refs).
   The verified genuine gaps: their
   `dh-worker/src/restore_engine.rs:163` calls
   `resolve_pages(snapshot_ref, None, false)` — **baseline-delta
   restore is never used** — and FULL manifests appear only for roots,
   not the §2.1(f) `chain_depth` cadence. Also likely reusable rather
   than greenfield: the hypervisor's `m7_fork_verify.rs` /
   `-4s9.29` acceptance machinery — inventory before building
   (see `02-` item 3).

## Side-Fix Required For Any Of This To Record Cleanly

`snapstore-pov`: the beads Dolt remote diverged ("no common ancestor"),
blocking normal `bd dolt push`. Its own description requires an explicit
owner decision and forbids force-push. Without it, M8's bead/evidence
updates strand locally — fix it early, with the owner in the loop.

## Cross-Repo Picture

- **Hypervisor**: done with its half's prerequisites; the joint session
  needs their coordination (worker wiring + a guest), not new
  milestones. Their round-2 request (OOM fix) is independent but shares
  lab logistics — one session calendar, two requests.
- **Orchestrator M6 / Phase 5 gate 5**: the downstream consumers.
- **`snapstore-8qx`** (vendored-proto swap): stays parked — doubly
  blocked on control-plane's unexecuted proto-freeze request (no
  `proto-v*` tag exists) *and* on this repo authoring its real
  `snapstore/v1` upstream (control-plane's round-2 request covers the
  receiving side when that day comes).
