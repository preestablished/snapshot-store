# Requested Work

## Immediate Lane (No Gate — Do These Today)

1. **Correct the ledger — and make the gate tracker-enforced.** Edit
   `snapstore-675`'s description/comments to record the stale-blocker
   correction with citations (`-a6s`, `-9e4`, `-cw2`, `-4s9.29`) and
   its true phase placement (P2 joint close-out). Then file child
   beads for items 3–6 below **with dep edges** (`bd dep add` — the
   joint-run bead blocked by `28z`/`feb`/`nn4`; the harness bead
   unblocked), so `bd ready` enforces this request's gating instead of
   prose re-creating the documentary-blocker failure mode this very
   request corrects. Note: until `pov` is fixed these edits live
   local-only — acceptable; say so in the bead comment.
2. **`snapstore-pov` — escalate, then repair.** The fix needs an
   explicit owner (Matt) decision — that's the constraint, not agent
   effort. Surface the decision ask immediately; repair (no
   force-push) once given; record the procedure.
3. **Harness + tooling, inventory first (ungated by round-1;
   fake-testable).** Step 0 is an inventory: what did the hypervisor's
   `-4s9.29`/M7 acceptance (`m7_fork_verify.rs`, `dh-verify`) actually
   drive through the real store? Extend that machinery with the
   store-side per-child `PutSnapshot` ref-identity assertion if it
   fits — state which repo hosts the harness — and build greenfield
   only if it doesn't. Driver: the worker gRPC per INTEGRATION §2.
   Resumability mechanism: persist the per-child ref table (content
   addressing makes re-runs idempotent). Include the sibling-dedup
   measurement. The **corruption negative must be semantic**: alter
   one input event *before sealing* (or drive a modified burst), so a
   red result provably comes from ref divergence — a raw bit-flip just
   trips container checksums upstream and proves nothing about this
   gate.

## Gated Lane (Entry: Round-1 Resolved — `28z`/`feb` Measured Or The Recorded Hardware Escalation Taken; `nn4` Fixed)

4. **Complete the wiring gaps** (with the hypervisor): baseline-delta
   restore actually used (`restore_engine.rs:163` currently passes no
   baseline) and the §2.1(f) FULL-manifest `chain_depth` cadence —
   then a smoke before the big run. The OOM leak fix is deliberately
   *not* an entry condition: the leak is per-Run (retainers freed at
   Run teardown) and the 1000× run is 1000 short burst-Runs — but see
   the session-hygiene rule in `03-`.
5. **The joint acceptance.** One coordinated session: 1000× fork →
   restore → re-execute → `PutSnapshot`, every re-executed child's ref
   identical; dedup ≥94% measured; fork→commit / restore latencies vs
   ARCHITECTURE §7.1. Evidence in a timestamped `target/` root; BM
   rows appended to `docs/bench-baseline.md` (same hardware section as
   round-1's rows).
6. **Make it permanent — and flag the deviation if you take it.** The
   plan says "permanent in CI (P0 on failure)". The realistic shape,
   per hypervisor precedent, is a bounded per-merge/nightly variant
   (their 100-child canary) plus the operator-run full 1000× — prefer
   *extending their existing required-for-merge determinism lane* over
   inventing a parallel gate. If the full 1000× lives outside CI,
   record that explicitly as a plan deviation with phases-track
   sign-off in the resolution — not as silent compliance. The
   semantic-corruption demo turns the installed check red once,
   recorded.

## Acceptance Criteria

1. `675` corrected with citations; child beads + dep edges in place
   (immediate); `pov` decision obtained and repair recorded (on owner
   availability — the escalation timestamp counts as progress).
2. Harness merged (host repo stated, inventory findings recorded) with
   fake-backed tests including the semantic-corruption negative.
3. Joint-session evidence: 1000/1000 ref-identity table, dedup number,
   latency rows vs §7.1, hostnames + revs (both repos + image).
4. The regression gate live in the agreed lane(s) with links recorded
   in both trackers; deviation (if any) signed off in the resolution.
5. `docs/bench-baseline.md` updated — one hardware record shared with
   round-1's rows.

## Out Of Scope For This Request

- Round-1's scope (`28z`/`feb`/`nn4`) — predecessor for the gated lane.
- `snapstore-8qx` / the `snapstore/v1` proto upstream — parked; the
  control-plane round-2 request
  (`phase4-snapstore-promotion-and-vdev-playbook/`) covers the
  receiving side. Reciprocal handshake, mirrored here so both sides'
  texts agree: whichever side is ready first (their playbook, or this
  repo's authored schema) leaves the ready-signal in the other's
  request dir.
- M9 (`agz`) — Phase 8.
- Hypervisor-side defects surfaced by the joint run — filed to them,
  linked here.
