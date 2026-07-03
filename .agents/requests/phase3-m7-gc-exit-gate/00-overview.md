# Request: Land M7 GC — The Last Unowned Phase 3 Exit Gate

## Who Is Asking

The `rom-operator-bridge` project, acting as the Phase 3 validation
surface. Filed 2026-07-03. This is the fourth request in a series that
has now cleared every other Phase 3 feeder: guest-sdk Ms4 (accepted
2026-07-02), the hypervisor framebuffer contract (`5698d7e`), and
reference-workload M4 engineering + follow-ups (through `d61e300`).

## Why snapshot-store, Why Now

Phase 3's exit gate (determinism docs,
`phases/phase-3-workload-in-the-box.md`) has four items. Three are
owned and moving. Item 4 is yours, and nothing has started:

> 4. snapshot-store GC property tests green (safety: never collects
>    pages reachable from a live manifest; completeness: collects
>    everything else).

The phase doc is explicit about timing: M7 is "needed before
exploration (Phase 5) can run long; **this is the last quiet window to
land it**." Once the coordinated boot/READY sequence completes and
first-room exploration starts, every GC bug becomes a live-fire
incident against real trajectory data instead of a property-test
counterexample.

Current state we verified (2026-07-03): `TriggerGc` returns
`UNIMPLEMENTED` (`crates/snapstore-server/src/service.rs` ~line 1030),
and your bead `snapstore-z5o` (P2, open, created 2026-06-10) names the
milestone. **Caution: that bead's head-start description overstates the
code** — see `01-…` for what actually exists (pins/tombstones are real;
the `gc_commit_gate` write side and GC read-path invalidate hooks are
not). The repo is clean on `main` with no commits since the phase-2
merge, apart from the commit filing this request.

## The Ask In One Paragraph

Implement M7 as your `IMPLEMENTATION-PLAN.md` already specifies it —
mark (epoch fence + commit gate), tombstone reaping, pack compaction
with index repoint and retry-on-race reads, manifest sweep,
`TriggerGc`, watermark auto-trigger, `gc_*` metrics — with the
**model-based property suite as the centerpiece** (that suite, green,
is the literal Phase 3 exit-gate wording), plus the crash-harness
extension with kills inside GC. The full acceptance and benchmark bars
are your plan's own; `02-…` states which subset the Phase 3 gate
actually needs, so you can sequence the rest.

## Files In This Request

| File | Contents |
|---|---|
| `01-context-and-anchors.md` | Verified current state, existing infrastructure to build on, deployment cautions |
| `02-what-the-phase-gate-needs.md` | The minimal gate-satisfying subset vs the full M7 bar; acceptance criteria |
| `03-verification-offer.md` | What the bridge side offers, evidence conventions, handback shape |
