# What The Phase 3 Gate Needs (vs The Full M7 Bar)

Your `IMPLEMENTATION-PLAN.md` §M7 defines the complete milestone. The
Phase 3 exit gate needs a specific subset **green and evidenced**; the
rest (notably the 100k-node/30 GB benchmark) matters for Phase 5 scale
and can trail. Sequencing is yours — this split just says what unblocks
the phase gate soonest.

## Gate-Required (Phase 3 Exit Item 4)

1. **A working GC**: mark with the epoch fence + commit gate, tombstone
   reaping, manifest sweep, and `TriggerGc` actually collecting — the
   property suite needs a real subject. Pack compaction with index
   repoint + retry-on-race reads is part of the M7 definition and the
   crash-kill surface, so it is in scope; if you find a way to stage it
   after a first sweep-only gate pass, say so explicitly in
   `04-resolution.md` rather than silently narrowing — disclose and
   proceed; the bridge side validates post-hoc in `05-verification.md`,
   no synchronous sign-off needed.
2. **The model-based property suite, green**, with the plan's three
   invariants as named properties:
   - (a) *safety* — GC never removes a page or manifest reachable from
     a live manifest (R1), across commit chains, fork siblings, pruned
     subtrees, pins/unpins, GC at random points, and concurrent commits
     during GC via controlled interleaving;
   - (b) *completeness* — after a quiescent GC, physical pages equal
     the model's reachable set exactly;
   - (c) *read correctness during GC* — reads served concurrently
     return correct bytes (R2 retry path).
   Oracle: refcount-free, brute-force mark from scratch each step, per
   the plan.
3. **Negative proof**: at least one deliberately-broken-GC run per
   invariant class demonstrating the suite catches it (over-collection
   for safety, leaked garbage for completeness, torn read for R2) —
   guard-reversion or fault-injection, recorded in the evidence. A
   property suite that has never seen its subject fail is not gate
   evidence.
4. **Crash-harness extension**: kills inside GC (compaction copy, index
   repoint, unlink) with recovery never losing reachable data — space
   leaks reclaimed by the next cycle are acceptable, per the plan.
5. **CI wiring + case counts**: the plan's own AC — ≥ 500 property
   cases in PR CI, ≥ 10k nightly — which likely maps onto your existing
   `ci.yaml`/`nightly.yaml` split with no new lane (see `01-…`'s CI
   note).

## Explicitly Deferrable Past The Gate

- The **benchmark bar** (100k-node tree, < 60 s under 200 MB/s ingest,
  p99 commit latency < 2× idle): Phase 5 material — and genuinely
  separable: the plan lists it under §M7's `BM:` header, distinct from
  the `AC:` items (this split is our sequencing judgment, offered for
  your overrule). File it as its own bead when you defer it and
  `bd dep add` it against `snapstore-z5o` (and the M9 watermark bead if
  related); don't let it block the gate.
- Watermark **auto-trigger** polish and `gc_*` metrics dashboards
  beyond basic counters — needed for operability (Phase 6), not for
  the property-suite gate. Basic `gc_*` counters should still land with
  the implementation since the tests will want them.

## Acceptance Criteria

Verified by you (durable artifacts, evidence.json discipline):

1. Property suite green at the PR-CI case count, and one ≥ 10k-case
   run recorded with seed, git rev, and case table. (Seed capture for a
   *passing* proptest run needs an explicitly seeded runner — see
   `01-…`; proptest only auto-records failing seeds.)
2. The three negative proofs recorded (what was broken, what the suite
   reported, counterexample seed).
3. Crash matrix extended and green; recovery evidence for each new
   kill point.
4. `bd` bead `snapstore-z5o` closed with the artifact root in the
   reason.

Verified jointly / by us:

5. Restore-after-GC, tightened for scheduling: using the property
   suite's own op-sequence generator (new work — see `01-…`), you
   populate a scratch snapstore with a fork tree of at least 1,000
   nodes including at least 100 pruned subtrees, run `TriggerGc`, and
   confirm via `Stats` (or the response, per your sync/async decision)
   that collection actually ran. Hand back the scratch data-root path
   plus the exact refs expected to survive. We then drive a scratch
   `dh-workerd` against that data root and restore every surviving
   ref; any restore failure is a joint-verification failure to resolve
   before criterion 4's bead-close is honored.
