# Context And Anchors (Verified 2026-07-03)

## What Already Exists To Build On

- **The real head start** (correcting bead `snapstore-z5o`'s
  description, which overstates it): pins and tombstones are genuinely
  stored and honored (tables, `pin()` RPC, CLI, crash-harness checks),
  and tombstone counts surface in stats (`service.rs` ~line 1021). But
  the `gc_commit_gate` is an explicit **no-op stub** — a `RwLock<()>`
  whose read lock is taken on the commit path
  (`crates/snapstore-store/src/lib.rs` ~line 384, commented "no-op stub
  until M7/M9"); **the write side (the mark fence) does not exist**,
  and there are no GC-tied read-path invalidate hooks (the only
  invalidation code is generic pack-handle cache invalidation used
  during normal ingest). Scope the mark-fence and read-retry work as
  implementation, not wiring. Recommend correcting the bead description
  when you claim it — it currently encodes the same overstatement.
- **Test infrastructure crates, characterized precisely:**
  `snapstore-crash` is the crash harness M7 extends (kills inside GC —
  compaction copy, index repoint, unlink) and is also where seeded
  **op-sequence** generation lives today (`src/child.rs`, `Scenario`).
  `snapstore-testgen` generates synthetic guest **page content**
  (zero/text/random profiles, dirty rates) — useful for populating
  stores, but it has no fork-tree/prune/pin op-sequence machinery. The
  model-based generator the property suite needs (commit chains, fork
  siblings, pruned subtrees, pins, GC interleavings) is **new work**;
  the oracle shape (refcount-free, brute-force mark each step) is
  specified in `IMPLEMENTATION-PLAN.md` §M7.
- **Where the suite lives is a decision point** (flagging it because it
  interacts with CI wiring): only `snapstore-manifest` uses proptest
  today. New dev-dependency on `snapstore-store`, a tests/ suite in
  `snapstore-server`, or a dedicated crate are all defensible — your
  call, but state it in the resolution so the CI case-count wiring is
  reviewable. Note proptest does not log the master seed of a *passing*
  run by default (`proptest-regressions/` only records failures) — the
  "recorded with seed" acceptance item needs an explicitly seeded
  runner (e.g. `TestRunner::new_with_rng`) with the seed logged.
- **Scratch servers are a solved pattern here:** `crates/snapstore-server/tests/server.rs`
  has `serve_for_tests` + TempDir + UDS, including a `TriggerGc →
  UNIMPLEMENTED` stub test (~line 793) sitting ready to become the
  first real assertion.
- **Invariant references:** safety is ARCHITECTURE R1, read-correctness
  during GC is R2 (retry-on-race) — cite them in test names/comments so
  the suite reads as the gate evidence it is.

## Sibling-Ecosystem Precedents Worth Copying

Three sibling repos just went through exactly this "prove it can fail"
discipline; their patterns transfer directly:

- **Negative tests are mandatory, guard-reversion-proven:** every new
  invariant test should be shown to fail when its guard is disabled
  (see reference-workload commits `fe91261`/`209b241`/`ef59c73`, each
  verified by temporarily reverting the guarded branch). A GC safety
  suite that cannot detect an over-collection proves nothing.
- **Evidence discipline:** artifact root + `evidence.json` with git
  rev, host/kernel, per-case tables and recorded hashes — the shape
  guest-sdk's Ms4 acceptance used
  (`~/git/preestablished/guest-sdk/target/m4-acceptance-20260702T135319Z/`) and the
  hypervisor's M9 acceptance before it.
- **CI reality in this repo:** you already have `ci.yaml`
  (fmt/clippy/test + crash-smoke PR gate) and `nightly.yaml`
  (1000-cycle crash suite, fuzz, perf) on hosted runners — and
  `nightly.yaml`'s own header argues self-hosted is not warranted here.
  The ≥500-PR / ≥10k-nightly property case counts likely map straight
  onto that existing split with no new lane. If you do decide a
  self-hosted lane is needed, the shared Intel runner label is
  `[self-hosted, intel, kvm]` for guest-sdk and reference-workload,
  `[self-hosted, kvm-intel]` for the hypervisor — pick deliberately.

## Deployment Cautions

- **A production snapstore-server from this repo is live on this host**
  (pid file under the rom-bridge-o73 runtime; serving the deployed
  `dh-workerd` over UDS). Do not test against it, restart it, or point
  anything at its config/data root. All GC testing runs against
  scratch instances.
- The coordinated Phase 3 boot/READY sequence (imminent, operator-run)
  will write a new READY snapshot **through that deployed instance**.
  If your work needs a server restart or schema/on-disk change to the
  deployed instance, coordinate with the operator — the bridge side
  owns the restart choreography for the deployed runtime and its
  lease-invalidation caveats.
- The proto surface (`TriggerGc` request/response) already exists,
  both messages currently empty. The project's standing rule ("the
  proto crate only grows", determinism `phases/README.md`) nominally
  routes schema changes through `control-plane/proto/` — but
  control-plane's snapstore proto is an unwired placeholder and your
  own `proto/snapshot_store.proto` declares itself the canonical
  vendored copy until `adopt-snapstore-proto-v1` (`snapstore-8qx`)
  lands. Land additive fields in your canonical copy; mirror to
  control-plane when that adoption happens. Whether `TriggerGc` becomes
  synchronous (counts in the response) or fire-and-forget (poll `Stats`
  afterward) is your call — state it in the resolution, since both the
  property harness and the joint verification key off it.

## Cross-Repo Trail (For Navigation)

- Phase state and prior request/resolution pairs:
  `~/git/preestablished/determinism-hypervisor/.agents/requests/rom-bridge-getframebuffer-region-contract/`
  (00–06), `~/git/preestablished/guest-sdk/.agents/requests/phase3-ms4-region-publication-acceptance/`
  (00–06), `~/git/preestablished/reference-workload/.agents/plans/phase3-m4-first-room-unblock/`
  (00–07 + addenda). The 07-verification addendum in the last one is
  the current Phase 3 scoreboard.
