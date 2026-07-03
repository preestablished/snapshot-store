# WI4 — Model-Based Property Suite (the Phase 3 gate)

Location (D1): `crates/snapstore-server/tests/gc_properties.rs`, plus a
`tests/gc_model/` support module. Dev-deps: `proptest` (workspace lock has
1.11.0 — the seeded-runner API in §6 is valid there), `snapstore-manifest`
with `features = ["test-strategies"]` (the precedent: manifest Cargo.toml
`test-strategies = ["dep:proptest"]`).

Feature plumbing (exact — three traps here):
- snapstore-server `[features]`: `gc-test-hooks =
  ["snapstore-store/gc-test-hooks"]` (a crate CANNOT dev-depend on itself
  with features; forward the store feature through the server's own).
- Declare the target with required-features so plain
  `cargo test --workspace` stays green (the hooks types are feature-gated
  out otherwise — note snapstore-store's `#[cfg(any(test, ...))]` `test`
  cfg does NOT apply to dependents):
  ```toml
  [[test]]
  name = "gc_properties"
  required-features = ["gc-test-hooks"]
  ```
- CI invocation: `cargo test -p snapstore-server --test gc_properties
  --features snapstore-server/gc-test-hooks` (valid syntax), plus a
  matching clippy invocation — the existing failpoints clippy line
  (ci.yaml:35) does NOT cover `gc-test-hooks` code (different feature,
  different crates); add a separate `cargo clippy -p snapstore-server
  --tests --features snapstore-server/gc-test-hooks -- -D warnings` step
  (06 §1).

Everything here runs **in-process** against `SnapshotStore` + `MetaDb` +
`run_gc_cycle` on TempDirs — no gRPC in the property loop (speed,
determinism). One additional RPC-level smoke test uses `serve_for_tests`
(build_server.rs:242) to cover TriggerGc end-to-end.

## 1. Op-sequence generator (new work — nothing existing provides this)

Generated value: `Vec<Op>` over a small alphabet, with generator-maintained
validity state (proptest `Strategy` over an op-tape; use `prop::collection`
+ state machine in a `fn ops_strategy(max_len: usize)`; see the manifest
crate's `prop_recursive` usage at lib.rs:772 for style):

```rust
enum Op {
    CommitFull { exp: u8, pages: PageSpec },            // new root manifest + node
    CommitDelta { parent: NodeSel, dirty: PageSpec },   // child manifest + node (fork siblings arise from repeated parents)
    CommitOrphan { pages: PageSpec },                   // put_snapshot with NO create_node (the "discard" path)
    PutPagesOnly { pages: PageSpec },                   // pages with no manifest (pre-fence-page ammo for Race A)
    Pin { target: RefSel }, Unpin { target: RefSel },
    Prune { node: NodeSel },
    Gc { aggressive: bool, interleave: Vec<InterleaveOp> }, // InterleaveOp ⊂ {CommitDelta, CommitOrphan, Pin, CreateNodeLate} pinned to GcPoint hooks
    Read { target: NodeSel },                           // resolve_pages during normal flow
}
```

`PageSpec` reuses `snapstore-testgen` content profiles (seeded, small —
8–64 pages/guest, dedup-heavy so packs share pages across siblings).
`NodeSel`/`RefSel` are indices into the model's live lists (generator keeps
a shadow list so selections are always valid). Keep `max_pack_bytes` tiny
in `StoreOpts` (e.g. 64 KiB) so sequences produce many packs and compaction
actually exercises multi-pack sweeps.

`Gc.interleave`: the suite installs a `GcHooks.at` callback that executes
the listed ops at chosen `GcPoint`s (e.g. `AfterCopy`, `BeforeFinalize`,
`BeforeManifestSweep`) — **controlled interleaving** of concurrent commits
during GC without nondeterministic threads. A separate reader thread covers
R2 (§4). `CreateNodeLate` = put_snapshot before the fence via prior ops,
create_node inside the hook → replays Race B.

## 2. Model (refcount-free oracle)

`struct Model` mirrors semantics only — no refcounts, no incremental state:
nodes (per experiment, with pruned flags), pins, manifests
(ref → entries + parent), the full page-content map. After every op, the
oracle recomputes from scratch:

```rust
fn reachable(&self) -> (HashSet<Ref32>, HashSet<PageHash>) {
    // roots: all un-REAPED node refs (pruned-but-unreaped included when
    // grace > 0; suite runs grace = 0 so reaped == pruned-subtree rows)
    // + pins; walk parent chains; union page hashes. Brute force each step.
}
```

Model GC applies the same visible semantics as the real cycle (reap pruned
subtrees at grace 0, drop unreachable manifests/pages) — but computed by
brute force, per IMPLEMENTATION-PLAN §M7's oracle rule.

## 3. Suite config

`GcOpts { compact_threshold: 1.01, rotate_active_first: true, tombstone_grace_cycles: 0 }`
for the quiescent-exactness property — threshold > 1.0 forces compaction of
every pre-fence pack, INCLUDING 100%-live ones (1.0 < 1.01; the quiescent
property therefore rewrites all data each cycle — intended, comment it),
and rotation first makes all data sweepable; without these, whole-pack
granularity legitimately retains garbage and completeness cannot be exact.

**Legal-outcome rule the model must encode:** an acked `PutPages` whose
pages were never referenced by a committed manifest MAY be collected (they
are unreachable; natural rotation with 64 KiB test packs constantly moves
them below the fence). A later `put_snapshot` referencing them then
correctly fails `MissingPages`, and the client re-puts (idempotent). The
generator must either re-put pages at commit time or treat that failure as
a legal outcome — NOT an R1 violation. Disclose this deviation from
ARCHITECTURE §4.3's "pages ≥ fence_pack are protected" intuition in
04-resolution.md (that rule protects only the current active pack, not
earlier unmanifested ingest). A second
property run uses default opts (0.5) and asserts **safety only** plus
"physical ⊇ reachable" (leak-bounded, no exactness) — proving the
production config path too.

## 4. The three named properties (cite invariants in test names)

- `prop_gc_safety_r1`: after every op (esp. after each `Gc`), for every
  model-reachable node/pin ref: `get_snapshot` succeeds and
  `resolve_pages(ref, None, false)` yields exactly the model's page bytes
  (byte-compare, not just presence). Covers commit chains, fork siblings,
  pruned subtrees, pins/unpins, GC at random points, and the interleaved
  commits from §1.
- `prop_gc_completeness`: after a **quiescent** aggressive `Gc` (no
  interleave ops), physical state == model reachable set exactly:
  `pages().unique_pages() == |model.reachable_pages|`, and
  `list_manifest_refs()` set-equals model reachable manifests, and meta has
  no reaped rows/tombstones left.
- `prop_gc_read_correctness_r2`: for `Gc` ops, spawn one reader thread
  before the cycle that continuously `resolve_pages`+byte-verifies a
  random sample of model-reachable refs until the cycle ends; any error or
  byte mismatch fails. (This one uses a real thread on purpose — R2 is
  about racing the repoint/unlink window; the `GcPoint` hooks make the
  window wide by sleeping briefly at `AfterRepoint`.) Also assert the
  retry counter (expose a test-only read-retry counter on PageStore behind
  `gc-test-hooks`) is > 0 across the nightly run — proof the race was
  actually exercised, not just survived.

Plus RPC smoke: populate via ops, `TriggerGc{compact_aggressively:true}`
over UDS, assert response counts and Stats fields move.

## 5. Negative proofs (gate AC 2 — "a suite that has never seen its subject fail proves nothing")

Three `#[test]`s (not proptests) that run a fixed seeded op-sequence with a
`Sabotage` mode (02 §8) and assert **the property fails**
(`catch_unwind` / run the property fn and expect Err), recording the seed:

| Sabotage | Broken invariant | Expected detection |
|---|---|---|
| `DropPinsFromRoots` | safety R1 (over-collection) | `prop_gc_safety_r1` reports missing pinned manifest/pages |
| `SkipIndexRemoveOfDead` (or skip one pack's compaction) | completeness | `prop_gc_completeness` reports leaked garbage |
| `UnlinkBeforeRepoint` | R2 (torn read) | `prop_gc_read_correctness_r2` reader errors |

Also `SkipLateRootsDrain` → safety failure via the Race A replay sequence —
include as a fourth if stable. Each negative test logs
`NEGATIVE-PROOF <sabotage> seed=<seed> detected=<what>` so 06's evidence
script can scrape the table. This is the guard-reversion discipline the
sibling repos used (reference-workload `fe91261`/`209b241`/`ef59c73`),
implemented as permanent tests instead of one-off reverts.

## 6. Case counts + seeded runner (gate AC 1)

proptest only auto-records **failing** seeds (`proptest-regressions/`); a
passing run's seed must be captured explicitly. Runner shape:

```rust
fn runner() -> TestRunner {
    let cases: u32 = env("GC_PROP_CASES").unwrap_or(64);      // dev default small
    match env("GC_PROP_SEED") {
        Some(s) => { log!("GC_PROP_SEED={s} cases={cases}");   // printed into evidence
                     TestRunner::new_with_rng(cfg(cases), TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes(s))) }
        None => TestRunner::new(cfg(cases)),
    }
}
```

Use explicit `runner.run(&strategy, |ops| ...)` calls inside plain
`#[test]`s (not the `proptest!` macro) so the runner/seed/case-count is
controllable and loggable. PR CI sets `GC_PROP_CASES=500`; nightly and the
evidence run set `GC_PROP_CASES=10000 GC_PROP_SEED=<recorded>` (06).
Shrinking stays enabled (default) — counterexample seeds + shrunk op-tapes
go in failure output.

## 7. Runtime budget

500 cases × (≤64 ops × small pages) must stay in single-digit PR-CI
minutes: keep guests ≤64 pages, packs 64 KiB, `PAGE_SIZE` fixed 4096 (real
constant — do not fake it), and run the three properties over the same
generated tape per case (one store build, three checks) rather than three
independent generations. Measure and record actual wall-clock in the
evidence; if 10k cases exceed ~45 min nightly, split across the two
existing nightly job slots or shard by seed — do NOT lower the count.
