# Resolution: M7 GC Landed (Phase 3 Exit-Gate Item 4)

Filed 2026-07-03 by the snapshot-store side, per the handback shape in
`03-verification-offer.md`. Plan + two-subagent review trail:
`.agents/plans/phase3-m7-gc-exit-gate/` (00–07).

## Commits on main

| SHA | Contents |
|---|---|
| `45007a1` | Plan (00–06) |
| `9a2deee` | Plan revision after two independent reviews (07-review-log.md — 3 blockers, 5 majors found in the draft design, all fixed pre-implementation) |
| `3a539c7` | WI1 + WI2(store): storage surfaces, epoch/commit-gate protocol, sweep engine |
| `e4511cf` | WI2(orch) + WI3: orchestrator, TriggerGc, metrics, `[gc]` config, watermark auto-trigger, CLI/client |
| `0de6573` | WI4 + WI5: property suite (four engine bugs found+fixed), crash-harness GC kills (one more engine bug found+fixed), evidence script |
| (this commit) | Evidence artifacts recorded, resolution filed, bead closed |

## What landed (the full M7 `AC:` scope — nothing staged out)

Mark with epoch fence + commit-gate write side; tombstone reaping
(grace-cycle horizon); pack compaction with index repoint +
retry-on-race reads; manifest sweep; `TriggerGc`; watermark
auto-trigger; `gc_*` metrics; the model-based property suite as the
centerpiece; crash-harness kills inside GC (six new failpoints). The
`BM:` benchmark bar is deferred as its own bead (below), per your 02
split.

## Decisions you asked us to state

- **TriggerGc semantics (D2):** synchronous by default — the response
  carries `nodes_reaped / manifests_deleted / pages_reclaimed /
  bytes_reclaimed / packs_compacted / duration_ms`; `detach=true` gives
  the API.md fire-and-forget shape (poll `Stats`). `already_running` is
  a response field, not a gRPC error (R4 latch). Proto fields are
  additive on the vendored canonical copy; mirror to control-plane when
  `adopt-snapstore-proto-v1` (`snapstore-8qx`) lands. Note the grace
  design consequence: a freshly-pruned subtree reaps on the *second*
  cycle (cycle 1 records the fence horizon) — the CLI/RPC evidence
  below shows exactly that.
- **Suite location (D1) + CI (D7):** `snapstore-server/tests/gc_properties.rs`
  behind a `gc-test-hooks` feature chain (server→store→pagestore),
  `[[test]] required-features` so plain workspace tests stay green.
  PR CI: 500 cases (env `GC_PROP_CASES`). Nightly: `gc-properties-deep`
  sharded 4×2500 with `GC_PROP_SEED=${{ run_id }}<shard>` logged — 10k
  in one hosted-runner slot exceeded the timeout at measured
  throughput; the plan sanctioned sharding over lowering the count. No
  new runner lane.
- **Race protocol (disclosed deviation from the doc's fence-only
  rule):** ARCHITECTURE §4.2–4.3's fence rule alone does not protect
  (a) pages ingested pre-fence but referenced by post-fence manifests,
  nor (b) CreateNode/Pin of a pre-fence orphan manifest. We added a
  gated, *validating* `register_live_ref` (registration into an
  epoch-scoped late-roots set + full dependency-closure verification,
  both under the commit gate) and made the sweep finalize packs and
  manifest-unlink batches under the gate write lock with
  drain-and-re-mark straggler loops. `put_snapshot` (both exit paths,
  including the idempotent early return), `create_node`, and `pin` all
  register. Full protocol: plan 02 §1; adversarial-review trail: plan 07.
- **Pin behavior change:** pinning a ref that does not resolve to a
  stored manifest (or whose pages were collected) is now
  `FAILED_PRECONDITION`. Previously the handler did no validation and a
  garbage pin was silently created (a latent `DanglingPin`). Flagging
  since `replay-renderer` pins goal paths (R5) — valid refs are
  unaffected.
- **Legal-outcome rule (disclosed):** an acked `PutPages` whose pages
  were never referenced by any committed manifest is collectible
  garbage; a later commit referencing them gets `MissingPages` and
  re-puts (idempotent). The model encodes this; ARCHITECTURE §4.3's
  "pages ≥ fence_pack" intuition protects only the current active pack.
- **physical_page_bytes:** now `unique_pages × 4133` (37-byte record
  header included; pack header/footer overhead excluded, per-field
  comment says so). This settles the accounting debt service.rs
  assigned to M7.

## Engine bugs the test suites caught (the "prove it can fail" dividend)

All five were real, all fixed in `0de6573` with the finding tape/seed in
a code comment:

1. `delete_pack` left the swept pack in the deferred-fdatasync set —
   the next commit's group-commit `sync()` ENOENT'd and **every
   subsequent commit failed** until restart.
2. `rotate_active`'s empty-pack skip could leave the fence below a
   prior cycle's compaction packs — their garbage unreclaimable while
   ingest is idle.
3. The R2 read-retry arm matched only the `Pack`-shaped ENOENT — the
   retry was **unreachable for the exact repoint→unlink race it exists
   for** (fresh opens surface as `StoreError::Io`). Found by the racing
   reader at seed 42; the `GC_READ_RETRIES` counter was 0 before the
   fix and fires reliably after.
4. Reopen after a kill between GC-pack seal and sidecar write persisted
   a CRC-valid **empty** sidecar (index-scan sidecar write ×
   first-writer-wins dedup) — every compacted page silently
   unreachable on the next open. fsck gained
   `SidecarRecordCountMismatch`; both recovery sites now write sidecars
   from scanned records.
5. (Design-review stage, pre-implementation:) the draft late-roots
   protocol protected registered manifests but not their dependency
   closures — three R1-violating interleavings, fixed in the plan
   before any code (plan 07, blockers A1–A3).

## Evidence

Artifact root: `target/m7-acceptance-20260703T063635Z/` (evidence.json:
git rev `0de6573`, host infra-control / kernel 6.8.0-124, rustc 1.96.1,
per-section results; produced by `scripts/m7-evidence.sh`, re-runnable
verbatim — the run necessarily precedes the commit recording it, so
`git_status_clean:false` in evidence.json reflects only these
resolution/fixture files, not code).

1. **Property suite, deep seeded run:** `GC_PROP_CASES=10000
   GC_PROP_SEED=20260703` — **7 tests passed, 0 failed** (the 10k-case
   three-check property plus negative proofs, R2 exerciser, RPC smoke),
   wall-clock 7520 s release on the pinned six cores. Seed line printed
   into the log (proptest records only failing seeds; the runner is
   explicitly seeded per plan 04 §6). R2 retry counter across the run:
   **21 retries** taken by the repoint→unlink race path (plus the
   deterministic exerciser's guaranteed hit) — the race was exercised,
   not merely survived.
2. **Negative proofs** (permanent tests, not one-off reverts):
   `DropPinsFromRoots` → safety R1 detects the over-collected pinned
   manifest; `SkipIndexRemoveOfDead` → completeness detects leaked
   index entries; `UnlinkBeforeRepoint` → deterministic in-window torn
   read at `AfterUnlink`; `SkipLateRootsDrain` → safety detects the
   lost mid-cycle commit. Scraped table in
   `negative-proofs.txt` at the artifact root.
3. **Crash matrix:** `DONE cycles=1000 inv_failures=0 fsck_violations=0
   matrix_cycles=750 matrix_failures=0 elapsed=287.94s
   total_leaked_pages=1640` (1000 randomized cycles + 15-failpoint
   matrix ×50, seed 20260703; six GC kill points:
   compact-copy, compact-seal, index-repoint, pack-unlink,
   manifest-unlink, reap-txn). Recovery invariant: journal-reachable
   refs (nodes + pins − reaped subtrees carried in `gc_done` lines)
   always resolve; space leaks are recorded and reclaimed by the
   post-recovery cycle.
4. **Joint restore-after-GC input (your criterion 5):**
   `target/m7-joint-fixture-20260703/` — scratch data root (1,000
   nodes / 4 experiments / 100 pruned subtrees / 20 pins), populated by
   `snapstore-crash populate-gc-fixture --seed 1290`, GC'd via
   `TriggerGc{compact_aggressively}` over UDS (two cycles; the stats
   trail is in the directory), `expected-surviving-refs.txt` with 900
   refs — **all 900 verified to resolve post-GC** from this side.
   README.md in the directory has the bridge-side procedure. The
   deployed instance and its data root were never touched.

## Deferred (with bead IDs)

- `snapstore-feb` — the M7 `BM:` benchmark bar (100k-node tree, <60 s
  under 200 MB/s ingest, p99 < 2× idle); `bd dep`'d on `snapstore-z5o`;
  needs NVMe-class hardware (same posture as `snapstore-28z`).
- `snapstore-nn4` — pre-existing `page_channel_fallback` test flake
  (fails ~30–50% on clean main; verified unrelated to M7).
- No FullStack (gRPC-server-kill) GC crash scenario: the release server
  has no failpoints and random-timing kills of short cycles add flake,
  not coverage (plan 05 §5). Say the word and we file a bead.

## Bead

`snapstore-z5o` closed with the artifact root in the reason — with the
explicit note that criterion 4's close is honored only after your
criterion-5 joint restore check passes; we hold the fixture directory
until your `05-verification.md` lands. Known infra note: `bd dolt push`
fails with "no common ancestor" (`snapstore-pov`); the beads DB is
committed to git regardless.
