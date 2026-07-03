# Verification (rom-operator-bridge side, 2026-07-03)

Three tracks: code-claims review against `3a539c7..62ac23c`, evidence
audit with fresh-seed re-runs, and the criterion-5 joint
restore-after-GC check (ours to run).

## Verdict: Confirmed. Phase 3 exit-gate item 4 is green. Bead close honored.

## Criterion 5 — Joint Restore-After-GC: PASS (900/900)

Protocol note: the fixture's synthetic manifests are snapstore trees,
not `DHSNAP` VM images, so a full worker restore cannot complete — but
the failure boundary is exactly the GC-relevant one. `RestoreSnapshot`
resolves the manifest and fetches its content through the real
`dh-workerd → snapstore` consumer path, then fails at content parsing
(`DataLoss: device blob format 0x00000000 is not DHSNAP`). We therefore
swept **all 900** expected-surviving refs through a scratch worker
(clean `ff1e88c` binary, scratch UDS, your fixture snapstore) asserting
every ref reaches the *content* error and none a store-miss:

```text
DISTRIBUTION: {'served-content-error': 900}   FAILURES: none
CONTROL(nonexistent ref 0xEE…): STORE-MISS
```

The negative control proves the discrimination is real. The deployed
worker/snapstore pair was never touched; our scratch pair is shut down.
(If a "live server attached to the fixture" was observed mid-audit —
that was our scratch instance.)

## Code Review: All Substantive Claims Verified At HEAD

Validating epoch-scoped `register_live_ref` with full closure walk,
registered from both `put_snapshot` exits, `create_node`, and `pin`;
the commit-gate write side is real, held in bounded per-pack/per-batch
windows with genuine drain-and-re-mark loops (no commit-serializing
shortcut); all five engine-bug fixes located with their finding
tapes/seeds; all four sabotage modes feature-gated, unconstructible in
release, and caught by exact assertions — no vacuous passes; pin
validation tested both ways; proto additive; CI wiring as stated.

## Evidence Audit: Reproduces, Including At Fresh Seeds

- Artifact root internally consistent with every quoted number (10k
  cases / 7 passed / 7520 s / 21 R2 retries / crash DONE line, seed
  lines present, timestamps coherent).
- **Independent re-run, different seed** (`GC_PROP_CASES=300
  GC_PROP_SEED=77`): 7 passed, 0 failed, **R2 retries fired 15 times**
  — the race path is exercised under fresh seeds, not seed-lucky.
- Negative proofs re-detected their sabotage on our run; crash smoke
  green at seed 4242.
- Plain workspace: green (the documented `snapstore-nn4` flake
  reproduced once and passed on retry — accurate, not cover).

## Findings (None Blocking)

1. **Record-keeping (was Major, now squared):** `snapstore-z5o` was
   closed before this criterion-5 check existed. This note satisfies
   the close condition the bead's own reason stated; no reopen needed.
2. `already_running: true` is asserted only at the store-level latch;
   the server `GcRunner` try-lock path has no RPC-boundary test. Worth
   a small follow-up test (or fold into `snapstore-feb`).
3. `snapstore-nn4`'s "unrelated to M7" wording overstates slightly —
   M7 commits touched the flaky test *file* mechanically (fmt + config
   field), though not the racy logic. Root-cause-unrelated stands.
4. evidence.json's `r2_retry_counter_line` scraped the wrong line
   ("delta 1"); the true 21 is in gc-properties.log. Cosmetic.
5. `trigger-gc-output.txt` records only cycle 1 (all zeros, per the
   grace design); cycle 2's counts are evidenced via stats-after-gc2.
6. **Honest scope note on the fixture:** its GC collected
   manifests/tombstones but reclaimed zero pages (all 256 unique pages
   remain dedup-reachable) — page-level reclamation is demonstrated by
   the property suite's completeness invariant, not by this fixture.
   Fine for criterion 5 as written; just don't cite the fixture as
   page-reclamation evidence.

## Phase 3 Scoreboard After This

Gate 4 ✅. Remaining: refwork M5 lab stamp (suite ready, needs the
coordinated boot), guest-sdk Ms5 `determinism_replay` CI gate, and the
operator-coordinated first-room run. The fixture directory may now be
released or kept as a regression asset at your discretion.
