# WI6 — CI, Evidence, Joint Verification, Handback

## 1. CI wiring (gate AC: ≥500 cases PR, ≥10k nightly — D7: no new lane)

`ci.yaml` rust job, after the existing failpoints test step (both jobs
need the pinned control-plane sibling checkout — copy the existing
checkout block verbatim):

```yaml
      - run: cargo test -p snapstore-server --test gc_properties --features snapstore-server/gc-test-hooks
        working-directory: repo
        env:
          GC_PROP_CASES: "500"
```

Feature-lint wiring (two separate concerns — do not conflate):
- failpoints: add `snapstore-meta` to the existing failpoints clippy/test
  lines (ci.yaml:35/37) — it gains the feature for `gc-reap-txn` (01 §4).
  snapstore-server does NOT get a failpoints feature.
- gc-test-hooks: a NEW step `cargo clippy -p snapstore-server --tests
  --features snapstore-server/gc-test-hooks -- -D warnings` (the
  failpoints line cannot lint this code — different feature).

`nightly.yaml`, new job:

```yaml
  gc-properties-deep:
    runs-on: ubuntu-latest
    timeout-minutes: 90
    steps:
      # (checkout blocks as in the other jobs)
      - run: cargo test -p snapstore-server --test gc_properties --features snapstore-server/gc-test-hooks -- --nocapture
        working-directory: repo
        env:
          GC_PROP_CASES: "10000"
          GC_PROP_SEED: ${{ github.run_id }}
```

`--nocapture` so the logged `GC_PROP_SEED=… cases=…` line lands in the CI
log (the "recorded with seed" AC). Existing crash-suite job automatically
picks up the extended matrix (05 §4).

## 2. Evidence run (gate AC 1–3; sibling-precedent discipline)

Artifact root: `target/m7-acceptance-<UTC-timestamp>/` (shape:
guest-sdk's `m4-acceptance-20260702T135319Z`). Produce `evidence.json`
with: git rev, host + kernel (`uname -a`), rustc version, and per-section
results. Run on the reference box (taskset -c 0-5 per bench posture memory
— though no benchmark gates here, keep the convention). Contents:

1. **Property suite**: one `GC_PROP_CASES=10000 GC_PROP_SEED=<chosen>`
   run; record seed, case count, wall-clock, per-property pass, and the
   R2 retry counter (>0 required, 04 §4).
2. **Negative proofs**: the scraped `NEGATIVE-PROOF` table (04 §5) —
   sabotage mode, seed, what the suite reported.
3. **Crash matrix**: `--cycles 1000 --matrix-passes 50 --seed <fixed>`;
   per-failpoint pass counts, leak-counter summary, zero invariant
   violations.
4. **Joint restore-after-GC input** (§3 below): data-root path + expected
   surviving refs file.

Write a small `scripts/m7-evidence.sh` that runs 1–3 and assembles
`evidence.json` so the bridge side can re-run it verbatim (their review
re-runs and recomputes hashes).

## 3. Joint verification artifact (acceptance item 5 — bridge drives it)

Mechanism (decided — review flagged "a test module can't be invoked"):
a `snapstore-crash` subcommand:

```
cargo run -p snapstore-crash -- populate-gc-fixture \
  --dir <scratch-data-root> --seed <u64> --nodes 1000 --pruned-subtrees 100
```

It reuses the generator via a small shared module (put the op-tape
generator + model in `snapstore-crash/src/gc_fixture.rs`, re-exported for
the property suite to import — snapstore-server already dev-depends on
nothing from crash, so the dependency goes crash → server-lib for
`run_gc_cycle` (no cycle: server does not depend on crash; verified).
The subcommand writes, into `--dir`:

- the populated store (≥1,000 nodes across ≥3 experiments, ≥100 pruned
  subtrees, pins on a sample of survivors),
- `expected-surviving-refs.txt` — one lowercase 64-hex ref per line,
  sorted, exactly the model's reachable manifest set,
- `fixture-manifest.json` — seed, params, git rev, counts.

Then: run `TriggerGc{compact_aggressively:true}` against a scratch server
on that root; confirm collection ran via the response counts and Stats.
Hand back:

- the scratch data-root path (leave the directory in place),
- `expected-surviving-refs.txt`,
- the exact server binary + config used.

The bridge drives a scratch `dh-workerd` against it and restores every
surviving ref. **Do not touch the deployed instance or its data root**
(pid under rom-bridge-o73 runtime).

## 4. Handback — `.agents/requests/phase3-m7-gc-exit-gate/04-resolution.md`

Write it per the request's 03 file: commit SHAs on main; artifact root;
case counts + seeds; negative-proof table; crash-matrix results; the D1–D7
decision statements (00-overview) — explicitly including: TriggerGc
sync/detach semantics (D2), suite location + CI wiring (D1/D7), the
late-roots protocol as a disclosed deviation from the doc's fence-only
rule (02 §1 — disclose, don't silently narrow), the R2-read-path
"already existed" correction, and the deferred benchmark bead id (§5).
The bridge responds with 05-verification.md; the joint restore check must
pass before the bead close is honored.

## 5. Bead hygiene (gate AC 4)

- Deferred benchmark: `bd create "M7 BM: GC benchmark bar (100k-node tree, <60s under 200 MB/s ingest, p99 commit <2x idle)" -d "Deferred from the Phase 3 gate per request 02 (BM: vs AC: split). Needs NVMe-class hardware — see snapstore-28z precedent. Measure GC of ~30 GB physical, 50% garbage; record in docs/bench-baseline.md." -t task -p 2 --silent`
  then `bd dep add <new> snapstore-z5o` (and relate to `snapstore-agz`
  M9 watermarks if the implementation touches shared config).
- Close `snapstore-z5o` with `-r` naming the artifact root — only after
  the joint restore check (per acceptance criteria ordering, criterion 5
  gates criterion 4's close being honored; close may be filed with the
  resolution but note the joint-check dependency).
- Session close protocol: `git pull --rebase`, `bd dolt push`, `git push`,
  verify up-to-date. Note: `bd dolt push` currently fails with "no common
  ancestor" (`snapstore-pov` tracks it) — do not let that block `git push`;
  mention it in the handback.

## 6. Suggested implementation order (for the coding agent)

Note: the 00-overview diagram shows WI4 depending only on WI2, but WI4's
RPC smoke test needs WI3's TriggerGc handler, and WI5's child needs
`run_gc_cycle` importable (placed by WI2/WI3). Follow THIS linear order;
do not parallelize WI4 ahead of WI3.

1. WI1 (01) — surfaces + unit tests; `cargo test --workspace` green.
2. WI2 (02) — engine + in-crate tests; failpoints compile under the
   feature; clippy failpoints line green.
3. WI3 (03) — proto/server/config/CLI; update the UNIMPLEMENTED tests.
4. WI4 (04) — generator + model + three properties at low case counts;
   iterate on WI2 bugs; then negative proofs; then 500/10k runs.
5. WI5 (05) — harness ops + matrix + recovery checks; 25-cycle smoke.
6. WI6 — CI edits, evidence run, joint artifact, resolution, beads.

Commit per WI (logical units); run the full PR-CI command set locally
before each commit (fmt, clippy -D warnings incl. failpoints line, test
--workspace, failpoints tests, crash smoke, gc properties at 500).
