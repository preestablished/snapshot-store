# WI5 - Closeout, Handback, And Session Hygiene

This file is the implementation closeout contract. The work is not done when
the 1000x command exits; it is done when trackers, docs, CI, and cross-repo
handback agree.

## Implementation Order

1. Reconcile beads and writable Dolt state per `01-tracker-and-entry.md`.
2. Inventory the hypervisor M7 harness and choose the host repo for M8 harness
   ownership.
3. Add fake-backed harness tests, ref-table persistence, shared-page accounting, and
   semantic-corruption negative.
4. Wire baseline-delta restore and FULL cadence; run a small smoke.
5. Run or confirm the qualified Phase 5 hardware rows on the same NVMe-class
   soak host.
6. Run M8 full 1000x acceptance and write the evidence root.
7. Install the bounded permanent checks in both repos.
8. Update `docs/bench-baseline.md` and request resolution files.
9. Close or update beads, push beads, commit code/docs, and push/merge per the
   repo's current workflow guidance.
10. Commit or otherwise publish this plan artifact before handing it to an
    implementation agent; an untracked `.agents/plans/...` directory is not a
    handoff.

## Session Hygiene

The request calls out shared-box risk. The full M8 session must observe:

| Risk | Required handling |
|---|---|
| Bridge production worker | Coordinate restart/load window with bridge owner before occupying the box |
| OOM/capture-engine leak | Do not run long `RunWithFrameCapture` streams during M8 until the hypervisor leak fix is landed or explicitly cleared |
| Single KVM runner | Use workflow concurrency or operator scheduling so M8 does not starve nightly drift/canary jobs |
| Dirty repo state | Evidence must record dirty status; final acceptance should use clean revs unless sign-off says otherwise |
| Hardware qualification | Do not accept SATA/local root numbers as NVMe-class M8 BM rows |

## Request Resolution Files

Write these in `.agents/requests/phase2-closeout-m8-joint-fork-integrity/`:

| File | When | Contents |
|---|---|---|
| `04-resolution-immediate.md` | After tracker correction and `pov`/Dolt state is resolved or shown absent | Bead graph, stale-blocker correction, Dolt push result, drift from original request |
| `05-resolution.md` | After full M8 closeout | Revs, evidence root, benchmark rows, CI links, semantic negative, deviations/sign-off |
| Later verification file | If phases-track responds | External verification result |

If the named request beads remain absent, say that plainly in `04-resolution-immediate.md`
and link the replacement bead IDs. Do not leave the reader hunting for
nonexistent `snapstore-675` or `snapstore-pov` records.

## Quality Gates

Minimum local gates before handback:

| Change area | Gate |
|---|---|
| Rust code | `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` in the repo changed |
| Snapshot-store evidence parser | Unit tests for schema pass/fail bars and stale/missing child rows |
| Hypervisor harness | Host-only fake tests plus ignored live smoke documented |
| CI workflow | Syntax checked by review and exercised by one run where feasible |
| Docs only | Markdown/link sanity and no stale command names |

Cross-repo changes require running the relevant gates in each repo. If a gate
cannot run locally because it needs KVM or the reference box, record the exact
operator command and the reason.

## Per-Repo Closeout Checklist

Run this checklist separately in snapshot-store and determinism-hypervisor for
any repo that received changes:

```bash
git status --short --branch
bd preflight
bd dolt pull
# close or update completed/in-progress beads here
bd dolt push
git add <changed files>
git commit -m "<repo-specific summary>"
git status --short --branch
```

Then follow that repo's current `bd prime` git guidance. In this snapshot-store
checkout, `bd prime` currently says the branch is ephemeral and code is merged
to main locally rather than pushed. If a later `bd prime` reports a normal
upstream-push workflow, follow that newer instruction and verify the branch is
clean and up to date afterward.

## Handback Summary

The final handback should include:

| Item | Required content |
|---|---|
| Tracker | Active/closed bead IDs and dependency graph summary |
| Evidence | `target/m8-joint-fork-integrity-<UTC>/evidence.json`, plus artifact URLs if CI uploaded them |
| Results | 1000/1000 ref identity, shared-page ratio, p50/p99 latencies, semantic negative result |
| CI | Required check names/links in both repos; deviation approval if full 1000x is operator-run |
| Docs | `docs/bench-baseline.md` section name and request resolution filenames |
| Residual risk | Any measured misses, hardware caveats, or hypervisor-side follow-up beads |

Closeout must push beads with `bd dolt push`. Follow the current `bd prime`
session close protocol for code/docs. If the branch remains ephemeral, merge to
local `main` as that protocol directs rather than inventing a remote push step.
