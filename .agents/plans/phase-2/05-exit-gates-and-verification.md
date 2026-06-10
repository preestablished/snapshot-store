# Phase 2 (snapshot-store M4–M6) — gates, measurement, sign-off

Of the program's Phase 2 exit gates, this plan owns:

> 3. snapshot-store crash-injection suite green (commit ordering: pages →
>    manifest → node row survives kill -9 at every failpoint).

…and the **store-side prerequisites** for gates 1, 2, and 4 (Platform
Milestone 1, latency budgets, worker daemon) — those are signed at M8/joint
time, after `determinism-hypervisor` lands M4–M7. This document defines what
"M4/M5/M6 done" means so hypervisor M4 can start on a stable surface.

## Gate S1 — service surface complete (unblocks hypervisor M4)

**Definition.** Every `determinism.snapstore.v1` RPC from API.md §1 is served
on TCP + UDS (with `TriggerGc` as documented `UNIMPLEMENTED`), and
`snapstore-client` exposes the full typed surface with transport fallback,
footer verification, and the INTEGRATION.md §6 retry policy.

**How measured.**

```bash
cargo test -p snapstore-server -p snapstore-client      # incl. error-detail round-trips
cargo test -p snapstore-server --test e2e_exploration   # reduced step count (PR profile)
E2E_STEPS=10000 cargo test -p snapstore-server --test e2e_exploration -- --ignored  # sign-off
```

Sign-off run: 10k steps × two concurrent experiments through the public API
only; final per-experiment Stats match driver bookkeeping; injected timeouts
force CreateNode blind-retries with **zero duplicate nodes**; tonic health
SERVING; Prometheus counters consistent (ingested == new + deduped, node
counts == driver's).

## Gate S2 — spec-conformance of formats (replaces phase-1 G2 artifacts)

**Definition.** `.spm` container and input-log container match API.md §2/§3
byte-precisely; manifest property suite + **new golden vector** green; flatten
correct vs naive reference.

```bash
cargo test -p snapstore-manifest -p snapstore-store
PROPTEST_CASES=4096 cargo test -p snapstore-manifest    # deep run at sign-off
```

**Rules.** Golden-vector discipline carries over from phase 1: it changed
*once*, deliberately, in the commit that introduced the `.spm` format; from
then on a failing vector means an accidental format break — revert or bump the
container version explicitly. Proptest regression files committed. After
hypervisor M4 starts, the container format is **frozen** absent a coordinated
version bump.

## Gate S3 — M4 benchmarks

| BM | Spec target | Gate on SATA reference box |
|---|---|---|
| PutPages over UDS gRPC (256-page msgs, dedup-warm = transport+hash bound) | ≥ 600 MB/s | spec number as-is (no disk writes involved) |
| PutPages cold (disk-bound) | — | informational; consistency-check against G1 (~450 MiB/s ceiling) |
| QueryNodes page of 1,000 over UDS | p50 < 4 ms | as-is |
| PutSnapshot already-paged (2k-entry delta) | p50 < 3 ms | as-is |
| CreateNode + 16 KiB inline log | p50 < 1.5 ms | as-is |
| UpdateNodes(256) | p50 < 3 ms | as-is |
| flatten 64-deep × 2k-entry chain (warm) | < 2 ms | as-is |

## Gate S4 — M5 benchmarks (MAP.md principle 2 — misses are release blockers)

| BM | Spec target | Gate on SATA reference box |
|---|---|---|
| PUT_BATCH dedup-warm (transport+hash bound) | ≥ 1.5 GB/s sustained | spec number as-is |
| PUT_BATCH cold sustained (disk-bound) | (1.5 GB/s assumes NVMe) | record actual; gate = G1-consistent (≥ 400 MiB/s); NVMe re-validation required at M8 |
| GET_BATCH warm (page cache) | ≥ 2.5 GB/s | as-is |
| 16 clients × 8 MiB deltas: p99 commit incl. fsync | < 40 ms | **record actual; fsync-bound on SATA — formal gate decision at sign-off** (see hardware rule) |
| 16 clients aggregate ingest | ≥ 1.2 GB/s | dedup-warm as-is; cold recorded |

**Hardware rule (G1 precedent).** Spec numbers assume the NVMe box
(ARCHITECTURE.md §7.1). Transport-/CPU-bound measurements gate at spec values
on any hardware. Disk-bound measurements gate at the fio/G1-derived ceiling of
the reference machine, with the spec value re-validated on NVMe-class hardware
**before M8 sign-off** (phase exit gate 2 — "delta commit 8 ms p50" — is
signed there). Every recorded number carries machine identity, kernel, rustc,
and `vm.dirty_*` settings, appended to `docs/bench-baseline.md`.

## Gate S5 — crash-injection suite green (phase exit gate 3)

```bash
cargo run -p snapstore-crash -- --cycles 25 --matrix-passes 1   # PR profile
cargo run -p snapstore-crash -- --cycles 1000 --matrix-passes 50 # nightly / sign-off
```

**Definition.** 1,000 randomized kill cycles with zero invariant violations;
failpoint matrix (9 boundaries) × 50 kills each; SQLite batch-atomicity loop
×200; full-stack (server-process) scenario green; `snapstorectl fsck --deep`
clean after every recovery. Any violation is a P0.

## Full verification checklist (sign-off)

```
[ ] cargo build --workspace --all-targets                      (macOS and Linux)
[ ] cargo test --workspace                                     (Linux reference box)
[ ] cargo clippy --workspace --all-targets -- -D warnings
[ ] S1: e2e 10k×2 sign-off run green; health + metrics verified
[ ] S2: PROPTEST_CASES=4096 manifest suite green; new golden vector committed
        with explanatory message; fuzz target runs 10 min clean (nightly job)
[ ] S3: M4 BM table recorded in docs/bench-baseline.md with machine identity
[ ] S4: M5 BM table recorded; disk-bound actuals + gate decisions noted;
        NVMe re-validation flagged as M8 entry item
[ ] S5: nightly crash job green (1000 cycles + matrix ×50); PR smoke required
[ ] cross-repo request extend-snapstore-proto-v1 fulfilled (or fallback
        vendored proto in place with swap-back issue filed)
[ ] stale NodeMeta re-export removed from snapstore-types
[ ] docs drift check: API.md §1/§2/§3/§4 vs as-built (file doc issues upstream
        if implementation forced any deviation)
[ ] all beads issues for M4–M6 closed; follow-ups filed (M7 GC, M8 joint
        milestone plan, M9 watermarks/backup)
[ ] git push + bd dolt push clean
```

## What this plan explicitly does NOT require

- **M8** — hypervisor integration, 1000-way fork determinism regression,
  measured fork/restore latencies, sibling-dedup ≥ 94%: separate plan after
  `determinism-hypervisor` M4–M7 (per program Phase 2 doc, "joint close-out").
- **M7 GC** — mark/sweep, compaction, `TriggerGc` semantics, gc metrics
  (RPC returns `UNIMPLEMENTED`; pins/tombstones are stored and honored later).
- **M9** — disk watermarks (`RESOURCE_EXHAUSTED` refusal), scheduled fsck,
  cold backup/restore drill.
- No `ReleaseSnapshot`, no `ResolveArtifact`, no `ListNodes` — deliberately
  absent per API.md §1; do not add them under integration pressure.
