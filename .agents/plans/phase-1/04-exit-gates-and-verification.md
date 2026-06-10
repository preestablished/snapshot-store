# Phase 1 exit gates — measurement and sign-off

snapshot-store owns two of the four Phase 1 exit-gate items:

> 3. snapshot-store M1/M2 benchmark gates met on synthetic data (≥1.5 GB/s
>    fast-path ingest target, manifest round-trip property tests green).

## G1 — fast-path ingest ≥ 1.5 GB/s

**Definition.** Throughput of `PageStore::ingest` on the cold path (all-new
pages: hash + index probe + buffered append, no fsync per batch), measured
over ≥ 4 GiB of synthetic data from `snapstore-testgen`
(`busy_workload` profile, fixed seed), on the reference Intel box.

**How measured.**

```bash
cargo bench -p snapstore-pagestore -- ingest_fastpath_cold
```

Criterion reports `Throughput::Bytes`; the gate number is the reported
mean GB/s. Also recorded (informational, not gated): `ingest_fastpath_warm`
(dedup-dominated) and `ingest_plus_sync`.

**Rules.**
- Sign-off happens on the reference machine only; record machine identity,
  kernel, and rustc version alongside the number in the sign-off note.
- CI runs the bench as a smoke test and flags >10% regressions; CI absolute
  numbers are not the gate.
- Fixed seed, fixed profile, store on a local NVMe path (not tmpfs — tmpfs
  would measure memcpy, not the store; the contract is page-cache writes,
  but the file must be backed by the real target filesystem).

**If we miss 1.5 GB/s**, the knobs in expected order of payoff: bigger write
buffer / fewer write syscalls; rayon hash batch size; shard count / lock
contention; `PageLoc` publication batching. Hashing alone benches multi-GB/s,
so a miss is almost certainly the append or index path — profile before
turning knobs.

## G2 — manifest round-trip property tests green

**Definition.** All `snapstore-manifest` property tests and the
`snapstore-store` commit/resolve integration tests pass.

**How measured.**

```bash
cargo test -p snapstore-manifest            # proptest round-trip / ref-stability / strictness + golden vector
cargo test -p snapstore-store               # commit→resolve byte-identity, multi-epoch, reopen
PROPTEST_CASES=4096 cargo test -p snapstore-manifest   # sign-off run, deeper case count
```

**Rules.**
- The golden-vector test must pass unchanged — if it fails, the format
  changed; either revert or bump `SNAPSHOT_MANIFEST_VERSION` deliberately
  and regenerate the vector in the same commit with an explanatory message.
- Proptest regression files (`proptest-regressions/`) are committed.

## Full verification checklist (phase sign-off)

```
[ ] cargo build --workspace --all-targets        # requires determinism-proto request fulfilled;
                                                 # until then: -p each Phase 1 crate
[ ] cargo test  -p snapstore-types -p snapstore-testgen -p snapstore-pagestore
[ ] cargo test  -p snapstore-manifest -p snapstore-store -p snapstore-meta
[ ] cargo clippy --workspace -- -D warnings
[ ] cargo bench -p snapstore-pagestore           # on reference machine
[ ] G1 number ≥ 1.5 GB/s recorded with machine identity
[ ] PROPTEST_CASES=4096 deep run green
[ ] torn-write recovery + index rebuild tests green (M1 WI2/WI3)
[ ] multi-epoch dedup integration test green (M2 WI3)
[ ] lineage property test green (M3 WI3)
[ ] all beads issues for M1–M3 closed; follow-ups filed
[x] control-plane request `publish-determinism-proto` filed 2026-06-10
    (~/.agents/projects/control-plane/requests/publish-determinism-proto/)
[ ] git push + bd dolt push clean
```

## What Phase 1 explicitly does NOT require from this repo

To keep scope honest:
- No gRPC/proto server surface (`snapstore-server` stays a stub).
- No compression, GC, scrubbing, or pack compaction.
- No integration with the hypervisor — first real-guest contact is
  hypervisor M4, which is gated on the determinism gate, not on us.
- No manifest-level deltas (page dedup covers Phase 1 storage efficiency).
