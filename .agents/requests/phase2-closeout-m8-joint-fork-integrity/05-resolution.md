# M8 Joint Fork-Integrity Resolution

Resolved 2026-07-12 on the operator-qualified Intel/SATA reference host.

## Full Acceptance

The counted evidence is in
`../determinism-hypervisor/target/m8-joint-fork-integrity-20260712T004334Z/`.
`scripts/m8_joint_fork_integrity_evidence.py` accepts it as valid.

| Item | Result |
|---|---|
| Snapshot-store | clean `08aedbfedbd45a13628f73e4eab669c6a0e21627` |
| Determinism-hypervisor | clean `776a80f4ee1550081612b0b593ea4218a108856d` |
| Guest SDK | clean `0fcddf455db6a386aa52d12560b1db74fc6cf4b1` |
| Store root | qualified SATA `/home/infra-admin/snapshot-store-bench-m8/m8-store-20260712T004334Z` |
| Positive result | 1,000/1,000 original/replay snapshot refs identical |
| Seed/state shape | 1,000 distinct child refs; one deterministic final state hash |
| Sharing | 94.166% aggregate shared pages |
| Restore wiring | baseline-delta used in all 1,000 child rows |
| FULL cadence | rollover smoke passed |
| Semantic negative | first pad event changed before sealing; committed ref mismatch observed |

Latency telemetry is recorded in `docs/bench-baseline.md`: fork-to-commit p50
212.921 ms and p99 482.244 ms; baseline-delta restore p50 1,226.299 ms and p99
2,205.566 ms; replay restore-to-commit p50 1,412.491 ms and p99 2,299.727 ms.
All eleven validator bars are green. The per-child CSV has 1,001 lines
including its header and the JSONL has exactly 1,000 contiguous rows.

The run used initramfs BLAKE3
`36f50484f9fc1a8cfe6dd024dccac0a0ce4ab7f504b1e2cea357a00f97390b7d`
and game-image BLAKE3
`96cdaa2380b593e1f3377fc5bf23a16a74e0e277a08ce988ea532b5a91c8c194`.
The evidence records the unrelated control-plane fixture checkout as dirty;
the three implementation/guest repositories named above were clean.
The strengthened evidence contract additionally validates UTC RFC3339 times,
complete host/KVM/mount identity, all guest image hashes, machine-config hash,
exact command/environment, structured artifacts, and deviations. This accepted
artifact has no run deviation.

## Defect Found And Fixed

The first real-emulator trial proved execution and input logs deterministic but
produced different incremental snapshot refs because KVM dirty tracking exposed
different unchanged-page supersets after fork and restore. Hypervisor commit
`5b9dd2d` canonicalizes a DELTA by comparing candidate live-page hashes with the
flattened parent and omitting unchanged pages. Its regression snapshots
identical memory from deliberately different dirty supersets and requires an
identical ref. M8 also uses final-hash verification rather than inheriting M7
epoch targets that may fall inside an unlandable PMU signal-delivery window.
The focused real-emulator preflight and full 1,000-child run both passed.

Tracked hypervisor defect: `determinism-hypervisor-apd8`.

## CI Permanence

Snapshot-store `main` requires `m8-ref-identity-bounded`,
`rust (ubuntu-latest)`, and `crash-smoke`. Exact-SHA snapshot-store run
<https://github.com/preestablished/snapshot-store/actions/runs/29174163626>
is green for `08aedbf`. Exact hypervisor commit `776a80f` is green across its
hosted x86, hosted ARM, and KVM lanes in
<https://github.com/preestablished/determinism-hypervisor/actions/runs/29174139210>.

The permanent required checks are bounded (eight children per merge, 100 in
the hypervisor nightly); the full 1,000-child acceptance remains an operator
run. Matt Spurlin approved this bounded-required/full-operator split on
2026-07-12 as satisfying M8 permanence, with any ref-identity or replay
divergence remaining P0. The approval is recorded in
`06-phases-track-verification.md`.

## Tracker Disposition

- `snapshot-store-m0u`: closed after qualified predecessor evidence
  `target/phase5-readiness-20260711T183613Z`.
- `snapshot-store-4ua`: ready to close on this validated full evidence.
- `snapshot-store-2dl`: closed after phases-track sign-off.
- `snapshot-store-orm`: closed after all child requirements completed.
- `determinism-hypervisor-apd8`: ready to close after exact-SHA CI completes.
