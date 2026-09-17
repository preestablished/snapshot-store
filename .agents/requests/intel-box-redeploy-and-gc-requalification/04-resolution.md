# Resolution: Intel-Box Redeploy and GC/Transport Requalification

## Summary

`snapstore-server` now runs on the Intel box (`infra-control`) as a release
build from the committed overlay template, over the data root the hypervisor
handoff created. The READY root is pinned and survived an aggressive GC cycle
(gate G1), and that property is covered in CI. The Phase-5 evidence script was
rerun at HEAD on the storage the data root actually uses. The box has no NVMe,
so this is the attested SATA reference host again; the same performance bars
miss as on 2026-07-11 and the operator accepted each as a floor on 2026-09-17.
All correctness bars are met.

## Commits

| SHA | Contents |
|---|---|
| `157d8c5` | Clippy fixes for stable 1.98 (CI was red on `main` before this) |
| `b5718e5` | Overlay template, unit example, deploy + pin runbooks, `pin-ready-root.sh`, `template_example_parses`, `pinned_orphan_snapshot_survives_gc` |
| `3c0e02c` | Runbook corrections from the first run (READY is a DELTA; confirm process exit) |
| this commit | Requalification section, evidence-script path fix, this resolution |

## Deployment (package 01)

Overlay written beside the handoff config (the handoff file is untouched);
`SNAPSTORE_CONFIG_OVERLAY` appended to the handoff env. Server started with
the `nohup setsid` + pid-file form. `/healthz` 200; 88 `snapstore_` metric
lines; `snapstorectl stats` OK over the UDS; `pages.sock` bound; no
`STORE_VERSION` mismatch. Restart drill passed: after stop/start the READY
manifest reopened. The private handoff record
(`snapstore-plan-c-handoff-record.private.txt`, in the handoff evidence
directory) carries the build SHA, `STORE_VERSION`, and stats output.
`gc.auto` remains `false`; enabling it is an operator decision now that G1
has passed.

Deviations:

- A long-lived server started from the handoff-written config (ephemeral
  ports, no page channel) was already running with `dh-workerd` attached. It
  did not exit on SIGTERM within 30 s while the worker held the UDS open, and
  the overlay server was started before that was noticed, so two servers
  shared the data root for about a minute. The stack was idle (zero CPU time,
  all slot icounts 0). The old server was SIGKILLed and the overlay server
  restarted cleanly; meta `integrity_check`, stats, and the READY manifest
  were all intact afterwards. Filed `snapshot-store-qlum` (shutdown hang);
  `snapshot-store-h2yv` (data-root lock) is the structural fix. The runbook
  now requires confirming process exit.

## READY root pin and identity (package 02)

| Field | Value |
|---|---|
| READY ref | `b2e4d219…` (DELTA, `guest_ram_bytes` 134217728 = `M9_LINUX_MEM_BYTES`) |
| Parent ref | `add63f5b…` (FULL) |
| Emulator version | `refwork-emu 0.2.3` (bundle `0.2.0` manifest) |
| Image ref | recorded privately |
| Pin | already present before this plan ran: `newly_pinned=false` on both runs, `pins_total` 1 -> 1 |
| Pin date checked | 2026-09-17 |

Because the ref was already pinned, the stored note is the earlier pin's, not
this plan's `ready-root image=… emu=… date=… by=snapstore-plan-C` convention
(`Pin` on an existing ref does not replace the note). The identity record is
kept privately as `snapstore-plan-c-ready-root-identity.private.txt`.

G1: `snapstorectl gc --aggressive` -> `manifests_deleted=0`,
`snapstore_gc_manifests_deleted_total` 0 -> 0; `dump-manifest` of the READY
ref and of its FULL parent both exit 0 afterwards. **Pass.** The plan expected
a FULL manifest; the handoff actually produces a DELTA over a FULL parent, and
the pin roots the whole chain. `docs/ops/ready-root-pin.md` was corrected.

## Storage-class decision (package 03, D3)

No NVMe device exists on the box (`sda`/`sdc` Samsung SSD 860, `sdb` Samsung
SSD 850; all SATA, non-rotational). Data root and bench root are both on `/`
(`ubuntu--vg--1-ubuntu--lv`, ext4, backed by `sda`). Per D3 the 2026-07-10
posture stands: one counted rerun at the current commit on the attested SATA
host, then explicit accepted floors. No tuning runs were made (D4 applies only
with NVMe or on operator request; the operator chose to accept floors).

Operator attestation (G2): `PHASE5_ACTUAL_SOAK_HOST=true`,
`PHASE5_SAME_AS_I5_SATA_REFERENCE=true`; `hardware_qualification.qualified=true`.

## Per-Bar Results

Evidence root: `target/phase5-readiness-20260917T053354Z/` at commit `3c0e02c`.

| Bar | Target | Measured | Disposition |
|---|---:|---:|---|
| `page_channel_fallback` | 50 green runs | 50 runs, 0 failures | met |
| PUT_BATCH warm sustained | >= 1.5 GB/s | 0.859 GB/s | miss (accepted floor, operator decision 2026-09-17) |
| GET_BATCH warm sustained | >= 2.5 GB/s | 0.571 GB/s | miss (accepted floor, operator decision 2026-09-17) |
| 16 clients x 8 MiB p99 | < 40 ms | 1,185.02 ms | miss (accepted floor, operator decision 2026-09-17) |
| 16 clients aggregate | >= 1.2 GB/s | 0.166 GB/s | miss (accepted floor, operator decision 2026-09-17) |
| CreateNode + inline log p50 | < 1.5 ms | 7.634 ms | miss (accepted floor, operator decision 2026-09-17) |
| UpdateNodes(256) p50 | < 3 ms | 15.666 ms | miss (accepted floor, operator decision 2026-09-17) |
| M7 reclaiming GC | < 60 s under 200 MB/s ingest | 1,486.996 s | miss (accepted floor, operator decision 2026-09-17) |
| M7 nodes reaped | 50,000 | 50,000 | met |
| M7 garbage reclaimed | >= 3,900,000 pages / 15.974 GB predicted | 3,878,811 pages / 16.031 GB | met (harness tolerance) |
| Commit ingest during reclaiming GC | >= 200 MB/s | 111.050 MB/s | miss (accepted floor, operator decision 2026-09-17) |
| Commit p99 during reclaiming GC | < 2 x 401.359 ms idle | 3,527.673 ms | miss (accepted floor, operator decision 2026-09-17) |
| Commit errors (idle / during GC) | 0 | 0 / 0 across 19,690 reclaim samples | met |

Accepted floors (operator: Matt, 2026-09-17, host `infra-control`, SATA `sda`):
each "miss" row above is accepted at its measured value. The misses remain
failures in `evidence.json`.

Two earlier attempts on the same day are void and were deleted: the first
overlapped a CI job on the box's self-hosted runner, the second failed in M5
because the bench-root path exceeded the UDS `SUN_LEN` limit. The counted run
used a short bench root on the same device. The M5/M7 `results.json` files
were relocated into the evidence root after a relative-path bug in the script
(fixed in this commit; details in `docs/bench-baseline.md`).

## Phase 5 Soak Risk

GC on this host reclaims roughly 16 GB in ~25 minutes and, while it runs,
holds commit ingest to ~110 MB/s with multi-second p99 commits. A 4-hour soak
must therefore keep the search inside that ceiling: keep `gc.auto` off or
schedule manual cycles between bursts, and bound tree churn so sustained
commit ingest stays under ~110 MB/s (the orchestrator's budget knob is named
by plan D). Transport throughput roughly doubled since July and idle commit
p99 improved (622 -> 401 ms), but the reclaim cycle is unchanged within noise,
i.e. device-bound. An NVMe rerun remains upside validation only.

## Issue Disposition

July-plan ids (`snapstore-feb`, `snapstore-28z`, `snapstore-8qx`,
`snapstore-agz`, `snapstore-nn4`) do not exist in the `bn` hub and are cited
as history only. The 57 imported Phase-1 issues were left untouched.

| Issue | Disposition |
|---|---|
| `snapshot-store-ee8q` | Closed: deployment live, health + restart drill passed |
| `snapshot-store-vb3a` | Closed: CI test added, READY ref pinned, G1 passed |
| `snapshot-store-fga8` | Closed: counted run recorded, every bar dispositioned |
| `snapshot-store-tvjc` | Open, P2, optional: bounded tuning not run (floors accepted) |
| `snapshot-store-7hec` | Open: cross-plan request for the handoff to call `Pin` itself (the current handoff stack evidently already pins; confirm and close from plan B) |
| `snapshot-store-h2yv` | Open, P1: advisory `flock` on `<data_root>/LOCK` |
| `snapshot-store-qlum` | Open, P1 bug: graceful shutdown hangs while a client holds the UDS |

## Local Verification

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets -- -D warnings` | pass (after `157d8c5`) |
| `cargo build --workspace` / `cargo test --workspace` | pass |
| `cargo test -p snapstore-server --test server -- trigger_gc pinned_orphan` | pass |
| `python3 scripts/phase5_readiness_evidence_test.py` | pass (21 tests) |
| GitHub CI on `3c0e02c` | success (rust, crash-smoke, m8-ref-identity-bounded) |
