# Phase 2 Plan — snapshot-store (overview)

## Goal

Finish the **service surface** of snapshot-store so it can unblock
`determinism-hypervisor` M4. This plan covers exactly the first snapshot-store
block of the program's Phase 2 doc
(`~/.agents/projects/determinism/phases/phase-2-fork-and-replay.md`):

1. **M4 — gRPC surface + client lib.**
2. **M5 — fast path** (UDS SEQPACKET page channel with memfd fd-passing for
   co-located hypervisor workers). *Depends on M4.*
3. **M6 — durability: crash-injection harness.** *Parallel with the hypervisor
   work; in-repo it runs parallel with M5.*

**Explicitly out of scope for this plan:** M8 (hypervisor integration +
determinism regression, the joint Platform Milestone 1 test). That gets its own
plan after `determinism-hypervisor` completes its M4–M7 chain. Also out of
scope: M7 (GC) and M9 (backup) — later phases. Pin/Unpin/PruneSubtree *RPCs*
are in scope (they are metadata operations the orchestrator needs); the GC that
consumes pins/tombstones is not.

## Current state (gap analysis, 2026-06-10)

Phase 1 (M0–M3) is signed off, but two of its deliverables were deliberately
**simplified relative to the normative design docs** that M4's RPC surface is
specified against (`~/.agents/projects/determinism/docs/snapshot-store/`:
API.md, ARCHITECTURE.md, INTEGRATION.md). M4 cannot be "wire up gRPC" — it
must first converge the on-disk formats and DB schema to spec. Honest
inventory:

| Area | Phase 1 as-built | What M4 needs (normative) |
|---|---|---|
| Manifest (`snapstore-manifest`) | Own struct (`version, parent, icount, virtual_ns, memory regions, devices`) + own codec; **no delta manifests, no flatten** (explicitly deferred in phase-1 plan) | `.spm` container per API.md §2: 96-byte header (`SPSMAN01`), DELTA flag, `guest_ram_bytes`, 40-byte `(page_index, page_hash)` entries, device blob (optional zstd), self-verifying BLAKE3 footer (`snapshot_ref == footer`); `flatten()` for ResolvePages |
| Metadata DB (`snapstore-meta`) | `snapshots` table keyed by `SnapshotRef` (parent, label, ancestors/descendants); synchronous API, no actor | ARCHITECTURE.md §5.3 schema v1: experiment-scoped `nodes` (composite PK, caller-assigned `node_id`), `input_logs`, `pins`, `tombstones`, `kv_metadata` (CAS), `meta` (logical counter); writer **actor** with 256-command txn batching + 4 read connections (§5.2) |
| Types (`snapstore-types`) | `PageHash`, `PackId`, `PageLoc`, `SnapshotRef` | add `LogId`, `NodeId`, `ExperimentId`, `NodeStatus` |
| Proto (`determinism-proto`, sibling control-plane repo) | Hand-written `NodeMeta` (5 fields, stale shape) + `PutSnapshotRequest`; **empty** `service SnapshotStore {}`; no prost/tonic codegen | Full `determinism.snapstore.v1` service (API.md §1, ~20 RPCs) with tonic server/client codegen + error-detail messages |
| Server / client crates | Phase 0 stubs (≈13 / ≈7 LOC) | tonic services on TCP `:7410` + UDS, config loader, `/healthz` `/metrics`, tracing; client lib with transport fallback, footer verification, retry policy |
| CLI | none | `snapstorectl` (subset; see 02) |
| Crash testing | in-process torn-tail/rotation tests in `snapstore-pagestore` | child-process kill -9 harness with failpoints at every fsync/rename boundary, deep fsck, invariant checks |
| Pack/sidecar on-disk format | `SPK1` 20-byte header, 37-byte record headers (hash+flags+len, **no per-record crc32c**), `SPKF` footer with body BLAKE3, `.idx` CRC32 sidecars (`pack.rs`, `index.rs`) | ARCHITECTURE.md §2.1–2.2 specifies `SPPACK01` 64-byte header, `CREC` records with crc32c, `.sppx` BLAKE3-footer sidecars — **as-built format kept** (repo-internal, no cross-repo consumer reads packs; phase-1 durability tests green); deviation documented, upstream doc amendment filed (risk 6) |
| Pagestore read path | `PageStore::get` opens the pack file **per page read**, and `PackReader::open` footer-validates by scanning the whole pack body (`ingest.rs:295`, `pack.rs:309`) | ResolvePages / GET_BATCH read gates (≥ 2.5 GB/s warm) need cached pack readers + batched pread — ARCHITECTURE.md §6's File-handle LRU (cap 256). New work item 01 WI6 |
| Fast path | none | `snapstore-localpath` SEQPACKET + memfd channel (Linux-only) |
| Async runtime | none (workspace has no tokio/tonic/prost) | tokio rt-multi-thread, tonic, prost, tonic-health, tonic-types, prometheus, nix |

What carries forward: `snapstore-pagestore`'s write path (packs, sharded
index, ingest, recovery — G1 met at ~461 MiB/s on the SATA reference box),
`snapstore-testgen`, and the torn-tail/rotation test suite. The pagestore
**read path** does not carry forward unchanged — see the table row above and
01 WI6.

## Target crate layout after this plan

```
crates/
  snapstore-types       # + LogId, NodeId, ExperimentId, NodeStatus
  snapstore-pagestore   # + failpoints; read-path cache (WI6); commit barrier w/ group fsync
  snapstore-testgen     # + scripted multi-experiment exploration driver helpers
  snapstore-manifest    # REWRITTEN to .spm container spec (delta + flatten)   [01]
  snapstore-store       # commit/resolve over .spm; PutSnapshot validation     [01]
  snapstore-meta        # REWRITTEN: schema v1 per ARCHITECTURE.md §5.3, actor [01]
  snapstore-server      # tonic services, config, health/metrics, main()      [02]
  snapstore-client      # transport fallback, verify, retries; page channel   [02,03]
  snapstore-cli         # NEW: snapstorectl                                    [02]
  snapstore-localpath   # NEW: SEQPACKET+memfd page channel (Linux-only)      [03]
  snapstore-crash       # NEW: crash-injection harness + deep-fsck library    [04]
```

Deviation from ARCHITECTURE.md §1 (which shows a repo-root `tests/` dir for the
crash harness): we use a workspace crate `snapstore-crash` instead — a virtual
workspace root can't own integration tests, and the harness needs its own deps
(`fail`, `nix`) and a runner binary.

## Plan documents

| File | Scope |
|---|---|
| `01-m4-foundations.md` | spec alignment: types, `.spm` manifest rewrite (delta + flatten), meta DB v1 schema + writer actor + KV/logs/pins/tombstones/prune |
| `02-m4-grpc-and-client.md` | proto + codegen (cross-repo request), tonic server, structured errors, `snapstore-client`, `snapstorectl`, end-to-end synthetic exploration test, M4 benchmarks |
| `03-m5-page-channel.md` | `snapstore-localpath`: protocol, server half, client auto-selection, fd-hygiene tests, M5 benchmarks |
| `04-m6-crash-harness.md` | failpoint instrumentation, deep fsck, kill -9 harness, invariant checks, CI wiring |
| `05-exit-gates-and-verification.md` | how each gate is measured, hardware caveats, sign-off checklist |

## Ordering and parallelism

```
[01] WI1 manifest v2 ───┐
[01] WI4 meta v2 ───────┼──► [02] gRPC server + client + ctl + e2e ──► [03] M5 page channel
[01] WI6 read path ─────┤                                 │    (WI1 codec can start during [02])
vendored proto (02 WI1) ┘                                 │
[01] WI5 failpoint hooks ──► [04] M6 harness (library mode) ──► [04] full-stack mode
                                                          (parallel with [03])
```

- **[01] is the long pole and starts first.** Manifest v2 (WI1+WI3) and meta
  v2 (WI2+WI4) are independent of each other and parallelizable; the read-path
  work (WI6) is a third independent stream.
- The proto is vendored in-repo (02 WI1) so nothing blocks on control-plane;
  the cross-repo request (below) is a publish/adopt follow-up, off the
  critical path.
- [02] needs all of [01]. [03] needs [02] (client auto-selection, server
  wiring) — except 03 WI1 (the pure codec), which can run parallel with [02].
  [04] library mode needs only [01] and runs parallel with [02]/[03]; its
  full-stack mode needs [02].
- M6 is the in-repo parallel track exactly as the phase doc orders it: it must
  be green for the phase exit gate but does not block hypervisor M4 — [02]
  does. **Ship [02] before polishing anything.**

## Risks / open issues

1. **Spec-divergence rework breaks Phase 1 artifacts deliberately.** The
   manifest rewrite invalidates the phase-1 golden vector and the meta rewrite
   drops the `snapshots` table. Both are safe *now* (zero external consumers;
   the hypervisor hasn't started M4) and unsafe later — this is the last cheap
   moment. Discipline: golden vector regenerated in the same commit as the
   format change with an explanatory message; `snapstore-store`'s
   `commit(meta_db)` auto-register coupling is removed (workers never write
   node rows — INTEGRATION.md §2.1).
2. **Proto ownership.** Decision (review-driven inversion of the original
   plan): the **vendored proto is primary** — `proto/snapshot_store.proto` at
   this repo root with local `tonic-build`, per ARCHITECTURE.md §1's
   "canonical until control-plane exists" rule. Rationale: `determinism-proto`
   today is hand-written structs with **no protoc/prost/tonic infrastructure**
   — landing codegen there is real work on another team's critical path, and
   it was the only cross-repo item gating hypervisor M4. The control-plane
   request (below) becomes a publish/adopt follow-up; type paths are kept
   swappable via module re-export. Until the swap, **pin the control-plane
   checkout to a rev in `ci.yaml`** (it currently tracks default-branch HEAD —
   an unpinned cross-repo CI coupling that bites the moment `NodeMeta` is
   replaced upstream).
3. **Linux-only surfaces vs darwin dev machine.** `SOCK_SEQPACKET`, `memfd_create`,
   `/proc/self/fd` audits, and meaningful fsync/kill semantics are Linux-only.
   `snapstore-localpath` and the harness's fd/kill assertions are
   `cfg(target_os = "linux")`; the workspace must stay green on macOS (stubs +
   portable unit tests). All M5/M6 acceptance runs happen on the Intel
   reference box.
4. **Benchmark gates assume NVMe; the reference box is SATA.** Phase 1 set the
   precedent (G1 lowered 1.5 GB/s → 400 MiB/s). M4/M5 BM targets (PutPages
   ≥ 600 MB/s, PUT_BATCH ≥ 1.5 GB/s sustained, delta commit 8 ms p50) are
   NVMe-class numbers. Approach: measure **transport-bound** variants
   (dedup-warm workloads that don't hit the disk write path) against the spec
   numbers, gate **disk-bound** variants against the fio/G1-derived SATA
   ceiling, and record both. The NVMe-class absolute gates are re-validated on
   real hardware at M8 time (phase exit gate 2 is signed there, not here).
   Details in 05.
5. **tonic-over-UDS + rich error details** need `tonic` ≥ 0.12-era APIs and
   `tonic-types` for `google.rpc.Status` details; pin versions in the workspace
   `Cargo.toml` and verify UDS connector + details round-trip early in [02].
6. **Pack on-disk format diverges from ARCHITECTURE.md §2.1–2.2** (see gap
   table). Kept as-built: the format is repo-internal (nothing outside this
   repo reads packs), phase-1 torn-tail durability tests are green against it,
   and per-record BLAKE3 verification on tail-scan is stronger than the spec's
   crc32c. Action: file an upstream doc issue
   (`~/.agents/projects/determinism/reviews/`-style) to amend ARCHITECTURE §2
   to as-built, or schedule convergence as an explicit later milestone — do
   NOT let the deep fsck or GC work silently assume the spec layout (04 WI2 is
   written against as-built).
7. **fsync storm under concurrent commits.** `PageStore::sync()` fdatasyncs
   every dirty pack under the active-pack lock; 16 workers committing
   concurrently (S4: p99 < 40 ms) through a naive per-PutSnapshot sync would
   serialize on it — phase 1 removed inline fsyncs from rotation for a 2.25×
   gain (commit 0d8ef62) for exactly this reason. Mitigation: group-commit
   barrier specified in 01 WI3 — the most likely non-hardware cause of an S4
   miss.
8. **sync↔async bridge is the seam every benchmark flows through.** The
   workspace is fully synchronous; tonic is async. Channel types at the
   boundary, `spawn_blocking` strategy, and whether `snapstore-client` offers
   a blocking facade (KVM vCPU loops aren't tokio-native) are decided in a
   short design note **before** 02 WI2 starts (02 WI2 lists the questions).
9. **CI infrastructure debt.** Current CI is fmt+build+test only: no clippy
   (despite phase-1 sign-off claiming it), no fuzz dir, no nightly jobs, and
   the nightly crash/bench commitments imply a Linux runner decision
   (GitHub-hosted vs self-hosted reference box). Owned explicitly by 04 WI5;
   clippy lands in PR CI immediately.

## Cross-repo requests

One request to control-plane — a **non-blocking follow-up** (the vendored
proto is primary; see risk 2), filed as
`~/.agents/projects/control-plane/requests/adopt-snapstore-proto-v1/`:

- **`adopt-snapstore-proto-v1`** — adopt this repo's
  `proto/snapshot_store.proto` (the full `determinism.snapstore.v1` service
  from API.md §1: all RPCs + messages + `NodeMeta` in its 15-field shape +
  error-detail messages `MissingPages`, `MissingNodes`, `CurrentGeneration`)
  as the canonical copy, replacing the placeholder at
  `proto/determinism/snapstore/v1/snapshot_store.proto`, and publish
  prost/tonic-generated code (server + client) behind a `snapstore-grpc`
  feature (so types-only consumers don't inherit tonic). Note this requires
  control-plane to stand up protoc/prost build infrastructure it does not
  have today. Acceptance: this repo swaps its `tonic-build` output for the
  published crate with no type-path changes (module re-export contract) and
  deletes the vendored copy; the old hand-written
  `NodeMeta`/`PutSnapshotRequest` are replaced by generated ones.

## Task tracking

Each work item in 01–04 maps to one beads issue (`bd create`, `-l impl` etc.),
with `bd dep add` edges matching the ordering above. Create the issues when
work starts, per repo convention (issue before code). The e2e/bench gate items
map to `-l testing` issues referenced by 05's checklist.
