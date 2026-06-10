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
| Fast path | none | `snapstore-localpath` SEQPACKET + memfd channel (Linux-only) |
| Async runtime | none (workspace has no tokio/tonic/prost) | tokio rt-multi-thread, tonic, prost, tonic-health, tonic-types, prometheus, nix |

What carries forward unchanged: `snapstore-pagestore` (packs, sharded index,
ingest, recovery — G1 met at ~461 MiB/s on the SATA reference box),
`snapstore-testgen`, the pagestore torn-tail/rotation test suite.

## Target crate layout after this plan

```
crates/
  snapstore-types       # + LogId, NodeId, ExperimentId, NodeStatus
  snapstore-pagestore   # + failpoints feature; durable-barrier hook for PutSnapshot
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
[01] WI1 manifest v2 ──┐
[01] WI2 meta v2 ──────┼──► [02] gRPC server + client + ctl + e2e ──► [03] M5 page channel
proto request ─────────┘                                  │
(control-plane)                                           │
[01] WI3 failpoint hooks ──► [04] M6 harness (library mode) ──► [04] full-stack mode
                                                          (parallel with [03])
```

- **[01] is the long pole and starts first.** Manifest v2 and meta v2 are
  independent of each other and parallelizable.
- The cross-repo proto request (below) is filed on day 1; nothing in [01]
  depends on it.
- [02] needs all of [01] plus the proto. [03] needs [02] (client
  auto-selection, server wiring). [04] library mode needs only [01] and runs
  parallel with [02]/[03]; its full-stack mode needs [02].
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
2. **`determinism-proto` cross-repo dependency.** The full service + tonic
   codegen lives in control-plane's crate. Mitigation: file the request
   (below) immediately; fallback if not fulfilled before [01] completes:
   vendor `proto/snapshot_store.proto` in this repo with local `tonic-build`,
   per ARCHITECTURE.md §1's "canonical until control-plane exists" escape
   hatch, and swap to the published crate when it lands.
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

## Cross-repo requests

One request to control-plane, filed when this plan lands, as
`~/.agents/projects/control-plane/requests/extend-snapstore-proto-v1/`:

- **`extend-snapstore-proto-v1`** — replace the placeholder
  `proto/determinism/snapshot_store.proto` content with the full
  `determinism.snapstore.v1` service from API.md §1 (all RPCs + messages +
  `NodeMeta` in its 15-field shape + error-detail messages `MissingPages`,
  `MissingNodes`, `CurrentGeneration`), and publish prost/tonic-generated code
  (server + client) behind the existing `snapstore` feature (or a new
  `snapstore-grpc` feature so the types-only consumers don't inherit tonic).
  Acceptance: `snapstore-server` builds a `SnapshotStoreServer` and
  `snapstore-client` a `SnapshotStoreClient` from the crate; the old
  hand-written `NodeMeta`/`PutSnapshotRequest` are replaced by generated ones.

## Task tracking

Each work item in 01–04 maps to one beads issue (`bd create`, `-l impl` etc.),
with `bd dep add` edges matching the ordering above. Create the issues when
work starts, per repo convention (issue before code). The e2e/bench gate items
map to `-l testing` issues referenced by 05's checklist.
