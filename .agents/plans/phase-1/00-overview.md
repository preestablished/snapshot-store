# Phase 1 Plan — snapshot-store (overview)

## Goal

Take snapshot-store from Phase 0 scaffolding to Phase 1 exit: a standalone page
store that works against synthetic data, with no hypervisor dependency.

Phase 1 milestones (from the program plan):

1. **M1 — page store core**: 1 GiB append-only packs, sharded index, ingest
   pipeline. Tested against a synthetic-guest generator, no real guest needed.
2. **M2 — manifest codec + snapshot commit/resolve.** Depends on M1.
3. **M3 — metadata DB** (`snapstore-meta`, SQLite schema, lineage queries).
   Parallel with M2 once M1 lands.

## Exit gates (Phase 1, snapshot-store portion)

- **G1 (throughput)**: fast-path ingest ≥ 1.5 GB/s on synthetic data,
  measured by a reproducible Criterion benchmark.
- **G2 (correctness)**: manifest round-trip property tests green —
  encode/decode identity, commit/resolve byte-identity, ref stability.

## Current state (gap analysis, 2026-06-10)

All four existing crates are Phase 0 stubs:

| Crate | State |
|---|---|
| `snapstore-types` | `SnapshotRef([u8; 32])` newtype + proto re-export only |
| `snapstore-manifest` | `Manifest { version, ref_hint }` placeholder, no codec |
| `snapstore-client` | one sample proto-request builder |
| `snapstore-server` | returns baseline benchmark names |

Nothing from M1/M2/M3 exists. No benchmarks, no property tests, no SQLite.

## Target crate layout after Phase 1

```
crates/
  snapstore-types       # shared types: PageHash, PageRef, SnapshotRef, PackId (extend)
  snapstore-pagestore   # NEW (M1): pack files, sharded index, ingest pipeline
  snapstore-testgen     # NEW (M1): synthetic-guest page/workload generator
  snapstore-manifest    # M2: manifest model + deterministic codec
  snapstore-store       # NEW (M2): commit/resolve façade over pagestore+manifest
  snapstore-meta        # NEW (M3): SQLite metadata DB, lineage queries
  snapstore-server      # unchanged this phase (wire-up is a later phase)
  snapstore-client      # unchanged this phase
vendor/
  determinism-proto     # NEW (M1 WI0): temporary stub; retired when the
                        # control-plane request is fulfilled (see risk 1)
```

Rationale for new crates rather than growing `snapstore-server`: the page store
and test generator must be usable from benches and from the hypervisor repo
later without dragging in server/proto dependencies. `snapstore-testgen` is a
normal crate (not a `#[cfg(test)]` module) so benches, integration tests, and
other repos can use the same generator.

## Plan documents

| File | Scope |
|---|---|
| `01-m1-page-store-core.md` | pack format, sharded index, ingest pipeline, synthetic generator, bench harness |
| `02-m2-manifest-codec.md` | manifest model, deterministic codec, commit/resolve, property tests |
| `03-m3-metadata-db.md` | `snapstore-meta` schema, lineage queries, integration |
| `04-exit-gates-and-verification.md` | how each gate is measured, CI wiring, sign-off checklist |

## Ordering and parallelism

```
M1 (pagestore + testgen + bench) ──► M2 (manifest/commit/resolve)
                                 └─► M3 (snapstore-meta)        [parallel with M2]
```

- M1 is the long pole; start it first.
- M2 WI1/WI2 (manifest model + codec) depend only on the `PageHash` type from
  M1 WI0 and can start almost immediately; M2 WI3 (commit/resolve) needs M1
  complete. M3 has no code dependency on M1 at all (`SnapshotRef` already
  exists in Phase 0 `snapstore-types`) — the upstream "after M1" sequencing
  is a staffing choice, not a technical constraint. The gate-bearing
  artifacts still respect the upstream `M1 ──► M2, M3` ordering.
- M2 and M3 are independent of each other; their only touchpoint is the
  optional commit→register integration (M3 WI4), which lands after both.
- Within M1, the synthetic generator (Work item 1) has no dependency on the
  pack store and can be built in parallel with the pack format.

## Risks / open issues

1. **`determinism-proto` does not exist yet, and a dangling path dep poisons
   the whole workspace.** At implementation time this repo is checked out at
   `/home/infra-admin/git/preestablished/snapshot-store`, so the workspace
   path `../control-plane/crates/determinism-proto` resolves to the sibling
   control-plane checkout — but control-plane is currently an empty scaffold
   (README only) and does not yet contain the crate. Verified: Cargo loads
   every workspace member's manifest (including all path deps) before *any*
   build, so even `cargo test -p snapstore-pagestore` and
   `cargo bench -p snapstore-pagestore` fail while the dep dangles — making
   an optional/feature-gated dep no help and both exit gates unrunnable.
   Mitigation (M1 WI0): **vendor a minimal stub crate** at
   `vendor/determinism-proto` inside this repo (the `snapstore` feature, the
   two message types, nothing else) and point the workspace path dep at it.
   On request fulfillment, flip the path back to
   `../control-plane/crates/determinism-proto` — a one-line change. New
   Phase 1 crates additionally take **zero proto dependency** (today only
   `snapstore-client` and `snapstore-types` use it; `snapstore-types` makes
   it optional behind a `proto` feature in WI0) so the stub's surface stays
   frozen at the Phase 0 floor.
2. **Benchmark variance**: 1.5 GB/s is hardware-sensitive. Pin the benchmark
   to the Intel box profile, document the reference machine, and treat CI
   numbers as smoke (regression %) rather than absolute gate. The absolute
   gate is signed off on the reference machine. The bench must also contend
   with kernel dirty-page throttling — methodology is pinned in 01, WI5.
3. **fsync policy vs throughput**: the fast path must define its durability
   contract explicitly (see 01, "Durability"). Deciding this late would force
   a rewrite of the ingest pipeline.
4. **Determinism of encoding** (M2): manifest encoding must be canonical —
   no HashMap iteration order, no serde default-dependent output. The codec
   is hand-rolled for this reason.

## Cross-repo requests

Anything we need from control-plane is requested as markdown files in
`~/.agents/projects/control-plane/requests/<request-name>/`. Phase 1 needs one
request, **filed 2026-06-10**:

- **`publish-determinism-proto`**
  (`~/.agents/projects/control-plane/requests/publish-determinism-proto/`) —
  ship `crates/determinism-proto` with a `snapstore` cargo feature providing
  the `snapstore::v1` module (`PutSnapshotRequest { manifest: Vec<u8> }`,
  `NodeMeta`) that `snapstore-client`/`snapstore-types` already import. With
  the vendored stub in place (risk 1) this is not gate-blocking; fulfillment
  retires the stub via the one-line path flip. See the request's
  `01-crate-spec.md` for the exact contract and `02-acceptance.md` for how
  fulfillment is verified.

## Task tracking

Each work item below maps to one beads issue (`bd create`), with `bd dep add`
edges matching the ordering above. Create the issues when work starts, per
repo convention (issue before code).
