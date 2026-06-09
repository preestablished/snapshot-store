# Project Planning with Beads

## Agent Instructions

You are an expert software architect creating a comprehensive task breakdown. This task graph will be executed by AI agents working in parallel, coordinated through MCP Agent Mail with file reservations to prevent conflicts.

<quality_expectations>
Create a thorough, production-ready task graph. Include all necessary setup, implementation, testing, and documentation tasks. Go beyond the basics - consider edge cases, error handling, security considerations, and integration points. Each task should be specific enough for an agent to execute independently without ambiguity.
</quality_expectations>

## Project Information

### Links to Relevant Documentation

- `/Users/punk1290/.agents/projects/determinism/docs/snapshot-store/ARCHITECTURE.md` — normative architecture: crate layout (8 crates), core types, on-disk layout (§2 packs/sidecars, §5.3–5.4 SQLite schema and canonical queries)
- `/Users/punk1290/.agents/projects/determinism/docs/snapshot-store/API.md` — gRPC surface, manifest codec spec (§2), page channel protocol (§4)
- `/Users/punk1290/.agents/projects/determinism/docs/snapshot-store/IMPLEMENTATION-PLAN.md` — ordered milestones M0–M9 with acceptance criteria and benchmark gates (this plan covers M0 remainders through M3)
- `/Users/punk1290/.agents/projects/determinism/docs/snapshot-store/INTEGRATION.md` — cross-repo flows and retry/idempotency contracts (§6)
- `/Users/punk1290/.agents/projects/determinism/docs/snapshot-store/README.md` — service overview and runbook seed
- `/Users/punk1290/.agents/projects/determinism/phases/phase-1-deterministic-execution.md` — Phase 1 program plan; snapshot-store is the independent parallel track (M1 → M2, M3)
- `/Users/punk1290/.agents/projects/determinism/docs/MAP.md` — program-wide conventions and principles

### Project Description

Phase 1 (Deterministic Execution) snapshot-store track. The snapshot page store must work standalone against synthetic data — zero hypervisor dependency. Scope, in order:

- **M0 remainders**: the repo currently has 4 of 8 planned crate skeletons (`snapstore-types`, `snapstore-manifest`, `snapstore-server`, `snapstore-client`) and a minimal CI (fmt, build, test only — see Technical Stack for what CI still lacks). Remaining M0 work:
  - Add the missing skeleton crates: `snapstore-pagestore`, `snapstore-meta`, `snapstore-localpath`, and a minimal `snapstore-cli` whose only working subcommand at M0 is `bench fio-baseline` (the full `snapstorectl` subcommand set is an M4 deliverable — this is a deliberate carve-out).
  - Pin the key dependencies in `[workspace.dependencies]` — the current workspace `Cargo.toml` pins only `determinism-proto`; this is an explicit task, not done state.
  - Complete `snapstore-types`, **including the library error enum**: variants with a documented 1:1 mapping to the eventual gRPC codes (`FailedPrecondition` carrying missing-page/missing-node details, `AlreadyExists`, `FailedPrecondition` + current generation for CAS, `ResourceExhausted`, …). M2/M3 acceptance tests assert these library errors; gRPC statuses don't exist until M4.
  - Build the **synthetic guest generator** as a workspace library crate `crates/snapstore-synthgen` (deterministic 128 MiB images seeded by u64 + seeded burst mutation of 256–2,048 pages), consumed as a dev-dependency by all downstream test work. A bare root `tests/` directory is not a Cargo compile target — do not put synthgen there.
  - `/healthz` + `/metrics` HTTP stub, JSON tracing, `config.toml` loader.
  - CI additions: clippy `-D warnings` gate; nightly workflow scaffold (the fuzz job lands with M2).
  - fio NVMe baseline recorded to `docs/bench-baseline.md` — an **Intel-box manual task** that must NOT block M1 (see gating rules in Specific Requirements).
- **M1 — Page store core**: data-root initialization (`STORE_VERSION` = "1\n" with refuse-on-mismatch, `store.uuid`, `tmp/` staging dir); pack format + sidecar (append, seal, fallocate, crc32c), startup rebuild including torn-tail truncation, sharded in-memory index, rayon batch hashing, single pack-writer task with commit barrier, zero-page short-circuit. The pack header's `created_epoch` ("logical counter at creation") is an **injected dependency** (trait or fn parameter) — the real logical counter is an M3 deliverable; M1 uses a stub constant in tests and M3 wires the real source in later.
- **M2 — Manifest codec + snapshot commit/resolve**: `snapstore-manifest` encode/decode/validate/flatten (pure, fuzzable — no I/O beyond `&[u8]`). The commit/resolve orchestration (`manifests/` dir I/O, missing-page detection against the page index, pages→manifest fsync ordering, loose `.spm` write discipline, flatten LRU) lives in **`snapstore-pagestore`**, which owns the data root; it is exposed as a library API that the tonic server wires up in M4 (out of scope). *Depends on M1.*
- **M3 — Metadata DB (`snapstore-meta`)**: schema v1 DDL (experiment-scoped `nodes`/`tombstones` with composite keys, `kv_metadata` CAS table), migrations table, meta actor + read pool, logical counter (including wiring the real counter into M1's injected `created_epoch` seam, replacing the stub), all canonical queries, caller-assigned node-id handling (u64↔i64 bit-cast, idempotent insert), metadata KV with generation CAS, input-log container validation + storage, pins, tombstones, PruneSubtree transaction. *Parallel with M2 after M1.*

Do not start a milestone until its predecessor's acceptance criteria pass in CI. M4+ (gRPC surface, fast path, crash harness, GC, hypervisor integration, backup) are out of scope for this phase plan.

### Technical Stack

- Rust 2021, Cargo workspace (`crates/*`, resolver 2), Apache-2.0
- Crates per ARCHITECTURE.md §1 plus one addition: `snapstore-types`, `snapstore-pagestore`, `snapstore-manifest`, `snapstore-meta`, `snapstore-localpath`, `snapstore-server`, `snapstore-client`, `snapstore-cli`, and `snapstore-synthgen` (test-support library crate); cross-crate integration tests live in a workspace-member test crate, not a bare root `tests/` dir
- Dependency rules: `types ← {pagestore, manifest, meta, localpath} ← server`; `manifest` is pure (no I/O beyond `&[u8]`); `pagestore` and `meta` know nothing about gRPC
- **Canonical proto seam**: `determinism-proto` from the sibling `../control-plane` repo — a workspace path dependency that is **already pinned and consumed** (`snapstore-types` re-exports `determinism_proto::snapstore::v1::NodeMeta`). Local builds and CI require the sibling `control-plane` checkout (CI does a dual checkout). Do NOT create a local `proto/snapshot_store.proto`: ARCHITECTURE.md §1's "canonical until control-plane exists" note is stale — control-plane exists and is canonical. Proto *message types* are therefore available from day 0; the tonic *server/transport* surface enters at M4 (out of scope here beyond skeletons).
- Key dependencies to pin in `[workspace.dependencies]` (an M0 task — currently only `determinism-proto` is pinned): `tokio` (rt-multi-thread), `rusqlite` (bundled SQLite, no serde_json feature), `blake3` (rayon feature for batch hashing), `zstd`, `postcard`, `serde`, `tracing`, `prometheus`, `nix`, `crossbeam-channel`, `parking_lot`
- Testing: `proptest`, `criterion`, `cargo-fuzz` (nightly fuzz job lands with M2)
- CI today: GitHub Actions (`.github/workflows/ci.yaml`) on `ubuntu-latest` — **fmt, build, test only**, with a dual checkout of the sibling `control-plane` repo. M0 adds the clippy `-D warnings` gate and a nightly workflow scaffold; M2 adds the nightly cargo-fuzz job (`Manifest::decode`, 10 min). Benchmarks never run as hosted-CI pass/fail (see Specific Requirements).
- **Linux-only policy**: functional code uses Linux-only APIs (`fallocate` now; memfd/SEQPACKET/io priorities in later milestones). The development host is macOS — correctness is defined by tests passing on Linux (CI runners and the Intel box). cfg-gate or stub non-Linux builds; never weaken a test to make it pass on macOS.
- Target host: single Intel box, local NVMe only storage tier; SQLite via rusqlite for metadata

### Specific Requirements

**M0 acceptance:** CI green; `snapstorectl bench fio-baseline` writes `docs/bench-baseline.md` (seq write/read QD32, 4k randread); synthgen produces bit-identical guests for the same u64 seed.

**M1 acceptance criteria:**
- Ingest 1 M synthetic pages, restart process, index identical (full compare)
- Torn-tail matrix: truncate open pack at every byte offset of the last record (parameterized); startup always recovers to the last whole record, no panic
- Dedup: ingesting the same 100k pages twice stores them once (`pages_new==0` on second pass)
- Benchmarks: single-stream ingest ≥ 1.5 GB/s (pre-hashed memory source); hash+ingest ≥ 1.0 GB/s; index probe ≥ 5 M lookups/s across 8 threads

**M2 acceptance criteria:**
- Round-trip property: ∀ generated manifests, `decode(encode(m)) == m` and ref stable
- Canonicality: shuffled-input entries encode to identical bytes (sort enforced)
- `cargo fuzz` target on `Manifest::decode` runs 10 min in nightly CI, no crashes
- Flatten correctness vs naive reference implementation (proptest, chains ≤ 64)
- Commit-with-missing-pages returns `FAILED_PRECONDITION` listing exactly the gaps
- Benchmarks: flatten 64-deep chain of 2k-entry deltas < 2 ms warm; PutSnapshot (manifest already-paged) p50 < 3 ms

**M3 acceptance criteria:**
- 1 M-node synthetic tree (branching ~8): GetPath(depth 5k) < 40 ms p99; QueryNodes frontier scan streams correctly with `created_after` cursor (no gaps/dupes under concurrent writes — interleaving test)
- CreateNode idempotency: replaying any prefix of a synthetic experiment's CreateNode stream (duplicates included, any interleaving) yields a byte-identical tree; key reuse with different content ⇒ `ALREADY_EXISTS`, zero rows changed
- Multi-experiment isolation: two interleaved synthetic experiments sharing page content never observe each other's nodes via any tree query; per-experiment Stats match per-driver bookkeeping
- KV CAS: concurrent writers on one key ⇒ exactly one winner per generation, losers get `FAILED_PRECONDITION` + current generation; create-only (`expected_generation=0`) and delete-CAS covered; 16 MiB value-cap rejection covered
- UpdateNodes atomicity: one bad id ⇒ zero rows changed
- Kill -9 during a 256-update batch ⇒ on restart the batch is wholly present or wholly absent (loop ×200). **Scope note:** the source plan runs this in the M6 crash-injection harness, which is out of scope here. The in-scope deliverable is a minimal kill-loop test — child process runs the batch workload, parent SIGKILLs at randomized seeded points, restart + invariant check; no failpoints. The M6 harness absorbs this test later.
- Benchmarks: CreateNode+inline log (16 KiB) p50 < 1.5 ms; UpdateNodes(256) p50 < 3 ms; PutMetadata (64 KiB value) p50 < 2 ms; sustained ≥ 5k node-mutations/s through the actor

**Cross-cutting:**
- **Two classes of acceptance criteria — do not conflate them in the task graph:**
  - *CI-correctness ACs*: deterministic tests runnable on hosted `ubuntu-latest` runners (torn-tail matrix, restart index-equivalence, dedup invariants, manifest proptests, flatten-vs-reference, idempotency/isolation/CAS/atomicity tests, the kill-loop). These gate milestones in CI.
  - *Intel-box benchmark gates*: the fio baseline and every throughput/latency number (≥ 1.5 GB/s ingest, ≥ 5 M lookups/s, all p50/p99 targets, ≥ 5k mutations/s). These run on the Intel box's NVMe via criterion + `snapstorectl bench`, are recorded in `docs/bench-baseline.md`, and are verified before milestone close — they are **never** wired as hosted-CI pass/fail steps. Task generation must emit them as separate "run benchmark gate on Intel box, record results" tasks.
- Milestone gating in the task graph: create an **explicit acceptance-gate bead per milestone** (M0-gate, M1-gate, …); every M2 and M3 bead depends on the M1 gate bead, never directly on individual M1 beads. M2 ∥ M3 after M1. **Gate beads depend only on that milestone's CI-correctness AC beads** — for every milestone, the Intel-box benchmark beads are parallel, non-blocking children of the milestone (labeled `bench`), verified before the milestone is *declared closed* by a human but never wired as dependencies of the gate bead or of downstream work. This is the same no-deadlock rule as the M0 fio carve-out, applied uniformly.
- Minimal M0→M1 edge set: M1 depends only on the skeleton crates, workspace dependency pinning, completed `snapstore-types` (incl. the error enum), and `snapstore-synthgen`. The fio baseline, `/healthz`+`/metrics` stub, config loader, and JSON tracing are parallel M0 work that must NOT block M1 — in particular, the fio baseline is a manual Intel-box task and wiring it as a blocker would deadlock autonomous execution.
- Benchmark enforcement is a deliverable, not a judgment call: build a comparison harness that parses criterion/`snapstorectl bench` output, compares against `docs/bench-baseline.md` with ±15% tolerance, and exits nonzero on regression; it runs nightly on the Intel box.
- Phase 1 exit gate (snapshot-store portion): satisfied by M1's single-stream ingest benchmark (≥ 1.5 GB/s, pre-hashed memory source) plus manifest round-trip property tests green. The phase doc's "fast-path ingest" wording refers to the M5 page channel, which is out of scope here — do not create an exit-gate task that requires M5 functionality.
- All integration testing uses `snapstore-synthgen` — no hypervisor dependency anywhere in this phase
- Explicit versions everywhere (STORE_VERSION, schema_version); readers reject unknown versions loudly

---

## Your Task

Analyze this project and create a comprehensive **Beads task graph** using the `bd` CLI. Beads provides dependency-aware, conflict-free task management for multi-agent execution.

---

<critical_constraint>
Your ONLY output is a bash shell script. Do NOT use `bd add` — the correct command to create a bead is `bd create`. Use `bd dep add` for dependencies. Do not implement anything yourself.
</critical_constraint>

## Output Format

Generate a shell script that creates the full task graph. The script should:

1. **Initialize Beads** (if not already initialized)
2. **Create all beads** with appropriate priorities
3. **Establish dependencies** between beads
4. **Add labels** for phase grouping

### Example Output

```bash
#!/bin/bash
# Project: snapshot-store
# Generated: 2026-06-09

set -e

# Initialize beads if needed
if [ ! -d ".beads" ]; then
    bd init
fi

echo "Creating project beads..."

# ========================================
# M0: Skeletons, types, synthgen, CI
# ========================================

M0_CRATES=$(bd create "Add skeleton crates: pagestore, meta, localpath, cli" -p 0 --label m0 --silent)

M0_DEPS=$(bd create "Pin key dependencies in [workspace.dependencies]" -p 0 --label m0 --silent)

M0_TYPES=$(bd create "Complete snapstore-types incl. library error enum" -p 0 --label m0 --silent)
bd dep add $M0_TYPES $M0_CRATES

M0_SYNTHGEN=$(bd create "Build snapstore-synthgen workspace crate" -p 0 --label m0 --silent)
bd dep add $M0_SYNTHGEN $M0_CRATES

M0_CLIPPY=$(bd create "Add clippy -D warnings gate to CI" -p 1 --label ci --silent)

M0_GATE=$(bd create "M0 acceptance gate: CI-correctness ACs green" -p 0 --label m0 --silent)
bd dep add $M0_GATE $M0_TYPES
bd dep add $M0_GATE $M0_SYNTHGEN
bd dep add $M0_GATE $M0_DEPS

# ========================================
# M1: Page store core
# ========================================

M1_DATAROOT=$(bd create "Data-root init: STORE_VERSION, store.uuid, tmp/" -p 0 --label m1 --silent)
bd dep add $M1_DATAROOT $M0_GATE

M1_PACKS=$(bd create "Pack format + sidecar: append, seal, fallocate, crc32c" -p 0 --label m1 --silent)
bd dep add $M1_PACKS $M1_DATAROOT

# ... continue for all milestones, ending each with its gate bead ...

echo ""
echo "Bead graph created! View with:"
echo "  bd ready              # List unblocked tasks"
```

---

## Bead Creation Guidelines

### Priority Levels
- `-p 0` = Critical (blocking other work)
- `-p 1` = High (important but not blocking)
- `-p 2` = Medium (standard work)
- `-p 3` = Low (nice to have)

### Labels (Phase Grouping)
Use `--label` to group beads by milestone:
- `m0` - Skeletons, types, synthgen, baselines
- `m1` - Page store core
- `m2` - Manifest codec + snapshot commit/resolve
- `m3` - Metadata DB
- `testing` - Cross-crate test infrastructure
- `bench` - Intel-box benchmark gates and the comparison harness
- `ci` - CI workflow changes (clippy gate, nightly scaffold, fuzz job)
- `docs` - Documentation (bench-baseline.md, runbook seeds)

### Dependency Rules
1. Never create cycles
2. Every bead should have a clear dependency chain back to setup tasks
3. Use `bd dep add CHILD PARENT` (child depends on parent completing first)
4. Parallel work should share a common ancestor, not depend on each other

### Task Granularity
- Each bead should be completable in **under 750 lines of code**
- Tasks should be atomic enough for one agent to complete without coordination
- If a task requires multiple file areas, consider splitting by file area

---

## File Reservation Planning

For each major work area, note the file patterns that will need exclusive reservation:

```bash
# Example reservation notes (add as bead descriptions)
# Pack/index work:      crates/snapstore-pagestore/**
# Manifest codec:       crates/snapstore-manifest/**
# Metadata DB:          crates/snapstore-meta/**
# Shared types/errors:  crates/snapstore-types/**
# Synthetic generator:  crates/snapstore-synthgen/**
# CLI (bench):          crates/snapstore-cli/**
# CI workflows:         .github/workflows/**
# Workspace manifest:   Cargo.toml, Cargo.lock
```

This helps agents claim appropriate file surfaces when they start work.

---

## Context Documentation

Agents must read the normative source docs before implementing — they are listed under "Links to Relevant Documentation" above (`~/.agents/projects/determinism/docs/snapshot-store/`: ARCHITECTURE.md is normative for formats and schema; API.md for the manifest codec and error semantics; IMPLEMENTATION-PLAN.md for milestone ACs). Reference these paths directly in bead descriptions where relevant.

---

## Verification Steps

After generating the script:

1. **Run it**: `chmod +x setup-beads.sh && ./setup-beads.sh`
2. **Check ready work**: `bd ready` should show initial setup tasks

---

## Completeness Checklist

Ensure your task graph includes:

- [ ] All setup and configuration tasks
- [ ] Core architecture and shared utilities
- [ ] Feature implementation tasks (broken into small units)
- [ ] Error handling and edge cases
- [ ] Unit and integration tests for each feature
- [ ] API documentation
- [ ] Security considerations (input validation, auth checks)
- [ ] Performance considerations where relevant
- [ ] CI/CD and deployment tasks
- [ ] Clear dependency chains with no cycles
