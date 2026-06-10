# M4 — gRPC surface, client lib, snapstorectl

The deliverable that unblocks `determinism-hypervisor` M4. Everything here
depends on 01 (manifest v2, meta v2) and on proto availability (WI1).

## Work item 1 — proto + generated code

**Primary path (review-driven inversion — see 00 risk 2):** vendor
`proto/snapshot_store.proto` at this repo root — the full
`determinism.snapstore.v1` service from API.md §1: all ~20 RPCs, all messages,
`NodeMeta` in its 15-field shape, `NodeStatus` enum, and the error-detail
messages `MissingPages { repeated bytes page_hashes; bytes parent_ref }`,
`MissingNodes { repeated uint64 node_ids }`,
`CurrentGeneration { uint64 generation }` — generated locally with
`tonic-build` in `snapstore-server`/`snapstore-client` `build.rs`, per
ARCHITECTURE.md §1 ("canonical until control-plane exists" — control-plane
exists but has no protoc/prost infrastructure today). Generated types are
re-exported through one module so the later swap to the published crate is a
Cargo.toml change.

**Follow-up (non-blocking):** the `adopt-snapstore-proto-v1` request to
control-plane (00-overview "Cross-repo requests") publishes the same proto
from `determinism-proto`; we swap and delete the vendored copy when it lands.
Until then, pin the control-plane checkout rev in `ci.yaml` (currently
unpinned default-branch HEAD).

Workspace additions either way: `tokio` (rt-multi-thread), `tonic`, `prost`,
`tonic-health`, `tonic-types` (rich status details), `prometheus`, `tracing` +
`tracing-subscriber` (JSON), `axum` or `hyper` for the tiny HTTP stub — pinned
in the workspace `Cargo.toml`.

**AC:** `SnapshotStoreServer`/`SnapshotStoreClient` types compile in both
crates; the stale hand-written `NodeMeta` re-export in `snapstore-types` is
removed.

## Work item 2 — `snapstore-server`: wiring and runtime

`crates/snapstore-server` becomes a real binary (`main()`):

- **Sync↔async bridge design note — written and agreed BEFORE this WI starts**
  (review finding: the workspace is fully synchronous, tonic is async, and
  every benchmark flows through this seam). It must decide: (a) the channel
  type at the handler→actor boundary — a bounded `crossbeam` send from a tonic
  handler blocks a runtime worker; the meta actor's ingress likely becomes a
  tokio mpsc (or crossbeam behind `spawn_blocking`) with oneshot replies;
  (b) where `spawn_blocking` is used vs a dedicated thread (per-message
  `spawn_blocking` won't sustain 600 MB/s PutPages — ingest wants a
  long-lived blocking task per stream or a buffered handoff); (c) how
  PutSnapshot's group-commit fsync wait (01 WI3) is awaited without pinning a
  runtime thread; (d) whether `snapstore-client` is async-only or also offers
  a blocking facade — a **consumer-facing API decision** (KVM vCPU worker
  loops aren't tokio-native; check with the hypervisor plan before choosing
  async-only).
- **Config**: `config.toml` loader matching ARCHITECTURE.md §9 (data_root,
  `grpc_tcp_addr` :7410, `grpc_uds_path`, `page_channel_path` (used in 03),
  `http_addr` :7411, `[pagestore]`, `[meta]` sections). Defaults compiled in;
  file overrides; unknown keys rejected loudly.
- **Startup sequence** (ARCHITECTURE.md §8): `STORE_VERSION` check (write
  `1\n` on first init; refuse on mismatch), `store.uuid`, open meta DB
  (`PRAGMA integrity_check` — refuse on failure), pagestore open (existing
  recovery), clean `tmp/`, remove bad-footer `.spm` files, reconcile node
  `snapshot_ref`s → manifests → indexed pages (missing ⇒ mark node PRUNED, log
  loudly, `snapstore_integrity_errors_total`). Serve `UNAVAILABLE` until
  recovery completes (tonic-health NOT_SERVING → SERVING).
- **Transports**: the same tonic service bound on TCP and UDS (socket mode
  0660). Verify the UDS incoming-stream wiring early — it's the path every BM
  uses.
- **Observability**: JSON tracing to stderr; `/healthz` + `/metrics`
  (Prometheus) on `http_addr`; day-one metrics from ARCHITECTURE.md §7.3
  (`snapstore_pages_ingested_total{dedup}`, `snapstore_commit_seconds`,
  `snapstore_resolve_seconds`, `snapstore_flatten_depth`,
  `snapstore_meta_txn_seconds`, `snapstore_nodes{status}`, integrity counter).

### RPC implementations (API.md §1, exhaustive for v1)

| Group | RPCs | Backing (from 01) |
|---|---|---|
| pages & snapshots | `PutPages` (client-stream, ≤256 pages/msg, server hashes — clients cannot lie), `PutSnapshot`, `GetSnapshot`, `ResolvePages` (server-stream, Modes A/B, `hashes_only`), `HasPages` (≤4096) | `PageStore::ingest` + store façade WI3 |
| input logs | `PutInputLog`, `GetInputLog` | meta WI4 + container codec WI2 |
| tree | `CreateNode`, `UpdateNodes`, `GetNode`, `GetChildren`, `GetPath`, `QueryNodes` (server-stream, ≤512/msg) | meta WI4; **CreateNode additionally validates `snapshot_ref` resolves to a stored manifest at this layer** (meta can't see manifests) ⇒ `NOT_FOUND` — the P0 commit-ordering signal callers rely on (API.md §1.4, INTEGRATION.md §6). Manifests are immutable-once-present, so a pre-check before dispatching to the actor is race-free |
| metadata KV | `PutMetadata`, `GetMetadata`, `DeleteMetadata` | meta WI4 (CAS) |
| lifecycle | `PruneSubtree`, `Pin`, `Unpin`, `Stats`, `TriggerGc` | meta WI4; **`TriggerGc` returns `UNIMPLEMENTED`** until M7 (documented); Stats `gc_*` fields zero |

`Stats` store-section semantics (decided now, not hand-waved): maintained
counters, not on-demand scans. `unique_pages`/`physical_page_bytes` from the
page index + pack accounting; `manifests_total` and `logical_page_bytes`
maintained at `put_snapshot` time — every manifest flattens to full coverage,
so its logical size is exactly `guest_ram_bytes`, making
`logical_page_bytes = Σ guest_ram_bytes` over stored manifests a trivial
running counter; `dedup_ratio = logical / physical`. Counters are
recomputable at startup from index + manifest dir. Post-GC exactness (live vs
stored) is M7's problem and noted as such in the field docs.

Validation at the RPC boundary: 32-byte hash lengths, experiment-id length,
batch caps, page-size multiples — all `INVALID_ARGUMENT` before touching
storage. Disk-watermark `RESOURCE_EXHAUSTED` refusal is M9 scope (no watermark
checks yet); leave the error mapping in place.

## Work item 3 — structured error model

One error-mapping module: storage/meta error enums → gRPC codes per API.md
§1.7, with `tonic-types` rich-status details:

- `PutSnapshot` missing pages / unknown parent ⇒ `FAILED_PRECONDITION` +
  `MissingPages`
- `UpdateNodes` unknown ids ⇒ `NOT_FOUND` + `MissingNodes` (txn rolled back)
- KV CAS mismatch ⇒ `FAILED_PRECONDITION` + `CurrentGeneration` (0 = absent)
- CreateNode key reuse ⇒ `ALREADY_EXISTS`; parent PRUNED / root conflicts ⇒
  `FAILED_PRECONDITION`; malformed containers ⇒ `INVALID_ARGUMENT`

**AC:** a round-trip test per detail type (server raises, client decodes the
typed detail).

## Work item 4 — `snapstore-client`

The Rust lib the hypervisor/orchestrator/replay-renderer link
(INTEGRATION.md preamble). Public surface ≈ one `SnapstoreClient` with typed
methods mirroring the RPCs, plus:

- **Transport selection**: explicit endpoint config + auto mode — UDS gRPC if
  the socket exists and connects, else TCP. (Page-channel slot added in 03;
  the enum and selection logic are written now so 03 only adds an arm.)
- **Footer verification**: `GetSnapshot`/`GetInputLog`/`GetPath(include_logs)`
  results re-verified against their BLAKE3 footers before being returned
  (corrupt ⇒ typed error, never silent).
- **Retry policy** (INTEGRATION.md §6): every content/key-idempotent op
  blind-retries on timeout/`UNAVAILABLE` with exponential backoff capped 30 s —
  `PutPages`, `PutSnapshot`, `PutInputLog`, **`CreateNode`** (idempotent on
  `(experiment_id, node_id)`), reads. CAS ops (`PutMetadata`/`DeleteMetadata`
  with `expected_generation`) **never auto-retry** — `FAILED_PRECONDITION` +
  generation surfaces to the caller. `ALREADY_EXISTS` and `MissingPages`
  surface immediately (caller-bug / caller-action signals).
- Helpers: build-and-put snapshot from `(parent, entries, device_blob)`;
  `resolve_pages` into a caller buffer; typed error enum re-exporting the
  detail messages. Note: the build-snapshot helper makes `snapstore-client`
  depend on `snapstore-manifest`, a deliberate deviation from
  ARCHITECTURE.md §1's dependency rule (client = types + proto + localpath) —
  benign (manifest is pure, no I/O), recorded in 05's docs-drift item so it
  reads as a decision, not drift.

**AC:** retry tests against a flaky in-process server (injected timeouts ⇒
blind retry, no duplicate nodes; CAS never retried); footer-verification
negative test.

## Work item 5 — `snapstorectl` (`crates/snapstore-cli`)

Thin binary over `snapstore-client` (and direct store access where noted).
Phase-2 subcommand set: `stats`, `dump-manifest <ref>` (decoded header +
entries summary), `get-node`, `query` (QueryNodes filters), `prune`, `pin` /
`unpin`, `kv get|put|delete` (with `--expected-generation`), `bench put-pages`
(drives WI6's BM workload). `fsck [--deep]` lands with 04 (offline, direct
store access). `gc` deferred to M7 (prints unimplemented).

**AC:** each subcommand exercised once in a CLI integration test against a
spawned server on a temp dir.

## Work item 6 — end-to-end test + benchmarks (the M4 gate)

**E2E (upstream M4 AC):** synthetic "exploration" of **10k steps across two
concurrent experiments**, through the public API only — per step: build child
delta container from `snapstore-testgen` epoch mutation, `PutPages` (gRPC),
`PutInputLog`, `PutSnapshot`, `CreateNode`, periodic `UpdateNodes` batches,
`QueryNodes` frontier scans with `created_after` cursor, `GetPath` spot
checks, checkpoint `PutMetadata` CAS writes. Final per-experiment `Stats`
consistent with each driver's own bookkeeping; injected timeouts force
CreateNode blind-retries with no duplicate nodes; tonic health serving;
Prometheus counters populated and sane (ingested = new + deduped, etc.).
Runs against UDS in CI (tmp dir store); target < ~5 min so it stays in PR CI
with reduced step count, full 10k in nightly.

**BM (criterion + `snapstorectl bench`):**
- `PutPages` over UDS gRPC, 256-page messages: **transport gate** measured
  dedup-warm (pages already stored ⇒ no disk writes): ≥ 600 MB/s spec target;
  **disk-bound** cold variant recorded against the G1-derived SATA ceiling
  (informational here; NVMe sign-off at M8). See 05.
- `QueryNodes` page of 1,000: p50 < 4 ms over UDS.

## Dependencies and ordering

```
WI1 proto ──┬─► WI2 server ──► WI3 errors ──► WI6 e2e+BM
(01 done) ──┘        └────────► WI4 client ──► WI5 ctl ──► WI6
```

WI2 and WI4 can proceed in parallel once WI1 compiles; WI6 gates the
milestone. **Hypervisor M4 is unblocked at WI1–WI4 + the e2e smoke profile
(reduced step count) + the Gate-S2 format freeze** — the hypervisor needs a
stable, correct surface, not the throughput numbers. The full 10k-step
sign-off run and the BM gates complete in parallel with early hypervisor
integration; WI5 (`snapstorectl`) is not on the critical path at all.
