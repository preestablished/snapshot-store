#!/usr/bin/env bash
# Project: snapshot-store — Phase 1 (Deterministic Execution) task graph, M0 remainders through M3
# Generated: 2026-06-09
#
# Source docs (normative, referenced in bead descriptions):
#   ~/.agents/projects/determinism/docs/snapshot-store/ARCHITECTURE.md   (formats, schema, concurrency)
#   ~/.agents/projects/determinism/docs/snapshot-store/API.md            (manifest codec §2, log container §3, error semantics)
#   ~/.agents/projects/determinism/docs/snapshot-store/IMPLEMENTATION-PLAN.md (milestone ACs M0–M3)
#
# Gating rules encoded below:
#   - One acceptance-gate bead per milestone; gates depend ONLY on that milestone's
#     CI-correctness AC beads. Intel-box benchmark beads (label: bench) are parallel,
#     non-blocking children — never dependencies of gates or downstream work.
#   - Every M1 bead depends on M0-gate; every M2/M3 root bead depends on M1-gate
#     (never on individual M1 beads). M2 and M3 run in parallel after M1.
#   - Minimal M0→M1 edge set: M0-gate covers only skeleton crates, dependency pinning,
#     snapstore-types (incl. error enum), synthgen, and the clippy CI gate. The fio
#     baseline, healthz/metrics stub, config loader, JSON tracing, and nightly scaffold
#     are parallel M0 work that must NOT block M1.

set -euo pipefail

if [ ! -d ".beads" ]; then
    bd init
fi

echo "Creating snapshot-store Phase 1 (M0-M3) beads..."

# ============================================================
# M0 — Skeletons, types, synthgen, CI, baselines
# ============================================================

M0_CRATES=$(bd create "Add skeleton crates: pagestore, meta, localpath, cli, tests" \
  -d 'Create crates/snapstore-pagestore, crates/snapstore-meta, crates/snapstore-localpath, crates/snapstore-cli, and crates/snapstore-tests (workspace-member integration-test crate; a bare root tests/ dir is NOT a Cargo compile target). Each crate: Cargo.toml inheriting workspace edition/version/license, stub lib.rs (main.rs for cli) that compiles clean under clippy -D warnings. snapstore-cli is minimal: its only working M0 subcommand will be bench fio-baseline (separate bead); the full snapstorectl subcommand set is an M4 deliverable (deliberate carve-out). Respect dependency rules: types <- {pagestore, manifest, meta, localpath} <- server; pagestore and meta know nothing about gRPC. Do NOT create a local proto file - determinism-proto from ../control-plane is the canonical seam. Ref: ARCHITECTURE.md section 1. Reserves: crates/**, Cargo.toml.' \
  -p 0 -l m0 --silent)

M0_PIN=$(bd create "Pin key dependencies in [workspace.dependencies]" \
  -d 'Workspace Cargo.toml currently pins only determinism-proto - this is an explicit task, not done state. Add pinned versions for: tokio (rt-multi-thread), rusqlite (bundled SQLite, serde_json feature OFF), blake3 (rayon feature for batch hashing), zstd, postcard, serde, tracing + tracing-subscriber (json), prometheus, nix, crossbeam-channel, parking_lot, hashbrown, crc32c, toml; dev-deps: proptest, criterion. tonic/prost transport enters at M4. Ref: ARCHITECTURE.md section 1 key-dependency list, plan Technical Stack. Reserves: Cargo.toml, Cargo.lock.' \
  -p 0 -l m0 --silent)

M0_TYPES=$(bd create "Complete snapstore-types incl. library error enum" \
  -d 'Per ARCHITECTURE.md section 1 core types: PageHash([u8;32] BLAKE3-256 of exactly 4096 bytes), SnapshotRef, LogId, NodeId (caller-assigned u64, root=0), ExperimentId (1..=128 UTF-8 bytes, validated), PAGE_SIZE=4096, NodeStatus (Frontier/Expanded/Pruned/Goal), PackLoc{pack_id,offset}. Keep the existing re-export of determinism_proto snapstore v1 NodeMeta. LIBRARY ERROR ENUM with documented 1:1 mapping to eventual gRPC codes: InvalidArgument, NotFound, AlreadyExists (CreateNode key reuse), FailedPrecondition carrying structured details (MissingPages{page_hashes,parent_ref}, MissingNodes{node_ids}, CurrentGeneration{generation}, pruned-parent/root conflicts, CAS mismatch), ResourceExhausted, Unavailable. M2/M3 acceptance tests assert these library errors; gRPC statuses do not exist until M4. Unit tests for invariants and detail payloads. Ref: API.md section 1.7. Reserves: crates/snapstore-types/**.' \
  -p 0 -l m0 --silent)
bd dep add "$M0_TYPES" "$M0_PIN"

M0_SYNTHGEN=$(bd create "Build snapstore-synthgen deterministic guest generator" \
  -d 'New workspace library crate crates/snapstore-synthgen (NOT a bare root tests/ dir - that is not a Cargo compile target). Deterministic 128 MiB guest images seeded by u64 (seeded PRNG, no ambient randomness or time), plus seeded burst mutation dirtying 256-2048 random-but-seeded pages per burst. API: generate image, iterate (page_index, page bytes), apply burst returning the dirty page set; helpers to emit page batches for ingest tests and entry lists for manifest tests. Consumed as a dev-dependency by all downstream M1-M3 test work; zero hypervisor dependency anywhere in this phase. CI AC test: same u64 seed produces bit-identical guests and identical burst sequences. Reserves: crates/snapstore-synthgen/**.' \
  -p 0 -l m0 --silent)
bd dep add "$M0_SYNTHGEN" "$M0_TYPES"

M0_CONFIG=$(bd create "config.toml loader in snapstore-server" \
  -d 'Config loading per ARCHITECTURE.md section 9 defaults: data_root, grpc_tcp_addr, grpc_uds_path, page_channel_path, http_addr; [pagestore] pack_max_bytes/ingest_queue_pages/ingest_hash_threads/flatten_cache_entries/mmap_sealed_packs; [meta] input_log_max_bytes (4 MiB)/metadata_value_max_bytes (16 MiB)/read_connections/write_batch_max; [gc]; [backup]. serde+toml, defaults applied for absent fields, loud typed errors on invalid values. Unit tests. Parallel M0 work - must NOT block M1 or the M0 gate. Reserves: crates/snapstore-server/src/config*.' \
  -p 1 -l m0 --silent)
bd dep add "$M0_CONFIG" "$M0_PIN"

M0_OBS=$(bd create "/healthz + /metrics HTTP stub and JSON tracing" \
  -d 'Minimal HTTP listener on config http_addr serving /healthz (200) and /metrics (prometheus text exposition from a shared registry that later milestones register into); JSON tracing-subscriber init for main(). Smoke test binds an ephemeral port and asserts both endpoints respond. Parallel M0 work - must NOT block M1 or the M0 gate. Reserves: crates/snapstore-server/**.' \
  -p 1 -l m0 --silent)
bd dep add "$M0_OBS" "$M0_CONFIG"

M0_CLI_BENCH=$(bd create "snapstorectl bench fio-baseline subcommand" \
  -d 'The only working M0 subcommand of crates/snapstore-cli. Shells out to fio (seq write QD32, seq read QD32, 4k randread), parses results, writes/updates docs/bench-baseline.md in a stable machine-parseable format the regression comparison harness will read later. Linux-only policy: cfg-gate or stub non-Linux builds; correctness is defined by Linux. Reserves: crates/snapstore-cli/**.' \
  -p 1 -l m0 --silent)
bd dep add "$M0_CLI_BENCH" "$M0_CRATES"
bd dep add "$M0_CLI_BENCH" "$M0_PIN"

M0_FIO=$(bd create "Run fio NVMe baseline on Intel box, record bench-baseline.md" \
  -d 'MANUAL Intel-box task: run snapstorectl bench fio-baseline against the Intel box NVMe and commit docs/bench-baseline.md (seq write/read QD32, 4k randread). All later benchmark targets are sanity-checked against this file. Must NOT block M1 or the M0 gate - no non-bench bead may ever depend on this (wiring it as a blocker would deadlock autonomous execution). Verified by a human before M0 is declared closed.' \
  -p 1 -l bench --silent)
bd dep add "$M0_FIO" "$M0_CLI_BENCH"

M0_CLIPPY=$(bd create "Add clippy -D warnings gate to CI" \
  -d 'Extend .github/workflows/ci.yaml (currently fmt, build, test with a dual checkout of the sibling control-plane repo) with cargo clippy --workspace --all-targets -- -D warnings. Keep the dual-checkout layout working. Reserves: .github/workflows/**.' \
  -p 0 -l ci --silent)

M0_NIGHTLY=$(bd create "Nightly CI workflow scaffold" \
  -d 'Add .github/workflows/nightly.yaml scaffold: cron schedule, dual checkout of sibling control-plane, placeholder job. The cargo-fuzz job lands with M2 (separate bead); leave a clearly marked extension point. Benchmarks NEVER run as hosted-CI pass/fail. Reserves: .github/workflows/**.' \
  -p 1 -l ci --silent)
bd dep add "$M0_NIGHTLY" "$M0_CLIPPY"

M0_GATE=$(bd create "M0 acceptance gate: CI-correctness ACs green" \
  -d 'Verify in hosted CI: workspace builds all 9 crates; fmt + clippy -D warnings + tests green; synthgen bit-identical AC test passes. Deliberately excludes (minimal M0->M1 edge set): fio baseline (manual Intel-box, label bench, non-blocking), healthz/metrics stub, config loader, JSON tracing, nightly scaffold - parallel M0 work verified before a human declares M0 closed, never blockers. ALL M1 beads depend on this gate and on nothing else in M0.' \
  -p 0 -l m0 --silent)
bd dep add "$M0_GATE" "$M0_CRATES"
bd dep add "$M0_GATE" "$M0_PIN"
bd dep add "$M0_GATE" "$M0_TYPES"
bd dep add "$M0_GATE" "$M0_SYNTHGEN"
bd dep add "$M0_GATE" "$M0_CLIPPY"

# ============================================================
# M1 — Page store core (packs, index, ingest)
# ============================================================

M1_DATAROOT=$(bd create "Data-root init: STORE_VERSION, store.uuid, tmp staging" \
  -d 'In crates/snapstore-pagestore: initialize/open the data root per ARCHITECTURE.md section 2: STORE_VERSION file containing ASCII 1 plus newline, refuse to start on mismatch or unknown version with a loud typed error; store.uuid (16 random bytes, written exactly once); create pages/packs/, manifests/, tmp/ (same-filesystem staging for atomic renames); clean tmp/ at startup. Unit tests incl. version-mismatch refusal and idempotent reopen. Reserves: crates/snapstore-pagestore/src/root*.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_DATAROOT" "$M0_GATE"

M1_PACK_CODEC=$(bd create "Pack + sidecar byte formats: encode, decode, validate" \
  -d 'Per ARCHITECTURE.md sections 2.1-2.2. Pack header (64 B): magic SPPACK01, version u16=1 (readers reject unknown versions loudly), flags, pack_id u32, store_uuid 16 B, created_epoch u64. created_epoch (logical counter at creation) is an INJECTED dependency - trait or fn parameter; M1 tests use a stub constant and M3 wires the real logical counter into this seam later. Page record (4144 B stride, 8-byte aligned): rec_magic CREC, rec_flags (bit0 zstd reserved, v1 raw), page_hash 32 B (BLAKE3 of uncompressed 4096), payload_len, crc32c of payload, 4096 B payload. Sidecar .sppx: magic SPPIDX01, version=1, pack_id, entry_count, 40 B entries sorted by page_hash {hash, offset}, trailing BLAKE3 of all preceding bytes. Round-trip + corruption + unknown-version-rejection unit tests. All little-endian. Reserves: crates/snapstore-pagestore/src/pack*, src/sidecar*.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_PACK_CODEC" "$M0_GATE"

M1_INDEX=$(bd create "Sharded in-memory page index with synced bit" \
  -d 'page_hash -> PackLoc map sharded 256 ways by hash[0]: parking_lot RwLock + hashbrown HashMap per shard, ~40 B/entry (ARCHITECTURE.md section 2.3). Entries carry a synced (durable) bit so snapshot commit can verify every referenced page is present AND durable, flushing via commit barrier when needed (section 3 step 7). API: probe (dedup check = one shard read-lock + map probe), insert, bulk publish after fsync, mark-synced, full iteration for restart-equivalence compare in tests. Concurrency unit tests across shards. Reserves: crates/snapstore-pagestore/src/index*.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_INDEX" "$M0_GATE"

M1_WRITER=$(bd create "Single pack-writer task with commit barrier and seal" \
  -d 'Exactly one writer task owns the open pack (append-only files want one appender - ARCHITECTURE.md sections 3, 6, 7.3): drain the bounded ingest queue, batch >=64 records per pwritev, fallocate 1 GiB at pack open (avoid extent churn), fdatasync per batch boundary, explicit commit-barrier operation (flush outstanding batches then mark index entries synced), seal when pack_max_bytes reached: write .sppx (sorted entries), full fsync of pack and sidecar, open next pack with fresh pack_id and injected created_epoch. Publish PackLoc into the index only after durability. Linux-only APIs (fallocate) cfg-gated; never weaken tests to pass on macOS. Reserves: crates/snapstore-pagestore/src/writer*.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_WRITER" "$M1_DATAROOT"
bd dep add "$M1_WRITER" "$M1_PACK_CODEC"
bd dep add "$M1_WRITER" "$M1_INDEX"

M1_REBUILD=$(bd create "Startup rebuild incl. torn-tail truncation" \
  -d 'Rebuild the in-memory page index at startup per ARCHITECTURE.md section 2.2: read every .sppx sequentially (verify trailing BLAKE3; sidecars are the authoritative location data), then scan the tail of the single unsealed pack record-by-record validating rec_magic + crc32c, ftruncate to the last whole-record boundary at the first torn record. Never panic on any torn state. This is the entire crash-recovery path for pages - packs are append-only and self-framing, no WAL. Unit tests with synthetic torn packs. Reserves: crates/snapstore-pagestore/src/rebuild*.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_REBUILD" "$M1_DATAROOT"
bd dep add "$M1_REBUILD" "$M1_PACK_CODEC"
bd dep add "$M1_REBUILD" "$M1_INDEX"

M1_INGEST=$(bd create "Ingest pipeline: rayon batch hashing, dedup, zero-page short-circuit" \
  -d 'Per ARCHITECTURE.md section 3: batch-hash incoming 4096 B pages on a bounded rayon pool (ingest_hash_threads, blake3 rayon feature; hash before dedup probe, always - the hash IS the identity); probe the index and drop duplicates; short-circuit the precomputed all-zeros page hash (never enqueued or stored; manifest entries still record it normally); enqueue novel pages to the pack writer via the bounded ingest queue (ingest_queue_pages default 65536, async backpressure when full). Library result mirrors PutPagesResponse semantics: pages_received / pages_new / pages_deduped + hashes in arrival order. Counters exported to the shared prometheus registry (snapstore_pages_ingested_total{dedup}). Reserves: crates/snapstore-pagestore/src/ingest*.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_INGEST" "$M1_WRITER"

M1_TEST_RESTART=$(bd create "M1 AC test: 1M-page ingest, restart, index identical" \
  -d 'In crates/snapstore-tests using snapstore-synthgen: ingest 1M synthetic pages, snapshot the full index contents, drop the store, reopen (startup rebuild), full compare - rebuilt index identical to pre-restart (every hash, PackLoc, count). Tmpdir on CI runner disk; keep runtime CI-feasible but do NOT shrink below 1M pages (the AC is explicit). CI-correctness AC - gates M1.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_TEST_RESTART" "$M1_INGEST"
bd dep add "$M1_TEST_RESTART" "$M1_REBUILD"

M1_TEST_TORN=$(bd create "M1 AC test: torn-tail truncation matrix" \
  -d 'Parameterized matrix test: write an open pack with known records, then for EVERY byte offset within the last record (all 4144 offsets, plus header-boundary edge cases) truncate the file there and run startup recovery: always recovers to the last whole record, never panics, index matches exactly the surviving whole records, file truncated to the good boundary. CI-correctness AC - gates M1.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_TEST_TORN" "$M1_WRITER"
bd dep add "$M1_TEST_TORN" "$M1_REBUILD"

M1_TEST_DEDUP=$(bd create "M1 AC test: dedup invariant + zero-page short-circuit" \
  -d 'Ingest the same 100k synthetic pages twice: second pass reports pages_new==0 and pages_deduped==100k, physical pack bytes unchanged after pass two. Additionally assert the zero-page short-circuit: batches containing all-zero pages never write them to packs yet they report as present/deduped consistently. CI-correctness AC - gates M1.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_TEST_DEDUP" "$M1_INGEST"

M1_BENCH_IMPL=$(bd create "M1 criterion benches: ingest, hash+ingest, index probe" \
  -d 'criterion benches in crates/snapstore-pagestore/benches: single-stream ingest with pre-hashed memory source (target >=1.5 GB/s), hash+ingest (>=1.0 GB/s), index probe across 8 threads (>=5M lookups/s). Benches must BUILD in hosted CI (cargo bench --no-run) but the numbers are NEVER asserted as hosted-CI pass/fail - they are Intel-box gates recorded separately. Output format stable for the regression comparison harness.' \
  -p 1 -l m1 --silent)
bd dep add "$M1_BENCH_IMPL" "$M1_INGEST"

M1_BENCH_RUN=$(bd create "Run M1 benchmark gate on Intel box, record results" \
  -d 'Intel-box task: run the M1 criterion benches on the Intel box NVMe, sanity-check against the fio numbers, record results in docs/bench-baseline.md. Targets: single-stream ingest >=1.5 GB/s pre-hashed (this number also satisfies the snapshot-store portion of the Phase 1 exit gate), hash+ingest >=1.0 GB/s, index probe >=5M lookups/s across 8 threads. Non-blocking: never a dependency of the M1 gate or of downstream work; verified by a human before M1 is declared closed.' \
  -p 1 -l bench --silent)
bd dep add "$M1_BENCH_RUN" "$M1_BENCH_IMPL"
bd dep add "$M1_BENCH_RUN" "$M0_FIO"

M1_GATE=$(bd create "M1 acceptance gate: CI-correctness ACs green" \
  -d 'All M1 CI-correctness AC tests green in hosted CI: restart index-equivalence (1M pages), torn-tail matrix (every byte offset), dedup invariant + zero-page short-circuit. Intel-box benchmark numbers are intentionally NOT dependencies (label bench, human-verified before declaring M1 closed). Every M2 and M3 bead depends on this gate - never on individual M1 beads. M2 and M3 proceed in PARALLEL after this gate.' \
  -p 0 -l m1 --silent)
bd dep add "$M1_GATE" "$M1_TEST_RESTART"
bd dep add "$M1_GATE" "$M1_TEST_TORN"
bd dep add "$M1_GATE" "$M1_TEST_DEDUP"

# ============================================================
# M2 — Manifest codec + snapshot commit/resolve (parallel with M3)
# ============================================================

M2_CODEC=$(bd create "Manifest encode/decode/validate (pure, fuzzable)" \
  -d 'crates/snapstore-manifest per API.md section 2 - pure, no I/O beyond byte slices, fuzzable. Structs: Manifest{version,delta,parent,guest_ram_bytes,entries,device_blob}, ManifestEntry{page_index,page_hash}, DeviceBlob{format,zstd,bytes,raw_len}. encode -> canonical bytes + BLAKE3 footer: exactly one valid encoding per snapshot (entries sorted ascending by page_index, strictly unique, fixed field order). decode with FULL validation: magic SPSMAN01, version=1 (reject unknown loudly), flags bits 2-15 must be 0, header_len==96, parent all-zero iff DELTA clear, guest_ram_bytes multiple of 4096, page_size==4096 (v1 readers reject otherwise), sort/uniqueness, FULL contiguity 0..N-1, DEV_ZSTD blob decompresses to device_blob_raw_len (result discarded - storage keeps containers byte-identical), footer recompute match. snapshot_ref(buf) = blake3 of all but last 32 bytes. Errors via the snapstore-types error enum. Reserves: crates/snapstore-manifest/**.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_CODEC" "$M1_GATE"

M2_FLATTEN=$(bd create "flatten(chain): shadowing merge + gap detection" \
  -d 'pub fn flatten over a child-first manifest chain per API.md section 2: child entries shadow parent entries with equal page_index; pure in-memory merge over sorted entry arrays; result must cover every index 0..guest_ram_bytes/4096 - a gap is a typed corruption error (P0 signal), never a panic. Accepts any chain depth; the server never rewrites manifests (implicit FULL conversion would change the content-derived ref). Reserves: crates/snapstore-manifest/**.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_FLATTEN" "$M2_CODEC"

M2_TEST_ROUNDTRIP=$(bd create "M2 AC proptest: round-trip + canonicality" \
  -d 'proptest strategies generating arbitrary valid manifests (FULL and DELTA, varied entry counts, device blobs incl. DEV_ZSTD): decode(encode(m)) == m and snapshot_ref stable across re-encode; canonicality: entries supplied in shuffled order encode to byte-identical output (sort enforced). Also one half of the Phase 1 exit gate (snapshot-store portion). CI-correctness AC - gates M2.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_TEST_ROUNDTRIP" "$M2_CODEC"

M2_TEST_FLATTEN=$(bd create "M2 AC proptest: flatten vs naive reference" \
  -d 'proptest: flatten correctness against a naive reference implementation (rebuild full page map by walking the chain root-ward) for random chains of depth <= 64 with 2k-entry deltas; plus negative cases asserting gap detection errors. CI-correctness AC - gates M2.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_TEST_FLATTEN" "$M2_FLATTEN"

M2_FUZZ_TARGET=$(bd create "cargo-fuzz target on Manifest::decode" \
  -d 'cargo-fuzz target feeding arbitrary bytes to Manifest::decode (plus snapshot_ref on the valid corpus); seed corpus generated from the proptest strategies. Must never panic, OOM, or hit UB - every failure is a typed error. Reserves: fuzz/** (or crates/snapstore-manifest/fuzz/**).' \
  -p 1 -l m2 --silent)
bd dep add "$M2_FUZZ_TARGET" "$M2_CODEC"

M2_CI_FUZZ=$(bd create "Nightly CI job: 10-min fuzz of Manifest::decode" \
  -d 'Wire the cargo-fuzz target into .github/workflows/nightly.yaml: nightly toolchain, 10 minutes wall, fail on crash, upload crash artifacts. This IS an M2 CI-correctness AC (the M2 gate depends on it) - the AC reads: cargo fuzz target on Manifest::decode runs 10 min in nightly CI, no crashes. Reserves: .github/workflows/**.' \
  -p 1 -l ci --silent)
bd dep add "$M2_CI_FUZZ" "$M2_FUZZ_TARGET"
bd dep add "$M2_CI_FUZZ" "$M0_NIGHTLY"

M2_SPM_IO=$(bd create "Loose .spm write discipline + manifests dir I/O" \
  -d 'In crates/snapstore-pagestore (which owns the data root - NOT in the pure manifest crate): manifests/<first-byte-hex>/<hash-hex>.spm layout per ARCHITECTURE.md section 2.4; atomic write via tmp/ + fsync(file) + rename(2) + fsync(parent dir); read returns stored container byte-identical; startup cleanup removes .spm files with bad footers (incomplete writes). Exposed as library API; the tonic server wires it up in M4 (out of scope). Reserves: crates/snapstore-pagestore/src/manifest_io*.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_SPM_IO" "$M2_CODEC"

M2_COMMIT=$(bd create "Snapshot commit: PutSnapshot library path" \
  -d 'Library API in snapstore-pagestore (server wiring is M4): validate the container via snapstore-manifest (InvalidArgument on malformed/bad footer/unknown version); verify parent_manifest_hash resolves to a stored manifest with identical guest_ram_bytes (FailedPrecondition if missing); verify EVERY referenced page_hash is present AND durable in the page index (synced bit; issue a commit barrier to the pack writer if needed); missing pages -> FailedPrecondition with MissingPages detail listing exactly the gap hashes; then atomic .spm write; return SnapshotRef. Enforces the pages -> manifest fsync ordering invariant (ARCHITECTURE.md section 3 steps 7-9): returns only after rename + dir fsync. Idempotent re-commit of a stored manifest succeeds. Reserves: crates/snapstore-pagestore/src/commit*.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_COMMIT" "$M2_SPM_IO"

M2_RESOLVE=$(bd create "GetSnapshot/ResolvePages library path + flatten LRU" \
  -d 'Library API: GetSnapshot returns the stored container byte-identical; ResolvePages Mode A (full flatten of the chain) and Mode B (delta-only vs baseline_ref, which must be a chain ancestor - typed error otherwise) with hashes_only option, results ascending by page_index in bounded batches; flatten LRU cache (flatten_cache_entries default 1024) keyed by SnapshotRef - sibling restores hit it constantly; page payload reads = index probe + pread at PackLoc with an LRU of File handles per pack (cap 256). Unit tests: both modes, cache hit behavior, baseline-not-ancestor, snapstore_flatten_depth histogram exported. CI-correctness unit tests gate M2 via this bead. Reserves: crates/snapstore-pagestore/src/resolve*.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_RESOLVE" "$M2_SPM_IO"
bd dep add "$M2_RESOLVE" "$M2_FLATTEN"

M2_TEST_MISSING=$(bd create "M2 AC test: commit-with-missing-pages lists exact gaps" \
  -d 'In crates/snapstore-tests with synthgen: build a manifest referencing pages partially absent from the store; commit -> library FailedPrecondition error whose MissingPages detail lists EXACTLY the gap hashes (no more, no fewer); ingest the gaps, retry -> success with the same SnapshotRef. Asserts snapstore-types error variants (gRPC statuses do not exist until M4). CI-correctness AC - gates M2.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_TEST_MISSING" "$M2_COMMIT"

M2_BENCH_IMPL=$(bd create "M2 criterion benches: flatten chain, PutSnapshot" \
  -d 'criterion benches: flatten of a 64-deep chain of 2k-entry deltas, warm (target <2 ms); PutSnapshot with manifest already-paged (target p50 <3 ms). Build in hosted CI via cargo bench --no-run; numbers are Intel-box gates only, never hosted-CI pass/fail. Stable output for the comparison harness.' \
  -p 1 -l m2 --silent)
bd dep add "$M2_BENCH_IMPL" "$M2_RESOLVE"
bd dep add "$M2_BENCH_IMPL" "$M2_COMMIT"

M2_BENCH_RUN=$(bd create "Run M2 benchmark gate on Intel box, record results" \
  -d 'Intel-box task: run M2 benches on the Intel box NVMe, record into docs/bench-baseline.md (flatten 64-deep 2k-entry chain <2 ms warm; PutSnapshot already-paged p50 <3 ms). Non-blocking: never a dependency of the M2 gate or downstream work; human-verified before M2 is declared closed.' \
  -p 1 -l bench --silent)
bd dep add "$M2_BENCH_RUN" "$M2_BENCH_IMPL"
bd dep add "$M2_BENCH_RUN" "$M0_FIO"

M2_GATE=$(bd create "M2 acceptance gate: CI-correctness ACs green" \
  -d 'All M2 CI-correctness ACs green: round-trip + canonicality proptests, flatten-vs-reference proptest, commit-with-missing-pages exact-gaps test, resolve unit tests, nightly cargo-fuzz job (10 min, no crashes) wired and passing. Intel-box benchmarks excluded by design (label bench, human-verified). M4+ (gRPC surface) out of scope for this phase plan.' \
  -p 0 -l m2 --silent)
bd dep add "$M2_GATE" "$M2_TEST_ROUNDTRIP"
bd dep add "$M2_GATE" "$M2_TEST_FLATTEN"
bd dep add "$M2_GATE" "$M2_TEST_MISSING"
bd dep add "$M2_GATE" "$M2_CI_FUZZ"
bd dep add "$M2_GATE" "$M2_RESOLVE"

# ============================================================
# M3 — Metadata DB (snapstore-meta), parallel with M2 after M1
# ============================================================

M3_SCHEMA=$(bd create "Schema v1 DDL, migrations table, pragma discipline" \
  -d 'crates/snapstore-meta: open/create meta/tree.db. Schema v1 DDL exactly per ARCHITECTURE.md section 5.3: meta KV table (schema_version, store_uuid, logical_counter rows); experiment-scoped nodes with composite PK (experiment_id, node_id), parent FK, depth, snapshot_ref, input_log_id FK, status, scores, visit/expand counts, logical-counter timestamps, attrs blob, plus the 5 indexes (parent, status+progress, status+novelty, created, snapshot_ref global); kv_metadata (key PK, value, generation, updated_at); input_logs (log_id PK, size, inner_version, content, created_at); pins; tombstones - all WITHOUT ROWID. Migrations/versioning: refuse unknown schema_version loudly (explicit versions everywhere). Pragmas on every connection per section 5.2: journal_mode=WAL, synchronous=FULL (writer), foreign_keys=ON, wal_autocheckpoint=4000, mmap_size=268435456, busy_timeout=5000. PRAGMA integrity_check at startup, refuse on failure. Reserves: crates/snapstore-meta/src/schema*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_SCHEMA" "$M1_GATE"

M3_ACTOR=$(bd create "Meta actor (single writer) + read pool" \
  -d 'Per ARCHITECTURE.md section 5.2: one writer connection owned by a dedicated blocking thread (the meta actor); all mutations arrive on a crossbeam channel as commands carrying oneshot reply senders; the actor drains up to write_batch_max=256 queued commands into one BEGIN IMMEDIATE .. COMMIT transaction (this is what makes bulk updates cheap and CAS race-free by construction). Read pool of read_connections=4 with PRAGMA query_only=ON used from tokio via spawn_blocking; WAL gives stable snapshots; reads never enter the actor. Clean shutdown drains the queue. snapstore_meta_txn_seconds metric. Reserves: crates/snapstore-meta/src/actor*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_ACTOR" "$M3_SCHEMA"

M3_COUNTER=$(bd create "Logical counter + wire real created_epoch into M1 seam" \
  -d 'Logical counter per ARCHITECTURE.md section 5.3: monotonic, assigned inside the writer actor, flushed to the meta table on every writer txn; startup value = max(persisted, max(nodes.created_at), max(nodes.updated_at)) + 1. Ordering guarantees come from this counter, never from node-id values. ALSO: provide the production implementation of the created_epoch dependency injected in M1 pack headers, replacing the M1 stub constant at the integration wiring point (the M1 seam is a trait or fn parameter; this bead supplies the real source and an integration test proving new packs carry real epochs). Reserves: crates/snapstore-meta/src/counter*, the pagestore wiring point in crates/snapstore-tests.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_COUNTER" "$M3_ACTOR"

M3_LOGS=$(bd create "Input-log container validation + idempotent storage" \
  -d 'Validate the SILG container per API.md section 3: magic SILG, container_version=1 (reject unknown loudly), flags=0, inner_format_version surfaced into input_logs.inner_version, payload_len, BLAKE3 footer over preceding bytes == log_id; enforce 24 + payload_len + 32 <= input_log_max_bytes (4 MiB platform cap) -> InvalidArgument. Storage inline in SQLite (atomicity with node insert; content-addressed): INSERT OR IGNORE, verify size matches on conflict - idempotent re-put returns the same log_id. Get returns the container byte-identical. Reserves: crates/snapstore-meta/src/logs*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_LOGS" "$M3_COUNTER"

M3_NODES=$(bd create "CreateNode: idempotent insert, u64 bit-cast, validation" \
  -d 'CreateNode library path per API.md section 1.4 + ARCHITECTURE.md section 5.3 notes: caller-assigned u64 node_id stored as i64 bit-cast (round-trips exactly; never compared by SQL ordering - cursors use logical counters); INSERT conflict on (experiment_id, node_id) -> re-read the row and compare immutable fields (parent_node_id, snapshot_ref, input_log_id): identical -> return stored NodeMeta with success (enables blind retry); different -> AlreadyExists, zero rows changed. Validation: parent exists and is not PRUNED (FailedPrecondition), root rules (node_id 0 is the experiment root, parent unset iff root, root-already-exists conflict), depth = parent depth + 1 enforced in code, snapshot_ref and input_log_id existence -> NotFound; experiments are implicit (root row creates the experiment). Optional inline input_log_container stored atomically in the same txn (input_log_id must then be empty). Reserves: crates/snapstore-meta/src/nodes*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_NODES" "$M3_COUNTER"
bd dep add "$M3_NODES" "$M3_LOGS"

M3_QUERIES=$(bd create "Canonical read queries: children, path, scan, stats" \
  -d 'Implement ARCHITECTURE.md section 5.4 canonical SQL on the read pool: get children; recursive-CTE path-to-root returned root-first (GetPath semantics, optional inline input-log containers parallel to nodes[1..]); QueryNodes filtered scan - statuses, min/max progress, min novelty, depth bounds, created_after / updated_after exclusive logical-counter cursors, the three OrderBy modes (created ascending is the stable sync cursor), limit with streamed pages; Stats per-experiment and store-wide (counts by status, max_depth, best_progress_score, logical_counter, experiments_total). QueryNodes is the ONLY scan primitive - no ListNodes. Reserves: crates/snapstore-meta/src/queries*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_QUERIES" "$M3_ACTOR"

M3_UPDATES=$(bd create "UpdateNodes: bulk all-or-nothing transaction" \
  -d 'Per API.md section 1.4: up to 4096 NodeUpdates within ONE experiment applied in ONE transaction (all-or-nothing) through the writer actor; optional status/progress/novelty/attrs (attrs is full replace), visit_count_delta and expand_count_delta are ADDED not assigned, touch_visited sets last_visited_at to the txn logical counter; updated_at = txn counter on every touched row. ANY unknown id -> NotFound with MissingNodes detail listing the missing ids, txn rolls back, zero rows changed. Returns (updated_at, applied). Reserves: crates/snapstore-meta/src/updates*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_UPDATES" "$M3_NODES"

M3_KV=$(bd create "Metadata KV with generation CAS" \
  -d 'kv_metadata per API.md section 1.5 + ARCHITECTURE.md section 5.3: Put with expected_generation unset (unconditional last-writer-wins) / 0 (create-only, key must not exist) / N>0 (UPDATE .. WHERE key=? AND generation=? inside the writer actor txn; changes()==0 -> FailedPrecondition with CurrentGeneration detail, generation 0 in the detail meaning key absent). Generation = 1 on first create, +1 per successful write; after delete the history restarts at 1. Get -> (value, generation), NotFound if absent. Delete unconditional or CAS. Key 1..=512 UTF-8 bytes, value <= metadata_value_max_bytes (16 MiB) -> InvalidArgument on violation. This is the single-writer enforcement primitive for the orchestrator - semantics must be exact. Reserves: crates/snapstore-meta/src/kv*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_KV" "$M3_COUNTER"

M3_PINS_TOMB=$(bd create "Pins, tombstones, PruneSubtree transaction" \
  -d 'Pin/Unpin rows (snapshot_ref PK, reason, created_at) - pins are GC roots, period (rule R5). PruneSubtree per ARCHITECTURE.md section 4.4 phase one, in ONE txn: verify the node exists and is not the experiment root (node_id 0) unless allow_root=true (safety interlock); recursive-CTE subtree collect; set status=PRUNED on all collected rows AND insert one tombstones row for the subtree root (node_count recorded); DELETE IS DEFERRED to GC reaping (M7, out of scope) - two-phase so prune is observable and crash-resumable. Returns nodes_pruned. CI unit tests: CTE correctness on branched trees, allow_root interlock, idempotent re-prune, tombstone row contents. Reserves: crates/snapstore-meta/src/prune*.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_PINS_TOMB" "$M3_NODES"

M3_TEST_CURSOR=$(bd create "M3 AC test: 1M-node tree, GetPath + cursor interleaving" \
  -d 'In crates/snapstore-tests with synthgen-driven drivers: build a 1M-node synthetic tree (branching ~8, with a depth-5k spine); assert GetPath(depth 5k) correctness (root-first order, exact rows); QueryNodes frontier scan with created_after cursor streams with NO gaps and NO dupes under concurrent writes - interleaving test with a writer task creating nodes while a reader pages via the cursor. The <40 ms p99 latency number is an Intel-box benchmark, NOT asserted here. CI-correctness AC - gates M3.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_TEST_CURSOR" "$M3_QUERIES"
bd dep add "$M3_TEST_CURSOR" "$M3_NODES"

M3_TEST_IDEMP=$(bd create "M3 AC test: CreateNode idempotency replay" \
  -d 'Generate a synthetic experiment CreateNode stream; replay ANY prefix with duplicates included in arbitrary interleavings -> byte-identical tree (full table compare against the reference run); key reuse with DIFFERENT immutable content -> AlreadyExists and zero rows changed (verified by before/after table compare). CI-correctness AC - gates M3.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_TEST_IDEMP" "$M3_NODES"

M3_TEST_ISOLATION=$(bd create "M3 AC test: multi-experiment isolation + Stats" \
  -d 'Two interleaved synthetic experiments sharing page content: neither ever observes the other via any tree query (GetNode, GetChildren, GetPath, QueryNodes, per-experiment Stats); per-experiment Stats match each driver own bookkeeping exactly (node counts by status, max depth, best score). Page/manifest storage is global by design - only tree rows carry the experiment dimension. CI-correctness AC - gates M3.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_TEST_ISOLATION" "$M3_NODES"
bd dep add "$M3_TEST_ISOLATION" "$M3_QUERIES"

M3_TEST_CAS=$(bd create "M3 AC test: KV CAS contention, caps, delete-CAS" \
  -d 'Concurrent writers hammering one key: exactly one winner per generation, losers receive FailedPrecondition with CurrentGeneration detail; create-only path (expected_generation=0) covered; delete-CAS covered; 16 MiB value-cap rejection covered (InvalidArgument). Asserts library error enum variants (gRPC statuses arrive in M4). CI-correctness AC - gates M3.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_TEST_CAS" "$M3_KV"

M3_TEST_ATOMIC=$(bd create "M3 AC test: UpdateNodes atomicity" \
  -d 'UpdateNodes batch where exactly one id is unknown -> zero rows changed (full-table before/after compare), error is NotFound with MissingNodes detail listing precisely the bad ids; a valid retry then applies fully. CI-correctness AC - gates M3.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_TEST_ATOMIC" "$M3_UPDATES"

M3_TEST_KILL=$(bd create "M3 AC test: kill -9 loop x200 on 256-update batches" \
  -d 'Minimal kill-loop (SCOPE NOTE: the source plan runs this in the M6 crash-injection harness, out of scope here; the M6 harness absorbs this test later - NO failpoints now): child process runs 256-update batch workloads against tree.db; parent SIGKILLs the child at randomized SEEDED points (reproducible); restart + invariant check: each batch is wholly present or wholly absent, never partial; loop x200. Linux-only, runs in hosted CI (ubuntu-latest). Lives in crates/snapstore-tests. CI-correctness AC - gates M3.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_TEST_KILL" "$M3_UPDATES"

M3_BENCH_IMPL=$(bd create "M3 criterion benches: node mutations, KV, GetPath" \
  -d 'criterion / snapstorectl bench coverage: CreateNode with inline 16 KiB log (target p50 <1.5 ms); UpdateNodes(256) (p50 <3 ms); PutMetadata 64 KiB value (p50 <2 ms); sustained >=5k node-mutations/s through the actor; GetPath depth-5k p99 <40 ms on the 1M-node tree. Build in hosted CI (cargo bench --no-run); numbers gate only on the Intel box. Stable output for the comparison harness.' \
  -p 1 -l m3 --silent)
bd dep add "$M3_BENCH_IMPL" "$M3_NODES"
bd dep add "$M3_BENCH_IMPL" "$M3_UPDATES"
bd dep add "$M3_BENCH_IMPL" "$M3_KV"
bd dep add "$M3_BENCH_IMPL" "$M3_LOGS"
bd dep add "$M3_BENCH_IMPL" "$M3_QUERIES"

M3_BENCH_RUN=$(bd create "Run M3 benchmark gate on Intel box, record results" \
  -d 'Intel-box task: run the M3 benches on the Intel box NVMe and record into docs/bench-baseline.md (CreateNode+16 KiB log p50 <1.5 ms; UpdateNodes(256) p50 <3 ms; PutMetadata 64 KiB p50 <2 ms; >=5k mutations/s sustained; GetPath depth-5k p99 <40 ms). Non-blocking: never a dependency of the M3 gate or downstream work; human-verified before M3 is declared closed.' \
  -p 1 -l bench --silent)
bd dep add "$M3_BENCH_RUN" "$M3_BENCH_IMPL"
bd dep add "$M3_BENCH_RUN" "$M0_FIO"

M3_GATE=$(bd create "M3 acceptance gate: CI-correctness ACs green" \
  -d 'All M3 CI-correctness ACs green in hosted CI: 1M-node tree GetPath + cursor interleaving, CreateNode idempotency replay, multi-experiment isolation + Stats, KV CAS contention + caps, UpdateNodes atomicity, kill -9 loop x200, PruneSubtree/pins/tombstones unit tests. Intel-box benchmark numbers excluded by design (label bench, human-verified before close). M4+ (gRPC, fast path, crash harness, GC) out of scope for this phase plan.' \
  -p 0 -l m3 --silent)
bd dep add "$M3_GATE" "$M3_TEST_CURSOR"
bd dep add "$M3_GATE" "$M3_TEST_IDEMP"
bd dep add "$M3_GATE" "$M3_TEST_ISOLATION"
bd dep add "$M3_GATE" "$M3_TEST_CAS"
bd dep add "$M3_GATE" "$M3_TEST_ATOMIC"
bd dep add "$M3_GATE" "$M3_TEST_KILL"
bd dep add "$M3_GATE" "$M3_PINS_TOMB"

# ============================================================
# Cross-cutting: bench harness, phase exit, docs
# ============================================================

BENCH_HARNESS=$(bd create "Benchmark regression comparison harness (15 pct tolerance)" \
  -d 'A deliverable, not a judgment call: harness (script or small tool crate, e.g. tools/bench-check) that parses criterion and snapstorectl bench output, compares against docs/bench-baseline.md with plus/minus 15 percent tolerance, and exits nonzero on regression. Wire a nightly invocation that runs ON THE INTEL BOX (self-hosted runner or cron) - never as hosted-CI pass/fail. Reserves: tools/**, docs/bench-baseline.md format.' \
  -p 1 -l bench --silent)
bd dep add "$BENCH_HARNESS" "$M0_FIO"
bd dep add "$BENCH_HARNESS" "$M1_BENCH_IMPL"

PHASE_EXIT=$(bd create "Record Phase 1 exit gate (snapshot-store portion)" \
  -d 'Verify and record (docs/bench-baseline.md note + phase tracker): the snapshot-store portion of the Phase 1 exit gate is satisfied by (1) the M1 single-stream ingest benchmark >=1.5 GB/s with a pre-hashed memory source on the Intel box, and (2) manifest round-trip property tests green in CI. The phase-doc fast-path-ingest wording refers to the M5 page channel which is OUT OF SCOPE - do not require or wait for M5 functionality.' \
  -p 1 -l bench --silent)
bd dep add "$PHASE_EXIT" "$M1_BENCH_RUN"
bd dep add "$PHASE_EXIT" "$M2_TEST_ROUNDTRIP"

DOCS_API=$(bd create "Rustdoc pass over public library APIs" \
  -d 'Crate-level and public-item rustdoc for snapstore-types (incl. the error-to-gRPC mapping table), snapstore-pagestore (pack/sidecar formats, commit ordering invariant, references to ARCHITECTURE.md sections 2-3), snapstore-manifest (canonical encoding rules, API.md section 2), snapstore-meta (schema, actor model, CAS semantics), snapstore-synthgen (determinism contract). Enable missing-docs lint for the library crates or document exceptions; cargo doc warning-clean in CI.' \
  -p 2 -l docs --silent)
bd dep add "$DOCS_API" "$M2_GATE"
bd dep add "$DOCS_API" "$M3_GATE"

echo ""
echo "Bead graph created:"
bd list | tail -1 >/dev/null 2>&1 || true
echo "  M0: skeletons, pinning, types, synthgen, CI, fio baseline (+gate)"
echo "  M1: data root, pack codec, index, writer, rebuild, ingest, AC tests, benches (+gate)"
echo "  M2: manifest codec, flatten, fuzz, spm I/O, commit, resolve, AC tests, benches (+gate)"
echo "  M3: schema, actor, counter, logs, nodes, queries, updates, KV, prune, AC tests, benches (+gate)"
echo "  Cross: bench comparison harness, Phase 1 exit record, rustdoc pass"
echo ""
echo "Inspect with:"
echo "  bd ready          # unblocked tasks (expect: skeleton crates, dep pinning, clippy gate)"
echo "  bd dep tree       # full dependency tree"
echo "  bd dep cycles     # must report none"
