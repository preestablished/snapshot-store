# WI3 - `snapstore-28z` M5 Transport Revalidation

The existing ignored test already covers the core M5 transport rows:
`crates/snapstore-server/tests/page_channel_perf.rs:42`. It prints PUT_BATCH,
GET_BATCH, and the 16-client commit row at `:243`.

This work item makes that harness evidence-grade and runs it on the hardware
qualified by WI2.

## Harness Changes

1. Add `SNAPSTORE_BENCH_ROOT` support. Replace `TempDir::new()` at
   `page_channel_perf.rs:45` with `tempfile::Builder::new().prefix(...).tempdir_in(root)`.
   Fail with a clear message when the env var is absent for ignored perf runs.

2. Add machine-readable output. Use an env var such as
   `SNAPSTORE_M5_BENCH_JSON=$EVIDENCE_ROOT/m5-transport/results.json`.
   Add `serde_json = "1"` to `crates/snapstore-server/Cargo.toml` dev-deps;
   the crate currently has `serde` but not `serde_json`, and both this harness
   and `gc_readiness_bench` need JSON output.

   Suggested schema:

   ```json
   {
     "put_batch_warm_1_stream_gbps": 0.0,
     "put_batch_warm_sustained_gbps": 0.0,
     "get_batch_warm_1_stream_gbps": 0.0,
     "get_batch_warm_sustained_gbps": 0.0,
     "commit_16x8mib_p50_ms": 0.0,
     "commit_16x8mib_p99_ms": 0.0,
     "commit_16x8mib_aggregate_gbps": 0.0,
     "samples": {
       "commit_latencies_ms": []
     }
   }
   ```

3. Add the deferred CreateNode/UpdateNodes p50 rows. Keep them in the same
   ignored test or a second ignored test in the same file.

   Use `serve_for_tests` plus raw gRPC or the normal client against the same
   `SNAPSTORE_BENCH_ROOT` store. Pre-create the snapshot ref outside the timed
   window. For the CreateNode row, build a valid 16 KiB input-log container with
   `snapstore_client::helpers::build_input_log_container`, set `input_log_id`
   to the container's log id, and time only the CreateNode RPC/transaction. The
   server/meta path validates the inline container and only inserts it when the
   id is absent, so invalid bytes or reused ids make the row meaningless.
   Measure:

   | Row | Spec |
   |---|---:|
   | CreateNode + 16 KiB inline log | p50 `< 1.5 ms` |
   | UpdateNodes(256) | p50 `< 3 ms` |

   Use at least 500 samples after a warm-up batch. Print and write p50/p95/p99
   even though only p50 gates those rows.

4. Keep the existing bars exactly:

   | Row | Bar |
   |---|---:|
   | PUT_BATCH dedup-warm sustained, 4 streams | `>= 1.5 GB/s` |
   | GET_BATCH warm sustained, 4 streams | `>= 2.5 GB/s` |
   | 16 clients x 8 MiB deltas p99 commit | `< 40 ms` |
   | 16 clients aggregate ingest | `>= 1.2 GB/s` |

5. Add a short CPU/memory/disk profile capture around the run. The evidence
   script can run `pidstat`, `iostat`, or `perf stat` if present. Missing tools
   are not fatal, but the absence must be recorded.

## Run Command

From the qualified host:

```bash
SNAPSTORE_BENCH_ROOT=/mnt/phase5-scratch \
SNAPSTORE_M5_BENCH_JSON="$EVIDENCE_ROOT/m5-transport/results.json" \
cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture \
  2>&1 | tee "$EVIDENCE_ROOT/m5-transport/page_channel_perf.log"
```

Also run the library read-path Criterion bench as supporting evidence for the
warm-read row already mentioned in `docs/bench-baseline.md:36`:

```bash
SNAPSTORE_BENCH_ROOT=/mnt/phase5-scratch \
cargo bench -p snapstore-pagestore --bench read_path -- \
  --warm-up-time 2 --measurement-time 8 \
  2>&1 | tee "$EVIDENCE_ROOT/m5-transport/read_path.log"
```

If `read_path.rs` still uses `TempDir::new()`, add the same
`SNAPSTORE_BENCH_ROOT` plumbing before counting the result. Apply the same rule
to any supporting Criterion bench counted in the evidence, including
`snapstore-pagestore/benches/ingest.rs` and
`snapstore-store/benches/put_snapshot.rs`.

## Bottleneck Attribution

If a row misses, collect enough evidence to choose one primary attribution:

| Attribution | Evidence |
|---|---|
| CPU or memory bus | high CPU utilization, poor scaling with more streams, low disk busy |
| Disk/fsync | high await/util in iostat, fio ceiling close to measured throughput, p99 tracks flush latency |
| Code path | profile points inside hashing, memfd, manifest fsync, SQLite actor, lock contention, or page-channel copy path |

Candidate profile commands:

```bash
perf stat -d -- cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture
pidstat -durh 1
iostat -xz 1
```

Use the tools available on the lab box; record unavailable tools rather than
silently omitting attribution.

## Documentation

Update `docs/bench-baseline.md` with a new section after the current M5 table:

```markdown
## Phase 5 Readiness Revalidation - <hostname>, <date>

<machine identity and disk>

| BM | Spec target | Measured | Status | Evidence |
|---|---:|---:|---|---|
...
```

For `snapstore-28z`, close only if every deferred row in this file has a
measured value. If any row is left out, rescope the bead explicitly and name the
missing row.
