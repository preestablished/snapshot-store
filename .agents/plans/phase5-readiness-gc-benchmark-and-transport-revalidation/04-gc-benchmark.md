# WI4 - `snapstore-feb` M7 GC Benchmark

Build and run the deferred M7 `BM:` bar:

> 100k-node tree, about 30 GB physical, about 50% garbage, full GC in less than
> 60 seconds while concurrent ingest sustains 200 MB/s, with p99 commit latency
> during GC less than 2x idle p99.

No harness exists today. Add one as an ignored integration test:
`crates/snapstore-server/tests/gc_readiness_bench.rs`.

## Harness Placement

Use an ignored test rather than Criterion. This benchmark is a large
acceptance-style scenario with one or a few parameterized runs, not a
microbenchmark.

Add env vars:

| Env var | Meaning | Default |
|---|---|---|
| `SNAPSTORE_BENCH_ROOT` | Parent directory for scratch store | required |
| `SNAPSTORE_GC_BENCH_JSON` | JSON output path | optional |
| `SNAPSTORE_GC_NODES` | Total nodes | `100000` |
| `SNAPSTORE_GC_PHYSICAL_GB` | Approx physical page bytes | `30` |
| `SNAPSTORE_GC_GARBAGE_FRACTION` | Target garbage fraction after pruning | `0.50` |
| `SNAPSTORE_GC_INGEST_MBPS` | Concurrent ingest rate | `200` |
| `SNAPSTORE_GC_SEED` | Deterministic generator seed | fixed recorded seed |

Use `tempfile::Builder::tempdir_in(SNAPSTORE_BENCH_ROOT)`. The test should
print enough progress that a long lab run does not look hung.

## Data Model

Use in-process APIs so the benchmark measures the GC/manifest/page/meta paths
without gRPC noise:

| Operation | API |
|---|---|
| Page ingest | `SnapshotStore::pages().ingest(...)` |
| Manifest commit | `SnapshotStore::put_snapshot(...)` |
| Build full/delta containers | `snapstore_store::build` (`crates/snapstore-store/src/lib.rs:1026`) |
| Create node rows | `MetaDb::create_node` (`crates/snapstore-meta/src/lib.rs:190`) |
| Prune subtrees | `MetaDb::prune_subtree` (`crates/snapstore-meta/src/lib.rs:322`) |
| Run GC | `snapstore_server::gc::run_gc_cycle` (`crates/snapstore-server/src/gc.rs:170`) |

Recommended population shape:

1. Generate a deterministic tree of 100,000 nodes. Branching factor 8 is close
   to the earlier plan language and gives shallow enough metadata operations.
2. Give each node a small full snapshot with unique pages. Choose pages per node
   so `nodes * pages_per_node * PAGE_SIZE` is about 30 GB. For 100k nodes,
   75 pages per node is about 28.6 GiB.
3. Stream pages; never hold 30 GB in memory. Build one node's pages, ingest,
   commit, create the node, then drop buffers.
4. Prune deterministic subtrees until the model predicts about 50% of physical
   page bytes are garbage. Prefer pruning subtree roots whose page ownership is
   easy to account for. Record selected subtree roots and predicted garbage
   bytes in the JSON.
5. Run one preparatory GC cycle if needed to establish the tombstone grace
   horizon, then repopulate/prune for the counted run. The counted run must be
   the one with ~30 GB physical and ~50% garbage. If grace-cycle behavior
   requires two cycles to reclaim, record both and use the reclaiming cycle for
   the `<60s` bar only when that matches production config; otherwise state the
   config used.

For deterministic page generation, reuse the style from
`snapstore-testgen::SyntheticGuest` but generate per node/page from `(seed,
node_id, page_index)` so the harness can stream.

## Idle Commit-Latency Baseline

Before running GC, measure idle commit latency with the same concurrent
committer workload used during GC:

| Parameter | Value |
|---|---:|
| workers | enough to sustain 200 MB/s with headroom; start at 4 and scale |
| commit payload | 8 MiB deltas or full snapshots, matching the M5 16-client row where practical |
| duration | at least 60 seconds |
| rate target | 200 MB/s aggregate payload |

Record every commit latency in milliseconds. Compute p50, p95, p99, aggregate
MB/s, and error count. This idle p99 is the denominator for the `2x` bar.

## Concurrent GC Run

Run the same committer workload while invoking:

```rust
run_gc_cycle(
    &store,
    &meta,
    &GcOpts {
        compact_threshold: 0.5,
        rotate_active_first: true,
        tombstone_grace_cycles: <production default unless documented>,
    },
    &GcHooks::none(),
)
```

Use production defaults unless a benchmark-specific option is required to make
the BM condition meaningful. Any deviation must appear in `results.json`,
`docs/bench-baseline.md`, and the request resolution.

The committer should be rate-limited, not best-effort. A token-bucket loop is
good enough: allow `SNAPSTORE_GC_INGEST_MBPS * elapsed` bytes, sleep when ahead,
and record actual achieved throughput. If actual throughput is below 200 MB/s,
the GC benchmark did not pass even if GC itself finished quickly.

Measure:

| Metric | Bar |
|---|---:|
| `gc_duration_ms` | `< 60000` |
| `gc_pages_reclaimed` / `gc_bytes_reclaimed` | close to predicted 50% garbage |
| `ingest_during_gc_mbps` | `>= 200` |
| `commit_idle_p99_ms` | recorded |
| `commit_during_gc_p99_ms` | `< 2 * commit_idle_p99_ms` |
| `commit_errors` | `0` unless explained by legal missing-page retry behavior |

## JSON Output

Suggested schema:

```json
{
  "config": {
    "nodes": 100000,
    "target_physical_gb": 30,
    "target_garbage_fraction": 0.5,
    "ingest_target_mbps": 200,
    "seed": 20260708
  },
  "population": {
    "nodes_created": 100000,
    "physical_page_bytes_before_gc": 0,
    "predicted_garbage_bytes": 0,
    "pruned_subtrees": []
  },
  "idle_commit": {
    "duration_s": 0.0,
    "throughput_mbps": 0.0,
    "p50_ms": 0.0,
    "p95_ms": 0.0,
    "p99_ms": 0.0
  },
  "gc_run": {
    "duration_ms": 0,
    "pages_reclaimed": 0,
    "bytes_reclaimed": 0,
    "packs_compacted": 0,
    "packs_deleted": 0,
    "ingest_mbps": 0.0,
    "commit_p99_ms": 0.0
  },
  "pass": {
    "gc_under_60s": false,
    "ingest_at_200_mbps": false,
    "p99_under_2x_idle": false
  }
}
```

Keep raw latency samples in `m7-gc-benchmark/commit-latencies.csv`.

## Run Command

```bash
SNAPSTORE_BENCH_ROOT=/mnt/phase5-scratch \
SNAPSTORE_GC_BENCH_JSON="$EVIDENCE_ROOT/m7-gc-benchmark/results.json" \
cargo test -p snapstore-server --test gc_readiness_bench --release -- --ignored --nocapture \
  2>&1 | tee "$EVIDENCE_ROOT/m7-gc-benchmark/gc_readiness_bench.log"
```

For development, require scaled-down smoke runs:

```bash
SNAPSTORE_BENCH_ROOT=/mnt/phase5-scratch \
SNAPSTORE_GC_NODES=1000 SNAPSTORE_GC_PHYSICAL_GB=1 SNAPSTORE_GC_INGEST_MBPS=50 \
cargo test -p snapstore-server --test gc_readiness_bench --release -- --ignored --nocapture
```

## Failure Analysis

If the full bar misses, collect and report:

| Question | Evidence |
|---|---|
| Did GC finish but ingest fall below 200 MB/s? | committer throughput, CPU and iostat logs |
| Did p99 exceed 2x idle because of GC gate pauses? | latency samples aligned to GC phases, `gc_running`, trace logs |
| Did GC exceed 60s because of pack compaction I/O? | bytes copied, packs compacted, disk util |
| Did CPU/memory saturate? | `perf stat`, `pidstat`, stream scaling comparison |
| Did garbage fraction differ from target? | predicted vs reclaimed bytes |

The resolution must translate a miss into a Phase 5 soak posture, for example:
"GC keeps pace up to N MB/s sustained ingest on host H; above that, expansion
throttling is required."
