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
| `SNAPSTORE_GC_MODE` | Counted GC mode | `trigger_gc_aggressive` |

Use `tempfile::Builder::tempdir_in(SNAPSTORE_BENCH_ROOT)`. The test should
print enough progress that a long lab run does not look hung.

## Data Model

Use in-process APIs so the benchmark measures the GC/manifest/page/meta paths
without gRPC noise. Split API usage by phase:

Population, before any GC is running:

| Operation | API |
|---|---|
| Page ingest | `SnapshotStore::pages().ingest(...)` |
| Manifest commit | `SnapshotStore::put_snapshot(...)` |
| Build full/delta containers | `snapstore_store::build` (`crates/snapstore-store/src/lib.rs:1026`) |
| Create node rows | `MetaDb::create_node` (`crates/snapstore-meta/src/lib.rs:190`) |
| Prune subtrees | `MetaDb::prune_subtree` (`crates/snapstore-meta/src/lib.rs:322`) |
| Run GC | `snapstore_server::gc::run_gc_cycle` (`crates/snapstore-server/src/gc.rs:170`) |

Concurrent commits during GC:

| Operation | Rule |
|---|---|
| Commit manifest | `SnapshotStore::put_snapshot(...)` is safe; it holds the commit gate and registers the live ref |
| Attach committed ref to meta | Prefer the server/gRPC CreateNode path, or exactly mirror the server sequence: hold `store.commit_gate()`, call `store.register_live_ref(...)`, then `meta.create_node(...)` |
| Avoid | Direct `MetaDb::create_node` during GC without the gate/register step; it can create a node for a ref the manifest sweep is allowed to delete |

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
5. With production `tombstone_grace_cycles = 1`, use this exact sequence:
   populate -> prune -> uncounted grace/fence GC cycle -> counted reclaiming GC
   cycle. Do not repopulate or reprune between the grace cycle and the counted
   cycle. Record both durations and reports. The `<60s` bar applies only to the
   counted cycle that has `nodes_reaped > 0` and reclaimed bytes near the
   predicted garbage. If a different tombstone grace is used, mark it as a
   benchmark-specific deviation in JSON, `docs/bench-baseline.md`, and the
   resolution.

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

The counted mode is `TriggerGc { compact_aggressively: true }` semantics, run
in-process to avoid gRPC noise:

```rust
run_gc_cycle(
    &store,
    &meta,
    &GcOpts {
        compact_threshold: 0.9,
        rotate_active_first: true,
        tombstone_grace_cycles: 1,
    },
    &GcHooks::none(),
)
```

This matches the RPC's aggressive path in `TriggerGc` and avoids the
`compact_threshold = 0.5` edge where exactly 50% live packs can be skipped
because sweep compacts only when `liveness < compact_threshold`. If the agent
also wants to record default-GC behavior (`compact_threshold = 0.5`,
`rotate_active_first = false`), do it as an informational row; it does not
replace the aggressive counted bar.

The committer should be rate-limited, not best-effort. A token-bucket loop is
good enough: allow `SNAPSTORE_GC_INGEST_MBPS * elapsed` bytes, sleep when ahead,
and record actual achieved throughput. If actual throughput is below 200 MB/s,
the GC benchmark did not pass even if GC itself finished quickly.

Run GC and committer loops on blocking execution contexts: either plain
`std::thread` workers or `tokio::task::spawn_blocking`. Do not run synchronous
`run_gc_cycle` or `put_snapshot` loops on core Tokio runtime threads. Use a
barrier so commit-latency sampling starts before the counted reclaiming cycle
enters GC and continues until it completes; label samples as `before_gc`,
`during_grace_gc`, `during_reclaiming_gc`, or `after_gc`.

Measure:

| Metric | Bar |
|---|---:|
| `gc_mode` | `trigger_gc_aggressive` with exact `GcOpts` recorded |
| `grace_cycle_duration_ms` | recorded, not the pass/fail cycle |
| `reclaiming_gc_duration_ms` | `< 60000` |
| `nodes_reaped` | `> 0` in the counted reclaiming cycle |
| `gc_pages_reclaimed` / `gc_bytes_reclaimed` | close to predicted 50% garbage |
| `ingest_during_gc_mbps` | `>= 200` |
| `commit_idle_p99_ms` | recorded |
| `commit_during_reclaiming_gc_p99_ms` | `< 2 * commit_idle_p99_ms` |
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
    "seed": 20260708,
    "gc_mode": "trigger_gc_aggressive",
    "gc_opts": {
      "compact_threshold": 0.9,
      "rotate_active_first": true,
      "tombstone_grace_cycles": 1
    }
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
  "grace_gc_run": {
    "duration_ms": 0,
    "nodes_reaped": 0,
    "pages_reclaimed": 0,
    "bytes_reclaimed": 0
  },
  "reclaiming_gc_run": {
    "duration_ms": 0,
    "nodes_reaped": 0,
    "pages_reclaimed": 0,
    "bytes_reclaimed": 0,
    "packs_compacted": 0,
    "packs_deleted": 0,
    "ingest_mbps": 0.0,
    "commit_p99_ms": 0.0
  },
  "pass": {
    "gc_under_60s": false,
    "reclaiming_cycle_reaped_nodes": false,
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
