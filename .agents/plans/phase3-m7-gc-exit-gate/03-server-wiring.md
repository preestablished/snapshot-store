# WI3 — Server Wiring

## 1. Proto (vendored canonical copy, `proto/snapshot_store.proto`)

Both messages are empty today (last lines of the file). Additive only;
the standing rule routes schema through control-plane, but our copy is
canonical until `adopt-snapstore-proto-v1` (`snapstore-8qx`) — mirror
these fields there when that adoption lands (note it in 04-resolution.md).

```proto
message TriggerGcRequest {
  bool compact_aggressively = 1;  // threshold 0.9 + rotate active pack first
  bool detach = 2;                // true: fire-and-forget (poll Stats); false (default): run to completion
}
message TriggerGcResponse {
  bool started = 1;               // false only when already_running
  bool already_running = 2;       // R4 latch was held
  // Populated only when detach = false:
  uint64 nodes_reaped = 3;
  uint64 manifests_deleted = 4;
  uint64 pages_reclaimed = 5;
  uint64 bytes_reclaimed = 6;
  uint64 packs_compacted = 7;
  uint64 duration_ms = 8;
}
```

`StoreStats` additions (existing: `gc_runs_total = 11`,
`gc_pages_reclaimed_total = 12`, hardcoded 0 at service.rs:1023-1024):

```proto
  uint64 gc_bytes_reclaimed_total = 13;
  uint64 gc_last_finished_logical_counter = 14;  // 0 = never
```

Codegen: nothing to do beyond editing the file — both `snapstore-server`
and `snapstore-client` build.rs regenerate; the re-export seam is
`pub mod snapstore_proto` (server lib.rs:10-12, client lib.rs:13).

## 2. TriggerGc handler (service.rs:1030-1037 replaces the stub)

- Build `GcOpts` from config + `compact_aggressively`.
- `detach=false` (default): `spawn_blocking(run_gc_cycle)` and await;
  map `GcError::AlreadyRunning` → `started=false, already_running=true`
  (NOT a gRPC error — the caller polls Stats; matches API.md's
  `started` semantics). Fill counts from `GcReport`.
- `detach=true`: spawn the same on a background task, return
  `started=true` immediately.
- The R4 latch is `SnapshotStore::begin_gc_epoch` itself — no separate
  server-side flag (avoids two sources of truth). Auto-trigger and RPC
  contend on the same latch.

Stats handler: replace the two hardcoded zeros with `meta.gc_state()`
values + new fields 13/14. Update the existing UNIMPLEMENTED test at
`tests/server.rs:788-795` to the new contract (first real assertion, as
the request notes).

## 3. Metrics (server/src/metrics.rs)

Follow the existing `register_*_with_registry!` pattern; pre-initialize
label combos:

- `snapstore_gc_cycles_total` (IntCounter)
- `snapstore_gc_pages_reclaimed_total`, `snapstore_gc_bytes_reclaimed_total` (IntCounters)
- `snapstore_gc_manifests_deleted_total`, `snapstore_gc_nodes_reaped_total`, `snapstore_gc_packs_compacted_total` (IntCounters)
- `snapstore_gc_running` (IntGauge, 0/1)
- `snapstore_gc_cycle_seconds` (Histogram)

Basic counters are gate-adjacent ("the tests will want them" — request 02);
dashboards are Phase 6. `serve_for_tests_with_metrics`
(build_server.rs:249) lets tests assert them directly.

## 4. Config (server/src/config.rs — structs are deny_unknown_fields)

```toml
[gc]
auto = false                # watermark auto-trigger; default OFF in M7
                            # (deployed-instance caution: flipping it on is
                            # an operator decision at upgrade time)
trigger_disk_pct = 80
check_interval_secs = 60
compact_threshold = 0.5
tombstone_grace_cycles = 1
```

`GcConfig` with serde defaults mirroring the above; absent section =
defaults (matches `load_config` behavior).

## 5. Watermark auto-trigger (basic, in scope; polish deferred)

First background task in the server: in `build_server.rs`, when
`gc.auto`, spawn a tokio task: every `check_interval_secs`, `statvfs`
(via `nix`, already a dependency) on `data_root`; if used% >=
`trigger_disk_pct`, run a cycle through the same latch (skip silently if
already running). Log start/finish at info. Shutdown: select on the
existing `shutdown_tx` oneshot so `ServerHandle::shutdown` stays clean.
No 95% hard-refusal work here — that is M9 (`snapstore-agz`).

## 6. CLI + client

- `snapstore-client`: `trigger_gc` (client.rs:781, blocking.rs:176)
  gains the request fields and returns the response struct. Keep it
  **outside** the `with_retry` wrapper: blind-retrying a GC trigger on a
  dropped connection could double-run cycles back-to-back; the caller
  decides.
- `snapstore-cli` `Gc` subcommand (main.rs:338-341 stub): flags
  `--aggressive`, `--detach`; prints the report counts; exit 0 on
  success, 2 when `already_running` (script-friendly distinct code).
  Update the CLI test that expects nonzero exit (cli tests:258-262).

## 7. Server-side race-B calls (from 02 §1.2)

- `create_node` handler: call `store.note_live_ref(&snapshot_ref)`
  **before** the `has_manifest` validation (service.rs:502).
- `pin` handler: same, before `meta.pin`.
Document both call sites with a comment pointing at
`.agents/plans/phase3-m7-gc-exit-gate/02-gc-engine.md §1`.
