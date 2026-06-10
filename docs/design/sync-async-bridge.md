# Sync↔async bridge — design note (decided before 02 WI2)

The workspace below the server is fully synchronous (pagestore, store
façade, meta actor); tonic is async. Every M4/M5 benchmark flows through
this seam, so the plan (02 WI2, 00-overview risk 8) requires these four
decisions to be written down before server work starts.

## (a) Handler → meta-actor boundary

The meta actor's ingress **stays a bounded crossbeam channel** and the
crate stays tokio-free. tonic handlers reach it through
`tokio::task::spawn_blocking(move || meta.create_node(...))` — the sync
facade call (send + block on reply) happens on a blocking-pool thread,
never on a runtime worker.

Rationale: the actor already batches up to 256 commands per txn, so the
per-call cost is one channel round-trip; a tokio-mpsc ingress would force
tokio into `snapstore-meta` and break the requirement that [04]'s harness
and the test suite drive it without a runtime. The blocking pool default
cap (512) is far above the 16-worker design point; a parked blocking
thread per in-flight meta call is bounded and cheap.

## (b) `spawn_blocking` vs dedicated threads for ingest

Per-message `spawn_blocking` cannot sustain 600 MB/s PutPages (a pool
hop + rayon warm-up per 256-page message). Each `PutPages` stream gets
**one long-lived blocking task** for its whole lifetime: the async
handler loop reads stream messages and feeds them over a bounded
`std::sync::mpsc::sync_channel` (capacity 4 messages ≈ 4 MiB) into that
task, which loops `PageStore::ingest(batch)` and accumulates
new/deduped counts and the running batch-hash state. Backpressure is the
bounded channel; the gRPC flow-control window does the rest. The same
pattern serves `ResolvePages` in the other direction (blocking producer,
async sender).

## (c) PutSnapshot's group-commit wait

The group-commit barrier (01 WI3) is a sync condvar/`Mutex` structure
inside `snapstore-store` (waiters park until the flush covering their
ingest-seq completes). The server wraps the **whole** `put_snapshot`
call — validation, barrier wait, manifest write — in one
`spawn_blocking`. No runtime thread is pinned; 16 concurrent commits =
at most 16 parked blocking-pool threads, and all of them are released by
one fdatasync pass. We do *not* build an async-native barrier now: it
would put tokio types into the store crate for zero measured benefit at
the design concurrency. Revisit only if blocking-pool saturation shows
up in the S4 16-client benchmark.

## (d) Client API shape

`snapstore-client` is **async-first with a blocking facade**
(`snapstore_client::blocking::SnapstoreClient`), because KVM vCPU worker
loops aren't tokio-native and INTEGRATION names the hypervisor as a
direct consumer. The blocking facade owns a small
`tokio::runtime::Runtime` (current_thread) and wraps each async method
in `block_on`; it is a thin mechanical layer, no duplicated logic.
Hypervisor M4 integrates on the blocking facade; orchestrator-side
(already async) uses the async client directly.

## Channel-type summary

| Seam | Type | Bound |
|---|---|---|
| tonic handler → meta actor | crossbeam bounded (existing) via `spawn_blocking` | 1024 cmds |
| PutPages stream → ingest task | `std::sync::mpsc::sync_channel` | 4 messages |
| ingest task / put_snapshot → handler | return value of the blocking task | — |
| group-commit waiters | condvar inside store façade | — |
| page-channel (03) connections | one blocking task per connection, same pattern as (b) | `ingest_queue_pages` |
