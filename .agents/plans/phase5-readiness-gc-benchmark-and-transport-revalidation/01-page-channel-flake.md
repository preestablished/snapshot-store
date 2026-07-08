# WI1 - Fix or Root-Cause `snapstore-nn4`

The flaky test is `cargo test -p snapstore-client --test page_channel_fallback`.
The bead says the failure rate is roughly 30-50% and points at
metrics-count assertions racing the fallback path.

## Likely Root Cause

The fallback tests use server-side `snapstore_page_channel_batches_total{op="get"}`
as proof that the client used GET_BATCH. Several assertions read the counter
immediately after the client receives a page-channel response:

| Assertion | File anchor |
|---|---|
| live resolve uses GET_BATCH | `crates/snapstore-client/tests/page_channel_fallback.rs:260` |
| Mode B resolve uses GET_BATCH | `crates/snapstore-client/tests/page_channel_fallback.rs:330` |
| duplicate hashes use GET_BATCH | `crates/snapstore-client/tests/page_channel_fallback.rs:357` |
| blocking client uses GET_BATCH | `crates/snapstore-client/tests/page_channel_fallback.rs:460` |

On the server side the GET_BATCH metric is incremented after
`send_datagram(... GET_BATCH_DATA ...)` returns
(`crates/snapstore-server/src/page_channel.rs:583`). A client can receive the
datagram and return before the page-channel handler has scheduled far enough to
increment the metric. That makes raw `get_batches() > before` assertions an
observability race even when the fast path worked.

## Implementation

1. Reproduce before editing and capture the failure mode:

   ```bash
   mkdir -p target/phase5-readiness-flake-repro
   for i in $(seq 1 20); do
     cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1 --nocapture \
       2>&1 | tee "target/phase5-readiness-flake-repro/run-${i}.log" || exit 1
   done
   ```

   If a failure is not a metric-delta assertion, stop and update this work item
   with the actual failure before fixing.

2. Replace raw positive metric assertions with a polling helper in
   `crates/snapstore-client/tests/page_channel_fallback.rs`:

   ```rust
   async fn wait_for_get_batches_gt(metrics: &Metrics, before: f64) {
       let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
       loop {
           if get_batches(metrics) > before {
               return;
           }
           assert!(tokio::time::Instant::now() < deadline, "GET_BATCH metric did not increment");
           tokio::time::sleep(std::time::Duration::from_millis(10)).await;
       }
   }
   ```

   Use it for the four positive fast-path assertions. Keep negative assertions
   (`hashes_only`, empty Mode B, invalid baseline) as exact zero-delta checks,
   but make them check after the operation has completed and after any owned
   client/fake server handles have been dropped.

3. Keep the existing global test guard. Do not remove it; it already serializes
   tests that bind UDS/SEQPACKET sockets and read shared process metrics.

4. If the repro shows a real fallback-path bug instead of metric scheduling:
   file a P0 bead, add a minimized failing test that does not depend on metrics,
   and fix the transport path before any benchmark work. A real race here makes
   WI3 and WI4 numbers untrustworthy.

## Verification

Required commands:

```bash
cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1
for i in $(seq 1 50); do
  cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1 >/tmp/page-channel-fallback-${i}.log 2>&1 || {
    cat /tmp/page-channel-fallback-${i}.log
    exit 1
  }
done
```

Also run the normal client crate tests:

```bash
cargo test -p snapstore-client
```

Evidence to copy under `target/phase5-readiness-<UTC>/flake/`:

| File | Contents |
|---|---|
| `repro-summary.txt` | Pre-fix run count, failure count, first failing assertion |
| `postfix-50x.log` | The 50-run loop output or a compact summary with command line |
| `root-cause.txt` | One paragraph: test-harness metric race vs real transport race |

Close `snapstore-nn4` only after the 50-run loop is green and the close reason
names the root cause.
