# Intel-box deployment of `snapstore-server`

**GC rule (read first):** the READY root snapshot written by the hypervisor
handoff is protected from GC only by a pin. Do not run `snapstorectl gc`, and
do not set `[gc] auto = true`, until `docs/ops/ready-root-pin.md` has pinned
the ref and its GC-safety check has passed. The template ships `auto = false`.

This runbook brings up a long-lived, release-build `snapstore-server` over the
data root that `dh-m9-ready-handoff` (determinism-hypervisor) created. Paths
are named only by the env vars of the handoff env file
(`bridge-real-restore-snapshot.env`); never echo their values into committed
files or shared logs. Debug builds are not supported for this deployment
(measured multi-x slowdown) — release only.

## Inputs

From the handoff env file: `SNAPSTORE_DATA_ROOT`, `SNAPSTORE_CONFIG_PATH`
(the handoff-written config — never edited), `SNAPSTORE_GRPC_UDS_PATH`,
`BRIDGE_REAL_SNAPSHOT_REF`. `BRIDGE_ENV` below is the path of that file.

## Steps

1. Source the handoff env:

   ```bash
   set -a; . "$BRIDGE_ENV"; set +a
   ```

2. Write the overlay as a sibling of the handoff config's data root (never the
   handoff file itself — the handoff regenerates it on every run):

   ```bash
   export SNAPSTORE_CONFIG_OVERLAY="$(dirname "$SNAPSTORE_DATA_ROOT")/config.overlay.toml"
   sed -e "s|<SNAPSTORE_DATA_ROOT>|$SNAPSTORE_DATA_ROOT|g" \
       -e "s|<SNAPSTORE_GRPC_UDS_PATH>|$SNAPSTORE_GRPC_UDS_PATH|g" \
       deploy/intel-box/config.toml.example > "$SNAPSTORE_CONFIG_OVERLAY"
   grep -q '^SNAPSTORE_CONFIG_OVERLAY=' "$BRIDGE_ENV" ||
     printf 'SNAPSTORE_CONFIG_OVERLAY=%s\n' "$SNAPSTORE_CONFIG_OVERLAY" >> "$BRIDGE_ENV"
   ```

3. Build: `cargo build --release -p snapstore-server -p snapstore-cli`.

4. Start under supervision. Exactly one `snapstore-server` may run on a data
   root: a second one silently takes over the UDS and cleans `store/tmp/`.
   Stop any server already serving this root first (step 8a).

   - systemd: install `deploy/intel-box/snapstore-server.service.example`
     (fill in `<checkout>` and `<handoff-env>`), then
     `systemctl --user enable --now snapstore-server`.
   - fallback (no usable systemd scope):

     ```bash
     nohup setsid target/release/snapstore-server --config "$SNAPSTORE_CONFIG_OVERLAY" \
       >> "$SNAPSTORE_SERVER_LOG" 2>&1 < /dev/null &
     echo $! > "$SNAPSTORE_SERVER_PIDFILE"
     ```

     with the log and pid file in the private runtime directory beside the UDS.

   The log must show `snapstore-server starting` with the overlay's
   `data_root` and must not show a `STORE_VERSION` mismatch (that means the
   wrong data root).

5. Health:

   ```bash
   curl -fsS http://127.0.0.1:7411/healthz
   curl -fsS http://127.0.0.1:7411/metrics | grep -c '^snapstore_'      # > 0
   target/release/snapstorectl --endpoint "uds:$SNAPSTORE_GRPC_UDS_PATH" stats
   ls -l "$SNAPSTORE_DATA_ROOT/pages.sock"                               # page channel bound
   ```

6. Restart drill: stop, start, repeat step 5, then

   ```bash
   target/release/snapstorectl --endpoint "uds:$SNAPSTORE_GRPC_UDS_PATH" \
     dump-manifest "$BRIDGE_REAL_SNAPSHOT_REF" > /dev/null
   ```

   must exit 0 (the READY manifest reopened). `NotFound` here means the overlay
   points at a different data root than the handoff.

7. Handoff record (private, in the handoff evidence directory; hashes and
   counts only). Consumers read these names:

   | Name | Value |
   |---|---|
   | `SNAPSTORE_GRPC_UDS_PATH` | from the handoff env (worker: `dh-workerd serve --snapstore-uds`) |
   | `SNAPSTORE_HTTP_ADDR` | `127.0.0.1:7411` |
   | `SNAPSTORE_PAGE_CHANNEL_PATH` | `$SNAPSTORE_DATA_ROOT/pages.sock` |
   | `SNAPSTORE_CONFIG_OVERLAY` | overlay path from step 2 |

   plus the server build SHA (`git rev-parse HEAD`), the `STORE_VERSION`
   contents, and the `snapstorectl stats` output. A co-located orchestrator may
   use `tcp:127.0.0.1:7410`. All ports are loopback-only; there is no TLS or
   remote exposure in this deployment.

8. Re-apply after every new `dh-m9-ready-handoff` run — in this order, because
   the handoff starts its own ephemeral server on the same data root and
   refuses a live socket:

   a. stop the supervised server (SIGTERM) and confirm the **process** has
      exited, not just that a signal was sent: graceful shutdown can wait
      indefinitely on a connected client (`dh-workerd` holds the UDS open).
      If it has not exited after ~30 s and the stack is idle, SIGKILL it —
      the store is crash-safe — and only then start a new server;
   b. run the handoff (hypervisor runbook); it must not leave its own
      long-lived server running;
   c. repeat steps 1–2 (env values may have changed);
   d. start the server (steps 4–5);
   e. re-pin the new READY ref and re-run the GC-safety check
      (`docs/ops/ready-root-pin.md`).
