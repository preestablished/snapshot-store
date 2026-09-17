# READY root pin and identity

The hypervisor handoff takes the READY snapshot but creates neither a node nor
a pin for it. GC roots are live nodes plus pins, so without a pin the first GC
cycle deletes the READY manifest and its pages. The pin is made from this
repo's CLI immediately after every handoff; a node is *not* created here
(`CreateNode(root, node_id = 0)` is the orchestrator's bootstrap step and
would collide).

Prerequisite: the server from `docs/ops/intel-box-deploy.md` is running with
`[gc] auto = false`, and the handoff env is sourced.

## Inputs

`SNAPSTORE_GRPC_UDS_PATH`, `BRIDGE_REAL_SNAPSHOT_REF` (64 hex),
`BRIDGE_WORKLOAD_IMAGE_REF` (handoff env), and `REFWORK_EMU_VERSION` (the
emulator version string from the workload bundle manifest).
`deploy/intel-box/pin-ready-root.sh` refuses empty values and literal
`<placeholder>` values.

## Conventions

Pin note:

```
ready-root image=<BRIDGE_WORKLOAD_IMAGE_REF> emu=<REFWORK_EMU_VERSION> date=<YYYY-MM-DD> by=snapstore-plan-C
```

Root `NodeMeta.attrs` is owned by the orchestrator (its versioned `ORCHNA1`
envelope carries `workload_image_ref`, `emu_version`, `worker_image_identity`,
`adopted_from`, and `machine_config_hash`; the snapshot ref is
`NodeMeta.snapshot_ref`). The store stays image-agnostic: `attrs` is opaque
bytes and this procedure defines no separate map.

## Procedure

1. Pin (idempotent). There is no `ListPins` RPC; verify through `pins_total`:

   ```bash
   ctl() { target/release/snapstorectl --endpoint "uds:$SNAPSTORE_GRPC_UDS_PATH" "$@"; }
   ctl --json stats | jq .store.pins_total        # before
   deploy/intel-box/pin-ready-root.sh              # newly_pinned=true, +1
   deploy/intel-box/pin-ready-root.sh              # newly_pinned=false, unchanged
   ```

2. GC-safety check (stop/go gate), with `gc.auto = false`:

   ```bash
   curl -fsS http://127.0.0.1:7411/metrics | grep '^snapstore_gc_manifests_deleted_total'
   ctl gc --aggressive
   ctl dump-manifest "$BRIDGE_REAL_SNAPSHOT_REF"   # must exit 0
   curl -fsS http://127.0.0.1:7411/metrics | grep '^snapstore_gc_manifests_deleted_total'
   ```

   The manifest must be FULL (`delta = false`) with the demo guest's
   `guest_ram_bytes`. The deleted count may rise for unrelated garbage; the
   READY manifest must not be among it. **If `dump-manifest` fails: stop.**
   Recovery is a fresh handoff run; file a P0 issue.

3. Only after step 2 passes may `[gc] auto = true` be set in the overlay
   (restart, then re-run the deploy doc's health step).

4. Identity record (private, beside the handoff evidence): ref, image ref, emu
   version, pin date, `newly_pinned`. Committed documents show at most the
   first 8 hex chars of any ref.

## Rules

- This procedure never unpins. After a new handoff the old pin stays until the
  operator unpins it explicitly (`snapstorectl unpin <ref>`); a stale READY
  root costs one guest image of pages.
- The property relied on here is covered in CI by
  `pinned_orphan_snapshot_survives_gc` (`crates/snapstore-server/tests/server.rs`).
