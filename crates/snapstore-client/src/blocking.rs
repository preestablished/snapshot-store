//! Blocking facade over the async `SnapstoreClient`.
//!
//! Owns a `current_thread` tokio `Runtime` and delegates each method to the
//! async client via `block_on`.  This is a thin, mechanical wrapper — no logic
//! lives here; all error handling and retry live in the async layer.
//!
//! Intended for KVM vCPU worker loops that are not tokio-native
//! (sync-async bridge design note, decision d).

use snapstore_manifest::DeviceBlob;
use snapstore_types::{LogId, PageHash, SnapshotRef};

use crate::{
    client::SnapstoreClient as AsyncClient,
    error::ClientResult,
    snapstore_proto::{
        CreateNodeRequest, NodeMeta, NodeUpdate, PathElement, QueryNodesRequest, StatsResponse,
    },
    transport::Transport,
};

/// Blocking client for `determinism.snapstore.v1.SnapshotStore`.
///
/// Each method blocks the calling thread until the RPC completes.
pub struct SnapstoreClient {
    async_client: AsyncClient,
    rt: tokio::runtime::Runtime,
}

impl SnapstoreClient {
    /// Connect using the given `Transport` configuration.
    pub fn connect(transport: Transport) -> ClientResult<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| crate::error::ClientError::Transport(format!("runtime build: {e}")))?;
        let async_client = rt.block_on(AsyncClient::connect(transport))?;
        Ok(Self { async_client, rt })
    }

    // ── pages & snapshots ──────────────────────────────────────────────────

    pub fn put_pages(&self, pages: Vec<(u64, Vec<u8>)>) -> ClientResult<(u64, u64)> {
        self.rt.block_on(self.async_client.put_pages(pages))
    }

    pub fn put_snapshot(&self, container: Vec<u8>) -> ClientResult<SnapshotRef> {
        self.rt.block_on(self.async_client.put_snapshot(container))
    }

    pub fn get_snapshot(&self, snapshot_ref: SnapshotRef) -> ClientResult<Vec<u8>> {
        self.rt
            .block_on(self.async_client.get_snapshot(snapshot_ref))
    }

    pub fn resolve_pages(
        &self,
        snapshot_ref: SnapshotRef,
        baseline_ref: Option<SnapshotRef>,
        hashes_only: bool,
    ) -> ClientResult<Vec<(u64, PageHash, Option<bytes::Bytes>)>> {
        self.rt.block_on(
            self.async_client
                .resolve_pages(snapshot_ref, baseline_ref, hashes_only),
        )
    }

    pub fn has_pages(&self, page_hashes: Vec<PageHash>) -> ClientResult<Vec<bool>> {
        self.rt.block_on(self.async_client.has_pages(page_hashes))
    }

    // ── input logs ────────────────────────────────────────────────────────

    pub fn put_input_log(&self, container: Vec<u8>) -> ClientResult<(LogId, bool)> {
        self.rt.block_on(self.async_client.put_input_log(container))
    }

    pub fn get_input_log(&self, log_id: LogId) -> ClientResult<Vec<u8>> {
        self.rt.block_on(self.async_client.get_input_log(log_id))
    }

    // ── tree ──────────────────────────────────────────────────────────────

    pub fn create_node(&self, req: CreateNodeRequest) -> ClientResult<NodeMeta> {
        self.rt.block_on(self.async_client.create_node(req))
    }

    pub fn update_nodes(
        &self,
        experiment_id: String,
        updates: Vec<NodeUpdate>,
    ) -> ClientResult<u64> {
        self.rt
            .block_on(self.async_client.update_nodes(experiment_id, updates))
    }

    pub fn get_node(&self, experiment_id: String, node_id: u64) -> ClientResult<NodeMeta> {
        self.rt
            .block_on(self.async_client.get_node(experiment_id, node_id))
    }

    pub fn get_children(&self, experiment_id: String, node_id: u64) -> ClientResult<Vec<NodeMeta>> {
        self.rt
            .block_on(self.async_client.get_children(experiment_id, node_id))
    }

    pub fn get_path(
        &self,
        experiment_id: String,
        node_id: u64,
        include_logs: bool,
    ) -> ClientResult<Vec<PathElement>> {
        self.rt.block_on(
            self.async_client
                .get_path(experiment_id, node_id, include_logs),
        )
    }

    pub fn query_nodes(&self, req: QueryNodesRequest) -> ClientResult<Vec<NodeMeta>> {
        self.rt.block_on(self.async_client.query_nodes(req))
    }

    // ── metadata KV ──────────────────────────────────────────────────────

    pub fn put_metadata(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_generation: Option<u64>,
    ) -> ClientResult<u64> {
        self.rt.block_on(
            self.async_client
                .put_metadata(key, value, expected_generation),
        )
    }

    pub fn get_metadata(&self, key: Vec<u8>) -> ClientResult<(Vec<u8>, u64)> {
        self.rt.block_on(self.async_client.get_metadata(key))
    }

    pub fn delete_metadata(
        &self,
        key: Vec<u8>,
        expected_generation: Option<u64>,
    ) -> ClientResult<bool> {
        self.rt
            .block_on(self.async_client.delete_metadata(key, expected_generation))
    }

    // ── lifecycle ────────────────────────────────────────────────────────

    pub fn prune_subtree(
        &self,
        experiment_id: String,
        node_id: u64,
        allow_root: bool,
    ) -> ClientResult<u64> {
        self.rt.block_on(
            self.async_client
                .prune_subtree(experiment_id, node_id, allow_root),
        )
    }

    pub fn pin(&self, snapshot_ref: SnapshotRef, note: String) -> ClientResult<bool> {
        self.rt.block_on(self.async_client.pin(snapshot_ref, note))
    }

    pub fn unpin(&self, snapshot_ref: SnapshotRef) -> ClientResult<bool> {
        self.rt.block_on(self.async_client.unpin(snapshot_ref))
    }

    pub fn stats(&self, experiment_id: Option<String>) -> ClientResult<StatsResponse> {
        self.rt.block_on(self.async_client.stats(experiment_id))
    }

    pub fn trigger_gc(&self) -> ClientResult<()> {
        self.rt.block_on(self.async_client.trigger_gc())
    }

    // ── composite ────────────────────────────────────────────────────────

    pub fn put_snapshot_from_parts(
        &self,
        parent: Option<&SnapshotRef>,
        guest_ram_bytes: u64,
        pages: Vec<(u64, Vec<u8>)>,
        device_blob: DeviceBlob,
    ) -> ClientResult<SnapshotRef> {
        self.rt.block_on(self.async_client.put_snapshot_from_parts(
            parent,
            guest_ram_bytes,
            pages,
            device_blob,
        ))
    }
}
