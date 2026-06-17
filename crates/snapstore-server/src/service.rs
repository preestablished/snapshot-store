//! tonic service implementation for `determinism.snapstore.v1.SnapshotStore`.
//!
//! # Sync↔async bridge decisions (docs/design/sync-async-bridge.md)
//!
//! (a) Meta calls: `tokio::task::spawn_blocking` wrapping the synchronous
//!     facade call.  Never blocks a runtime worker.
//! (b) PutPages: one long-lived blocking task per stream, fed by a bounded
//!     `std::sync::mpsc::sync_channel(4)`.
//! (c) PutSnapshot: whole `put_snapshot` call (incl. group-commit wait) in
//!     one `spawn_blocking`.
//! (d) ResolvePages: blocking producer via `spawn_blocking`, async sender
//!     to the gRPC stream.

use std::pin::Pin;
use std::sync::{mpsc as std_mpsc, Arc};
use std::time::Instant;

use tokio::sync::mpsc as tokio_mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status, Streaming};

use snapstore_manifest::input_log::InputLogContainer;
use snapstore_meta::{CreateNodeParams, MetaDb, NodeUpdate, QueryFilter, QueryOrder};
use snapstore_store::SnapshotStore;
use snapstore_types::{ExperimentId, LogId, NodeId, NodeStatus, SnapshotRef, PAGE_SIZE};

use crate::errors::{meta_error_to_status, put_error_to_status};
use crate::metrics::Metrics;
use crate::snapstore_proto::snapshot_store_server::SnapshotStore as SnapshotStoreService;
use crate::snapstore_proto::*;

// ── NodeStatus mapping ────────────────────────────────────────────────────────
//
// snapstore-types NodeStatus u8: Frontier=0, Expanded=1, Pruned=2, Goal=3
// proto NodeStatus:            UNSPECIFIED=0, FRONTIER=1, EXPANDED=2, PRUNED=3, GOAL=4
//
// UNSPECIFIED in requests defaults to FRONTIER per spec.

fn proto_status_to_types(s: i32) -> snapstore_types::NodeStatus {
    match s {
        0 => NodeStatus::Frontier, // UNSPECIFIED → FRONTIER
        1 => NodeStatus::Frontier, // FRONTIER
        2 => NodeStatus::Expanded, // EXPANDED
        3 => NodeStatus::Pruned,   // PRUNED
        4 => NodeStatus::Goal,     // GOAL
        _ => NodeStatus::Frontier,
    }
}

fn types_status_to_proto(s: NodeStatus) -> i32 {
    match s {
        NodeStatus::Frontier => 1,
        NodeStatus::Expanded => 2,
        NodeStatus::Pruned => 3,
        NodeStatus::Goal => 4,
    }
}

fn node_row_to_proto(row: &snapstore_meta::NodeRow) -> NodeMeta {
    NodeMeta {
        experiment_id: row.experiment_id.as_str().to_owned(),
        node_id: row.node_id.0,
        parent_node_id: row.parent_node_id.map(|n| n.0),
        depth: row.depth as u64,
        snapshot_ref: row.snapshot_ref.to_bytes().to_vec(),
        input_log_id: row
            .input_log_id
            .map(|l| l.to_bytes().to_vec())
            .unwrap_or_default(),
        status: types_status_to_proto(row.status),
        score: row.score,
        visit_count: row.visit_count,
        icount: row.icount,
        virtual_ns: row.virtual_ns,
        created_at: row.created_at,
        updated_at: row.updated_at,
        last_visited_at: row.last_visited_at,
        attrs: row.attrs.clone().unwrap_or_default(),
    }
}

// ── validate helpers ──────────────────────────────────────────────────────────

#[allow(clippy::result_large_err)]
fn validate_hash32(bytes: &[u8], field: &str) -> Result<[u8; 32], Status> {
    if bytes.len() != 32 {
        return Err(Status::new(
            Code::InvalidArgument,
            format!("{field}: expected 32-byte hash, got {} bytes", bytes.len()),
        ));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(arr)
}

#[allow(clippy::result_large_err)]
fn validate_experiment_id(s: &str) -> Result<ExperimentId, Status> {
    ExperimentId::new(s)
        .map_err(|e| Status::new(Code::InvalidArgument, format!("experiment_id: {e}")))
}

// ── Service state ─────────────────────────────────────────────────────────────

/// Clone-safe server state passed to each tonic handler.
#[derive(Clone)]
pub struct SnapshotStoreServer {
    pub store: Arc<SnapshotStore>,
    pub meta: Arc<MetaDb>,
    pub metrics: Arc<Metrics>,
}

// ── tonic impl ────────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl SnapshotStoreService for SnapshotStoreServer {
    // ── PutPages (client-stream) ──────────────────────────────────────────────

    async fn put_pages(
        &self,
        request: Request<Streaming<PutPagesRequest>>,
    ) -> Result<Response<PutPagesResponse>, Status> {
        let mut stream = request.into_inner();
        let store = Arc::clone(&self.store);
        let metrics = Arc::clone(&self.metrics);

        // Bridge decision (b): one long-lived blocking task for the whole stream.
        // The async handler feeds a bounded sync channel; the task loops
        // PageStore::ingest per message, accumulating counts and the rolling hash.
        let (tx, rx) = std_mpsc::sync_channel::<Vec<Vec<u8>>>(4);

        // Spawn the ingest blocking task first.
        let ingest_handle = tokio::task::spawn_blocking(move || {
            let page_store = store.pages();
            let mut pages_new: u64 = 0;
            let mut pages_deduped: u64 = 0;
            let mut hasher = blake3::Hasher::new();

            while let Ok(batch) = rx.recv() {
                let batch: Vec<Vec<u8>> = batch;

                // Convert to fixed-size refs.
                let page_refs: Vec<&[u8; PAGE_SIZE]> = batch
                    .iter()
                    .map(|p| {
                        let arr: &[u8; PAGE_SIZE] = p.as_slice().try_into().unwrap();
                        arr
                    })
                    .collect();

                let outcomes = page_store
                    .ingest(&page_refs)
                    .map_err(|e| format!("ingest error: {e}"))?;

                for outcome in &outcomes {
                    let hash_bytes = outcome.hash.to_bytes();
                    hasher.update(&hash_bytes);
                    if outcome.newly_written {
                        pages_new += 1;
                        metrics.pages_ingested.with_label_values(&["new"]).inc();
                    } else {
                        pages_deduped += 1;
                        metrics.pages_ingested.with_label_values(&["dup"]).inc();
                    }
                }
            }

            let batch_hash = hasher.finalize();
            Ok::<_, String>((pages_new, pages_deduped, batch_hash.as_bytes().to_vec()))
        });

        // Async handler reads gRPC stream messages and feeds the sync channel.
        loop {
            let msg = stream
                .message()
                .await
                .map_err(|e| Status::new(Code::Internal, format!("stream read error: {e}")))?;
            let msg = match msg {
                None => break,
                Some(m) => m,
            };

            // Validate per-message constraints.
            if msg.pages.len() > 256 {
                // Drop the sender to signal abort, drain the task.
                drop(tx);
                let _ = ingest_handle.await;
                return Err(Status::new(
                    Code::InvalidArgument,
                    format!(
                        "PutPages: too many pages per message: {} (max 256)",
                        msg.pages.len()
                    ),
                ));
            }
            for (i, page) in msg.pages.iter().enumerate() {
                if page.len() != PAGE_SIZE {
                    drop(tx);
                    let _ = ingest_handle.await;
                    return Err(Status::new(
                        Code::InvalidArgument,
                        format!(
                            "PutPages: page[{i}] is {} bytes, expected {PAGE_SIZE}",
                            page.len()
                        ),
                    ));
                }
            }

            if tx.send(msg.pages).is_err() {
                // Ingest task died — the join will reveal the error.
                break;
            }
        }

        // Close the channel so the blocking task exits its loop.
        drop(tx);

        let result = ingest_handle
            .await
            .map_err(|e| Status::new(Code::Internal, format!("ingest task panicked: {e}")))?;

        let (pages_new, pages_deduped, batch_blake3) =
            result.map_err(|e| Status::new(Code::Internal, format!("ingest error: {e}")))?;

        Ok(Response::new(PutPagesResponse {
            pages_new,
            pages_deduped,
            batch_blake3,
        }))
    }

    // ── PutSnapshot ──────────────────────────────────────────────────────────

    async fn put_snapshot(
        &self,
        request: Request<PutSnapshotRequest>,
    ) -> Result<Response<PutSnapshotResponse>, Status> {
        let container = request.into_inner().container;
        let store = Arc::clone(&self.store);
        let metrics = Arc::clone(&self.metrics);

        // Bridge decision (c): whole put_snapshot (incl. group-commit) in one
        // spawn_blocking.
        let start = Instant::now();
        let result = tokio::task::spawn_blocking(move || store.put_snapshot(&container))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("put_snapshot task panicked: {e}")))?;

        metrics
            .commit_seconds
            .observe(start.elapsed().as_secs_f64());

        let snap_ref = result.map_err(put_error_to_status)?;
        Ok(Response::new(PutSnapshotResponse {
            snapshot_ref: snap_ref.to_bytes().to_vec(),
        }))
    }

    // ── GetSnapshot ──────────────────────────────────────────────────────────

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let r = request.into_inner();
        let ref_bytes = validate_hash32(&r.snapshot_ref, "snapshot_ref")?;
        let snap_ref = SnapshotRef::from_bytes(ref_bytes);
        let store = Arc::clone(&self.store);

        let container = tokio::task::spawn_blocking(move || store.get_snapshot(&snap_ref))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("get_snapshot panicked: {e}")))?
            .map_err(|e| match e {
                snapstore_store::StoreError::NotFound => {
                    Status::new(Code::NotFound, "snapshot not found")
                }
                other => Status::new(Code::Internal, format!("get_snapshot error: {other}")),
            })?;

        Ok(Response::new(GetSnapshotResponse { container }))
    }

    // ── ResolvePages (server-stream) ──────────────────────────────────────────

    type ResolvePagesStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<ResolvePagesResponse, Status>> + Send>>;

    async fn resolve_pages(
        &self,
        request: Request<ResolvePagesRequest>,
    ) -> Result<Response<Self::ResolvePagesStream>, Status> {
        let r = request.into_inner();
        let ref_bytes = validate_hash32(&r.snapshot_ref, "snapshot_ref")?;
        let snap_ref = SnapshotRef::from_bytes(ref_bytes);

        let baseline: Option<SnapshotRef> = if r.baseline_ref.is_empty() {
            None
        } else {
            let b = validate_hash32(&r.baseline_ref, "baseline_ref")?;
            Some(SnapshotRef::from_bytes(b))
        };

        let hashes_only = r.hashes_only;
        let store = Arc::clone(&self.store);
        let metrics = Arc::clone(&self.metrics);

        // Bridge decision (b) variant: blocking producer, async consumer.
        let (tx, rx) = tokio_mpsc::channel::<Result<ResolvePagesResponse, Status>>(8);

        let start = Instant::now();
        tokio::task::spawn_blocking(move || {
            let resolved = match store.resolve_pages(&snap_ref, baseline.as_ref(), hashes_only) {
                Ok(r) => r,
                Err(e) => {
                    let status = match e {
                        snapstore_store::StoreError::NotFound => {
                            Status::new(Code::NotFound, "snapshot not found")
                        }
                        snapstore_store::StoreError::BaselineNotAncestor => Status::new(
                            Code::InvalidArgument,
                            "baseline_ref is not in the parent chain",
                        ),
                        other => {
                            Status::new(Code::Internal, format!("resolve_pages error: {other}"))
                        }
                    };
                    let _ = tx.blocking_send(Err(status));
                    return;
                }
            };

            let mut batch: Vec<ResolvedPage> = Vec::with_capacity(512);
            for item in resolved {
                let (page_index, page_hash, payload) = match item {
                    Ok(i) => i,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(Status::new(
                            Code::Internal,
                            format!("resolve error: {e}"),
                        )));
                        return;
                    }
                };
                let proto_page = ResolvedPage {
                    page_index,
                    page_hash: page_hash.to_bytes().to_vec(),
                    payload: payload.map(|b| b.to_vec()).unwrap_or_default(),
                };
                batch.push(proto_page);
                if batch.len() >= 512 {
                    let msg = ResolvePagesResponse {
                        pages: std::mem::take(&mut batch),
                    };
                    if tx.blocking_send(Ok(msg)).is_err() {
                        return;
                    }
                }
            }
            if !batch.is_empty() {
                let _ = tx.blocking_send(Ok(ResolvePagesResponse { pages: batch }));
            }
            metrics
                .resolve_seconds
                .observe(start.elapsed().as_secs_f64());
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    // ── HasPages ─────────────────────────────────────────────────────────────

    async fn has_pages(
        &self,
        request: Request<HasPagesRequest>,
    ) -> Result<Response<HasPagesResponse>, Status> {
        let r = request.into_inner();
        if r.page_hashes.len() > 4096 {
            return Err(Status::new(
                Code::InvalidArgument,
                format!(
                    "HasPages: too many hashes: {} (max 4096)",
                    r.page_hashes.len()
                ),
            ));
        }

        #[allow(clippy::result_large_err)]
        fn parse_hash(h: &[u8]) -> Result<snapstore_types::PageHash, Status> {
            validate_hash32(h, "page_hashes[]").map(snapstore_types::PageHash::from_bytes)
        }
        #[allow(clippy::result_large_err)]
        let hashes_result: Result<Vec<snapstore_types::PageHash>, Status> =
            r.page_hashes.iter().map(|h| parse_hash(h)).collect();
        let hashes = hashes_result?;

        let store = Arc::clone(&self.store);
        let present = tokio::task::spawn_blocking(move || store.has_pages(&hashes))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("has_pages panicked: {e}")))?
            .map_err(|e| Status::new(Code::Internal, format!("has_pages error: {e}")))?;

        Ok(Response::new(HasPagesResponse { present }))
    }

    // ── PutInputLog ──────────────────────────────────────────────────────────

    async fn put_input_log(
        &self,
        request: Request<PutInputLogRequest>,
    ) -> Result<Response<PutInputLogResponse>, Status> {
        let container = request.into_inner().container;

        // Validate SILG container.
        InputLogContainer::decode(&container).map_err(|e| {
            Status::new(
                Code::InvalidArgument,
                format!("invalid SILG container: {e}"),
            )
        })?;

        let log_id =
            LogId::from_bytes(*blake3::hash(&container[..container.len() - 32]).as_bytes());

        let meta = Arc::clone(&self.meta);
        let log_id_clone = log_id;
        let newly_stored =
            tokio::task::spawn_blocking(move || meta.put_input_log(log_id_clone, &container))
                .await
                .map_err(|e| Status::new(Code::Internal, format!("put_input_log panicked: {e}")))?
                .map_err(meta_error_to_status)?;

        Ok(Response::new(PutInputLogResponse {
            log_id: log_id.to_bytes().to_vec(),
            newly_stored,
        }))
    }

    // ── GetInputLog ──────────────────────────────────────────────────────────

    async fn get_input_log(
        &self,
        request: Request<GetInputLogRequest>,
    ) -> Result<Response<GetInputLogResponse>, Status> {
        let r = request.into_inner();
        let id_bytes = validate_hash32(&r.log_id, "log_id")?;
        let log_id = LogId::from_bytes(id_bytes);
        let meta = Arc::clone(&self.meta);

        let maybe_bytes = tokio::task::spawn_blocking(move || meta.get_input_log(&log_id))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("get_input_log panicked: {e}")))?
            .map_err(meta_error_to_status)?;

        match maybe_bytes {
            None => Err(Status::new(Code::NotFound, "input log not found")),
            Some(container) => Ok(Response::new(GetInputLogResponse { container })),
        }
    }

    // ── CreateNode ────────────────────────────────────────────────────────────

    async fn create_node(
        &self,
        request: Request<CreateNodeRequest>,
    ) -> Result<Response<CreateNodeResponse>, Status> {
        let r = request.into_inner();

        let exp_id = validate_experiment_id(&r.experiment_id)?;
        let snap_ref_bytes = validate_hash32(&r.snapshot_ref, "snapshot_ref")?;
        let snap_ref = SnapshotRef::from_bytes(snap_ref_bytes);

        // Validate input_log_id if set.
        let input_log_id: Option<LogId> = if r.input_log_id.is_empty() {
            None
        } else {
            let b = validate_hash32(&r.input_log_id, "input_log_id")?;
            Some(LogId::from_bytes(b))
        };

        // Validate inline_input_log if set.
        let inline_log: Option<Vec<u8>> = if r.inline_input_log.is_empty() {
            None
        } else {
            InputLogContainer::decode(&r.inline_input_log).map_err(|e| {
                Status::new(
                    Code::InvalidArgument,
                    format!("invalid inline SILG container: {e}"),
                )
            })?;
            Some(r.inline_input_log)
        };

        // BEFORE dispatching to the meta actor: verify snapshot_ref resolves to
        // a stored manifest.  Manifests are immutable-once-present (content-
        // addressed write-once), so a true return is a permanent guarantee —
        // there is no TOCTOU race here.
        {
            let store = Arc::clone(&self.store);
            let snap_ref_check = snap_ref.clone();
            let has_it = tokio::task::spawn_blocking(move || store.has_manifest(&snap_ref_check))
                .await
                .map_err(|e| Status::new(Code::Internal, format!("has_manifest panicked: {e}")))?;
            if !has_it {
                return Err(Status::new(
                    Code::NotFound,
                    "snapshot_ref not found: PutSnapshot must precede CreateNode",
                ));
            }
        }

        let parent_node_id = r.parent_node_id.map(NodeId);
        let node_id = NodeId(r.node_id);
        let status = proto_status_to_types(r.status);

        let params = CreateNodeParams {
            experiment_id: exp_id,
            node_id,
            parent_node_id,
            snapshot_ref: snap_ref,
            input_log_id,
            inline_log_container: inline_log,
            status,
            score: r.score,
            icount: r.icount,
            virtual_ns: r.virtual_ns,
            attrs: if r.attrs.is_empty() {
                None
            } else {
                Some(r.attrs)
            },
        };

        let meta = Arc::clone(&self.meta);
        let start = Instant::now();
        let row = tokio::task::spawn_blocking(move || meta.create_node(params))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("create_node panicked: {e}")))?
            .map_err(meta_error_to_status)?;
        self.metrics
            .meta_txn_seconds
            .observe(start.elapsed().as_secs_f64());

        Ok(Response::new(CreateNodeResponse {
            node: Some(node_row_to_proto(&row)),
        }))
    }

    // ── UpdateNodes ───────────────────────────────────────────────────────────

    async fn update_nodes(
        &self,
        request: Request<UpdateNodesRequest>,
    ) -> Result<Response<UpdateNodesResponse>, Status> {
        let r = request.into_inner();
        let exp_id = validate_experiment_id(&r.experiment_id)?;

        let updates: Vec<NodeUpdate> = r
            .updates
            .iter()
            .map(|u| {
                let status = u.status.map(proto_status_to_types);
                NodeUpdate {
                    node_id: NodeId(u.node_id),
                    status,
                    score: u.score,
                    attrs: if u.attrs.as_ref().map(|a| a.is_empty()).unwrap_or(true) {
                        None
                    } else {
                        u.attrs.clone()
                    },
                    visit_count_delta: u.visit_count_delta.unwrap_or(0),
                    touch_visited: u.touch_visited,
                    icount: u.icount,
                    virtual_ns: u.virtual_ns,
                }
            })
            .collect();

        let meta = Arc::clone(&self.meta);
        let start = Instant::now();
        let updated_at = tokio::task::spawn_blocking(move || meta.update_nodes(exp_id, updates))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("update_nodes panicked: {e}")))?
            .map_err(meta_error_to_status)?;
        self.metrics
            .meta_txn_seconds
            .observe(start.elapsed().as_secs_f64());

        Ok(Response::new(UpdateNodesResponse { updated_at }))
    }

    // ── GetNode ───────────────────────────────────────────────────────────────

    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let r = request.into_inner();
        let exp_id = validate_experiment_id(&r.experiment_id)?;
        let node_id = NodeId(r.node_id);
        let meta = Arc::clone(&self.meta);

        let row = tokio::task::spawn_blocking(move || meta.get_node(&exp_id, node_id))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("get_node panicked: {e}")))?
            .map_err(meta_error_to_status)?;

        match row {
            None => Err(Status::new(Code::NotFound, "node not found")),
            Some(r) => Ok(Response::new(GetNodeResponse {
                node: Some(node_row_to_proto(&r)),
            })),
        }
    }

    // ── GetChildren ───────────────────────────────────────────────────────────

    async fn get_children(
        &self,
        request: Request<GetChildrenRequest>,
    ) -> Result<Response<GetChildrenResponse>, Status> {
        let r = request.into_inner();
        let exp_id = validate_experiment_id(&r.experiment_id)?;
        let node_id = NodeId(r.node_id);
        let meta = Arc::clone(&self.meta);

        let rows = tokio::task::spawn_blocking(move || meta.get_children(&exp_id, node_id))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("get_children panicked: {e}")))?
            .map_err(meta_error_to_status)?;

        Ok(Response::new(GetChildrenResponse {
            nodes: rows.iter().map(node_row_to_proto).collect(),
        }))
    }

    // ── GetPath ───────────────────────────────────────────────────────────────

    async fn get_path(
        &self,
        request: Request<GetPathRequest>,
    ) -> Result<Response<GetPathResponse>, Status> {
        let r = request.into_inner();
        let exp_id = validate_experiment_id(&r.experiment_id)?;
        let node_id = NodeId(r.node_id);
        let include_logs = r.include_logs;
        let meta = Arc::clone(&self.meta);

        let path_result =
            tokio::task::spawn_blocking(move || meta.get_path(&exp_id, node_id, include_logs))
                .await
                .map_err(|e| Status::new(Code::Internal, format!("get_path panicked: {e}")))?
                .map_err(meta_error_to_status)?;

        let elements = path_result
            .iter()
            .map(|(row, log_bytes)| PathElement {
                node: Some(node_row_to_proto(row)),
                input_log_container: log_bytes.clone().unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(GetPathResponse { elements }))
    }

    // ── QueryNodes (server-stream) ────────────────────────────────────────────

    type QueryNodesStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<QueryNodesResponse, Status>> + Send>>;

    async fn query_nodes(
        &self,
        request: Request<QueryNodesRequest>,
    ) -> Result<Response<Self::QueryNodesStream>, Status> {
        let r = request.into_inner();
        let exp_id = validate_experiment_id(&r.experiment_id)?;

        let status_filter = match r.status {
            Some(s) => {
                // 0 = UNSPECIFIED (omit filter), others map directly
                if s == 0 {
                    None
                } else {
                    Some(proto_status_to_types(s))
                }
            }
            None => None,
        };

        let order = match r.order {
            1 => QueryOrder::CreatedAt,
            2 => QueryOrder::UpdatedAt,
            3 => QueryOrder::NodeId,
            _ => QueryOrder::CreatedAt,
        };

        let _user_limit = if r.limit == 0 {
            512u32
        } else {
            r.limit.min(4096)
        };

        let meta = Arc::clone(&self.meta);
        let created_after = r.created_after;
        let updated_after = r.updated_after;
        let parent_filter = r.parent_node_id.map(NodeId);

        let (tx, rx) = tokio_mpsc::channel::<Result<QueryNodesResponse, Status>>(8);

        tokio::task::spawn_blocking(move || {
            let mut cursor_created: Option<u64> = created_after;
            let mut cursor_updated: Option<u64> = updated_after;
            // Internal page size: always 512 for efficient DB queries; the
            // user_limit applies as a cap on the total rows streamed.
            // Per spec, limit 0 → server default (512), capped at 4096.
            // The streaming contract: send ALL matching rows, chunked ≤512/msg.
            const INTERNAL_BATCH: u32 = 512;

            loop {
                let filter = QueryFilter {
                    experiment_id: exp_id.clone(),
                    status: status_filter,
                    parent_node_id: parent_filter,
                    min_depth: r.min_depth.map(|v| v as u32),
                    max_depth: r.max_depth.map(|v| v as u32),
                    order,
                    created_after: cursor_created,
                    updated_after: cursor_updated,
                    limit: Some(INTERNAL_BATCH),
                };

                let rows = match meta.query_nodes(filter) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(meta_error_to_status(e)));
                        return;
                    }
                };

                let done = rows.len() < INTERNAL_BATCH as usize;

                // Chunk into ≤512 per message.
                for chunk in rows.chunks(512) {
                    let msg = QueryNodesResponse {
                        nodes: chunk.iter().map(node_row_to_proto).collect(),
                    };
                    if tx.blocking_send(Ok(msg)).is_err() {
                        return;
                    }
                }

                if done {
                    break;
                }

                // Advance cursors for the next page.
                if let Some(last) = rows.last() {
                    match order {
                        QueryOrder::CreatedAt => cursor_created = Some(last.created_at),
                        QueryOrder::UpdatedAt => cursor_updated = Some(last.updated_at),
                        QueryOrder::NodeId => {
                            // no cursor on node_id — break after first page
                            break;
                        }
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    // ── PutMetadata ───────────────────────────────────────────────────────────

    async fn put_metadata(
        &self,
        request: Request<PutMetadataRequest>,
    ) -> Result<Response<PutMetadataResponse>, Status> {
        let r = request.into_inner();
        if r.key.is_empty() || r.key.len() > 512 {
            return Err(Status::new(
                Code::InvalidArgument,
                format!("key length {} not in 1..=512", r.key.len()),
            ));
        }
        if r.value.len() > 16 * 1024 * 1024 {
            return Err(Status::new(
                Code::InvalidArgument,
                format!("value too large: {} bytes (max 16 MiB)", r.value.len()),
            ));
        }

        let meta = Arc::clone(&self.meta);
        let generation = tokio::task::spawn_blocking(move || {
            meta.put_metadata(r.key, r.value, r.expected_generation)
        })
        .await
        .map_err(|e| Status::new(Code::Internal, format!("put_metadata panicked: {e}")))?
        .map_err(meta_error_to_status)?;

        Ok(Response::new(PutMetadataResponse { generation }))
    }

    // ── GetMetadata ───────────────────────────────────────────────────────────

    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataResponse>, Status> {
        let r = request.into_inner();
        let meta = Arc::clone(&self.meta);

        let result = tokio::task::spawn_blocking(move || meta.get_metadata(&r.key))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("get_metadata panicked: {e}")))?
            .map_err(meta_error_to_status)?;

        match result {
            None => Err(Status::new(Code::NotFound, "key not found")),
            Some((value, generation)) => {
                Ok(Response::new(GetMetadataResponse { value, generation }))
            }
        }
    }

    // ── DeleteMetadata ────────────────────────────────────────────────────────

    async fn delete_metadata(
        &self,
        request: Request<DeleteMetadataRequest>,
    ) -> Result<Response<DeleteMetadataResponse>, Status> {
        let r = request.into_inner();
        let meta = Arc::clone(&self.meta);

        let deleted =
            tokio::task::spawn_blocking(move || meta.delete_metadata(r.key, r.expected_generation))
                .await
                .map_err(|e| Status::new(Code::Internal, format!("delete_metadata panicked: {e}")))?
                .map_err(meta_error_to_status)?;

        Ok(Response::new(DeleteMetadataResponse { deleted }))
    }

    // ── PruneSubtree ──────────────────────────────────────────────────────────

    async fn prune_subtree(
        &self,
        request: Request<PruneSubtreeRequest>,
    ) -> Result<Response<PruneSubtreeResponse>, Status> {
        let r = request.into_inner();
        let exp_id = validate_experiment_id(&r.experiment_id)?;
        let node_id = NodeId(r.node_id);
        let allow_root = r.allow_root;
        let meta = Arc::clone(&self.meta);

        let nodes_pruned =
            tokio::task::spawn_blocking(move || meta.prune_subtree(exp_id, node_id, allow_root))
                .await
                .map_err(|e| Status::new(Code::Internal, format!("prune_subtree panicked: {e}")))?
                .map_err(meta_error_to_status)?;

        Ok(Response::new(PruneSubtreeResponse { nodes_pruned }))
    }

    // ── Pin ───────────────────────────────────────────────────────────────────

    async fn pin(&self, request: Request<PinRequest>) -> Result<Response<PinResponse>, Status> {
        let r = request.into_inner();
        let ref_bytes = validate_hash32(&r.snapshot_ref, "snapshot_ref")?;
        let snap_ref = SnapshotRef::from_bytes(ref_bytes);
        let note = if r.note.is_empty() {
            None
        } else {
            Some(r.note)
        };
        let meta = Arc::clone(&self.meta);

        let list_before = {
            let meta2 = Arc::clone(&self.meta);
            tokio::task::spawn_blocking(move || meta2.list_pins())
                .await
                .map_err(|e| Status::new(Code::Internal, format!("pin (list) panicked: {e}")))?
                .map_err(meta_error_to_status)?
        };

        let snap_ref_check = snap_ref.clone();
        let already = list_before.iter().any(|p| p.snapshot_ref == snap_ref_check);

        tokio::task::spawn_blocking(move || meta.pin(snap_ref, note))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("pin panicked: {e}")))?
            .map_err(meta_error_to_status)?;

        Ok(Response::new(PinResponse {
            newly_pinned: !already,
        }))
    }

    // ── Unpin ─────────────────────────────────────────────────────────────────

    async fn unpin(
        &self,
        request: Request<UnpinRequest>,
    ) -> Result<Response<UnpinResponse>, Status> {
        let r = request.into_inner();
        let ref_bytes = validate_hash32(&r.snapshot_ref, "snapshot_ref")?;
        let snap_ref = SnapshotRef::from_bytes(ref_bytes);
        let meta = Arc::clone(&self.meta);

        let was_pinned = tokio::task::spawn_blocking(move || meta.unpin(&snap_ref))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("unpin panicked: {e}")))?
            .map_err(meta_error_to_status)?;

        Ok(Response::new(UnpinResponse { was_pinned }))
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        let r = request.into_inner();
        let exp_id_str = r.experiment_id.clone();
        let exp_id_opt: Option<ExperimentId> = if exp_id_str.is_empty() {
            None
        } else {
            Some(validate_experiment_id(&exp_id_str)?)
        };

        let meta = Arc::clone(&self.meta);
        let store = Arc::clone(&self.store);
        let exp_id_clone = exp_id_opt.clone();

        let meta_stats = tokio::task::spawn_blocking(move || meta.stats(exp_id_clone.as_ref()))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("stats (meta) panicked: {e}")))?
            .map_err(meta_error_to_status)?;

        let (manifests_total, logical_page_bytes) =
            tokio::task::spawn_blocking(move || store.manifest_count_and_logical_bytes())
                .await
                .map_err(|e| Status::new(Code::Internal, format!("stats (store) panicked: {e}")))?;

        // unique_pages is an index-size read — no blocking needed but we run
        // in spawn_blocking for consistency with bridge rules.
        let store2 = Arc::clone(&self.store);
        let unique_pages = tokio::task::spawn_blocking(move || store2.pages().unique_pages())
            .await
            .map_err(|e| Status::new(Code::Internal, format!("stats (pages) panicked: {e}")))?;

        let physical_page_bytes = unique_pages * PAGE_SIZE as u64;
        // physical_page_bytes approximation: unique_pages × 4096.
        // The per-record header overhead (37 bytes) is excluded for simplicity;
        // exact byte accounting is M7's responsibility.
        let dedup_ratio = if physical_page_bytes == 0 {
            0.0
        } else {
            logical_page_bytes as f64 / physical_page_bytes as f64
        };

        // Update node-status gauges from meta stats.
        if exp_id_opt.is_none() {
            // Global stats don't have per-status breakdown; skip gauge update
            // (we'd need to query_nodes which is expensive on the stats path).
        }

        let experiment_stats = if exp_id_opt.is_some() {
            let nodes_total = meta_stats.exp_nodes_frontier
                + meta_stats.exp_nodes_expanded
                + meta_stats.exp_nodes_pruned
                + meta_stats.exp_nodes_goal;

            // Update per-status gauges.
            self.metrics
                .nodes
                .with_label_values(&["frontier"])
                .set(meta_stats.exp_nodes_frontier as i64);
            self.metrics
                .nodes
                .with_label_values(&["expanded"])
                .set(meta_stats.exp_nodes_expanded as i64);
            self.metrics
                .nodes
                .with_label_values(&["pruned"])
                .set(meta_stats.exp_nodes_pruned as i64);
            self.metrics
                .nodes
                .with_label_values(&["goal"])
                .set(meta_stats.exp_nodes_goal as i64);

            Some(ExperimentStats {
                experiment_id: exp_id_str,
                nodes_total,
                nodes_frontier: meta_stats.exp_nodes_frontier,
                nodes_expanded: meta_stats.exp_nodes_expanded,
                nodes_pruned: meta_stats.exp_nodes_pruned,
                nodes_goal: meta_stats.exp_nodes_goal,
                max_depth: meta_stats.exp_max_depth as u64,
                input_logs_total: meta_stats.exp_input_log_count,
                input_log_bytes: meta_stats.input_logs_bytes,
            })
        } else {
            None
        };

        Ok(Response::new(StatsResponse {
            store: Some(StoreStats {
                unique_pages,
                physical_page_bytes,
                manifests_total,
                logical_page_bytes,
                dedup_ratio,
                experiments_total: meta_stats.experiments_count,
                nodes_total: meta_stats.total_nodes,
                pins_total: meta_stats.pins_count,
                tombstones_total: meta_stats.tombstones_count,
                logical_counter: meta_stats.logical_counter,
                gc_runs_total: 0,            // zero until M7
                gc_pages_reclaimed_total: 0, // zero until M7
            }),
            experiment: experiment_stats,
        }))
    }

    // ── TriggerGc ─────────────────────────────────────────────────────────────

    async fn trigger_gc(
        &self,
        _request: Request<TriggerGcRequest>,
    ) -> Result<Response<TriggerGcResponse>, Status> {
        Err(Status::new(Code::Unimplemented, "GC lands at M7"))
    }
}
