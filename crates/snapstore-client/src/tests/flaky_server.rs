//! In-process `FlakyServer` test fixture.
//!
//! Implements the generated `SnapshotStoreServer` trait with:
//! - In-memory storage (HashMap-backed).
//! - Configurable failure injection: fail the first N calls of a named RPC
//!   with `UNAVAILABLE` or with a `FAILED_PRECONDITION` + detail payload.
//! - Served over a real UDS socket in a tempdir so the UDS connector is
//!   exercised end-to-end.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

use crate::details;
use crate::snapstore_proto::{
    snapshot_store_server::{SnapshotStore, SnapshotStoreServer},
    *,
};

// ── failure injection types ────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum InjectError {
    Unavailable,
    FailedPreconditionWithDetail(Vec<u8>),
}

/// A named failure rule: fail the first `n` calls to `rpc_name` with `error`.
#[derive(Clone, Debug)]
pub struct FailureRule {
    pub rpc_name: String,
    pub n: usize,
    pub error: InjectError,
}

// ── per-RPC call counters ──────────────────────────────────────────────────────

#[derive(Default)]
pub struct CallCounts {
    pub put_pages: AtomicU64,
    pub put_snapshot: AtomicU64,
    pub get_snapshot: AtomicU64,
    pub create_node: AtomicU64,
    pub put_metadata: AtomicU64,
    pub delete_metadata: AtomicU64,
    pub put_input_log: AtomicU64,
    pub get_input_log: AtomicU64,
    pub update_nodes: AtomicU64,
    pub get_node: AtomicU64,
    pub get_children: AtomicU64,
    pub get_path: AtomicU64,
    pub query_nodes: AtomicU64,
    pub has_pages: AtomicU64,
    pub resolve_pages: AtomicU64,
    pub prune_subtree: AtomicU64,
    pub pin: AtomicU64,
    pub unpin: AtomicU64,
    pub stats: AtomicU64,
    pub trigger_gc: AtomicU64,
}

// ── server state ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct State {
    /// page_hash → page data (4096 bytes)
    pages: HashMap<Vec<u8>, Vec<u8>>,
    /// snapshot_ref → container
    snapshots: HashMap<Vec<u8>, Vec<u8>>,
    /// log_id → container
    input_logs: HashMap<Vec<u8>, Vec<u8>>,
    /// (experiment_id, node_id) → NodeMeta
    nodes: HashMap<(String, u64), NodeMeta>,
    /// kv: key → (value, generation)
    kv: HashMap<Vec<u8>, (Vec<u8>, u64)>,
    /// pins: snapshot_ref set
    pins: std::collections::HashSet<Vec<u8>>,
    /// logical counter for tree ops
    logical_counter: u64,
    /// per-rpc call failure counters
    failure_counts: HashMap<String, usize>,
}

impl State {
    fn check_inject(&mut self, rpc_name: &str, rules: &[FailureRule]) -> Option<Status> {
        for rule in rules {
            if rule.rpc_name == rpc_name {
                let count = self.failure_counts.entry(rpc_name.to_owned()).or_insert(0);
                if *count < rule.n {
                    *count += 1;
                    return Some(match &rule.error {
                        InjectError::Unavailable => Status::unavailable("injected failure"),
                        InjectError::FailedPreconditionWithDetail(detail) => {
                            details::status_with_detail(
                                tonic::Code::FailedPrecondition,
                                "injected detail error",
                                detail.clone(),
                            )
                        }
                    });
                }
            }
        }
        None
    }
}

// ── server struct ─────────────────────────────────────────────────────────────

pub struct FlakyServer {
    state: Mutex<State>,
    pub call_counts: Arc<CallCounts>,
    failure_rules: Vec<FailureRule>,
    /// When set, override the batch_blake3 in PutPages responses with this.
    pub override_batch_blake3: Option<Vec<u8>>,
}

impl FlakyServer {
    pub fn new(rules: Vec<FailureRule>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            call_counts: Arc::new(CallCounts::default()),
            failure_rules: rules,
            override_batch_blake3: None,
        }
    }

    pub fn with_bad_batch_blake3(rules: Vec<FailureRule>) -> Self {
        let mut s = Self::new(rules);
        s.override_batch_blake3 = Some(vec![0xde; 32]);
        s
    }
}

#[async_trait]
impl SnapshotStore for FlakyServer {
    type ResolvePagesStream =
        tokio_stream::wrappers::ReceiverStream<Result<ResolvePagesResponse, Status>>;
    type QueryNodesStream =
        tokio_stream::wrappers::ReceiverStream<Result<QueryNodesResponse, Status>>;

    async fn put_pages(
        &self,
        request: Request<tonic::Streaming<PutPagesRequest>>,
    ) -> Result<Response<PutPagesResponse>, Status> {
        self.call_counts.put_pages.fetch_add(1, Ordering::SeqCst);
        {
            let mut s = self.state.lock().unwrap();
            if let Some(e) = s.check_inject("put_pages", &self.failure_rules) {
                return Err(e);
            }
        }
        use tokio_stream::StreamExt;
        let mut stream = request.into_inner();

        let mut pages_new = 0u64;
        let mut pages_deduped = 0u64;
        let mut batch_hasher = blake3::Hasher::new();

        while let Some(msg) = stream.next().await {
            let msg = msg?;
            let mut s = self.state.lock().unwrap();
            for page_data in msg.pages {
                if page_data.len() != 4096 {
                    return Err(Status::invalid_argument("page must be 4096 bytes"));
                }
                let hash = *blake3::hash(&page_data).as_bytes();
                batch_hasher.update(&hash);
                let entry = s.pages.entry(hash.to_vec());
                match entry {
                    std::collections::hash_map::Entry::Occupied(_) => pages_deduped += 1,
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert(page_data);
                        pages_new += 1;
                    }
                }
            }
        }

        let batch_blake3 = if let Some(bad) = &self.override_batch_blake3 {
            bad.clone()
        } else {
            batch_hasher.finalize().as_bytes().to_vec()
        };

        Ok(Response::new(PutPagesResponse {
            pages_new,
            pages_deduped,
            batch_blake3,
        }))
    }

    async fn put_snapshot(
        &self,
        request: Request<PutSnapshotRequest>,
    ) -> Result<Response<PutSnapshotResponse>, Status> {
        self.call_counts.put_snapshot.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("put_snapshot", &self.failure_rules) {
            return Err(e);
        }
        let container = request.into_inner().container;
        if container.len() < 32 {
            return Err(Status::invalid_argument("container too short"));
        }
        let body_len = container.len() - 32;
        let hash = *blake3::hash(&container[..body_len]).as_bytes();
        s.snapshots.insert(hash.to_vec(), container);
        Ok(Response::new(PutSnapshotResponse {
            snapshot_ref: hash.to_vec(),
        }))
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        self.call_counts.get_snapshot.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("get_snapshot", &self.failure_rules) {
            return Err(e);
        }
        let key = request.into_inner().snapshot_ref;
        match s.snapshots.get(&key) {
            Some(c) => Ok(Response::new(GetSnapshotResponse {
                container: c.clone(),
            })),
            None => Err(Status::not_found("snapshot not found")),
        }
    }

    async fn resolve_pages(
        &self,
        _request: Request<ResolvePagesRequest>,
    ) -> Result<Response<Self::ResolvePagesStream>, Status> {
        self.call_counts
            .resolve_pages
            .fetch_add(1, Ordering::SeqCst);
        // Stub: return empty stream.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn has_pages(
        &self,
        request: Request<HasPagesRequest>,
    ) -> Result<Response<HasPagesResponse>, Status> {
        self.call_counts.has_pages.fetch_add(1, Ordering::SeqCst);
        let s = self.state.lock().unwrap();
        let present = request
            .into_inner()
            .page_hashes
            .iter()
            .map(|h| s.pages.contains_key(h))
            .collect();
        Ok(Response::new(HasPagesResponse { present }))
    }

    async fn put_input_log(
        &self,
        request: Request<PutInputLogRequest>,
    ) -> Result<Response<PutInputLogResponse>, Status> {
        self.call_counts
            .put_input_log
            .fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("put_input_log", &self.failure_rules) {
            return Err(e);
        }
        let container = request.into_inner().container;
        if container.len() < 32 {
            return Err(Status::invalid_argument("container too short"));
        }
        let body_len = container.len() - 32;
        let hash = *blake3::hash(&container[..body_len]).as_bytes();
        let key = hash.to_vec();
        let newly_stored = !s.input_logs.contains_key(key.as_slice());
        if newly_stored {
            s.input_logs.insert(key.clone(), container);
        }
        Ok(Response::new(PutInputLogResponse {
            log_id: key,
            newly_stored,
        }))
    }

    async fn get_input_log(
        &self,
        request: Request<GetInputLogRequest>,
    ) -> Result<Response<GetInputLogResponse>, Status> {
        self.call_counts
            .get_input_log
            .fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("get_input_log", &self.failure_rules) {
            return Err(e);
        }
        let key = request.into_inner().log_id;
        match s.input_logs.get(&key) {
            Some(c) => Ok(Response::new(GetInputLogResponse {
                container: c.clone(),
            })),
            None => Err(Status::not_found("input log not found")),
        }
    }

    async fn create_node(
        &self,
        request: Request<CreateNodeRequest>,
    ) -> Result<Response<CreateNodeResponse>, Status> {
        self.call_counts.create_node.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("create_node", &self.failure_rules) {
            return Err(e);
        }
        let req = request.into_inner();
        let key = (req.experiment_id.clone(), req.node_id);
        if let Some(existing) = s.nodes.get(&key) {
            // Idempotent on (experiment_id, node_id): return the stored row.
            return Ok(Response::new(CreateNodeResponse {
                node: Some(existing.clone()),
            }));
        }
        s.logical_counter += 1;
        let lc = s.logical_counter;
        let node = NodeMeta {
            experiment_id: req.experiment_id,
            node_id: req.node_id,
            parent_node_id: req.parent_node_id,
            depth: req.parent_node_id.map(|_| 1).unwrap_or(0),
            snapshot_ref: req.snapshot_ref,
            input_log_id: req.input_log_id,
            status: req.status,
            score: req.score,
            visit_count: 0,
            icount: req.icount,
            virtual_ns: req.virtual_ns,
            created_at: lc,
            updated_at: lc,
            last_visited_at: 0,
            attrs: req.attrs,
        };
        s.nodes.insert(key, node.clone());
        Ok(Response::new(CreateNodeResponse { node: Some(node) }))
    }

    async fn update_nodes(
        &self,
        request: Request<UpdateNodesRequest>,
    ) -> Result<Response<UpdateNodesResponse>, Status> {
        self.call_counts.update_nodes.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("update_nodes", &self.failure_rules) {
            return Err(e);
        }
        let req = request.into_inner();
        // Check all ids exist first.
        let mut missing = Vec::new();
        for u in &req.updates {
            if !s
                .nodes
                .contains_key(&(req.experiment_id.clone(), u.node_id))
            {
                missing.push(u.node_id);
            }
        }
        if !missing.is_empty() {
            let detail = details::encode_missing_nodes(&missing);
            return Err(details::status_with_detail(
                tonic::Code::NotFound,
                "missing nodes",
                detail,
            ));
        }
        s.logical_counter += 1;
        let lc = s.logical_counter;
        for u in req.updates {
            if let Some(node) = s.nodes.get_mut(&(req.experiment_id.clone(), u.node_id)) {
                if let Some(st) = u.status {
                    node.status = st;
                }
                if let Some(sc) = u.score {
                    node.score = Some(sc);
                }
                node.updated_at = lc;
                if u.touch_visited {
                    node.last_visited_at = lc;
                }
                if let Some(delta) = u.visit_count_delta {
                    node.visit_count = (node.visit_count as i64 + delta).max(0) as u64;
                }
            }
        }
        Ok(Response::new(UpdateNodesResponse { updated_at: lc }))
    }

    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        self.call_counts.get_node.fetch_add(1, Ordering::SeqCst);
        let s = self.state.lock().unwrap();
        let req = request.into_inner();
        match s.nodes.get(&(req.experiment_id, req.node_id)) {
            Some(n) => Ok(Response::new(GetNodeResponse {
                node: Some(n.clone()),
            })),
            None => Err(Status::not_found("node not found")),
        }
    }

    async fn get_children(
        &self,
        request: Request<GetChildrenRequest>,
    ) -> Result<Response<GetChildrenResponse>, Status> {
        self.call_counts.get_children.fetch_add(1, Ordering::SeqCst);
        let s = self.state.lock().unwrap();
        let req = request.into_inner();
        let nodes: Vec<NodeMeta> = s
            .nodes
            .values()
            .filter(|n| {
                n.experiment_id == req.experiment_id && n.parent_node_id == Some(req.node_id)
            })
            .cloned()
            .collect();
        Ok(Response::new(GetChildrenResponse { nodes }))
    }

    async fn get_path(
        &self,
        request: Request<GetPathRequest>,
    ) -> Result<Response<GetPathResponse>, Status> {
        self.call_counts.get_path.fetch_add(1, Ordering::SeqCst);
        let s = self.state.lock().unwrap();
        let req = request.into_inner();
        // Walk parent pointers from node back to root, then reverse.
        let mut path = Vec::new();
        let mut cur_id = req.node_id;
        loop {
            match s.nodes.get(&(req.experiment_id.clone(), cur_id)) {
                None => return Err(Status::not_found("node not found in path")),
                Some(n) => {
                    path.push(PathElement {
                        node: Some(n.clone()),
                        input_log_container: vec![],
                    });
                    match n.parent_node_id {
                        None => break,
                        Some(p) => cur_id = p,
                    }
                }
            }
        }
        path.reverse();
        Ok(Response::new(GetPathResponse { elements: path }))
    }

    async fn query_nodes(
        &self,
        request: Request<QueryNodesRequest>,
    ) -> Result<Response<Self::QueryNodesStream>, Status> {
        self.call_counts.query_nodes.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let state = self.state.lock().unwrap();
        let req = request.into_inner();
        let nodes: Vec<NodeMeta> = state
            .nodes
            .values()
            .filter(|n| n.experiment_id == req.experiment_id)
            .cloned()
            .collect();
        drop(state);
        tokio::spawn(async move {
            for chunk in nodes.chunks(512) {
                let _ = tx
                    .send(Ok(QueryNodesResponse {
                        nodes: chunk.to_vec(),
                    }))
                    .await;
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn put_metadata(
        &self,
        request: Request<PutMetadataRequest>,
    ) -> Result<Response<PutMetadataResponse>, Status> {
        self.call_counts.put_metadata.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("put_metadata", &self.failure_rules) {
            return Err(e);
        }
        let req = request.into_inner();
        let current_gen = s.kv.get(&req.key).map(|(_, g)| *g).unwrap_or(0);
        if let Some(expected) = req.expected_generation {
            if expected != current_gen {
                let detail = details::encode_current_generation(current_gen);
                return Err(details::status_with_detail(
                    tonic::Code::FailedPrecondition,
                    "cas mismatch",
                    detail,
                ));
            }
        }
        let new_gen = current_gen + 1;
        s.kv.insert(req.key, (req.value, new_gen));
        Ok(Response::new(PutMetadataResponse {
            generation: new_gen,
        }))
    }

    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataResponse>, Status> {
        let s = self.state.lock().unwrap();
        let key = request.into_inner().key;
        match s.kv.get(&key) {
            Some((v, g)) => Ok(Response::new(GetMetadataResponse {
                value: v.clone(),
                generation: *g,
            })),
            None => Err(Status::not_found("key not found")),
        }
    }

    async fn delete_metadata(
        &self,
        request: Request<DeleteMetadataRequest>,
    ) -> Result<Response<DeleteMetadataResponse>, Status> {
        self.call_counts
            .delete_metadata
            .fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.check_inject("delete_metadata", &self.failure_rules) {
            return Err(e);
        }
        let req = request.into_inner();
        let current_gen = s.kv.get(&req.key).map(|(_, g)| *g).unwrap_or(0);
        if let Some(expected) = req.expected_generation {
            if expected != current_gen {
                let detail = details::encode_current_generation(current_gen);
                return Err(details::status_with_detail(
                    tonic::Code::FailedPrecondition,
                    "cas mismatch on delete",
                    detail,
                ));
            }
        }
        let deleted = s.kv.remove(&req.key).is_some();
        Ok(Response::new(DeleteMetadataResponse { deleted }))
    }

    async fn prune_subtree(
        &self,
        _request: Request<PruneSubtreeRequest>,
    ) -> Result<Response<PruneSubtreeResponse>, Status> {
        self.call_counts
            .prune_subtree
            .fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(PruneSubtreeResponse { nodes_pruned: 0 }))
    }

    async fn pin(&self, request: Request<PinRequest>) -> Result<Response<PinResponse>, Status> {
        self.call_counts.pin.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        let key = request.into_inner().snapshot_ref;
        let newly_pinned = s.pins.insert(key);
        Ok(Response::new(PinResponse { newly_pinned }))
    }

    async fn unpin(
        &self,
        request: Request<UnpinRequest>,
    ) -> Result<Response<UnpinResponse>, Status> {
        self.call_counts.unpin.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        let key = request.into_inner().snapshot_ref;
        let was_pinned = s.pins.remove(&key);
        Ok(Response::new(UnpinResponse { was_pinned }))
    }

    async fn stats(
        &self,
        _request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        self.call_counts.stats.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(StatsResponse {
            store: Some(crate::snapstore_proto::StoreStats::default()),
            experiment: None,
        }))
    }

    async fn trigger_gc(
        &self,
        _request: Request<TriggerGcRequest>,
    ) -> Result<Response<TriggerGcResponse>, Status> {
        self.call_counts.trigger_gc.fetch_add(1, Ordering::SeqCst);
        Err(Status::unimplemented("GC not implemented until M7"))
    }
}

// ── server launch helpers ──────────────────────────────────────────────────────

/// Start a `FlakyServer` on a UDS socket in `dir`. Returns the socket path
/// and a `JoinHandle` for the server task.
pub async fn start_flaky_server(
    server: FlakyServer,
    dir: &std::path::Path,
) -> (PathBuf, tokio::task::JoinHandle<()>) {
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;

    let socket_path = dir.join("snapstore.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind UDS");
    let stream = UnixListenerStream::new(listener);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(SnapshotStoreServer::new(server))
            .serve_with_incoming(stream)
            .await
            .ok();
    });

    // Give the server a moment to be ready.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    (socket_path, handle)
}

/// Build a client connected to a UDS socket at `socket_path`.
pub async fn client_for_uds(socket_path: &std::path::Path) -> crate::SnapstoreClient {
    crate::SnapstoreClient::connect(crate::Transport::Uds(socket_path.to_owned()))
        .await
        .expect("connect to flaky server")
}
