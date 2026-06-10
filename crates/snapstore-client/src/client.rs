//! Async `SnapstoreClient` — typed methods over every RPC in the service.

use bytes::Bytes;
use snapstore_manifest::DeviceBlob;
use snapstore_types::{LogId, PageHash, SnapshotRef};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::{
    error::{decode_status, ClientError, ClientResult},
    helpers,
    retry::with_retry,
    snapstore_proto::{
        snapshot_store_client::SnapshotStoreClient as RawClient, CreateNodeRequest,
        DeleteMetadataRequest, GetChildrenRequest, GetInputLogRequest, GetMetadataRequest,
        GetNodeRequest, GetPathRequest, GetSnapshotRequest, HasPagesRequest, NodeMeta, NodeUpdate,
        PathElement, PinRequest, PruneSubtreeRequest, PutInputLogRequest, PutMetadataRequest,
        PutPagesRequest, PutSnapshotRequest, QueryNodesRequest, ResolvePagesRequest, StatsResponse,
        UnpinRequest, UpdateNodesRequest,
    },
    transport::Transport,
};

/// Async client for the `determinism.snapstore.v1.SnapshotStore` service.
///
/// All methods are async and return `ClientResult<T>`.  Retryable errors are
/// automatically retried with exponential backoff (see `retry` module).
///
/// Clone is cheap: the underlying `Channel` is reference-counted.
///
/// # Page-channel fast path (Linux only)
///
/// When the `Transport::Auto` configuration includes a `page_channel_path` that
/// exists and connects, `put_pages` sends page bytes over the SEQPACKET channel
/// instead of gRPC.  Every operation keeps its pure-gRPC equivalent; any
/// `ChannelError` that is not `CrossCheckMismatch` causes a silent fallback to
/// gRPC (logged at WARN level).
///
/// `CrossCheckMismatch` is a P0 determinism signal — it surfaces as
/// `ClientError::BatchBlake3Mismatch` and is never retried or fallen back.
#[derive(Clone)]
pub struct SnapstoreClient {
    inner: RawClient<Channel>,
    /// Page-channel client, present when the `page_channel_path` exists and
    /// connects at construction time (Linux only).
    #[cfg(target_os = "linux")]
    page_channel: Option<std::sync::Arc<snapstore_localpath::client::PageChannelClient>>,
}

impl SnapstoreClient {
    /// Connect using the given `Transport` configuration.
    pub async fn connect(transport: Transport) -> ClientResult<Self> {
        #[cfg(target_os = "linux")]
        let page_channel = try_connect_page_channel(&transport);

        let channel = transport.connect().await?;

        Ok(Self {
            inner: RawClient::new(channel),
            #[cfg(target_os = "linux")]
            page_channel,
        })
    }

    /// Build a client from an already-established `Channel`.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: RawClient::new(channel),
            #[cfg(target_os = "linux")]
            page_channel: None,
        }
    }

    // ── pages & snapshots ──────────────────────────────────────────────────

    /// Upload pages to the server, returning `(pages_new, pages_deduped)`.
    ///
    /// Pages are sent in batches of at most 256 per message.  After the stream
    /// completes, the returned `batch_blake3` is cross-checked against a local
    /// computation over the per-page BLAKE3 hashes (in stream order).  A
    /// mismatch is a P0 integrity error (`ClientError::BatchBlake3Mismatch`)
    /// and is never retried.
    ///
    /// The entire upload is retried on transient errors because the operation
    /// is content-idempotent (server deduplicates by hash).
    ///
    /// # Page-channel fast path
    ///
    /// On Linux, when the client was constructed via `Transport::Auto` with a
    /// `page_channel_path` that connected, page bytes are sent over the
    /// SEQPACKET channel.  Any channel error that is not
    /// `CrossCheckMismatch` causes a transparent fallback to gRPC (WARN log).
    /// `CrossCheckMismatch` surfaces immediately as
    /// `ClientError::BatchBlake3Mismatch`.
    pub async fn put_pages(&self, pages: Vec<(u64, Vec<u8>)>) -> ClientResult<(u64, u64)> {
        // Validate page sizes up-front.
        for (idx, data) in &pages {
            if data.len() != 4096 {
                return Err(ClientError::Transport(format!(
                    "page at index {idx} must be exactly 4096 bytes, got {}",
                    data.len()
                )));
            }
        }

        // ── Page-channel fast path (Linux only) ───────────────────────────────
        #[cfg(target_os = "linux")]
        if let Some(ref pc) = self.page_channel {
            match put_pages_via_channel(pc.as_ref(), &pages) {
                Ok(result) => return Ok(result),
                Err(snapstore_localpath::ChannelError::CrossCheckMismatch { expected, actual }) => {
                    // P0 — never fall back silently.
                    tracing::warn!(
                        expected = %expected,
                        actual = %actual,
                        "page-channel: batch_blake3 cross-check mismatch (P0)"
                    );
                    return Err(ClientError::BatchBlake3Mismatch { expected, actual });
                }
                Err(e) => {
                    // Non-fatal channel error — fall back to gRPC.
                    tracing::warn!(err = %e, "page-channel: error, falling back to gRPC");
                }
            }
        }

        let inner = self.inner.clone();
        with_retry(|| {
            let pages = pages.clone();
            let mut inner = inner.clone();
            async move {
                // Compute the local batch_blake3 as we prepare messages.
                let mut local_hasher = blake3::Hasher::new();
                let mut messages: Vec<PutPagesRequest> = Vec::new();
                let mut chunk_pages = Vec::<bytes::Bytes>::new();

                for (_, data) in &pages {
                    let ph = blake3::hash(data);
                    local_hasher.update(ph.as_bytes());
                    chunk_pages.push(Bytes::from(data.clone()));
                    if chunk_pages.len() == 256 {
                        messages.push(PutPagesRequest {
                            pages: chunk_pages.iter().map(|b| b.to_vec()).collect(),
                        });
                        chunk_pages.clear();
                    }
                }
                if !chunk_pages.is_empty() {
                    messages.push(PutPagesRequest {
                        pages: chunk_pages.iter().map(|b| b.to_vec()).collect(),
                    });
                }

                let local_batch_hash = *local_hasher.finalize().as_bytes();

                let (tx, rx) = tokio::sync::mpsc::channel(16);
                for msg in messages {
                    tx.send(msg)
                        .await
                        .map_err(|_| ClientError::Transport("send channel closed".into()))?;
                }
                drop(tx);

                let response = inner
                    .put_pages(ReceiverStream::new(rx))
                    .await
                    .map_err(decode_status)?;
                let resp = response.into_inner();

                // Cross-check batch_blake3.
                if resp.batch_blake3.len() != 32 {
                    return Err(ClientError::Transport(
                        "server returned batch_blake3 with wrong length".into(),
                    ));
                }
                let server_hash: [u8; 32] = resp.batch_blake3.as_slice().try_into().unwrap();
                if local_batch_hash != server_hash {
                    return Err(ClientError::BatchBlake3Mismatch {
                        expected: hex(&local_batch_hash),
                        actual: hex(&server_hash),
                    });
                }

                Ok((resp.pages_new, resp.pages_deduped))
            }
        })
        .await
    }

    /// Store a `.spm` manifest container.  The container is returned
    /// byte-identically by `get_snapshot`.
    pub async fn put_snapshot(&self, container: Vec<u8>) -> ClientResult<SnapshotRef> {
        let inner = self.inner.clone();
        with_retry(|| {
            let container = container.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .put_snapshot(PutSnapshotRequest { container })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                let arr: [u8; 32] =
                    resp.snapshot_ref.as_slice().try_into().map_err(|_| {
                        ClientError::Transport("snapshot_ref is not 32 bytes".into())
                    })?;
                Ok(SnapshotRef::from_bytes(arr))
            }
        })
        .await
    }

    /// Fetch a stored `.spm` container, verifying its BLAKE3 footer against
    /// the requested `snapshot_ref`.  Corruption returns
    /// `ClientError::CorruptPayload`.
    pub async fn get_snapshot(&self, snapshot_ref: SnapshotRef) -> ClientResult<Vec<u8>> {
        let inner = self.inner.clone();
        let sr = snapshot_ref.clone();
        with_retry(|| {
            let sr = sr.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .get_snapshot(GetSnapshotRequest {
                        snapshot_ref: sr.to_bytes().to_vec(),
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                let container = resp.container;
                verify_container_footer(&container, &sr)?;
                Ok(container)
            }
        })
        .await
    }

    /// Resolve the page set for a snapshot.
    ///
    /// Returns `Vec<(page_index, page_hash, Option<payload>)>` where payload
    /// is `None` when `hashes_only` is true.
    pub async fn resolve_pages(
        &self,
        snapshot_ref: SnapshotRef,
        baseline_ref: Option<SnapshotRef>,
        hashes_only: bool,
    ) -> ClientResult<Vec<(u64, PageHash, Option<Bytes>)>> {
        let inner = self.inner.clone();
        let sr = snapshot_ref.clone();
        let br = baseline_ref.clone();
        with_retry(|| {
            let sr = sr.clone();
            let br = br.clone();
            let mut inner = inner.clone();
            async move {
                use tokio_stream::StreamExt;

                let req = ResolvePagesRequest {
                    snapshot_ref: sr.to_bytes().to_vec(),
                    baseline_ref: br
                        .as_ref()
                        .map(|r| r.to_bytes().to_vec())
                        .unwrap_or_default(),
                    hashes_only,
                };
                let mut stream = inner
                    .resolve_pages(req)
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                let mut result = Vec::new();
                while let Some(msg) = stream.next().await {
                    let msg = msg.map_err(decode_status)?;
                    for page in msg.pages {
                        let hash_arr: [u8; 32] =
                            page.page_hash.as_slice().try_into().map_err(|_| {
                                ClientError::Transport("resolved page has non-32-byte hash".into())
                            })?;
                        let payload = if hashes_only || page.payload.is_empty() {
                            None
                        } else {
                            Some(Bytes::from(page.payload))
                        };
                        result.push((page.page_index, PageHash::from_bytes(hash_arr), payload));
                    }
                }
                Ok(result)
            }
        })
        .await
    }

    /// Bulk existence probe for pages. Returns a parallel boolean slice.
    pub async fn has_pages(&self, page_hashes: Vec<PageHash>) -> ClientResult<Vec<bool>> {
        let inner = self.inner.clone();
        with_retry(|| {
            let hashes = page_hashes.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .has_pages(HasPagesRequest {
                        page_hashes: hashes.iter().map(|h| h.as_bytes().to_vec()).collect(),
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.present)
            }
        })
        .await
    }

    // ── input logs ────────────────────────────────────────────────────────

    /// Store a SILG input-log container. Returns `(log_id, newly_stored)`.
    pub async fn put_input_log(&self, container: Vec<u8>) -> ClientResult<(LogId, bool)> {
        let inner = self.inner.clone();
        with_retry(|| {
            let container = container.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .put_input_log(PutInputLogRequest { container })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                let arr: [u8; 32] = resp
                    .log_id
                    .as_slice()
                    .try_into()
                    .map_err(|_| ClientError::Transport("log_id is not 32 bytes".into()))?;
                Ok((LogId::from_bytes(arr), resp.newly_stored))
            }
        })
        .await
    }

    /// Fetch a stored SILG input-log container, verifying its footer against
    /// the requested `log_id`.
    pub async fn get_input_log(&self, log_id: LogId) -> ClientResult<Vec<u8>> {
        let inner = self.inner.clone();
        with_retry(move || {
            let lid = log_id;
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .get_input_log(GetInputLogRequest {
                        log_id: lid.as_bytes().to_vec(),
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                let container = resp.container;
                verify_input_log_footer(&container, &lid)?;
                Ok(container)
            }
        })
        .await
    }

    // ── tree ──────────────────────────────────────────────────────────────

    /// Create a node.  Idempotent on `(experiment_id, node_id)`.
    pub async fn create_node(&self, req: CreateNodeRequest) -> ClientResult<NodeMeta> {
        let inner = self.inner.clone();
        with_retry(|| {
            let req = req.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .create_node(req)
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                resp.node
                    .ok_or_else(|| ClientError::Transport("create_node returned empty node".into()))
            }
        })
        .await
    }

    /// Bulk update nodes in one transaction.  NOT retried if the response
    /// indicates `MissingNodes` — that surfaces to the caller.
    pub async fn update_nodes(
        &self,
        experiment_id: String,
        updates: Vec<NodeUpdate>,
    ) -> ClientResult<u64> {
        let inner = self.inner.clone();
        // UpdateNodes is NOT listed as idempotent in the plan; it modifies
        // mutable state (visit counts, status). Retry only on transient.
        with_retry(|| {
            let experiment_id = experiment_id.clone();
            let updates = updates.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .update_nodes(UpdateNodesRequest {
                        experiment_id,
                        updates,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.updated_at)
            }
        })
        .await
    }

    /// Fetch a single node by id.
    pub async fn get_node(&self, experiment_id: String, node_id: u64) -> ClientResult<NodeMeta> {
        let inner = self.inner.clone();
        with_retry(|| {
            let experiment_id = experiment_id.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .get_node(GetNodeRequest {
                        experiment_id,
                        node_id,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                resp.node
                    .ok_or_else(|| ClientError::Transport("get_node returned empty node".into()))
            }
        })
        .await
    }

    /// Fetch all direct children of a node.
    pub async fn get_children(
        &self,
        experiment_id: String,
        node_id: u64,
    ) -> ClientResult<Vec<NodeMeta>> {
        let inner = self.inner.clone();
        with_retry(|| {
            let experiment_id = experiment_id.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .get_children(GetChildrenRequest {
                        experiment_id,
                        node_id,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.nodes)
            }
        })
        .await
    }

    /// Fetch the root-first path from the experiment root to `node_id`.
    ///
    /// When `include_logs` is true, each `PathElement` may carry an
    /// `input_log_container`; those containers are footer-verified before
    /// being returned.
    pub async fn get_path(
        &self,
        experiment_id: String,
        node_id: u64,
        include_logs: bool,
    ) -> ClientResult<Vec<PathElement>> {
        let inner = self.inner.clone();
        with_retry(|| {
            let experiment_id = experiment_id.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .get_path(GetPathRequest {
                        experiment_id,
                        node_id,
                        include_logs,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();

                // Footer-verify any attached input_log containers.
                for elem in &resp.elements {
                    let container = &elem.input_log_container;
                    if !container.is_empty() {
                        // Derive the log_id from the container's own footer.
                        let expected =
                            snapstore_manifest::input_log::InputLogContainer::log_id(container);
                        // Recompute the actual footer hash.
                        let footer_start = container.len().saturating_sub(32);
                        let actual: [u8; 32] =
                            container[footer_start..].try_into().map_err(|_| {
                                ClientError::Transport(
                                    "input_log_container too short for footer".into(),
                                )
                            })?;
                        // log_id *is* the body hash; verify footer == blake3(body).
                        let body_hash = blake3::hash(&container[..footer_start]);
                        if body_hash.as_bytes() != &actual {
                            return Err(ClientError::CorruptInputLog {
                                expected: hex(expected.as_bytes()),
                                actual: hex(&actual),
                            });
                        }
                    }
                }
                Ok(resp.elements)
            }
        })
        .await
    }

    /// Filtered scan of nodes. Returns all pages collected from the stream.
    pub async fn query_nodes(&self, req: QueryNodesRequest) -> ClientResult<Vec<NodeMeta>> {
        let inner = self.inner.clone();
        with_retry(|| {
            let req = req.clone();
            let mut inner = inner.clone();
            async move {
                use tokio_stream::StreamExt;

                let mut stream = inner
                    .query_nodes(req)
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                let mut nodes = Vec::new();
                while let Some(msg) = stream.next().await {
                    let msg = msg.map_err(decode_status)?;
                    nodes.extend(msg.nodes);
                }
                Ok(nodes)
            }
        })
        .await
    }

    // ── metadata KV ──────────────────────────────────────────────────────

    /// Upsert a key-value pair.
    ///
    /// - `expected_generation = None` → unconditional upsert; **retried** on
    ///   transient errors.
    /// - `expected_generation = Some(g)` → CAS; **never retried**;
    ///   `FAILED_PRECONDITION` surfaces as `ClientError::CasFailed`.
    pub async fn put_metadata(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_generation: Option<u64>,
    ) -> ClientResult<u64> {
        let is_cas = expected_generation.is_some();
        let inner = self.inner.clone();

        let do_call = || {
            let key = key.clone();
            let value = value.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .put_metadata(PutMetadataRequest {
                        key,
                        value,
                        expected_generation,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.generation)
            }
        };

        if is_cas {
            // CAS: single attempt only.
            do_call().await
        } else {
            with_retry(do_call).await
        }
    }

    /// Fetch a key-value pair. Returns `(value, generation)`.
    pub async fn get_metadata(&self, key: Vec<u8>) -> ClientResult<(Vec<u8>, u64)> {
        let inner = self.inner.clone();
        with_retry(|| {
            let key = key.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .get_metadata(GetMetadataRequest { key })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok((resp.value, resp.generation))
            }
        })
        .await
    }

    /// Delete a key-value pair.
    ///
    /// - Without `expected_generation`: unconditional; retried on transient.
    /// - With `expected_generation`: CAS; never retried.
    pub async fn delete_metadata(
        &self,
        key: Vec<u8>,
        expected_generation: Option<u64>,
    ) -> ClientResult<bool> {
        let is_cas = expected_generation.is_some();
        let inner = self.inner.clone();

        let do_call = || {
            let key = key.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .delete_metadata(DeleteMetadataRequest {
                        key,
                        expected_generation,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.deleted)
            }
        };

        if is_cas {
            do_call().await
        } else {
            with_retry(do_call).await
        }
    }

    // ── lifecycle ────────────────────────────────────────────────────────

    /// Prune the subtree rooted at `node_id`.
    pub async fn prune_subtree(
        &self,
        experiment_id: String,
        node_id: u64,
        allow_root: bool,
    ) -> ClientResult<u64> {
        let inner = self.inner.clone();
        with_retry(|| {
            let experiment_id = experiment_id.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .prune_subtree(PruneSubtreeRequest {
                        experiment_id,
                        node_id,
                        allow_root,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.nodes_pruned)
            }
        })
        .await
    }

    /// Pin a snapshot, preventing GC from reclaiming its pages.
    pub async fn pin(&self, snapshot_ref: SnapshotRef, note: String) -> ClientResult<bool> {
        let inner = self.inner.clone();
        with_retry(|| {
            let sr = snapshot_ref.clone();
            let note = note.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .pin(PinRequest {
                        snapshot_ref: sr.to_bytes().to_vec(),
                        note,
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.newly_pinned)
            }
        })
        .await
    }

    /// Unpin a snapshot.
    pub async fn unpin(&self, snapshot_ref: SnapshotRef) -> ClientResult<bool> {
        let inner = self.inner.clone();
        with_retry(|| {
            let sr = snapshot_ref.clone();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .unpin(UnpinRequest {
                        snapshot_ref: sr.to_bytes().to_vec(),
                    })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp.was_pinned)
            }
        })
        .await
    }

    /// Fetch store and optionally experiment stats.
    pub async fn stats(&self, experiment_id: Option<String>) -> ClientResult<StatsResponse> {
        let inner = self.inner.clone();
        with_retry(|| {
            let exp = experiment_id.clone().unwrap_or_default();
            let mut inner = inner.clone();
            async move {
                let resp = inner
                    .stats(crate::snapstore_proto::StatsRequest { experiment_id: exp })
                    .await
                    .map_err(decode_status)?
                    .into_inner();
                Ok(resp)
            }
        })
        .await
    }

    /// Trigger GC.  Returns `UNIMPLEMENTED` until M7.
    pub async fn trigger_gc(&self) -> ClientResult<()> {
        let mut inner = self.inner.clone();
        inner
            .trigger_gc(crate::snapstore_proto::TriggerGcRequest {})
            .await
            .map_err(decode_status)?;
        Ok(())
    }

    // ── composite helpers ─────────────────────────────────────────────────

    /// Build a `.spm` container from raw page data and store it.
    ///
    /// This method:
    /// 1. Hashes every page locally.
    /// 2. Calls `put_pages` with all pages.
    /// 3. Builds the container via `build_snapshot_container`.
    /// 4. Calls `put_snapshot`.
    ///
    /// The pages are sent unconditionally (deduplication happens server-side).
    ///
    /// **Note**: depends on `snapstore-manifest` — see the documented deviation
    /// in `helpers.rs`.
    pub async fn put_snapshot_from_parts(
        &self,
        parent: Option<&SnapshotRef>,
        guest_ram_bytes: u64,
        pages: Vec<(u64, Vec<u8>)>,
        device_blob: DeviceBlob,
    ) -> ClientResult<SnapshotRef> {
        // Upload pages.
        self.put_pages(pages.clone()).await?;

        // Build the container.
        let page_refs: Vec<(u64, &[u8; 4096])> = pages
            .iter()
            .map(|(idx, data)| {
                let arr: &[u8; 4096] = data.as_slice().try_into().expect("already validated 4096");
                (*idx, arr)
            })
            .collect();

        let container =
            helpers::build_snapshot_container(parent, guest_ram_bytes, &page_refs, device_blob)
                .map_err(|e| ClientError::Transport(format!("build_snapshot_container: {e}")))?;

        self.put_snapshot(container).await
    }
}

// ── page-channel helpers (Linux only) ────────────────────────────────────────

/// Try to connect a [`PageChannelClient`] from the `page_channel_path` in a
/// `Transport::Auto` config.  Returns `None` if the path is absent or
/// the connect fails (the caller falls back to gRPC).
#[cfg(target_os = "linux")]
fn try_connect_page_channel(
    transport: &Transport,
) -> Option<std::sync::Arc<snapstore_localpath::client::PageChannelClient>> {
    use crate::transport::Transport;
    let path = match transport {
        Transport::Auto {
            page_channel_path: Some(p),
            ..
        } => p,
        _ => return None,
    };
    if !path.exists() {
        return None;
    }
    match snapstore_localpath::client::PageChannelClient::connect(path) {
        Ok(c) => {
            tracing::debug!(path = %path.display(), "page-channel connected");
            Some(std::sync::Arc::new(c))
        }
        Err(e) => {
            tracing::debug!(path = %path.display(), err = %e, "page-channel connect failed; will use gRPC");
            None
        }
    }
}

/// Upload `pages` via the page channel, chunking into PUT_BATCH_MAX_PAGES
/// batches.  Returns the cumulative `(pages_new, pages_deduped)`.
///
/// Called synchronously from an async context: this is a blocking call.
/// For the scale targeted here (co-located clients) this is acceptable;
/// a future version can move this to `spawn_blocking`.
#[cfg(target_os = "linux")]
fn put_pages_via_channel(
    pc: &snapstore_localpath::client::PageChannelClient,
    pages: &[(u64, Vec<u8>)],
) -> Result<(u64, u64), snapstore_localpath::ChannelError> {
    use snapstore_localpath::proto::PUT_BATCH_MAX_PAGES;

    let mut total_new: u64 = 0;
    let mut total_deduped: u64 = 0;

    for chunk in pages.chunks(PUT_BATCH_MAX_PAGES as usize) {
        // Build &[&[u8; 4096]] from the chunk.
        let page_refs: Vec<&[u8; 4096]> = chunk
            .iter()
            .map(|(_, data)| {
                let arr: &[u8; 4096] = data.as_slice().try_into().expect("already validated 4096");
                arr
            })
            .collect();
        let outcome = pc.put_batch(&page_refs)?;
        total_new += outcome.pages_new as u64;
        total_deduped += outcome.pages_deduped as u64;
    }

    Ok((total_new, total_deduped))
}

// ── footer verification helpers ───────────────────────────────────────────────

/// Verify a `.spm` container's BLAKE3 footer against the expected
/// `SnapshotRef`.  The footer IS the blake3(body), so this checks
/// `blake3(container[..len-32]) == snapshot_ref.to_bytes()`.
fn verify_container_footer(container: &[u8], snapshot_ref: &SnapshotRef) -> ClientResult<()> {
    if container.len() < 32 {
        return Err(ClientError::corrupt_snapshot(
            &snapshot_ref.to_bytes(),
            b"",
            "get_snapshot: container too short",
        ));
    }
    let footer_start = container.len() - 32;
    let body_hash = blake3::hash(&container[..footer_start]);
    let stored_footer: [u8; 32] = container[footer_start..]
        .try_into()
        .expect("length checked above");
    let expected_bytes = snapshot_ref.to_bytes();
    if body_hash.as_bytes() != &stored_footer {
        return Err(ClientError::corrupt_snapshot(
            &expected_bytes,
            &stored_footer,
            "footer mismatch: stored hash differs from computed hash",
        ));
    }
    if body_hash.as_bytes() != &expected_bytes {
        return Err(ClientError::corrupt_snapshot(
            &expected_bytes,
            body_hash.as_bytes(),
            "get_snapshot: container hash does not match requested ref",
        ));
    }
    Ok(())
}

/// Verify an input-log container's BLAKE3 footer against the expected `LogId`.
fn verify_input_log_footer(container: &[u8], log_id: &LogId) -> ClientResult<()> {
    if container.len() < 32 {
        return Err(ClientError::CorruptInputLog {
            expected: hex(log_id.as_bytes()),
            actual: "<container too short>".into(),
        });
    }
    let footer_start = container.len() - 32;
    let body_hash = blake3::hash(&container[..footer_start]);
    let expected_bytes = log_id.to_bytes();
    if body_hash.as_bytes() != &expected_bytes {
        return Err(ClientError::CorruptInputLog {
            expected: hex(&expected_bytes),
            actual: hex(body_hash.as_bytes()),
        });
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
