//! Async `SnapstoreClient` — typed methods over every RPC in the service.

use bytes::Bytes;
use snapstore_manifest::DeviceBlob;
use snapstore_types::{LogId, PageHash, SnapshotRef, PAGE_SIZE};
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
/// exists and connects, bulk page bytes may move over the SEQPACKET channel:
/// `put_pages` sends page payloads through `PUT_BATCH`, and
/// `resolve_pages(..., hashes_only=false)` receives committed page hashes over
/// gRPC before fetching payloads through `GET_BATCH`. Metadata and control flow
/// stay on gRPC, and every operation keeps its pure-gRPC equivalent.
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
                let mut chunk_pages = Vec::<Vec<u8>>::with_capacity(256);

                for (_, data) in &pages {
                    let ph = blake3::hash(data);
                    local_hasher.update(ph.as_bytes());
                    chunk_pages.push(data.clone());
                    if chunk_pages.len() == 256 {
                        messages.push(PutPagesRequest {
                            pages: std::mem::take(&mut chunk_pages),
                        });
                        chunk_pages.reserve(256);
                    }
                }
                if !chunk_pages.is_empty() {
                    messages.push(PutPagesRequest { pages: chunk_pages });
                }

                let local_batch_hash = *local_hasher.finalize().as_bytes();

                // All messages are already materialized: hand tonic a plain
                // iterator stream. Pre-filling a bounded channel here deadlocks
                // once the message count exceeds the capacity, because nothing
                // drains the receiver until `put_pages` is awaited (>16 chunks
                // = >4096 pages hung forever; bead 0vl). Two consequences a
                // future edit must keep: `messages` (~the full page payload)
                // stays resident for the RPC's duration, and it is rebuilt on
                // every retry attempt because the iterator stream is
                // single-use — construction must stay inside this closure.
                let response = inner
                    .put_pages(tokio_stream::iter(messages))
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
    ///
    /// # Page-channel fast path
    ///
    /// On Linux, when the client was constructed via `Transport::Auto` with a
    /// connected `page_channel_path` and `hashes_only` is false, metadata and
    /// ordered page hashes are first resolved through gRPC with
    /// `hashes_only=true`. Payload bytes are then fetched by hash over
    /// page-channel `GET_BATCH`, validated against the resolved hashes, and
    /// attached to the returned entries. Ordinary channel failures fall back to
    /// the pure-gRPC payload stream; consistency or integrity failures do not.
    pub async fn resolve_pages(
        &self,
        snapshot_ref: SnapshotRef,
        baseline_ref: Option<SnapshotRef>,
        hashes_only: bool,
    ) -> ClientResult<Vec<(u64, PageHash, Option<Bytes>)>> {
        #[cfg(target_os = "linux")]
        if !hashes_only {
            if let Some(ref pc) = self.page_channel {
                let resolved = self
                    .resolve_pages_via_grpc(snapshot_ref.clone(), baseline_ref.clone(), true)
                    .await?;
                if resolved.is_empty() {
                    return Ok(resolved);
                }

                let resolved_hashes: Vec<(u64, PageHash)> = resolved
                    .iter()
                    .map(|(page_index, page_hash, _)| (*page_index, *page_hash))
                    .collect();
                match resolve_payloads_via_channel(pc.as_ref(), &resolved_hashes) {
                    Ok(payloads) => {
                        let with_payloads = resolved_hashes
                            .into_iter()
                            .zip(payloads)
                            .map(|((page_index, page_hash), payload)| {
                                (page_index, page_hash, Some(payload))
                            })
                            .collect();
                        return Ok(with_payloads);
                    }
                    Err(ResolveViaChannelError::Fatal(err)) => return Err(err),
                    Err(ResolveViaChannelError::Fallback(err)) => {
                        tracing::warn!(
                            err = %err,
                            "page-channel: GET_BATCH failed, falling back to gRPC resolve_pages"
                        );
                    }
                }
            }
        }

        self.resolve_pages_via_grpc(snapshot_ref, baseline_ref, hashes_only)
            .await
    }

    async fn resolve_pages_via_grpc(
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

#[cfg(target_os = "linux")]
enum ResolveViaChannelError {
    Fallback(snapstore_localpath::ChannelError),
    Fatal(ClientError),
}

/// Fetch resolved page payloads by hash through page-channel `GET_BATCH`.
///
/// The helper uses dense ordinal slots rather than page indexes so sparse guest
/// address spaces cannot imply large client-side allocations.
#[cfg(target_os = "linux")]
fn resolve_payloads_via_channel(
    pc: &snapstore_localpath::client::PageChannelClient,
    resolved: &[(u64, PageHash)],
) -> Result<Vec<Bytes>, ResolveViaChannelError> {
    use snapstore_localpath::proto::GET_BATCH_MAX_PER_DATAGRAM;

    if resolved.is_empty() {
        return Ok(Vec::new());
    }

    let mut channel_results = Vec::with_capacity(resolved.len());
    for (chunk_index, chunk) in resolved
        .chunks(GET_BATCH_MAX_PER_DATAGRAM as usize)
        .enumerate()
    {
        let base = chunk_index * GET_BATCH_MAX_PER_DATAGRAM as usize;
        let reqs: Vec<(PageHash, u64)> = chunk
            .iter()
            .enumerate()
            .map(|(i, (_, page_hash))| (*page_hash, (base + i) as u64))
            .collect();
        let mut got = pc.get_batch(&reqs).map_err(classify_get_batch_error)?;
        channel_results.append(&mut got);
    }

    attach_payloads_from_channel_results(resolved, channel_results)
        .map_err(ResolveViaChannelError::Fatal)
}

#[cfg(target_os = "linux")]
fn classify_get_batch_error(err: snapstore_localpath::ChannelError) -> ResolveViaChannelError {
    use snapstore_localpath::{
        proto::ErrorCode,
        ChannelError::{CrossCheckMismatch, Io, Peer, Protocol, Unsupported, Wire},
    };

    match err {
        Io(e) => ResolveViaChannelError::Fallback(Io(e)),
        Unsupported => ResolveViaChannelError::Fallback(Unsupported),
        Peer {
            code: ErrorCode::Overload,
            detail,
        } => ResolveViaChannelError::Fallback(Peer {
            code: ErrorCode::Overload,
            detail,
        }),
        Protocol(msg) if msg.contains("server closed connection") => {
            ResolveViaChannelError::Fallback(Protocol(msg))
        }
        Peer {
            code: ErrorCode::NotFound,
            detail,
        } => ResolveViaChannelError::Fatal(ClientError::corrupt_payload(
            "page-channel GET_BATCH returned NotFound for a hash from ResolvePages",
            "all committed ResolvePages hashes readable through the connected page channel",
            detail,
        )),
        Peer { code, detail } => ResolveViaChannelError::Fatal(ClientError::corrupt_payload(
            "page-channel GET_BATCH peer error",
            "valid GET_BATCH_DATA response",
            format!("{code:?}: {detail}"),
        )),
        Wire(e) => ResolveViaChannelError::Fatal(ClientError::corrupt_payload(
            "page-channel GET_BATCH wire error",
            "valid GET_BATCH_DATA response",
            e.to_string(),
        )),
        Protocol(msg) => ResolveViaChannelError::Fatal(ClientError::corrupt_payload(
            "page-channel GET_BATCH protocol violation",
            "valid GET_BATCH_DATA response",
            msg,
        )),
        CrossCheckMismatch { expected, actual } => {
            ResolveViaChannelError::Fatal(ClientError::BatchBlake3Mismatch { expected, actual })
        }
    }
}

#[cfg(target_os = "linux")]
fn attach_payloads_from_channel_results(
    resolved: &[(u64, PageHash)],
    channel_results: Vec<(u64, Vec<u8>)>,
) -> ClientResult<Vec<Bytes>> {
    if channel_results.len() != resolved.len() {
        return Err(ClientError::corrupt_payload(
            "page-channel GET_BATCH scatter cardinality mismatch",
            resolved.len().to_string(),
            channel_results.len().to_string(),
        ));
    }

    let mut payloads: Vec<Option<Bytes>> = vec![None; resolved.len()];
    for (dst_slot, payload) in channel_results {
        let slot = usize::try_from(dst_slot).map_err(|_| {
            ClientError::corrupt_payload(
                "page-channel GET_BATCH returned dst_slot outside usize range",
                format!("0..{}", resolved.len()),
                dst_slot.to_string(),
            )
        })?;
        if slot >= resolved.len() {
            return Err(ClientError::corrupt_payload(
                "page-channel GET_BATCH returned out-of-range dst_slot",
                format!("0..{}", resolved.len()),
                dst_slot.to_string(),
            ));
        }
        if payloads[slot].is_some() {
            return Err(ClientError::corrupt_payload(
                "page-channel GET_BATCH returned duplicate dst_slot",
                "unique dense ordinal slots",
                dst_slot.to_string(),
            ));
        }
        if payload.len() != PAGE_SIZE {
            return Err(ClientError::corrupt_payload(
                format!(
                    "page-channel GET_BATCH returned wrong payload length for page_index {}",
                    resolved[slot].0
                ),
                PAGE_SIZE.to_string(),
                payload.len().to_string(),
            ));
        }

        let actual = blake3::hash(&payload);
        let expected = resolved[slot].1;
        if actual.as_bytes() != expected.as_bytes() {
            return Err(ClientError::corrupt_payload(
                format!(
                    "page-channel GET_BATCH payload hash mismatch for page_index {}",
                    resolved[slot].0
                ),
                hex(expected.as_bytes()),
                hex(actual.as_bytes()),
            ));
        }

        payloads[slot] = Some(Bytes::from(payload));
    }

    payloads
        .into_iter()
        .enumerate()
        .map(|(slot, payload)| {
            payload.ok_or_else(|| {
                ClientError::corrupt_payload(
                    "page-channel GET_BATCH missing dst_slot",
                    slot.to_string(),
                    "<missing>".to_owned(),
                )
            })
        })
        .collect()
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

#[cfg(all(test, target_os = "linux"))]
mod page_channel_resolve_tests {
    use super::*;

    fn page(seed: u8) -> Vec<u8> {
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = seed;
        data[PAGE_SIZE - 1] = seed.wrapping_mul(17);
        data
    }

    fn hash(data: &[u8]) -> PageHash {
        PageHash::from_bytes(*blake3::hash(data).as_bytes())
    }

    fn assert_corrupt_context(err: ClientError, expected_context: &str) {
        match err {
            ClientError::CorruptPayload(detail) => {
                assert!(
                    detail.context.contains(expected_context),
                    "context {:?} did not contain {:?}",
                    detail.context,
                    expected_context
                );
            }
            other => panic!("expected CorruptPayload, got {other:?}"),
        }
    }

    #[test]
    fn attach_payloads_uses_dense_ordinals_for_sparse_duplicate_hashes() {
        let p0 = page(1);
        let p2 = page(2);
        let resolved = vec![(0, hash(&p0)), (1_000_000, hash(&p0)), (7, hash(&p2))];
        let payloads = attach_payloads_from_channel_results(
            &resolved,
            vec![(2, p2.clone()), (0, p0.clone()), (1, p0.clone())],
        )
        .expect("valid scatter");

        assert_eq!(payloads.len(), 3);
        assert_eq!(payloads[0], Bytes::from(p0.clone()));
        assert_eq!(payloads[1], Bytes::from(p0));
        assert_eq!(payloads[2], Bytes::from(p2));
    }

    #[test]
    fn attach_payloads_rejects_duplicate_slots() {
        let p0 = page(1);
        let p1 = page(2);
        let resolved = vec![(0, hash(&p0)), (1, hash(&p1))];
        let err = attach_payloads_from_channel_results(&resolved, vec![(0, p0.clone()), (0, p0)])
            .expect_err("duplicate slot must fail");
        assert_corrupt_context(err, "duplicate dst_slot");
    }

    #[test]
    fn attach_payloads_rejects_out_of_range_slots() {
        let p0 = page(1);
        let resolved = vec![(0, hash(&p0))];
        let err = attach_payloads_from_channel_results(&resolved, vec![(4, p0)])
            .expect_err("out-of-range slot must fail");
        assert_corrupt_context(err, "out-of-range dst_slot");
    }

    #[test]
    fn attach_payloads_rejects_missing_slots() {
        let p0 = page(1);
        let p1 = page(2);
        let resolved = vec![(0, hash(&p0)), (1, hash(&p1))];
        let err = attach_payloads_from_channel_results(&resolved, vec![(0, p0)])
            .expect_err("missing slot must fail");
        assert_corrupt_context(err, "cardinality mismatch");
    }

    #[test]
    fn attach_payloads_rejects_wrong_length() {
        let p0 = page(1);
        let mut short = p0.clone();
        short.pop();
        let resolved = vec![(0, hash(&p0))];
        let err = attach_payloads_from_channel_results(&resolved, vec![(0, short)])
            .expect_err("wrong length must fail");
        assert_corrupt_context(err, "wrong payload length");
    }

    #[test]
    fn attach_payloads_rejects_hash_mismatch() {
        let p0 = page(1);
        let p1 = page(2);
        let resolved = vec![(0, hash(&p0))];
        let err = attach_payloads_from_channel_results(&resolved, vec![(0, p1)])
            .expect_err("hash mismatch must fail");
        assert_corrupt_context(err, "payload hash mismatch");
    }
}
