//! End-to-end synthetic exploration test — M4 gate S1.
//!
//! Two concurrent experiments ("exp-a", "exp-b"), each driven by its own
//! tokio task + SyntheticGuest + SnapstoreClient over UDS, for `steps/2`
//! steps each.
//!
//! Per-step actions:
//!   * PutPages (gRPC) for dirty pages of the delta container
//!   * PutInputLog (small SILG container)
//!   * PutSnapshot (FULL at step 0, DELTA at step > 0)
//!   * CreateNode (with idempotent replay every ~10 steps)
//!   * Every 8 steps: UpdateNodes batch on sample of prior nodes
//!   * Every 25 steps: QueryNodes cursor scan (cursor contract: no gaps/dupes)
//!   * Every 50 steps: GetPath spot check
//!   * Every 16 steps: PutMetadata CAS write
//!
//! Final checks:
//!   * Per-experiment Stats == driver bookkeeping
//!   * Store Stats: manifests_total, logical_page_bytes, ingested counters
//!   * tonic-health SERVING over the same UDS
//!   * Prometheus /metrics HTTP scrape: pages_ingested{new}+{dup} == totals
//!   * ResolvePages sanity: Mode A (full coverage) + Mode B (vs ancestor)
//!
//! Default step count: 400 (PR CI).  Sign-off: E2E_STEPS=10000 + --ignored.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use snapstore_client::helpers::{build_input_log_container, build_snapshot_container};
use snapstore_client::{SnapstoreClient, Transport};
// Use the client's proto types for SnapstoreClient calls (same proto, different crate instance).
use snapstore_client::snapstore_proto::{
    CreateNodeRequest, NodeUpdate as ProtoNodeUpdate, QueryNodesRequest,
};
use snapstore_manifest::DeviceBlob;
use snapstore_server::{
    build_server::serve_for_tests_with_metrics, config::ServerConfig, metrics::Metrics,
};
use snapstore_testgen::{GuestProfile, SyntheticGuest};
use snapstore_types::{LogId, SnapshotRef, PAGE_SIZE};

// ── helpers ───────────────────────────────────────────────────────────────────

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

/// Build a SnapstoreClient from a UDS path.
async fn make_snapstore_client(uds_path: PathBuf) -> SnapstoreClient {
    SnapstoreClient::connect(Transport::Uds(uds_path))
        .await
        .expect("SnapstoreClient::connect")
}

/// Build a raw gRPC channel for health checking.
async fn make_raw_channel(uds_path: PathBuf) -> Channel {
    Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_uri: tonic::transport::Uri| {
            let path = uds_path.clone();
            async move {
                let stream = UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("raw channel connect")
}

/// Minimal HTTP GET helper using tokio's TcpStream — avoids pulling in reqwest.
async fn http_get_text(url: &str) -> Result<String, String> {
    // Parse the URL minimally: assume http://host:port/path.
    let without_scheme = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("not http: {url}"))?;
    let (host_port, path) = without_scheme
        .split_once('/')
        .map(|(h, p)| (h, format!("/{p}")))
        .unwrap_or((without_scheme, "/".to_string()));

    let mut stream = tokio::net::TcpStream::connect(host_port)
        .await
        .map_err(|e| format!("connect {host_port}: {e}"))?;

    let request = format!("GET {path} HTTP/1.0\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("read: {e}"))?;

    let raw = String::from_utf8_lossy(&buf);
    // Strip the HTTP headers — body is after the first blank line.
    if let Some(pos) = raw.find("\r\n\r\n") {
        Ok(raw[pos + 4..].to_string())
    } else if let Some(pos) = raw.find("\n\n") {
        Ok(raw[pos + 2..].to_string())
    } else {
        Ok(raw.to_string())
    }
}

// ── per-experiment bookkeeping ─────────────────────────────────────────────────

struct ExpBookkeeping {
    /// Snapshot refs in creation order (index == step).
    snap_refs: Vec<SnapshotRef>,
    /// Log ids in creation order.
    #[allow(dead_code)]
    log_ids: Vec<LogId>,
    /// Total pages_new accumulated from PutPages.
    total_new: u64,
    /// Total pages_deduped accumulated from PutPages.
    total_deduped: u64,
    /// guest_ram_bytes (fixed for this experiment).
    guest_ram_bytes: u64,
    /// Next expected CAS generation for the KV checkpoint key.
    kv_generation: u64,
}

impl ExpBookkeeping {
    fn new(total_pages: usize) -> Self {
        Self {
            snap_refs: Vec::new(),
            log_ids: Vec::new(),
            total_new: 0,
            total_deduped: 0,
            guest_ram_bytes: total_pages as u64 * PAGE_SIZE as u64,
            kv_generation: 0,
        }
    }
}

/// Issue FULL snapshots every this many steps to prevent the delta chain from
/// exceeding MAX_CHAIN (4096).  256 gives a safe margin.
const FULL_INTERVAL: usize = 256;

// ── driver function ───────────────────────────────────────────────────────────

/// Run one experiment for `steps` steps.  Returns bookkeeping.
async fn run_experiment(
    exp_id: String,
    steps: usize,
    seed: u64,
    client: SnapstoreClient,
) -> ExpBookkeeping {
    let profile = GuestProfile {
        total_pages: 256,
        ..GuestProfile::idle_linux()
    };
    let mut guest = SyntheticGuest::new(seed, profile);

    let mut bk = ExpBookkeeping::new(guest.total_pages());
    let kv_key = format!("checkpoint:{exp_id}").into_bytes();

    for step in 0..steps {
        // ── advance guest ──────────────────────────────────────────────────
        // Issue FULL snapshots periodically to prevent the delta chain from
        // exceeding MAX_CHAIN (4096).  On FULL steps we still advance the guest
        // so the RNG progression remains deterministic.
        let is_full = step == 0 || step % FULL_INTERVAL == 0;

        // Always advance the guest (even on FULL steps).
        let _dirty = if step == 0 {
            vec![] // no epoch advance at step 0 — initial state
        } else {
            guest.step_epoch()
        };

        // For FULL steps, cover all pages in the current guest state.
        // For delta steps, only the dirty subset.
        let dirty_indices: Vec<u64> = if is_full {
            (0..guest.total_pages() as u64).collect()
        } else {
            _dirty
        };

        // Collect (page_index, page_bytes) from the guest's current state.
        let all_pages: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
        let pages_with_data: Vec<(u64, Vec<u8>)> = dirty_indices
            .iter()
            .map(|&idx| (idx, all_pages[idx as usize].1.to_vec()))
            .collect();

        // ── PutPages ──────────────────────────────────────────────────────
        let (new_cnt, dup_cnt) = client
            .put_pages(pages_with_data.clone())
            .await
            .expect("put_pages");
        bk.total_new += new_cnt;
        bk.total_deduped += dup_cnt;

        // ── Build snapshot container ──────────────────────────────────────
        let page_refs: Vec<(u64, &[u8; PAGE_SIZE])> = pages_with_data
            .iter()
            .map(|(idx, data)| (*idx, data.as_slice().try_into().unwrap()))
            .collect();

        // FULL containers have no parent (reset the delta chain).
        let parent = if is_full {
            None
        } else {
            bk.snap_refs.last().cloned()
        };

        let container = build_snapshot_container(
            parent.as_ref(),
            bk.guest_ram_bytes,
            &page_refs,
            empty_blob(),
        )
        .expect("build_snapshot_container");

        // ── PutInputLog ───────────────────────────────────────────────────
        let log_payload = format!("{exp_id}:step:{step}").into_bytes();
        let log_container = build_input_log_container(1, &log_payload);
        let (log_id, _) = client
            .put_input_log(log_container)
            .await
            .expect("put_input_log");
        bk.log_ids.push(log_id);

        // ── PutSnapshot ───────────────────────────────────────────────────
        let snap_ref = client.put_snapshot(container).await.expect("put_snapshot");
        bk.snap_refs.push(snap_ref.clone());

        // ── CreateNode ────────────────────────────────────────────────────
        let parent_node = if step == 0 {
            None
        } else {
            Some(step as u64 - 1)
        };
        let req = CreateNodeRequest {
            experiment_id: exp_id.clone(),
            node_id: step as u64,
            parent_node_id: parent_node,
            snapshot_ref: snap_ref.to_bytes().to_vec(),
            input_log_id: log_id.as_bytes().to_vec(),
            inline_input_log: vec![],
            status: 1, // FRONTIER
            score: Some(step as f64 * 0.1),
            icount: step as u64 * 1000,
            virtual_ns: step as u64 * 1_000_000,
            attrs: vec![],
        };

        let node_meta = client.create_node(req.clone()).await.expect("create_node");

        // ── Idempotent replay every ~10 steps ─────────────────────────────
        // Replaying the identical request must return the same stored row.
        if step % 10 == 9 {
            let node_meta2 = client
                .create_node(req.clone())
                .await
                .expect("idempotent replay must succeed");
            assert_eq!(
                node_meta2.node_id, node_meta.node_id,
                "idempotent replay: node_id mismatch at step={step} exp={exp_id}"
            );
            assert_eq!(
                node_meta2.created_at, node_meta.created_at,
                "idempotent replay: created_at changed at step={step} exp={exp_id}"
            );

            // Mismatched replay (different snapshot_ref on an existing node) must
            // return AlreadyExists.  The bad snapshot_ref must also be stored (the
            // server validates it before checking the node row).  Use the
            // previous step's snapshot_ref — it is always stored.
            if step > 0 {
                let prev_ref = bk.snap_refs[step - 1].to_bytes().to_vec();
                let bad_req = CreateNodeRequest {
                    snapshot_ref: prev_ref,
                    ..req.clone()
                };
                let err = client
                    .create_node(bad_req)
                    .await
                    .expect_err("mismatched replay must fail");
                assert!(
                    matches!(err, snapstore_client::ClientError::AlreadyExists),
                    "mismatched replay: expected AlreadyExists got {err:?} at step={step} exp={exp_id}"
                );
            }
        }

        // ── UpdateNodes every 8 steps ─────────────────────────────────────
        if step > 0 && step % 8 == 0 {
            let sample_end = step.min(4);
            let updates: Vec<ProtoNodeUpdate> = (0..sample_end)
                .map(|i| ProtoNodeUpdate {
                    node_id: i as u64,
                    status: None,
                    score: Some(i as f64 * 0.5),
                    attrs: None,
                    visit_count_delta: Some(1),
                    touch_visited: true,
                    icount: None,
                    virtual_ns: None,
                })
                .collect();

            client
                .update_nodes(exp_id.clone(), updates)
                .await
                .expect("update_nodes");
        }

        // ── QueryNodes cursor scan every 25 steps ─────────────────────────
        if step > 0 && step % 25 == 0 {
            let mut seen: HashSet<u64> = HashSet::new();
            let mut cursor: Option<u64> = None;
            let page_limit = 10u32;

            loop {
                let nodes = client
                    .query_nodes(QueryNodesRequest {
                        experiment_id: exp_id.clone(),
                        status: None,
                        parent_node_id: None,
                        min_depth: None,
                        max_depth: None,
                        created_after: cursor,
                        updated_after: None,
                        order: 1, // CREATED_AT
                        limit: page_limit,
                    })
                    .await
                    .expect("query_nodes cursor");

                if nodes.is_empty() {
                    break;
                }

                for n in &nodes {
                    assert!(
                        seen.insert(n.node_id),
                        "cursor duplicate: node_id={} exp={exp_id} step={step}",
                        n.node_id
                    );
                }

                cursor = Some(nodes.last().unwrap().created_at);

                if nodes.len() < page_limit as usize {
                    break;
                }
            }

            // No gaps: every node 0..=step must appear.
            let expected = step + 1;
            assert_eq!(
                seen.len(),
                expected,
                "cursor scan: got {} nodes, expected {expected} at step={step} exp={exp_id}",
                seen.len()
            );
            for id in 0..=step as u64 {
                assert!(
                    seen.contains(&id),
                    "cursor scan: missing node_id={id} at step={step} exp={exp_id}"
                );
            }
        }

        // ── GetPath spot check every 50 steps ─────────────────────────────
        if step > 0 && step % 50 == 0 {
            let elements = client
                .get_path(exp_id.clone(), step as u64, false)
                .await
                .expect("get_path");

            // The path goes root-first from the experiment root (node_id=0) to
            // the current node.  Length is always step+1 regardless of FULL
            // snap intervals (the node tree is unchanged; only the snapshot
            // parent chain is periodically reset).
            assert_eq!(
                elements.len(),
                step + 1,
                "get_path len={} expected={} at step={step} exp={exp_id}",
                elements.len(),
                step + 1
            );

            // Verify snapshot_refs match bookkeeping.
            for (i, elem) in elements.iter().enumerate() {
                let expected_ref = bk.snap_refs[i].to_bytes().to_vec();
                let node = elem.node.as_ref().expect("path element has node");
                assert_eq!(
                    node.snapshot_ref, expected_ref,
                    "get_path snap_ref mismatch at pos={i} step={step} exp={exp_id}"
                );
            }
        }

        // ── PutMetadata CAS checkpoint every 16 steps ─────────────────────
        if step % 16 == 0 {
            let value = format!("{exp_id}:checkpoint:{step}").into_bytes();
            let new_gen = client
                .put_metadata(kv_key.clone(), value, Some(bk.kv_generation))
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "put_metadata CAS step={step} exp={exp_id} gen={}: {e:?}",
                        bk.kv_generation
                    )
                });
            bk.kv_generation += 1;
            assert_eq!(
                new_gen, bk.kv_generation,
                "CAS gen mismatch: expected={} got={new_gen} step={step} exp={exp_id}",
                bk.kv_generation
            );
        }
    }

    bk
}

// ── shared test driver ────────────────────────────────────────────────────────

async fn run_e2e(steps: usize) {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(Metrics::new(&registry));

    // Pre-bind the HTTP listener so we know the actual port.
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("http listener bind");
    let http_addr = http_listener.local_addr().expect("local_addr");
    // Drop the listener — the server will re-bind the same port.
    // (Port stays in TIME_WAIT-free state since nothing has used it yet.)
    drop(http_listener);

    let config = ServerConfig {
        data_root: data_root.clone(),
        grpc_tcp_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_uds_path: Some(data_root.join("snapstore.sock")),
        page_channel_path: None,
        http_addr,
        pagestore: Default::default(),
        meta: Default::default(),
        page_channel: Default::default(),
        gc: Default::default(),
    };

    let (handle, uds_path) = serve_for_tests_with_metrics(config, Arc::clone(&metrics), registry)
        .await
        .expect("serve_for_tests_with_metrics");

    let steps_a = steps / 2;
    let steps_b = steps - steps_a;

    let client_a = make_snapstore_client(uds_path.clone()).await;
    let client_b = make_snapstore_client(uds_path.clone()).await;

    // Run both experiments concurrently.
    let (bk_a, bk_b) = tokio::join!(
        run_experiment(
            "exp-a".to_string(),
            steps_a,
            0xDEAD_BEEF_0000_0001u64,
            client_a
        ),
        run_experiment(
            "exp-b".to_string(),
            steps_b,
            0xCAFE_BABE_0000_0002u64,
            client_b
        ),
    );

    // ── Final consistency checks ───────────────────────────────────────────────
    let cc = make_snapstore_client(uds_path.clone()).await;

    // Per-experiment Stats.
    let stats_a = cc
        .stats(Some("exp-a".to_string()))
        .await
        .expect("stats exp-a");
    let stats_b = cc
        .stats(Some("exp-b".to_string()))
        .await
        .expect("stats exp-b");

    let exp_a = stats_a.experiment.as_ref().expect("exp stats for exp-a");
    let exp_b = stats_b.experiment.as_ref().expect("exp stats for exp-b");

    assert_eq!(
        exp_a.nodes_total, steps_a as u64,
        "exp-a nodes_total={} expected={steps_a}",
        exp_a.nodes_total
    );
    assert_eq!(
        exp_b.nodes_total, steps_b as u64,
        "exp-b nodes_total={} expected={steps_b}",
        exp_b.nodes_total
    );

    // Store Stats — read from stats_b (stats_a.store may share the same registry).
    let store = stats_b.store.as_ref().expect("store stats");

    let total_manifests = steps_a + steps_b;
    assert_eq!(
        store.manifests_total, total_manifests as u64,
        "manifests_total={} expected={total_manifests}",
        store.manifests_total
    );

    let expected_logical =
        bk_a.guest_ram_bytes * steps_a as u64 + bk_b.guest_ram_bytes * steps_b as u64;
    assert_eq!(
        store.logical_page_bytes, expected_logical,
        "logical_page_bytes={} expected={expected_logical}",
        store.logical_page_bytes
    );

    // Prometheus /metrics scrape.
    let metrics_body = http_get_text(&format!("http://{http_addr}/metrics"))
        .await
        .expect("GET /metrics");

    let (prom_new, prom_dup) = parse_pages_ingested(&metrics_body);
    let driver_new = bk_a.total_new + bk_b.total_new;
    let driver_dup = bk_a.total_deduped + bk_b.total_deduped;

    assert_eq!(
        prom_new, driver_new,
        "prometheus pages_ingested{{new}}={prom_new} expected={driver_new}"
    );
    assert_eq!(
        prom_dup, driver_dup,
        "prometheus pages_ingested{{dup}}={prom_dup} expected={driver_dup}"
    );

    // tonic-health SERVING check over UDS.
    let raw_ch = make_raw_channel(uds_path.clone()).await;
    let mut health_client = tonic_health::pb::health_client::HealthClient::new(raw_ch);
    let health_resp = health_client
        .check(tonic_health::pb::HealthCheckRequest {
            service: "determinism.snapstore.v1.SnapshotStore".to_string(),
        })
        .await
        .expect("health check");
    assert_eq!(
        health_resp.into_inner().status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
        "health check must report SERVING"
    );

    // ResolvePages sanity.
    if steps_a > 0 {
        // Mode A: full coverage of the final snapshot.
        let final_ref_a = bk_a.snap_refs.last().unwrap().clone();
        let total_pages_a = bk_a.guest_ram_bytes / PAGE_SIZE as u64;

        let resolved_a = cc
            .resolve_pages(final_ref_a.clone(), None, false)
            .await
            .expect("resolve_pages mode A exp-a");
        assert_eq!(
            resolved_a.len(),
            total_pages_a as usize,
            "resolve_pages mode A: len={} expected={}",
            resolved_a.len(),
            total_pages_a
        );

        // Mode B: delta from a recent ancestor that is guaranteed to be in
        // the same chain segment as the final snapshot.  The final snapshot's
        // chain segment starts at the most recent FULL reset (<= FULL_INTERVAL
        // steps back from the last step).
        if steps_a >= 2 {
            // Find the most recent FULL-reset snapshot index.
            let last_full_step = ((steps_a - 1) / FULL_INTERVAL) * FULL_INTERVAL;
            // Pick an ancestor a few steps after the last FULL.
            // This ancestor is guaranteed to be in the same chain as the final snap.
            let ancestor_idx = if last_full_step + 1 < steps_a {
                last_full_step + 1
            } else {
                last_full_step
            };
            if ancestor_idx < steps_a - 1 {
                let ancestor_ref = bk_a.snap_refs[ancestor_idx].clone();
                let delta = cc
                    .resolve_pages(final_ref_a, Some(ancestor_ref), false)
                    .await
                    .expect("resolve_pages mode B exp-a");
                // Delta pages must be <= total pages.
                assert!(
                    delta.len() <= total_pages_a as usize,
                    "resolve_pages mode B: len={} > total pages={}",
                    delta.len(),
                    total_pages_a
                );
            }
        }
    }

    drop(handle);
}

/// Parse `snapstore_pages_ingested_total` from Prometheus text format.
/// Returns `(new, dup)`.
fn parse_pages_ingested(body: &str) -> (u64, u64) {
    let mut new_val = 0u64;
    let mut dup_val = 0u64;
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if !line.contains("snapstore_pages_ingested_total") {
            continue;
        }
        // Value is after the last space.
        let val = line
            .rsplit_once(' ')
            .and_then(|(_, v)| v.trim().parse::<f64>().ok())
            .unwrap_or(0.0) as u64;
        if line.contains(r#"dedup="new""#) {
            new_val = val;
        } else if line.contains(r#"dedup="dup""#) {
            dup_val = val;
        }
    }
    (new_val, dup_val)
}

// ── test entry points ─────────────────────────────────────────────────────────

/// PR CI — reduced step count (default 400).
#[tokio::test(flavor = "multi_thread")]
async fn e2e_exploration() {
    let steps: usize = std::env::var("E2E_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400);
    run_e2e(steps).await;
}

/// 10k sign-off run.
///
/// Run with:
/// ```text
/// E2E_STEPS=10000 cargo test -p snapstore-server --test e2e_exploration --release \
///   -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn e2e_exploration_signoff() {
    let steps: usize = std::env::var("E2E_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .expect("E2E_STEPS must be set for sign-off run (e.g. E2E_STEPS=10000)");
    run_e2e(steps).await;
}
