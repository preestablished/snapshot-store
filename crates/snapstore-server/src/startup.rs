//! Server startup sequence.
//!
//! Performs, in order:
//! 1. `STORE_VERSION` check (write `1\n` on first init; refuse mismatch).
//! 2. Open meta DB at `<data_root>/meta/tree.db`; run `PRAGMA integrity_check`.
//! 3. Open `SnapshotStore` (pagestore recovery runs inside).
//! 4. Walk `manifests/`: remove `.spm` files with bad footers; count violations.
//! 5. Reconcile: for each experiment / node, verify `snapshot_ref` resolves
//!    to a stored manifest — missing ⇒ mark node `PRUNED`, log loudly.

use crate::config::ServerConfig;
use crate::metrics::Metrics;
use snapstore_meta::{MetaConfig, MetaDb, NodeUpdate};
use snapstore_pagestore::StoreOptions as PageStoreOptions;
use snapstore_store::{SnapshotStore, StoreOpts};
use snapstore_types::NodeStatus;
use std::fs;
use std::io::Write;
use std::path::Path;

const STORE_VERSION: &str = "1\n";
const STORE_VERSION_FILE: &str = "STORE_VERSION";

#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("STORE_VERSION mismatch: expected 1, found {found:?}")]
    VersionMismatch { found: String },
    #[error("meta integrity_check failed: {0}")]
    MetaIntegrity(String),
    #[error("meta open failed: {0}")]
    MetaOpen(snapstore_meta::MetaError),
    #[error("store open failed: {0}")]
    StoreOpen(snapstore_store::StoreError),
}

/// Fully-opened, reconciled server state.
pub struct ServerState {
    pub store: SnapshotStore,
    pub meta: MetaDb,
}

/// Run the startup sequence and return an open, reconciled `ServerState`.
pub fn run_startup(config: &ServerConfig, metrics: &Metrics) -> Result<ServerState, StartupError> {
    let data_root = &config.data_root;
    fs::create_dir_all(data_root)?;

    // ── Step 1: STORE_VERSION ─────────────────────────────────────────────────
    let version_path = data_root.join(STORE_VERSION_FILE);
    if version_path.exists() {
        let content = fs::read_to_string(&version_path)?;
        if content != STORE_VERSION {
            return Err(StartupError::VersionMismatch { found: content });
        }
    } else {
        let mut f = std::fs::File::create(&version_path)?;
        f.write_all(STORE_VERSION.as_bytes())?;
        f.sync_all()?;
    }

    // ── Step 2: open meta DB ──────────────────────────────────────────────────
    let meta_dir = data_root.join("meta");
    fs::create_dir_all(&meta_dir)?;
    let meta_path = meta_dir.join("tree.db");

    let meta_config = MetaConfig {
        input_log_max_bytes: config
            .meta
            .input_log_max_bytes
            .unwrap_or(snapstore_meta::DEFAULT_INPUT_LOG_MAX_BYTES),
    };
    let meta = MetaDb::open_with_config(&meta_path, meta_config).map_err(StartupError::MetaOpen)?;

    meta.integrity_check()
        .map_err(|e| StartupError::MetaIntegrity(e.to_string()))?;

    // ── Step 3: open SnapshotStore ────────────────────────────────────────────
    let store_dir = data_root.join("store");
    fs::create_dir_all(&store_dir)?;

    let ps_opts = PageStoreOptions {
        write_buf_size: config.pagestore.write_buf_size.unwrap_or(4 * 1024 * 1024),
        max_pack_bytes: config
            .pagestore
            .max_pack_bytes
            .unwrap_or(snapstore_pagestore::pack::PACK_MAX_BYTES),
        read_handle_cap: config.pagestore.read_handle_cap.unwrap_or(256),
    };
    let store_opts = StoreOpts {
        pagestore: ps_opts,
        ..StoreOpts::default()
    };
    // SnapshotStore::open_with_options already cleans its own tmp/ directory
    // (via clean_tmp_dir at open time) — no duplication needed here.
    let store = SnapshotStore::open_with_options(&store_dir, store_opts)
        .map_err(StartupError::StoreOpen)?;

    // ── Step 4: remove .spm files with bad footers ────────────────────────────
    let manifests_dir = store_dir.join("manifests");
    if manifests_dir.exists() {
        purge_bad_footer_manifests(&manifests_dir, metrics);
    }

    // ── Step 5: reconcile snapshot_refs ──────────────────────────────────────
    reconcile_snapshot_refs(&store, &meta, metrics);

    Ok(ServerState { store, meta })
}

// ── Step 4 helper ─────────────────────────────────────────────────────────────

/// Walk `manifests/` and remove any `.spm` file whose BLAKE3 footer or
/// filename does not match the content hash.
fn purge_bad_footer_manifests(manifests_dir: &Path, metrics: &Metrics) {
    let Ok(shard_iter) = fs::read_dir(manifests_dir) else {
        return;
    };
    for shard_entry in shard_iter.flatten() {
        let Ok(ft) = shard_entry.file_type() else {
            continue;
        };
        if !ft.is_dir() {
            continue;
        }
        let Ok(file_iter) = fs::read_dir(shard_entry.path()) else {
            continue;
        };
        for entry in file_iter.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("spm") {
                continue;
            }
            if let Some(violation) = check_spm_integrity(&path) {
                tracing::error!(
                    path = %path.display(),
                    violation = %violation,
                    "removing corrupt .spm file"
                );
                if let Err(e) = fs::remove_file(&path) {
                    tracing::error!(path = %path.display(), err = %e, "failed to remove bad .spm");
                }
                metrics.integrity_errors.inc();
            }
        }
    }
}

/// Returns `Some(violation_reason)` if the manifest fails integrity checks.
///
/// Checks:
/// 1. `blake3(body) == footer` (where body = bytes[..len-32])
/// 2. The 64-hex filename stem equals the hash.
fn check_spm_integrity(path: &Path) -> Option<String> {
    // Get expected ref from filename.
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    if stem.len() != 64 {
        return Some(format!("filename stem is not 64 hex chars: {stem}"));
    }
    let expected_ref = hex_to_32(stem)?;

    let bytes = fs::read(path).ok()?;
    if bytes.len() < 32 {
        return Some("file too short to contain footer".to_string());
    }

    // Check 1: footer == blake3(body).
    let body = &bytes[..bytes.len() - 32];
    let computed = *blake3::hash(body).as_bytes();
    let stored_footer: [u8; 32] = bytes[bytes.len() - 32..].try_into().ok()?;
    if computed != stored_footer {
        return Some("blake3(body) != stored footer".to_string());
    }

    // Check 2: filename ref matches computed hash.
    if computed != expected_ref {
        return Some("computed hash != filename ref".to_string());
    }

    None
}

fn hex_to_32(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ── Step 5 helper ─────────────────────────────────────────────────────────────

/// For each experiment × node: if `snapshot_ref` is not a stored manifest,
/// mark the node `PRUNED` via `update_nodes` and bump the integrity counter.
fn reconcile_snapshot_refs(store: &SnapshotStore, meta: &MetaDb, metrics: &Metrics) {
    let experiments = match meta.list_experiments() {
        Ok(e) => e,
        Err(err) => {
            tracing::error!(err = %err, "reconcile: list_experiments failed");
            return;
        }
    };

    for exp_id in &experiments {
        let mut cursor: Option<u64> = None;
        loop {
            let mut filter = snapstore_meta::QueryFilter::new(exp_id.clone());
            filter.created_after = cursor;
            filter.limit = Some(512);

            let nodes = match meta.query_nodes(filter) {
                Ok(n) => n,
                Err(err) => {
                    tracing::error!(experiment = %exp_id, err = %err, "reconcile: query_nodes failed");
                    break;
                }
            };
            if nodes.is_empty() {
                break;
            }
            let last_at = nodes.last().unwrap().created_at;

            for node in &nodes {
                if node.status == NodeStatus::Pruned {
                    // Already pruned — no action needed.
                    continue;
                }
                let snap_ref = &node.snapshot_ref;
                if !store.has_manifest(snap_ref) {
                    tracing::error!(
                        experiment = %exp_id,
                        node_id = node.node_id.0,
                        snapshot_ref = ?snap_ref,
                        "reconcile: snapshot_ref not in store — marking node PRUNED"
                    );
                    let update = NodeUpdate {
                        node_id: node.node_id,
                        status: Some(NodeStatus::Pruned),
                        ..NodeUpdate::new(node.node_id)
                    };
                    if let Err(e) = meta.update_nodes(exp_id.clone(), vec![update]) {
                        tracing::error!(
                            experiment = %exp_id,
                            node_id = node.node_id.0,
                            err = %e,
                            "reconcile: failed to mark node PRUNED"
                        );
                    }
                    metrics.integrity_errors.inc();
                }
            }

            if nodes.len() < 512 {
                break; // last page
            }
            cursor = Some(last_at);
        }
    }
}
