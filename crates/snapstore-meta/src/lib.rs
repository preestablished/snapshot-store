//! Meta v2 — SQLite-backed metadata store for the snapshot-store tree.
//!
//! # Design
//!
//! One writer actor thread owns the sole write `Connection`.  Commands arrive on
//! a bounded crossbeam channel; the actor drains up to `WRITE_BATCH_MAX` (256)
//! into a single `BEGIN IMMEDIATE … COMMIT`.  Each command runs inside a
//! `SAVEPOINT` so a single failing command rolls back only itself while the rest
//! of the batch commits.
//!
//! A read pool of `READ_POOL_SIZE` (4) connections is used for all reads;
//! they never go through the writer actor.
//!
//! The logical counter advances **per command**, not per transaction (see
//! `WRITE_BATCH_MAX` comment in the actor loop for the rationale — cursor
//! soundness under paged queries).
//!
//! # Note on snapshot_ref validation
//!
//! `create_node` stores `snapshot_ref` opaquely.  The rule that a `snapshot_ref`
//! must resolve to a stored manifest (⇒ `NOT_FOUND`) **cannot live in this
//! crate** — meta has no manifest visibility.  It is the caller (server layer)
//! responsibility to validate this before calling `create_node`.

#![forbid(unsafe_code)]

mod actor;
mod error;
mod pool;
mod schema;
mod types;

#[cfg(test)]
mod tests;

pub use error::MetaError;
pub use pool::PathResult;
pub use types::{
    CreateNodeParams, GcStateRow, NodeRow, NodeUpdate, PinRow, QueryFilter, QueryOrder, StatsRow,
    TombstoneRow,
};

use actor::{ActorCmd, WriterActor};
use pool::ReadPool;
use snapstore_types::{ExperimentId, LogId, NodeId, SnapshotRef};
use std::path::Path;
use std::sync::Arc;

const WRITE_BATCH_MAX: usize = 256;
const READ_POOL_SIZE: usize = 4;

/// Maximum size of an input-log container stored inline (default 16 MiB).
pub const DEFAULT_INPUT_LOG_MAX_BYTES: usize = 16 * 1024 * 1024;

/// KV key maximum (512 bytes) and value maximum (16 MiB).
pub const KV_KEY_MAX: usize = 512;
pub const KV_VALUE_MAX: usize = 16 * 1024 * 1024;

/// Minimum valid input-log container size (8 magic + 2 ver + 2 flags + 4 inner_ver +
/// 4 reserved + 8 payload_len + 32 footer = 60; we use 56 per spec for headroom).
pub const INPUT_LOG_MIN_BYTES: usize = 56;

/// Configuration passed to [`MetaDb::open`].
#[derive(Clone, Debug)]
pub struct MetaConfig {
    /// Maximum bytes for an inline input-log container (default 16 MiB).
    pub input_log_max_bytes: usize,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            input_log_max_bytes: DEFAULT_INPUT_LOG_MAX_BYTES,
        }
    }
}

// ---------------------------------------------------------------------------
// Public handle — Clone-by-Arc
// ---------------------------------------------------------------------------

struct MetaDbInner {
    sender: crossbeam_channel::Sender<ActorCmd>,
    actor_thread: Option<std::thread::JoinHandle<()>>,
    read_pool: ReadPool,
}

/// Clone-able handle to the metadata database.
///
/// Internally backed by a writer actor thread and a read connection pool.
/// All public methods are fully synchronous (no tokio dependency).
///
/// # Note on snapshot_ref validation
///
/// `create_node` accepts any 32-byte `snapshot_ref` blob without verifying it
/// exists in the page/manifest store.  The server layer **must** validate this
/// before calling `create_node` (API.md §1.4 NOT_FOUND rule).
#[derive(Clone)]
pub struct MetaDb(Arc<MetaDbInner>);

impl std::fmt::Debug for MetaDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaDb").finish_non_exhaustive()
    }
}

impl Drop for MetaDbInner {
    fn drop(&mut self) {
        // Signal the actor to shut down by closing the channel, then join.
        // We do this by dropping the sender (channel closes when no senders remain),
        // but we can't drop just the sender here without a trick — use a sentinel.
        // Instead we just let the channel drop naturally when the struct is freed;
        // the actor exits its loop when the channel is empty AND disconnected.
        // We still need to join to avoid a leak.
        if let Some(handle) = self.actor_thread.take() {
            // Drop our sender side so the actor sees disconnect.
            // The sender is held in the struct; we can't drop it here without
            // unsafe tricks.  Close by sending a Shutdown command instead.
            let _ = self.sender.send(ActorCmd::Shutdown);
            let _ = handle.join();
        }
    }
}

impl MetaDb {
    /// Open (or create) the metadata database at `path`.
    ///
    /// Creates parent directories if they do not exist.
    /// Applies the schema migration on first open.
    /// Refuses to open a database whose `schema_version > 1`.
    pub fn open(path: &Path) -> Result<Self, MetaError> {
        Self::open_with_config(path, MetaConfig::default())
    }

    /// Open with explicit configuration.
    pub fn open_with_config(path: &Path, config: MetaConfig) -> Result<Self, MetaError> {
        // Create parent directories.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| MetaError::Io(format!("create_dir_all: {e}")))?;
        }

        // Open and migrate the writer connection.
        let write_conn = schema::open_writer(path)?;
        // Re-derive the logical counter from the DB state.
        let initial_counter = schema::rederive_counter(&write_conn)?;

        // Open the read pool.
        let read_pool = ReadPool::open(path, READ_POOL_SIZE)?;

        // Spawn the writer actor.
        let (sender, receiver) = crossbeam_channel::bounded::<ActorCmd>(1024);
        let actor_thread = {
            let cfg = config.clone();
            std::thread::Builder::new()
                .name("snapstore-meta-writer".into())
                .spawn(move || {
                    WriterActor::new(write_conn, receiver, initial_counter, WRITE_BATCH_MAX, cfg)
                        .run();
                })
                .map_err(|e| MetaError::Io(format!("spawn writer: {e}")))?
        };

        Ok(MetaDb(Arc::new(MetaDbInner {
            sender,
            actor_thread: Some(actor_thread),
            read_pool,
        })))
    }

    // -----------------------------------------------------------------------
    // Write operations (routed through the actor)
    // -----------------------------------------------------------------------

    /// Create a new node.
    ///
    /// Rules enforced:
    /// - Root: `node_id == 0` iff `parent_node_id == None`; root uniqueness via PK.
    /// - Non-root: parent must exist and must not be `PRUNED`.
    /// - Depth is computed: `parent.depth + 1` (root depth = 0).
    /// - `input_log_id`: if `Some`, must exist in `input_logs` OR inline
    ///   container bytes provided (inserted in the same transaction).
    /// - Idempotency: PK conflict → re-read, compare immutable fields
    ///   (parent_node_id, snapshot_ref, input_log_id) → identical ⇒ return stored row;
    ///   different ⇒ `MetaError::AlreadyExists`.
    ///
    /// # Note
    /// `snapshot_ref` is stored opaquely.  The caller (server layer) **must** validate
    /// that it resolves to a stored manifest before calling this method.
    pub fn create_node(&self, params: CreateNodeParams) -> Result<NodeRow, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::CreateNode { params, reply: tx })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Bulk partial update of nodes — one command, all-or-nothing.
    ///
    /// Any unknown `node_id` in the batch rolls back the entire operation and
    /// returns `MetaError::MissingNodes` listing **all** missing ids.
    ///
    /// Returns the `updated_at` logical counter stamped on all affected rows.
    pub fn update_nodes(
        &self,
        experiment_id: ExperimentId,
        updates: Vec<NodeUpdate>,
    ) -> Result<u64, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::UpdateNodes {
                experiment_id,
                updates,
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Store an input-log container.
    ///
    /// Validates that `blake3(container[..len-32]) == log_id` and that
    /// `len >= INPUT_LOG_MIN_BYTES`.
    ///
    /// Returns `true` if newly inserted, `false` if already present (idempotent).
    pub fn put_input_log(&self, log_id: LogId, container: &[u8]) -> Result<bool, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::PutInputLog {
                log_id,
                container: container.to_vec(),
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Store or update a KV entry with optional CAS semantics.
    ///
    /// - `expected_generation = None` → unconditional upsert.
    /// - `expected_generation = Some(0)` → create-only (fail if exists).
    /// - `expected_generation = Some(n)` → update only if current generation == n.
    ///
    /// Returns the new generation on success.
    pub fn put_metadata(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_generation: Option<u64>,
    ) -> Result<u64, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::PutMetadata {
                key,
                value,
                expected_generation,
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Delete a KV entry with optional CAS semantics.
    ///
    /// Returns `true` if a row was deleted, `false` if the key was absent.
    pub fn delete_metadata(
        &self,
        key: Vec<u8>,
        expected_generation: Option<u64>,
    ) -> Result<bool, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::DeleteMetadata {
                key,
                expected_generation,
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Pin a snapshot ref with an optional note.
    pub fn pin(&self, snapshot_ref: SnapshotRef, note: Option<String>) -> Result<(), MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::Pin {
                snapshot_ref,
                note,
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Unpin a snapshot ref.  Returns `true` if a row was deleted.
    pub fn unpin(&self, snapshot_ref: &SnapshotRef) -> Result<bool, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::Unpin {
                snapshot_ref: snapshot_ref.clone(),
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Prune a subtree rooted at `node_id`.
    ///
    /// Recursively sets all nodes in the subtree to `PRUNED` status, inserts a
    /// tombstone row for the subtree root, all in one command/transaction.
    ///
    /// Returns the number of nodes pruned.
    ///
    /// `allow_root = false` rejects `node_id == 0`.
    pub fn prune_subtree(
        &self,
        experiment_id: ExperimentId,
        node_id: NodeId,
        allow_root: bool,
    ) -> Result<u64, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::PruneSubtree {
                experiment_id,
                node_id,
                allow_root,
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Reap one tombstoned subtree: recursive CTE from `(experiment_id,
    /// node_id)` over PRUNED rows, delete those node rows, delete
    /// `input_logs` no longer referenced by any node, delete the tombstone
    /// row. One transaction. Returns the number of node rows deleted.
    ///
    /// Idempotent: returns `Ok(0)` if the root row is already missing
    /// (already reaped — safe under crash-retry). Aborts with
    /// `MetaError::InvalidArgument` if any subtree row is not `PRUNED`
    /// (defense in depth — never deletes live rows).
    pub fn reap_tombstone(
        &self,
        experiment_id: &ExperimentId,
        node_id: NodeId,
    ) -> Result<u64, MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::ReapTombstone {
                experiment_id: experiment_id.clone(),
                node_id,
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    /// Overwrite the persisted GC cycle state (singleton `gc_state` row).
    pub fn set_gc_state(&self, s: GcStateRow) -> Result<(), MetaError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .sender
            .send(ActorCmd::SetGcState {
                state: s,
                reply: tx,
            })
            .map_err(|_| MetaError::ActorDead)?;
        rx.recv().map_err(|_| MetaError::ActorDead)?
    }

    // -----------------------------------------------------------------------
    // Read operations (use the read pool directly)
    // -----------------------------------------------------------------------

    /// Get a single node by (experiment_id, node_id).
    pub fn get_node(
        &self,
        experiment_id: &ExperimentId,
        node_id: NodeId,
    ) -> Result<Option<NodeRow>, MetaError> {
        self.0
            .read_pool
            .with(|conn| pool::get_node(conn, experiment_id, node_id))
    }

    /// Get the direct children of a node.
    pub fn get_children(
        &self,
        experiment_id: &ExperimentId,
        node_id: NodeId,
    ) -> Result<Vec<NodeRow>, MetaError> {
        self.0
            .read_pool
            .with(|conn| pool::get_children(conn, experiment_id, node_id))
    }

    /// Get the path from the root to `node_id`, root-first.
    ///
    /// If `include_logs` is true, also returns the raw container bytes of each
    /// node's `input_log_id` (if present).
    pub fn get_path(
        &self,
        experiment_id: &ExperimentId,
        node_id: NodeId,
        include_logs: bool,
    ) -> Result<pool::PathResult, MetaError> {
        self.0
            .read_pool
            .with(|conn| pool::get_path(conn, experiment_id, node_id, include_logs))
    }

    /// Query nodes with conjunctive filters and cursor-based paging.
    pub fn query_nodes(&self, filter: QueryFilter) -> Result<Vec<NodeRow>, MetaError> {
        self.0
            .read_pool
            .with(|conn| pool::query_nodes(conn, &filter))
    }

    /// Get an input-log container by its log_id.  Returns byte-identical content.
    pub fn get_input_log(&self, log_id: &LogId) -> Result<Option<Vec<u8>>, MetaError> {
        self.0
            .read_pool
            .with(|conn| pool::get_input_log(conn, log_id))
    }

    /// Get a KV entry by key.  Returns `(value, generation)` if found.
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>, MetaError> {
        self.0.read_pool.with(|conn| pool::get_metadata(conn, key))
    }

    /// List all pinned snapshot refs.
    pub fn list_pins(&self) -> Result<Vec<PinRow>, MetaError> {
        self.0.read_pool.with(pool::list_pins)
    }

    /// All distinct snapshot_refs that are GC roots: every `nodes.snapshot_ref`
    /// (ALL statuses — PRUNED-but-unreaped rows are conservatively live) plus
    /// every `pins.snapshot_ref`. Single read statement = point-in-time root
    /// set (WAL snapshot isolation).
    pub fn gc_root_refs(&self) -> Result<Vec<SnapshotRef>, MetaError> {
        self.0.read_pool.with(pool::gc_root_refs)
    }

    /// Tombstoned subtree roots with `created_at <= horizon` (logical counter).
    pub fn list_tombstones(&self, horizon: u64) -> Result<Vec<TombstoneRow>, MetaError> {
        self.0
            .read_pool
            .with(|conn| pool::list_tombstones(conn, horizon))
    }

    /// Persisted GC cycle state.
    pub fn gc_state(&self) -> Result<GcStateRow, MetaError> {
        self.0.read_pool.with(pool::gc_state)
    }

    /// Return aggregate statistics.
    ///
    /// If `experiment_id` is `Some`, also includes per-experiment node counts.
    pub fn stats(&self, experiment_id: Option<&ExperimentId>) -> Result<StatsRow, MetaError> {
        self.0
            .read_pool
            .with(|conn| pool::stats(conn, experiment_id))
    }

    /// Run `PRAGMA integrity_check` on a read connection.
    ///
    /// Returns `Ok(())` if the database reports "ok", `Err(MetaError::Io(…))`
    /// otherwise.  Called at startup to refuse serving a corrupt database.
    pub fn integrity_check(&self) -> Result<(), MetaError> {
        self.0.read_pool.with(|conn| {
            let result: String = conn
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .map_err(MetaError::Sqlite)?;
            if result == "ok" {
                Ok(())
            } else {
                Err(MetaError::Io(format!("integrity_check failed: {result}")))
            }
        })
    }

    /// List all distinct experiment ids present in the nodes table.
    ///
    /// Used by startup reconciliation to walk all experiments and verify
    /// that every node's `snapshot_ref` resolves to a stored manifest.
    pub fn list_experiments(&self) -> Result<Vec<ExperimentId>, MetaError> {
        self.0.read_pool.with(|conn| {
            let mut stmt = conn
                .prepare("SELECT DISTINCT experiment_id FROM nodes ORDER BY experiment_id")
                .map_err(MetaError::Sqlite)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(MetaError::Sqlite)?;
            let mut out = Vec::new();
            for r in rows {
                let s = r.map_err(MetaError::Sqlite)?;
                let eid = ExperimentId::new(s)
                    .map_err(|e| MetaError::Io(format!("invalid experiment_id: {e}")))?;
                out.push(eid);
            }
            Ok(out)
        })
    }
}
