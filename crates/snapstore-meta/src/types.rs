use snapstore_types::{ExperimentId, LogId, NodeId, NodeStatus, SnapshotRef};

// ---------------------------------------------------------------------------
// Row types returned from the DB
// ---------------------------------------------------------------------------

/// A single node row as returned by read operations.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeRow {
    pub experiment_id: ExperimentId,
    pub node_id: NodeId,
    pub parent_node_id: Option<NodeId>,
    pub depth: u32,
    pub snapshot_ref: SnapshotRef,
    pub input_log_id: Option<LogId>,
    pub status: NodeStatus,
    pub score: Option<f64>,
    pub visit_count: u64,
    pub icount: u64,
    pub virtual_ns: u64,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_visited_at: u64,
    pub attrs: Option<Vec<u8>>,
}

/// A pinned snapshot ref row.
#[derive(Clone, Debug, PartialEq)]
pub struct PinRow {
    pub snapshot_ref: SnapshotRef,
    pub note: Option<String>,
    pub created_at: u64,
}

/// Aggregate statistics.
#[derive(Clone, Debug, Default)]
pub struct StatsRow {
    // Global
    pub experiments_count: u64,
    pub total_nodes: u64,
    pub pins_count: u64,
    pub tombstones_count: u64,
    pub logical_counter: u64,
    pub kv_count: u64,
    pub input_logs_count: u64,
    pub input_logs_bytes: u64,

    // Per-experiment (populated when experiment_id is provided)
    pub exp_nodes_frontier: u64,
    pub exp_nodes_expanded: u64,
    pub exp_nodes_pruned: u64,
    pub exp_nodes_goal: u64,
    pub exp_max_depth: u32,
    pub exp_input_log_count: u64,
}

// ---------------------------------------------------------------------------
// Input types for write operations
// ---------------------------------------------------------------------------

/// Parameters for [`crate::MetaDb::create_node`].
#[derive(Clone, Debug)]
pub struct CreateNodeParams {
    pub experiment_id: ExperimentId,
    pub node_id: NodeId,
    /// `None` iff root (node_id == 0).
    pub parent_node_id: Option<NodeId>,
    pub snapshot_ref: SnapshotRef,
    /// The log_id, if any.
    pub input_log_id: Option<LogId>,
    /// If `input_log_id` is `Some` and the log does not yet exist in `input_logs`,
    /// these bytes are inserted in the same transaction.  The caller must ensure
    /// they are valid (the blake3 check is performed here).
    pub inline_log_container: Option<Vec<u8>>,
    pub status: NodeStatus,
    pub score: Option<f64>,
    /// Guest instruction count at this node (moved into the node row from
    /// the phase-1 manifest).
    pub icount: u64,
    pub virtual_ns: u64,
    pub attrs: Option<Vec<u8>>,
}

/// A partial update for one node in a [`crate::MetaDb::update_nodes`] call.
#[derive(Clone, Debug)]
pub struct NodeUpdate {
    pub node_id: NodeId,
    /// If `Some`, overwrite the status.
    pub status: Option<NodeStatus>,
    /// If `Some`, overwrite the score.
    pub score: Option<f64>,
    /// If `Some`, overwrite attrs.
    pub attrs: Option<Vec<u8>>,
    /// Added (delta) to the current visit_count.
    pub visit_count_delta: i64,
    /// If `true`, stamp `last_visited_at` with the transaction counter.
    pub touch_visited: bool,
    /// If `Some`, overwrite icount.
    pub icount: Option<u64>,
    /// If `Some`, overwrite virtual_ns.
    pub virtual_ns: Option<u64>,
}

impl NodeUpdate {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            status: None,
            score: None,
            attrs: None,
            visit_count_delta: 0,
            touch_visited: false,
            icount: None,
            virtual_ns: None,
        }
    }
}

// Default is useful for `..Default::default()` struct update syntax in tests.
// node_id defaults to ROOT (0).
impl Default for NodeUpdate {
    fn default() -> Self {
        Self::new(NodeId::ROOT)
    }
}

// ---------------------------------------------------------------------------
// Query types
// ---------------------------------------------------------------------------

/// Ordering for [`QueryFilter`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QueryOrder {
    #[default]
    CreatedAt,
    UpdatedAt,
    NodeId,
}

/// Conjunctive filter for [`crate::MetaDb::query_nodes`].
#[derive(Clone, Debug)]
pub struct QueryFilter {
    /// Required.
    pub experiment_id: ExperimentId,
    pub status: Option<NodeStatus>,
    pub parent_node_id: Option<NodeId>,
    pub min_depth: Option<u32>,
    pub max_depth: Option<u32>,
    pub order: QueryOrder,
    /// Exclusive cursor: return rows with `created_at > created_after`.
    pub created_after: Option<u64>,
    /// Exclusive cursor: return rows with `updated_at > updated_after`.
    pub updated_after: Option<u64>,
    /// Maximum rows returned (capped at 4096, default 512).
    pub limit: Option<u32>,
}

impl QueryFilter {
    pub fn new(experiment_id: ExperimentId) -> Self {
        Self {
            experiment_id,
            status: None,
            parent_node_id: None,
            min_depth: None,
            max_depth: None,
            order: QueryOrder::CreatedAt,
            created_after: None,
            updated_after: None,
            limit: None,
        }
    }

    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(512).min(4096)
    }
}

// Default is useful for struct update syntax in tests.
// experiment_id defaults to "default" — callers should always override this.
impl Default for QueryFilter {
    fn default() -> Self {
        Self::new(ExperimentId::new("default").unwrap())
    }
}
