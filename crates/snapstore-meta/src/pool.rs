use crate::error::MetaError;
use crate::schema::open_reader;
use crate::types::{GcStateRow, NodeRow, PinRow, QueryFilter, QueryOrder, StatsRow, TombstoneRow};
use rusqlite::{params, Connection, OptionalExtension};
use snapstore_types::{ExperimentId, LogId, NodeId, NodeStatus, SnapshotRef};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Return type for [`get_path`]: root-first list of (node, optional log bytes).
pub type PathResult = Vec<(NodeRow, Option<Vec<u8>>)>;

// ---------------------------------------------------------------------------
// Read pool
// ---------------------------------------------------------------------------

pub struct ReadPool {
    conns: Vec<Mutex<Connection>>,
    next: std::sync::atomic::AtomicUsize,
}

impl ReadPool {
    pub fn open(path: &Path, size: usize) -> Result<Self, MetaError> {
        let path_buf: PathBuf = path.to_path_buf();
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            conns.push(Mutex::new(open_reader(&path_buf)?));
        }
        Ok(Self {
            conns,
            next: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Execute `f` on the next available read connection (round-robin).
    pub fn with<F, T>(&self, f: F) -> Result<T, MetaError>
    where
        F: FnOnce(&Connection) -> Result<T, MetaError>,
    {
        let idx = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.conns.len();
        let guard = self.conns[idx].lock().unwrap();
        f(&guard)
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers (pub(crate) for use in actor.rs)
// ---------------------------------------------------------------------------

pub fn node_id_to_sql(id: NodeId) -> i64 {
    id.0 as i64
}

pub fn snapshot_ref_to_sql(r: &SnapshotRef) -> &[u8] {
    &r.0
}

pub fn log_id_to_sql(id: &LogId) -> &[u8] {
    &id.0
}

pub fn status_to_sql(s: NodeStatus) -> i64 {
    s.as_u8() as i64
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

pub(crate) const NODE_COLS: &str = "experiment_id, node_id, parent_node_id, depth, snapshot_ref, \
    input_log_id, status, score, visit_count, icount, virtual_ns, \
    created_at, updated_at, last_visited_at, attrs";

pub(crate) fn row_to_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<NodeRow> {
    let experiment_id_str: String = row.get(0)?;
    let node_id_raw: i64 = row.get(1)?;
    let parent_raw: Option<i64> = row.get(2)?;
    let depth: i64 = row.get(3)?;
    let snapshot_ref_blob: Vec<u8> = row.get(4)?;
    let input_log_id_blob: Option<Vec<u8>> = row.get(5)?;
    let status_raw: i64 = row.get(6)?;
    let score: Option<f64> = row.get(7)?;
    let visit_count: i64 = row.get(8)?;
    let icount: i64 = row.get(9)?;
    let virtual_ns: i64 = row.get(10)?;
    let created_at: i64 = row.get(11)?;
    let updated_at: i64 = row.get(12)?;
    let last_visited_at: i64 = row.get(13)?;
    let attrs: Option<Vec<u8>> = row.get(14)?;

    let status = NodeStatus::from_u8(status_raw as u8).ok_or_else(|| {
        rusqlite::Error::InvalidColumnType(6, "status".into(), rusqlite::types::Type::Integer)
    })?;

    let experiment_id = ExperimentId::new(experiment_id_str).map_err(|_| {
        rusqlite::Error::InvalidColumnType(0, "experiment_id".into(), rusqlite::types::Type::Text)
    })?;

    let mut snap_arr = [0u8; 32];
    snap_arr.copy_from_slice(&snapshot_ref_blob[..32]);

    let input_log_id = input_log_id_blob.map(|b| {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&b[..32]);
        LogId(arr)
    });

    Ok(NodeRow {
        experiment_id,
        node_id: NodeId(node_id_raw as u64),
        parent_node_id: parent_raw.map(|v| NodeId(v as u64)),
        depth: depth as u32,
        snapshot_ref: SnapshotRef(snap_arr),
        input_log_id,
        status,
        score,
        visit_count: visit_count as u64,
        icount: icount as u64,
        virtual_ns: virtual_ns as u64,
        created_at: created_at as u64,
        updated_at: updated_at as u64,
        last_visited_at: last_visited_at as u64,
        attrs,
    })
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

pub fn get_node(
    conn: &Connection,
    experiment_id: &ExperimentId,
    node_id: NodeId,
) -> Result<Option<NodeRow>, MetaError> {
    conn.query_row(
        &format!("SELECT {NODE_COLS} FROM nodes WHERE experiment_id=?1 AND node_id=?2"),
        params![experiment_id.as_str(), node_id_to_sql(node_id)],
        row_to_node,
    )
    .optional()
    .map_err(MetaError::from)
}

pub fn get_children(
    conn: &Connection,
    experiment_id: &ExperimentId,
    node_id: NodeId,
) -> Result<Vec<NodeRow>, MetaError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {NODE_COLS} FROM nodes \
         WHERE experiment_id=?1 AND parent_node_id=?2 \
         ORDER BY node_id ASC"
    ))?;
    let rows = stmt.query_map(
        params![experiment_id.as_str(), node_id_to_sql(node_id)],
        row_to_node,
    )?;
    rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
}

pub fn get_path(
    conn: &Connection,
    experiment_id: &ExperimentId,
    node_id: NodeId,
    include_logs: bool,
) -> Result<PathResult, MetaError> {
    // Recursive CTE: walk up to the root collecting ancestor node_ids,
    // then select all those nodes ordered by depth ASC (root-first).
    let sql = format!(
        "WITH RECURSIVE path_cte(nid) AS (
            SELECT node_id FROM nodes
            WHERE experiment_id=?1 AND node_id=?2
            UNION ALL
            SELECT n.parent_node_id FROM nodes n
            JOIN path_cte p ON n.experiment_id=?1 AND n.node_id=p.nid
            WHERE n.parent_node_id IS NOT NULL
         )
         SELECT {NODE_COLS} FROM nodes
         WHERE experiment_id=?1 AND node_id IN (SELECT nid FROM path_cte)
         ORDER BY depth ASC"
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params![experiment_id.as_str(), node_id_to_sql(node_id)],
        row_to_node,
    )?;
    let nodes: Vec<NodeRow> = rows.collect::<Result<Vec<_>, _>>()?;

    if !include_logs {
        return Ok(nodes.into_iter().map(|n| (n, None)).collect());
    }

    let mut result = Vec::with_capacity(nodes.len());
    for node in nodes {
        let log_bytes = if let Some(ref lid) = node.input_log_id {
            get_input_log(conn, lid)?
        } else {
            None
        };
        result.push((node, log_bytes));
    }
    Ok(result)
}

pub fn query_nodes(conn: &Connection, filter: &QueryFilter) -> Result<Vec<NodeRow>, MetaError> {
    let limit = filter.effective_limit() as i64;

    let order_col = match filter.order {
        QueryOrder::CreatedAt => "created_at",
        QueryOrder::UpdatedAt => "updated_at",
        QueryOrder::NodeId => "node_id",
    };

    // Build the WHERE clause. experiment_id is always required.
    // We use a single parameterised SQL approach with Option<i64> extras
    // that are bound as NULL when unused.  The WHERE clause uses "(?N IS NULL OR col=?N)"
    // for optional conjuncts — SQLite query planner handles this well with the indexes.

    let sql = format!(
        "SELECT {NODE_COLS} FROM nodes
         WHERE experiment_id=?1
           AND (?2 IS NULL OR status=?2)
           AND (?3 IS NULL OR parent_node_id=?3)
           AND (?4 IS NULL OR depth>=?4)
           AND (?5 IS NULL OR depth<=?5)
           AND (?6 IS NULL OR created_at>?6)
           AND (?7 IS NULL OR updated_at>?7)
         ORDER BY {order_col} ASC
         LIMIT ?8"
    );

    let status_val: Option<i64> = filter.status.map(status_to_sql);
    let parent_val: Option<i64> = filter.parent_node_id.map(node_id_to_sql);
    let min_depth: Option<i64> = filter.min_depth.map(|d| d as i64);
    let max_depth: Option<i64> = filter.max_depth.map(|d| d as i64);
    let created_after: Option<i64> = filter.created_after.map(|v| v as i64);
    let updated_after: Option<i64> = filter.updated_after.map(|v| v as i64);

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params![
            filter.experiment_id.as_str(),
            status_val,
            parent_val,
            min_depth,
            max_depth,
            created_after,
            updated_after,
            limit,
        ],
        row_to_node,
    )?;

    rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
}

pub fn get_input_log(conn: &Connection, log_id: &LogId) -> Result<Option<Vec<u8>>, MetaError> {
    conn.query_row(
        "SELECT content FROM input_logs WHERE log_id=?1",
        params![log_id_to_sql(log_id)],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()
    .map_err(MetaError::from)
}

pub fn get_metadata(conn: &Connection, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>, MetaError> {
    conn.query_row(
        "SELECT value, generation FROM kv_metadata WHERE key=?1",
        params![key],
        |row| {
            let v: Vec<u8> = row.get(0)?;
            let g: i64 = row.get(1)?;
            Ok((v, g as u64))
        },
    )
    .optional()
    .map_err(MetaError::from)
}

pub fn list_pins(conn: &Connection) -> Result<Vec<PinRow>, MetaError> {
    let mut stmt =
        conn.prepare("SELECT snapshot_ref, note, created_at FROM pins ORDER BY created_at ASC")?;
    let rows = stmt.query_map([], |row| {
        let blob: Vec<u8> = row.get(0)?;
        let note: Option<String> = row.get(1)?;
        let created_at: i64 = row.get(2)?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&blob[..32]);
        Ok(PinRow {
            snapshot_ref: SnapshotRef(arr),
            note,
            created_at: created_at as u64,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
}

/// All distinct snapshot_refs that are GC roots: every `nodes.snapshot_ref`
/// (ALL statuses — PRUNED-but-unreaped rows are conservatively live) plus
/// every `pins.snapshot_ref`.  A single `UNION` query is one read
/// statement, which in WAL mode observes one consistent point-in-time
/// snapshot — no explicit `BEGIN` needed.
pub fn gc_root_refs(conn: &Connection) -> Result<Vec<SnapshotRef>, MetaError> {
    let mut stmt = conn.prepare(
        "SELECT snapshot_ref FROM nodes
         UNION
         SELECT snapshot_ref FROM pins",
    )?;
    let rows = stmt.query_map([], |row| {
        let blob: Vec<u8> = row.get(0)?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&blob[..32]);
        Ok(SnapshotRef(arr))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
}

/// Tombstoned subtree roots with `created_at <= horizon` (logical counter).
pub fn list_tombstones(conn: &Connection, horizon: u64) -> Result<Vec<TombstoneRow>, MetaError> {
    // Clamp so a caller-supplied `u64::MAX` ("no upper bound") doesn't
    // bit-cast to a negative i64 and exclude every row.
    let horizon_i64 = horizon.min(i64::MAX as u64) as i64;
    let mut stmt = conn.prepare(
        "SELECT experiment_id, node_id, created_at FROM tombstones \
         WHERE created_at <= ?1 ORDER BY created_at ASC",
    )?;
    let rows = stmt.query_map(params![horizon_i64], |row| {
        let experiment_id_str: String = row.get(0)?;
        let node_id_raw: i64 = row.get(1)?;
        let created_at: i64 = row.get(2)?;
        let experiment_id = ExperimentId::new(experiment_id_str).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                0,
                "experiment_id".into(),
                rusqlite::types::Type::Text,
            )
        })?;
        Ok(TombstoneRow {
            experiment_id,
            node_id: NodeId(node_id_raw as u64),
            created_at: created_at as u64,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
}

/// Persisted M7 GC cycle state (the `gc_state` singleton row).
pub fn gc_state(conn: &Connection) -> Result<GcStateRow, MetaError> {
    conn.query_row(
        "SELECT cycles_total, last_fence_counter, last_finished_at, last_freed_bytes \
         FROM gc_state WHERE id=1",
        [],
        |row| {
            let cycles_total: i64 = row.get(0)?;
            let last_fence_counter: i64 = row.get(1)?;
            let last_finished_at: i64 = row.get(2)?;
            let last_freed_bytes: i64 = row.get(3)?;
            Ok(GcStateRow {
                cycles_total: cycles_total as u64,
                last_fence_counter: last_fence_counter as u64,
                last_finished_at: last_finished_at as u64,
                last_freed_bytes: last_freed_bytes as u64,
            })
        },
    )
    .map_err(MetaError::from)
}

pub fn stats(
    conn: &Connection,
    experiment_id: Option<&ExperimentId>,
) -> Result<StatsRow, MetaError> {
    let logical_counter: u64 = conn
        .query_row("SELECT logical_counter FROM meta WHERE id=1", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|v| v as u64)?;

    let experiments_count: u64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT experiment_id) FROM nodes",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v as u64)?;

    let total_nodes: u64 = conn
        .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get::<_, i64>(0))
        .map(|v| v as u64)?;

    let pins_count: u64 = conn
        .query_row("SELECT COUNT(*) FROM pins", [], |row| row.get::<_, i64>(0))
        .map(|v| v as u64)?;

    let tombstones_count: u64 = conn
        .query_row("SELECT COUNT(*) FROM tombstones", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|v| v as u64)?;

    let kv_count: u64 = conn
        .query_row("SELECT COUNT(*) FROM kv_metadata", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|v| v as u64)?;

    let input_logs_count: u64 = conn
        .query_row("SELECT COUNT(*) FROM input_logs", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|v| v as u64)?;

    let input_logs_bytes: u64 = conn
        .query_row(
            "SELECT COALESCE(SUM(LENGTH(content)), 0) FROM input_logs",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v as u64)?;

    let mut row = StatsRow {
        experiments_count,
        total_nodes,
        pins_count,
        tombstones_count,
        logical_counter,
        kv_count,
        input_logs_count,
        input_logs_bytes,
        ..Default::default()
    };

    if let Some(exp) = experiment_id {
        let exp_str = exp.as_str();

        let frontier_status = NodeStatus::Frontier.as_u8() as i64;
        let expanded_status = NodeStatus::Expanded.as_u8() as i64;
        let pruned_status = NodeStatus::Pruned.as_u8() as i64;
        let goal_status = NodeStatus::Goal.as_u8() as i64;

        row.exp_nodes_frontier = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE experiment_id=?1 AND status=?2",
                params![exp_str, frontier_status],
                |r| r.get::<_, i64>(0),
            )
            .map(|v| v as u64)?;

        row.exp_nodes_expanded = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE experiment_id=?1 AND status=?2",
                params![exp_str, expanded_status],
                |r| r.get::<_, i64>(0),
            )
            .map(|v| v as u64)?;

        row.exp_nodes_pruned = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE experiment_id=?1 AND status=?2",
                params![exp_str, pruned_status],
                |r| r.get::<_, i64>(0),
            )
            .map(|v| v as u64)?;

        row.exp_nodes_goal = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE experiment_id=?1 AND status=?2",
                params![exp_str, goal_status],
                |r| r.get::<_, i64>(0),
            )
            .map(|v| v as u64)?;

        row.exp_max_depth = conn
            .query_row(
                "SELECT COALESCE(MAX(depth), 0) FROM nodes WHERE experiment_id=?1",
                params![exp_str],
                |r| r.get::<_, i64>(0),
            )
            .map(|v| v as u32)?;

        row.exp_input_log_count = conn
            .query_row(
                "SELECT COUNT(DISTINCT input_log_id) FROM nodes \
                 WHERE experiment_id=?1 AND input_log_id IS NOT NULL",
                params![exp_str],
                |r| r.get::<_, i64>(0),
            )
            .map(|v| v as u64)?;
    }

    Ok(row)
}
