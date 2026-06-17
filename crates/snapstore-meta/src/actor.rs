use crate::error::MetaError;
use crate::pool::{log_id_to_sql, node_id_to_sql, snapshot_ref_to_sql, status_to_sql};
use crate::types::{CreateNodeParams, NodeRow, NodeUpdate};
use crate::{MetaConfig, INPUT_LOG_MIN_BYTES, KV_KEY_MAX, KV_VALUE_MAX};
use crossbeam_channel::{Receiver, Sender};
use rusqlite::{params, Connection, OptionalExtension};
use snapstore_types::{ExperimentId, LogId, NodeId, NodeStatus, SnapshotRef};

// ---------------------------------------------------------------------------
// Command enum
// ---------------------------------------------------------------------------

pub enum ActorCmd {
    CreateNode {
        params: CreateNodeParams,
        reply: Sender<Result<NodeRow, MetaError>>,
    },
    UpdateNodes {
        experiment_id: ExperimentId,
        updates: Vec<NodeUpdate>,
        reply: Sender<Result<u64, MetaError>>,
    },
    PutInputLog {
        log_id: LogId,
        container: Vec<u8>,
        reply: Sender<Result<bool, MetaError>>,
    },
    PutMetadata {
        key: Vec<u8>,
        value: Vec<u8>,
        expected_generation: Option<u64>,
        reply: Sender<Result<u64, MetaError>>,
    },
    DeleteMetadata {
        key: Vec<u8>,
        expected_generation: Option<u64>,
        reply: Sender<Result<bool, MetaError>>,
    },
    Pin {
        snapshot_ref: SnapshotRef,
        note: Option<String>,
        reply: Sender<Result<(), MetaError>>,
    },
    Unpin {
        snapshot_ref: SnapshotRef,
        reply: Sender<Result<bool, MetaError>>,
    },
    PruneSubtree {
        experiment_id: ExperimentId,
        node_id: NodeId,
        allow_root: bool,
        reply: Sender<Result<u64, MetaError>>,
    },
    Shutdown,
}

// ---------------------------------------------------------------------------
// Reply dispatch helpers
// ---------------------------------------------------------------------------

enum CmdResult {
    NodeRow(Result<NodeRow, MetaError>),
    U64(Result<u64, MetaError>),
    Bool(Result<bool, MetaError>),
    Unit(Result<(), MetaError>),
}

fn dispatch_cmd(conn: &Connection, cmd: &ActorCmd, counter: u64, config: &MetaConfig) -> CmdResult {
    match cmd {
        ActorCmd::CreateNode { params, .. } => {
            CmdResult::NodeRow(create_node_impl(conn, params, counter, config))
        }
        ActorCmd::UpdateNodes {
            experiment_id,
            updates,
            ..
        } => CmdResult::U64(update_nodes_impl(conn, experiment_id, updates, counter)),
        ActorCmd::PutInputLog {
            log_id, container, ..
        } => CmdResult::Bool(put_input_log_impl(conn, log_id, container, counter, config)),
        ActorCmd::PutMetadata {
            key,
            value,
            expected_generation,
            ..
        } => CmdResult::U64(put_metadata_impl(
            conn,
            key,
            value,
            *expected_generation,
            counter,
        )),
        ActorCmd::DeleteMetadata {
            key,
            expected_generation,
            ..
        } => CmdResult::Bool(delete_metadata_impl(conn, key, *expected_generation)),
        ActorCmd::Pin {
            snapshot_ref, note, ..
        } => CmdResult::Unit(pin_impl(conn, snapshot_ref, note.as_deref(), counter)),
        ActorCmd::Unpin { snapshot_ref, .. } => CmdResult::Bool(unpin_impl(conn, snapshot_ref)),
        ActorCmd::PruneSubtree {
            experiment_id,
            node_id,
            allow_root,
            ..
        } => CmdResult::U64(prune_subtree_impl(
            conn,
            experiment_id,
            *node_id,
            *allow_root,
            counter,
        )),
        ActorCmd::Shutdown => CmdResult::Unit(Ok(())),
    }
}

fn send_result(cmd: ActorCmd, result: CmdResult) {
    match (cmd, result) {
        (ActorCmd::CreateNode { reply, .. }, CmdResult::NodeRow(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::UpdateNodes { reply, .. }, CmdResult::U64(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::PutInputLog { reply, .. }, CmdResult::Bool(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::PutMetadata { reply, .. }, CmdResult::U64(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::DeleteMetadata { reply, .. }, CmdResult::Bool(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::Pin { reply, .. }, CmdResult::Unit(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::Unpin { reply, .. }, CmdResult::Bool(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::PruneSubtree { reply, .. }, CmdResult::U64(r)) => {
            let _ = reply.send(r);
        }
        (ActorCmd::Shutdown, _) => {}
        _ => {
            // type mismatch — should never happen
        }
    }
}

fn send_error(cmd: ActorCmd, e: MetaError) {
    // Build a CmdResult error from the type the channel expects.
    match cmd {
        ActorCmd::CreateNode { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::UpdateNodes { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::PutInputLog { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::PutMetadata { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::DeleteMetadata { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::Pin { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::Unpin { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::PruneSubtree { reply, .. } => {
            let _ = reply.send(Err(e));
        }
        ActorCmd::Shutdown => {}
    }
}

// ---------------------------------------------------------------------------
// WriterActor
// ---------------------------------------------------------------------------

pub struct WriterActor {
    conn: Connection,
    receiver: Receiver<ActorCmd>,
    counter: u64,
    batch_max: usize,
    config: MetaConfig,
}

impl WriterActor {
    pub fn new(
        conn: Connection,
        receiver: Receiver<ActorCmd>,
        initial_counter: u64,
        batch_max: usize,
        config: MetaConfig,
    ) -> Self {
        Self {
            conn,
            receiver,
            counter: initial_counter,
            batch_max,
            config,
        }
    }

    pub fn run(mut self) {
        loop {
            // Block waiting for the first command.
            let first = match self.receiver.recv() {
                Ok(ActorCmd::Shutdown) | Err(_) => return,
                Ok(cmd) => cmd,
            };

            // Drain up to batch_max commands (non-blocking), including first.
            let mut batch = Vec::with_capacity(self.batch_max);
            batch.push(first);
            let mut shutdown_pending = false;
            while batch.len() < self.batch_max {
                match self.receiver.try_recv() {
                    Ok(ActorCmd::Shutdown) => {
                        shutdown_pending = true;
                        break;
                    }
                    Ok(cmd) => batch.push(cmd),
                    Err(_) => break,
                }
            }

            self.execute_batch(batch);
            if shutdown_pending {
                return;
            }
        }
    }

    fn execute_batch(&mut self, batch: Vec<ActorCmd>) {
        // Begin the transaction.
        if let Err(_e) = self.conn.execute_batch("BEGIN IMMEDIATE") {
            // Catastrophic — can't begin a transaction. Send errors to all.
            for cmd in batch {
                send_error(cmd, MetaError::Io("BEGIN IMMEDIATE failed".into()));
            }
            return;
        }

        // Execute each command inside a SAVEPOINT.
        let mut pending_sends: Vec<(ActorCmd, CmdResult)> = Vec::with_capacity(batch.len());
        let mut counter_dirty = false;

        for (i, cmd) in batch.into_iter().enumerate() {
            let sp_name = format!("sp{i}");
            if self
                .conn
                .execute_batch(&format!("SAVEPOINT \"{sp_name}\""))
                .is_err()
            {
                // Can't even create a savepoint — skip and error.
                send_error(cmd, MetaError::Io("savepoint creation failed".into()));
                continue;
            }

            // Advance counter for this command.
            let cmd_counter = self.counter;
            self.counter += 1;
            counter_dirty = true;

            let result = dispatch_cmd(&self.conn, &cmd, cmd_counter, &self.config);
            let succeeded = match &result {
                CmdResult::NodeRow(r) => r.is_ok(),
                CmdResult::U64(r) => r.is_ok(),
                CmdResult::Bool(r) => r.is_ok(),
                CmdResult::Unit(r) => r.is_ok(),
            };

            if succeeded {
                let _ = self.conn.execute_batch(&format!("RELEASE \"{sp_name}\""));
            } else {
                let _ = self
                    .conn
                    .execute_batch(&format!("ROLLBACK TO \"{sp_name}\""));
                let _ = self.conn.execute_batch(&format!("RELEASE \"{sp_name}\""));
            }

            pending_sends.push((cmd, result));
        }

        // Flush the counter to meta once per transaction.
        if counter_dirty {
            let _ = self.conn.execute(
                "UPDATE meta SET logical_counter=?1 WHERE id=1",
                params![self.counter as i64],
            );
        }

        // Commit.
        if let Err(_e) = self.conn.execute_batch("COMMIT") {
            let _ = self.conn.execute_batch("ROLLBACK");
        }

        // Fire all reply sends after commit.
        for (cmd, result) in pending_sends {
            send_result(cmd, result);
        }
    }
}

// ---------------------------------------------------------------------------
// Node columns constant
// ---------------------------------------------------------------------------

const NODE_COLS: &str = "experiment_id, node_id, parent_node_id, depth, snapshot_ref, \
    input_log_id, status, score, visit_count, icount, virtual_ns, \
    created_at, updated_at, last_visited_at, attrs";

fn row_to_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<NodeRow> {
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

fn fetch_node(
    conn: &Connection,
    experiment_id: &ExperimentId,
    node_id: NodeId,
) -> Result<Option<NodeRow>, rusqlite::Error> {
    conn.query_row(
        &format!("SELECT {NODE_COLS} FROM nodes WHERE experiment_id=?1 AND node_id=?2"),
        params![experiment_id.as_str(), node_id_to_sql(node_id)],
        row_to_node,
    )
    .optional()
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

fn create_node_impl(
    conn: &Connection,
    p: &CreateNodeParams,
    counter: u64,
    config: &MetaConfig,
) -> Result<NodeRow, MetaError> {
    // Root rule: node_id==0 iff parent is None.
    if p.node_id.is_root() && p.parent_node_id.is_some() {
        return Err(MetaError::InvalidArgument(
            "root node (node_id=0) must have no parent".into(),
        ));
    }
    if !p.node_id.is_root() && p.parent_node_id.is_none() {
        return Err(MetaError::InvalidArgument(
            "non-root node must have a parent".into(),
        ));
    }

    // Compute depth and validate parent.
    let depth: u32 = if let Some(parent_id) = p.parent_node_id {
        let parent_row =
            fetch_node(conn, &p.experiment_id, parent_id)?.ok_or(MetaError::ParentNotFound)?;
        if parent_row.status == NodeStatus::Pruned {
            return Err(MetaError::ParentNotFound);
        }
        parent_row.depth + 1
    } else {
        0
    };

    // Handle inline log container.
    if let Some(log_id) = &p.input_log_id {
        let log_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM input_logs WHERE log_id=?1",
                params![log_id_to_sql(log_id)],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;

        if !log_exists {
            match &p.inline_log_container {
                Some(container) => {
                    put_input_log_impl(conn, log_id, container, counter, config)?;
                }
                None => {
                    return Err(MetaError::InvalidArgument(
                        "input_log_id not found and no inline container provided".into(),
                    ));
                }
            }
        }
    }

    let snapshot_ref_bytes: &[u8] = snapshot_ref_to_sql(&p.snapshot_ref);
    let input_log_id_bytes: Option<&[u8]> = p.input_log_id.as_ref().map(log_id_to_sql);
    let status_val = status_to_sql(p.status);
    let counter_i64 = counter as i64;

    let res = conn.execute(
        "INSERT INTO nodes \
         (experiment_id, node_id, parent_node_id, depth, snapshot_ref, input_log_id, \
          status, score, visit_count, icount, virtual_ns, created_at, updated_at, \
          last_visited_at, attrs) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?11, 0, ?12)",
        params![
            p.experiment_id.as_str(),
            node_id_to_sql(p.node_id),
            p.parent_node_id.map(node_id_to_sql),
            depth as i64,
            snapshot_ref_bytes,
            input_log_id_bytes,
            status_val,
            p.score,
            p.icount as i64,
            p.virtual_ns as i64,
            counter_i64,
            p.attrs.as_deref(),
        ],
    );

    match res {
        Ok(_) => {
            let row = fetch_node(conn, &p.experiment_id, p.node_id)?
                .ok_or(MetaError::InvalidArgument("inserted node not found".into()))?;
            Ok(row)
        }
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation =>
        {
            // PK conflict — idempotency check.
            let existing =
                fetch_node(conn, &p.experiment_id, p.node_id)?.ok_or(MetaError::AlreadyExists)?;

            let parent_matches = existing.parent_node_id == p.parent_node_id;
            let snap_matches = existing.snapshot_ref == p.snapshot_ref;
            let log_matches = existing.input_log_id == p.input_log_id;

            if parent_matches && snap_matches && log_matches {
                Ok(existing)
            } else {
                Err(MetaError::AlreadyExists)
            }
        }
        Err(e) => Err(MetaError::Sqlite(e)),
    }
}

fn update_nodes_impl(
    conn: &Connection,
    experiment_id: &ExperimentId,
    updates: &[NodeUpdate],
    counter: u64,
) -> Result<u64, MetaError> {
    if updates.is_empty() {
        return Ok(counter);
    }

    // First pass: verify ALL node_ids exist. Collect all missing at once.
    let mut missing = Vec::new();
    for u in updates {
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE experiment_id=?1 AND node_id=?2",
                params![experiment_id.as_str(), node_id_to_sql(u.node_id)],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;
        if !exists {
            missing.push(u.node_id);
        }
    }

    if !missing.is_empty() {
        return Err(MetaError::MissingNodes(missing));
    }

    // Second pass: apply updates.
    let counter_i64 = counter as i64;
    for u in updates {
        let node_sql = node_id_to_sql(u.node_id);
        let exp_str = experiment_id.as_str();

        // Always stamp updated_at.
        conn.execute(
            "UPDATE nodes SET updated_at=?1 WHERE experiment_id=?2 AND node_id=?3",
            params![counter_i64, exp_str, node_sql],
        )?;

        if let Some(status) = u.status {
            conn.execute(
                "UPDATE nodes SET status=?1 WHERE experiment_id=?2 AND node_id=?3",
                params![status_to_sql(status), exp_str, node_sql],
            )?;
        }

        if let Some(score) = u.score {
            conn.execute(
                "UPDATE nodes SET score=?1 WHERE experiment_id=?2 AND node_id=?3",
                params![score, exp_str, node_sql],
            )?;
        }

        if let Some(ref attrs) = u.attrs {
            conn.execute(
                "UPDATE nodes SET attrs=?1 WHERE experiment_id=?2 AND node_id=?3",
                params![attrs.as_slice(), exp_str, node_sql],
            )?;
        }

        if u.visit_count_delta != 0 {
            conn.execute(
                "UPDATE nodes SET visit_count=visit_count+?1 WHERE experiment_id=?2 AND node_id=?3",
                params![u.visit_count_delta, exp_str, node_sql],
            )?;
        }

        if u.touch_visited {
            conn.execute(
                "UPDATE nodes SET last_visited_at=?1 WHERE experiment_id=?2 AND node_id=?3",
                params![counter_i64, exp_str, node_sql],
            )?;
        }

        if let Some(icount) = u.icount {
            conn.execute(
                "UPDATE nodes SET icount=?1 WHERE experiment_id=?2 AND node_id=?3",
                params![icount as i64, exp_str, node_sql],
            )?;
        }

        if let Some(vns) = u.virtual_ns {
            conn.execute(
                "UPDATE nodes SET virtual_ns=?1 WHERE experiment_id=?2 AND node_id=?3",
                params![vns as i64, exp_str, node_sql],
            )?;
        }
    }

    Ok(counter)
}

/// Validate and store an input-log container.
///
/// Validates:
/// - `len >= INPUT_LOG_MIN_BYTES`
/// - `len <= config.input_log_max_bytes`
/// - `blake3(container[..len-32]) == log_id`
///
/// Returns `true` if newly inserted, `false` if already present.
pub(crate) fn put_input_log_impl(
    conn: &Connection,
    log_id: &LogId,
    container: &[u8],
    counter: u64,
    config: &MetaConfig,
) -> Result<bool, MetaError> {
    if container.len() < INPUT_LOG_MIN_BYTES {
        return Err(MetaError::LogTooSmall);
    }
    if container.len() > config.input_log_max_bytes {
        return Err(MetaError::LogTooLarge);
    }

    // Validate blake3(content[..len-32]) == log_id.
    let hash_region = &container[..container.len() - 32];
    let computed = blake3::hash(hash_region);
    if computed.as_bytes() != &log_id.0 {
        return Err(MetaError::LogIdMismatch);
    }

    // Read inner_format_version from bytes 8..12 LE (documented container offset).
    let inner_fmt_ver = if container.len() >= 12 {
        u32::from_le_bytes(container[8..12].try_into().unwrap()) as i64
    } else {
        0i64
    };

    let rows = conn.execute(
        "INSERT OR IGNORE INTO input_logs (log_id, inner_format_version, content, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![
            log_id_to_sql(log_id),
            inner_fmt_ver,
            container,
            counter as i64,
        ],
    )?;

    Ok(rows > 0)
}

fn put_metadata_impl(
    conn: &Connection,
    key: &[u8],
    value: &[u8],
    expected_generation: Option<u64>,
    counter: u64,
) -> Result<u64, MetaError> {
    if key.is_empty() || key.len() > KV_KEY_MAX {
        return Err(MetaError::InvalidArgument(format!(
            "key length {} out of range 1..={}",
            key.len(),
            KV_KEY_MAX
        )));
    }
    if value.len() > KV_VALUE_MAX {
        return Err(MetaError::InvalidArgument(format!(
            "value length {} exceeds max {}",
            value.len(),
            KV_VALUE_MAX
        )));
    }

    let counter_i64 = counter as i64;

    match expected_generation {
        None => {
            // Unconditional upsert — compute next generation.
            let current_gen: Option<i64> = conn
                .query_row(
                    "SELECT generation FROM kv_metadata WHERE key=?1",
                    params![key],
                    |row| row.get(0),
                )
                .optional()?;

            let new_gen = current_gen.map(|g| g + 1).unwrap_or(1i64);
            conn.execute(
                "INSERT INTO kv_metadata (key, value, generation, updated_at) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value, generation=excluded.generation, updated_at=excluded.updated_at",
                params![key, value, new_gen, counter_i64],
            )?;
            Ok(new_gen as u64)
        }
        Some(0) => {
            // Create-only: must not exist.
            let res = conn.execute(
                "INSERT INTO kv_metadata (key, value, generation, updated_at) VALUES (?1, ?2, 1, ?3)",
                params![key, value, counter_i64],
            );
            match res {
                Ok(_) => Ok(1),
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ffi::ErrorCode::ConstraintViolation =>
                {
                    let current: i64 = conn
                        .query_row(
                            "SELECT generation FROM kv_metadata WHERE key=?1",
                            params![key],
                            |row| row.get(0),
                        )
                        .optional()?
                        .unwrap_or(0);
                    Err(MetaError::CasFailed {
                        current: current as u64,
                    })
                }
                Err(e) => Err(MetaError::Sqlite(e)),
            }
        }
        Some(expected_gen) => {
            // CAS update: generation must match.
            let rows = conn.execute(
                "UPDATE kv_metadata SET value=?1, generation=generation+1, updated_at=?2 \
                 WHERE key=?3 AND generation=?4",
                params![value, counter_i64, key, expected_gen as i64],
            )?;
            if rows == 0 {
                let current: i64 = conn
                    .query_row(
                        "SELECT generation FROM kv_metadata WHERE key=?1",
                        params![key],
                        |row| row.get(0),
                    )
                    .optional()?
                    .unwrap_or(0);
                Err(MetaError::CasFailed {
                    current: current as u64,
                })
            } else {
                let new_gen: i64 = conn.query_row(
                    "SELECT generation FROM kv_metadata WHERE key=?1",
                    params![key],
                    |row| row.get(0),
                )?;
                Ok(new_gen as u64)
            }
        }
    }
}

fn delete_metadata_impl(
    conn: &Connection,
    key: &[u8],
    expected_generation: Option<u64>,
) -> Result<bool, MetaError> {
    if key.is_empty() || key.len() > KV_KEY_MAX {
        return Err(MetaError::InvalidArgument(format!(
            "key length {} out of range 1..={}",
            key.len(),
            KV_KEY_MAX
        )));
    }

    let rows = match expected_generation {
        None => conn.execute("DELETE FROM kv_metadata WHERE key=?1", params![key])?,
        Some(gen) => conn.execute(
            "DELETE FROM kv_metadata WHERE key=?1 AND generation=?2",
            params![key, gen as i64],
        )?,
    };

    if rows == 0 {
        if let Some(_gen) = expected_generation {
            let current: Option<i64> = conn
                .query_row(
                    "SELECT generation FROM kv_metadata WHERE key=?1",
                    params![key],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(cur) = current {
                // Key exists but generation mismatch.
                return Err(MetaError::CasFailed {
                    current: cur as u64,
                });
            }
            // Key doesn't exist — already deleted.
            return Ok(false);
        }
        return Ok(false);
    }

    Ok(true)
}

fn pin_impl(
    conn: &Connection,
    snapshot_ref: &SnapshotRef,
    note: Option<&str>,
    counter: u64,
) -> Result<(), MetaError> {
    conn.execute(
        "INSERT OR IGNORE INTO pins (snapshot_ref, note, created_at) VALUES (?1, ?2, ?3)",
        params![snapshot_ref_to_sql(snapshot_ref), note, counter as i64],
    )?;
    Ok(())
}

fn unpin_impl(conn: &Connection, snapshot_ref: &SnapshotRef) -> Result<bool, MetaError> {
    let rows = conn.execute(
        "DELETE FROM pins WHERE snapshot_ref=?1",
        params![snapshot_ref_to_sql(snapshot_ref)],
    )?;
    Ok(rows > 0)
}

fn prune_subtree_impl(
    conn: &Connection,
    experiment_id: &ExperimentId,
    node_id: NodeId,
    allow_root: bool,
    counter: u64,
) -> Result<u64, MetaError> {
    if node_id.is_root() && !allow_root {
        return Err(MetaError::PruneRootDenied);
    }

    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM nodes WHERE experiment_id=?1 AND node_id=?2",
            params![experiment_id.as_str(), node_id_to_sql(node_id)],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;
    if !exists {
        return Err(MetaError::MissingNodes(vec![node_id]));
    }

    let pruned_status = NodeStatus::Pruned.as_u8() as i64;
    let counter_i64 = counter as i64;
    let exp_str = experiment_id.as_str();
    let node_sql = node_id_to_sql(node_id);

    // Recursive CTE to collect and update the entire subtree atomically.
    let rows_updated = conn.execute(
        "WITH RECURSIVE subtree(experiment_id, node_id) AS (
            SELECT experiment_id, node_id FROM nodes
            WHERE experiment_id=?1 AND node_id=?2
            UNION ALL
            SELECT n.experiment_id, n.node_id FROM nodes n
            JOIN subtree s ON n.experiment_id=s.experiment_id AND n.parent_node_id=s.node_id
         )
         UPDATE nodes SET status=?3, updated_at=?4
         WHERE (experiment_id, node_id) IN (SELECT experiment_id, node_id FROM subtree)",
        params![exp_str, node_sql, pruned_status, counter_i64],
    )?;

    // Tombstone the subtree root.
    conn.execute(
        "INSERT OR IGNORE INTO tombstones (experiment_id, node_id, created_at) VALUES (?1, ?2, ?3)",
        params![exp_str, node_sql, counter_i64],
    )?;

    Ok(rows_updated as u64)
}
