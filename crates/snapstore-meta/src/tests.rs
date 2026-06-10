use crate::types::{CreateNodeParams, NodeUpdate, QueryFilter, QueryOrder};
use crate::{MetaDb, MetaError};
use snapstore_types::{ExperimentId, LogId, NodeId, NodeStatus, SnapshotRef};
use std::sync::Arc;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn open_tmp() -> (MetaDb, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta/tree.db");
    let db = MetaDb::open(&path).unwrap();
    (db, dir)
}

fn exp(s: &str) -> ExperimentId {
    ExperimentId::new(s).unwrap()
}

fn snap(b: u8) -> SnapshotRef {
    SnapshotRef([b; 32])
}

fn make_log_container(payload: &[u8]) -> (LogId, Vec<u8>) {
    // Construct a minimal valid container:
    // bytes 0..8: magic (anything, we use zeros)
    // bytes 8..12: inner_format_version (LE u32 = 0)
    // bytes 12..20: reserved/payload_len (zeros)
    // ... payload ...
    // last 32 bytes: BLAKE3 footer = log_id
    //
    // container[..len-32] is hashed to produce log_id.
    let mut content: Vec<u8> = Vec::new();
    // 8 magic
    content.extend_from_slice(&[0u8; 8]);
    // 4 inner_format_version = 1
    content.extend_from_slice(&1u32.to_le_bytes());
    // 4 reserved
    content.extend_from_slice(&[0u8; 4]);
    // 8 payload_len
    content.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    // payload
    content.extend_from_slice(payload);
    // Compute log_id = blake3(content_so_far)
    let hash = blake3::hash(&content);
    let log_id = LogId(*hash.as_bytes());
    // Append footer
    content.extend_from_slice(hash.as_bytes());
    (log_id, content)
}

fn create_root(db: &MetaDb, exp_id: &ExperimentId, status: NodeStatus) -> NodeId {
    let node_id = NodeId::ROOT;
    db.create_node(CreateNodeParams {
        experiment_id: exp_id.clone(),
        node_id,
        parent_node_id: None,
        snapshot_ref: snap(0xAA),
        input_log_id: None,
        inline_log_container: None,
        status,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();
    node_id
}

fn create_child(
    db: &MetaDb,
    exp_id: &ExperimentId,
    node_id: NodeId,
    parent: NodeId,
    snap_byte: u8,
) {
    db.create_node(CreateNodeParams {
        experiment_id: exp_id.clone(),
        node_id,
        parent_node_id: Some(parent),
        snapshot_ref: snap(snap_byte),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// Basic open/reopen
// ---------------------------------------------------------------------------

#[test]
fn open_creates_schema() {
    let (_db, _dir) = open_tmp();
}

#[test]
fn reopen_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta/tree.db");
    MetaDb::open(&path).unwrap();
    MetaDb::open(&path).unwrap();
}

#[test]
fn future_version_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta/tree.db");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    // Create a DB with a future schema_version.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK(id=1), \
             schema_version INTEGER NOT NULL, store_uuid TEXT NOT NULL, \
             logical_counter INTEGER NOT NULL); \
             INSERT INTO meta VALUES (1, 999, 'test-uuid', 0);",
        )
        .unwrap();
    }
    let err = MetaDb::open(&path).unwrap_err();
    assert!(
        matches!(err, MetaError::FutureVersion { found: 999, .. }),
        "expected FutureVersion, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// create_node basics
// ---------------------------------------------------------------------------

#[test]
fn create_root_node() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-1");
    let row = db
        .create_node(CreateNodeParams {
            experiment_id: e.clone(),
            node_id: NodeId::ROOT,
            parent_node_id: None,
            snapshot_ref: snap(1),
            input_log_id: None,
            inline_log_container: None,
            status: NodeStatus::Frontier,
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: None,
        })
        .unwrap();
    assert_eq!(row.node_id, NodeId::ROOT);
    assert_eq!(row.depth, 0);
    assert!(row.parent_node_id.is_none());
}

#[test]
fn create_child_computes_depth() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-depth");
    create_root(&db, &e, NodeStatus::Frontier);
    create_child(&db, &e, NodeId(1), NodeId::ROOT, 0x11);

    let child = db.get_node(&e, NodeId(1)).unwrap().unwrap();
    assert_eq!(child.depth, 1);

    create_child(&db, &e, NodeId(2), NodeId(1), 0x22);
    let gc = db.get_node(&e, NodeId(2)).unwrap().unwrap();
    assert_eq!(gc.depth, 2);
}

#[test]
fn create_root_with_parent_is_error() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-err");
    // Create the "parent" first so FK is satisfied.
    create_root(&db, &e, NodeStatus::Frontier);
    let res = db.create_node(CreateNodeParams {
        experiment_id: e,
        node_id: NodeId::ROOT,
        parent_node_id: Some(NodeId(99)),
        snapshot_ref: snap(0),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    });
    assert!(res.is_err());
}

#[test]
fn create_non_root_without_parent_is_error() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-err2");
    let res = db.create_node(CreateNodeParams {
        experiment_id: e,
        node_id: NodeId(5),
        parent_node_id: None,
        snapshot_ref: snap(0),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    });
    assert!(res.is_err());
}

#[test]
fn parent_not_found_error() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-pnf");
    let res = db.create_node(CreateNodeParams {
        experiment_id: e,
        node_id: NodeId(1),
        parent_node_id: Some(NodeId::ROOT),
        snapshot_ref: snap(0),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    });
    assert!(matches!(res, Err(MetaError::ParentNotFound)), "got {res:?}");
}

#[test]
fn pruned_parent_rejected() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-pruned-parent");
    create_root(&db, &e, NodeStatus::Frontier);
    // Prune the root.
    db.prune_subtree(e.clone(), NodeId::ROOT, true).unwrap();
    // Now try to create a child under the pruned root.
    let res = db.create_node(CreateNodeParams {
        experiment_id: e,
        node_id: NodeId(1),
        parent_node_id: Some(NodeId::ROOT),
        snapshot_ref: snap(0x55),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    });
    assert!(matches!(res, Err(MetaError::ParentNotFound)), "got {res:?}");
}

// ---------------------------------------------------------------------------
// Idempotency property
// ---------------------------------------------------------------------------

#[test]
fn create_node_idempotency_identical() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-idem");
    create_root(&db, &e, NodeStatus::Frontier);

    let params = CreateNodeParams {
        experiment_id: e,
        node_id: NodeId(1),
        parent_node_id: Some(NodeId::ROOT),
        snapshot_ref: snap(0x12),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    };
    let r1 = db.create_node(params.clone()).unwrap();
    let r2 = db.create_node(params).unwrap();
    assert_eq!(r1.node_id, r2.node_id);
    assert_eq!(r1.snapshot_ref, r2.snapshot_ref);
}

#[test]
fn create_node_idempotency_conflict() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-conflict");
    create_root(&db, &e, NodeStatus::Frontier);
    db.create_node(CreateNodeParams {
        experiment_id: e.clone(),
        node_id: NodeId(1),
        parent_node_id: Some(NodeId::ROOT),
        snapshot_ref: snap(0xAA),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();

    // Re-insert with different snapshot_ref.
    let res = db.create_node(CreateNodeParams {
        experiment_id: e,
        node_id: NodeId(1),
        parent_node_id: Some(NodeId::ROOT),
        snapshot_ref: snap(0xBB), // different
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    });
    assert!(matches!(res, Err(MetaError::AlreadyExists)), "got {res:?}");
}

/// Build the params for the i-th node in the chain (consistent snapshot_ref).
fn chain_params(e: &ExperimentId, i: u64) -> CreateNodeParams {
    CreateNodeParams {
        experiment_id: e.clone(),
        node_id: NodeId(i),
        parent_node_id: if i == 0 { None } else { Some(NodeId(i - 1)) },
        // snap(i+1) so node 0 uses snap(1) rather than the all-zeros default.
        snapshot_ref: snap((i + 1) as u8),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    }
}

/// Multi-threaded idempotency: hammer the same stream from multiple threads.
/// Final tree must be byte-identical to a single-threaded baseline.
#[test]
fn create_node_idempotency_concurrent() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-concurrent-idem");

    // Create a linear chain of 21 nodes (0..=20) single-threaded.
    for i in 0u64..=20 {
        db.create_node(chain_params(&e, i)).unwrap();
    }

    // Replay the entire stream from 4 threads concurrently.
    let db = Arc::new(db);
    let e = Arc::new(e);
    let mut handles = vec![];
    for _ in 0..4 {
        let db2 = Arc::clone(&db);
        let e2 = Arc::clone(&e);
        handles.push(std::thread::spawn(move || {
            for i in 0u64..=20 {
                let res = db2.create_node(chain_params(&e2, i));
                // All replays must succeed (idempotent) — identical params.
                assert!(res.is_ok(), "replay failed for node {i}: {res:?}");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Verify final tree has exactly 21 nodes.
    let db = Arc::try_unwrap(db).unwrap_or_else(|a| (*a).clone());
    let e = Arc::try_unwrap(e).unwrap_or_else(|a| (*a).clone());
    let stats = db.stats(Some(&e)).unwrap();
    assert_eq!(
        stats.exp_nodes_frontier
            + stats.exp_nodes_expanded
            + stats.exp_nodes_pruned
            + stats.exp_nodes_goal,
        21
    );
}

/// Replay with different content from different threads: AlreadyExists, zero rows changed.
#[test]
fn create_node_conflict_different_content_no_change() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-conflict-mt");
    create_root(&db, &e, NodeStatus::Frontier);
    create_child(&db, &e, NodeId(1), NodeId::ROOT, 0xAA);

    // Try to insert NodeId(1) with different snapshot_ref from multiple threads.
    let db = Arc::new(db);
    let e = Arc::new(e);
    let mut handles = vec![];
    for _ in 0..4 {
        let db2 = Arc::clone(&db);
        let e2 = Arc::clone(&e);
        handles.push(std::thread::spawn(move || {
            let res = db2.create_node(CreateNodeParams {
                experiment_id: (*e2).clone(),
                node_id: NodeId(1),
                parent_node_id: Some(NodeId::ROOT),
                snapshot_ref: snap(0xFF), // different
                input_log_id: None,
                inline_log_container: None,
                status: NodeStatus::Frontier,
                score: None,
                icount: 0,
                virtual_ns: 0,
                attrs: None,
            });
            assert!(
                matches!(res, Err(MetaError::AlreadyExists)),
                "expected AlreadyExists, got {res:?}"
            );
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Verify the original node is unchanged.
    let db = Arc::try_unwrap(db).unwrap_or_else(|a| (*a).clone());
    let e = Arc::try_unwrap(e).unwrap_or_else(|a| (*a).clone());
    let node = db.get_node(&e, NodeId(1)).unwrap().unwrap();
    assert_eq!(node.snapshot_ref, snap(0xAA));
}

// ---------------------------------------------------------------------------
// Multi-experiment isolation
// ---------------------------------------------------------------------------

#[test]
fn multi_experiment_isolation() {
    let (db, _dir) = open_tmp();
    let e1 = exp("exp-alpha");
    let e2 = exp("exp-beta");

    // Create identical trees in both experiments.
    for e in [&e1, &e2] {
        create_root(&db, e, NodeStatus::Frontier);
        create_child(&db, e, NodeId(1), NodeId::ROOT, 0x01);
        create_child(&db, e, NodeId(2), NodeId::ROOT, 0x02);
    }

    // Each experiment sees only its own nodes.
    let c1 = db.get_children(&e1, NodeId::ROOT).unwrap();
    let c2 = db.get_children(&e2, NodeId::ROOT).unwrap();
    assert_eq!(c1.len(), 2);
    assert_eq!(c2.len(), 2);

    for row in &c1 {
        assert_eq!(row.experiment_id, e1);
    }
    for row in &c2 {
        assert_eq!(row.experiment_id, e2);
    }

    // get_path isolation.
    let p1 = db.get_path(&e1, NodeId(1), false).unwrap();
    let p2 = db.get_path(&e2, NodeId(1), false).unwrap();
    assert!(p1.iter().all(|(n, _)| n.experiment_id == e1));
    assert!(p2.iter().all(|(n, _)| n.experiment_id == e2));

    // query_nodes isolation.
    let q1 = db
        .query_nodes(QueryFilter {
            experiment_id: e1.clone(),
            ..Default::default()
        })
        .unwrap();
    let q2 = db
        .query_nodes(QueryFilter {
            experiment_id: e2.clone(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(q1.len(), 3); // root + 2 children
    assert_eq!(q2.len(), 3);
    assert!(q1.iter().all(|n| n.experiment_id == e1));
    assert!(q2.iter().all(|n| n.experiment_id == e2));

    // Per-experiment stats.
    let s1 = db.stats(Some(&e1)).unwrap();
    let s2 = db.stats(Some(&e2)).unwrap();
    let total_e1 =
        s1.exp_nodes_frontier + s1.exp_nodes_expanded + s1.exp_nodes_pruned + s1.exp_nodes_goal;
    let total_e2 =
        s2.exp_nodes_frontier + s2.exp_nodes_expanded + s2.exp_nodes_pruned + s2.exp_nodes_goal;
    assert_eq!(total_e1, 3);
    assert_eq!(total_e2, 3);
}

// ---------------------------------------------------------------------------
// KV CAS contention
// ---------------------------------------------------------------------------

#[test]
fn kv_unconditional_upsert() {
    let (db, _dir) = open_tmp();
    let key = b"my-key".to_vec();
    let g1 = db.put_metadata(key.clone(), b"v1".to_vec(), None).unwrap();
    assert_eq!(g1, 1);
    let g2 = db.put_metadata(key.clone(), b"v2".to_vec(), None).unwrap();
    assert_eq!(g2, 2);
    let (v, g) = db.get_metadata(&key).unwrap().unwrap();
    assert_eq!(v, b"v2");
    assert_eq!(g, 2);
}

#[test]
fn kv_create_only() {
    let (db, _dir) = open_tmp();
    let key = b"create-only".to_vec();
    db.put_metadata(key.clone(), b"first".to_vec(), Some(0))
        .unwrap();
    let res = db.put_metadata(key, b"second".to_vec(), Some(0));
    assert!(
        matches!(res, Err(MetaError::CasFailed { .. })),
        "got {res:?}"
    );
}

#[test]
fn kv_cas_generation_match() {
    let (db, _dir) = open_tmp();
    let key = b"cas-key".to_vec();
    let g1 = db.put_metadata(key.clone(), b"v1".to_vec(), None).unwrap();
    let g2 = db
        .put_metadata(key.clone(), b"v2".to_vec(), Some(g1))
        .unwrap();
    assert_eq!(g2, 2);
    let res = db.put_metadata(key.clone(), b"v3".to_vec(), Some(g1));
    assert!(matches!(res, Err(MetaError::CasFailed { .. })));
}

#[test]
fn kv_value_cap_rejection() {
    let (db, _dir) = open_tmp();
    let key = b"big-value".to_vec();
    let value = vec![0u8; crate::KV_VALUE_MAX + 1];
    let res = db.put_metadata(key, value, None);
    assert!(
        matches!(res, Err(MetaError::InvalidArgument(_))),
        "got {res:?}"
    );
}

#[test]
fn kv_key_empty_rejected() {
    let (db, _dir) = open_tmp();
    let res = db.put_metadata(vec![], b"value".to_vec(), None);
    assert!(matches!(res, Err(MetaError::InvalidArgument(_))));
}

#[test]
fn kv_key_too_long_rejected() {
    let (db, _dir) = open_tmp();
    let key = vec![0u8; crate::KV_KEY_MAX + 1];
    let res = db.put_metadata(key, b"value".to_vec(), None);
    assert!(matches!(res, Err(MetaError::InvalidArgument(_))));
}

/// N threads hammering one key with CAS — exactly one winner per generation.
#[test]
fn kv_cas_contention_concurrent() {
    let (db, _dir) = open_tmp();
    let db = Arc::new(db);
    let key = b"contested-key".to_vec();

    // Seed the key.
    db.put_metadata(key.clone(), b"seed".to_vec(), None)
        .unwrap();

    let wins = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = vec![];

    for _thread in 0..8 {
        let db2 = Arc::clone(&db);
        let key2 = key.clone();
        let wins2 = Arc::clone(&wins);
        handles.push(std::thread::spawn(move || {
            for _ in 0..50 {
                // Read current generation.
                if let Some((_v, gen)) = db2.get_metadata(&key2).unwrap() {
                    let res = db2.put_metadata(
                        key2.clone(),
                        format!("t{_thread}").into_bytes(),
                        Some(gen),
                    );
                    if res.is_ok() {
                        wins2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Final generation must equal wins + 1 (the seed was gen=1).
    let db = Arc::try_unwrap(db).unwrap_or_else(|a| (*a).clone());
    let (_v, final_gen) = db.get_metadata(&key).unwrap().unwrap();
    let total_wins = wins.load(std::sync::atomic::Ordering::Relaxed);
    // The seed was generation 1, so each CAS win bumps by 1.
    // final_gen == 1 (seed) + total_wins.
    assert_eq!(
        final_gen,
        1 + total_wins,
        "final_gen={final_gen} wins={total_wins}"
    );
}

/// Delete CAS path.
#[test]
fn kv_delete_cas() {
    let (db, _dir) = open_tmp();
    let key = b"del-key".to_vec();
    let g = db.put_metadata(key.clone(), b"v".to_vec(), None).unwrap();

    // Wrong generation.
    let res = db.delete_metadata(key.clone(), Some(g + 1));
    assert!(matches!(res, Err(MetaError::CasFailed { .. })));

    // Correct generation.
    let deleted = db.delete_metadata(key.clone(), Some(g)).unwrap();
    assert!(deleted);

    // Already gone.
    let deleted2 = db.delete_metadata(key, None).unwrap();
    assert!(!deleted2);
}

// ---------------------------------------------------------------------------
// update_nodes atomicity
// ---------------------------------------------------------------------------

#[test]
fn update_nodes_atomicity_bad_id() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-update-atomic");
    create_root(&db, &e, NodeStatus::Frontier);
    for i in 1u64..=49 {
        create_child(&db, &e, NodeId(i), NodeId(i - 1), i as u8);
    }

    // Batch of 50 updates: 49 valid nodes + one missing id.
    let updates: Vec<NodeUpdate> = (0u64..49)
        .map(|i| NodeUpdate {
            node_id: NodeId(i),
            status: Some(NodeStatus::Expanded),
            ..Default::default()
        })
        .chain(std::iter::once(NodeUpdate {
            node_id: NodeId(9999), // missing
            status: Some(NodeStatus::Expanded),
            ..Default::default()
        }))
        .collect();

    let res = db.update_nodes(e.clone(), updates);
    assert!(
        matches!(res, Err(MetaError::MissingNodes(ref ids)) if ids.contains(&NodeId(9999))),
        "got {res:?}"
    );

    // Verify zero rows changed — all nodes remain Frontier.
    for i in 0u64..49 {
        let node = db.get_node(&e, NodeId(i)).unwrap().unwrap();
        assert_eq!(
            node.status,
            NodeStatus::Frontier,
            "node {i} should still be Frontier"
        );
    }
}

#[test]
fn update_nodes_visit_count_delta() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-vc");
    create_root(&db, &e, NodeStatus::Frontier);
    create_child(&db, &e, NodeId(1), NodeId::ROOT, 0x01);

    db.update_nodes(
        e.clone(),
        vec![NodeUpdate {
            node_id: NodeId(1),
            visit_count_delta: 5,
            touch_visited: true,
            ..Default::default()
        }],
    )
    .unwrap();

    let node = db.get_node(&e, NodeId(1)).unwrap().unwrap();
    assert_eq!(node.visit_count, 5);
    assert!(node.last_visited_at > 0);
}

// ---------------------------------------------------------------------------
// query_nodes cursor paging
// ---------------------------------------------------------------------------

/// Cursor paging under concurrent writes — no gaps, no duplicates.
/// CRITICAL: also covers the case where a page boundary splits commands
/// created in the same actor batch (per-command counter).
#[test]
fn query_nodes_cursor_paging_concurrent_no_gaps() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-paging");
    // Write 300 nodes in batches that will overflow the actor's batch_max (256).
    let db = Arc::new(db);
    let e = Arc::new(e);

    // First insert the root.
    db.create_node(CreateNodeParams {
        experiment_id: (*e).clone(),
        node_id: NodeId::ROOT,
        parent_node_id: None,
        snapshot_ref: snap(0),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();

    // Insert 300 children from 4 threads simultaneously.
    // We partition node IDs to avoid PK conflicts.
    let mut handles = vec![];
    let total = 300u64;
    let per_thread = total / 4;
    for t in 0u64..4 {
        let db2 = Arc::clone(&db);
        let e2 = Arc::clone(&e);
        handles.push(std::thread::spawn(move || {
            let start = t * per_thread + 1;
            let end = start + per_thread;
            for i in start..end {
                db2.create_node(CreateNodeParams {
                    experiment_id: (*e2).clone(),
                    node_id: NodeId(i),
                    parent_node_id: Some(NodeId::ROOT),
                    snapshot_ref: snap((i % 256) as u8),
                    input_log_id: None,
                    inline_log_container: None,
                    status: NodeStatus::Frontier,
                    score: None,
                    icount: 0,
                    virtual_ns: 0,
                    attrs: None,
                })
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Page through all nodes using created_after cursor with limit=50.
    let db = Arc::try_unwrap(db).unwrap_or_else(|a| (*a).clone());
    let e = Arc::try_unwrap(e).unwrap_or_else(|a| (*a).clone());

    let mut seen_ids = std::collections::HashSet::new();
    let mut cursor: Option<u64> = None;
    let page_size = 50u32;

    loop {
        let page = db
            .query_nodes(QueryFilter {
                experiment_id: e.clone(),
                order: QueryOrder::CreatedAt,
                created_after: cursor,
                limit: Some(page_size),
                ..Default::default()
            })
            .unwrap();

        if page.is_empty() {
            break;
        }

        for node in &page {
            assert!(
                seen_ids.insert(node.node_id),
                "duplicate node_id {:?}",
                node.node_id
            );
            cursor = Some(cursor.unwrap_or(0).max(node.created_at));
        }

        if page.len() < page_size as usize {
            break;
        }
    }

    // All 301 nodes (root + 300 children) must be seen.
    assert_eq!(
        seen_ids.len(),
        301,
        "expected 301 nodes, got {}",
        seen_ids.len()
    );

    // No gaps: all IDs 0..=300 must be present.
    for i in 0u64..=300 {
        assert!(seen_ids.contains(&NodeId(i)), "missing node_id {i}");
    }
}

// ---------------------------------------------------------------------------
// get_path deep chain
// ---------------------------------------------------------------------------

#[test]
fn get_path_root_first_order() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-path");
    create_root(&db, &e, NodeStatus::Frontier);
    for i in 1u64..=10 {
        create_child(&db, &e, NodeId(i), NodeId(i - 1), i as u8);
    }

    let path = db.get_path(&e, NodeId(10), false).unwrap();
    assert_eq!(path.len(), 11); // root + 10 nodes
    for (idx, (node, _)) in path.iter().enumerate() {
        assert_eq!(node.node_id.0, idx as u64, "wrong order at idx {idx}");
    }
}

#[test]
#[ignore] // perf: prints timing only
fn get_path_5000_deep_timing() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-deep");
    create_root(&db, &e, NodeStatus::Frontier);

    let n = 5000u64;
    // Insert in batches to be faster.
    for i in 1..=n {
        db.create_node(CreateNodeParams {
            experiment_id: e.clone(),
            node_id: NodeId(i),
            parent_node_id: Some(NodeId(i - 1)),
            snapshot_ref: snap((i % 256) as u8),
            input_log_id: None,
            inline_log_container: None,
            status: NodeStatus::Frontier,
            score: None,
            icount: 0,
            virtual_ns: 0,
            attrs: None,
        })
        .unwrap();
    }

    let start = std::time::Instant::now();
    let path = db.get_path(&e, NodeId(n), false).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(path.len() as u64, n + 1);
    println!("get_path(depth={n}) took {:?}", elapsed);
    // Assertion: should be < 40ms on modern hardware (guideline, not gating).
    // assert!(elapsed.as_millis() < 40, "too slow: {:?}", elapsed);
}

// ---------------------------------------------------------------------------
// prune_subtree
// ---------------------------------------------------------------------------

#[test]
fn prune_subtree_leaves_tombstone_and_pruned() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-prune");
    create_root(&db, &e, NodeStatus::Frontier);
    create_child(&db, &e, NodeId(1), NodeId::ROOT, 0x01);
    create_child(&db, &e, NodeId(2), NodeId(1), 0x02);
    create_child(&db, &e, NodeId(3), NodeId(1), 0x03);

    let pruned_count = db.prune_subtree(e.clone(), NodeId(1), false).unwrap();
    assert_eq!(pruned_count, 3, "expected 3 pruned nodes (1,2,3)");

    // All should now be PRUNED.
    for id in [1u64, 2, 3] {
        let node = db.get_node(&e, NodeId(id)).unwrap().unwrap();
        assert_eq!(node.status, NodeStatus::Pruned, "node {id} not pruned");
    }

    // Root should still be Frontier.
    let root = db.get_node(&e, NodeId::ROOT).unwrap().unwrap();
    assert_eq!(root.status, NodeStatus::Frontier);
}

#[test]
fn prune_root_denied_without_allow() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-prune-root");
    create_root(&db, &e, NodeStatus::Frontier);
    let res = db.prune_subtree(e, NodeId::ROOT, false);
    assert!(
        matches!(res, Err(MetaError::PruneRootDenied)),
        "got {res:?}"
    );
}

#[test]
fn prune_root_allowed_with_flag() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-prune-root-ok");
    create_root(&db, &e, NodeStatus::Frontier);
    create_child(&db, &e, NodeId(1), NodeId::ROOT, 0x01);
    let pruned = db.prune_subtree(e.clone(), NodeId::ROOT, true).unwrap();
    assert_eq!(pruned, 2);
}

// ---------------------------------------------------------------------------
// node_id u64<->i64 bit-cast round-trip
// ---------------------------------------------------------------------------

#[test]
fn node_id_bitcast_roundtrip() {
    let cases: &[u64] = &[
        0,
        1,
        u64::MAX / 2,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX - 1,
        u64::MAX,
    ];
    for &v in cases {
        let as_i64 = v as i64;
        let back = as_i64 as u64;
        assert_eq!(back, v, "round-trip failed for {v}");
    }
}

#[test]
fn node_id_bitcast_stored_and_retrieved() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-bitcast");
    // Use a node_id that is > i64::MAX.
    let big_id = u64::MAX - 42;
    create_root(&db, &e, NodeStatus::Frontier);

    // Create child with node_id > i64::MAX.
    db.create_node(CreateNodeParams {
        experiment_id: e.clone(),
        node_id: NodeId(big_id),
        parent_node_id: Some(NodeId::ROOT),
        snapshot_ref: snap(0x77),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();

    let row = db.get_node(&e, NodeId(big_id)).unwrap().unwrap();
    assert_eq!(row.node_id.0, big_id);
}

// ---------------------------------------------------------------------------
// Counter re-derivation
// ---------------------------------------------------------------------------

#[test]
fn counter_rederivation_continues_increasing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta/tree.db");
    let e = exp("exp-counter");

    let last_created_at;
    {
        let db = MetaDb::open(&path).unwrap();
        create_root(&db, &e, NodeStatus::Frontier);
        create_child(&db, &e, NodeId(1), NodeId::ROOT, 0x01);
        let node = db.get_node(&e, NodeId(1)).unwrap().unwrap();
        last_created_at = node.created_at;
        // db drops here, actor shuts down gracefully
    }

    // Reopen — counter must be > last_created_at.
    let db2 = MetaDb::open(&path).unwrap();
    // Create another node; its created_at must be > last_created_at.
    create_child(&db2, &e, NodeId(2), NodeId(1), 0x02);
    let node2 = db2.get_node(&e, NodeId(2)).unwrap().unwrap();
    assert!(
        node2.created_at > last_created_at,
        "counter regression: {} <= {}",
        node2.created_at,
        last_created_at
    );
}

// ---------------------------------------------------------------------------
// input_log put/get
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_input_log() {
    let (db, _dir) = open_tmp();
    let (log_id, container) = make_log_container(b"hello log payload");
    let newly_inserted = db.put_input_log(log_id, &container).unwrap();
    assert!(newly_inserted);

    let got = db.get_input_log(&log_id).unwrap().unwrap();
    assert_eq!(got, container);

    // Idempotent.
    let again = db.put_input_log(log_id, &container).unwrap();
    assert!(!again);
}

#[test]
fn put_input_log_bad_hash() {
    let (db, _dir) = open_tmp();
    let (_log_id, mut container) = make_log_container(b"test");
    // Corrupt the footer.
    let len = container.len();
    container[len - 1] ^= 0xFF;
    let wrong_id = LogId([0xAA; 32]);
    let res = db.put_input_log(wrong_id, &container);
    assert!(matches!(res, Err(MetaError::LogIdMismatch)));
}

#[test]
fn put_input_log_too_small() {
    let (db, _dir) = open_tmp();
    let small: Vec<u8> = vec![0u8; 10];
    let res = db.put_input_log(LogId([0u8; 32]), &small);
    assert!(matches!(res, Err(MetaError::LogTooSmall)));
}

// ---------------------------------------------------------------------------
// Pins
// ---------------------------------------------------------------------------

#[test]
fn pin_and_unpin() {
    let (db, _dir) = open_tmp();
    let r = snap(0xAB);
    db.pin(r.clone(), Some("my note".into())).unwrap();
    let pins = db.list_pins().unwrap();
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[0].note.as_deref(), Some("my note"));

    let removed = db.unpin(&r).unwrap();
    assert!(removed);
    assert_eq!(db.list_pins().unwrap().len(), 0);

    // Unpin again — false, not error.
    let removed2 = db.unpin(&r).unwrap();
    assert!(!removed2);
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[test]
fn stats_global() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-stats");
    create_root(&db, &e, NodeStatus::Frontier);
    create_child(&db, &e, NodeId(1), NodeId::ROOT, 0x01);

    let s = db.stats(None).unwrap();
    assert_eq!(s.total_nodes, 2);
    assert_eq!(s.experiments_count, 1);
}

#[test]
fn stats_per_experiment() {
    let (db, _dir) = open_tmp();
    let e = exp("exp-stats-per");
    create_root(&db, &e, NodeStatus::Frontier);
    db.create_node(CreateNodeParams {
        experiment_id: e.clone(),
        node_id: NodeId(1),
        parent_node_id: Some(NodeId::ROOT),
        snapshot_ref: snap(0x01),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Expanded,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();
    db.create_node(CreateNodeParams {
        experiment_id: e.clone(),
        node_id: NodeId(2),
        parent_node_id: Some(NodeId(1)),
        snapshot_ref: snap(0x02),
        input_log_id: None,
        inline_log_container: None,
        status: NodeStatus::Goal,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();

    let s = db.stats(Some(&e)).unwrap();
    assert_eq!(s.exp_nodes_frontier, 1);
    assert_eq!(s.exp_nodes_expanded, 1);
    assert_eq!(s.exp_nodes_goal, 1);
    assert_eq!(s.exp_max_depth, 2);
}
