#![forbid(unsafe_code)]

use rusqlite::{params, Connection, OptionalExtension};
use snapstore_types::SnapshotRef;
use std::path::Path;

const MIGRATION_001: &str = include_str!("migrations/001_initial.sql");
const SUPPORTED_VERSION: i64 = 1;

/// A record stored in the metadata database for one snapshot.
pub struct SnapshotRecord {
    pub r: SnapshotRef,
    pub parent: Option<SnapshotRef>,
    pub icount: u64,
    pub virtual_ns: u64,
    pub created_at: u64,
    pub label: Option<String>,
    pub page_count: u64,
    pub new_pages: u64,
}

/// Errors returned by [`MetaDb`] operations.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("parent snapshot not found in DB")]
    ParentNotFound,
    #[error("conflicting re-registration of existing ref")]
    ConflictingRegister,
    #[error("database schema version {found} is newer than supported {supported}")]
    FutureVersion { found: i64, supported: i64 },
}

/// SQLite-backed metadata store for snapshots.
pub struct MetaDb {
    conn: Connection,
}

impl std::fmt::Debug for MetaDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaDb").finish_non_exhaustive()
    }
}

impl MetaDb {
    /// Open (or create) the metadata database at `path`.
    ///
    /// Applies pending migrations and enforces WAL + foreign-key PRAGMAs.
    pub fn open(path: &Path) -> Result<Self, MetaError> {
        let conn = Connection::open(path)?;

        // Enable WAL and foreign keys.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;

        // Check whether schema_version exists.
        let has_schema_version: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='schema_version'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;

        if has_schema_version {
            // Validate version.
            let version: i64 =
                conn.query_row("SELECT version FROM schema_version", [], |row| row.get(0))?;
            if version > SUPPORTED_VERSION {
                return Err(MetaError::FutureVersion {
                    found: version,
                    supported: SUPPORTED_VERSION,
                });
            }
        } else {
            // First open — run the initial migration in a transaction.
            conn.execute_batch(&format!(
                "BEGIN;\n{}\nCOMMIT;",
                MIGRATION_001
            ))?;
        }

        Ok(Self { conn })
    }

    /// Register a new snapshot.  Idempotent for identical re-inserts;
    /// returns [`MetaError::ConflictingRegister`] if the ref exists with
    /// different data.
    pub fn register(&self, rec: &SnapshotRecord) -> Result<(), MetaError> {
        // Resolve parent ref → row id (must already be in DB).
        let parent_id: Option<i64> = if let Some(pref) = &rec.parent {
            let pid: Option<i64> = self
                .conn
                .query_row(
                    "SELECT id FROM snapshots WHERE ref = ?1",
                    params![pref.to_bytes().as_slice()],
                    |row| row.get(0),
                )
                .optional()?;
            match pid {
                Some(id) => Some(id),
                None => return Err(MetaError::ParentNotFound),
            }
        } else {
            None
        };

        // Check whether this ref is already registered.
        let existing: Option<(i64, Option<i64>, i64, i64, i64, Option<String>, i64, i64)> = self
            .conn
            .query_row(
                "SELECT id, parent_id, icount, virtual_ns, created_at, label, \
                         page_count, new_pages \
                 FROM snapshots WHERE ref = ?1",
                params![rec.r.to_bytes().as_slice()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()?;

        if let Some(ex) = existing {
            // Already present — verify all fields match.
            let data_matches = ex.1 == parent_id
                && ex.2 == rec.icount as i64
                && ex.3 == rec.virtual_ns as i64
                && ex.4 == rec.created_at as i64
                && ex.5 == rec.label
                && ex.6 == rec.page_count as i64
                && ex.7 == rec.new_pages as i64;
            if data_matches {
                return Ok(()); // idempotent
            } else {
                return Err(MetaError::ConflictingRegister);
            }
        }

        self.conn.execute(
            "INSERT INTO snapshots \
             (ref, parent_id, icount, virtual_ns, created_at, label, page_count, new_pages) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                rec.r.to_bytes().as_slice(),
                parent_id,
                rec.icount as i64,
                rec.virtual_ns as i64,
                rec.created_at as i64,
                rec.label.as_deref(),
                rec.page_count as i64,
                rec.new_pages as i64,
            ],
        )?;

        Ok(())
    }

    /// Look up a snapshot by its ref.
    pub fn get(&self, r: &SnapshotRef) -> Result<Option<SnapshotRecord>, MetaError> {
        self.conn
            .query_row(
                "SELECT s.ref, p.ref, s.icount, s.virtual_ns, s.created_at, \
                         s.label, s.page_count, s.new_pages \
                 FROM snapshots s \
                 LEFT JOIN snapshots p ON p.id = s.parent_id \
                 WHERE s.ref = ?1",
                params![r.to_bytes().as_slice()],
                row_to_record,
            )
            .optional()
            .map_err(MetaError::from)
    }

    /// Look up a snapshot by its human-readable label.
    pub fn get_by_label(&self, label: &str) -> Result<Option<SnapshotRecord>, MetaError> {
        self.conn
            .query_row(
                "SELECT s.ref, p.ref, s.icount, s.virtual_ns, s.created_at, \
                         s.label, s.page_count, s.new_pages \
                 FROM snapshots s \
                 LEFT JOIN snapshots p ON p.id = s.parent_id \
                 WHERE s.label = ?1",
                params![label],
                row_to_record,
            )
            .optional()
            .map_err(MetaError::from)
    }

    /// Attach or detach a label from a snapshot.  `label = None` clears it.
    pub fn set_label(&self, r: &SnapshotRef, label: Option<&str>) -> Result<(), MetaError> {
        self.conn.execute(
            "UPDATE snapshots SET label = ?1 WHERE ref = ?2",
            params![label, r.to_bytes().as_slice()],
        )?;
        Ok(())
    }

    /// Return the chain of ancestors for `r`, root-first (oldest ancestor at
    /// index 0, immediate parent last).
    pub fn ancestors(&self, r: &SnapshotRef) -> Result<Vec<SnapshotRecord>, MetaError> {
        let mut stmt = self.conn.prepare(
            "WITH RECURSIVE anc(id) AS (
                SELECT parent_id FROM snapshots WHERE ref = ?1
                UNION ALL
                SELECT s.parent_id FROM snapshots s JOIN anc ON s.id = anc.id
                WHERE s.parent_id IS NOT NULL
             )
             SELECT s.ref, p.ref, s.icount, s.virtual_ns, s.created_at,
                    s.label, s.page_count, s.new_pages
             FROM anc
             JOIN snapshots s ON s.id = anc.id
             LEFT JOIN snapshots p ON p.id = s.parent_id
             ORDER BY s.id ASC",
        )?;

        let rows = stmt.query_map(params![r.to_bytes().as_slice()], row_to_record)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
    }

    /// Return all descendants of `r` in BFS order (children before
    /// grandchildren).
    pub fn descendants(&self, r: &SnapshotRef) -> Result<Vec<SnapshotRecord>, MetaError> {
        let mut stmt = self.conn.prepare(
            "WITH RECURSIVE desc_cte(id, depth) AS (
                SELECT id, 0 FROM snapshots WHERE ref = ?1
                UNION ALL
                SELECT s.id, d.depth + 1
                FROM snapshots s JOIN desc_cte d ON s.parent_id = d.id
             )
             SELECT s.ref, p.ref, s.icount, s.virtual_ns, s.created_at,
                    s.label, s.page_count, s.new_pages
             FROM desc_cte d
             JOIN snapshots s ON s.id = d.id
             LEFT JOIN snapshots p ON p.id = s.parent_id
             WHERE d.depth > 0
             ORDER BY d.depth ASC, s.id ASC",
        )?;

        let rows = stmt.query_map(params![r.to_bytes().as_slice()], row_to_record)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
    }

    /// Return the immediate children of `r`.
    pub fn children(&self, r: &SnapshotRef) -> Result<Vec<SnapshotRecord>, MetaError> {
        let mut stmt = self.conn.prepare(
            "SELECT s.ref, p.ref, s.icount, s.virtual_ns, s.created_at,
                    s.label, s.page_count, s.new_pages
             FROM snapshots s
             JOIN snapshots parent ON parent.id = s.parent_id
             LEFT JOIN snapshots p ON p.id = s.parent_id
             WHERE parent.ref = ?1
             ORDER BY s.id ASC",
        )?;

        let rows = stmt.query_map(params![r.to_bytes().as_slice()], row_to_record)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
    }

    /// Return all snapshots that have no children (leaf nodes / heads).
    ///
    /// Uses `NOT EXISTS` rather than `NOT IN` to correctly handle NULL
    /// parent_id values.
    pub fn heads(&self) -> Result<Vec<SnapshotRecord>, MetaError> {
        let mut stmt = self.conn.prepare(
            "SELECT s.ref, p.ref, s.icount, s.virtual_ns, s.created_at,
                    s.label, s.page_count, s.new_pages
             FROM snapshots s
             LEFT JOIN snapshots p ON p.id = s.parent_id
             WHERE NOT EXISTS (
                 SELECT 1 FROM snapshots c WHERE c.parent_id = s.id
             )
             ORDER BY s.id ASC",
        )?;

        let rows = stmt.query_map([], row_to_record)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(MetaError::from)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Map a query row (ref BLOB, parent_ref BLOB|NULL, icount, virtual_ns,
/// created_at, label, page_count, new_pages) into a [`SnapshotRecord`].
fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<SnapshotRecord> {
    let ref_blob: Vec<u8> = row.get(0)?;
    let parent_blob: Option<Vec<u8>> = row.get(1)?;
    let icount: i64 = row.get(2)?;
    let virtual_ns: i64 = row.get(3)?;
    let created_at: i64 = row.get(4)?;
    let label: Option<String> = row.get(5)?;
    let page_count: i64 = row.get(6)?;
    let new_pages: i64 = row.get(7)?;

    let r = blob_to_ref(&ref_blob);
    let parent = parent_blob.as_deref().map(blob_to_ref_slice);

    Ok(SnapshotRecord {
        r,
        parent,
        icount: icount as u64,
        virtual_ns: virtual_ns as u64,
        created_at: created_at as u64,
        label,
        page_count: page_count as u64,
        new_pages: new_pages as u64,
    })
}

fn blob_to_ref(b: &[u8]) -> SnapshotRef {
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&b[..32]);
    SnapshotRef::from_bytes(arr)
}

fn blob_to_ref_slice(b: &[u8]) -> SnapshotRef {
    blob_to_ref(b)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    fn open_tmp() -> MetaDb {
        let f = NamedTempFile::new().unwrap();
        MetaDb::open(f.path()).unwrap()
    }

    fn make_ref(b: u8) -> SnapshotRef {
        SnapshotRef::from_bytes([b; 32])
    }

    fn simple_rec(r: SnapshotRef, parent: Option<SnapshotRef>) -> SnapshotRecord {
        SnapshotRecord {
            r,
            parent,
            icount: 1,
            virtual_ns: 0,
            created_at: 42,
            label: None,
            page_count: 10,
            new_pages: 5,
        }
    }

    // -----------------------------------------------------------------------
    // Existing tests
    // -----------------------------------------------------------------------

    #[test]
    fn open_creates_schema() {
        let _db = open_tmp();
    }

    #[test]
    fn register_and_get() {
        let db = open_tmp();
        let r = make_ref(0x01);
        db.register(&simple_rec(r.clone(), None)).unwrap();
        let got = db.get(&r).unwrap().expect("should find it");
        assert_eq!(got.r, r);
        assert_eq!(got.parent, None);
        assert_eq!(got.icount, 1);
    }

    #[test]
    fn register_idempotent() {
        let db = open_tmp();
        let r = make_ref(0x02);
        db.register(&simple_rec(r.clone(), None)).unwrap();
        db.register(&simple_rec(r.clone(), None)).unwrap(); // second call ok
    }

    #[test]
    fn register_conflict_detected() {
        let db = open_tmp();
        let r = make_ref(0x03);
        db.register(&simple_rec(r.clone(), None)).unwrap();
        let mut conflict = simple_rec(r.clone(), None);
        conflict.icount = 999;
        let err = db.register(&conflict).unwrap_err();
        assert!(matches!(err, MetaError::ConflictingRegister));
    }

    #[test]
    fn parent_not_found_error() {
        let db = open_tmp();
        let r = make_ref(0x04);
        let parent = make_ref(0x05);
        let err = db.register(&simple_rec(r, Some(parent))).unwrap_err();
        assert!(matches!(err, MetaError::ParentNotFound));
    }

    #[test]
    fn label_roundtrip() {
        let db = open_tmp();
        let r = make_ref(0x06);
        let mut rec = simple_rec(r.clone(), None);
        rec.label = Some("my-label".to_string());
        db.register(&rec).unwrap();
        let got = db.get_by_label("my-label").unwrap().expect("label lookup");
        assert_eq!(got.r, r);
    }

    #[test]
    fn set_label_and_clear() {
        let db = open_tmp();
        let r = make_ref(0x07);
        db.register(&simple_rec(r.clone(), None)).unwrap();
        db.set_label(&r, Some("tag")).unwrap();
        assert!(db.get_by_label("tag").unwrap().is_some());
        db.set_label(&r, None).unwrap();
        assert!(db.get_by_label("tag").unwrap().is_none());
    }

    #[test]
    fn ancestors_chain() {
        let db = open_tmp();
        let root = make_ref(0x10);
        let child = make_ref(0x11);
        let grandchild = make_ref(0x12);
        db.register(&simple_rec(root.clone(), None)).unwrap();
        db.register(&simple_rec(child.clone(), Some(root.clone()))).unwrap();
        db.register(&simple_rec(grandchild.clone(), Some(child.clone()))).unwrap();

        let ancs = db.ancestors(&grandchild).unwrap();
        assert_eq!(ancs.len(), 2);
        // root-first order
        assert_eq!(ancs[0].r, root);
        assert_eq!(ancs[1].r, child);
    }

    #[test]
    fn descendants_order() {
        let db = open_tmp();
        let root = make_ref(0x20);
        let c1 = make_ref(0x21);
        let c2 = make_ref(0x22);
        let gc = make_ref(0x23);
        db.register(&simple_rec(root.clone(), None)).unwrap();
        db.register(&simple_rec(c1.clone(), Some(root.clone()))).unwrap();
        db.register(&simple_rec(c2.clone(), Some(root.clone()))).unwrap();
        db.register(&simple_rec(gc.clone(), Some(c1.clone()))).unwrap();

        let desc = db.descendants(&root).unwrap();
        assert_eq!(desc.len(), 3);
        // c1 and c2 at depth 1 before gc at depth 2
        let names: Vec<_> = desc.iter().map(|r| r.r.clone()).collect();
        assert!(names[0] == c1 || names[0] == c2);
        assert!(names[1] == c1 || names[1] == c2);
        assert_eq!(names[2], gc);
    }

    #[test]
    fn children_only_direct() {
        let db = open_tmp();
        let root = make_ref(0x30);
        let c1 = make_ref(0x31);
        let gc = make_ref(0x32);
        db.register(&simple_rec(root.clone(), None)).unwrap();
        db.register(&simple_rec(c1.clone(), Some(root.clone()))).unwrap();
        db.register(&simple_rec(gc.clone(), Some(c1.clone()))).unwrap();

        let ch = db.children(&root).unwrap();
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].r, c1);
    }

    #[test]
    fn heads_no_children() {
        let db = open_tmp();
        let root = make_ref(0x40);
        let c1 = make_ref(0x41);
        let c2 = make_ref(0x42);
        db.register(&simple_rec(root.clone(), None)).unwrap();
        db.register(&simple_rec(c1.clone(), Some(root.clone()))).unwrap();
        db.register(&simple_rec(c2.clone(), Some(root.clone()))).unwrap();

        let heads = db.heads().unwrap();
        assert_eq!(heads.len(), 2);
        let hrefs: Vec<_> = heads.iter().map(|r| r.r.clone()).collect();
        assert!(hrefs.contains(&c1));
        assert!(hrefs.contains(&c2));
    }

    #[test]
    fn future_version_rejected() {
        let f = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(f.path()).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER NOT NULL); \
                 INSERT INTO schema_version VALUES (999);",
            )
            .unwrap();
        }
        let err = MetaDb::open(f.path()).unwrap_err();
        assert!(matches!(err, MetaError::FutureVersion { found: 999, .. }));
    }

    // -----------------------------------------------------------------------
    // 1. Migration tests
    // -----------------------------------------------------------------------

    /// Opening an existing v1 DB a second time is a no-op (no error).
    #[test]
    fn migration_reopen_v1_is_noop() {
        let f = NamedTempFile::new().unwrap();
        // First open creates schema.
        MetaDb::open(f.path()).unwrap();
        // Second open sees schema_version=1 and must succeed without error.
        MetaDb::open(f.path()).unwrap();
    }

    /// Manually inserting schema_version=9999 causes FutureVersion on open.
    #[test]
    fn migration_future_version_9999() {
        let f = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(f.path()).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER NOT NULL); \
                 INSERT INTO schema_version VALUES (9999);",
            )
            .unwrap();
        }
        let err = MetaDb::open(f.path()).unwrap_err();
        assert!(
            matches!(err, MetaError::FutureVersion { found: 9999, .. }),
            "expected FutureVersion{{found: 9999}}, got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------------
    // 2. Chain test: 100-deep chain
    // -----------------------------------------------------------------------

    /// Register a 100-deep chain (snapshot 0 = root, snapshot 100 = tip).
    /// - `ancestors(tip)` returns 100 records (indices 0..99), root-first.
    ///   (ancestors does NOT include the tip itself)
    /// - `heads()` returns just the tip.
    #[test]
    fn chain_100_deep() {
        let db = open_tmp();

        // snapshot i uses SnapshotRef([i as u8; 32]), i in 0..=100
        // snapshot 0 is root (no parent), snapshot i has parent i-1.
        for i in 0u8..=100 {
            let r = SnapshotRef([i; 32]);
            let parent = if i == 0 {
                None
            } else {
                Some(SnapshotRef([(i - 1); 32]))
            };
            db.register(&simple_rec(r, parent)).unwrap();
        }

        let tip = SnapshotRef([100u8; 32]);

        // ancestors(tip) returns 100 records: snapshots 0..=99 (not tip itself)
        let ancs = db.ancestors(&tip).unwrap();
        assert_eq!(
            ancs.len(),
            100,
            "expected 100 ancestors, got {}",
            ancs.len()
        );
        // Root-first: ancs[0] should be snapshot 0, ancs[99] should be snapshot 99
        assert_eq!(ancs[0].r, SnapshotRef([0u8; 32]), "first ancestor should be root");
        assert_eq!(ancs[99].r, SnapshotRef([99u8; 32]), "last ancestor should be tip's parent");

        // heads() returns just the tip (snapshot 100)
        let heads = db.heads().unwrap();
        assert_eq!(heads.len(), 1, "expected 1 head, got {}", heads.len());
        assert_eq!(heads[0].r, tip, "head should be the tip");
    }

    // -----------------------------------------------------------------------
    // 3. Fork test
    // -----------------------------------------------------------------------

    /// Register root + 2 children.
    /// - `descendants(root)` returns 2
    /// - `children(root)` returns 2 direct children
    /// - `heads()` returns both children
    #[test]
    fn fork_two_children() {
        let db = open_tmp();
        let root = SnapshotRef([0xA0u8; 32]);
        let child_a = SnapshotRef([0xA1u8; 32]);
        let child_b = SnapshotRef([0xA2u8; 32]);

        db.register(&simple_rec(root.clone(), None)).unwrap();
        db.register(&simple_rec(child_a.clone(), Some(root.clone()))).unwrap();
        db.register(&simple_rec(child_b.clone(), Some(root.clone()))).unwrap();

        let desc = db.descendants(&root).unwrap();
        assert_eq!(desc.len(), 2, "descendants should be 2, got {}", desc.len());

        let ch = db.children(&root).unwrap();
        assert_eq!(ch.len(), 2, "direct children should be 2, got {}", ch.len());
        let child_refs: Vec<_> = ch.iter().map(|r| r.r.clone()).collect();
        assert!(child_refs.contains(&child_a));
        assert!(child_refs.contains(&child_b));

        let heads = db.heads().unwrap();
        assert_eq!(heads.len(), 2, "heads should be 2 (both children), got {}", heads.len());
        let head_refs: Vec<_> = heads.iter().map(|r| r.r.clone()).collect();
        assert!(head_refs.contains(&child_a));
        assert!(head_refs.contains(&child_b));
    }

    // -----------------------------------------------------------------------
    // 4. Idempotency / error tests
    // -----------------------------------------------------------------------

    /// Re-registering an identical record returns Ok.
    #[test]
    fn idempotency_identical_reregister() {
        let db = open_tmp();
        let r = SnapshotRef([0xB0u8; 32]);
        let rec = simple_rec(r.clone(), None);
        db.register(&rec).unwrap();
        // Exact same data → Ok
        db.register(&simple_rec(r, None)).unwrap();
    }

    /// Conflicting re-register (same ref, different data) returns ConflictingRegister.
    #[test]
    fn idempotency_conflicting_reregister() {
        let db = open_tmp();
        let r = SnapshotRef([0xB1u8; 32]);
        db.register(&simple_rec(r.clone(), None)).unwrap();
        let mut conflict = simple_rec(r, None);
        conflict.icount = 42; // different from the default 1
        let err = db.register(&conflict).unwrap_err();
        assert!(
            matches!(err, MetaError::ConflictingRegister),
            "expected ConflictingRegister, got {:?}",
            err
        );
    }

    /// Registering with a parent ref not in DB returns ParentNotFound.
    #[test]
    fn idempotency_parent_not_found() {
        let db = open_tmp();
        let r = SnapshotRef([0xB2u8; 32]);
        let missing_parent = SnapshotRef([0xFFu8; 32]);
        let err = db.register(&simple_rec(r, Some(missing_parent))).unwrap_err();
        assert!(
            matches!(err, MetaError::ParentNotFound),
            "expected ParentNotFound, got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------------
    // 5. Property test: deterministic random tree of 50 snapshots
    // -----------------------------------------------------------------------

    /// Build a deterministic pseudo-random tree of 50 snapshots.
    /// The tree is mirrored in a HashMap<[u8;32], Option<[u8;32]>>.
    /// Verifies:
    ///   - For each snapshot, `ancestors(r)` matches the chain computed from the map.
    ///   - For a mid-chain ref, `descendants(r)` includes all expected descendants.
    ///   - `heads()` returns exactly the leaf nodes (no children in the map).
    #[test]
    fn property_random_tree_50() {
        // ----------------------------------------------------------------
        // Build the tree deterministically.
        // Node i gets byte pattern [i as u8; 32].  The parent of node i (i>0)
        // is chosen as node (lcg(i) % i) where lcg is a simple LCG step,
        // giving a varied but reproducible parent selection.
        // ----------------------------------------------------------------
        const N: usize = 50;

        // Simple LCG: next = (a * x + c) % m  with Knuth's constants.
        fn lcg(x: usize) -> usize {
            x.wrapping_mul(1664525).wrapping_add(1013904223)
        }

        // ref_of(i) → [i as u8; 32]
        let ref_of = |i: usize| -> [u8; 32] { [i as u8; 32] };

        // Build parent map: ref → Option<parent_ref>
        let mut parent_map: HashMap<[u8; 32], Option<[u8; 32]>> = HashMap::new();
        parent_map.insert(ref_of(0), None); // root

        let db = open_tmp();
        db.register(&simple_rec(SnapshotRef(ref_of(0)), None)).unwrap();

        for i in 1..N {
            let parent_idx = lcg(i) % i;
            let parent_ref = ref_of(parent_idx);
            parent_map.insert(ref_of(i), Some(parent_ref));
            db.register(&simple_rec(
                SnapshotRef(ref_of(i)),
                Some(SnapshotRef(parent_ref)),
            ))
            .unwrap();
        }

        // ----------------------------------------------------------------
        // Helper: compute expected ancestors for node i from the map.
        // Returns refs in root-first order (not including i itself).
        // ----------------------------------------------------------------
        let ancestors_of = |start: usize| -> Vec<[u8; 32]> {
            let mut chain = Vec::new();
            let mut cur = ref_of(start);
            loop {
                match parent_map.get(&cur).copied() {
                    Some(Some(p)) => {
                        chain.push(p);
                        cur = p;
                    }
                    _ => break,
                }
            }
            chain.reverse(); // root-first
            chain
        };

        // ----------------------------------------------------------------
        // Verify: ancestors for every node.
        // ----------------------------------------------------------------
        for i in 0..N {
            let r = SnapshotRef(ref_of(i));
            let got_ancs = db.ancestors(&r).unwrap();
            let expected = ancestors_of(i);
            assert_eq!(
                got_ancs.len(),
                expected.len(),
                "ancestors({}) length mismatch: got {}, expected {}",
                i,
                got_ancs.len(),
                expected.len()
            );
            for (j, (got, exp)) in got_ancs.iter().zip(expected.iter()).enumerate() {
                assert_eq!(
                    got.r,
                    SnapshotRef(*exp),
                    "ancestors({}) index {} mismatch",
                    i,
                    j
                );
            }
        }

        // ----------------------------------------------------------------
        // Verify: descendants for node 0 (root) includes all other nodes.
        // ----------------------------------------------------------------
        let root = SnapshotRef(ref_of(0));
        let desc = db.descendants(&root).unwrap();
        assert_eq!(
            desc.len(),
            N - 1,
            "descendants(root) should have {} entries, got {}",
            N - 1,
            desc.len()
        );

        // ----------------------------------------------------------------
        // Verify: descendants for a mid-chain node (node 1) includes
        // all nodes whose ancestor chain passes through node 1.
        // ----------------------------------------------------------------
        let mid_ref = SnapshotRef(ref_of(1));
        let got_desc: Vec<_> = db.descendants(&mid_ref).unwrap()
            .into_iter()
            .map(|rec| rec.r.0)
            .collect();

        // Compute expected descendants of node 1 from the map.
        let mut expected_desc: Vec<[u8; 32]> = Vec::new();
        for i in 0..N {
            if i == 1 { continue; }
            // Walk the ancestor chain; if we hit node 1, it's a descendant.
            let mut cur = ref_of(i);
            loop {
                match parent_map.get(&cur).copied() {
                    Some(Some(p)) => {
                        if p == ref_of(1) || cur == ref_of(1) {
                            expected_desc.push(ref_of(i));
                            break;
                        }
                        cur = p;
                    }
                    _ => break,
                }
            }
        }

        for exp in &expected_desc {
            assert!(
                got_desc.contains(exp),
                "descendants(1) missing {:?}",
                exp[0]
            );
        }

        // ----------------------------------------------------------------
        // Verify: heads() returns exactly the leaf nodes.
        // ----------------------------------------------------------------
        // A leaf is a node that appears as no other node's parent.
        let has_children: std::collections::HashSet<[u8; 32]> = parent_map
            .values()
            .filter_map(|p| *p)
            .collect();
        let expected_heads: Vec<[u8; 32]> = (0..N)
            .map(ref_of)
            .filter(|r| !has_children.contains(r))
            .collect();

        let got_heads: Vec<_> = db.heads().unwrap()
            .into_iter()
            .map(|rec| rec.r.0)
            .collect();

        assert_eq!(
            got_heads.len(),
            expected_heads.len(),
            "heads() count mismatch: got {}, expected {}",
            got_heads.len(),
            expected_heads.len()
        );
        for exp in &expected_heads {
            assert!(
                got_heads.contains(exp),
                "heads() missing ref with byte {:?}",
                exp[0]
            );
        }
    }
}
