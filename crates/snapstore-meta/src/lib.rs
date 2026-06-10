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
}
