use crate::error::MetaError;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use uuid::Uuid;

const MIGRATION_001: &str = include_str!("migrations/001_initial.sql");
const SUPPORTED_VERSION: i64 = 1;

/// Open a writer connection, apply WAL pragmas, run migration if needed.
pub fn open_writer(path: &Path) -> Result<Connection, MetaError> {
    let conn = Connection::open(path)?;
    apply_writer_pragmas(&conn)?;
    run_migration(&conn)?;
    Ok(conn)
}

/// Open a read-only connection.
pub fn open_reader(path: &Path) -> Result<Connection, MetaError> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA query_only=ON; \
         PRAGMA busy_timeout=5000; \
         PRAGMA mmap_size=268435456;",
    )?;
    Ok(conn)
}

fn apply_writer_pragmas(conn: &Connection) -> Result<(), MetaError> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; \
         PRAGMA synchronous=FULL; \
         PRAGMA foreign_keys=ON; \
         PRAGMA wal_autocheckpoint=4000; \
         PRAGMA mmap_size=268435456; \
         PRAGMA busy_timeout=5000;",
    )?;
    Ok(())
}

fn run_migration(conn: &Connection) -> Result<(), MetaError> {
    // Check whether the meta table (our schema sentinel) exists.
    let has_meta: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='meta'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;

    if has_meta {
        // Already migrated — check version.
        let version: i64 =
            conn.query_row("SELECT schema_version FROM meta WHERE id=1", [], |row| {
                row.get(0)
            })?;
        if version > SUPPORTED_VERSION {
            return Err(MetaError::FutureVersion {
                found: version,
                supported: SUPPORTED_VERSION,
            });
        }
        return Ok(());
    }

    // First open — apply migration.
    conn.execute_batch(&format!("BEGIN;\n{}\nCOMMIT;", MIGRATION_001))?;

    // Seed the meta row.
    let store_uuid = Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO meta (id, schema_version, store_uuid, logical_counter) VALUES (1, 1, ?1, 0)",
        params![store_uuid],
    )?;

    // Record in migrations table.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    conn.execute(
        "INSERT INTO _migrations (id, name, applied_at) VALUES (1, '001_initial', ?1)",
        params![now],
    )?;

    Ok(())
}

/// Re-derive the logical counter from the persisted state on startup.
///
/// `counter = max(persisted, max(created_at), max(updated_at)) + 1`
pub fn rederive_counter(conn: &Connection) -> Result<u64, MetaError> {
    let persisted: i64 = conn
        .query_row("SELECT logical_counter FROM meta WHERE id=1", [], |row| {
            row.get(0)
        })
        .optional()?
        .unwrap_or(0);

    let max_created: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(created_at), 0) FROM nodes",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);

    let max_updated: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(updated_at), 0) FROM nodes",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);

    let max_val = persisted.max(max_created).max(max_updated);
    // Cast through u64 to handle values that are bit-patterns of large u64.
    let as_u64 = max_val as u64;
    Ok(as_u64.saturating_add(1))
}
