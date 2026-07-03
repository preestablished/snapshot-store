use crate::error::MetaError;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use uuid::Uuid;

const MIGRATION_001: &str = include_str!("migrations/001_initial.sql");
const MIGRATION_002: &str = include_str!("migrations/002_gc_state.sql");
const SUPPORTED_VERSION: i64 = 2;

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

/// Version-stepping migration runner.
///
/// Reads the current `schema_version` (treating "no `meta` table at all" as
/// version 0) and, while it is below `SUPPORTED_VERSION`, applies migration
/// `v+1` — DDL + seed data + `UPDATE meta SET schema_version = v+1` + a
/// `_migrations` row — all inside ONE transaction per step (`apply_migration_step`).
/// This lets a store land on any past `schema_version` (including the
/// original single-migration v1 stores) and step forward one version at a
/// time to `SUPPORTED_VERSION`, instead of short-circuiting after the first
/// migration ever applied to a given DB file.
///
/// Crash-safety: each step's DDL + seed + version bump commit atomically
/// (the phase-2 crash harness kills inside exactly this window), so a kill
/// mid-step leaves the DB at the pre-step version, safely retryable on
/// reopen.
fn run_migration(conn: &Connection) -> Result<(), MetaError> {
    loop {
        let has_meta: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='meta'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;

        let current_version: i64 = if !has_meta {
            0
        } else {
            // The row read is `optional` to self-heal stores initialized
            // before seeding moved into the migration transaction (table
            // present, singleton row missing).
            let version: Option<i64> = conn
                .query_row("SELECT schema_version FROM meta WHERE id=1", [], |row| {
                    row.get(0)
                })
                .optional()?;
            match version {
                Some(v) => v,
                None => {
                    seed_meta_rows(conn)?;
                    continue;
                }
            }
        };

        if current_version > SUPPORTED_VERSION {
            return Err(MetaError::FutureVersion {
                found: current_version,
                supported: SUPPORTED_VERSION,
            });
        }
        if current_version == SUPPORTED_VERSION {
            return Ok(());
        }

        apply_migration_step(conn, current_version + 1)?;
        // Loop again: re-read the version and apply the next step, if any.
    }
}

/// Apply migration step `target_version` (i.e. step from `target_version - 1`
/// to `target_version`) inside one transaction.
///
/// A kill at any instant leaves either the pre-step version fully intact or
/// the post-step version fully intact — a committed-DDL-but-unstamped state
/// must be unreachable.
fn apply_migration_step(conn: &Connection, target_version: i64) -> Result<(), MetaError> {
    let tx = conn.unchecked_transaction()?;
    match target_version {
        1 => {
            tx.execute_batch(MIGRATION_001)?;
            seed_meta_rows(&tx)?;
        }
        2 => {
            tx.execute_batch(MIGRATION_002)?;
            tx.execute("UPDATE meta SET schema_version=2 WHERE id=1", [])?;
            let now = now_unix();
            tx.execute(
                "INSERT OR IGNORE INTO _migrations (id, name, applied_at) VALUES (2, '002_gc_state', ?1)",
                params![now],
            )?;
        }
        v => {
            return Err(MetaError::Io(format!(
                "no migration DDL defined for schema version {v}"
            )));
        }
    }
    tx.commit()?;
    Ok(())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Seed the meta singleton row (schema_version=1) and the `_migrations` row
/// for 001. Used both by the first-open path (step to v1) and the self-heal
/// path (table present, singleton row missing — legacy pre-seed-in-txn
/// stores).
fn seed_meta_rows(conn: &Connection) -> Result<(), MetaError> {
    let store_uuid = Uuid::new_v4().to_string();
    conn.execute(
        "INSERT OR IGNORE INTO meta (id, schema_version, store_uuid, logical_counter) \
         VALUES (1, 1, ?1, 0)",
        params![store_uuid],
    )?;
    let now = now_unix();
    conn.execute(
        "INSERT OR IGNORE INTO _migrations (id, name, applied_at) VALUES (1, '001_initial', ?1)",
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
