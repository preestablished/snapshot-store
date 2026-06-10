# M3 — Metadata DB (`snapstore-meta`)

**Crates:** `snapstore-meta` (new)
**Depends on:** nothing — `SnapshotRef` already exists in Phase 0
`snapstore-types`, so M3 can technically start any time; the upstream
"after M1" sequencing is a staffing choice
**Parallel with:** M2

## Scope

SQLite-backed metadata for snapshots: registration, labels, and lineage
queries. The page store and manifests are the source of truth for *content*;
`snapstore-meta` is the queryable view of *relationships* — who descends from
whom, what exists, when it was made. Pure library crate, no proto, no server.

Per the program plan this runs parallel with M2 after M1: it depends only on
`SnapshotRef` from `snapstore-types` and treats manifest data as caller-
provided fields, so it never blocks on (or is blocked by) M2's codec work.

## Work item 1 — crate + schema

Dependency: `rusqlite` with the `bundled` feature (no system sqlite drift —
determinism program, pinned everything). WAL mode, foreign keys ON.

Schema v1:

```sql
CREATE TABLE schema_version (version INTEGER NOT NULL);

CREATE TABLE snapshots (
    id          INTEGER PRIMARY KEY,
    ref         BLOB NOT NULL UNIQUE,        -- 32-byte SnapshotRef
    parent_id   INTEGER REFERENCES snapshots(id),
    icount      INTEGER NOT NULL,
    virtual_ns  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,            -- unix nanos, caller-supplied
    label       TEXT,                        -- optional human name
    page_count  INTEGER NOT NULL,
    new_pages   INTEGER NOT NULL             -- pages not deduped at commit
);
CREATE INDEX idx_snapshots_parent ON snapshots(parent_id);
CREATE UNIQUE INDEX idx_snapshots_label ON snapshots(label) WHERE label IS NOT NULL;
```

Notes:
- `parent_id` is a tree in Phase 1 (single timeline ⇒ chains; the schema
  supports *tree-shaped* forking in Phase 2 — multiple children per parent —
  with no migration). If Phase 2 lineage turns out to need true multi-parent
  DAG edges (merges), that's a `snapshot_parents` join table and a migration;
  the migration machinery in WI1 is the hedge, not the single column.
- `created_at` is caller-supplied, not `now()`: keeps the crate clock-free
  and tests deterministic.
- Migrations: embedded numbered SQL scripts run inside one transaction at
  `open()`; `schema_version` row tracks position. v1 is migration 001.

## Work item 2 — API

```rust
pub struct MetaDb { conn: rusqlite::Connection }

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

impl MetaDb {
    pub fn open(path: &Path) -> Result<Self>;       // creates + migrates
    pub fn register(&self, rec: &SnapshotRecord) -> Result<()>;
    pub fn get(&self, r: &SnapshotRef) -> Result<Option<SnapshotRecord>>;
    pub fn get_by_label(&self, label: &str) -> Result<Option<SnapshotRecord>>;
    pub fn set_label(&self, r: &SnapshotRef, label: Option<&str>) -> Result<()>;

    // Lineage queries (recursive CTEs over parent_id):
    /// Root-first chain of ancestors, ending at `r` itself.
    pub fn ancestors(&self, r: &SnapshotRef) -> Result<Vec<SnapshotRecord>>;
    /// All transitive descendants of `r` (BFS order).
    pub fn descendants(&self, r: &SnapshotRef) -> Result<Vec<SnapshotRecord>>;
    /// Direct children only.
    pub fn children(&self, r: &SnapshotRef) -> Result<Vec<SnapshotRecord>>;
    /// Snapshots with no children (timeline tips).
    pub fn heads(&self) -> Result<Vec<SnapshotRecord>>;
}
```

Implementation note for `heads()`: do **not** write
`WHERE id NOT IN (SELECT parent_id FROM snapshots)` — the subquery contains
NULL `parent_id`s (every root row), and SQL's `NOT IN` with NULL returns an
empty set. Use `NOT EXISTS` or filter `parent_id IS NOT NULL`.

Semantics:
- `register` is idempotent for an identical record (commit retries must not
  fail); a *conflicting* re-register of the same ref is an error.
- Registering with a `parent` not present in the DB is an error — lineage
  must be gap-free.
- Cycle prevention: parent must already exist and rows are immutable after
  insert (no `UPDATE` of `parent_id` in the API), so cycles are impossible
  by construction; no runtime cycle check needed.

## Work item 3 — tests

- Migration: open on empty file creates v1; reopen is a no-op; opening a
  future-versioned DB fails cleanly.
- Chain test: register a 100-deep chain; `ancestors(tip)` returns 101 records
  root-first; `heads()` returns just the tip.
- Fork-shape test (future-proofing): two children of one parent;
  `descendants(root)` and `children()` correct; `heads()` returns both tips.
- Idempotent re-register passes; conflicting re-register and missing-parent
  registration fail.
- Property test (reuses no M2 code): generate random trees, mirror them in a
  `HashMap` model, check `ancestors`/`descendants`/`heads` against the model.

## Work item 4 — M2↔M3 integration (depends on M2 WI3 *and* M3 WI1–3)

An explicit work item with its own beads issue and a line in the 04 sign-off
checklist — end-of-phase glue with no owner is how integrations silently
slip. `SnapshotStore::commit` (M2) optionally takes a `&MetaDb` and registers
the snapshot after the manifest is durable — DB write strictly last, so the
DB never references an unresolvable ref; a crash between manifest-write and
DB-register leaves an orphan manifest (harmless, re-registerable), never a
dangling DB row. The `new_pages` value comes from the commit's
`IngestOutcome.newly_written` counts.

Acceptance: one integration test in `snapstore-store` — commit with a MetaDb,
verify the record (ref, parent, icount, page counts) matches the manifest;
re-commit of identical state re-registers idempotently.

This is the only M2↔M3 touchpoint and lands after both; neither milestone
waits on the other for its own acceptance.
