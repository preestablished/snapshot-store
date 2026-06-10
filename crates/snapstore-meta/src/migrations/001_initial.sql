-- meta v2 schema (schema_version = 1)
-- Stored in <root>/meta/tree.db

CREATE TABLE IF NOT EXISTS _migrations (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);

CREATE TABLE meta (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version   INTEGER NOT NULL,
    store_uuid       TEXT NOT NULL,
    logical_counter  INTEGER NOT NULL
);

CREATE TABLE nodes (
    experiment_id    TEXT    NOT NULL,
    node_id          INTEGER NOT NULL,
    parent_node_id   INTEGER,
    depth            INTEGER NOT NULL,
    snapshot_ref     BLOB    NOT NULL,
    input_log_id     BLOB,
    status           INTEGER NOT NULL,
    score            REAL,
    visit_count      INTEGER NOT NULL DEFAULT 0,
    icount           INTEGER NOT NULL DEFAULT 0,
    virtual_ns       INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    last_visited_at  INTEGER NOT NULL DEFAULT 0,
    attrs            BLOB,
    PRIMARY KEY (experiment_id, node_id),
    FOREIGN KEY (experiment_id, parent_node_id) REFERENCES nodes(experiment_id, node_id)
);

CREATE TABLE input_logs (
    log_id                BLOB    PRIMARY KEY,
    inner_format_version  INTEGER NOT NULL,
    content               BLOB    NOT NULL,
    created_at            INTEGER NOT NULL
);

CREATE TABLE kv_metadata (
    key         BLOB    PRIMARY KEY,
    value       BLOB    NOT NULL,
    generation  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE pins (
    snapshot_ref  BLOB PRIMARY KEY,
    note          TEXT,
    created_at    INTEGER NOT NULL
);

CREATE TABLE tombstones (
    experiment_id  TEXT    NOT NULL,
    node_id        INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (experiment_id, node_id)
);

CREATE INDEX idx_nodes_parent  ON nodes(experiment_id, parent_node_id);
CREATE INDEX idx_nodes_status  ON nodes(experiment_id, status);
CREATE INDEX idx_nodes_created ON nodes(created_at);
CREATE INDEX idx_nodes_updated ON nodes(updated_at);
CREATE INDEX idx_nodes_ref     ON nodes(snapshot_ref);
