CREATE TABLE schema_version (
    version INTEGER NOT NULL
);
INSERT INTO schema_version VALUES (1);

CREATE TABLE snapshots (
    id          INTEGER PRIMARY KEY,
    ref         BLOB    NOT NULL UNIQUE,
    parent_id   INTEGER REFERENCES snapshots(id),
    icount      INTEGER NOT NULL,
    virtual_ns  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    label       TEXT,
    page_count  INTEGER NOT NULL,
    new_pages   INTEGER NOT NULL
);

CREATE INDEX idx_snapshots_parent ON snapshots(parent_id);
CREATE UNIQUE INDEX idx_snapshots_label ON snapshots(label) WHERE label IS NOT NULL;
