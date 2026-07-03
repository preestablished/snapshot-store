-- meta v2 schema step to schema_version = 2
-- Adds the singleton gc_state row persisting M7 GC cycle bookkeeping.

CREATE TABLE gc_state (
    id                   INTEGER PRIMARY KEY CHECK (id = 1),
    cycles_total         INTEGER NOT NULL,
    last_fence_counter   INTEGER NOT NULL,
    last_finished_at     INTEGER NOT NULL,
    last_freed_bytes     INTEGER NOT NULL
);

INSERT INTO gc_state (id, cycles_total, last_fence_counter, last_finished_at, last_freed_bytes)
VALUES (1, 0, 0, 0, 0);
