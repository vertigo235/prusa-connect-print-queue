CREATE TABLE queue_items (
    id           TEXT PRIMARY KEY,
    device_id    TEXT NOT NULL,
    storage       TEXT NOT NULL,
    source        TEXT NOT NULL,
    source_size   INTEGER NOT NULL,
    source_mtime  INTEGER NOT NULL,
    source_sha256 TEXT NOT NULL DEFAULT '',
    destination   TEXT NOT NULL,
    display_name TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    position     INTEGER NOT NULL,
    state        TEXT NOT NULL,
    transfer_id  TEXT NOT NULL DEFAULT '',
    command_id   TEXT NOT NULL DEFAULT '',
    job_id       TEXT NOT NULL DEFAULT '',
    error        TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMP NOT NULL,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL
);

CREATE INDEX queue_items_device_order ON queue_items (device_id, state, position, created_at, id);
CREATE INDEX queue_items_transfer ON queue_items (transfer_id);
CREATE INDEX queue_items_command ON queue_items (command_id);
CREATE INDEX queue_items_job ON queue_items (job_id);
