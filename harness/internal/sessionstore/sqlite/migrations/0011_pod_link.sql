-- +goose Up
-- +goose StatementBegin

ALTER TABLE sessions ADD COLUMN executor TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN stream_id TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN pod_seq INTEGER NOT NULL DEFAULT 0;

CREATE TABLE pod_commands (
    session_id TEXT NOT NULL,
    kind       TEXT NOT NULL,
    key        TEXT NOT NULL,
    turn_id    TEXT NOT NULL DEFAULT '',
    payload    BLOB NOT NULL DEFAULT x'',
    PRIMARY KEY (session_id, kind, key)
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE pod_commands;
ALTER TABLE sessions DROP COLUMN pod_seq;
ALTER TABLE sessions DROP COLUMN stream_id;
ALTER TABLE sessions DROP COLUMN executor;
-- +goose StatementEnd
