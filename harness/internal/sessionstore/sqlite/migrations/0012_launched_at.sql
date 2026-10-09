-- +goose Up
-- +goose StatementBegin

ALTER TABLE sessions ADD COLUMN launched_at INTEGER NOT NULL DEFAULT 0;
UPDATE sessions SET launched_at = created_at;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE sessions DROP COLUMN launched_at;
-- +goose StatementEnd
