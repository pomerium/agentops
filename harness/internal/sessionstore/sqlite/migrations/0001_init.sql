-- +goose Up
-- +goose StatementBegin

CREATE TABLE sessions (
    id                 TEXT    NOT NULL PRIMARY KEY,
    client_id          TEXT    NOT NULL,
    conversation_ref   TEXT    NOT NULL,
    template_name      TEXT    NOT NULL DEFAULT '',
    template_spec      TEXT    NOT NULL DEFAULT '',
    initial_prompt     TEXT    NOT NULL DEFAULT '',
    sandbox_claim_name TEXT    NOT NULL DEFAULT '',
    sandbox_name       TEXT    NOT NULL DEFAULT '',
    acp_session_id     TEXT    NOT NULL DEFAULT '',
    status             TEXT    NOT NULL DEFAULT 'pending',
    run_id             TEXT    NOT NULL DEFAULT '',
    approval_url       TEXT    NOT NULL DEFAULT '',
    run_expires_at     INTEGER NOT NULL DEFAULT 0,
    approver_subject   TEXT    NOT NULL DEFAULT '',
    parent_session_id  TEXT    NOT NULL DEFAULT '',
    event_seq          INTEGER NOT NULL DEFAULT 0,
    turn_seq           INTEGER NOT NULL DEFAULT 0,
    suspended_at       INTEGER NOT NULL DEFAULT 0,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_sessions_live_conversation ON sessions (client_id, conversation_ref)
    WHERE status NOT IN ('ended', 'interrupted');
CREATE INDEX idx_sessions_conversation ON sessions (client_id, conversation_ref, created_at);
CREATE INDEX idx_sessions_client_updated ON sessions (client_id, updated_at);
CREATE INDEX idx_sessions_status ON sessions (status);

CREATE TABLE session_events (
    session_id TEXT    NOT NULL,
    seq        INTEGER NOT NULL,
    event_type TEXT    NOT NULL,
    turn_id    TEXT    NOT NULL DEFAULT '',
    at         INTEGER NOT NULL,
    payload    BLOB    NOT NULL DEFAULT x'',
    PRIMARY KEY (session_id, seq)
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE session_events;
DROP TABLE sessions;
-- +goose StatementEnd
