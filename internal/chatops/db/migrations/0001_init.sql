-- +goose Up
-- +goose StatementBegin

-- mcp_credentials holds the per-(slack_user, server) OAuth credentials acquired
-- by the broker, including the dynamic client registration needed to refresh
-- tokens across process restarts.
CREATE TABLE mcp_credentials (
    slack_user_id     TEXT    NOT NULL,
    server_name       TEXT    NOT NULL,
    server_url        TEXT    NOT NULL,
    access_token      TEXT    NOT NULL,
    refresh_token     TEXT    NOT NULL DEFAULT '',
    token_type        TEXT    NOT NULL DEFAULT 'Bearer',
    expiry            INTEGER NOT NULL DEFAULT 0, -- unix seconds; 0 means no expiry
    auth_url          TEXT    NOT NULL DEFAULT '',
    token_url         TEXT    NOT NULL DEFAULT '',
    client_id         TEXT    NOT NULL DEFAULT '',
    client_secret     TEXT    NOT NULL DEFAULT '',
    scopes            TEXT    NOT NULL DEFAULT '',
    registration_json TEXT    NOT NULL DEFAULT '',
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    PRIMARY KEY (slack_user_id, server_name)
);

-- oauth_flows holds in-flight authorization-code+PKCE flow state. A row is
-- created when an auth link is posted and deleted once the callback completes.
CREATE TABLE oauth_flows (
    flow_id               TEXT    NOT NULL PRIMARY KEY,
    state                 TEXT    NOT NULL UNIQUE,
    slack_user_id         TEXT    NOT NULL,
    slack_team_id         TEXT    NOT NULL DEFAULT '',
    slack_channel_id      TEXT    NOT NULL,
    slack_thread_ts       TEXT    NOT NULL,
    server_name           TEXT    NOT NULL,
    server_url            TEXT    NOT NULL,
    workflow_name         TEXT    NOT NULL DEFAULT '',
    auth_url              TEXT    NOT NULL,
    token_url             TEXT    NOT NULL,
    registration_endpoint TEXT    NOT NULL DEFAULT '',
    client_id             TEXT    NOT NULL,
    client_secret         TEXT    NOT NULL DEFAULT '',
    scopes                TEXT    NOT NULL DEFAULT '',
    pkce_verifier         TEXT    NOT NULL,
    redirect_uri          TEXT    NOT NULL,
    created_at            INTEGER NOT NULL,
    expires_at            INTEGER NOT NULL
);

-- sessions binds an active Slack thread to a sandbox/ACP session.
CREATE TABLE sessions (
    id                 TEXT    NOT NULL PRIMARY KEY,
    slack_channel_id   TEXT    NOT NULL,
    slack_thread_ts    TEXT    NOT NULL,
    slack_user_id      TEXT    NOT NULL,
    workflow_name      TEXT    NOT NULL DEFAULT '',
    sandbox_claim_name TEXT    NOT NULL DEFAULT '',
    sandbox_name       TEXT    NOT NULL DEFAULT '',
    acp_session_id     TEXT    NOT NULL DEFAULT '',
    status             TEXT    NOT NULL DEFAULT 'pending',
    -- slack_response_url stores the slash command's response_url so the OAuth
    -- callback can edit the original ephemeral connect prompt in place.
    slack_response_url TEXT    NOT NULL DEFAULT '',
    -- initial_prompt holds the text typed alongside the @mention; it is replayed
    -- as the session's first turn once the sandbox is live.
    initial_prompt     TEXT    NOT NULL DEFAULT '',
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_sessions_thread ON sessions (slack_channel_id, slack_thread_ts);
CREATE INDEX idx_sessions_status ON sessions (status);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE sessions;
DROP TABLE oauth_flows;
DROP TABLE mcp_credentials;
-- +goose StatementEnd
