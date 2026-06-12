-- name: UpsertMCPCredential :exec
INSERT INTO mcp_credentials (
    slack_user_id, server_name, server_url,
    access_token, refresh_token, token_type, expiry,
    auth_url, token_url, client_id, client_secret, scopes, registration_json,
    created_at, updated_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(slack_user_id, server_name) DO UPDATE SET
    server_url        = excluded.server_url,
    access_token      = excluded.access_token,
    refresh_token     = excluded.refresh_token,
    token_type        = excluded.token_type,
    expiry            = excluded.expiry,
    auth_url          = excluded.auth_url,
    token_url         = excluded.token_url,
    client_id         = excluded.client_id,
    client_secret     = excluded.client_secret,
    scopes            = excluded.scopes,
    registration_json = excluded.registration_json,
    updated_at        = excluded.updated_at;

-- name: GetMCPCredential :one
SELECT * FROM mcp_credentials
WHERE slack_user_id = ? AND server_name = ?;

-- name: ListMCPCredentialsByUser :many
SELECT * FROM mcp_credentials
WHERE slack_user_id = ?
ORDER BY server_name;

-- name: DeleteMCPCredential :exec
DELETE FROM mcp_credentials
WHERE slack_user_id = ? AND server_name = ?;

-- name: CreateOAuthFlow :exec
INSERT INTO oauth_flows (
    flow_id, state, slack_user_id, slack_team_id,
    slack_channel_id, slack_thread_ts,
    server_name, server_url, workflow_name,
    auth_url, token_url, registration_endpoint,
    client_id, client_secret, scopes,
    pkce_verifier, redirect_uri,
    created_at, expires_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
);

-- name: GetOAuthFlowByState :one
SELECT * FROM oauth_flows
WHERE state = ?;

-- name: GetOAuthFlow :one
SELECT * FROM oauth_flows
WHERE flow_id = ?;

-- name: DeleteOAuthFlow :exec
DELETE FROM oauth_flows
WHERE flow_id = ?;

-- name: DeleteExpiredOAuthFlows :exec
DELETE FROM oauth_flows
WHERE expires_at < ?;

-- name: CreateSession :exec
INSERT INTO sessions (
    id, slack_channel_id, slack_thread_ts, slack_user_id,
    workflow_name, initial_prompt, status, slack_response_url,
    created_at, updated_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
);

-- name: GetSession :one
SELECT * FROM sessions
WHERE id = ?;

-- name: GetSessionByThread :one
SELECT * FROM sessions
WHERE slack_channel_id = ? AND slack_thread_ts = ?;

-- name: UpdateSessionSandbox :exec
UPDATE sessions
SET sandbox_claim_name = ?, sandbox_name = ?, status = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateSessionACP :exec
UPDATE sessions
SET acp_session_id = ?, status = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateSessionStatus :exec
UPDATE sessions
SET status = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateSessionResponseURL :exec
UPDATE sessions
SET slack_response_url = ?, updated_at = ?
WHERE id = ?;

-- name: ListActiveSessions :many
SELECT * FROM sessions
WHERE status NOT IN ('ended', 'failed')
ORDER BY created_at;

-- name: DeleteSession :exec
DELETE FROM sessions
WHERE id = ?;
