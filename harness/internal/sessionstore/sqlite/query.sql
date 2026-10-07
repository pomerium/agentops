-- name: CreateSession :exec
INSERT INTO sessions (
    id, client_id, conversation_ref,
    template_name, template_spec, initial_prompt, status,
    parent_session_id,
    created_at, updated_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
);

-- name: GetSession :one
SELECT * FROM sessions
WHERE id = ?;

-- name: GetLiveSessionByConversation :one
SELECT * FROM sessions
WHERE client_id = ? AND conversation_ref = ?
  AND status NOT IN ('ended', 'interrupted')
ORDER BY created_at DESC
LIMIT 1;

-- name: GetLatestSessionByConversation :one
SELECT * FROM sessions
WHERE client_id = ? AND conversation_ref = ?
ORDER BY created_at DESC
LIMIT 1;

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

-- name: FinishSession :execrows
UPDATE sessions
SET status = ?, updated_at = ?
WHERE id = ? AND status NOT IN ('ended', 'interrupted');

-- name: UpdateSessionRun :exec
UPDATE sessions
SET run_id = ?, approval_url = ?, run_expires_at = ?, status = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateSessionRunExpiry :exec
UPDATE sessions
SET run_expires_at = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateSessionApprover :exec
UPDATE sessions
SET approver_subject = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateSessionSuspended :exec
UPDATE sessions
SET status = ?, suspended_at = ?, updated_at = ?
WHERE id = ?;

-- name: ListActiveSessions :many
SELECT * FROM sessions
WHERE status NOT IN ('ended', 'interrupted')
ORDER BY created_at;

-- name: ListSessionsByClient :many
SELECT * FROM sessions
WHERE client_id = ? AND updated_at >= ?
ORDER BY created_at DESC;

-- name: ListLiveSessionsByClient :many
SELECT * FROM sessions
WHERE client_id = ? AND status NOT IN ('ended', 'interrupted') AND updated_at >= ?
ORDER BY created_at DESC;

-- name: NextSessionEventSeq :one
UPDATE sessions
SET event_seq = event_seq + 1
WHERE id = ?
RETURNING event_seq;

-- name: NextTurnSeq :one
UPDATE sessions
SET turn_seq = turn_seq + 1
WHERE id = ?
RETURNING turn_seq;

-- name: InsertSessionEvent :exec
INSERT INTO session_events (session_id, seq, event_type, turn_id, at, payload)
VALUES (?, ?, ?, ?, ?, ?);

-- name: ListSessionEvents :many
SELECT * FROM session_events
WHERE session_id = ? AND seq > ?
ORDER BY seq
LIMIT ?;

