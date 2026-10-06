package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	migrations "github.com/pomerium/agentops/harness/internal/sessionstore/sqlite/migrations"
	sqlcgen "github.com/pomerium/agentops/harness/internal/sessionstore/sqlite/sqlc"
)

const statusPrefix = "SESSION_STATE_"

func statusText(s api.SessionState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), statusPrefix))
}

func statusOf(text string) api.SessionState {
	return api.SessionState(pb.SessionState_value[statusPrefix+strings.ToUpper(text)])
}

type Store struct {
	db *sql.DB
	q  *sqlcgen.Queries
}

var _ sessionstore.Store = (*Store)(nil)

func Open(ctx context.Context, path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=temp_store(MEMORY)&_pragma=synchronous(NORMAL)", url.PathEscape(path))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db, q: sqlcgen.New(db)}, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrations.FS)
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func toUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func fromUnix(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(n, 0).UTC()
}

func sessionFromRow(r sqlcgen.Session) sessionstore.Session {
	return sessionstore.Session{
		ID:               r.ID,
		ClientID:         r.ClientID,
		ConversationRef:  r.ConversationRef,
		TemplateName:     r.TemplateName,
		TemplateSpec:     r.TemplateSpec,
		InitialPrompt:    r.InitialPrompt,
		SandboxClaimName: r.SandboxClaimName,
		SandboxName:      r.SandboxName,
		ACPSessionID:     r.AcpSessionID,
		Status:           statusOf(r.Status),
		RunID:            r.RunID,
		ApprovalURL:      r.ApprovalUrl,
		RunExpiresAt:     fromUnix(r.RunExpiresAt),
		ApproverSubject:  r.ApproverSubject,
		ParentSessionID:  r.ParentSessionID,
		EventSeq:         r.EventSeq,
		TurnSeq:          r.TurnSeq,
		SuspendedAt:      fromUnix(r.SuspendedAt),
		CreatedAt:        fromUnix(r.CreatedAt),
		UpdatedAt:        fromUnix(r.UpdatedAt),
	}
}

func (s *Store) CreateSession(ctx context.Context, sess sessionstore.Session) error {
	now := time.Now().Unix()
	if sess.Status == pb.SessionState_SESSION_STATE_UNSPECIFIED {
		sess.Status = api.StatePending
	}
	err := s.q.CreateSession(ctx, sqlcgen.CreateSessionParams{
		ID:              sess.ID,
		ClientID:        sess.ClientID,
		ConversationRef: sess.ConversationRef,
		TemplateName:    sess.TemplateName,
		TemplateSpec:    sess.TemplateSpec,
		InitialPrompt:   sess.InitialPrompt,
		Status:          statusText(sess.Status),
		ParentSessionID: sess.ParentSessionID,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	if isUniqueViolation(err) {
		return fmt.Errorf("%w: %v", sessionstore.ErrConflict, err)
	}
	return err
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func oneSession(r sqlcgen.Session, err error) (sessionstore.Session, error) {
	if errors.Is(err, sql.ErrNoRows) {
		return sessionstore.Session{}, sessionstore.ErrNotFound
	}
	if err != nil {
		return sessionstore.Session{}, err
	}
	return sessionFromRow(r), nil
}

func (s *Store) GetSession(ctx context.Context, id string) (sessionstore.Session, error) {
	return oneSession(s.q.GetSession(ctx, id))
}

func (s *Store) GetLiveSessionByConversation(ctx context.Context, clientID, conversationRef string) (sessionstore.Session, error) {
	return oneSession(s.q.GetLiveSessionByConversation(ctx, sqlcgen.GetLiveSessionByConversationParams{
		ClientID:        clientID,
		ConversationRef: conversationRef,
	}))
}

func (s *Store) GetLatestSessionByConversation(ctx context.Context, clientID, conversationRef string) (sessionstore.Session, error) {
	return oneSession(s.q.GetLatestSessionByConversation(ctx, sqlcgen.GetLatestSessionByConversationParams{
		ClientID:        clientID,
		ConversationRef: conversationRef,
	}))
}

func (s *Store) UpdateSessionSandbox(ctx context.Context, id, claimName, sandboxName string, status api.SessionState) error {
	return s.q.UpdateSessionSandbox(ctx, sqlcgen.UpdateSessionSandboxParams{
		SandboxClaimName: claimName,
		SandboxName:      sandboxName,
		Status:           statusText(status),
		UpdatedAt:        time.Now().Unix(),
		ID:               id,
	})
}

func (s *Store) UpdateSessionACP(ctx context.Context, id, acpSessionID string, status api.SessionState) error {
	return s.q.UpdateSessionACP(ctx, sqlcgen.UpdateSessionACPParams{
		AcpSessionID: acpSessionID,
		Status:       statusText(status),
		UpdatedAt:    time.Now().Unix(),
		ID:           id,
	})
}

func (s *Store) UpdateSessionStatus(ctx context.Context, id string, status api.SessionState) error {
	return s.q.UpdateSessionStatus(ctx, sqlcgen.UpdateSessionStatusParams{
		Status:    statusText(status),
		UpdatedAt: time.Now().Unix(),
		ID:        id,
	})
}

func (s *Store) UpdateSessionApprover(ctx context.Context, id, approverSubject string) error {
	return s.q.UpdateSessionApprover(ctx, sqlcgen.UpdateSessionApproverParams{
		ApproverSubject: approverSubject,
		UpdatedAt:       time.Now().Unix(),
		ID:              id,
	})
}

func (s *Store) UpdateSessionSuspended(ctx context.Context, id string, status api.SessionState, suspendedAt time.Time) error {
	return s.q.UpdateSessionSuspended(ctx, sqlcgen.UpdateSessionSuspendedParams{
		Status:      statusText(status),
		SuspendedAt: toUnix(suspendedAt),
		UpdatedAt:   time.Now().Unix(),
		ID:          id,
	})
}

func (s *Store) UpdateSessionRun(ctx context.Context, id, runID, approvalURL string, runExpiresAt time.Time, status api.SessionState) error {
	return s.q.UpdateSessionRun(ctx, sqlcgen.UpdateSessionRunParams{
		RunID:        runID,
		ApprovalUrl:  approvalURL,
		RunExpiresAt: toUnix(runExpiresAt),
		Status:       statusText(status),
		UpdatedAt:    time.Now().Unix(),
		ID:           id,
	})
}

func (s *Store) UpdateSessionRunExpiry(ctx context.Context, id string, runExpiresAt time.Time) error {
	return s.q.UpdateSessionRunExpiry(ctx, sqlcgen.UpdateSessionRunExpiryParams{
		RunExpiresAt: toUnix(runExpiresAt),
		UpdatedAt:    time.Now().Unix(),
		ID:           id,
	})
}

func (s *Store) ListActiveSessions(ctx context.Context) ([]sessionstore.Session, error) {
	return sessionsFromRows(s.q.ListActiveSessions(ctx))
}

func (s *Store) ListSessionsByClient(ctx context.Context, clientID string, liveOnly bool, updatedSince time.Time) ([]sessionstore.Session, error) {
	since := toUnix(updatedSince)
	if liveOnly {
		return sessionsFromRows(s.q.ListLiveSessionsByClient(ctx, sqlcgen.ListLiveSessionsByClientParams{
			ClientID: clientID, UpdatedAt: since,
		}))
	}
	return sessionsFromRows(s.q.ListSessionsByClient(ctx, sqlcgen.ListSessionsByClientParams{
		ClientID: clientID, UpdatedAt: since,
	}))
}

func sessionsFromRows(rows []sqlcgen.Session, err error) ([]sessionstore.Session, error) {
	if err != nil {
		return nil, err
	}
	out := make([]sessionstore.Session, 0, len(rows))
	for _, r := range rows {
		out = append(out, sessionFromRow(r))
	}
	return out, nil
}

func (s *Store) NextTurnSeq(ctx context.Context, sessionID string) (int64, error) {
	n, err := s.q.NextTurnSeq(ctx, sessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, sessionstore.ErrNotFound
	}
	return n, err
}

func (s *Store) AppendSessionEvent(ctx context.Context, sessionID, eventType, turnID string, at time.Time, payload []byte) (int64, error) {
	if payload == nil {
		payload = []byte{}
	}
	var seq int64
	err := s.inTx(ctx, func(q *sqlcgen.Queries) error {
		n, err := q.NextSessionEventSeq(ctx, sessionID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessionstore.ErrNotFound
		}
		if err != nil {
			return err
		}
		seq = n
		return q.InsertSessionEvent(ctx, sqlcgen.InsertSessionEventParams{
			SessionID: sessionID,
			Seq:       n,
			EventType: eventType,
			TurnID:    turnID,
			At:        at.UnixMilli(),
			Payload:   payload,
		})
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *Store) ListSessionEvents(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]sessionstore.SessionEvent, error) {
	if limit <= 0 {
		limit = sessionstore.DefaultEventPage
	}
	rows, err := s.q.ListSessionEvents(ctx, sqlcgen.ListSessionEventsParams{
		SessionID: sessionID,
		Seq:       afterSeq,
		Limit:     int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]sessionstore.SessionEvent, 0, len(rows))
	for _, r := range rows {
		out = append(out, sessionstore.SessionEvent{
			SessionID: r.SessionID,
			Seq:       r.Seq,
			Type:      r.EventType,
			TurnID:    r.TurnID,
			At:        time.UnixMilli(r.At).UTC(),
			Payload:   r.Payload,
		})
	}
	return out, nil
}

func (s *Store) inTx(ctx context.Context, fn func(q *sqlcgen.Queries) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(s.q.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit()
}
