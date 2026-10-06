package sessionstore

import (
	"context"
	"errors"
	"time"

	"github.com/pomerium/agentops/harness/api"
)

const DefaultEventPage = 500

var (
	ErrNotFound = errors.New("store: not found")
	ErrConflict = errors.New("store: conflict")
)

type Session struct {
	ID               string
	ClientID         string
	ConversationRef  string
	TemplateName     string
	TemplateSpec     string
	InitialPrompt    string
	SandboxClaimName string
	SandboxName      string
	ACPSessionID     string
	Status           api.SessionState
	RunID            string
	ApprovalURL      string
	RunExpiresAt     time.Time
	ApproverSubject  string
	ParentSessionID  string
	EventSeq         int64
	TurnSeq          int64
	SuspendedAt      time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type SessionEvent struct {
	SessionID string
	Seq       int64
	Type      string
	TurnID    string
	At        time.Time
	Payload   []byte
}

type Sessions interface {
	CreateSession(ctx context.Context, sess Session) error
	GetSession(ctx context.Context, id string) (Session, error)
	GetLiveSessionByConversation(ctx context.Context, clientID, conversationRef string) (Session, error)
	GetLatestSessionByConversation(ctx context.Context, clientID, conversationRef string) (Session, error)
	ListActiveSessions(ctx context.Context) ([]Session, error)
	ListSessionsByClient(ctx context.Context, clientID string, liveOnly bool, updatedSince time.Time) ([]Session, error)
	UpdateSessionSandbox(ctx context.Context, id, claimName, sandboxName string, status api.SessionState) error
	UpdateSessionACP(ctx context.Context, id, acpSessionID string, status api.SessionState) error
	UpdateSessionStatus(ctx context.Context, id string, status api.SessionState) error
	UpdateSessionRun(ctx context.Context, id, runID, approvalURL string, runExpiresAt time.Time, status api.SessionState) error
	UpdateSessionRunExpiry(ctx context.Context, id string, runExpiresAt time.Time) error
	UpdateSessionApprover(ctx context.Context, id, approverSubject string) error
	UpdateSessionSuspended(ctx context.Context, id string, status api.SessionState, suspendedAt time.Time) error
	NextTurnSeq(ctx context.Context, sessionID string) (int64, error)
}

type Events interface {
	AppendSessionEvent(ctx context.Context, sessionID, eventType, turnID string, at time.Time, payload []byte) (int64, error)
	ListSessionEvents(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]SessionEvent, error)
}

type Store interface {
	Sessions
	Events
	Ping(ctx context.Context) error
	Close() error
}
