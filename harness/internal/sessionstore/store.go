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

	Executor string
	StreamID string
	PodSeq   int64
}

type PodCommand struct {
	SessionID string
	Kind      string
	Key       string
	TurnID    string
	Payload   []byte
}

type SessionEvent struct {
	SessionID string
	Seq       int64
	Type      string
	TurnID    string
	At        time.Time
	Payload   []byte
}

type NewSessionEvent struct {
	Type    string
	TurnID  string
	At      time.Time
	Payload []byte
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
	UpdateSessionLink(ctx context.Context, id, executor, streamID string, podSeq int64) error
	AdvancePodSeq(ctx context.Context, id string, podSeq int64) error
	NextTurnSeq(ctx context.Context, sessionID string) (int64, error)
}

type Events interface {
	AppendSessionEvent(ctx context.Context, sessionID, eventType, turnID string, at time.Time, payload []byte) (int64, error)
	AppendPodEvent(ctx context.Context, sessionID, eventType, turnID string, at time.Time, payload []byte, podSeq int64) (int64, error)
	AppendPodTurnEnd(ctx context.Context, sessionID, eventType, turnID string, at time.Time, payload []byte, podSeq int64) (int64, error)
	ListSessionEvents(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]SessionEvent, error)
	FinishSession(ctx context.Context, sessionID string, status api.SessionState, events []NewSessionEvent) ([]int64, error)
}

type PodCommands interface {
	PutPodCommand(ctx context.Context, cmd PodCommand) error
	ListPodCommands(ctx context.Context, sessionID string) ([]PodCommand, error)
	DeletePodCommand(ctx context.Context, sessionID, kind, key string) error
	DeletePodCommandsForTurn(ctx context.Context, sessionID, turnID string) error
	DeletePodCommands(ctx context.Context, sessionID string) error
}

type Store interface {
	Sessions
	Events
	PodCommands
	Ping(ctx context.Context) error
	Close() error
}
