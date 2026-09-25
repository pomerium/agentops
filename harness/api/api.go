// Package api is the client-neutral Session/Turn/Event API: the surface a
// Slack app, a web app, or a CI bot drives an agent session through, and the
// durable event log that surface reads from.
//
// This package is the whole of what a client compiles against. It holds the
// verb interface, the request/response types, the typed error set and the event
// vocabulary — and it imports nothing but the standard library, so depending on
// it costs a client no Kubernetes, no SQLite and no platform internals. The
// implementation lives behind it and is not importable.
//
// The vocabularies and the error set are in vocab.go, generated from
// proto/vocabulary.yaml — the one place they are written down, so this package,
// the two SDKs and docs/clients.md cannot come to name different sets.
//
// The types are transport-neutral by construction: the same API interface is
// satisfied in-process by the platform's Service and over the wire by a Connect
// client, so a caller written against one runs unchanged against the other.
// Nothing here knows what a Slack thread is; conversation identity arrives as an
// opaque, client-owned ConversationRef.
//
// The division of labour, which everything here leans on:
//
//   - Conversation semantics are the client's. Who may speak in a thread, how a
//     handoff reads, what a transcript carries — none of that is here.
//   - Session and authority semantics are the platform's. The idle clocks, the
//     suspend/revive machinery, the approval ceremony, the seal, and the
//     session→template binding live here and cannot be overridden by a client.
//     In particular the template is recorded on the session at creation and a
//     revive reuses the STORED spec, so a client cannot re-point a consented
//     conversation at different upstreams.
package api

import (
	"context"
	"fmt"
	"time"
)

// Live reports whether a state still owns cluster resources, which is also what
// makes a ConversationRef unavailable to a second session.
func (s SessionState) Live() bool { return liveSessionState(s) }

// Terminal reports whether a session in this state can never run again.
func (s SessionState) Terminal() bool { return !s.Live() }

// --- errors ------------------------------------------------------------------

// Error wraps a sentinel with a human-readable detail. Use Errorf to build one
// and errors.Is to test it.
type Error struct {
	Err    error
	Detail string
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.Err.Error()
	}
	return e.Err.Error() + ": " + e.Detail
}

func (e *Error) Unwrap() error { return e.Err }

// Errorf builds an *Error carrying a sentinel and a formatted detail. It is
// exported because the transport reconstructs errors on the client side: an
// error that crossed the wire has to satisfy the same errors.Is checks as one
// raised in-process.
func Errorf(sentinel error, format string, args ...any) error {
	return &Error{Err: sentinel, Detail: fmt.Sprintf(format, args...)}
}

// --- views -------------------------------------------------------------------

// SessionView is what a client sees of a session. It carries no Slack, no
// Kubernetes and no credential detail by construction.
type SessionView struct {
	ID              string       `json:"id"`
	ConversationRef string       `json:"conversation_ref"`
	State           SessionState `json:"state"`
	Template        string       `json:"template"`
	// LastSeq is the highest event sequence allocated for this session, so a
	// client can start a subscription from a known point.
	LastSeq int64 `json:"last_seq"`
}

// TemplateSummary is one entry of ListTemplates: what a client may run.
type TemplateSummary struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// --- requests ----------------------------------------------------------------

// CreateSessionRequest opens a session.
//
// Note on ApprovalPrompt: the design's verb table omits a prompt, but the
// approval ceremony cannot exist without one — the consent page shows the
// approver what they are authorizing, and the run is created before anyone can
// approve. So the opening ask is part of creation, and Prompt is for every turn
// after it.
type CreateSessionRequest struct {
	// ClientID is the verified caller identity. Over the wire it is never sent:
	// the handler stamps it from the assertion Pomerium verified on the route, so
	// a client cannot state its own. In-process callers fill it in themselves.
	ClientID string `json:"client_id"`
	// Template is the AgentTemplate name to run. It is snapshotted onto the
	// session and every later run of this session uses the snapshot.
	Template string `json:"template"`
	// ConversationRef is an opaque, client-owned string, unique per client while a
	// session is live.
	ConversationRef string `json:"conversation_ref"`
	// ParentSessionID records lineage for a derived session.
	ParentSessionID string `json:"parent_session_id,omitempty"`
	// ApprovalPrompt is what the human approver reads on the consent page. It
	// never carries conversation content the approver did not ask for; a client
	// that seeds an agent from a transcript says so in a bounded clause instead.
	ApprovalPrompt string `json:"approval_prompt,omitempty"`
	// InitialPrompt is the first turn, run as soon as the agent is attached. Empty
	// opens the session idle.
	InitialPrompt string `json:"initial_prompt,omitempty"`
	// SystemPromptAppendix is the client's rendering guidance, appended to the
	// template's own system prompt. How output should be formatted is a property
	// of where it will be displayed, so it belongs to the client — the platform
	// has no opinion about mrkdwn. It is snapshotted with the template, so a
	// revive keeps it.
	SystemPromptAppendix string `json:"system_prompt_appendix,omitempty"`
}

// SessionRef addresses a session by id or by conversation, always within a
// client. Exactly one of SessionID/ConversationRef must be set.
type SessionRef struct {
	ClientID        string `json:"client_id"`
	SessionID       string `json:"session_id,omitempty"`
	ConversationRef string `json:"conversation_ref,omitempty"`
	// IncludeTerminal makes a lookup by ConversationRef return the most recent
	// session the conversation had even if it has ended.
	//
	// It exists because "what ran here before?" is a real question with a real
	// consequence: a client seeding a successor session from an old conversation
	// has to know which template that conversation ran, or it will carry a
	// transcript into an agent wired to upstreams nobody consented to. Without
	// this the client can only see live sessions and has to guess.
	//
	// Verbs that act on a session (Prompt, EndSession) ignore it: they
	// operate on the live one or on nothing.
	IncludeTerminal bool `json:"include_terminal,omitempty"`
}

// PromptRequest sends one turn.
type PromptRequest struct {
	Ref     SessionRef `json:"ref"`
	Content string     `json:"content"`
	// IdempotencyKey makes the prompt safe to retry: a prompt repeating a key the
	// session accepted in the last ten minutes starts nothing and returns the first
	// one's turn id. Only the key is compared, not the content. Optional; without
	// one a prompt must not be retried, because one whose response was lost may
	// already have started its turn.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// PromptResult reports the turn the prompt opened.
type PromptResult struct {
	TurnID string `json:"turn_id"`
}

// RespondPermissionRequest answers an outstanding permission request.
//
// It is idempotent within a short window: the platform remembers how it resolved
// a request for a few minutes, so a client that could not tell a slow round-trip
// from a lost one gets the same answer back rather than an error. That memory
// outlives the live session too — a decision relayed just as the session ended
// is still a decision that was recorded. Past the window, or for a request that
// never existed, the answer is ErrUnknownRequest.
//
// Who inside the client decided is not something the platform can verify — a
// client is trusted to relay its principal's tool-call decisions, and the
// authority ceiling stays with what the run token can reach.
type RespondPermissionRequest struct {
	Ref       SessionRef `json:"ref"`
	RequestID string     `json:"request_id"`
	OptionID  string     `json:"option_id"`
}

// EndSessionRequest ends a session.
type EndSessionRequest struct {
	Ref SessionRef `json:"ref"`
	// Reason is recorded on the session_ended event; empty means EndEnded.
	Reason string `json:"reason,omitempty"`
}

// ListSessionsRequest enumerates a client's sessions. It is always scoped to the
// caller.
type ListSessionsRequest struct {
	ClientID string `json:"client_id"`
	// LiveOnly drops ended and interrupted sessions.
	LiveOnly bool `json:"live_only,omitempty"`
	// UpdatedSince returns only sessions touched at or after this instant.
	//
	// It is what a client's own reconciliation sweep pages on — "which of my
	// sessions moved since I last looked" — and LiveOnly cannot express that: a
	// session that has just gone terminal is precisely the one worth hearing
	// about, and LiveOnly is what hides it. Zero means no lower bound.
	UpdatedSince time.Time `json:"updated_since,omitempty"`
}

// KeepaliveInterval is how often a subscription carries a keepalive while
// nothing else is happening on it.
//
// It is part of the contract rather than each side's private constant: the
// server promises it and the client times out on several of them elapsing, so
// two copies that drift make a client either reconnect for nothing or wait
// forever on a stream that is already dead.
//
// It exists because HTTP/2 PINGs terminate at each hop — a proxy in the path
// answers them on behalf of a harness that is gone — so an application-level
// tick is the only liveness signal that crosses the whole path.
const KeepaliveInterval = 20 * time.Second

// EventsRequest reads a session's event history.
type EventsRequest struct {
	Ref SessionRef `json:"ref"`
	// AfterSeq returns events strictly after this sequence; 0 starts at the
	// beginning.
	AfterSeq int64 `json:"after_seq,omitempty"`
	// Limit caps the page; 0 means the server default.
	Limit int `json:"limit,omitempty"`
}

// SubscribeRequest opens a live event subscription. History before AfterSeq is
// not replayed; history after it is, so a reconnecting client resumes exactly
// where it stopped.
type SubscribeRequest struct {
	Ref      SessionRef `json:"ref"`
	AfterSeq int64      `json:"after_seq,omitempty"`
}

// Subscription is a live event feed for one session. Events arrive in Seq order
// and the channel closes when the session's log is finished (or the caller's
// context ends). Delivery is at-least-once: dedup by Seq.
type Subscription interface {
	// Events yields the feed. It is closed when nothing further will arrive.
	Events() <-chan Event
	// Close releases the subscription. It is idempotent.
	Close()
}

// API is the client-neutral verb surface. P2 wraps this in HTTP+SSE and gRPC;
// P1 calls it directly.
type API interface {
	CreateSession(ctx context.Context, req CreateSessionRequest) (SessionView, error)
	Prompt(ctx context.Context, req PromptRequest) (PromptResult, error)
	RespondPermission(ctx context.Context, req RespondPermissionRequest) error
	EndSession(ctx context.Context, req EndSessionRequest) error
	GetSession(ctx context.Context, ref SessionRef) (SessionView, error)
	ListSessions(ctx context.Context, req ListSessionsRequest) ([]SessionView, error)
	ListTemplates(ctx context.Context, clientID string) ([]TemplateSummary, error)
	ListEvents(ctx context.Context, req EventsRequest) ([]Event, error)
	Subscribe(ctx context.Context, req SubscribeRequest) (Subscription, error)
}
