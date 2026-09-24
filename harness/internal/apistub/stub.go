// Package apistub is a conformance server for the Harness API: the real
// apiserver, on the real Connect transport, in front of a scripted in-memory
// api.API.
//
// It exists so an SDK in any language can be tested against the wire the
// harness actually speaks with no cluster, no Pomerium and no human approval
// behind it. Identity arrives in a plain header instead of a verified
// assertion, and that substitution is the only thing here that is not
// production's: the codec, the envelope framing, the error details, the opening
// keepalive and the streaming lifecycle are all the real implementation's, so
// an SDK that passes against this one is talking to the same server.
//
// It also answers for conditions no real server can be asked to produce on
// demand — a stream that dies mid-flight, a response carrying a field this
// build has never heard of, a session log that ends before it says anything.
// Those are what a client's hardest paths exist for, and they are otherwise
// only reachable by breaking something. See scenario.go.
//
// Nothing here reaches production: Dockerfile.harness builds ./cmd/harness
// alone, and this package lives under internal/.
package apistub

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/api/wire"
)

// SentinelPrefix addresses a scripted error rather than a session: a ref whose
// session_id is "sentinel/ErrNotRevivable", or a CreateSession whose template
// is, fails with that published sentinel from whichever verb was called.
//
// It is a ref value rather than a control endpoint so that a conformance test
// can provoke all ten errors with nothing but the SDK under test — an SDK that
// needed a side channel to be tested would be tested through code no user runs.
const SentinelPrefix = "sentinel/"

// The scripted turns, addressed by prompt content. A conformance suite needs a
// turn to unfold a particular way — with a permission request in the middle, or
// carrying an event type the SDK has never heard of — and asking for it by
// content keeps the whole scenario expressible through the SDK's own verbs.
const (
	// PromptPermission runs a turn that stops on a permission request and waits
	// for RespondPermission before finishing.
	PromptPermission = "stub:permission"
	// PromptUnknownEvent runs a turn that emits an event type and payload from a
	// future version of the platform, then finishes normally. A client must
	// deliver the unknown event and carry on.
	PromptUnknownEvent = "stub:unknown-event"
	// PromptZeroEvent runs a turn that emits an entirely zero-valued event —
	// which on the JSON path arrives as the object {}, since proto3 omits zero
	// fields — then finishes normally. Absent must read as the proto3 default,
	// and a seq of 0 must not break dedup.
	PromptZeroEvent = "stub:zero-event"
)

// StubApproverSubject is the approver every scripted session records. There is
// no IdP here; the ceremony is played out on the log so a client can be tested
// against the events it really has to render.
const StubApproverSubject = "stub|approver"

// maxEventPage is the page ListEvents clamps to, mirroring the service.
const maxEventPage = 500

// clockEpoch is where a stub's clock starts. Fixed, and advanced one second per
// event, so a conformance suite can assert on timestamps: a test that has to
// treat every time as "some time" cannot catch a timestamp that stopped being
// sent at all.
var clockEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Stub is a scripted in-memory api.API. It is safe for concurrent use, which it
// has to be: a conformance suite drives verbs on one connection while reading
// the event stream on another, and that interleaving is half of what it tests.
type Stub struct {
	mu       sync.Mutex
	sessions map[string]*session
	nextID   int
	clock    time.Time
	tmpls    []api.TemplateSummary
}

// New builds a stub with the given templates. An empty list gets a default
// pair, because ListTemplates returning nothing is a poor thing to write a
// conformance test against.
func New(templates ...api.TemplateSummary) *Stub {
	if len(templates) == 0 {
		templates = []api.TemplateSummary{
			{Name: "runid", Description: "the scripted template"},
			{Name: "demo", Description: "a second template, so a list has two entries"},
		}
	}
	return &Stub{
		sessions: map[string]*session{},
		clock:    clockEpoch,
		tmpls:    templates,
	}
}

var _ api.API = (*Stub)(nil)

// session is one scripted session: its view, its log, and whoever is listening.
type session struct {
	view   api.SessionView
	events []api.Event
	subs   map[*subscription]struct{}
	// finished marks a log that will never grow again, which is what closes a
	// subscriber's channel and what makes a later Subscribe succeed with nothing
	// to deliver rather than hang.
	finished bool
	// pending is the permission request a scripted turn is blocked on, if any.
	pending string
	// turn is the turn a blocked permission request belongs to.
	turn string
	// idempotencyKey is what CreateSession recognizes a retry by.
	idempotencyKey string
	// turns counts completed turns, for naming the next one.
	turns int
}

// --- verbs -------------------------------------------------------------------

func (s *Stub) CreateSession(_ context.Context, req api.CreateSessionRequest) (api.SessionView, error) {
	if err := scriptedError(req.Template); err != nil {
		return api.SessionView{}, err
	}
	if strings.TrimSpace(req.ApprovalPrompt) == "" {
		return api.SessionView{}, api.Errorf(api.ErrInvalidArgument, "an approval prompt is required")
	}
	if strings.TrimSpace(req.ConversationRef) == "" {
		return api.SessionView{}, api.Errorf(api.ErrInvalidArgument, "a conversation ref is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// An idempotency key returns the session it already made rather than a second
	// one, and a live conversation refuses a second session outright. Both are the
	// service's semantics and both are things a client's retry path depends on.
	for _, sess := range s.sessions {
		if sess.view.ClientID != req.ClientID {
			continue
		}
		// The conversation has to match too, not just the key. A key identifies a
		// retry of one call, and a client that reused one for a different
		// conversation would otherwise be handed a session for the wrong thread —
		// silently, which is the worst way to find out.
		if req.IdempotencyKey != "" && sess.idempotencyKey == req.IdempotencyKey &&
			sess.view.ConversationRef == req.ConversationRef {
			return sess.view, nil
		}
		if sess.view.ConversationRef == req.ConversationRef && sess.view.State.Live() {
			return api.SessionView{}, api.Errorf(api.ErrConflict,
				"conversation %q already has a live session", req.ConversationRef)
		}
	}

	s.nextID++
	id := fmt.Sprintf("stub-%03d", s.nextID)
	now := s.tick()
	sess := &session{
		view: api.SessionView{
			ID:              id,
			ClientID:        req.ClientID,
			ConversationRef: req.ConversationRef,
			State:           api.StatePending,
			Template:        req.Template,
			ParentSessionID: req.ParentSessionID,
			Principal:       req.Principal,
			CreatedAt:       now,
			UpdatedAt:       now,
		},
		subs:           map[*subscription]struct{}{},
		idempotencyKey: req.IdempotencyKey,
	}
	s.sessions[id] = sess

	// What the caller gets back is the session as it stands right now: pending,
	// and with no approval URL. The URL arrives on the log a moment later, and
	// this is the single most commonly mis-assumed thing about the API — a client
	// that reads it off the CreateSession response reads an empty string.
	created := sess.view

	// The launch, played out. A real one takes a human; here the approval lands
	// immediately so a suite can get to a turn without one.
	s.transition(sess, api.StateLaunching, api.ReasonLaunch)
	s.transition(sess, api.StateAwaitingApproval, "")
	sess.view.ApprovalURL = "https://stub.invalid/approve/" + id
	s.emit(sess, api.EventApprovalRequired, "", api.ApprovalRequired{
		ApprovalURL: sess.view.ApprovalURL,
		ExpiresAt:   s.peek().Add(10 * time.Minute),
	})
	sess.view.ApproverSubject = StubApproverSubject
	s.emit(sess, api.EventApproved, "", api.Approved{ApproverSubject: StubApproverSubject})
	s.transition(sess, api.StateRunning, "")

	if req.InitialPrompt != "" {
		s.runTurn(sess, req.InitialPrompt)
	}
	return created, nil
}

func (s *Stub) Prompt(_ context.Context, req api.PromptRequest) (api.PromptResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(req.Ref)
	if err != nil {
		return api.PromptResult{}, err
	}
	if strings.TrimSpace(req.Content) == "" {
		return api.PromptResult{}, api.Errorf(api.ErrInvalidArgument, "a prompt needs content")
	}

	revived := false
	switch sess.view.State {
	case api.StateRunning:
	case api.StateSuspended:
		// Revive is a Prompt, not a verb of its own: same workspace, new pod, new
		// run, and a fresh approval the client has to deliver again.
		revived = true
		s.transition(sess, api.StateRunning, api.ReasonRevive)
		s.emit(sess, api.EventRevived, "", api.Revived{})
		sess.view.ApprovalURL = "https://stub.invalid/approve/" + sess.view.ID
		s.emit(sess, api.EventApprovalRequired, "", api.ApprovalRequired{
			ApprovalURL: sess.view.ApprovalURL,
			ExpiresAt:   s.peek().Add(10 * time.Minute),
		})
	default:
		return api.PromptResult{}, api.Errorf(api.ErrInvalidState,
			"a prompt does not apply to a %s session", sess.view.State)
	}

	turn := s.runTurn(sess, req.Content)
	return api.PromptResult{TurnID: turn, Revived: revived}, nil
}

func (s *Stub) CancelTurn(_ context.Context, req api.CancelTurnRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(req.Ref)
	if err != nil {
		return err
	}
	turn := req.TurnID
	if turn == "" {
		turn = sess.turn
	}
	// A cancelled turn takes any outstanding permission request with it, which is
	// what stops a client leaving buttons live forever.
	if sess.pending != "" {
		s.emit(sess, api.EventPermissionResolved, sess.turn, api.PermissionResolved{
			RequestID: sess.pending, Resolution: api.ResolutionSuperseded,
		})
		sess.pending = ""
	}
	s.emit(sess, api.EventTurnCompleted, turn, api.TurnCompleted{StopReason: "cancelled"})
	return nil
}

func (s *Stub) RespondPermission(_ context.Context, req api.RespondPermissionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(req.Ref)
	if err != nil {
		return err
	}
	if req.RequestID == "" {
		return api.Errorf(api.ErrInvalidArgument, "a request id is required")
	}
	if sess.pending != req.RequestID {
		return api.Errorf(api.ErrUnknownRequest, "no outstanding request %q", req.RequestID)
	}
	turn := sess.turn
	sess.pending = ""
	s.emit(sess, api.EventPermissionResolved, turn, api.PermissionResolved{
		RequestID: req.RequestID, Resolution: req.OptionID,
	})
	// The turn the request blocked now finishes, so a suite sees a permission
	// answered and a turn completed rather than a session that stops mid-turn.
	s.emit(sess, api.EventToolCall, turn, api.ToolCall{
		ID: "call-1", Title: "deploy", Status: api.ToolCallCompleted, Update: true,
	})
	s.finishTurn(sess, turn)
	return nil
}

func (s *Stub) Suspend(_ context.Context, ref api.SessionRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(ref)
	if err != nil {
		return err
	}
	if sess.view.State != api.StateRunning {
		return api.Errorf(api.ErrInvalidState, "a %s session cannot be suspended", sess.view.State)
	}
	s.transition(sess, api.StateSuspended, api.ReasonClient)
	sess.view.SuspendedAt = s.peek()
	s.emit(sess, api.EventSuspended, "", api.Suspended{
		Reason: api.ReasonClient, RetainedFor: time.Hour,
	})
	return nil
}

func (s *Stub) EndSession(_ context.Context, req api.EndSessionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(req.Ref)
	if err != nil {
		return err
	}
	if sess.view.State.Terminal() {
		return api.Errorf(api.ErrInvalidState, "this session has already ended")
	}
	reason := req.Reason
	if reason == "" {
		reason = api.EndEnded
	}
	s.transition(sess, api.StateEnded, reason)
	s.emit(sess, api.EventSessionEnded, "", api.SessionEnded{Reason: reason})
	s.finish(sess)
	return nil
}

func (s *Stub) DeleteWorkspace(_ context.Context, ref api.SessionRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(ref)
	if err != nil {
		return err
	}
	s.finish(sess)
	delete(s.sessions, sess.view.ID)
	return nil
}

func (s *Stub) GetSession(_ context.Context, ref api.SessionRef) (api.SessionView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(ref)
	if err != nil {
		return api.SessionView{}, err
	}
	return sess.view, nil
}

func (s *Stub) ListSessions(_ context.Context, req api.ListSessionsRequest) ([]api.SessionView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []api.SessionView
	for _, sess := range s.sessions {
		if sess.view.ClientID != req.ClientID {
			continue
		}
		if req.LiveOnly && !sess.view.State.Live() {
			continue
		}
		if !req.UpdatedSince.IsZero() && sess.view.UpdatedAt.Before(req.UpdatedSince) {
			continue
		}
		out = append(out, sess.view)
	}
	// Ordered by id, which is creation order: a map's range order would make an
	// otherwise-correct SDK test flake.
	slices.SortFunc(out, func(a, b api.SessionView) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (s *Stub) ListTemplates(_ context.Context, _ string) ([]api.TemplateSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]api.TemplateSummary(nil), s.tmpls...), nil
}

func (s *Stub) ReissueApproval(_ context.Context, ref api.SessionRef) (api.Approval, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(ref)
	if err != nil {
		return api.Approval{}, err
	}
	if sess.view.State.Terminal() {
		return api.Approval{}, api.Errorf(api.ErrInvalidState, "this session has ended")
	}
	url := "https://stub.invalid/approve/" + sess.view.ID + "?reissued=1"
	expires := s.peek().Add(10 * time.Minute)
	sess.view.ApprovalURL = url
	s.emit(sess, api.EventApprovalRequired, "", api.ApprovalRequired{
		ApprovalURL: url, ExpiresAt: expires, Reissued: true,
	})
	return api.Approval{ApprovalURL: url, ExpiresAt: expires}, nil
}

func (s *Stub) ListEvents(_ context.Context, req api.EventsRequest) ([]api.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(req.Ref)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 || limit > maxEventPage {
		limit = maxEventPage
	}
	out := make([]api.Event, 0, min(limit, len(sess.events)))
	for _, ev := range sess.events {
		if ev.Seq <= req.AfterSeq {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, ev)
	}
	return out, nil
}

func (s *Stub) SetSessionMetadata(_ context.Context, req api.SetSessionMetadataRequest) error {
	if len(req.Metadata) > api.MaxMetadataBytes {
		return api.Errorf(api.ErrInvalidArgument,
			"metadata is %d bytes, over the %d-byte cap", len(req.Metadata), api.MaxMetadataBytes)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(req.Ref)
	if err != nil {
		return err
	}
	sess.view.Metadata = req.Metadata
	sess.view.UpdatedAt = s.tick()
	return nil
}

func (s *Stub) Subscribe(_ context.Context, req api.SubscribeRequest) (api.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(req.Ref)
	if err != nil {
		return nil, err
	}

	var backlog []api.Event
	for _, ev := range sess.events {
		if ev.Seq > req.AfterSeq {
			backlog = append(backlog, ev)
		}
	}
	// Room for the replay plus whatever a test appends afterwards. A stub feeds a
	// test, not a production log, so a buffer is honest here where a blocking
	// broadcast would deadlock a suite that reads its stream after driving a turn.
	sub := &subscription{
		stub: s,
		sess: sess,
		out:  make(chan api.Event, len(backlog)+256),
	}
	for _, ev := range backlog {
		sub.out <- ev
	}
	if sess.finished {
		close(sub.out)
		sub.closed = true
		return sub, nil
	}
	sess.subs[sub] = struct{}{}
	return sub, nil
}

// subscription is one live feed. Closing it unregisters it; only a finished log
// closes the channel, because the handler ranges over it and a consumer-side
// close would race the broadcast.
type subscription struct {
	stub   *Stub
	sess   *session
	out    chan api.Event
	closed bool
}

func (s *subscription) Events() <-chan api.Event { return s.out }

func (s *subscription) Close() {
	s.stub.mu.Lock()
	defer s.stub.mu.Unlock()
	delete(s.sess.subs, s)
}

// --- the scripted turn -------------------------------------------------------

// runTurn plays a turn out on the log. Called with the lock held.
func (s *Stub) runTurn(sess *session, content string) string {
	turn := fmt.Sprintf("%s-t%d", sess.view.ID, sess.turns+1)

	switch content {
	case PromptPermission:
		// The turn stops here. RespondPermission resumes it, which is what lets a
		// suite test the round trip rather than just the rendering.
		s.emit(sess, api.EventToolCall, turn, api.ToolCall{
			ID: "call-1", Title: "deploy", Kind: "execute", Status: api.ToolCallPending,
			InvocationMessage: "deploy to production",
			ToolInput:         json.RawMessage(`{"target":"production"}`),
		})
		sess.pending = "req-1"
		sess.turn = turn
		s.emit(sess, api.EventPermissionRequest, turn, api.PermissionRequest{
			RequestID: "req-1",
			Summary:   "Deploy to production?",
			Options: []api.PermissionOption{
				{ID: "allow", Name: "Allow", Kind: "allow_once"},
				{ID: "deny", Name: "Deny", Kind: "reject_once"},
			},
			Deadline:   s.peek().Add(5 * time.Minute),
			ToolCallID: "call-1",
		})
		return turn

	case PromptUnknownEvent:
		// An event from a later version of the platform. Types are additive and a
		// client must pass one it does not know through rather than fail on it.
		s.emitRaw(sess, api.Event{
			Type:    "future_event_type",
			TurnID:  turn,
			Payload: json.RawMessage(`{"shape":"nobody here has ever seen","count":7}`),
		})

	case PromptZeroEvent:
		// Every field zero. On the JSON path proto3 omits them all, so this arrives
		// as {"event":{}} — no seq, no type, no timestamp — and absent has to read
		// as the default rather than as missing data. Its seq of 0 also means a
		// correct client drops it as already-seen, which is the right behaviour and
		// not a crash.
		s.broadcast(sess, api.Event{})
	}

	s.emit(sess, api.EventAgentMessage, turn, api.AgentMessage{
		PartID: turn + "-p1", Text: "working on it", Final: false,
	})
	s.emit(sess, api.EventToolCall, turn, api.ToolCall{
		ID: "call-1", Title: "read the file", Kind: "read", Status: api.ToolCallPending,
	})
	s.emit(sess, api.EventToolCall, turn, api.ToolCall{
		ID: "call-1", Title: "read the file", Kind: "read", Status: api.ToolCallCompleted, Update: true,
	})
	s.emit(sess, api.EventAgentMessage, turn, api.AgentMessage{
		PartID: turn + "-p2", Text: "done: " + content, Final: true,
	})
	s.finishTurn(sess, turn)
	return turn
}

// finishTurn closes a turn out with its counters and its stop reason.
func (s *Stub) finishTurn(sess *session, turn string) {
	sess.turns++
	s.emit(sess, api.EventUsage, turn, api.Usage{
		InputTokens: 120, OutputTokens: 40, TotalTokens: 160, CostUSD: 0.0012,
	})
	s.emit(sess, api.EventTurnCompleted, turn, api.TurnCompleted{StopReason: "end_turn"})
}

// --- log mechanics -----------------------------------------------------------

// emit appends a typed event to the log and hands it to every subscriber.
// Called with the lock held.
func (s *Stub) emit(sess *session, typ api.EventType, turnID string, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		// A payload this package cannot marshal is a bug in this package, and a
		// silently empty payload would send a conformance suite hunting in the SDK.
		panic(fmt.Sprintf("apistub: marshal %s payload: %v", typ, err))
	}
	s.emitRaw(sess, api.Event{Type: typ, TurnID: turnID, Payload: raw})
}

// emitRaw appends an event, allocating its sequence and stamping its time.
func (s *Stub) emitRaw(sess *session, ev api.Event) {
	ev.SessionID = sess.view.ID
	ev.Seq = sess.view.LastSeq + 1
	ev.At = s.tick()
	sess.view.LastSeq = ev.Seq
	sess.view.UpdatedAt = ev.At
	sess.events = append(sess.events, ev)
	s.broadcast(sess, ev)
}

// broadcast delivers an event to the live subscribers without recording it. The
// zero-event scenario is the only caller that wants that split: an event with
// seq 0 must not disturb the log's sequence or its history.
func (s *Stub) broadcast(sess *session, ev api.Event) {
	for sub := range sess.subs {
		select {
		case sub.out <- ev:
		default:
			// A subscriber this far behind is a test that stopped reading. Dropping
			// would break the ordering contract the SDKs are being tested against, so
			// say so loudly rather than quietly deliver a log with a hole in it.
			panic("apistub: a subscriber's buffer is full; the test is not reading its stream")
		}
	}
}

// transition moves a session's state and records the move, in that order, so
// the event's "old" is what the session actually left rather than whatever the
// last caller happened to leave on the view.
func (s *Stub) transition(sess *session, next api.SessionState, reason string) {
	prev := sess.view.State
	sess.view.State = next
	s.emit(sess, api.EventStateChanged, "", api.StateChanged{Old: prev, New: next, Reason: reason})
}

// finish marks a log as never growing again and closes every feed on it, which
// is what a client sees as a clean end of stream.
func (s *Stub) finish(sess *session) {
	if sess.finished {
		return
	}
	sess.finished = true
	for sub := range sess.subs {
		if !sub.closed {
			close(sub.out)
			sub.closed = true
		}
		delete(sess.subs, sub)
	}
}

// tick returns the current instant and advances the clock. Called with the lock
// held.
func (s *Stub) tick() time.Time {
	now := s.clock
	s.clock = s.clock.Add(time.Second)
	return now
}

// peek returns the current instant without advancing it, for the expiry times
// that describe a moment rather than mark one.
func (s *Stub) peek() time.Time { return s.clock }

// find resolves a ref within its client. Called with the lock held.
//
// Another client's session is not found rather than forbidden, which is the
// service's behaviour and worth a conformance test of its own: a client that
// could tell "exists but not yours" from "does not exist" could enumerate
// somebody else's conversations.
func (s *Stub) find(ref api.SessionRef) (*session, error) {
	if err := scriptedError(ref.SessionID); err != nil {
		return nil, err
	}
	switch {
	case ref.SessionID == "" && ref.ConversationRef == "":
		return nil, api.Errorf(api.ErrInvalidArgument, "a session id or a conversation ref is required")
	case ref.SessionID != "" && ref.ConversationRef != "":
		return nil, api.Errorf(api.ErrInvalidArgument, "exactly one of session id / conversation ref")
	}

	if ref.SessionID != "" {
		sess, ok := s.sessions[ref.SessionID]
		if !ok || sess.view.ClientID != ref.ClientID {
			return nil, api.Errorf(api.ErrNotFound, "no session %q", ref.SessionID)
		}
		return sess, nil
	}

	var best *session
	for _, sess := range s.sessions {
		if sess.view.ClientID != ref.ClientID || sess.view.ConversationRef != ref.ConversationRef {
			continue
		}
		if !ref.IncludeTerminal && sess.view.State.Terminal() {
			continue
		}
		if best == nil || sess.view.CreatedAt.After(best.view.CreatedAt) {
			best = sess
		}
	}
	if best == nil {
		return nil, api.Errorf(api.ErrNotFound, "no session for conversation %q", ref.ConversationRef)
	}
	return best, nil
}

// scriptedError reads a sentinel ref and returns the published error it names.
func scriptedError(value string) error {
	name, ok := strings.CutPrefix(value, SentinelPrefix)
	if !ok {
		return nil
	}
	entry, ok := wire.Sentinels()[name]
	if !ok {
		return api.Errorf(api.ErrInvalidArgument, "%q names no published sentinel", name)
	}
	return api.Errorf(entry.Err, "scripted by the conformance stub")
}
