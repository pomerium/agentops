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
	view api.SessionView
	// clientID, createdAt and updatedAt are what the stub scopes, orders and
	// filters by. The view does not carry them: no client reads them back.
	clientID  string
	createdAt time.Time
	updatedAt time.Time
	events    []api.Event
	subs      map[*subscription]struct{}
	// finished marks a log that will never grow again. A later Subscribe gets the
	// backlog and then a clean end rather than a feed that hangs; a feed already
	// open is deliberately left open (see finish).
	finished bool
	// pending is the permission request a scripted turn is blocked on, if any.
	pending string
	// turn is the turn a blocked permission request belongs to.
	turn string
	// turns counts completed turns, for naming the next one.
	turns int
	// keyed maps each idempotency key a prompt was accepted with to the turn it
	// started. The platform forgets a key after ten minutes; the stub keeps it for
	// its lifetime, which a suite never outlasts.
	keyed map[string]string
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

	// A live conversation refuses a second session outright, as the service does.
	for _, sess := range s.sessions {
		if sess.clientID != req.ClientID {
			continue
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
			ConversationRef: req.ConversationRef,
			State:           api.StatePending,
			Template:        req.Template,
		},
		clientID:  req.ClientID,
		createdAt: now,
		updatedAt: now,
		subs:      map[*subscription]struct{}{},
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
	s.emit(sess, api.EventApprovalRequired, "", api.ApprovalRequired{
		ApprovalURL: "https://stub.invalid/approve/" + id,
		ExpiresAt:   s.peek().Add(10 * time.Minute),
	})
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
	sess, err := s.findLive(req.Ref)
	if err != nil {
		return api.PromptResult{}, err
	}
	if strings.TrimSpace(req.Content) == "" {
		return api.PromptResult{}, api.Errorf(api.ErrInvalidArgument, "a prompt needs content")
	}
	// A repeated key is answered before the state is checked, as on the platform:
	// the retry of an accepted prompt gets its turn back whatever that turn has
	// since done to the session.
	if turn, ok := sess.keyed[req.IdempotencyKey]; ok {
		return api.PromptResult{TurnID: turn}, nil
	}

	if sess.view.State != api.StateRunning {
		return api.PromptResult{}, api.Errorf(api.ErrInvalidState,
			"a prompt does not apply to a %s session", sess.view.State)
	}

	turn := s.runTurn(sess, req.Content)
	if req.IdempotencyKey != "" {
		if sess.keyed == nil {
			sess.keyed = map[string]string{}
		}
		sess.keyed[req.IdempotencyKey] = turn
	}
	return api.PromptResult{TurnID: turn}, nil
}

func (s *Stub) RespondPermission(_ context.Context, req api.RespondPermissionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.findLive(req.Ref)
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

func (s *Stub) EndSession(_ context.Context, req api.EndSessionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.findLive(req.Ref)
	if err != nil {
		return err
	}
	if sess.view.State.Terminal() {
		return nil // idempotent, as on the platform: ending an ended session is done
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
		if sess.clientID != req.ClientID {
			continue
		}
		if req.LiveOnly && !sess.view.State.Live() {
			continue
		}
		if !req.UpdatedSince.IsZero() && sess.updatedAt.Before(req.UpdatedSince) {
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
	sub := newSubscription(s, sess)
	for _, ev := range backlog {
		sub.push(ev)
	}
	if sess.finished {
		sub.end()
		return sub, nil
	}
	sess.subs[sub] = struct{}{}
	return sub, nil
}

// subscription is one live feed. Events are queued without a bound and handed
// on by one goroutine per feed, so a broadcast never blocks, never drops and
// never fails however far behind a test's reader falls: a stub serves a test, and
// a test that pauses its reader must not change what the log says.
//
// Only the pump closes out, and only once the feed has been ended and drained,
// because the handler ranges over it. Close stops the pump without closing out.
type subscription struct {
	stub *Stub
	sess *session
	out  chan api.Event

	mu    sync.Mutex
	queue []api.Event
	ended bool
	wake  chan struct{}
	done  chan struct{}
	stop  sync.Once
}

func newSubscription(stub *Stub, sess *session) *subscription {
	sub := &subscription{
		stub: stub,
		sess: sess,
		out:  make(chan api.Event),
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	go sub.pump()
	return sub
}

func (s *subscription) Events() <-chan api.Event { return s.out }

func (s *subscription) Close() {
	s.stub.mu.Lock()
	delete(s.sess.subs, s)
	s.stub.mu.Unlock()
	s.stop.Do(func() { close(s.done) })
}

// push queues an event for the feed.
func (s *subscription) push(ev api.Event) {
	s.mu.Lock()
	s.queue = append(s.queue, ev)
	s.mu.Unlock()
	s.poke()
}

// end closes the feed once everything queued has been delivered.
func (s *subscription) end() {
	s.mu.Lock()
	s.ended = true
	s.mu.Unlock()
	s.poke()
}

func (s *subscription) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *subscription) pump() {
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			ended := s.ended
			s.mu.Unlock()
			if ended {
				close(s.out)
				return
			}
			select {
			case <-s.wake:
			case <-s.done:
				return
			}
			continue
		}
		ev := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()
		select {
		case s.out <- ev:
		case <-s.done:
			return
		}
	}
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
	sess.updatedAt = ev.At
	sess.events = append(sess.events, ev)
	s.broadcast(sess, ev)
}

// broadcast delivers an event to the live subscribers without recording it. The
// zero-event scenario is the only caller that wants that split: an event with
// seq 0 must not disturb the log's sequence or its history.
func (s *Stub) broadcast(sess *session, ev api.Event) {
	for sub := range sess.subs {
		sub.push(ev)
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

// finish marks a log as never growing again. A feed opened after this gets the
// backlog and then a clean end of stream; a feed already open is left open.
//
// That is the one place the stub is stricter than the platform, on purpose: a
// client must stop on the session_ended event, and one that instead waits for the
// stream to end would pass against a server that closes it straight away. Here it
// hangs, which is what makes the mistake visible.
func (s *Stub) finish(sess *session) {
	sess.finished = true
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
		if !ok || sess.clientID != ref.ClientID {
			return nil, api.Errorf(api.ErrNotFound, "no session %q", ref.SessionID)
		}
		return sess, nil
	}

	var best *session
	for _, sess := range s.sessions {
		if sess.clientID != ref.ClientID || sess.view.ConversationRef != ref.ConversationRef {
			continue
		}
		if !ref.IncludeTerminal && sess.view.State.Terminal() {
			continue
		}
		if best == nil || sess.createdAt.After(best.createdAt) {
			best = sess
		}
	}
	if best == nil {
		return nil, api.Errorf(api.ErrNotFound, "no session for conversation %q", ref.ConversationRef)
	}
	return best, nil
}

// findLive is find for a verb that acts on a session. Such a verb operates on the
// live session or on nothing, as SessionRef says, so IncludeTerminal — which is
// for reading history — is ignored rather than letting it act on an ended one.
func (s *Stub) findLive(ref api.SessionRef) (*session, error) {
	ref.IncludeTerminal = false
	return s.find(ref)
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
