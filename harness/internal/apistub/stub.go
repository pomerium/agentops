package apistub

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/api/server"
	"github.com/pomerium/agentops/harness/internal/apiserver"
)

const SentinelPrefix = "sentinel/"

const (
	PromptPermission = "stub:permission"

	PromptUnknownEvent = "stub:unknown-event"

	PromptZeroEvent = "stub:zero-event"
)

const StubApproverSubject = "stub|approver"

const maxEventPage = 500

var clockEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type Stub struct {
	mu       sync.Mutex
	sessions map[string]*session
	nextID   int
	clock    time.Time
	tmpls    []*pb.TemplateSummary
}

func New(templates ...*pb.TemplateSummary) *Stub {
	if len(templates) == 0 {
		templates = []*pb.TemplateSummary{
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

var _ harnessapipbconnect.HarnessAPIServiceHandler = (*Stub)(nil)

type session struct {
	view *pb.SessionView

	clientID  string
	createdAt time.Time
	updatedAt time.Time
	events    []*pb.Event
	subs      map[*subscription]struct{}

	finished bool

	pending string
	offered []*pb.PermissionOption

	turn string

	turns int

	keyed map[string]string
}

func (s *Stub) CreateSession(ctx context.Context, req *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	if err := scriptedError(req.GetTemplate()); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetApprovalPrompt()) == "" {
		return nil, api.Errorf(api.ErrInvalidArgument, "an approval prompt is required")
	}
	if strings.TrimSpace(req.GetConversationRef()) == "" {
		return nil, api.Errorf(api.ErrInvalidArgument, "a conversation ref is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sess := range s.sessions {
		if sess.clientID != clientID {
			continue
		}
		if sess.view.GetConversationRef() == req.GetConversationRef() && api.Live(sess.view.GetState()) {
			return nil, api.Errorf(api.ErrConflict,
				"conversation %q already has a live session", req.GetConversationRef())
		}
	}

	s.nextID++
	id := fmt.Sprintf("stub-%03d", s.nextID)
	now := s.tick()
	sess := &session{
		view: &pb.SessionView{
			Id:              id,
			ConversationRef: req.GetConversationRef(),
			State:           api.StatePending,
			Template:        req.GetTemplate(),
		},
		clientID:  clientID,
		createdAt: now,
		updatedAt: now,
		subs:      map[*subscription]struct{}{},
	}
	s.sessions[id] = sess

	created := proto.CloneOf(sess.view)

	s.transition(sess, api.StateLaunching, api.ReasonLaunch)
	s.transition(sess, api.StateAwaitingApproval, api.ReasonLaunch)
	s.emit(sess, &pb.Event{Payload: &pb.Event_ApprovalRequired{ApprovalRequired: &pb.ApprovalRequired{
		ApprovalUrl: "https://stub.invalid/approve/" + id,
		ExpiresAt:   timestamppb.New(s.peek().Add(10 * time.Minute)),
	}}})
	s.emit(sess, &pb.Event{Payload: &pb.Event_Approved{Approved: &pb.Approved{ApproverSubject: StubApproverSubject}}})
	s.transition(sess, api.StateRunning, api.ReasonLaunch)

	if req.GetInitialPrompt() != "" {
		s.runTurn(sess, req.GetInitialPrompt())
	}
	return &pb.CreateSessionResponse{Session: created}, nil
}

func (s *Stub) Prompt(ctx context.Context, req *pb.PromptRequest) (*pb.PromptResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.findLive(clientID, req.GetRef())
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetContent()) == "" {
		return nil, api.Errorf(api.ErrInvalidArgument, "a prompt needs content")
	}

	if turn, ok := sess.keyed[req.GetIdempotencyKey()]; ok {
		return &pb.PromptResponse{TurnId: turn}, nil
	}

	if sess.view.GetState() != api.StateRunning {
		return nil, api.Errorf(api.ErrInvalidState,
			"a prompt does not apply to a %s session", sess.view.GetState())
	}

	turn := s.runTurn(sess, req.GetContent())
	if key := req.GetIdempotencyKey(); key != "" {
		if sess.keyed == nil {
			sess.keyed = map[string]string{}
		}
		sess.keyed[key] = turn
	}
	return &pb.PromptResponse{TurnId: turn}, nil
}

func (s *Stub) RespondPermission(ctx context.Context, req *pb.RespondPermissionRequest) (*pb.RespondPermissionResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.findLive(clientID, req.GetRef())
	if err != nil {
		return nil, err
	}
	requestID := req.GetRequestId()
	if requestID == "" {
		return nil, api.Errorf(api.ErrInvalidArgument, "a request id is required")
	}
	if sess.pending != requestID {
		return nil, api.Errorf(api.ErrUnknownRequest, "no outstanding request %q", requestID)
	}
	if !slices.ContainsFunc(sess.offered, func(o *pb.PermissionOption) bool { return o.GetId() == req.GetOptionId() }) {
		return nil, api.Errorf(api.ErrInvalidArgument, "option %q was not offered", req.GetOptionId())
	}
	turn := sess.turn
	sess.pending = ""
	sess.offered = nil
	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_PermissionResolved{PermissionResolved: &pb.PermissionResolved{
		RequestId: requestID, Resolution: &pb.PermissionResolved_OptionId{OptionId: req.GetOptionId()},
	}}})

	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_ToolCall{ToolCall: &pb.ToolCall{
		Id: "call-1", Title: "deploy", Status: api.ToolCallCompleted, Update: true,
	}}})
	s.finishTurn(sess, turn)
	return &pb.RespondPermissionResponse{}, nil
}

func (s *Stub) EndSession(ctx context.Context, req *pb.EndSessionRequest) (*pb.EndSessionResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.findLive(clientID, req.GetRef())
	if err != nil {
		return nil, err
	}
	if api.Terminal(sess.view.GetState()) {
		return &pb.EndSessionResponse{}, nil
	}
	reason := req.GetReason()
	if reason == pb.EndReason_END_REASON_UNSPECIFIED {
		reason = api.EndEnded
	}

	s.transition(sess, api.StateEnded, pb.Reason_REASON_UNSPECIFIED)
	s.emit(sess, &pb.Event{Payload: &pb.Event_SessionEnded{SessionEnded: &pb.SessionEnded{Reason: reason}}})
	s.finish(sess)
	return &pb.EndSessionResponse{}, nil
}

func (s *Stub) GetSession(ctx context.Context, req *pb.GetSessionRequest) (*pb.GetSessionResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(clientID, req.GetRef())
	if err != nil {
		return nil, err
	}
	return &pb.GetSessionResponse{Session: proto.CloneOf(sess.view)}, nil
}

func (s *Stub) ListSessions(ctx context.Context, req *pb.ListSessionsRequest) (*pb.ListSessionsResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	since := api.Time(req.GetUpdatedSince())

	s.mu.Lock()
	defer s.mu.Unlock()
	out := &pb.ListSessionsResponse{Sessions: []*pb.SessionView{}}
	for _, sess := range s.sessions {
		if sess.clientID != clientID {
			continue
		}
		if req.GetLiveOnly() && !api.Live(sess.view.GetState()) {
			continue
		}
		if !since.IsZero() && sess.updatedAt.Before(since) {
			continue
		}
		out.Sessions = append(out.Sessions, proto.CloneOf(sess.view))
	}

	slices.SortFunc(out.Sessions, func(a, b *pb.SessionView) int { return strings.Compare(a.GetId(), b.GetId()) })
	return out, nil
}

func (s *Stub) ListTemplates(ctx context.Context, _ *pb.ListTemplatesRequest) (*pb.ListTemplatesResponse, error) {
	if _, err := apiserver.ClientID(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.ListTemplatesResponse{Templates: append([]*pb.TemplateSummary(nil), s.tmpls...)}, nil
}

func (s *Stub) ListEvents(ctx context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(clientID, req.GetRef())
	if err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > maxEventPage {
		limit = maxEventPage
	}
	out := make([]*pb.Event, 0, min(limit, len(sess.events)))
	for _, ev := range sess.events {
		if ev.GetSeq() <= req.GetAfterSeq() {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, ev)
	}
	return &pb.ListEventsResponse{Events: out}, nil
}

func (s *Stub) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream *connect.ServerStream[pb.SubscribeResponse]) error {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return err
	}
	sub, err := s.subscribe(clientID, req)
	if err != nil {
		return err
	}
	defer sub.Close()
	return server.Stream(ctx, stream, sub.Events())
}

func (s *Stub) subscribe(clientID string, req *pb.SubscribeRequest) (*subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.find(clientID, req.GetRef())
	if err != nil {
		return nil, err
	}

	var backlog []*pb.Event
	for _, ev := range sess.events {
		if ev.GetSeq() > req.GetAfterSeq() {
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

type subscription struct {
	stub *Stub
	sess *session
	out  chan *pb.Event

	mu    sync.Mutex
	queue []*pb.Event
	ended bool
	wake  chan struct{}
	done  chan struct{}
	stop  sync.Once
}

func newSubscription(stub *Stub, sess *session) *subscription {
	sub := &subscription{
		stub: stub,
		sess: sess,
		out:  make(chan *pb.Event),
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	go sub.pump()
	return sub
}

func (s *subscription) Events() <-chan *pb.Event { return s.out }

func (s *subscription) Close() {
	s.stub.mu.Lock()
	delete(s.sess.subs, s)
	s.stub.mu.Unlock()
	s.stop.Do(func() { close(s.done) })
}

func (s *subscription) push(ev *pb.Event) {
	s.mu.Lock()
	s.queue = append(s.queue, ev)
	s.mu.Unlock()
	s.poke()
}

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

func (s *Stub) runTurn(sess *session, content string) string {
	turn := fmt.Sprintf("%s-t%d", sess.view.GetId(), sess.turns+1)

	switch content {
	case PromptPermission:

		s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_ToolCall{ToolCall: &pb.ToolCall{
			Id: "call-1", Title: "deploy", Kind: "execute", Status: api.ToolCallPending,
			InvocationMessage: "deploy to production",
			ToolInput: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
				"target": structpb.NewStringValue("production"),
			}}),
		}}})
		sess.pending = "req-1"
		sess.turn = turn
		sess.offered = []*pb.PermissionOption{
			{Id: "allow", Name: "Allow", Kind: "allow_once"},
			{Id: "deny", Name: "Deny", Kind: "reject_once"},
		}
		s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_PermissionRequest{PermissionRequest: &pb.PermissionRequest{
			RequestId:  "req-1",
			Summary:    "Deploy to production?",
			Options:    sess.offered,
			Deadline:   timestamppb.New(s.peek().Add(5 * time.Minute)),
			ToolCallId: "call-1",
		}}})
		return turn

	case PromptUnknownEvent:

		ev := &pb.Event{TurnId: turn}
		ev.ProtoReflect().SetUnknown(futurePayload)
		s.emit(sess, ev)

	case PromptZeroEvent:

		s.broadcast(sess, &pb.Event{})
	}

	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_AgentMessage{AgentMessage: &pb.AgentMessage{
		PartId: turn + "-p1", Text: "working on it", Final: false,
	}}})
	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_ToolCall{ToolCall: &pb.ToolCall{
		Id: "call-1", Title: "read the file", Kind: "read", Status: api.ToolCallPending,
	}}})
	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_ToolCall{ToolCall: &pb.ToolCall{
		Id: "call-1", Title: "read the file", Kind: "read", Status: api.ToolCallCompleted, Update: true,
	}}})
	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_AgentMessage{AgentMessage: &pb.AgentMessage{
		PartId: turn + "-p2", Text: "done: " + content, Final: true,
	}}})
	s.finishTurn(sess, turn)
	return turn
}

var futurePayload = protowire.AppendBytes(
	protowire.AppendTag(nil, unknownFieldNumber, protowire.BytesType),
	protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), "nobody here has ever seen"),
)

func (s *Stub) finishTurn(sess *session, turn string) {
	sess.turns++
	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_Usage{Usage: &pb.Usage{
		InputTokens: 120, OutputTokens: 40, TotalTokens: 160, CostUsd: 0.0012,
	}}})
	s.emit(sess, &pb.Event{TurnId: turn, Payload: &pb.Event_TurnCompleted{TurnCompleted: &pb.TurnCompleted{StopReason: "end_turn"}}})
}

func (s *Stub) emit(sess *session, ev *pb.Event) {
	at := s.tick()
	ev.SessionId = sess.view.GetId()
	ev.Seq = sess.view.GetLastSeq() + 1
	ev.Timestamp = timestamppb.New(at)
	sess.view.LastSeq = ev.Seq
	sess.updatedAt = at
	sess.events = append(sess.events, ev)
	s.broadcast(sess, ev)
}

func (s *Stub) broadcast(sess *session, ev *pb.Event) {
	for sub := range sess.subs {
		sub.push(ev)
	}
}

func (s *Stub) transition(sess *session, next api.SessionState, reason api.Reason) {
	prev := sess.view.GetState()
	sess.view.State = next
	s.emit(sess, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{Old: prev, New: next, Reason: reason}}})
}

func (s *Stub) finish(sess *session) {
	sess.finished = true
}

func (s *Stub) tick() time.Time {
	now := s.clock
	s.clock = s.clock.Add(time.Second)
	return now
}

func (s *Stub) peek() time.Time { return s.clock }

func (s *Stub) find(clientID string, ref *pb.SessionRef) (*session, error) {
	return s.resolve(clientID, ref, ref.GetIncludeTerminal())
}

func (s *Stub) findLive(clientID string, ref *pb.SessionRef) (*session, error) {
	return s.resolve(clientID, ref, false)
}

func (s *Stub) resolve(clientID string, ref *pb.SessionRef, includeTerminal bool) (*session, error) {
	sessionID, conversationRef := ref.GetSessionId(), ref.GetConversationRef()
	if err := scriptedError(sessionID); err != nil {
		return nil, err
	}
	switch {
	case sessionID == "" && conversationRef == "":
		return nil, api.Errorf(api.ErrInvalidArgument, "a session id or a conversation ref is required")
	case sessionID != "" && conversationRef != "":
		return nil, api.Errorf(api.ErrInvalidArgument, "exactly one of session id / conversation ref")
	}

	if sessionID != "" {
		sess, ok := s.sessions[sessionID]
		if !ok || sess.clientID != clientID {
			return nil, api.Errorf(api.ErrNotFound, "no session %q", sessionID)
		}
		return sess, nil
	}

	var best *session
	for _, sess := range s.sessions {
		if sess.clientID != clientID || sess.view.GetConversationRef() != conversationRef {
			continue
		}
		if !includeTerminal && api.Terminal(sess.view.GetState()) {
			continue
		}
		if best == nil || sess.createdAt.After(best.createdAt) {
			best = sess
		}
	}
	if best == nil {
		return nil, api.Errorf(api.ErrNotFound, "no session for conversation %q", conversationRef)
	}
	return best, nil
}

func scriptedError(value string) error {
	name, ok := strings.CutPrefix(value, SentinelPrefix)
	if !ok {
		return nil
	}
	entry, ok := api.Sentinels()[pb.Sentinel(pb.Sentinel_value[name])]
	if !ok {
		return api.Errorf(api.ErrInvalidArgument, "%q names no published sentinel", name)
	}
	return api.Errorf(entry.Err, "scripted by the conformance stub")
}
