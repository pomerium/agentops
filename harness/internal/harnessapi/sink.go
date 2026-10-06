package harnessapi

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/sandbox"
)

type logSink struct {
	svc         *Service
	sessionID   string
	permTimeout time.Duration

	mu         sync.Mutex
	turnID     string
	part       int
	buf        strings.Builder
	thoughtBuf strings.Builder
	toolTitle  map[string]string
	waiters    map[string]chan permissionAnswer
}

type permissionAnswer struct {
	decision sandbox.PermissionDecision
	resolved *pb.PermissionResolved
}

var _ sandbox.EventSink = (*logSink)(nil)

func newLogSink(svc *Service, sessionID string, permTimeout time.Duration) *logSink {
	return &logSink{
		svc:         svc,
		sessionID:   sessionID,
		permTimeout: permTimeout,
		toolTitle:   map[string]string{},
		waiters:     map[string]chan permissionAnswer{},
	}
}

func (s *logSink) beginTurn(turnID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turnID = turnID
	s.part = 0
	s.buf.Reset()
	s.thoughtBuf.Reset()
}

func (s *logSink) endTurn(ctx context.Context) {
	s.flushThought(ctx)
	s.mu.Lock()
	seg := s.buf.String()
	s.buf.Reset()
	turnID, partID := s.turnID, s.nextPartLocked()
	s.mu.Unlock()
	if seg == "" {
		return
	}
	s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_AgentMessage{AgentMessage: &pb.AgentMessage{
		PartId: partID, Text: seg, Final: true,
	}}})
}

func (s *logSink) nextPartLocked() string {
	s.part++
	turn := s.turnID
	if turn == "" {
		turn = "t0"
	}
	return turn + "." + strconv.Itoa(s.part)
}

func (s *logSink) AgentMessage(ctx context.Context, text string) {
	if text == "" {
		return
	}
	s.flushThought(ctx)
	s.mu.Lock()
	s.buf.WriteString(text)
	s.mu.Unlock()
}

func (s *logSink) AgentThought(_ context.Context, text string) {
	if text == "" {
		return
	}
	s.mu.Lock()
	s.thoughtBuf.WriteString(text)
	s.mu.Unlock()
}

func (s *logSink) flushThought(ctx context.Context) {
	s.mu.Lock()
	if s.thoughtBuf.Len() == 0 {
		s.mu.Unlock()
		return
	}
	text := s.thoughtBuf.String()
	s.thoughtBuf.Reset()
	turnID := s.turnID
	s.mu.Unlock()
	s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_AgentThought{AgentThought: &pb.AgentThought{Text: text}}})
}

func (s *logSink) ToolCall(ctx context.Context, ev sandbox.ToolCallEvent) {
	s.flushThought(ctx)

	s.mu.Lock()
	title := ev.Title
	if title == "" {
		title = s.toolTitle[ev.ID]
	}
	if title != "" {
		s.toolTitle[ev.ID] = title
	}
	seg := s.buf.String()
	s.buf.Reset()
	turnID := s.turnID
	var partID string
	if seg != "" {
		partID = s.nextPartLocked()
	}
	s.mu.Unlock()

	if seg != "" {
		s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_AgentMessage{AgentMessage: &pb.AgentMessage{
			PartId: partID, Text: seg, Final: false,
		}}})
	}
	s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_ToolCall{ToolCall: &pb.ToolCall{
		Id:        ev.ID,
		Title:     title,
		Kind:      ev.Kind,
		Status:    api.NormalizeToolCallStatus(ev.Status),
		ToolInput: toolInput(ev.RawInput),
		Update:    ev.Update,
	}}})
}

func toolInput(v any) *structpb.Value {
	if v == nil {
		return nil
	}
	out, err := structpb.NewValue(v)
	if err != nil {
		return nil
	}
	return out
}

func (s *logSink) Usage(ctx context.Context, ev sandbox.UsageEvent) {
	s.mu.Lock()
	turnID := s.turnID
	s.mu.Unlock()
	s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_Usage{Usage: &pb.Usage{
		InputTokens:         ev.InputTokens,
		OutputTokens:        ev.OutputTokens,
		CachedInputTokens:   ev.CachedReadTokens,
		CacheCreationTokens: ev.CachedWriteTokens,
		ThoughtTokens:       ev.ThoughtTokens,
		TotalTokens:         ev.TotalTokens,
		CostUsd:             ev.CostUSD,
		ContextWindow:       ev.ContextWindow,
		ContextUsed:         ev.ContextUsed,
	}}})
}

func (s *logSink) Permission(ctx context.Context, req sandbox.PermissionRequest) (sandbox.PermissionDecision, error) {
	requestID := req.ToolCallID
	ch := make(chan permissionAnswer, 1)

	s.mu.Lock()
	s.waiters[requestID] = ch
	turnID := s.turnID
	title := req.Title
	if title == "" {
		title = s.toolTitle[req.ToolCallID]
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiters, requestID)
		s.mu.Unlock()
	}()

	options := make([]*pb.PermissionOption, 0, len(req.Options))
	for _, o := range req.Options {
		options = append(options, &pb.PermissionOption{Id: o.ID, Name: o.Name, Kind: o.Kind})
	}
	deadline := time.Now().Add(s.permTimeout)
	s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_PermissionRequest{PermissionRequest: &pb.PermissionRequest{
		RequestId:  requestID,
		Summary:    title,
		Options:    options,
		Deadline:   timestamppb.New(deadline),
		ToolCallId: req.ToolCallID,
	}}})
	resolved := func(r *pb.PermissionResolved) {
		r.RequestId = requestID
		s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_PermissionResolved{PermissionResolved: r}})
	}

	timer := time.NewTimer(s.permTimeout)
	defer timer.Stop()
	select {
	case a := <-ch:
		resolved(a.resolved)
		return a.decision, nil
	case <-timer.C:
		resolved(unanswered(api.ResolutionExpired))
		return sandbox.PermissionDecision{Cancelled: true}, nil
	case <-ctx.Done():
		resolved(unanswered(api.ResolutionSuperseded))
		return sandbox.PermissionDecision{Cancelled: true}, ctx.Err()
	}
}

func unanswered(why api.Resolution) *pb.PermissionResolved {
	return &pb.PermissionResolved{Resolution: &pb.PermissionResolved_Unanswered{Unanswered: why}}
}

func (s *logSink) resolvePermission(requestID string, d sandbox.PermissionDecision) bool {
	s.mu.Lock()
	ch := s.waiters[requestID]
	s.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- permissionAnswer{decision: d, resolved: &pb.PermissionResolved{
		Resolution: &pb.PermissionResolved_OptionId{OptionId: d.OptionID},
	}}:
	default:
	}
	return true
}

func (s *logSink) supersedeAll() {
	s.mu.Lock()
	waiters := make([]chan permissionAnswer, 0, len(s.waiters))
	for _, ch := range s.waiters {
		waiters = append(waiters, ch)
	}
	s.mu.Unlock()
	for _, ch := range waiters {
		select {
		case ch <- permissionAnswer{
			decision: sandbox.PermissionDecision{Cancelled: true},
			resolved: unanswered(api.ResolutionSuperseded),
		}:
		default:
		}
	}
}
