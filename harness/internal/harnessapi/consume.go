package harnessapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v7"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

const (
	commandPrompt     = "prompt"
	commandPermission = "permission"
)

const (
	podRetryInitial = 50 * time.Millisecond
	podRetryMax     = 5 * time.Second
)

func (s *Service) consume(b *binding) {
	defer close(b.consumed)
	ctx := telemetry.With(context.Background(), "session_id", b.sessionID)
	for {
		select {
		case <-b.done:
			return
		case <-b.session.Done():
			return
		case f := <-b.session.Inbox():
			switch {
			case f.GetState() != nil:
				s.reconcileState(ctx, b, f.GetState())
			case f.GetEvent() != nil:
				if !s.handleEvent(ctx, b, f.GetEvent()) {
					return
				}
			}
		}
	}
}

func (s *Service) handleEvent(ctx context.Context, b *binding, ev *agentlinkpb.AgentEvent) bool {
	turnID, seq := ev.GetTurnId(), ev.GetSeq()
	var out *pb.Event
	var permission *pb.PermissionRequest
	switch p := ev.GetPayload().(type) {
	case *agentlinkpb.AgentEvent_Message:
		out = &pb.Event{Payload: &pb.Event_AgentMessage{AgentMessage: &pb.AgentMessage{
			PartId: p.Message.GetPartId(), Text: p.Message.GetText(), Final: p.Message.GetFinal(),
		}}}
	case *agentlinkpb.AgentEvent_Thought:
		out = &pb.Event{Payload: &pb.Event_AgentThought{AgentThought: &pb.AgentThought{Text: p.Thought.GetText()}}}
	case *agentlinkpb.AgentEvent_ToolCall:
		c := p.ToolCall
		out = &pb.Event{Payload: &pb.Event_ToolCall{ToolCall: &pb.ToolCall{
			Id:        c.GetId(),
			Title:     c.GetTitle(),
			Kind:      c.GetKind(),
			Status:    api.NormalizeToolCallStatus(c.GetStatus()),
			ToolInput: toolInput(c.GetRawInput()),
			Update:    c.GetUpdate(),
		}}}
	case *agentlinkpb.AgentEvent_Usage:
		u := p.Usage
		out = &pb.Event{Payload: &pb.Event_Usage{Usage: &pb.Usage{
			InputTokens:         u.GetInputTokens(),
			OutputTokens:        u.GetOutputTokens(),
			CachedInputTokens:   u.GetCachedReadTokens(),
			CacheCreationTokens: u.GetCachedWriteTokens(),
			ThoughtTokens:       u.GetThoughtTokens(),
			TotalTokens:         u.GetTotalTokens(),
			CostUsd:             u.GetCostUsd(),
			ContextWindow:       u.GetContextWindow(),
			ContextUsed:         u.GetContextUsed(),
		}}}
	case *agentlinkpb.AgentEvent_PermissionRequest:
		req := p.PermissionRequest
		options := make([]*pb.PermissionOption, 0, len(req.GetOptions()))
		for _, o := range req.GetOptions() {
			options = append(options, &pb.PermissionOption{Id: o.GetId(), Name: o.GetName(), Kind: o.GetKind()})
		}
		permission = &pb.PermissionRequest{
			RequestId:  req.GetRequestId(),
			Summary:    req.GetSummary(),
			Options:    options,
			ToolCallId: req.GetToolCallId(),
		}
		out = &pb.Event{Payload: &pb.Event_PermissionRequest{PermissionRequest: permission}}
	case *agentlinkpb.AgentEvent_TurnFinished:
		if reason := p.TurnFinished.GetError(); reason != "" {
			out = &pb.Event{Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{Reason: agentErrorReason(reason)}}}
		} else {
			out = &pb.Event{Payload: &pb.Event_TurnCompleted{TurnCompleted: &pb.TurnCompleted{StopReason: p.TurnFinished.GetStopReason()}}}
		}
	case *agentlinkpb.AgentEvent_Exited:
		if !s.advancePod(ctx, b, seq) {
			return false
		}
		b.session.Ack(seq)
		code := p.Exited.GetExitCode()
		s.log.InfoContext(ctx, "the agent exited; ending the session", "session", b.sessionID, "exit_code", code)
		go s.stopOwned(context.WithoutCancel(ctx), b.sessionID, b.owner, stopSpec{
			end: api.EndAgentExit, detail: fmt.Sprintf("the agent exited with code %d", code),
		})
		return true
	default:
		if !s.advancePod(ctx, b, seq) {
			return false
		}
		b.session.Ack(seq)
		return true
	}

	out.TurnId = turnID
	req := ev.GetPermissionRequest()
	var waiter *permissionWaiter
	var fresh bool
	if req != nil {
		waiter, fresh = b.sink.register(req, func(id string) { s.expirePermission(context.WithoutCancel(ctx), b, id) })
	}
	if !s.recordPod(ctx, b, seq, func(ctx context.Context) error {
		if permission != nil {
			permission.Deadline = timestamppb.New(b.sink.stamp(waiter, fresh))
		}
		return s.emitPod(ctx, b.sessionID, out, seq)
	}) {
		if fresh {
			b.sink.drop(req.GetRequestId(), waiter)
		}
		return false
	}
	if fresh {
		b.sink.arm(req.GetRequestId(), waiter)
	}
	b.session.Ack(seq)
	if ev.GetTurnFinished() != nil {
		b.leave(turnID)
		if err := s.store.DeletePodCommandsForTurn(ctx, b.sessionID, turnID); err != nil {
			s.log.WarnContext(ctx, "could not clear the commands of a finished turn", "session", b.sessionID, "turn_id", turnID, "err", err)
		}
	}
	return true
}

func toolInput(raw []byte) *structpb.Value {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	out, err := structpb.NewValue(v)
	if err != nil {
		return nil
	}
	return out
}

func (s *Service) advancePod(ctx context.Context, b *binding, seq uint64) bool {
	return s.recordPod(ctx, b, seq, func(ctx context.Context) error { return s.store.AdvancePodSeq(ctx, b.sessionID, int64(seq)) })
}

func (s *Service) recordPod(ctx context.Context, b *binding, seq uint64, record func(ctx context.Context) error) bool {
	err := s.whileBound(ctx, b, record, func(err error, wait time.Duration) {
		s.log.WarnContext(ctx, "could not record an agent event; retrying before the ack",
			"session", b.sessionID, "seq", seq, "retry_in", wait, "err", err)
	})
	if err != nil {
		s.log.WarnContext(ctx, "an agent event was not recorded and stays unacked; the pod sends it again on the next stream",
			"session", b.sessionID, "seq", seq, "err", err)
		return false
	}
	return true
}

func (s *Service) whileBound(ctx context.Context, b *binding, op func(ctx context.Context) error, notify backoff.Notify) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-b.done:
		case <-b.session.Done():
		case <-ctx.Done():
		}
		cancel()
	}()
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = podRetryInitial
	policy.MaxInterval = podRetryMax
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		err := op(ctx)
		if errors.Is(err, sessionstore.ErrNotFound) {
			return struct{}{}, backoff.Permanent(err)
		}
		return struct{}{}, err
	}, backoff.WithBackOff(policy), backoff.WithMaxElapsedTime(0), backoff.WithNotify(notify))
	return err
}

func (s *Service) reconcileState(ctx context.Context, b *binding, st *agentlinkpb.AgentState) {
	b.sendMu.Lock()
	b.syncing = true
	b.sendMu.Unlock()
	var cmds []sessionstore.PodCommand
	err := s.whileBound(ctx, b, func(ctx context.Context) error {
		b.permMu.Lock()
		var err error
		cmds, err = s.store.ListPodCommands(ctx, b.sessionID)
		if err != nil {
			b.permMu.Unlock()
		}
		return err
	}, func(err error, wait time.Duration) {
		s.log.WarnContext(ctx, "could not read the commands the agent may not have; new turns wait",
			"session", b.sessionID, "retry_in", wait, "err", err)
	})
	if err != nil {
		s.log.WarnContext(ctx, "gave up reading the commands the agent may not have", "session", b.sessionID, "err", err)
		return
	}
	defer b.permMu.Unlock()
	b.sendMu.Lock()
	outstanding := slices.Clone(st.GetOutstandingTurnIds())
	pending := map[string]*agentlinkpb.PermissionRequest{}
	for _, req := range st.GetPendingPermissions() {
		pending[req.GetRequestId()] = req
	}

	var resend []*agentlinkpb.Prompt
	for _, c := range cmds {
		switch c.Kind {
		case commandPrompt:
			var p agentlinkpb.Prompt
			if err := proto.Unmarshal(c.Payload, &p); err != nil {
				s.dropCommand(ctx, c)
				continue
			}
			switch {
			case p.GetTurnSeq() > st.GetLastTurnSeq():
				resend = append(resend, &p)
			case !slices.Contains(outstanding, p.GetTurnId()):
				s.dropCommand(ctx, c)
			}
		case commandPermission:
			var d agentlinkpb.PermissionDecision
			if _, waits := pending[c.Key]; !waits || proto.Unmarshal(c.Payload, &d) != nil {
				s.dropCommand(ctx, c)
				continue
			}
			delete(pending, c.Key)
			b.session.Decide(d.GetRequestId(), d.GetOptionId(), d.GetCancelled())
		}
	}

	for _, p := range resend {
		outstanding = append(outstanding, p.GetTurnId())
	}
	for _, p := range b.held {
		if !slices.Contains(outstanding, p.GetTurnId()) {
			outstanding = append(outstanding, p.GetTurnId())
		}
	}
	b.setOutstanding(outstanding)
	for _, p := range resend {
		if !slices.ContainsFunc(b.held, func(h *agentlinkpb.Prompt) bool { return h.GetTurnId() == p.GetTurnId() }) {
			s.log.InfoContext(ctx, "sending a turn the agent did not receive", "session", b.sessionID, "turn_id", p.GetTurnId())
			b.held = append(b.held, p)
		}
	}
	b.syncing = false
	b.release()
	b.sendMu.Unlock()

	for _, req := range pending {
		b.sink.await(req, b.sink.deadline(), func(id string) { s.expirePermission(context.WithoutCancel(ctx), b, id) })
	}
	waiting := map[string]bool{}
	for _, req := range st.GetPendingPermissions() {
		waiting[req.GetRequestId()] = true
	}
	for _, id := range b.sink.pending() {
		if !waiting[id] {
			b.sink.take(id)
		}
	}
}

func (s *Service) dropCommand(ctx context.Context, c sessionstore.PodCommand) {
	if err := s.store.DeletePodCommand(ctx, c.SessionID, c.Kind, c.Key); err != nil {
		s.log.WarnContext(ctx, "could not clear a command the agent already has", "session", c.SessionID, "kind", c.Kind, "key", c.Key, "err", err)
	}
}

func (s *Service) sendPrompt(ctx context.Context, b *binding, turnID string, turnSeq uint64, text string) error {
	p := &agentlinkpb.Prompt{TurnId: turnID, TurnSeq: turnSeq, Text: text}
	payload, err := proto.Marshal(p)
	if err == nil {
		err = s.store.PutPodCommand(ctx, sessionstore.PodCommand{
			SessionID: b.sessionID, Kind: commandPrompt, Key: turnID, TurnID: turnID, Payload: payload,
		})
	}
	if err != nil {
		s.log.WarnContext(ctx, "could not save a turn; it is not sent", "session", b.sessionID, "turn_id", turnID, "err", err)
		return err
	}
	b.held = append(b.held, p)
	b.release()
	return nil
}

func (s *Service) ungate(b *binding) {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	b.gated = false
	b.release()
}

func (s *Service) saveDecision(ctx context.Context, b *binding, turnID, requestID, optionID string, cancelled bool) error {
	d := &agentlinkpb.PermissionDecision{RequestId: requestID, OptionId: optionID, Cancelled: cancelled}
	payload, err := proto.Marshal(d)
	if err == nil {
		err = s.store.PutPodCommand(ctx, sessionstore.PodCommand{
			SessionID: b.sessionID, Kind: commandPermission, Key: requestID, TurnID: turnID, Payload: payload,
		})
	}
	if err != nil {
		s.log.WarnContext(ctx, "could not save a permission decision; it is not sent", "session", b.sessionID, "request_id", requestID, "err", err)
	}
	return err
}

func (s *Service) expirePermission(ctx context.Context, b *binding, requestID string) {
	b.permMu.Lock()
	defer b.permMu.Unlock()
	w, ok := b.sink.take(requestID)
	if !ok {
		return
	}
	if err := s.saveDecision(ctx, b, w.turnID, requestID, "", true); err != nil {
		b.sink.restore(requestID, w, b.sink.expireRetry)
		return
	}
	s.emit(ctx, b.sessionID, &pb.Event{TurnId: w.turnID, Payload: &pb.Event_PermissionResolved{PermissionResolved: &pb.PermissionResolved{
		RequestId: requestID, Resolution: unanswered(api.ResolutionExpired).GetResolution(),
	}}})
	b.session.Decide(requestID, "", true)
}

func encodeExecutor(e agenticrun.Executor) string {
	data, _ := json.Marshal(e)
	return string(data)
}

func turnName(n int64) string { return "t" + strconv.FormatInt(n, 10) }
