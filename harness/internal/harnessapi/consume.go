package harnessapi

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

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
				s.handleEvent(ctx, b, f.GetEvent())
			}
		}
	}
}

func (s *Service) handleEvent(ctx context.Context, b *binding, ev *agentlinkpb.AgentEvent) {
	turnID, seq := ev.GetTurnId(), ev.GetSeq()
	var out *pb.Event
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
		deadline := b.sink.await(req, func(id string) { s.expirePermission(context.WithoutCancel(ctx), b, id) })
		options := make([]*pb.PermissionOption, 0, len(req.GetOptions()))
		for _, o := range req.GetOptions() {
			options = append(options, &pb.PermissionOption{Id: o.GetId(), Name: o.GetName(), Kind: o.GetKind()})
		}
		out = &pb.Event{Payload: &pb.Event_PermissionRequest{PermissionRequest: &pb.PermissionRequest{
			RequestId:  req.GetRequestId(),
			Summary:    req.GetSummary(),
			Options:    options,
			Deadline:   timestamppb.New(deadline),
			ToolCallId: req.GetToolCallId(),
		}}}
	case *agentlinkpb.AgentEvent_TurnFinished:
		if reason := p.TurnFinished.GetError(); reason != "" {
			out = &pb.Event{Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{Reason: agentErrorReason(reason)}}}
		} else {
			out = &pb.Event{Payload: &pb.Event_TurnCompleted{TurnCompleted: &pb.TurnCompleted{StopReason: p.TurnFinished.GetStopReason()}}}
		}
	case *agentlinkpb.AgentEvent_Exited:
		s.advancePod(ctx, b, seq)
		b.session.Ack(seq)
		code := p.Exited.GetExitCode()
		s.log.InfoContext(ctx, "the agent exited; ending the session", "session", b.sessionID, "exit_code", code)
		go s.stopOwned(context.WithoutCancel(ctx), b.sessionID, b.owner, stopSpec{
			end: api.EndAgentExit, detail: fmt.Sprintf("the agent exited with code %d", code),
		})
		return
	default:
		s.advancePod(ctx, b, seq)
		b.session.Ack(seq)
		return
	}

	out.TurnId = turnID
	s.emitPod(ctx, b.sessionID, out, seq)
	b.session.Ack(seq)
	if ev.GetTurnFinished() != nil {
		b.leave(turnID)
		if err := s.store.DeletePodCommandsForTurn(ctx, b.sessionID, turnID); err != nil {
			s.log.WarnContext(ctx, "could not clear the commands of a finished turn", "session", b.sessionID, "turn_id", turnID, "err", err)
		}
	}
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

func (s *Service) advancePod(ctx context.Context, b *binding, seq uint64) {
	if err := s.store.AdvancePodSeq(ctx, b.sessionID, int64(seq)); err != nil {
		s.log.WarnContext(ctx, "could not record the last agent event", "session", b.sessionID, "seq", seq, "err", err)
	}
}

func (s *Service) reconcileState(ctx context.Context, b *binding, st *agentlinkpb.AgentState) {
	cmds, err := s.store.ListPodCommands(ctx, b.sessionID)
	if err != nil {
		s.log.WarnContext(ctx, "could not read the commands the agent may not have", "session", b.sessionID, "err", err)
	}
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

	slices.SortFunc(resend, func(x, y *agentlinkpb.Prompt) int { return cmp.Compare(x.GetTurnSeq(), y.GetTurnSeq()) })
	for _, p := range resend {
		outstanding = append(outstanding, p.GetTurnId())
	}
	b.sendMu.Lock()
	for _, p := range b.held {
		if !slices.Contains(outstanding, p.GetTurnId()) {
			outstanding = append(outstanding, p.GetTurnId())
		}
	}
	b.setOutstanding(outstanding)
	if !b.gated {
		for _, p := range resend {
			s.log.InfoContext(ctx, "sending a turn the agent did not receive", "session", b.sessionID, "turn_id", p.GetTurnId())
			b.session.Prompt(p.GetTurnId(), p.GetTurnSeq(), p.GetText())
		}
	}
	b.sendMu.Unlock()

	for _, req := range pending {
		b.sink.await(req, func(id string) { s.expirePermission(context.WithoutCancel(ctx), b, id) })
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

func (s *Service) sendPrompt(ctx context.Context, b *binding, turnID string, turnSeq uint64, text string) {
	p := &agentlinkpb.Prompt{TurnId: turnID, TurnSeq: turnSeq, Text: text}
	defer func() {
		if b.gated {
			b.held = append(b.held, p)
			return
		}
		b.session.Prompt(turnID, turnSeq, text)
	}()
	payload, err := proto.Marshal(p)
	if err == nil {
		err = s.store.PutPodCommand(ctx, sessionstore.PodCommand{
			SessionID: b.sessionID, Kind: commandPrompt, Key: turnID, TurnID: turnID, Payload: payload,
		})
	}
	if err != nil {
		s.log.WarnContext(ctx, "could not save a turn before sending it; a restart before the agent gets it loses the turn",
			"session", b.sessionID, "turn_id", turnID, "err", err)
	}
}

func (s *Service) ungate(ctx context.Context, b *binding, opening *agentlinkpb.Prompt) {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	b.gated = false
	if opening != nil {
		s.sendPrompt(ctx, b, opening.GetTurnId(), opening.GetTurnSeq(), opening.GetText())
	}
	for _, p := range b.held {
		b.session.Prompt(p.GetTurnId(), p.GetTurnSeq(), p.GetText())
	}
	b.held = nil
}

func (s *Service) decide(ctx context.Context, b *binding, turnID, requestID, optionID string, cancelled bool) {
	d := &agentlinkpb.PermissionDecision{RequestId: requestID, OptionId: optionID, Cancelled: cancelled}
	payload, err := proto.Marshal(d)
	if err == nil {
		err = s.store.PutPodCommand(ctx, sessionstore.PodCommand{
			SessionID: b.sessionID, Kind: commandPermission, Key: requestID, TurnID: turnID, Payload: payload,
		})
	}
	if err != nil {
		s.log.WarnContext(ctx, "could not save a permission decision before sending it", "session", b.sessionID, "request_id", requestID, "err", err)
	}
	b.session.Decide(requestID, optionID, cancelled)
}

func (s *Service) expirePermission(ctx context.Context, b *binding, requestID string) {
	w, ok := b.sink.take(requestID)
	if !ok {
		return
	}
	s.emit(ctx, b.sessionID, &pb.Event{TurnId: w.turnID, Payload: &pb.Event_PermissionResolved{PermissionResolved: &pb.PermissionResolved{
		RequestId: requestID, Resolution: unanswered(api.ResolutionExpired).GetResolution(),
	}}})
	s.decide(ctx, b, w.turnID, requestID, "", true)
}

func encodeExecutor(e agenticrun.Executor) string {
	data, _ := json.Marshal(e)
	return string(data)
}

func turnName(n int64) string { return "t" + strconv.FormatInt(n, 10) }
