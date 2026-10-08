package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	acp "github.com/coder/acp-go-sdk"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

const stdoutDrainGrace = 2 * time.Second

type session struct {
	log       *slog.Logger
	streamID  []byte
	proc      *agentProc
	out       *outbox
	killDelay time.Duration
	agg       *Aggregator

	conn  *acp.ClientSideConnection
	acpID acp.SessionId

	mu          sync.Mutex
	lastTurnSeq uint64
	queue       []*agentlinkpb.Prompt
	running     *agentlinkpb.Prompt
	perms       []*permWait
	wake        chan struct{}
	finished    bool

	done chan struct{}
}

type permWait struct {
	req      *agentlinkpb.PermissionRequest
	decision chan *agentlinkpb.PermissionDecision
}

var _ acp.Client = (*session)(nil)

func newSession(log *slog.Logger, streamID []byte, proc *agentProc, killDelay time.Duration, maxOutbox int) *session {
	s := &session{
		log:       log,
		streamID:  bytes.Clone(streamID),
		proc:      proc,
		out:       newOutbox(maxOutbox),
		killDelay: killDelay,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	s.agg = NewAggregator(func(turnID string, ev *agentlinkpb.AgentEvent) {
		ev.TurnId = turnID
		s.out.append(ev, false)
	})
	return s
}

func (s *session) emit(turnID string, ev *agentlinkpb.AgentEvent, urgent bool) {
	ev.TurnId = turnID
	s.out.append(ev, urgent)
}

func (s *session) run(params *agentlinkpb.SessionParams) {
	defer close(s.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.conn = acp.NewClientSideConnection(s, s.proc.stdin, s.proc.stdout)
	s.conn.SetLogger(s.log)
	go s.watchExit(cancel)

	opened, err := openSession(ctx, s.log, s.conn, params)
	if err != nil {
		s.log.Error("agent-runner: the ACP session did not open", "err", err)
		s.emit("", &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_SessionFailed{SessionFailed: &agentlinkpb.SessionFailed{
			Reason: err.Error(), ResumeUnavailable: errors.Is(err, errResumeUnavailable),
		}}}, true)
		s.stop()
		s.finish()
		return
	}
	s.acpID = opened.id
	s.log.Info("agent-runner: ACP session open", "acp_session_id", string(opened.id), "resumable", opened.resumable)
	s.emit("", &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_SessionReady{SessionReady: &agentlinkpb.SessionReady{
		AcpSessionId: string(opened.id), Resumable: opened.resumable,
	}}}, true)

	for {
		p := s.nextTurn()
		if p == nil {
			break
		}
		s.runTurn(ctx, p)
	}
	s.finish()
}

func (s *session) watchExit(cancel context.CancelFunc) {
	<-s.proc.done
	s.out.release()
	s.proc.terminate(s.log, s.killDelay)
	select {
	case <-s.conn.Done():
	case <-time.After(stdoutDrainGrace):
	}
	_ = s.proc.stdout.Close()
	_ = s.proc.stderr.Close()
	_ = s.proc.stdin.Close()
	cancel()
}

func (s *session) nextTurn() *agentlinkpb.Prompt {
	for {
		s.mu.Lock()
		if s.proc.exited() {
			s.mu.Unlock()
			return nil
		}
		if len(s.queue) > 0 {
			p := s.queue[0]
			s.queue = s.queue[1:]
			s.running = p
			s.mu.Unlock()
			return p
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-s.proc.done:
		}
	}
}

func (s *session) runTurn(ctx context.Context, p *agentlinkpb.Prompt) {
	s.log.Debug("acp -> prompt", "turn_id", p.GetTurnId(), "chars", len(p.GetText()))
	s.agg.Begin(p.GetTurnId())
	resp, err := s.conn.Prompt(ctx, acp.PromptRequest{
		SessionId: s.acpID,
		Prompt:    []acp.ContentBlock{acp.TextBlock(p.GetText())},
	})
	s.agg.End()
	finished := &agentlinkpb.TurnFinished{}
	if err != nil {
		s.log.Warn("acp <- prompt failed", "turn_id", p.GetTurnId(), "err", err)
		finished.Error = fmt.Sprintf("acp prompt: %v", err)
	} else {
		finished.StopReason = string(resp.StopReason)
		if resp.Usage != nil {
			s.emit(p.GetTurnId(), &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Usage{Usage: usageFrom(resp.Usage)}}, false)
		}
	}
	s.emit(p.GetTurnId(), &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_TurnFinished{TurnFinished: finished}}, true)
	s.mu.Lock()
	s.running = nil
	s.mu.Unlock()
}

func (s *session) finish() {
	<-s.proc.done
	s.mu.Lock()
	queued := s.queue
	s.queue = nil
	s.finished = true
	s.mu.Unlock()
	for _, p := range queued {
		s.emit(p.GetTurnId(), &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_TurnFinished{TurnFinished: &agentlinkpb.TurnFinished{
			Error: "the agent exited before this turn ran",
		}}}, true)
	}
	code := s.proc.exitCode()
	s.log.Info("agent-runner: agent exited", "pid", s.proc.pid(), "exit_code", code)
	s.emit("", &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Exited{Exited: &agentlinkpb.AgentExited{ExitCode: code}}}, true)
}

func (s *session) prompt(p *agentlinkpb.Prompt) {
	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		s.log.Debug("agent-runner: ignoring a prompt after the agent exited", "turn_id", p.GetTurnId(), "turn_seq", p.GetTurnSeq())
		return
	}
	if p.GetTurnSeq() <= s.lastTurnSeq {
		s.mu.Unlock()
		s.log.Debug("agent-runner: ignoring a prompt it already has", "turn_id", p.GetTurnId(), "turn_seq", p.GetTurnSeq())
		return
	}
	s.lastTurnSeq = p.GetTurnSeq()
	s.queue = append(s.queue, p)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *session) decide(d *agentlinkpb.PermissionDecision) {
	s.mu.Lock()
	var w *permWait
	for i, p := range s.perms {
		if p.req.GetRequestId() == d.GetRequestId() {
			w = p
			s.perms = append(s.perms[:i:i], s.perms[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
	if w == nil {
		s.log.Debug("agent-runner: ignoring a decision for a request that does not wait", "request_id", d.GetRequestId())
		return
	}
	w.decision <- d
}

func (s *session) dropPermission(w *permWait) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, p := range s.perms {
		if p == w {
			s.perms = append(s.perms[:i:i], s.perms[i+1:]...)
			return
		}
	}
}

func (s *session) stop() {
	go s.proc.terminate(s.log, s.killDelay)
}

func (s *session) state() *agentlinkpb.AgentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := &agentlinkpb.AgentState{LastTurnSeq: s.lastTurnSeq}
	if s.running != nil {
		st.OutstandingTurnIds = append(st.OutstandingTurnIds, s.running.GetTurnId())
	}
	for _, p := range s.queue {
		st.OutstandingTurnIds = append(st.OutstandingTurnIds, p.GetTurnId())
	}
	for _, p := range s.perms {
		st.PendingPermissions = append(st.PendingPermissions, p.req)
	}
	return st
}

func (s *session) SessionUpdate(_ context.Context, n acp.SessionNotification) error {
	s.agg.Update(n.Update)
	return nil
}

func (s *session) RequestPermission(ctx context.Context, p acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	id := string(p.ToolCall.ToolCallId)
	req := &agentlinkpb.PermissionRequest{
		RequestId:  id,
		ToolCallId: id,
		TurnId:     s.agg.Current(),
	}
	if p.ToolCall.Title != nil {
		req.Summary = *p.ToolCall.Title
	} else {
		req.Summary = s.agg.Title(id)
	}
	for _, o := range p.Options {
		req.Options = append(req.Options, &agentlinkpb.PermissionOption{
			Id: string(o.OptionId), Name: o.Name, Kind: string(o.Kind),
		})
	}
	w := &permWait{req: req, decision: make(chan *agentlinkpb.PermissionDecision, 1)}
	s.mu.Lock()
	s.perms = append(s.perms, w)
	s.mu.Unlock()
	s.emit(req.GetTurnId(), &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_PermissionRequest{PermissionRequest: req}}, false)

	select {
	case d := <-w.decision:
		if d.GetCancelled() {
			return acp.RequestPermissionResponse{
				Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}},
			}, nil
		}
		return acp.RequestPermissionResponse{
			Outcome: acp.RequestPermissionOutcome{
				Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(d.GetOptionId())},
			},
		}, nil
	case <-ctx.Done():
		s.dropPermission(w)
		return acp.RequestPermissionResponse{
			Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}},
		}, nil
	}
}

func (s *session) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, errors.New("fs.readTextFile not supported by this client")
}

func (s *session) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, errors.New("fs.writeTextFile not supported by this client")
}

func (s *session) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, errors.New("terminal not supported by this client")
}

func (s *session) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, errors.New("terminal not supported by this client")
}

func (s *session) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, errors.New("terminal not supported by this client")
}

func (s *session) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, errors.New("terminal not supported by this client")
}

func (s *session) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, errors.New("terminal not supported by this client")
}
