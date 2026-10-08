package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

var ErrResumeUnavailable = errors.New("this conversation cannot be resumed")

var errLinkClosed = errors.New("the agent link closed before the session opened")

type Session struct {
	att       *Attachment
	acpID     string
	readySeq  uint64
	closeOnce sync.Once
}

func (s *Session) ID() string { return s.acpID }

func (s *Session) StreamID() []byte { return s.att.handle.StreamID() }

func (s *Session) ReadySeq() uint64 { return s.readySeq }

func (s *Session) Inbox() <-chan *agentlinkpb.AgentIOFrame { return s.att.handle.Inbox() }

func (s *Session) Done() <-chan struct{} { return s.att.handle.Done() }

func (s *Session) Ack(seq uint64) { s.att.handle.Ack(seq) }

func (s *Session) Prompt(turnID string, turnSeq uint64, text string) {
	s.att.handle.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Prompt{Prompt: &agentlinkpb.Prompt{
		TurnId: turnID, TurnSeq: turnSeq, Text: text,
	}}})
}

func (s *Session) Decide(requestID, optionID string, cancelled bool) {
	s.att.handle.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Permission{Permission: &agentlinkpb.PermissionDecision{
		RequestId: requestID, OptionId: optionID, Cancelled: cancelled,
	}}})
}

func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		s.att.handle.Shutdown("session_end")
		s.att.Forget()
	})
	return nil
}

func (s *Session) awaitReady(ctx context.Context) error {
	for {
		select {
		case f := <-s.att.handle.Inbox():
			ev := f.GetEvent()
			switch {
			case ev == nil:
			case ev.GetSessionReady() != nil:
				s.acpID, s.readySeq = ev.GetSessionReady().GetAcpSessionId(), ev.GetSeq()
				return nil
			case ev.GetSessionFailed() != nil:
				failed := ev.GetSessionFailed()
				s.att.handle.Ack(ev.GetSeq())
				if failed.GetResumeUnavailable() {
					return fmt.Errorf("%w: %s", ErrResumeUnavailable, failed.GetReason())
				}
				return fmt.Errorf("the agent session did not open: %s", failed.GetReason())
			default:
				return fmt.Errorf("the first agent event is %T, want SessionReady", ev.GetPayload())
			}
		case <-s.att.handle.Done():
			return errLinkClosed
		case <-ctx.Done():
			return fmt.Errorf("await the agent session: %w", ctx.Err())
		}
	}
}
