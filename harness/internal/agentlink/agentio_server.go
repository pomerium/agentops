package agentlink

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

var errProtocol = errors.New("agentio protocol violation")

func NewStreamID() []byte { return []byte(rand.Text()) }

func (s *Server) AgentIO(stream agentlinkpb.AgentLinkService_AgentIOServer) error {
	ctx := stream.Context()
	a, err := s.verify(ctx)
	if err != nil {
		return err
	}
	run, err := s.lookup(a)
	if err != nil {
		return err
	}
	if run.current() == nil {
		return status.Errorf(codes.FailedPrecondition, "run %s has no live attach", run.runID)
	}

	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "agent io closed before Open: %v", err)
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.FailedPrecondition, "the first AgentIO frame must be Open")
	}
	if !bytes.Equal(open.GetStreamId(), run.streamID) {
		err := fmt.Errorf("agent io names stream %q, want %q", open.GetStreamId(), run.streamID)
		s.log.Error("harness: agent io names a different stream", "run_id", run.runID, "err", err)
		s.fail(run, reasonStreamMismatch, err)
		return status.Errorf(codes.FailedPrecondition, "%s: %v", reasonStreamMismatch, err)
	}

	claim, err := run.claimIO(ctx)
	switch {
	case errors.Is(err, errForgotten):
		return status.Errorf(codes.NotFound, "run %s is no longer expected", run.runID)
	case errors.Is(err, errIOSuperseded):
		return status.Errorf(codes.Aborted, "run %s: %v", run.runID, err)
	case err != nil:
		return status.FromContextError(err).Err()
	}
	defer run.releaseIO(claim)

	resume := run.deliveredSeq()
	if err := stream.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Open{Open: &agentlinkpb.AgentIOOpen{
		Consumed: resume, StreamId: run.streamID,
	}}}); err != nil {
		return err
	}
	s.log.Info("harness: agent io attached", "run_id", run.runID, "resume_after", resume)

	err = s.serveIO(ctx, stream, run, claim)
	if errors.Is(err, errProtocol) {
		s.log.Error("harness: agent io protocol violation", "run_id", run.runID, "err", err)
		s.fail(run, reasonProtocol, err)
		return status.Errorf(codes.FailedPrecondition, "%s: %v", reasonProtocol, err)
	}
	return err
}

func (s *Server) serveIO(ctx context.Context, stream agentlinkpb.AgentLinkService_AgentIOServer, run *attachedRun, claim *ioClaim) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type recvResult struct {
		frame *agentlinkpb.AgentIOFrame
		err   error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			f, err := stream.Recv()
			select {
			case recvCh <- recvResult{f, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	sendErr := make(chan error, 1)
	go func() {
		if err := sendCommands(ctx, stream, run); err != nil {
			sendErr <- err
		}
	}()

	var pending *agentlinkpb.AgentIOFrame
	var pendingSeq uint64
	for {
		var in <-chan recvResult
		var out chan<- *agentlinkpb.AgentIOFrame
		if pending == nil {
			in = recvCh
		} else {
			out = run.inbox
		}
		select {
		case r := <-in:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					return nil
				}
				return r.err
			}
			switch f := r.frame; {
			case f.GetState() != nil:
				pending = f
			case f.GetEvent() != nil:
				seq, delivered := f.GetEvent().GetSeq(), run.deliveredSeq()
				switch {
				case seq <= delivered:
				case seq != delivered+1:
					return fmt.Errorf("%w: event %d after event %d", errProtocol, seq, delivered)
				default:
					pending, pendingSeq = f, seq
				}
			default:
				return fmt.Errorf("%w: the sidecar sent a %T", errProtocol, f.GetMsg())
			}
		case out <- pending:
			if pendingSeq > 0 {
				run.setDelivered(pendingSeq)
			}
			pending, pendingSeq = nil, 0
		case err := <-sendErr:
			return err
		case <-claim.closed:
			return status.Error(codes.Aborted, errIOSuperseded.Error())
		case <-run.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func sendCommands(ctx context.Context, stream agentlinkpb.AgentLinkService_AgentIOServer, run *attachedRun) error {
	var sent, lastAck uint64
	for {
		for {
			c, ok := run.nextCommand(sent)
			if !ok {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			if err := stream.Send(c.frame); err != nil {
				return err
			}
			run.commitCommand(c.id)
			sent = c.id
		}
		if acked := run.ackedSeq(); acked > lastAck {
			if ctx.Err() != nil {
				return nil
			}
			if err := stream.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Ack{Ack: &agentlinkpb.AgentIOAck{Consumed: acked}}}); err != nil {
				return err
			}
			lastAck = acked
		}
		select {
		case <-run.outReady:
			if ctx.Err() != nil {
				run.signalOut()
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Server) fail(run *attachedRun, reason string, cause error) {
	if run.opts.OnError != nil {
		run.opts.OnError(reason, cause)
	}
}
