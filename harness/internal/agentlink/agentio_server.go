package agentlink

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agentio"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

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
	if err := agentio.CheckStreamID(open, run.streamID); err != nil {
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

	if err := run.io.ValidateResume(open.GetConsumed()); err != nil {
		s.log.Error("harness: agent io resume is unserviceable", "run_id", run.runID, "err", err)
		s.fail(run, reasonResumeInvalid, err)
		return status.Errorf(codes.FailedPrecondition, "%s: %v", reasonResumeInvalid, err)
	}

	if err := stream.Send(agentio.OpenFrame(run.io.Consumed(), run.streamID)); err != nil {
		return err
	}
	run.io.StartRecorder()
	run.markIOReady()
	s.log.Info("harness: agent io attached", "run_id", run.runID,
		"peer_consumed", open.GetConsumed(), "our_consumed", run.io.Consumed())

	stop := make(chan struct{})
	go func() {
		defer close(stop)
		select {
		case <-claim.closed:
		case <-run.done:
		case <-ctx.Done():
		}
	}()

	err = run.io.Pump(ctx, stream, open.GetConsumed(), agentio.WithStop(stop))
	if errors.Is(err, agentio.ErrProtocol) {
		s.log.Error("harness: agent io protocol violation", "run_id", run.runID, "err", err)
		s.fail(run, reasonProtocol, err)
		return status.Errorf(codes.FailedPrecondition, "%s: %v", reasonProtocol, err)
	}
	return err
}

func (s *Server) fail(run *attachedRun, reason string, cause error) {
	if run.opts.OnError != nil {
		run.opts.OnError(reason, cause)
	}
}
