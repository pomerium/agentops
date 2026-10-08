package runner

import (
	"context"
	"log/slog"
	"os/exec"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

type blockedReplayStream struct {
	runnerpb.AgentRunnerService_RunServer
	ctx     context.Context
	frames  chan *runnerpb.RunnerClientFrame
	blocked chan struct{}
	release chan struct{}
}

func (s *blockedReplayStream) Context() context.Context { return s.ctx }

func (s *blockedReplayStream) Recv() (*runnerpb.RunnerClientFrame, error) {
	select {
	case f := <-s.frames:
		return f, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *blockedReplayStream) Send(f *runnerpb.RunnerServerFrame) error {
	if f.GetReplayStart() != nil {
		close(s.blocked)
		<-s.release
	}
	return nil
}

func TestAReplacedStreamReturnsWhileItsSendIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := &blockedReplayStream{
		ctx: ctx, frames: make(chan *runnerpb.RunnerClientFrame, 2),
		blocked: make(chan struct{}), release: make(chan struct{}),
	}
	defer close(st.release)
	svc := New()
	svc.sess = newSession(slog.Default(), []byte("A"), &agentProc{cmd: &exec.Cmd{}, done: make(chan struct{})}, time.Second, outboxMax)
	st.frames <- &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Join{Join: &runnerpb.Join{}}}
	st.frames <- &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Replay{Replay: &runnerpb.Replay{}}}
	done := make(chan error, 1)
	go func() { done <- svc.Run(st) }()
	<-st.blocked

	_, newer := context.WithCancelCause(context.Background())
	defer newer(nil)
	svc.claimClient(newer)
	select {
	case err := <-done:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("the replaced stream returned %v, want Aborted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced stream waited for its blocked Send")
	}
}
