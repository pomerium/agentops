package agentlink

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

type fakeIOStream struct {
	grpc.ServerStream
	ctx     context.Context
	block   bool
	entered chan struct{}
	unblock chan struct{}

	mu   sync.Mutex
	sent []*agentlinkpb.AgentIOFrame
	got  chan struct{}
}

func newFakeIOStream(ctx context.Context, block bool) *fakeIOStream {
	return &fakeIOStream{
		ctx: ctx, block: block,
		entered: make(chan struct{}), unblock: make(chan struct{}), got: make(chan struct{}, 16),
	}
}

func (s *fakeIOStream) Context() context.Context { return s.ctx }

func (s *fakeIOStream) Send(f *agentlinkpb.AgentIOFrame) error {
	if s.block {
		select {
		case <-s.entered:
		default:
			close(s.entered)
		}
		<-s.unblock
		return io.ErrClosedPipe
	}
	s.mu.Lock()
	s.sent = append(s.sent, f)
	s.mu.Unlock()
	s.got <- struct{}{}
	return nil
}

func (s *fakeIOStream) Recv() (*agentlinkpb.AgentIOFrame, error) {
	select {
	case <-s.unblock:
	case <-s.ctx.Done():
	}
	return nil, io.EOF
}

func (s *fakeIOStream) prompts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, f := range s.sent {
		if p := f.GetPrompt(); p != nil {
			out = append(out, p.GetTurnId())
		}
	}
	return out
}

func newIORun() *attachedRun {
	return &attachedRun{
		runID: "io", hbInterval: time.Hour,
		inbox:    make(chan *agentlinkpb.AgentIOFrame, inboxSize),
		ioTurn:   make(chan struct{}, 1),
		outReady: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func serveClaimed(t *testing.T, s *Server, run *attachedRun, stream *fakeIOStream) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	claim, err := run.claimIO(ctx)
	if err != nil {
		t.Fatalf("claimIO: %v", err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		defer run.releaseIO(claim)
		_ = s.serveIO(stream.ctx, stream, run, claim)
	}()
	return stopped
}

func TestANewAgentIOTakesOverWhileTheOldOneIsStuckInSend(t *testing.T) {
	s := &Server{log: slog.New(slog.DiscardHandler), now: time.Now}
	run := newIORun()
	h := &RunHandle{run: run}
	h.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Prompt{Prompt: &agentlinkpb.Prompt{TurnId: "t1", TurnSeq: 1}}})

	oldCtx, cancelOld := context.WithCancel(context.Background())
	defer cancelOld()
	old := newFakeIOStream(oldCtx, true)
	oldStopped := serveClaimed(t, s, run, old)
	t.Cleanup(func() {
		close(old.unblock)
		<-oldStopped
	})
	select {
	case <-old.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first stream never sent the queued prompt")
	}

	newCtx, cancelNew := context.WithCancel(context.Background())
	defer cancelNew()
	fresh := newFakeIOStream(newCtx, false)
	serveClaimed(t, s, run, fresh)
	h.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Prompt{Prompt: &agentlinkpb.Prompt{TurnId: "t2", TurnSeq: 2}}})

	deadline := time.After(2 * time.Second)
	for {
		if got := fresh.prompts(); len(got) == 2 {
			if got[0] != "t1" || got[1] != "t2" {
				t.Fatalf("the new stream sent prompts %v, want [t1 t2]", got)
			}
			return
		}
		select {
		case <-fresh.got:
		case <-deadline:
			t.Fatalf("the new stream sent prompts %v, want [t1 t2]", fresh.prompts())
		}
	}
}
