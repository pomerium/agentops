package harnessclient

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agentio"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

type bearer string

func (b bearer) Bearer() string { return string(b) }

func (bearer) Refresh() {}

type fakeLink struct {
	agentlinkpb.AgentLinkServiceClient
	attach *fakeAttach
}

func (l *fakeLink) Attach(ctx context.Context, _ ...grpc.CallOption) (agentlinkpb.AgentLinkService_AttachClient, error) {
	l.attach.ctx = ctx
	return l.attach, nil
}

func (l *fakeLink) AgentIO(ctx context.Context, _ ...grpc.CallOption) (agentlinkpb.AgentLinkService_AgentIOClient, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type fakeAttach struct {
	agentlinkpb.AgentLinkService_AttachClient
	ctx     context.Context
	exitErr error
	exited  chan int32

	mu    sync.Mutex
	acked bool
	hello *agentlinkpb.SidecarHello
}

func newFakeAttach(exitErr error) *fakeAttach {
	return &fakeAttach{exitErr: exitErr, exited: make(chan int32, 1)}
}

func (s *fakeAttach) Recv() (*agentlinkpb.ManagerFrame, error) {
	s.mu.Lock()
	acked := s.acked
	s.acked = true
	s.mu.Unlock()
	if !acked {
		return &agentlinkpb.ManagerFrame{Msg: &agentlinkpb.ManagerFrame_HelloAck{
			HelloAck: &agentlinkpb.ManagerHelloAck{},
		}}, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *fakeAttach) Send(f *agentlinkpb.SidecarFrame) error {
	switch {
	case f.GetHello() != nil:
		s.mu.Lock()
		s.hello = f.GetHello()
		s.mu.Unlock()
	case f.GetExited() != nil:
		s.exited <- f.GetExited().GetExitCode()
		return s.exitErr
	}
	return nil
}

func (s *fakeAttach) agentRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hello.GetAgentRunning()
}

func TestAnExitTheLinkDroppedIsReportedAfterReattach(t *testing.T) {
	exited := make(chan int32, 1)
	exited <- 7
	link := &fakeLink{attach: newFakeAttach(status.Error(codes.Unavailable, "link dropped"))}
	c := &Client{
		cfg:      Config{Token: bearer("Bearer pom_art_test")},
		log:      slog.New(slog.DiscardHandler),
		client:   link,
		statusCh: make(chan *agentlinkpb.Status, 4),
		agent:    &AgentSession{IO: agentio.New(), Exited: exited},
	}
	defer c.stopAgent()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.session(ctx, 1); status.Code(err) != codes.Unavailable {
		t.Fatalf("first session ended with %v, want the dropped exit send", err)
	}
	if code := <-link.attach.exited; code != 7 {
		t.Fatalf("first link saw exit %d, want 7", code)
	}

	second := newFakeAttach(nil)
	link.attach = second
	ctx2, cancel2 := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.session(ctx2, 2) }()
	select {
	case code := <-second.exited:
		if code != 7 {
			t.Errorf("reattached link saw exit %d, want 7", code)
		}
	case <-time.After(5 * time.Second):
		t.Error("the reattached link never learned that the agent exited")
	}
	cancel2()
	<-done
	if second.agentRunning() {
		t.Error("the reattach Hello reported a running agent after it exited")
	}
}
