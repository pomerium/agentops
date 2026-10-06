package harnessclient

import (
	"context"
	"io"
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
	helloed chan struct{}
	frames  chan *agentlinkpb.ManagerFrame

	mu    sync.Mutex
	acked bool
	hello *agentlinkpb.SidecarHello
}

func newFakeAttach(exitErr error) *fakeAttach {
	return &fakeAttach{
		exitErr: exitErr, exited: make(chan int32, 1), helloed: make(chan struct{}, 1),
		frames: make(chan *agentlinkpb.ManagerFrame, 4),
	}
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
	select {
	case f := <-s.frames:
		if f == nil {
			return nil, status.Error(codes.Unavailable, "link dropped")
		}
		return f, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeAttach) Send(f *agentlinkpb.SidecarFrame) error {
	switch {
	case f.GetHello() != nil:
		s.mu.Lock()
		s.hello = f.GetHello()
		s.mu.Unlock()
		s.helloed <- struct{}{}
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
		ioSlot:   make(chan struct{}, 1),
		agent:    &AgentSession{IO: agentio.New(), Exited: exited},
	}
	defer c.stopAgent()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.session(ctx, 1, c.cfg.Token.Bearer()); status.Code(err) != codes.Unavailable {
		t.Fatalf("first session ended with %v, want the dropped exit send", err)
	}
	if code := <-link.attach.exited; code != 7 {
		t.Fatalf("first link saw exit %d, want 7", code)
	}

	second := newFakeAttach(nil)
	link.attach = second
	ctx2, cancel2 := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.session(ctx2, 2, c.cfg.Token.Bearer()) }()
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
	if !second.agentRunning() {
		t.Error("the reattach Hello reported no agent while its exit was still unreported")
	}

	third := newFakeAttach(nil)
	link.attach = third
	ctx3, cancel3 := context.WithCancel(ctx)
	go func() { done <- c.session(ctx3, 3, c.cfg.Token.Bearer()) }()
	<-third.helloed
	cancel3()
	<-done
	if third.agentRunning() {
		t.Error("the Hello after the exit was reported still claimed a running agent")
	}
	select {
	case code := <-third.exited:
		t.Errorf("an exit already reported was sent again (%d)", code)
	default:
	}
}

type ioLink struct {
	agentlinkpb.AgentLinkServiceClient
	streams chan *fakeIO
}

func (l *ioLink) AgentIO(ctx context.Context, _ ...grpc.CallOption) (agentlinkpb.AgentLinkService_AgentIOClient, error) {
	s := <-l.streams
	s.ctx = ctx
	return s, nil
}

type fakeIO struct {
	agentlinkpb.AgentLinkService_AgentIOClient
	ctx    context.Context
	frames chan *agentlinkpb.AgentIOFrame
	opened chan uint64
}

func newFakeIO(frames ...*agentlinkpb.AgentIOFrame) *fakeIO {
	s := &fakeIO{frames: make(chan *agentlinkpb.AgentIOFrame, len(frames)), opened: make(chan uint64, 1)}
	for _, f := range frames {
		s.frames <- f
	}
	return s
}

func (s *fakeIO) Send(f *agentlinkpb.AgentIOFrame) error {
	if f.GetOpen() != nil {
		s.opened <- f.GetOpen().GetConsumed()
	}
	return nil
}

func (s *fakeIO) Recv() (*agentlinkpb.AgentIOFrame, error) {
	select {
	case f := <-s.frames:
		return f, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func TestAgentIOResumesOnlyAfterThePreviousPumpReturns(t *testing.T) {
	first := newFakeIO(append([]*agentlinkpb.AgentIOFrame{agentio.OpenFrame(0)},
		agentio.DataFrames([]byte("hello"), 5)...)...)
	second := newFakeIO(agentio.OpenFrame(0))
	link := &ioLink{streams: make(chan *fakeIO, 2)}
	link.streams <- first
	link.streams <- second
	c := &Client{
		log:      slog.New(slog.DiscardHandler),
		client:   link,
		statusCh: make(chan *agentlinkpb.Status, 4),
		ioSlot:   make(chan struct{}, 1),
	}
	ag := &AgentSession{IO: agentio.New()}
	defer ag.IO.Close(nil)
	lost := make(chan error, 2)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		c.pumpAgentIO(ctx1, ag, lost)
	}()
	<-first.opened
	buf := make([]byte, 5)
	if _, err := io.ReadFull(ag.IO.Inbound(), buf[:2]); err != nil {
		t.Fatalf("read the start of the delivery: %v", err)
	}
	cancel1()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go c.pumpAgentIO(ctx2, ag, lost)

	var declared uint64
	opened := false
	select {
	case declared = <-second.opened:
		opened = true
	case <-time.After(200 * time.Millisecond):
	}
	go func() { _, _ = io.ReadFull(ag.IO.Inbound(), buf[2:]) }()
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the previous pump never returned")
	}
	if !opened {
		select {
		case declared = <-second.opened:
		case <-time.After(5 * time.Second):
			t.Fatal("the new AgentIO stream never sent Open")
		}
	}
	if got := ag.IO.Consumed(); declared != got {
		t.Fatalf("Open declared %d consumed bytes, but %d were delivered", declared, got)
	}
}

type delayedStart struct {
	entered chan struct{}
	release chan struct{}
	agent   *AgentSession
}

func (r *delayedStart) Spawn(context.Context) (*AgentSession, error) {
	close(r.entered)
	<-r.release
	return r.agent, nil
}

func TestAnAgentThatStartsAfterShutdownIsStopped(t *testing.T) {
	stopped := make(chan struct{}, 1)
	ag := &AgentSession{IO: agentio.New(), Stop: func() { stopped <- struct{}{} }}
	defer ag.IO.Close(nil)
	r := &delayedStart{entered: make(chan struct{}), release: make(chan struct{}), agent: ag}
	c := &Client{cfg: Config{Runner: r}, log: slog.New(slog.DiscardHandler)}

	pending := c.startSpawn(context.Background())
	<-r.entered
	c.stopAgent()
	close(r.release)
	<-pending.done

	select {
	case <-stopped:
	default:
		t.Error("the agent that started after shutdown was never stopped")
	}
	if c.currentAgent() != nil {
		t.Error("shutdown left the late agent stored")
	}
}

func TestAReattachWatchesAnAgentThatFinishedStartingAfterTheDrop(t *testing.T) {
	exits := make(chan int32, 1)
	r := &delayedStart{
		entered: make(chan struct{}), release: make(chan struct{}),
		agent: &AgentSession{IO: agentio.New(), Exited: exits},
	}
	link := &fakeLink{attach: newFakeAttach(nil)}
	c := &Client{
		cfg:      Config{Token: bearer("Bearer pom_art_test"), Runner: r},
		log:      slog.New(slog.DiscardHandler),
		client:   link,
		statusCh: make(chan *agentlinkpb.Status, 4),
		ioSlot:   make(chan struct{}, 1),
	}
	defer c.stopAgent()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := link.attach
	first.frames <- &agentlinkpb.ManagerFrame{Msg: &agentlinkpb.ManagerFrame_Spawn{Spawn: &agentlinkpb.SpawnAgent{}}}
	done := make(chan error, 1)
	go func() { done <- c.session(ctx, 1, c.cfg.Token.Bearer()) }()
	<-r.entered
	first.frames <- nil
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("first session ended with %v, want the dropped link", err)
	}

	second := newFakeAttach(nil)
	link.attach = second
	go func() { done <- c.session(ctx, 2, c.cfg.Token.Bearer()) }()
	<-second.helloed
	close(r.release)
	exits <- 7

	select {
	case code := <-second.exited:
		if code != 7 {
			t.Errorf("reattached link saw exit %d, want 7", code)
		}
	case <-time.After(5 * time.Second):
		t.Error("the reattached link never watched the agent that finished starting after the drop")
	}
	if !second.agentRunning() {
		t.Error("the reattach Hello reported no agent while its start was still in flight")
	}
	cancel()
	<-done
}
