package harnessclient

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

type delayedStart struct {
	entered chan struct{}
	release chan struct{}
	agent   *AgentSession
}

func (r *delayedStart) Spawn(context.Context, []byte, *agentlinkpb.SessionParams) (*AgentSession, error) {
	close(r.entered)
	<-r.release
	return r.agent, nil
}

func (r *delayedStart) Join(context.Context) (*AgentSession, error) { return nil, ErrNoAgent }

func TestAnAgentThatStartsAfterShutdownIsStopped(t *testing.T) {
	sent := make(chan *runnerpb.RunnerClientFrame, 4)
	closed := make(chan struct{})
	ag := &AgentSession{
		Send:  func(f *runnerpb.RunnerClientFrame) error { sent <- f; return nil },
		Close: func() { close(closed) },
	}
	r := &delayedStart{entered: make(chan struct{}), release: make(chan struct{}), agent: ag}
	c := &Client{cfg: Config{Runner: r}, log: slog.New(slog.DiscardHandler)}

	pending := c.startSpawn(context.Background(), []byte("s"), &agentlinkpb.SessionParams{})
	<-r.entered
	c.stopAgent()
	close(r.release)
	<-pending.done

	select {
	case f := <-sent:
		if f.GetStop() == nil {
			t.Fatalf("frame sent to the late agent = %v, want Stop", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the agent that started after shutdown was never stopped")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the late agent's runner stream was never closed")
	}
	if c.currentAgent() != nil {
		t.Error("shutdown left the late agent stored")
	}
}

type replayLink struct {
	agentlinkpb.AgentLinkServiceClient
	streams chan *replayIO
}

func (l *replayLink) AgentIO(ctx context.Context, _ ...grpc.CallOption) (agentlinkpb.AgentLinkService_AgentIOClient, error) {
	s := &replayIO{ctx: ctx, states: make(chan *agentlinkpb.AgentState, 4)}
	l.streams <- s
	return s, nil
}

type replayIO struct {
	agentlinkpb.AgentLinkService_AgentIOClient
	ctx    context.Context
	opened bool
	states chan *agentlinkpb.AgentState
}

func (s *replayIO) Recv() (*agentlinkpb.AgentIOFrame, error) {
	if !s.opened {
		s.opened = true
		return &agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Open{Open: &agentlinkpb.AgentIOOpen{StreamId: []byte("s")}}}, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *replayIO) Send(f *agentlinkpb.AgentIOFrame) error {
	if st := f.GetState(); st != nil {
		s.states <- st
	}
	return nil
}

func replayStart(lastTurnSeq uint64) *runnerpb.RunnerServerFrame {
	return &runnerpb.RunnerServerFrame{Msg: &runnerpb.RunnerServerFrame_ReplayStart{ReplayStart: &runnerpb.ReplayStart{
		State: &agentlinkpb.AgentState{LastTurnSeq: lastTurnSeq},
	}}}
}

func TestAReplayStartLeftFromAnEarlierAgentIOIsNotForwarded(t *testing.T) {
	frames := make(chan *runnerpb.RunnerServerFrame, 8)
	replays := make(chan struct{}, 4)
	ag := &AgentSession{
		StreamID: []byte("s"),
		Frames:   frames,
		Send: func(f *runnerpb.RunnerClientFrame) error {
			if f.GetReplay() != nil {
				replays <- struct{}{}
			}
			return nil
		},
	}
	link := &replayLink{streams: make(chan *replayIO, 4)}
	c := &Client{log: slog.New(slog.DiscardHandler), client: link}

	firstCtx, dropFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- c.agentIO(firstCtx, ag) }()
	<-link.streams
	<-replays
	dropFirst()
	<-firstDone
	frames <- replayStart(1)
	frames <- &runnerpb.RunnerServerFrame{Msg: &runnerpb.RunnerServerFrame_Event{Event: &agentlinkpb.AgentEvent{Seq: 1}}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.agentIO(ctx, ag) }()
	defer func() { cancel(); <-done }()
	second := <-link.streams
	<-replays
	frames <- replayStart(2)

	select {
	case st := <-second.states:
		if st.GetLastTurnSeq() != 2 {
			t.Fatalf("the new AgentIO got the state of an earlier replay (last_turn_seq %d), want the answer to its own replay", st.GetLastTurnSeq())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the new AgentIO forwarded no state")
	}
}

type droppedAttach struct {
	agentlinkpb.AgentLinkService_AttachClient
	release chan struct{}
}

func (s *droppedAttach) Recv() (*agentlinkpb.ManagerFrame, error) {
	<-s.release
	return nil, io.EOF
}

func (s *droppedAttach) Send(*agentlinkpb.SidecarFrame) error { return io.ErrClosedPipe }

func attachEndWaiters() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "harnessclient.awaitAttachEnd(")
}

func TestATerminalStatusLeavesNoReceiverBehind(t *testing.T) {
	before := attachEndWaiters()
	stream := &droppedAttach{release: make(chan struct{})}
	t.Cleanup(func() { close(stream.release) })
	c := &Client{log: slog.New(slog.DiscardHandler), statusCh: make(chan *agentlinkpb.Status, 1)}
	c.statusCh <- &agentlinkpb.Status{State: agentlinkpb.Status_STATE_ERROR, Reason: ReasonLocalFailure}

	ctx, cancel := context.WithCancel(context.Background())
	if err := c.serve(ctx, ctx, stream, time.Hour, time.Hour); err == nil {
		t.Fatal("serve returned no error for a terminal status")
	}
	deadline := time.Now().Add(2 * time.Second)
	for attachEndWaiters() <= before {
		if time.Now().After(deadline) {
			t.Fatal("the terminal receiver never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	deadline = time.Now().Add(2 * time.Second)
	for attachEndWaiters() > before {
		if time.Now().After(deadline) {
			t.Fatal("the terminal receiver outlived its canceled session")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStopIsBoundedWhenTheRunnerStopsReading(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	closed := make(chan struct{})
	ag := &AgentSession{
		Send: func(*runnerpb.RunnerClientFrame) error {
			<-release
			return io.ErrClosedPipe
		},
		Close: func() {
			close(closed)
			unblock()
		},
	}
	done := make(chan struct{})
	go func() { stop(ag); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * closeGrace):
		unblock()
		t.Fatal("stop waited for a Stop that the runner never read")
	}
	select {
	case <-closed:
	default:
		t.Fatal("stop did not close the session")
	}
}
