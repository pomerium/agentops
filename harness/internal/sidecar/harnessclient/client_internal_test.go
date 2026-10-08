package harnessclient

import (
	"context"
	"log/slog"
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
