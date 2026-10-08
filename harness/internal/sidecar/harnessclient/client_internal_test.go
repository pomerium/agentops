package harnessclient

import (
	"context"
	"log/slog"
	"testing"
	"time"

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
