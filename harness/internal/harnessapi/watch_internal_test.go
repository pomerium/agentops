package harnessapi

import (
	"context"
	"log/slog"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

type revokedRuns struct{ agenticrun.RunClient }

func (revokedRuns) GetRun(context.Context, string) (*agenticrun.RunStatus, error) {
	return &agenticrun.RunStatus{Revoked: true}, nil
}

type quietLauncher struct{ Launcher }

func (quietLauncher) Suspend(context.Context, string) error  { return nil }
func (quietLauncher) Teardown(context.Context, string) error { return nil }
func (quietLauncher) LeaseLength() time.Duration             { return 0 }

type idleSession struct{}

func (idleSession) ID() string { return "acp" }
func (idleSession) Prompt(context.Context, string) (acp.StopReason, error) {
	return acp.StopReasonEndTurn, nil
}
func (idleSession) Cancel(context.Context) error { return nil }
func (idleSession) Close() error                 { return nil }

func attach(svc *Service, b *binding) bool {
	o := &owner{}
	return svc.claim(b.sessionID, o) && svc.register(o, b)
}

func readyBinding(svc *Service, sessionID string) *binding {
	b := newBinding(sessionID, "claim", idleSession{}, newLogSink(svc, sessionID, time.Minute))
	close(b.ready)
	return b
}

func TestAnOldRunWatchLeavesARevivedSessionAlone(t *testing.T) {
	ctx := context.Background()
	st := openStore(t)
	if err := st.CreateSession(ctx, sessionstore.Session{
		ID: "s1", ClientID: "client", ConversationRef: "conversation",
		TemplateName: "deploy", Status: api.StateRunning,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	svc := New(st, NewEventLog(st), quietLauncher{}, nil, revokedRuns{}, WithLogger(slog.New(slog.DiscardHandler)))
	svc.cfg.runWatchInterval = time.Millisecond

	old := readyBinding(svc, "s1")
	if !attach(svc, old) {
		t.Fatal("register the first binding")
	}
	svc.stopSession(ctx, "s1", stopSpec{suspend: api.ReasonIdle, end: api.EndIdle})
	revived := readyBinding(svc, "s1")
	if !attach(svc, revived) {
		t.Fatal("register the revived binding")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.watchRun(ctx, old, "old-run")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the old run watch never stopped")
	}
	if got := svc.lookup("s1"); got != revived {
		t.Errorf("the old run's watch stopped the revived session")
	}
}

func TestAnExitBeforeRegisterStopsTheLaunch(t *testing.T) {
	ctx := context.Background()
	st := openStore(t)
	svc := New(st, NewEventLog(st), quietLauncher{}, nil, nil, WithLogger(slog.New(slog.DiscardHandler)))
	_, o := svc.newLaunch(ctx, "client")
	defer o.cancel()
	if !svc.claim("s1", o) {
		t.Fatal("claim the launch")
	}

	svc.superviseLaunch(ctx, "s1", o)("agent_exited")

	if svc.register(o, readyBinding(svc, "s1")) {
		t.Error("registered an agent that had already exited")
	}
}

func TestAStaleSupervisorLeavesARevivedSessionAlone(t *testing.T) {
	ctx := context.Background()
	svc, _ := runningService(t, quietLauncher{})
	old := svc.lookup("s1").owner
	svc.stopSession(ctx, "s1", stopSpec{suspend: api.ReasonIdle, end: api.EndIdle})
	revived := readyBinding(svc, "s1")
	if !attach(svc, revived) {
		t.Fatal("attach the revived binding")
	}

	svc.superviseLaunch(ctx, "s1", old)("tunnel_lost")

	if got := svc.lookup("s1"); got != revived {
		t.Error("an old supervisor stopped the revived session")
	}
}
