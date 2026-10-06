package harnessapi

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

func runningService(t *testing.T, l Launcher) (*Service, sessionstore.Sessions) {
	t.Helper()
	st := openStore(t)
	if err := st.CreateSession(context.Background(), sessionstore.Session{
		ID: "s1", ClientID: "client", ConversationRef: "conversation",
		TemplateName: "deploy", Status: api.StateRunning,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := st.UpdateSessionSandbox(context.Background(), "s1", "claim", "sandbox", api.StateRunning); err != nil {
		t.Fatalf("UpdateSessionSandbox: %v", err)
	}
	svc := New(st, NewEventLog(st), l, nil, nil, WithLogger(slog.New(slog.DiscardHandler)))
	if !attach(svc, readyBinding(svc, "s1")) {
		t.Fatal("register the binding")
	}
	return svc, st
}

func TestACanceledStopStillRecordsTheEnd(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	svc.stopSession(canceled, "s1", stopSpec{end: api.EndEnded})

	got, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.Status != api.StateEnded {
		t.Errorf("a stop on a canceled context left the session %v, want %v", got.Status, api.StateEnded)
	}
}

type gatedSuspend struct {
	quietLauncher
	entered chan struct{}
	release chan struct{}

	mu        sync.Mutex
	teardowns []string
}

func (l *gatedSuspend) Suspend(context.Context, string) error {
	close(l.entered)
	<-l.release
	return nil
}

func (l *gatedSuspend) Teardown(_ context.Context, claim string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.teardowns = append(l.teardowns, claim)
	return nil
}

func TestAnEndDuringASuspendWins(t *testing.T) {
	ctx := context.Background()
	l := &gatedSuspend{entered: make(chan struct{}), release: make(chan struct{})}
	svc, st := runningService(t, l)

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.stopSession(ctx, "s1", stopSpec{suspend: api.ReasonIdle, end: api.EndIdle})
	}()
	<-l.entered
	svc.stopSession(ctx, "s1", stopSpec{end: api.EndEnded})
	close(l.release)
	<-done

	got, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.Status != api.StateEnded {
		t.Errorf("a session ended during its suspend is %v, want %v", got.Status, api.StateEnded)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !slices.Contains(l.teardowns, "claim") {
		t.Errorf("the ended session's workspace was kept: teardowns %v", l.teardowns)
	}
}

type turnOnIdleLog struct {
	slog.Handler
	b *binding
}

func (turnOnIdleLog) Enabled(context.Context, slog.Level) bool { return true }

func (h turnOnIdleLog) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "suspending an idle session" {
		h.b.touch()
	}
	return nil
}

func TestTheIdleSweepRechecksActivityBeforeItStops(t *testing.T) {
	svc, st := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	b.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())
	svc.cfg.sessionIdleTTL = time.Minute
	svc.log = slog.New(turnOnIdleLog{Handler: slog.DiscardHandler, b: b})

	svc.sweepIdle(context.Background())

	got, err := st.GetSession(context.Background(), "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.Status != api.StateRunning {
		t.Errorf("a session that became active was %v, want %v", got.Status, api.StateRunning)
	}
	if svc.lookup("s1") != b {
		t.Error("the idle sweep detached a session that became active")
	}
}
