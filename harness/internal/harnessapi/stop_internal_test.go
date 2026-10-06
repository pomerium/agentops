package harnessapi

import (
	"context"
	"errors"
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

func TestAStoppedBindingRunsNoMoreTurns(t *testing.T) {
	svc, _ := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	svc.stopSession(context.Background(), "s1", stopSpec{end: api.EndEnded})

	if b.enter() {
		b.leave()
		t.Error("a stopped binding accepted another turn")
	}
}

func TestNoEventFollowsSessionEnded(t *testing.T) {
	ctx := context.Background()
	svc, _ := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	b.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())
	svc.cfg.sessionIdleTTL = 2 * time.Hour
	svc.cfg.idleWarnLead = 90 * time.Minute

	svc.stopSession(ctx, "s1", stopSpec{end: api.EndEnded})
	svc.mu.Lock()
	svc.owners["s1"] = b.owner
	svc.mu.Unlock()
	svc.sweepIdle(ctx)
	approved := &runOutcome{}
	approved.markApproved()
	svc.reportAttachDelayed(ctx, "s1", "run", approved, time.Minute)

	events, err := svc.events.History(ctx, "s1", 0, 100)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var kinds []string
	for _, ev := range events {
		kinds = append(kinds, api.Kind(ev))
	}
	if kinds[len(kinds)-1] != "session_ended" {
		t.Errorf("events followed session_ended: %v", kinds)
	}
}

type failingStatusStore struct{ sessionstore.Sessions }

func (failingStatusStore) UpdateSessionStatus(context.Context, string, api.SessionState) error {
	return errors.New("the database is unavailable")
}

func TestAnEndTheStoreRefusedIsNotPublished(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})
	svc.store = failingStatusStore{Sessions: st}

	svc.endSession(ctx, "s1", api.StateRunning, api.EndEnded, "")

	events, err := svc.events.History(ctx, "s1", 0, 100)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	for _, ev := range events {
		if ev.GetSessionEnded() != nil || ev.GetStateChanged() != nil {
			t.Errorf("published %s although the store kept the session live", api.Kind(ev))
		}
	}
}

type refusedTeardown struct{ quietLauncher }

func (refusedTeardown) Teardown(context.Context, string) error {
	return errors.New("the workspace could not be deleted")
}

func TestAnEndStaysRetryableUntilTheWorkspaceIsGone(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, refusedTeardown{})

	if svc.stopSession(ctx, "s1", stopSpec{end: api.EndEnded}) {
		t.Error("a stop reported success although the workspace was not deleted")
	}
	row, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if api.Terminal(row.Status) {
		t.Fatalf("the session is %v although its workspace remains", row.Status)
	}

	svc.launcher = quietLauncher{}
	if !svc.stopSession(ctx, "s1", stopSpec{end: api.EndEnded}) {
		t.Error("a retried stop failed after deletion recovered")
	}
	row, err = st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.Status != api.StateEnded {
		t.Errorf("the retried stop left the session %v", row.Status)
	}
}

func TestAFailedLaunchStaysRetryableUntilTheWorkspaceIsGone(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, refusedTeardown{})
	if err := st.UpdateSessionStatus(ctx, "s1", api.StateLaunching); err != nil {
		t.Fatalf("UpdateSessionStatus: %v", err)
	}
	sess, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	svc.failLaunch(ctx, sess, launchOpts{}, &owner{}, "claim", api.EndRunCreateFailed, "")

	row, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if api.Terminal(row.Status) {
		t.Errorf("the failed launch is %v although its workspace remains", row.Status)
	}
}

func TestReconcileLeavesASessionWhoseWorkspaceRemains(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, refusedTeardown{})
	sessions, err := st.ListActiveSessions(ctx)
	if err != nil {
		t.Fatalf("ListActiveSessions: %v", err)
	}
	svc.Shutdown()

	svc.reconcile(ctx, sessions)

	row, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if api.Terminal(row.Status) {
		t.Errorf("reconcile marked the session %v although its workspace remains", row.Status)
	}
}
