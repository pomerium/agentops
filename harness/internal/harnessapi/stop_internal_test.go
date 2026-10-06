package harnessapi

import (
	"context"
	"log/slog"
	"testing"

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
	svc := New(st, NewEventLog(st), l, nil, nil, WithLogger(slog.New(slog.DiscardHandler)))
	if !svc.register(readyBinding(svc, "s1"), &launchSlot{outcome: &runOutcome{}}) {
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
