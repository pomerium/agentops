package harnessapi

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/sessionstore/sqlite"
)

func openStore(t *testing.T) *sqlite.Store {
	t.Helper()
	st, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestACanceledLaunchStillRecordsItsOutcome(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts launchOpts
		want api.SessionState
	}{
		{"launch", launchOpts{}, api.StateEnded},
		{"revive", launchOpts{revive: true}, api.StateSuspended},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := openStore(t)
			sess := sessionstore.Session{
				ID: "s1", ClientID: "client", ConversationRef: "conversation",
				TemplateName: "deploy", Status: api.StateAwaitingApproval,
			}
			if err := st.CreateSession(ctx, sess); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			svc := New(st, NewEventLog(st), nil, nil, nil, WithLogger(slog.New(slog.DiscardHandler)))

			canceled, cancel := context.WithCancel(ctx)
			cancel()
			svc.failLaunch(canceled, sess, tc.opts, &owner{}, "", api.EndRevoked, "this session's run was revoked")

			got, err := st.GetSession(ctx, sess.ID)
			if err != nil {
				t.Fatalf("GetSession: %v", err)
			}
			if got.Status != tc.want {
				t.Errorf("a launch failed on a canceled context left the session %v, want %v", got.Status, tc.want)
			}
		})
	}
}

type refusedLaunchRead struct{ Store }

func (refusedLaunchRead) GetSession(context.Context, string) (sessionstore.Session, error) {
	return sessionstore.Session{}, errors.New("the session could not be read")
}

func TestAReviveThatCannotReadItsSessionFailsItsTurn(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})
	svc.stopSession(ctx, "s1", stopSpec{suspend: api.ReasonIdle, end: api.EndIdle})
	launchCtx, o := svc.newLaunch(ctx, "client")
	if !svc.claim("s1", o) {
		t.Fatal("claim the revive")
	}
	svc.store = refusedLaunchRead{Store: st}

	svc.launch(launchCtx, o, "s1", launchOpts{revive: true, turnID: "t1", agentPrompt: "carry on"})

	events, err := svc.events.History(ctx, "s1", 0, 100)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	for _, ev := range events {
		if ev.GetTurnId() == "t1" && ev.GetTurnFailed() != nil {
			return
		}
	}
	t.Error("the revive's accepted turn never got TurnFailed")
}
