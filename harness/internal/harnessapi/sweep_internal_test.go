package harnessapi

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

type teardownRecorder struct {
	quietLauncher
	mu      sync.Mutex
	deleted []string
}

func (l *teardownRecorder) Teardown(_ context.Context, claim string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.deleted = append(l.deleted, claim)
	return nil
}

func TestRetentionSparesASessionThatMovedOn(t *testing.T) {
	for _, tc := range []struct {
		name   string
		moveOn func(*testing.T, *Service, sessionstore.Sessions)
	}{
		{"revived", func(t *testing.T, _ *Service, st sessionstore.Sessions) {
			if err := st.UpdateSessionStatus(context.Background(), "s1", api.StateRunning); err != nil {
				t.Fatalf("UpdateSessionStatus: %v", err)
			}
		}},
		{"reviving", func(t *testing.T, svc *Service, _ sessionstore.Sessions) {
			_, slot := svc.newLaunch(context.Background(), "client")
			if !svc.reserve("s1", slot) {
				t.Fatal("reserve the revive's slot")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := openStore(t)
			if err := st.CreateSession(ctx, sessionstore.Session{
				ID: "s1", ClientID: "client", ConversationRef: "conversation",
				TemplateName: "deploy", Status: api.StateSuspended,
			}); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			if err := st.UpdateSessionSandbox(ctx, "s1", "claim", "sandbox", api.StateSuspended); err != nil {
				t.Fatalf("UpdateSessionSandbox: %v", err)
			}
			if err := st.UpdateSessionSuspended(ctx, "s1", api.StateSuspended, time.Now().Add(-time.Hour)); err != nil {
				t.Fatalf("UpdateSessionSuspended: %v", err)
			}
			listed, err := st.ListActiveSessions(ctx)
			if err != nil {
				t.Fatalf("ListActiveSessions: %v", err)
			}
			launcher := &teardownRecorder{}
			svc := New(st, NewEventLog(st), launcher, nil, nil,
				WithSuspendedTTL(time.Minute), WithLogger(slog.New(slog.DiscardHandler)))

			tc.moveOn(t, svc, st)
			svc.sweepSuspended(ctx, listed)

			if len(launcher.deleted) != 0 {
				t.Errorf("retention released %v after the session moved on", launcher.deleted)
			}
			got, err := st.GetSession(ctx, "s1")
			if err != nil {
				t.Fatalf("GetSession: %v", err)
			}
			if api.Terminal(got.Status) {
				t.Errorf("retention ended a session that moved on: %v", got.Status)
			}
		})
	}
}
