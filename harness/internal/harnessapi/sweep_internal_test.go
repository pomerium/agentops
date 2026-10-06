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
			_, o := svc.newLaunch(context.Background(), "client")
			if !svc.claim("s1", o) {
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

type cancelOnTeardown struct {
	quietLauncher
	cancel context.CancelFunc
}

func (l cancelOnTeardown) Teardown(context.Context, string) error {
	l.cancel()
	return nil
}

func TestRetentionRecordsTheEndAfterItsSweepIsCanceled(t *testing.T) {
	svc, st := runningService(t, quietLauncher{})
	svc.stopSession(context.Background(), "s1", stopSpec{suspend: api.ReasonIdle, end: api.EndIdle})
	svc.cfg.suspendedTTL = time.Nanosecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.launcher = cancelOnTeardown{cancel: cancel}
	svc.releaseSuspended(ctx, "s1")

	got, err := st.GetSession(context.Background(), "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.Status != api.StateEnded {
		t.Errorf("a released workspace left the session %v, want %v", got.Status, api.StateEnded)
	}
}

func TestRetentionPublishesNoReleaseTheStoreDidNotSave(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})
	svc.stopSession(ctx, "s1", stopSpec{suspend: api.ReasonIdle, end: api.EndIdle})
	svc.cfg.suspendedTTL = time.Nanosecond
	svc.store = failingStatusStore{Sessions: st}

	svc.releaseSuspended(ctx, "s1")

	events, err := svc.events.History(ctx, "s1", 0, 100)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	for _, ev := range events {
		if ev.GetReleased() != nil {
			t.Error("published released although the store kept the session suspended")
		}
	}
}
