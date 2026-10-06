package harnessapi

import (
	"context"
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
			svc.failLaunch(canceled, sess, tc.opts, &runOutcome{}, "", api.EndRevoked, "this session's run was revoked")

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
