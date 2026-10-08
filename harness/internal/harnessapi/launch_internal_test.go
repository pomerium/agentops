package harnessapi

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/sandbox"
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

type linkAfterEvents struct {
	*sqlite.Store
	appended  chan struct{}
	appendOne sync.Once
}

func (s *linkAfterEvents) AppendPodEvent(ctx context.Context, sessionID, eventType, turnID string, at time.Time, payload []byte, podSeq int64) (int64, error) {
	defer s.appendOne.Do(func() { close(s.appended) })
	return s.Store.AppendPodEvent(ctx, sessionID, eventType, turnID, at, payload, podSeq)
}

func (s *linkAfterEvents) UpdateSessionLink(ctx context.Context, id, executor, streamID string, podSeq int64) error {
	select {
	case <-s.appended:
	case <-time.After(300 * time.Millisecond):
	}
	return s.Store.UpdateSessionLink(ctx, id, executor, streamID, podSeq)
}

type queuedSession struct {
	idleSession
	inbox chan *agentlinkpb.AgentIOFrame
}

func (s queuedSession) StreamID() []byte                        { return []byte("new-stream") }
func (s queuedSession) Inbox() <-chan *agentlinkpb.AgentIOFrame { return s.inbox }

type queuedLauncher struct {
	quietLauncher
	session LiveSession
}

func (l queuedLauncher) Activate(context.Context, *sandbox.Prepared, *sandbox.Attachment) (LiveSession, error) {
	return l.session, nil
}

func TestTheLinkIsSavedBeforeTheConsumerAdvancesThePodSeq(t *testing.T) {
	ctx := context.Background()
	st := &linkAfterEvents{Store: openStore(t), appended: make(chan struct{})}
	sess := sessionstore.Session{
		ID: "s1", ClientID: "client", ConversationRef: "conversation",
		TemplateName: "deploy", Status: api.StateAwaitingApproval,
	}
	if err := st.CreateSession(ctx, sess); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := st.Store.UpdateSessionLink(ctx, "s1", "", "old-stream", 0); err != nil {
		t.Fatalf("UpdateSessionLink: %v", err)
	}
	inbox := make(chan *agentlinkpb.AgentIOFrame, 1)
	inbox <- &agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Event{Event: &agentlinkpb.AgentEvent{
		Seq: 2, Payload: &agentlinkpb.AgentEvent_Thought{Thought: &agentlinkpb.AgentThought{Text: "thinking"}},
	}}}
	svc := New(st, NewEventLog(st), queuedLauncher{session: queuedSession{inbox: inbox}}, nil, revokedRuns{},
		WithLogger(slog.New(slog.DiscardHandler)))
	t.Cleanup(svc.Shutdown)
	o := &owner{outcome: &runOutcome{}}
	if !svc.claim("s1", o) {
		t.Fatal("claim the session")
	}
	prepared := &sandbox.Prepared{ClaimName: "claim", SandboxName: "sandbox", Executor: agenticrun.Executor{
		Namespace: "ns", ServiceAccount: "sa", PodName: "sandbox", PodUID: "uid",
	}}
	if svc.activateAndRun(ctx, sess, launchOpts{}, prepared, &sandbox.Attachment{}, "run-1", o) == nil {
		t.Fatal("activateAndRun failed")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := st.GetSession(ctx, "s1")
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if got.StreamID == "new-stream" && got.PodSeq == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("link = stream %q at pod seq %d, want new-stream at 2", got.StreamID, got.PodSeq)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
