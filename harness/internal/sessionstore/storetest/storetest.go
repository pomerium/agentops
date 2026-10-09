package storetest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

type Opener func(ctx context.Context) (sessionstore.Store, error)

func Run(t *testing.T, newDatabase func(t *testing.T) Opener) {
	cases := []struct {
		name string
		fn   func(t *testing.T, open Opener)
	}{
		{"Ping", testPing},
		{"Lifecycle", testLifecycle},
		{"CreateSessionStoresCreationFieldsOnly", testCreateSessionStoresCreationFieldsOnly},
		{"OneLiveSessionPerConversation", testOneLiveSessionPerConversation},
		{"DuplicateIDConflicts", testDuplicateIDConflicts},
		{"LivenessFollowsState", testLivenessFollowsState},
		{"StatusRoundTripsEveryState", testStatusRoundTripsEveryState},
		{"LookupsReturnErrNotFound", testLookupsReturnErrNotFound},
		{"LatestSessionByConversation", testLatestSessionByConversation},
		{"UpdatesChangeOnlyTheirFields", testUpdatesChangeOnlyTheirFields},
		{"SessionTimestampsAreSeconds", testSessionTimestampsAreSeconds},
		{"ListOrdering", testListOrdering},
		{"ListSessionsByClientFilters", testListSessionsByClientFilters},
		{"TurnSeqIsMonotonic", testTurnSeqIsMonotonic},
		{"TurnSeqConcurrent", testTurnSeqConcurrent},
		{"EventSeqIsMonotonic", testEventSeqIsMonotonic},
		{"EventSeqConcurrent", testEventSeqConcurrent},
		{"EventSeqSurvivesReopen", testEventSeqSurvivesReopen},
		{"ListSessionEventsFilters", testListSessionEventsFilters},
		{"ListSessionEventsDefaultLimit", testListSessionEventsDefaultLimit},
		{"EventPayloadIsOpaqueBytes", testEventPayloadIsOpaqueBytes},
		{"EventTimestampsKeepMilliseconds", testEventTimestampsKeepMilliseconds},
		{"FinishSessionSavesTheEndWithItsEvents", testFinishSessionSavesTheEndWithItsEvents},
		{"PodSeqOnlyMovesForward", testPodSeqOnlyMovesForward},
		{"RelinkingTheSameStreamKeepsItsPodSeq", testRelinkingTheSameStreamKeepsItsPodSeq},
		{"PodEventAndPodSeqAreOneWrite", testPodEventAndPodSeqAreOneWrite},
		{"PodCommandsOutbox", testPodCommandsOutbox},
		{"PodTurnEndClearsTheTurnsCommands", testPodTurnEndClearsTheTurnsCommands},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, newDatabase(t))
		})
	}
}

func openStore(t *testing.T, open Opener) sessionstore.Store {
	t.Helper()
	s, err := open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func create(t *testing.T, s sessionstore.Store, sess sessionstore.Session) {
	t.Helper()
	if err := s.CreateSession(context.Background(), sess); err != nil {
		t.Fatalf("CreateSession(%s): %v", sess.ID, err)
	}
}

func get(t *testing.T, s sessionstore.Store, id string) sessionstore.Session {
	t.Helper()
	got, err := s.GetSession(context.Background(), id)
	if err != nil {
		t.Fatalf("GetSession(%s): %v", id, err)
	}
	return got
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func nextSecond() time.Time {
	next := time.Now().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(next) + 5*time.Millisecond)
	return next
}

func allStates() []api.SessionState {
	var out []api.SessionState
	values := api.StatePending.Descriptor().Values()
	for i := range values.Len() {
		s := api.SessionState(values.Get(i).Number())
		if s != pb.SessionState_SESSION_STATE_UNSPECIFIED {
			out = append(out, s)
		}
	}
	return out
}

func ids(sessions []sessionstore.Session) []string {
	out := make([]string, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, s.ID)
	}
	return out
}

func wantErr(t *testing.T, what string, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Errorf("%s = %v, want %v", what, err, target)
	}
}

func assertSeconds(t *testing.T, field string, got time.Time) {
	t.Helper()
	if got.Nanosecond() != 0 {
		t.Errorf("%s = %v, want whole seconds", field, got)
	}
}

func testPing(t *testing.T, open Opener) {
	s := openStore(t, open)
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func testLifecycle(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)

	create(t, s, sessionstore.Session{
		ID:              "sess-1",
		ClientID:        "slack",
		ConversationRef: "slack:C1:168.100",
		TemplateName:    "deploy-service",
		TemplateSpec:    `{"v":1}`,
		Status:          api.StatePending,
	})

	got, err := s.GetLiveSessionByConversation(ctx, "slack", "slack:C1:168.100")
	if err != nil {
		t.Fatalf("GetLiveSessionByConversation: %v", err)
	}
	if got.ID != "sess-1" {
		t.Errorf("session mismatch: %+v", got)
	}

	must(t, s.UpdateSessionSandbox(ctx, "sess-1", "claim-1", "sbx-1", api.StateLaunching))
	must(t, s.UpdateSessionACP(ctx, "sess-1", "acp-sess-1", api.StateRunning))
	got = get(t, s, "sess-1")
	if got.SandboxClaimName != "claim-1" || got.SandboxName != "sbx-1" || got.ACPSessionID != "acp-sess-1" || got.Status != api.StateRunning {
		t.Errorf("session updates not persisted: %+v", got)
	}

	active, err := s.ListActiveSessions(ctx)
	must(t, err)
	if len(active) != 1 {
		t.Errorf("expected 1 active session, got %d", len(active))
	}

	must(t, s.UpdateSessionStatus(ctx, "sess-1", api.StateEnded))
	active, err = s.ListActiveSessions(ctx)
	must(t, err)
	if len(active) != 0 {
		t.Errorf("expected 0 active sessions after end, got %d", len(active))
	}
}

func testCreateSessionStoresCreationFieldsOnly(t *testing.T, open Opener) {
	s := openStore(t, open)
	past := time.Unix(1600000000, 0).UTC()
	before := time.Now().Truncate(time.Second)
	create(t, s, sessionstore.Session{
		ID:               "s1",
		ClientID:         "client",
		ConversationRef:  "conv",
		TemplateName:     "tmpl",
		TemplateSpec:     `{"spec":true}`,
		InitialPrompt:    "hello",
		ParentSessionID:  "parent",
		SandboxClaimName: "claim",
		SandboxName:      "sandbox",
		ACPSessionID:     "acp",
		RunID:            "run",
		ApprovalURL:      "https://approve",
		RunExpiresAt:     past,
		ApproverSubject:  "subject",
		EventSeq:         9,
		TurnSeq:          9,
		SuspendedAt:      past,
		LaunchedAt:       past,
		CreatedAt:        past,
		UpdatedAt:        past,
	})
	after := time.Now()
	got := get(t, s, "s1")

	want := sessionstore.Session{
		ID:              "s1",
		ClientID:        "client",
		ConversationRef: "conv",
		TemplateName:    "tmpl",
		TemplateSpec:    `{"spec":true}`,
		InitialPrompt:   "hello",
		ParentSessionID: "parent",
		Status:          api.StatePending,
		LaunchedAt:      got.LaunchedAt,
		CreatedAt:       got.CreatedAt,
		UpdatedAt:       got.UpdatedAt,
	}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("created session (-want +got):\n%s", d)
	}
	if got.CreatedAt.Before(before) || got.CreatedAt.After(after) {
		t.Errorf("CreatedAt = %v, want the store's clock in [%v, %v]", got.CreatedAt, before, after)
	}
	if !got.UpdatedAt.Equal(got.CreatedAt) {
		t.Errorf("UpdatedAt = %v, want CreatedAt %v", got.UpdatedAt, got.CreatedAt)
	}
	if !got.LaunchedAt.Equal(got.CreatedAt) {
		t.Errorf("LaunchedAt = %v, want CreatedAt %v", got.LaunchedAt, got.CreatedAt)
	}

	create(t, s, sessionstore.Session{ID: "s2", ClientID: "client", ConversationRef: "conv2", Status: api.StateSuspended})
	if got := get(t, s, "s2").Status; got != api.StateSuspended {
		t.Errorf("explicit status = %v, want %v", got, api.StateSuspended)
	}
}

func testOneLiveSessionPerConversation(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)

	create(t, s, sessionstore.Session{ID: "a", ClientID: "slack", ConversationRef: "slack:C1:1"})
	second := sessionstore.Session{ID: "b", ClientID: "slack", ConversationRef: "slack:C1:1"}
	wantErr(t, "second live CreateSession", s.CreateSession(ctx, second), sessionstore.ErrConflict)
	_, err := s.GetSession(ctx, "b")
	wantErr(t, "GetSession of the refused session", err, sessionstore.ErrNotFound)
	create(t, s, sessionstore.Session{ID: "c", ClientID: "stub", ConversationRef: "slack:C1:1"})
	create(t, s, sessionstore.Session{ID: "d", ClientID: "slack", ConversationRef: "slack:C1:2"})

	must(t, s.UpdateSessionStatus(ctx, "a", api.StateEnded))
	create(t, s, second)

	must(t, s.UpdateSessionStatus(ctx, "b", api.StateInterrupted))
	create(t, s, sessionstore.Session{ID: "e", ClientID: "slack", ConversationRef: "slack:C1:1"})
}

func testDuplicateIDConflicts(t *testing.T, open Opener) {
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "a", ClientID: "c", ConversationRef: "one", Status: api.StateEnded})
	err := s.CreateSession(context.Background(), sessionstore.Session{ID: "a", ClientID: "c", ConversationRef: "two"})
	wantErr(t, "CreateSession with a duplicate id", err, sessionstore.ErrConflict)
	if got := get(t, s, "a"); got.ConversationRef != "one" {
		t.Errorf("duplicate create overwrote the session: %+v", got)
	}
}

func testLivenessFollowsState(t *testing.T, open Opener) {
	s := openStore(t, open)
	for _, state := range allStates() {
		t.Run(state.String(), func(t *testing.T) {
			ctx := context.Background()
			live := api.Live(state)
			client, id := "c-"+state.String(), "s-"+state.String()
			create(t, s, sessionstore.Session{ID: id, ClientID: client, ConversationRef: "conv", Status: state})

			active, err := s.ListActiveSessions(ctx)
			must(t, err)
			if got := slices.Contains(ids(active), id); got != live {
				t.Errorf("ListActiveSessions contains it = %v, want %v", got, live)
			}

			liveOnly, err := s.ListSessionsByClient(ctx, client, true, time.Time{})
			must(t, err)
			if got := slices.Contains(ids(liveOnly), id); got != live {
				t.Errorf("ListSessionsByClient(liveOnly) contains it = %v, want %v", got, live)
			}
			all, err := s.ListSessionsByClient(ctx, client, false, time.Time{})
			must(t, err)
			if !slices.Equal(ids(all), []string{id}) {
				t.Errorf("ListSessionsByClient(all) = %v, want [%s]", ids(all), id)
			}

			_, err = s.GetLiveSessionByConversation(ctx, client, "conv")
			if live {
				must(t, err)
			} else {
				wantErr(t, "GetLiveSessionByConversation", err, sessionstore.ErrNotFound)
			}
			if got, err := s.GetLatestSessionByConversation(ctx, client, "conv"); err != nil || got.ID != id {
				t.Errorf("GetLatestSessionByConversation = %q, %v", got.ID, err)
			}

			err = s.CreateSession(ctx, sessionstore.Session{ID: id + "-2", ClientID: client, ConversationRef: "conv"})
			if live {
				wantErr(t, "second CreateSession", err, sessionstore.ErrConflict)
			} else {
				must(t, err)
			}
		})
	}
}

func testStatusRoundTripsEveryState(t *testing.T, open Opener) {
	s := openStore(t, open)
	for _, want := range allStates() {
		id := "s-" + want.String()
		create(t, s, sessionstore.Session{ID: id, ClientID: "stub", ConversationRef: id, Status: api.StateEnded})
		must(t, s.UpdateSessionStatus(context.Background(), id, want))
		if got := get(t, s, id).Status; got != want {
			t.Errorf("stored %v, read back %v", want, got)
		}
		id = "c-" + want.String()
		create(t, s, sessionstore.Session{ID: id, ClientID: "stub", ConversationRef: id, Status: want})
		if got := get(t, s, id).Status; got != want {
			t.Errorf("created with %v, read back %v", want, got)
		}
	}
}

func testLookupsReturnErrNotFound(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	_, err := s.GetSession(ctx, "nope")
	wantErr(t, "GetSession", err, sessionstore.ErrNotFound)
	_, err = s.GetLiveSessionByConversation(ctx, "c", "conv")
	wantErr(t, "GetLiveSessionByConversation", err, sessionstore.ErrNotFound)
	_, err = s.GetLatestSessionByConversation(ctx, "c", "conv")
	wantErr(t, "GetLatestSessionByConversation", err, sessionstore.ErrNotFound)
	_, err = s.NextTurnSeq(ctx, "nope")
	wantErr(t, "NextTurnSeq", err, sessionstore.ErrNotFound)
	_, err = s.AppendSessionEvent(ctx, "nope", "x", "", time.Now(), nil)
	wantErr(t, "AppendSessionEvent", err, sessionstore.ErrNotFound)
	evs, err := s.ListSessionEvents(ctx, "nope", 0, 10)
	if err != nil || len(evs) != 0 {
		t.Errorf("ListSessionEvents(unknown) = %v, %v, want empty", evs, err)
	}

	create(t, s, sessionstore.Session{ID: "s1", ClientID: "c", ConversationRef: "conv"})
	_, err = s.GetLiveSessionByConversation(ctx, "other", "conv")
	wantErr(t, "another client's GetLiveSessionByConversation", err, sessionstore.ErrNotFound)
	_, err = s.GetLatestSessionByConversation(ctx, "other", "conv")
	wantErr(t, "another client's GetLatestSessionByConversation", err, sessionstore.ErrNotFound)
}

func testLatestSessionByConversation(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	latest := func(want string) {
		t.Helper()
		got, err := s.GetLatestSessionByConversation(ctx, "c", "conv")
		if err != nil || got.ID != want {
			t.Errorf("GetLatestSessionByConversation = %q, %v, want %q", got.ID, err, want)
		}
	}

	create(t, s, sessionstore.Session{ID: "a", ClientID: "c", ConversationRef: "conv", Status: api.StateEnded})
	nextSecond()
	create(t, s, sessionstore.Session{ID: "b", ClientID: "c", ConversationRef: "conv", Status: api.StateInterrupted})
	latest("b")
	_, err := s.GetLiveSessionByConversation(ctx, "c", "conv")
	wantErr(t, "GetLiveSessionByConversation with only terminal sessions", err, sessionstore.ErrNotFound)

	nextSecond()
	must(t, s.UpdateSessionStatus(ctx, "a", api.StateEnded))
	latest("b")

	create(t, s, sessionstore.Session{ID: "c", ClientID: "c", ConversationRef: "conv"})
	latest("c")
	if got, err := s.GetLiveSessionByConversation(ctx, "c", "conv"); err != nil || got.ID != "c" {
		t.Errorf("GetLiveSessionByConversation = %q, %v, want c", got.ID, err)
	}
}

func testUpdatesChangeOnlyTheirFields(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	base := time.Unix(1700000000, 0).UTC()
	later := time.Unix(1800000000, 0).UTC()

	cases := []struct {
		name           string
		update         func(id string) error
		apply          func(*sessionstore.Session)
		bumpsUpdatedAt bool
	}{
		{
			"UpdateSessionSandbox",
			func(id string) error { return s.UpdateSessionSandbox(ctx, id, "claim-2", "sbx-2", api.StateLaunching) },
			func(s *sessionstore.Session) {
				s.SandboxClaimName, s.SandboxName, s.Status = "claim-2", "sbx-2", api.StateLaunching
			},
			true,
		},
		{
			"UpdateSessionACP",
			func(id string) error { return s.UpdateSessionACP(ctx, id, "acp-2", api.StatePending) },
			func(s *sessionstore.Session) { s.ACPSessionID, s.Status = "acp-2", api.StatePending },
			true,
		},
		{
			"UpdateSessionStatus",
			func(id string) error { return s.UpdateSessionStatus(ctx, id, api.StateEnded) },
			func(s *sessionstore.Session) { s.Status = api.StateEnded },
			true,
		},
		{
			"UpdateSessionRun",
			func(id string) error {
				return s.UpdateSessionRun(ctx, id, "run-2", "https://approve/2", later, api.StateAwaitingApproval)
			},
			func(s *sessionstore.Session) {
				s.RunID, s.ApprovalURL, s.RunExpiresAt, s.Status = "run-2", "https://approve/2", later, api.StateAwaitingApproval
			},
			true,
		},
		{
			"UpdateSessionRunExpiry",
			func(id string) error { return s.UpdateSessionRunExpiry(ctx, id, later) },
			func(s *sessionstore.Session) { s.RunExpiresAt = later },
			true,
		},
		{
			"UpdateSessionApprover",
			func(id string) error { return s.UpdateSessionApprover(ctx, id, "subject-2") },
			func(s *sessionstore.Session) { s.ApproverSubject = "subject-2" },
			true,
		},
		{
			"UpdateSessionSuspended",
			func(id string) error { return s.UpdateSessionSuspended(ctx, id, api.StateSuspended, later) },
			func(s *sessionstore.Session) { s.Status, s.SuspendedAt = api.StateSuspended, later },
			true,
		},
		{
			"UpdateSessionLaunched",
			func(id string) error { return s.UpdateSessionLaunched(ctx, id, api.StateLaunching, later) },
			func(s *sessionstore.Session) { s.Status, s.LaunchedAt = api.StateLaunching, later },
			true,
		},
		{
			"NextTurnSeq",
			func(id string) error { _, err := s.NextTurnSeq(ctx, id); return err },
			func(s *sessionstore.Session) { s.TurnSeq++ },
			false,
		},
		{
			"AppendSessionEvent",
			func(id string) error { _, err := s.AppendSessionEvent(ctx, id, "x", "", time.Now(), nil); return err },
			func(s *sessionstore.Session) { s.EventSeq++ },
			false,
		},
		{
			"UpdateSessionLink",
			func(id string) error { return s.UpdateSessionLink(ctx, id, "exec-2", "stream-2", 9) },
			func(s *sessionstore.Session) { s.Executor, s.StreamID, s.PodSeq = "exec-2", "stream-2", 9 },
			true,
		},
		{
			"AdvancePodSeq",
			func(id string) error { return s.AdvancePodSeq(ctx, id, 7) },
			func(s *sessionstore.Session) { s.PodSeq = 7 },
			false,
		},
		{
			"AppendPodEvent",
			func(id string) error { _, err := s.AppendPodEvent(ctx, id, "x", "", time.Now(), nil, 8); return err },
			func(s *sessionstore.Session) { s.EventSeq++; s.PodSeq = 8 },
			false,
		},
	}

	for i := range cases {
		id := fmt.Sprintf("s%d", i)
		create(t, s, sessionstore.Session{
			ID: id, ClientID: "c", ConversationRef: id, TemplateName: "tmpl", TemplateSpec: "{}",
			InitialPrompt: "hi", ParentSessionID: "parent",
		})
		must(t, s.UpdateSessionSandbox(ctx, id, "claim", "sbx", api.StateLaunching))
		must(t, s.UpdateSessionRun(ctx, id, "run", "https://approve", base, api.StateAwaitingApproval))
		must(t, s.UpdateSessionACP(ctx, id, "acp", api.StateRunning))
		must(t, s.UpdateSessionApprover(ctx, id, "subject"))
		must(t, s.UpdateSessionSuspended(ctx, id, api.StateSuspended, base))
		must(t, s.UpdateSessionStatus(ctx, id, api.StateRunning))
		must(t, s.UpdateSessionLink(ctx, id, "exec", "stream", 3))
		_, err := s.NextTurnSeq(ctx, id)
		must(t, err)
		_, err = s.AppendSessionEvent(ctx, id, "x", "", time.Now(), nil)
		must(t, err)
	}

	tick := nextSecond()
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id := fmt.Sprintf("s%d", i)
			want := get(t, s, id)
			must(t, c.update(id))
			got := get(t, s, id)
			c.apply(&want)
			if c.bumpsUpdatedAt {
				if got.UpdatedAt.Before(tick) {
					t.Errorf("UpdatedAt = %v, want at or after %v", got.UpdatedAt, tick)
				}
				want.UpdatedAt = got.UpdatedAt
			}
			if d := cmp.Diff(want, got); d != "" {
				t.Errorf("session after %s (-want +got):\n%s", c.name, d)
			}
		})
	}
}

func testSessionTimestampsAreSeconds(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	at := time.Date(2026, 1, 2, 3, 4, 5, 987654321, time.FixedZone("X", 3600))
	want := at.Truncate(time.Second)

	create(t, s, sessionstore.Session{ID: "s1", ClientID: "c", ConversationRef: "conv"})
	got := get(t, s, "s1")
	assertSeconds(t, "CreatedAt", got.CreatedAt)
	assertSeconds(t, "UpdatedAt", got.UpdatedAt)
	assertSeconds(t, "LaunchedAt", got.LaunchedAt)
	if !got.RunExpiresAt.IsZero() || !got.SuspendedAt.IsZero() {
		t.Errorf("unset timestamps = %v, %v, want zero", got.RunExpiresAt, got.SuspendedAt)
	}

	must(t, s.UpdateSessionRun(ctx, "s1", "run", "url", at, api.StateAwaitingApproval))
	must(t, s.UpdateSessionLaunched(ctx, "s1", api.StateLaunching, at))
	must(t, s.UpdateSessionSuspended(ctx, "s1", api.StateSuspended, at))
	got = get(t, s, "s1")
	for name, v := range map[string]time.Time{"RunExpiresAt": got.RunExpiresAt, "SuspendedAt": got.SuspendedAt, "LaunchedAt": got.LaunchedAt, "UpdatedAt": got.UpdatedAt} {
		assertSeconds(t, name, v)
	}
	if !got.RunExpiresAt.Equal(want) || !got.SuspendedAt.Equal(want) || !got.LaunchedAt.Equal(want) {
		t.Errorf("RunExpiresAt, SuspendedAt, LaunchedAt = %v, %v, %v, want %v", got.RunExpiresAt, got.SuspendedAt, got.LaunchedAt, want)
	}

	must(t, s.UpdateSessionRunExpiry(ctx, "s1", time.Time{}))
	must(t, s.UpdateSessionSuspended(ctx, "s1", api.StateRunning, time.Time{}))
	got = get(t, s, "s1")
	if !got.RunExpiresAt.IsZero() || !got.SuspendedAt.IsZero() {
		t.Errorf("zero timestamps read back as %v, %v", got.RunExpiresAt, got.SuspendedAt)
	}
}

func testListOrdering(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "a", ClientID: "c", ConversationRef: "1"})
	create(t, s, sessionstore.Session{ID: "x", ClientID: "other", ConversationRef: "1", Status: api.StateEnded})
	nextSecond()
	create(t, s, sessionstore.Session{ID: "b", ClientID: "other", ConversationRef: "2"})
	create(t, s, sessionstore.Session{ID: "c", ClientID: "c", ConversationRef: "3", Status: api.StateEnded})

	active, err := s.ListActiveSessions(ctx)
	must(t, err)
	if got, want := ids(active), []string{"a", "b"}; !slices.Equal(got, want) {
		t.Errorf("ListActiveSessions = %v, want %v (oldest first)", got, want)
	}
	byClient, err := s.ListSessionsByClient(ctx, "c", false, time.Time{})
	must(t, err)
	if got, want := ids(byClient), []string{"c", "a"}; !slices.Equal(got, want) {
		t.Errorf("ListSessionsByClient = %v, want %v (newest first)", got, want)
	}
	other, err := s.ListSessionsByClient(ctx, "other", false, time.Time{})
	must(t, err)
	if got, want := ids(other), []string{"b", "x"}; !slices.Equal(got, want) {
		t.Errorf("ListSessionsByClient(other) = %v, want %v", got, want)
	}
	none, err := s.ListSessionsByClient(ctx, "nobody", false, time.Time{})
	if err != nil || len(none) != 0 {
		t.Errorf("ListSessionsByClient(nobody) = %v, %v, want empty", ids(none), err)
	}
}

func testListSessionsByClientFilters(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	list := func(liveOnly bool, since time.Time, want ...string) {
		t.Helper()
		got, err := s.ListSessionsByClient(ctx, "c", liveOnly, since)
		if err != nil {
			t.Fatalf("ListSessionsByClient: %v", err)
		}
		if !slices.Equal(ids(got), want) {
			t.Errorf("ListSessionsByClient(liveOnly=%v, since=%v) = %v, want %v", liveOnly, since, ids(got), want)
		}
	}

	create(t, s, sessionstore.Session{ID: "a", ClientID: "c", ConversationRef: "1"})
	create(t, s, sessionstore.Session{ID: "b", ClientID: "c", ConversationRef: "2", Status: api.StateEnded})
	t1 := nextSecond()
	create(t, s, sessionstore.Session{ID: "c", ClientID: "c", ConversationRef: "3"})
	cUpdated := get(t, s, "c").UpdatedAt

	list(false, time.Time{}, "c", "a", "b")
	list(true, time.Time{}, "c", "a")
	list(false, t1, "c")
	list(true, t1, "c")
	list(false, cUpdated, "c")
	list(false, cUpdated.Add(999*time.Millisecond), "c")
	list(false, cUpdated.Add(time.Second))

	must(t, s.UpdateSessionStatus(ctx, "a", api.StateEnded))
	list(false, t1, "c", "a")
	list(true, t1, "c")
	list(true, time.Time{}, "c")
}

func testTurnSeqIsMonotonic(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	create(t, s, sessionstore.Session{ID: "s2", ClientID: "stub", ConversationRef: "c2"})
	for i := range 3 {
		n, err := s.NextTurnSeq(ctx, "s1")
		must(t, err)
		if want := int64(i + 1); n != want {
			t.Fatalf("turn seq = %d, want %d", n, want)
		}
	}
	if n, err := s.NextTurnSeq(ctx, "s2"); err != nil || n != 1 {
		t.Errorf("another session's first turn seq = %d, %v, want 1", n, err)
	}
	if _, err := s.AppendSessionEvent(ctx, "s1", "x", "", time.Now(), nil); err != nil {
		t.Fatal(err)
	}
	must(t, s.UpdateSessionStatus(ctx, "s1", api.StateSuspended))
	must(t, s.UpdateSessionStatus(ctx, "s1", api.StateEnded))
	if n, err := s.NextTurnSeq(ctx, "s1"); err != nil || n != 4 {
		t.Errorf("turn seq after events and status changes = %d, %v, want 4", n, err)
	}
	if got := get(t, s, "s1").TurnSeq; got != 4 {
		t.Errorf("Session.TurnSeq = %d, want 4", got)
	}
}

func testTurnSeqConcurrent(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	const workers, each = 8, 25
	seqs := concurrently(t, workers, each, func(int, int) (int64, error) { return s.NextTurnSeq(ctx, "s1") })
	assertDense(t, seqs, workers*each)
	if got := get(t, s, "s1").TurnSeq; got != workers*each {
		t.Errorf("Session.TurnSeq = %d, want %d", got, workers*each)
	}
}

func concurrently(t *testing.T, workers, each int, fn func(w, i int) (int64, error)) []int64 {
	t.Helper()
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		out  []int64
		errs []error
	)
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				n, err := fn(w, i)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					out = append(out, n)
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertDense(t *testing.T, seqs []int64, n int) {
	t.Helper()
	got := slices.Clone(seqs)
	slices.Sort(got)
	if len(got) != n {
		t.Fatalf("allocated %d sequences, want %d", len(got), n)
	}
	for i, v := range got {
		if v != int64(i+1) {
			t.Fatalf("sorted sequences[%d] = %d, want %d (duplicate or gap)", i, v, i+1)
		}
	}
}

func testEventSeqIsMonotonic(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	create(t, s, sessionstore.Session{ID: "s2", ClientID: "stub", ConversationRef: "c2"})
	_, err := s.NextTurnSeq(ctx, "s1")
	must(t, err)
	for i := range 3 {
		seq, err := s.AppendSessionEvent(ctx, "s1", "state_changed", "", time.Now(), nil)
		must(t, err)
		if want := int64(i + 1); seq != want {
			t.Fatalf("seq = %d, want %d", seq, want)
		}
	}
	if seq, err := s.AppendSessionEvent(ctx, "s2", "state_changed", "", time.Now(), nil); err != nil || seq != 1 {
		t.Errorf("another session's first seq = %d, %v, want 1", seq, err)
	}
	must(t, s.UpdateSessionStatus(ctx, "s1", api.StateEnded))
	if seq, err := s.AppendSessionEvent(ctx, "s1", "state_changed", "", time.Now(), nil); err != nil || seq != 4 {
		t.Errorf("seq after a status change = %d, %v, want 4", seq, err)
	}
	if got := get(t, s, "s1").EventSeq; got != 4 {
		t.Errorf("Session.EventSeq = %d, want 4", got)
	}
}

func testEventSeqConcurrent(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	create(t, s, sessionstore.Session{ID: "s2", ClientID: "stub", ConversationRef: "c2"})
	const workers, each = 8, 25

	var (
		mu      sync.Mutex
		written = map[string]map[int64]string{"s1": {}, "s2": {}}
	)
	concurrently(t, workers, each, func(w, i int) (int64, error) {
		session := []string{"s1", "s2"}[w%2]
		payload := fmt.Sprintf("%s/%d/%d", session, w, i)
		seq, err := s.AppendSessionEvent(ctx, session, "x", "", time.Now(), []byte(payload))
		if err == nil {
			mu.Lock()
			written[session][seq] = payload
			mu.Unlock()
		}
		return seq, err
	})
	if n := len(written["s1"]) + len(written["s2"]); n != workers*each {
		t.Fatalf("%d distinct sequences for %d appends: a sequence was handed out twice", n, workers*each)
	}

	for session, bySeq := range written {
		assertDense(t, slices.Collect(maps.Keys(bySeq)), len(bySeq))
		evs, err := s.ListSessionEvents(ctx, session, 0, 1000)
		must(t, err)
		if len(evs) != len(bySeq) {
			t.Fatalf("%s: read back %d events, want %d", session, len(evs), len(bySeq))
		}
		for i, ev := range evs {
			if ev.Seq != int64(i+1) {
				t.Fatalf("%s: event %d has seq %d", session, i, ev.Seq)
			}
			if string(ev.Payload) != bySeq[ev.Seq] {
				t.Errorf("%s: seq %d holds %q, but the append that got it wrote %q", session, ev.Seq, ev.Payload, bySeq[ev.Seq])
			}
		}
		if got := get(t, s, session).EventSeq; got != int64(len(bySeq)) {
			t.Errorf("%s: Session.EventSeq = %d, want %d", session, got, len(bySeq))
		}
	}
}

func testEventSeqSurvivesReopen(t *testing.T, open Opener) {
	ctx := context.Background()
	s, err := open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	at := time.Unix(1700000000, 123000000).UTC()
	for i := range 3 {
		seq, err := s.AppendSessionEvent(ctx, "s1", "state_changed", "", at, []byte(`{"n":1}`))
		must(t, err)
		if want := int64(i + 1); seq != want {
			t.Fatalf("seq = %d, want %d", seq, want)
		}
	}
	_, err = s.NextTurnSeq(ctx, "s1")
	must(t, err)
	must(t, s.Close())

	s2 := openStore(t, open)
	seq, err := s2.AppendSessionEvent(ctx, "s1", "state_changed", "", at, nil)
	must(t, err)
	if seq != 4 {
		t.Fatalf("seq after reopen = %d, want 4", seq)
	}
	if n, err := s2.NextTurnSeq(ctx, "s1"); err != nil || n != 2 {
		t.Fatalf("turn seq after reopen = %d, %v, want 2", n, err)
	}

	evs, err := s2.ListSessionEvents(ctx, "s1", 0, 100)
	must(t, err)
	if len(evs) != 4 {
		t.Fatalf("read back %d events, want 4", len(evs))
	}
	if !evs[0].At.Equal(at) {
		t.Errorf("timestamp round-trip lost precision: got %v, want %v", evs[0].At, at)
	}
	if string(evs[0].Payload) != `{"n":1}` {
		t.Errorf("payload round-trip = %q", evs[0].Payload)
	}
}

func testListSessionEventsFilters(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	create(t, s, sessionstore.Session{ID: "s2", ClientID: "stub", ConversationRef: "c2"})
	base := time.Unix(1700000000, 0).UTC()
	for i := range 5 {
		_, err := s.AppendSessionEvent(ctx, "s1", fmt.Sprintf("type-%d", i+1), fmt.Sprintf("turn-%d", i+1), base.Add(time.Duration(i)*time.Second), []byte{byte(i + 1)})
		must(t, err)
		_, err = s.AppendSessionEvent(ctx, "s2", "other", "", base, []byte{0xee})
		must(t, err)
	}
	seqs := func(evs []sessionstore.SessionEvent) []int64 {
		out := make([]int64, 0, len(evs))
		for _, ev := range evs {
			out = append(out, ev.Seq)
		}
		return out
	}
	check := func(afterSeq int64, limit int, want ...int64) {
		t.Helper()
		evs, err := s.ListSessionEvents(ctx, "s1", afterSeq, limit)
		must(t, err)
		if got := seqs(evs); !slices.Equal(got, want) {
			t.Errorf("ListSessionEvents(after=%d, limit=%d) = %v, want %v", afterSeq, limit, got, want)
		}
	}
	check(0, 10, 1, 2, 3, 4, 5)
	check(2, 10, 3, 4, 5)
	check(0, 2, 1, 2)
	check(1, 3, 2, 3, 4)
	check(5, 10)
	check(99, 10)

	evs, err := s.ListSessionEvents(ctx, "s1", 2, 1)
	must(t, err)
	want := []sessionstore.SessionEvent{{SessionID: "s1", Seq: 3, Type: "type-3", TurnID: "turn-3", At: base.Add(2 * time.Second), Payload: []byte{3}}}
	if d := cmp.Diff(want, evs); d != "" {
		t.Errorf("events (-want +got):\n%s", d)
	}
}

func testListSessionEventsDefaultLimit(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	for range sessionstore.DefaultEventPage + 1 {
		_, err := s.AppendSessionEvent(ctx, "s1", "x", "", time.Now(), nil)
		must(t, err)
	}
	for _, limit := range []int{0, -1} {
		evs, err := s.ListSessionEvents(ctx, "s1", 0, limit)
		must(t, err)
		if len(evs) != sessionstore.DefaultEventPage || evs[len(evs)-1].Seq != sessionstore.DefaultEventPage {
			t.Errorf("ListSessionEvents(limit=%d) returned %d events, want the first %d", limit, len(evs), sessionstore.DefaultEventPage)
		}
	}
}

func testEventPayloadIsOpaqueBytes(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	payloads := [][]byte{{0x52, 0x00, 0xff, 0xfe, 0x0a, 0x00}, nil, {}}
	for _, p := range payloads {
		_, err := s.AppendSessionEvent(ctx, "s1", "state_changed", "", time.Now(), p)
		must(t, err)
	}
	evs, err := s.ListSessionEvents(ctx, "s1", 0, 10)
	must(t, err)
	if len(evs) != len(payloads) {
		t.Fatalf("read back %d events, want %d", len(evs), len(payloads))
	}
	for i, p := range payloads {
		if string(evs[i].Payload) != string(p) {
			t.Errorf("payload %d round trip = %x, want %x", i, evs[i].Payload, p)
		}
	}
}

func testEventTimestampsKeepMilliseconds(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	at := time.Date(2026, 1, 2, 3, 4, 5, 123000000, time.FixedZone("X", 3600))
	_, err := s.AppendSessionEvent(ctx, "s1", "x", "", at, nil)
	must(t, err)
	evs, err := s.ListSessionEvents(ctx, "s1", 0, 1)
	must(t, err)
	if len(evs) != 1 || !evs[0].At.Equal(at) {
		t.Errorf("events = %+v, want one at %v", evs, at)
	}
}

func testFinishSessionSavesTheEndWithItsEvents(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	_, err := s.AppendSessionEvent(ctx, "s1", "state_changed", "", time.Now(), nil)
	must(t, err)

	at := time.Unix(1700000000, 123*int64(time.Millisecond)).UTC()
	seqs, err := s.FinishSession(ctx, "s1", api.StateEnded, []sessionstore.NewSessionEvent{
		{Type: "state_changed", At: at, Payload: []byte{1}},
		{Type: "session_ended", TurnID: "", At: at, Payload: []byte{2}},
	})
	must(t, err)
	if !slices.Equal(seqs, []int64{2, 3}) {
		t.Errorf("FinishSession seqs = %v, want [2 3]", seqs)
	}
	got := get(t, s, "s1")
	if got.Status != api.StateEnded || got.EventSeq != 3 {
		t.Errorf("after FinishSession: status %v, event seq %d; want ended, 3", got.Status, got.EventSeq)
	}
	evs, err := s.ListSessionEvents(ctx, "s1", 1, 10)
	must(t, err)
	want := []sessionstore.SessionEvent{
		{SessionID: "s1", Seq: 2, Type: "state_changed", At: at, Payload: []byte{1}},
		{SessionID: "s1", Seq: 3, Type: "session_ended", At: at, Payload: []byte{2}},
	}
	if d := cmp.Diff(want, evs); d != "" {
		t.Errorf("events (-want +got):\n%s", d)
	}

	_, err = s.FinishSession(ctx, "s1", api.StateInterrupted, []sessionstore.NewSessionEvent{{Type: "session_ended", At: at}})
	wantErr(t, "FinishSession on a finished session", err, sessionstore.ErrConflict)
	if got := get(t, s, "s1"); got.Status != api.StateEnded || got.EventSeq != 3 {
		t.Errorf("a refused FinishSession changed the session: status %v, event seq %d", got.Status, got.EventSeq)
	}

	_, err = s.FinishSession(ctx, "missing", api.StateEnded, nil)
	wantErr(t, "FinishSession on an unknown session", err, sessionstore.ErrNotFound)
}

func testRelinkingTheSameStreamKeepsItsPodSeq(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	must(t, s.UpdateSessionLink(ctx, "s1", "exec", "stream-1", 4))
	_, err := s.AppendPodEvent(ctx, "s1", "x", "", time.Now(), nil, 6)
	must(t, err)
	must(t, s.UpdateSessionLink(ctx, "s1", "exec", "stream-1", 4))
	if got := get(t, s, "s1").PodSeq; got != 6 {
		t.Errorf("pod seq after linking the same stream at 4 = %d, want 6", got)
	}
	must(t, s.UpdateSessionLink(ctx, "s1", "exec", "stream-2", 1))
	if got := get(t, s, "s1"); got.PodSeq != 1 || got.StreamID != "stream-2" {
		t.Errorf("after linking a new stream at 1: stream %q pod seq %d, want stream-2 at 1", got.StreamID, got.PodSeq)
	}
}

func testPodSeqOnlyMovesForward(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	must(t, s.UpdateSessionLink(ctx, "s1", "exec", "stream", 4))
	must(t, s.AdvancePodSeq(ctx, "s1", 6))
	must(t, s.AdvancePodSeq(ctx, "s1", 5))
	if got := get(t, s, "s1").PodSeq; got != 6 {
		t.Errorf("pod seq after advancing to 6 then 5 = %d, want 6", got)
	}
	_, err := s.AppendPodEvent(ctx, "s1", "x", "", time.Now(), nil, 2)
	must(t, err)
	if got := get(t, s, "s1").PodSeq; got != 6 {
		t.Errorf("pod seq after an event from pod seq 2 = %d, want 6", got)
	}
	wantErr(t, "AdvancePodSeq on an unknown session", s.AdvancePodSeq(ctx, "missing", 1), sessionstore.ErrNotFound)
	_, err = s.AppendPodEvent(ctx, "missing", "x", "", time.Now(), nil, 1)
	wantErr(t, "AppendPodEvent on an unknown session", err, sessionstore.ErrNotFound)
}

func testPodEventAndPodSeqAreOneWrite(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	at := time.Unix(1700000000, 0).UTC()
	seq, err := s.AppendPodEvent(ctx, "s1", "agent_message", "t1", at, []byte{7}, 12)
	must(t, err)
	got := get(t, s, "s1")
	if seq != 1 || got.EventSeq != 1 || got.PodSeq != 12 {
		t.Fatalf("after AppendPodEvent: seq %d, event seq %d, pod seq %d; want 1, 1, 12", seq, got.EventSeq, got.PodSeq)
	}
	evs, err := s.ListSessionEvents(ctx, "s1", 0, 10)
	must(t, err)
	want := []sessionstore.SessionEvent{{SessionID: "s1", Seq: 1, Type: "agent_message", TurnID: "t1", At: at, Payload: []byte{7}}}
	if d := cmp.Diff(want, evs); d != "" {
		t.Errorf("events (-want +got):\n%s", d)
	}
}

func testPodCommandsOutbox(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	create(t, s, sessionstore.Session{ID: "s2", ClientID: "stub", ConversationRef: "c2"})

	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s1", Kind: "prompt", Key: "t1", TurnID: "t1", Payload: []byte{1}}))
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s1", Kind: "permission", Key: "c1", TurnID: "t1", Payload: []byte{2}}))
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s1", Kind: "prompt", Key: "t2", TurnID: "t2", Payload: []byte{3}}))
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s1", Kind: "permission", Key: "c1", TurnID: "t1", Payload: []byte{4}}))
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s2", Kind: "prompt", Key: "t1", TurnID: "t1", Payload: []byte{5}}))

	got, err := s.ListPodCommands(ctx, "s1")
	must(t, err)
	want := []sessionstore.PodCommand{
		{SessionID: "s1", Kind: "prompt", Key: "t1", TurnID: "t1", Payload: []byte{1}},
		{SessionID: "s1", Kind: "permission", Key: "c1", TurnID: "t1", Payload: []byte{4}},
		{SessionID: "s1", Kind: "prompt", Key: "t2", TurnID: "t2", Payload: []byte{3}},
	}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("commands of s1 (-want +got):\n%s", d)
	}

	must(t, s.DeletePodCommandsForTurn(ctx, "s1", "t1"))
	must(t, s.DeletePodCommand(ctx, "s1", "prompt", "missing"))
	got, err = s.ListPodCommands(ctx, "s1")
	must(t, err)
	if len(got) != 1 || got[0].Key != "t2" {
		t.Errorf("commands after deleting turn t1 = %+v, want only t2", got)
	}

	_, err = s.FinishSession(ctx, "s1", api.StateEnded, nil)
	must(t, err)
	got, err = s.ListPodCommands(ctx, "s1")
	must(t, err)
	if len(got) != 0 {
		t.Errorf("an ended session kept its commands: %+v", got)
	}
	got, err = s.ListPodCommands(ctx, "s2")
	must(t, err)
	if len(got) != 1 {
		t.Errorf("ending s1 changed the commands of s2: %+v", got)
	}
	must(t, s.DeletePodCommand(ctx, "s2", "prompt", "t1"))
	if got, _ := s.ListPodCommands(ctx, "s2"); len(got) != 0 {
		t.Errorf("DeletePodCommand left %+v", got)
	}

	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s2", Kind: "prompt", Key: "t3", TurnID: "t3"}))
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s2", Kind: "permission", Key: "c3", TurnID: "t3"}))
	must(t, s.DeletePodCommands(ctx, "s2"))
	if got, _ := s.ListPodCommands(ctx, "s2"); len(got) != 0 {
		t.Errorf("DeletePodCommands left %+v", got)
	}
}

func testPodTurnEndClearsTheTurnsCommands(t *testing.T, open Opener) {
	ctx := context.Background()
	s := openStore(t, open)
	create(t, s, sessionstore.Session{ID: "s1", ClientID: "stub", ConversationRef: "c1"})
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s1", Kind: "prompt", Key: "t1", TurnID: "t1"}))
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s1", Kind: "permission", Key: "r1", TurnID: "t1"}))
	must(t, s.PutPodCommand(ctx, sessionstore.PodCommand{SessionID: "s1", Kind: "prompt", Key: "t2", TurnID: "t2"}))
	seq, err := s.AppendPodTurnEnd(ctx, "s1", "turn_completed", "t1", time.Now(), nil, 9)
	must(t, err)
	if seq != 1 {
		t.Errorf("event seq = %d, want 1", seq)
	}
	cmds, err := s.ListPodCommands(ctx, "s1")
	must(t, err)
	if len(cmds) != 1 || cmds[0].TurnID != "t2" {
		t.Errorf("commands after the end of t1 = %+v, want only t2's", cmds)
	}
	if got := get(t, s, "s1").PodSeq; got != 9 {
		t.Errorf("pod seq = %d, want 9", got)
	}
	_, err = s.AppendPodTurnEnd(ctx, "missing", "turn_completed", "t1", time.Now(), nil, 1)
	wantErr(t, "AppendPodTurnEnd on an unknown session", err, sessionstore.ErrNotFound)
}
