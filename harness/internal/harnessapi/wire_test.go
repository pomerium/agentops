package harnessapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/pomerium/agentops/harness/api"
	apiclient "github.com/pomerium/agentops/harness/api/client"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
)

func serveAPI(t *testing.T, h *harness, clientID string) harnessapipbconnect.HarnessAPIServiceClient {
	t.Helper()
	return serveImpl(t, h.svc, func(context.Context, http.Header) (string, error) {
		return clientID, nil
	})
}

func serveImpl(t *testing.T, impl harnessapipbconnect.HarnessAPIServiceHandler, identify apiserver.Identify) harnessapipbconnect.HarnessAPIServiceClient {
	t.Helper()
	base, hc := serveURL(t, impl, identify)
	c, err := apiclient.New(base, apiclient.WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

func wireSubscribeOptions(t *testing.T) []apiclient.SubscribeOption {
	return []apiclient.SubscribeOption{
		apiclient.WithKeepaliveInterval(200 * time.Millisecond),
		apiclient.WithMissedKeepalives(5),
		apiclient.WithLogger(testLogger(t)),
	}
}

func serveURL(t *testing.T, impl harnessapipbconnect.HarnessAPIServiceHandler, identify apiserver.Identify) (string, *http.Client) {
	t.Helper()
	srv, err := apiserver.New(impl, identify, apiserver.WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ts := httptest.NewUnstartedServer(srv.Handler())
	apiserver.EnableH2C(ts.Config)
	ts.Start()
	t.Cleanup(ts.Close)
	return ts.URL, ts.Client()
}

func TestWireLifecycle(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, harnessapi.WithPermissionTimeout(5*time.Second))
	h.launcher.gate = make(chan struct{})
	c := serveAPI(t, h, stubClient)

	created, err := c.CreateSession(ctx, &pb.CreateSessionRequest{
		Template:        "deploy",
		ConversationRef: "stub:wire-1",
		ApprovalPrompt:  "ship the thing",
		InitialPrompt:   "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession over the wire: %v", err)
	}
	view := created.GetSession()
	if view.GetState() != api.StatePending {
		t.Errorf("new session state = %q, want pending", view.GetState())
	}
	ref := byID(view.GetId())
	sub, err := apiclient.Subscribe(ctx, c, &pb.SubscribeRequest{Ref: ref}, wireSubscribeOptions(t)...)
	if err != nil {
		t.Fatalf("Subscribe over the wire: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	ev, at := rec.waitFor("approval_required", 0)
	approval := ev.GetApprovalRequired()
	if approval.GetApprovalUrl() == "" {
		t.Error("approval_required carried no URL for the client to deliver")
	}

	close(h.launcher.gate)
	_, at = rec.waitFor("approved", at)
	_, at = rec.waitFor("turn_completed", at)

	res, err := c.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "and again"})
	if err != nil {
		t.Fatalf("Prompt over the wire: %v", err)
	}
	if res.GetTurnId() == "" {
		t.Error("Prompt returned no turn id")
	}
	_, at = rec.waitFor("turn_completed", at)

	if _, err := c.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession over the wire: %v", err)
	}
	rec.waitFor("session_ended", at)

	select {
	case _, open := <-sub.Events():
		if open {
			for range sub.Events() {
			}
		}
	case <-time.After(5 * time.Second):
		t.Error("the subscription did not close after session_ended")
	}
}

func TestWireIgnoresClientSuppliedIdentity(t *testing.T) {
	ctx := as("admin")
	h := newHarness(t)
	c := serveAPI(t, h, "verified-client")

	created, err := c.CreateSession(ctx, &pb.CreateSessionRequest{
		Template:        "deploy",
		ConversationRef: "conv-forged",
		ApprovalPrompt:  "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	view := created.GetSession()
	row, err := h.store.GetSession(ctx, view.GetId())
	if err != nil {
		t.Fatalf("store.GetSession: %v", err)
	}
	if row.ClientID != "verified-client" {
		t.Fatalf("the session was created as %q; a client stated its own identity", row.ClientID)
	}
	get := &pb.GetSessionRequest{Ref: byID(view.GetId())}
	if _, err := h.svc.GetSession(as("verified-client"), get); err != nil {
		t.Fatalf("the verified client cannot see its own session: %v", err)
	}
	if _, err := h.svc.GetSession(as("admin"), get); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("the claimed identity owns the session: got %v, want ErrNotFound", err)
	}

	other := launchRunning(t, h, "stub:conv-other")
	if _, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: byID(other.GetId())}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("a forged ref reached another client's session: got %v, want ErrNotFound", err)
	}
}

func TestWireRejectsUnidentifiedRequests(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := serveImpl(t, h.svc, func(context.Context, http.Header) (string, error) {
		return "", apiserver.ErrNoIdentity
	})

	if _, err := c.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "conv-anon",
		ApprovalPrompt: "ship the thing",
	}); err == nil {
		t.Fatal("an unidentified caller created a session")
	}
	if list, err := h.store.ListSessionsByClient(ctx, "", false, time.Time{}); err != nil || len(list) > 0 {
		t.Errorf("an unidentified caller left %d sessions behind (%v)", len(list), err)
	}
	if _, err := h.svc.ListSessions(ctx, &pb.ListSessionsRequest{}); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("ListSessions with no caller: got %v, want Unauthenticated", err)
	}
}

func TestCrossClientAccessIsDeniedOverTheWire(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	owned := launchRunning(t, h, "stub:conv-1")
	c := serveAPI(t, h, "other")
	theirs := byID(owned.GetId())

	if _, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: theirs}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("GetSession across clients: got %v, want ErrNotFound", err)
	}
	if _, err := c.Prompt(ctx, &pb.PromptRequest{Ref: theirs, Content: "hi"}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Prompt across clients: got %v, want ErrNotFound", err)
	}
	if _, err := c.EndSession(ctx, &pb.EndSessionRequest{Ref: theirs}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("EndSession across clients: got %v, want ErrNotFound", err)
	}
	if _, err := c.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: theirs, RequestId: "tc-1", OptionId: "allow",
	}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("RespondPermission across clients: got %v, want ErrNotFound", err)
	}
	if _, err := c.ListEvents(ctx, &pb.ListEventsRequest{Ref: theirs}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("ListEvents across clients: got %v, want ErrNotFound", err)
	}
	if _, err := apiclient.Subscribe(ctx, c, &pb.SubscribeRequest{Ref: theirs}, wireSubscribeOptions(t)...); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Subscribe across clients: got %v, want ErrNotFound", err)
	}

	list, err := c.ListSessions(ctx, &pb.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if n := len(list.GetSessions()); n != 0 {
		t.Errorf("another client enumerated %d sessions over the wire, want 0", n)
	}
}

func TestWireErrorsRoundTripToSentinels(t *testing.T) {
	ctx := context.Background()
	for name, published := range api.Sentinels() {
		sentinel := published.Err
		t.Run(name.String(), func(t *testing.T) {
			stub := &stubAPI{err: api.Errorf(sentinel, "the detail that came with it")}
			c := serveImpl(t, stub, func(context.Context, http.Header) (string, error) {
				return stubClient, nil
			})
			_, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: byID("s1")})
			if !errors.Is(err, sentinel) {
				t.Fatalf("error over the wire = %v; errors.Is(%v) must still hold", err, sentinel)
			}
			if !strings.Contains(err.Error(), "the detail that came with it") {
				t.Errorf("the error detail did not survive the wire: %v", err)
			}
		})
	}

	t.Run("unclassified", func(t *testing.T) {
		stub := &stubAPI{err: errors.New("something nobody classified")}
		c := serveImpl(t, stub, func(context.Context, http.Header) (string, error) {
			return stubClient, nil
		})
		_, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: byID("s1")})
		if err == nil {
			t.Fatal("an unclassified failure came back as success")
		}
		if name, _, ok := api.Classify(err); ok {
			t.Errorf("an unclassified failure arrived as %v", name)
		}
	})
}

type stubAPI struct {
	harnessapipbconnect.HarnessAPIServiceHandler
	err error
}

func (s *stubAPI) GetSession(context.Context, *pb.GetSessionRequest) (*pb.GetSessionResponse, error) {
	return nil, s.err
}

func TestWireReplaysHistoryIdenticallyAfterRestart(t *testing.T) {
	ctx := as(stubClient)
	dbPath := t.TempDir() + "/wire-replay.db"

	h := newHarnessAt(t, dbPath)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "hello"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	rec := record(t, sub)
	rec.waitFor("turn_completed", 0)
	sub.Close()

	page, err := serveAPI(t, h, stubClient).ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents over the wire: %v", err)
	}
	before := page.GetEvents()
	if len(before) < 5 {
		t.Fatalf("expected a full launch's worth of events, got %d", len(before))
	}
	_ = h.store.Close()

	h2 := newHarnessAt(t, dbPath)
	page, err = serveAPI(t, h2, stubClient).ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents after restart: %v", err)
	}
	after := page.GetEvents()
	if len(after) != len(before) {
		t.Fatalf("replayed %d events over the wire, wrote %d", len(after), len(before))
	}
	for i := range before {
		if !proto.Equal(before[i], after[i]) {
			t.Errorf("event %d changed across the restart:\n before %v\n after  %v", i, before[i], after[i])
		}
	}

	unknown := protowire.AppendTag(nil, 99, protowire.BytesType)
	unknown = protowire.AppendString(unknown, "nobody here has ever seen this")
	if _, err := h2.store.AppendSessionEvent(ctx, view.GetId(), "something_new", "t1",
		time.Now().UTC().Truncate(time.Millisecond), unknown); err != nil {
		t.Fatalf("append an unknown payload: %v", err)
	}
	page, err = serveAPI(t, h2, stubClient).ListEvents(ctx, &pb.ListEventsRequest{
		Ref: ref, AfterSeq: after[len(after)-1].GetSeq(),
	})
	if err != nil {
		t.Fatalf("ListEvents for the unknown payload: %v", err)
	}
	tail := page.GetEvents()
	if len(tail) != 1 {
		t.Fatalf("read %d events after the known ones, want 1", len(tail))
	}
	if tail[0].GetPayload() != nil {
		t.Errorf("an unknown payload arrived as a known one: %v", tail[0])
	}
	if got := tail[0].ProtoReflect().GetUnknown(); string(got) != string(unknown) {
		t.Errorf("an unknown payload did not survive the wire: unknown fields %x, want %x", got, unknown)
	}
}

func TestWireJSONCarriesPayloadsAsJSON(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	h.launcher.gate = make(chan struct{})
	base, hc := serveURL(t, h.svc, func(context.Context, http.Header) (string, error) {
		return stubClient, nil
	})
	created, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:json-1",
		ApprovalPrompt: "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	view := created.GetSession()
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: byID(view.GetId())})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	record(t, sub).waitFor("approval_required", 0)
	sub.Close()

	body := strings.NewReader(`{"ref":{"sessionId":"` + view.GetId() + `"}}`)
	resp, err := hc.Post(base+"/harnessapi.v1.HarnessAPIService/ListEvents", "application/json", body)
	if err != nil {
		t.Fatalf("POST ListEvents: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListEvents over JSON: %s: %s", resp.Status, raw)
	}
	var page struct {
		Events []map[string]json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	var found bool
	for _, ev := range page.Events {
		p, ok := ev["approvalRequired"]
		if !ok {
			continue
		}
		found = true
		var approval struct {
			ApprovalURL string `json:"approvalUrl"`
		}
		if err := json.Unmarshal(p, &approval); err != nil || approval.ApprovalURL == "" {
			t.Errorf("approvalRequired is not an object carrying approvalUrl: %s", p)
		}
	}
	if !found {
		t.Errorf("no event carried an approvalRequired object: %s", raw)
	}
}

func TestWireSubscriptionSurvivesADroppedStream(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	log := testLogger(t)

	srv, err := apiserver.New(h.svc,
		func(context.Context, http.Header) (string, error) { return stubClient, nil },
		apiserver.WithLogger(log))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	var subscribes atomic.Int32
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/Subscribe") && subscribes.Add(1) == 1 {
			cut, cancel := context.WithTimeout(r.Context(), 300*time.Millisecond)
			defer cancel()
			r = r.WithContext(cut)
		}
		srv.Handler().ServeHTTP(w, r)
	}))
	apiserver.EnableH2C(ts.Config)
	ts.Start()
	t.Cleanup(ts.Close)

	c, err := apiclient.New(ts.URL, apiclient.WithHTTPClient(ts.Client()))
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	view := launchRunning(t, h, "stub:conv-drop")
	ref := byID(view.GetId())
	sub, err := apiclient.Subscribe(ctx, c, &pb.SubscribeRequest{Ref: ref},
		apiclient.WithKeepaliveInterval(200*time.Millisecond), apiclient.WithMissedKeepalives(5), apiclient.WithLogger(log))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	seqs := make(chan int64, 256)
	go func() {
		for ev := range sub.Events() {
			seqs <- ev.Seq
		}
		close(seqs)
	}()

	time.Sleep(3 * time.Second)
	if _, err := c.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "after the drop"}); err != nil {
		t.Fatalf("Prompt after the drop: %v", err)
	}
	if _, err := c.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}

	var last int64
	var count int
	deadline := time.After(15 * time.Second)
	for done := false; !done; {
		select {
		case seq, ok := <-seqs:
			if !ok {
				done = true
				continue
			}
			if seq <= last {
				t.Fatalf("event seq %d arrived after %d: the resume duplicated or reordered", seq, last)
			}
			if last != 0 && seq != last+1 {
				t.Errorf("gap in the delivered sequence: %d then %d", last, seq)
			}
			last, count = seq, count+1
		case <-deadline:
			t.Fatalf("the subscription did not reach the session's end; delivered %d events up to seq %d", count, last)
		}
	}
	if subscribes.Load() < 2 {
		t.Errorf("the stream was opened %d times; the drop should have forced a reconnect", subscribes.Load())
	}
	if count == 0 {
		t.Fatal("no events were delivered at all")
	}
}

func TestWireCarriesDerivedSessionLineage(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	c := serveAPI(t, h, stubClient)

	res, err := c.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:parent",
		ApprovalPrompt: "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession (parent): %v", err)
	}
	parent := res.GetSession()
	res, err = c.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:derived",
		ApprovalPrompt:  "continue that conversation [continues another person's conversation: 3 messages carried]",
		ParentSessionId: parent.GetId(),
	})
	if err != nil {
		t.Fatalf("CreateSession (derived): %v", err)
	}
	derived := res.GetSession()
	if derived.GetId() == parent.GetId() {
		t.Fatal("the derived session reused the parent")
	}
	stored, err := h.store.GetSession(ctx, derived.GetId())
	if err != nil {
		t.Fatalf("store.GetSession: %v", err)
	}
	if stored.ParentSessionID != parent.GetId() {
		t.Errorf("lineage did not survive the wire: parent_session_id = %q, want %q",
			stored.ParentSessionID, parent.GetId())
	}

	for _, id := range []string{parent.GetId(), derived.GetId()} {
		row, err := h.store.GetSession(ctx, id)
		if err != nil {
			t.Fatalf("store.GetSession: %v", err)
		}
		if row.ClientID != stubClient {
			t.Errorf("session %s belongs to %q, want %q", id, row.ClientID, stubClient)
		}
	}
	list, err := c.ListSessions(ctx, &pb.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if n := len(list.GetSessions()); n != 2 {
		t.Errorf("the client sees %d of its sessions, want 2", n)
	}
}
