package apistub_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	apiclient "github.com/pomerium/agentops/harness/api/client"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/internal/apistub"
)

func serve(t *testing.T) func(headers map[string]string, opts ...func(*apiclient.Config)) harnessapipbconnect.HarnessAPIServiceClient {
	t.Helper()
	ln, srv, err := apistub.Serve("127.0.0.1:0", apistub.New(), testLogger(t))
	if err != nil {
		t.Fatalf("apistub.Serve: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	base := "http://" + ln.Addr().String()
	return func(headers map[string]string, opts ...func(*apiclient.Config)) harnessapipbconnect.HarnessAPIServiceClient {
		cfg := apiclient.Config{
			BaseURL:    base,
			HTTPClient: headerClient{headers: headers},
		}
		for _, o := range opts {
			o(&cfg)
		}
		c, err := apiclient.New(cfg)
		if err != nil {
			t.Fatalf("client.New: %v", err)
		}
		return c
	}
}

type headerClient struct {
	headers map[string]string
}

func (h headerClient) Do(r *http.Request) (*http.Response, error) {
	for k, v := range h.headers {
		r.Header.Set(k, v)
	}
	return http.DefaultClient.Do(r)
}

type testWriter struct {
	t    *testing.T
	mu   sync.Mutex
	done bool
}

func newTestWriter(t *testing.T) *testWriter {
	w := &testWriter{t: t}
	t.Cleanup(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.done = true
	})
	return w
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.done {
		w.t.Logf("%s", p)
	}
	return len(p), nil
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(newTestWriter(t), &slog.HandlerOptions{Level: slog.LevelError}))
}

var briefKeepalive = apiclient.SubscribeOptions{
	KeepaliveInterval: 100 * time.Millisecond,
	MissedKeepalives:  3,
}

func subscribe(ctx context.Context, t *testing.T, c harnessapipbconnect.HarnessAPIServiceClient, id string, opts apiclient.SubscribeOptions) (*apiclient.Subscription, error) {
	if opts.Logger == nil {
		opts.Logger = testLogger(t)
	}
	return apiclient.Subscribe(ctx, c, &pb.SubscribeRequest{Ref: byID(id)}, opts)
}

func byID(id string) *pb.SessionRef { return &pb.SessionRef{SessionId: id} }

func listEvents(ctx context.Context, t *testing.T, c harnessapipbconnect.HarnessAPIServiceClient, ref *pb.SessionRef) []*pb.Event {
	t.Helper()
	res, err := c.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	return res.GetEvents()
}

func create(ctx context.Context, t *testing.T, c harnessapipbconnect.HarnessAPIServiceClient, req *pb.CreateSessionRequest) *pb.SessionView {
	t.Helper()
	if req.Template == "" {
		req.Template = "runid"
	}
	if req.ApprovalPrompt == "" {
		req.ApprovalPrompt = "ship it"
	}
	res, err := c.CreateSession(ctx, req)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	return res.GetSession()
}

func end(ctx context.Context, t *testing.T, c harnessapipbconnect.HarnessAPIServiceClient, id string) {
	t.Helper()
	if _, err := c.EndSession(ctx, &pb.EndSessionRequest{Ref: byID(id)}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
}

func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	created, err := c.CreateSession(ctx, &pb.CreateSessionRequest{
		Template:        "runid",
		ConversationRef: "conv-1",
		ApprovalPrompt:  "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	view := created.GetSession()
	if view.GetState() != api.StatePending {
		t.Errorf("a fresh session is %s, want pending", view.GetState())
	}

	events := listEvents(ctx, t, c, byID(view.GetId()))
	var approvalURL string
	for _, ev := range events {
		if p := ev.GetApprovalRequired(); p != nil {
			approvalURL = p.GetApprovalUrl()
		}
	}
	if approvalURL == "" {
		t.Fatal("no approval_required event carried a URL")
	}

	for i, ev := range events {
		if ev.GetSeq() != int64(i+1) {
			t.Fatalf("event %d has seq %d", i, ev.GetSeq())
		}
	}

	res, err := c.Prompt(ctx, &pb.PromptRequest{Ref: byID(view.GetId()), Content: "do the thing"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if res.GetTurnId() == "" {
		t.Error("Prompt returned no turn id")
	}

	if _, err := c.EndSession(ctx, &pb.EndSessionRequest{
		Ref: byID(view.GetId()), Reason: api.EndEnded,
	}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	after, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: byID(view.GetId())})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got := after.GetSession().GetState(); got != api.StateEnded {
		t.Errorf("state after EndSession is %s, want ended", got)
	}
}

func TestSentinels(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	for sentinel, entry := range api.Sentinels() {
		name := sentinel.String()
		t.Run(name, func(t *testing.T) {
			_, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: byID(apistub.SentinelPrefix + name)})
			if err == nil {
				t.Fatalf("%s: no error", name)
			}
			if !errors.Is(err, entry.Err) {
				t.Errorf("%s: got %v, which does not match its sentinel", name, err)
			}
			if got := connect.CodeOf(err); got != entry.Code {
				t.Errorf("%s: code %v, want %v", name, got, entry.Code)
			}
		})
	}
}

func TestCrossClientIsNotFound(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	alice := newClient(map[string]string{apistub.HeaderClient: "alice"})
	bob := newClient(map[string]string{apistub.HeaderClient: "bob"})

	view := create(ctx, t, alice, &pb.CreateSessionRequest{ConversationRef: "conv-1"})

	if _, err := bob.GetSession(ctx, &pb.GetSessionRequest{Ref: byID(view.GetId())}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("GetSession as another client: %v, want ErrNotFound", err)
	}

	if _, err := subscribe(ctx, t, bob, view.GetId(), apiclient.SubscribeOptions{}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Subscribe as another client: %v, want ErrNotFound", err)
	}
	sessions, err := bob.ListSessions(ctx, &pb.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if n := len(sessions.GetSessions()); n != 0 {
		t.Errorf("another client listed %d sessions", n)
	}
}

func TestSubscribeStopsOnSessionEnded(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view := create(ctx, t, c, &pb.CreateSessionRequest{ConversationRef: "conv-1"})
	sub, err := subscribe(ctx, t, c, view.GetId(), apiclient.SubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	end(ctx, t, c, view.GetId())

	last := drain(t, sub, 5*time.Second)
	if len(last) == 0 {
		t.Fatal("the feed delivered nothing")
	}
	if got := last[len(last)-1]; got.GetSessionEnded() == nil {
		t.Errorf("the feed's last event is %s, want session_ended", api.Kind(got))
	}
}

func TestSubscribeToFinishedLog(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view := create(ctx, t, c, &pb.CreateSessionRequest{ConversationRef: "conv-1"})
	end(ctx, t, c, view.GetId())

	sub, err := apiclient.Subscribe(ctx, c, &pb.SubscribeRequest{
		Ref: byID(view.GetId()), AfterSeq: 1 << 30,
	}, apiclient.SubscribeOptions{Logger: testLogger(t)})
	if err != nil {
		t.Fatalf("Subscribe to a finished log: %v, want success", err)
	}
	defer sub.Close()
	if got := drain(t, sub, 5*time.Second); len(got) != 0 {
		t.Errorf("a finished log delivered %d events", len(got))
	}
}

func TestResumeAfterDrop(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	driver := newClient(nil)

	view := create(ctx, t, driver, &pb.CreateSessionRequest{
		ConversationRef: "conv-1", InitialPrompt: "do the thing",
	})
	full := listEvents(ctx, t, driver, byID(view.GetId()))

	reader := newClient(map[string]string{
		apistub.HeaderScenario:    "drop-after=3",
		apistub.HeaderScenarioKey: "resume-test",
	})
	sub, err := subscribe(ctx, t, reader, view.GetId(), apiclient.SubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	go func() {

		time.Sleep(3 * time.Second)
		_, _ = driver.EndSession(ctx, &pb.EndSessionRequest{Ref: byID(view.GetId())})
	}()

	got := drain(t, sub, 20*time.Second)

	if len(got) < len(full) {
		t.Fatalf("delivered %d events, want at least the %d from before the drop", len(got), len(full))
	}
	for i, ev := range full {
		if got[i].GetSeq() != ev.GetSeq() {
			t.Fatalf("event %d has seq %d, want %d — the resume skipped or repeated", i, got[i].GetSeq(), ev.GetSeq())
		}
	}
	for i := 1; i < len(got); i++ {
		if got[i].GetSeq() <= got[i-1].GetSeq() {
			t.Fatalf("seq went %d then %d: a duplicate crossed the resume", got[i-1].GetSeq(), got[i].GetSeq())
		}
	}
}

func TestSilentStreamReconnects(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	driver := newClient(nil)

	view := create(ctx, t, driver, &pb.CreateSessionRequest{
		ConversationRef: "conv-1", InitialPrompt: "do the thing",
	})

	reader := newClient(map[string]string{
		apistub.HeaderScenario:    apistub.ScenarioSilent,
		apistub.HeaderScenarioKey: "silent-test",
	})
	sub, err := subscribe(ctx, t, reader, view.GetId(), briefKeepalive)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	go func() {
		time.Sleep(3 * time.Second)
		_, _ = driver.EndSession(ctx, &pb.EndSessionRequest{Ref: byID(view.GetId())})
	}()

	got := drain(t, sub, 20*time.Second)
	if len(got) == 0 {
		t.Fatal("nothing arrived: the client never gave up on a silent stream")
	}
	if got[0].GetSeq() != 1 {
		t.Errorf("the resumed feed starts at seq %d, want 1", got[0].GetSeq())
	}
}

func TestUnknownFieldAndEventTolerated(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	c := newClient(map[string]string{apistub.HeaderScenario: apistub.ScenarioUnknownField})

	res, err := c.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
		InitialPrompt: apistub.PromptUnknownEvent,
	})
	if err != nil {
		t.Fatalf("CreateSession with an unknown field in the response: %v", err)
	}
	view := res.GetSession()
	if view.GetId() == "" {
		t.Fatal("the response decoded to an empty view")
	}

	var sawFuture bool
	for _, ev := range listEvents(ctx, t, c, byID(view.GetId())) {
		if ev.GetPayload() == nil && len(ev.ProtoReflect().GetUnknown()) > 0 {
			sawFuture = true
		}
	}
	if !sawFuture {
		t.Error("the unknown payload did not survive the round trip")
	}
}

func TestZeroValuedEvent(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view := create(ctx, t, c, &pb.CreateSessionRequest{ConversationRef: "conv-1"})
	sub, err := subscribe(ctx, t, c, view.GetId(), apiclient.SubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	if _, err := c.Prompt(ctx, &pb.PromptRequest{
		Ref: byID(view.GetId()), Content: apistub.PromptZeroEvent,
	}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	end(ctx, t, c, view.GetId())

	got := drain(t, sub, 5*time.Second)

	if len(got) == 0 {
		t.Fatal("the zero-valued event killed the feed")
	}
	if last := got[len(got)-1]; last.GetSessionEnded() == nil {
		t.Errorf("the feed ended on %s, want session_ended", api.Kind(last))
	}
	for _, ev := range got {
		if ev.GetSeq() == 0 {
			t.Error("an event with seq 0 was delivered; dedup should have dropped it")
		}
	}
}

func TestPermissionRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view := create(ctx, t, c, &pb.CreateSessionRequest{
		ConversationRef: "conv-1", InitialPrompt: apistub.PromptPermission,
	})
	ref := byID(view.GetId())

	var req *pb.PermissionRequest
	for _, ev := range listEvents(ctx, t, c, ref) {
		if p := ev.GetPermissionRequest(); p != nil {
			req = p
		}
		if ev.GetTurnCompleted() != nil {
			t.Fatal("the turn completed while a permission request was outstanding")
		}
	}
	if req.GetRequestId() == "" {
		t.Fatal("no permission_request on the log")
	}
	if len(req.GetOptions()) == 0 {
		t.Error("the permission request offered no options")
	}

	answer := &pb.RespondPermissionRequest{Ref: ref, RequestId: req.GetRequestId(), OptionId: "allow"}
	if _, err := c.RespondPermission(ctx, answer); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}

	if _, err := c.RespondPermission(ctx, answer); !errors.Is(err, api.ErrUnknownRequest) {
		t.Errorf("answering twice: %v, want ErrUnknownRequest", err)
	}

	var resolved, completed bool
	for _, ev := range listEvents(ctx, t, c, ref) {
		switch ev.GetPayload().(type) {
		case *pb.Event_PermissionResolved:
			resolved = true
		case *pb.Event_TurnCompleted:
			completed = true
		}
	}
	if !resolved || !completed {
		t.Errorf("after answering: resolved=%v completed=%v, want both", resolved, completed)
	}
}

func TestConflict(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	req := &pb.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
	}
	if _, err := c.CreateSession(ctx, req); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := c.CreateSession(ctx, req); !errors.Is(err, api.ErrConflict) {
		t.Errorf("a second session on a live conversation: %v, want ErrConflict", err)
	}
}

func drain(t *testing.T, sub *apiclient.Subscription, budget time.Duration) []*pb.Event {
	t.Helper()
	var out []*pb.Event
	deadline := time.After(budget)
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-deadline:
			t.Fatalf("the feed did not close within %s (got %d events)", budget, len(out))
			return out
		}
	}
}

type lossyHTTPClient struct {
	next http.Client
	once sync.Once
}

func (l *lossyHTTPClient) Do(r *http.Request) (*http.Response, error) {
	lost := false
	if strings.HasSuffix(r.URL.Path, "/Prompt") {
		l.once.Do(func() { lost = true })
	}
	res, err := l.next.Do(r)
	if !lost || err != nil {
		return res, err
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	return nil, io.ErrUnexpectedEOF
}

func TestALostPromptResponseIsNotRetried(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	driver := newClient(nil)
	lossy := newClient(nil, func(cfg *apiclient.Config) {
		cfg.HTTPClient = &lossyHTTPClient{}
	})

	view := create(ctx, t, driver, &pb.CreateSessionRequest{ConversationRef: "lossy"})
	ref := byID(view.GetId())
	if _, err := lossy.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "deploy"}); err == nil {
		t.Error("Prompt reported success for a response that never arrived")
	}

	turns := 0
	for _, ev := range listEvents(ctx, t, driver, ref) {
		if ev.GetTurnCompleted() != nil {
			turns++
		}
	}
	if turns != 1 {
		t.Errorf("one prompt ran %d turns", turns)
	}
}

func TestALostKeyedPromptIsRetriedOnce(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	driver := newClient(nil)
	lossy := newClient(nil, func(cfg *apiclient.Config) {
		cfg.HTTPClient = &lossyHTTPClient{}
	})

	view := create(ctx, t, driver, &pb.CreateSessionRequest{ConversationRef: "lossy"})
	ref := byID(view.GetId())
	res, err := lossy.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "deploy", IdempotencyKey: "msg-1"})
	if err != nil {
		t.Fatalf("Prompt with a key was not retried past a lost response: %v", err)
	}

	var turns []string
	for _, ev := range listEvents(ctx, t, driver, ref) {
		if ev.GetTurnCompleted() != nil {
			turns = append(turns, ev.GetTurnId())
		}
	}
	if len(turns) != 1 {
		t.Fatalf("one keyed prompt ran %d turns", len(turns))
	}
	if turns[0] != res.GetTurnId() {
		t.Errorf("the retry returned turn %q; the turn that ran is %q", res.GetTurnId(), turns[0])
	}
}

func TestActingVerbsIgnoreIncludeTerminal(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)
	view := create(ctx, t, c, &pb.CreateSessionRequest{ConversationRef: "ended"})
	end(ctx, t, c, view.GetId())

	ref := &pb.SessionRef{ConversationRef: "ended", IncludeTerminal: true}
	if _, err := c.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("EndSession on an ended conversation: %v, want ErrNotFound", err)
	}
	if _, err := c.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "hi"}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Prompt on an ended conversation: %v, want ErrNotFound", err)
	}

	if got, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: ref}); err != nil || got.GetSession().GetId() != view.GetId() {
		t.Errorf("GetSession with include_terminal: %v, %v", got.GetSession().GetId(), err)
	}
}

func TestEndSessionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)
	view := create(ctx, t, c, &pb.CreateSessionRequest{ConversationRef: "twice"})
	for i := range 2 {
		if _, err := c.EndSession(ctx, &pb.EndSessionRequest{Ref: byID(view.GetId())}); err != nil {
			t.Fatalf("EndSession #%d: %v", i+1, err)
		}
	}
}
