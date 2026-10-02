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

// The stub is what two SDKs in other languages are tested against, so it gets
// tested here first, with the client this repo already trusts. A scenario that
// does not do what it claims would otherwise surface as a mysterious failure in
// TypeScript.

// serve starts a stub and returns a factory for clients of it. Headers are how
// a caller picks an identity and asks for a scenario, and the Go client has no
// option for arbitrary headers — so they go on through a wrapped HTTP client,
// which is also proof the wire needs nothing but headers to be driven.
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

// headerClient stamps fixed headers onto every request.
type headerClient struct {
	headers map[string]string
}

func (h headerClient) Do(r *http.Request) (*http.Response, error) {
	for k, v := range h.headers {
		r.Header.Set(k, v)
	}
	return http.DefaultClient.Do(r)
}

// testWriter goes quiet once its test ends: a subscription's reconnect loop can
// still be winding down then, and a Logf after the test returns panics.
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

// testLogger routes errors into the test log and drops the rest: a reconnect
// warning is what several tests provoke on purpose, and it is noise there.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(newTestWriter(t), &slog.HandlerOptions{Level: slog.LevelError}))
}

// briefKeepalive shrinks the liveness window to 300ms, so a test of dead-stream
// detection does not wait out the contract's minute.
var briefKeepalive = apiclient.SubscribeOptions{
	KeepaliveInterval: 100 * time.Millisecond,
	MissedKeepalives:  3,
}

// subscribe opens the resumable feed of one session through client.Subscribe.
func subscribe(ctx context.Context, t *testing.T, c harnessapipbconnect.HarnessAPIServiceClient, id string, opts apiclient.SubscribeOptions) (*apiclient.Subscription, error) {
	if opts.Logger == nil {
		opts.Logger = testLogger(t)
	}
	return apiclient.Subscribe(ctx, c, &pb.SubscribeRequest{Ref: byID(id)}, opts)
}

// byID is a ref to one session by its id.
func byID(id string) *pb.SessionRef { return &pb.SessionRef{SessionId: id} }

// listEvents reads a session's whole log, or fails the test.
func listEvents(ctx context.Context, t *testing.T, c harnessapipbconnect.HarnessAPIServiceClient, ref *pb.SessionRef) []*pb.Event {
	t.Helper()
	res, err := c.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	return res.GetEvents()
}

// create opens a session on a conversation with the defaults every test shares,
// or fails the test.
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

// end ends a session by id, or fails the test.
func end(ctx context.Context, t *testing.T, c harnessapipbconnect.HarnessAPIServiceClient, id string) {
	t.Helper()
	if _, err := c.EndSession(ctx, &pb.EndSessionRequest{Ref: byID(id)}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
}

// TestLifecycle is the shape every SDK's conformance suite mirrors: create, read
// the approval URL off the log rather than off the response, take a turn, end.
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

	// Seq is dense and ordered, which is what dedup-by-seq relies on.
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

// TestSentinels drives every published error over the wire and checks it still
// satisfies the same errors.Is check on the far side — including the two pairs
// that share a Connect code, which is the whole reason ErrorInfo exists — and
// still carries its Connect code for a client that only reads codes.
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

// TestCrossClientIsNotFound: another client's session does not exist, rather
// than existing and being refused. A client able to tell those apart could
// enumerate somebody else's conversations.
func TestCrossClientIsNotFound(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	alice := newClient(map[string]string{apistub.HeaderClient: "alice"})
	bob := newClient(map[string]string{apistub.HeaderClient: "bob"})

	view := create(ctx, t, alice, &pb.CreateSessionRequest{ConversationRef: "conv-1"})

	if _, err := bob.GetSession(ctx, &pb.GetSessionRequest{Ref: byID(view.GetId())}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("GetSession as another client: %v, want ErrNotFound", err)
	}
	// A refused subscribe fails at open, before any frame — which is what the
	// server's opening keepalive is for.
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

// TestSubscribeStopsOnSessionEnded: the stream is stopped by the EVENT, not only
// by the end of the connection. The stub deliberately leaves the feed open after
// session_ended when the subscriber arrived before it, so a client that waits
// for EOF instead would hang here.
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

// TestSubscribeToFinishedLog: a log that is already over is a SUCCESS with an
// empty feed. Reported as an error, a client would retry a closed log forever.
func TestSubscribeToFinishedLog(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view := create(ctx, t, c, &pb.CreateSessionRequest{ConversationRef: "conv-1"})
	end(ctx, t, c, view.GetId())

	// From the end of the log: nothing to replay, and nothing will follow.
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

// TestResumeAfterDrop kills the stream mid-flight and checks the client comes
// back with after_seq and delivers the rest exactly once.
func TestResumeAfterDrop(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	driver := newClient(nil)

	view := create(ctx, t, driver, &pb.CreateSessionRequest{
		ConversationRef: "conv-1", InitialPrompt: "do the thing",
	})
	full := listEvents(ctx, t, driver, byID(view.GetId()))

	// One-shot: the first connection dies after three envelopes, the reconnect
	// works. Without the key a correct client would reconnect forever.
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
		// Ended so the feed terminates and drain returns.
		time.Sleep(3 * time.Second)
		_, _ = driver.EndSession(ctx, &pb.EndSessionRequest{Ref: byID(view.GetId())})
	}()

	got := drain(t, sub, 20*time.Second)
	// Every event of the original log, in order, once each. A resume that skipped
	// would drop one; one that did not dedup would repeat the frames it already
	// delivered before the drop.
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

// TestSilentStreamReconnects: a stream that says nothing at all, keepalives
// included, is dead however healthy the socket looks.
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

// TestUnknownFieldAndEventTolerated: a response carrying a field this build has
// never heard of, and an event whose payload it has never heard of, both pass
// through.
// The additive-only contract is worth nothing if a client refuses to parse.
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

// TestZeroValuedEvent: an event with every field at its default. On the JSON
// codec proto3 omits them all, so it arrives as {} — absent has to read as the
// default, and a seq of 0 must not break dedup or the stream.
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
	// The zero event's seq of 0 is at or below everything already delivered, so a
	// correct client drops it as a duplicate. What matters is that the stream
	// survived it and kept delivering.
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

// TestPermissionRoundTrip: the turn stops on a permission request and finishes
// once it is answered.
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
	// Answering twice is an unknown request, not a second decision.
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

// TestConflict: a live conversation refuses a second session.
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

// drain collects a feed until it closes or the budget runs out.
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

// lossyHTTPClient delivers the first Prompt to the server and then loses its
// response, the way a connection reset after the server accepted it would.
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

// TestALostPromptResponseIsNotRetried: a prompt the server accepted runs once,
// even when the client never hears back. Resending it would start a second turn,
// and nothing on the wire could tell the two apart.
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

// TestALostKeyedPromptIsRetriedOnce: with an idempotency key the client does
// resend a prompt whose response was lost, and the resend gets the first turn
// back instead of starting a second one.
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

// TestActingVerbsIgnoreIncludeTerminal: a verb that acts on a session operates
// on the live one or on nothing, as SessionRef says, so an ended conversation is
// not found rather than acted on.
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
	// Reading history is what the flag is for, and that still works.
	if got, err := c.GetSession(ctx, &pb.GetSessionRequest{Ref: ref}); err != nil || got.GetSession().GetId() != view.GetId() {
		t.Errorf("GetSession with include_terminal: %v, %v", got.GetSession().GetId(), err)
	}
}

// TestEndSessionIsIdempotent: ending an ended session succeeds, as it does on the
// platform, so a client whose EndSession response was lost can safely ask again.
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
