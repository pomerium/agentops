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
	"github.com/pomerium/agentops/harness/api/wire"
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
func serve(t *testing.T) func(headers map[string]string, opts ...func(*apiclient.Config)) api.API {
	t.Helper()
	log := slog.New(slog.NewTextHandler(&testWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelError}))
	ln, srv, err := apistub.Serve("127.0.0.1:0", apistub.New(), log)
	if err != nil {
		t.Fatalf("apistub.Serve: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	base := "http://" + ln.Addr().String()
	return func(headers map[string]string, opts ...func(*apiclient.Config)) api.API {
		cfg := apiclient.Config{
			BaseURL:    base,
			HTTPClient: headerClient{headers: headers},
			Logger:     log,
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

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func briefKeepalive(cfg *apiclient.Config) {
	cfg.KeepaliveInterval = 100 * time.Millisecond
	cfg.MissedKeepalives = 3
}

// TestLifecycle is the shape every SDK's conformance suite mirrors: create, read
// the approval URL off the log rather than off the response, take a turn, end.
func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template:        "runid",
		ConversationRef: "conv-1",
		ApprovalPrompt:  "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if view.State != api.StatePending {
		t.Errorf("a fresh session is %s, want pending", view.State)
	}

	events, err := c.ListEvents(ctx, api.EventsRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
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
		if ev.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d", i, ev.Seq)
		}
	}

	res, err := c.Prompt(ctx, api.PromptRequest{
		Ref: api.SessionRef{SessionID: view.ID}, Content: "do the thing",
	})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if res.TurnID == "" {
		t.Error("Prompt returned no turn id")
	}

	if err := c.EndSession(ctx, api.EndSessionRequest{
		Ref: api.SessionRef{SessionID: view.ID}, Reason: api.EndEnded,
	}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	after, err := c.GetSession(ctx, api.SessionRef{SessionID: view.ID})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if after.State != api.StateEnded {
		t.Errorf("state after EndSession is %s, want ended", after.State)
	}
}

// TestSentinels drives every published error over the wire and checks it still
// satisfies the same errors.Is check on the far side — including the two pairs
// that share a Connect code, which is the whole reason ErrorInfo exists.
func TestSentinels(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	for sentinel, entry := range wire.Sentinels() {
		name := sentinel.String()
		t.Run(name, func(t *testing.T) {
			_, err := c.GetSession(ctx, api.SessionRef{SessionID: apistub.SentinelPrefix + name})
			if err == nil {
				t.Fatalf("%s: no error", name)
			}
			if !errors.Is(err, entry.Err) {
				t.Errorf("%s: got %v, which does not match its sentinel", name, err)
			}
			var cerr *connect.Error
			if errors.As(err, &cerr) && cerr.Code() != entry.Code {
				t.Errorf("%s: code %v, want %v", name, cerr.Code(), entry.Code)
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

	view, err := alice.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	if _, err := bob.GetSession(ctx, api.SessionRef{SessionID: view.ID}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("GetSession as another client: %v, want ErrNotFound", err)
	}
	if _, err := bob.Subscribe(ctx, api.SubscribeRequest{
		Ref: api.SessionRef{SessionID: view.ID},
	}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Subscribe as another client: %v, want ErrNotFound", err)
	}
	// A refused subscribe fails at open, before any frame — which is what the
	// server's opening keepalive is for.
	sessions, err := bob.ListSessions(ctx, api.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 0 {
		t.Errorf("another client listed %d sessions", len(sessions))
	}
}

// TestSubscribeStopsOnSessionEnded: the stream is stopped by the EVENT, not only
// by the end of the connection. The stub deliberately leaves the feed open after
// session_ended when the subscriber arrived before it, so a client that waits
// for EOF instead would hang here.
func TestSubscribeStopsOnSessionEnded(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sub, err := c.Subscribe(ctx, api.SubscribeRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	if err := c.EndSession(ctx, api.EndSessionRequest{Ref: api.SessionRef{SessionID: view.ID}}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}

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

	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := c.EndSession(ctx, api.EndSessionRequest{Ref: api.SessionRef{SessionID: view.ID}}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}

	// From the end of the log: nothing to replay, and nothing will follow.
	sub, err := c.Subscribe(ctx, api.SubscribeRequest{
		Ref: api.SessionRef{SessionID: view.ID}, AfterSeq: 1 << 30,
	})
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

	view, err := driver.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
		InitialPrompt: "do the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	full, err := driver.ListEvents(ctx, api.EventsRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}

	// One-shot: the first connection dies after three envelopes, the reconnect
	// works. Without the key a correct client would reconnect forever.
	reader := newClient(map[string]string{
		apistub.HeaderScenario:    "drop-after=3",
		apistub.HeaderScenarioKey: "resume-test",
	})
	sub, err := reader.Subscribe(ctx, api.SubscribeRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	go func() {
		// Ended so the feed terminates and drain returns.
		time.Sleep(3 * time.Second)
		_ = driver.EndSession(ctx, api.EndSessionRequest{Ref: api.SessionRef{SessionID: view.ID}})
	}()

	got := drain(t, sub, 20*time.Second)
	// Every event of the original log, in order, once each. A resume that skipped
	// would drop one; one that did not dedup would repeat the frames it already
	// delivered before the drop.
	if len(got) < len(full) {
		t.Fatalf("delivered %d events, want at least the %d from before the drop", len(got), len(full))
	}
	for i, ev := range full {
		if got[i].Seq != ev.Seq {
			t.Fatalf("event %d has seq %d, want %d — the resume skipped or repeated", i, got[i].Seq, ev.Seq)
		}
	}
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Fatalf("seq went %d then %d: a duplicate crossed the resume", got[i-1].Seq, got[i].Seq)
		}
	}
}

// TestSilentStreamReconnects: a stream that says nothing at all, keepalives
// included, is dead however healthy the socket looks.
func TestSilentStreamReconnects(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	driver := newClient(nil)

	view, err := driver.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
		InitialPrompt: "do the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	reader := newClient(map[string]string{
		apistub.HeaderScenario:    apistub.ScenarioSilent,
		apistub.HeaderScenarioKey: "silent-test",
	}, briefKeepalive)
	sub, err := reader.Subscribe(ctx, api.SubscribeRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	go func() {
		time.Sleep(3 * time.Second)
		_ = driver.EndSession(ctx, api.EndSessionRequest{Ref: api.SessionRef{SessionID: view.ID}})
	}()

	got := drain(t, sub, 20*time.Second)
	if len(got) == 0 {
		t.Fatal("nothing arrived: the client never gave up on a silent stream")
	}
	if got[0].Seq != 1 {
		t.Errorf("the resumed feed starts at seq %d, want 1", got[0].Seq)
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

	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
		InitialPrompt: apistub.PromptUnknownEvent,
	})
	if err != nil {
		t.Fatalf("CreateSession with an unknown field in the response: %v", err)
	}
	if view.ID == "" {
		t.Fatal("the response decoded to an empty view")
	}

	events, err := c.ListEvents(ctx, api.EventsRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	var sawFuture bool
	for _, ev := range events {
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

	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sub, err := c.Subscribe(ctx, api.SubscribeRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	if _, err := c.Prompt(ctx, api.PromptRequest{
		Ref: api.SessionRef{SessionID: view.ID}, Content: apistub.PromptZeroEvent,
	}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if err := c.EndSession(ctx, api.EndSessionRequest{Ref: api.SessionRef{SessionID: view.ID}}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}

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
		if ev.Seq == 0 {
			t.Error("an event with seq 0 was delivered; dedup should have dropped it")
		}
	}
}

// TestPermissionRoundTrip: the turn stops on a permission request and finishes
// once it is answered.
func TestPermissionRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)

	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
		InitialPrompt: apistub.PromptPermission,
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	events, err := c.ListEvents(ctx, api.EventsRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	var req *pb.PermissionRequest
	for _, ev := range events {
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
	if len(req.Options) == 0 {
		t.Error("the permission request offered no options")
	}

	if err := c.RespondPermission(ctx, api.RespondPermissionRequest{
		Ref: api.SessionRef{SessionID: view.ID}, RequestID: req.GetRequestId(), OptionID: "allow",
	}); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}
	// Answering twice is an unknown request, not a second decision.
	if err := c.RespondPermission(ctx, api.RespondPermissionRequest{
		Ref: api.SessionRef{SessionID: view.ID}, RequestID: req.GetRequestId(), OptionID: "allow",
	}); !errors.Is(err, api.ErrUnknownRequest) {
		t.Errorf("answering twice: %v, want ErrUnknownRequest", err)
	}

	events, err = c.ListEvents(ctx, api.EventsRequest{Ref: api.SessionRef{SessionID: view.ID}})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	var resolved, completed bool
	for _, ev := range events {
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

	req := api.CreateSessionRequest{
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
func drain(t *testing.T, sub api.Subscription, budget time.Duration) []*pb.Event {
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

	view, err := driver.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "lossy", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := api.SessionRef{SessionID: view.ID}
	if _, err := lossy.Prompt(ctx, api.PromptRequest{Ref: ref, Content: "deploy"}); err == nil {
		t.Error("Prompt reported success for a response that never arrived")
	}

	events, err := driver.ListEvents(ctx, api.EventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	turns := 0
	for _, ev := range events {
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

	view, err := driver.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "lossy", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := api.SessionRef{SessionID: view.ID}
	res, err := lossy.Prompt(ctx, api.PromptRequest{Ref: ref, Content: "deploy", IdempotencyKey: "msg-1"})
	if err != nil {
		t.Fatalf("Prompt with a key was not retried past a lost response: %v", err)
	}

	events, err := driver.ListEvents(ctx, api.EventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	var turns []string
	for _, ev := range events {
		if ev.GetTurnCompleted() != nil {
			turns = append(turns, ev.GetTurnId())
		}
	}
	if len(turns) != 1 {
		t.Fatalf("one keyed prompt ran %d turns", len(turns))
	}
	if turns[0] != res.TurnID {
		t.Errorf("the retry returned turn %q; the turn that ran is %q", res.TurnID, turns[0])
	}
}

// TestActingVerbsIgnoreIncludeTerminal: a verb that acts on a session operates
// on the live one or on nothing, as SessionRef says, so an ended conversation is
// not found rather than acted on.
func TestActingVerbsIgnoreIncludeTerminal(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)
	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "ended", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := c.EndSession(ctx, api.EndSessionRequest{Ref: api.SessionRef{SessionID: view.ID}}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}

	ref := api.SessionRef{ConversationRef: "ended", IncludeTerminal: true}
	if err := c.EndSession(ctx, api.EndSessionRequest{Ref: ref}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("EndSession on an ended conversation: %v, want ErrNotFound", err)
	}
	if _, err := c.Prompt(ctx, api.PromptRequest{Ref: ref, Content: "hi"}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Prompt on an ended conversation: %v, want ErrNotFound", err)
	}
	// Reading history is what the flag is for, and that still works.
	if got, err := c.GetSession(ctx, ref); err != nil || got.ID != view.ID {
		t.Errorf("GetSession with include_terminal: %v, %v", got.ID, err)
	}
}

// TestEndedEventLeavesALiveFeedOpen: the stub does not close a live feed after
// session_ended, so a client that waits for end-of-stream instead of stopping on
// the event hangs here rather than passing by accident.
func TestEndedEventLeavesALiveFeedOpen(t *testing.T) {
	s := apistub.New()
	ctx := context.Background()
	v, err := s.CreateSession(ctx, api.CreateSessionRequest{
		ClientID: "a", Template: "runid", ConversationRef: "c", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := api.SessionRef{ClientID: "a", SessionID: v.ID}
	sub, err := s.Subscribe(ctx, api.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	if err := s.EndSession(ctx, api.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	for ev := range sub.Events() {
		if ev.GetSessionEnded() == nil {
			continue
		}
		select {
		case _, open := <-sub.Events():
			if !open {
				t.Fatal("the feed closed right after session_ended")
			}
		case <-time.After(50 * time.Millisecond):
		}
		return
	}
	t.Fatal("the feed closed before session_ended arrived")
}

// TestAnUnreadSubscriberDoesNotBreakPrompts: a test that stops reading its feed
// does not make the stub panic, however much the session goes on to say.
func TestAnUnreadSubscriberDoesNotBreakPrompts(t *testing.T) {
	s := apistub.New()
	ctx := context.Background()
	v, err := s.CreateSession(ctx, api.CreateSessionRequest{
		ClientID: "a", Template: "runid", ConversationRef: "c", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := api.SessionRef{ClientID: "a", SessionID: v.ID}
	sub, err := s.Subscribe(ctx, api.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("a prompt panicked: %v", p)
		}
	}()
	for range 100 {
		if _, err := s.Prompt(ctx, api.PromptRequest{Ref: ref, Content: "work"}); err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	}
	// Nothing was dropped: the whole log arrives, in order.
	logged, err := s.ListEvents(ctx, api.EventsRequest{Ref: ref, Limit: 10000})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	for i := range logged {
		select {
		case ev := <-sub.Events():
			if ev.Seq != int64(i+1) {
				t.Fatalf("event %d arrived as seq %d", i+1, ev.Seq)
			}
		case <-time.After(time.Second):
			t.Fatalf("the feed stopped after %d of %d events", i, len(logged))
		}
	}
}

// TestEndSessionIsIdempotent: ending an ended session succeeds, as it does on the
// platform, so a client whose EndSession response was lost can safely ask again.
func TestEndSessionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	c := serve(t)(nil)
	view, err := c.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "twice", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := api.SessionRef{SessionID: view.ID}
	for i := range 2 {
		if err := c.EndSession(ctx, api.EndSessionRequest{Ref: ref}); err != nil {
			t.Fatalf("EndSession #%d: %v", i+1, err)
		}
	}
}
