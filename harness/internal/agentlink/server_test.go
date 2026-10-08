package agentlink_test

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/agentlink/agentlinktest"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(newTestWriter(t), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct {
	mu   sync.Mutex
	t    *testing.T
	done bool
}

func newTestWriter(t *testing.T) *testWriter {
	w := &testWriter{t: t}
	t.Cleanup(func() {
		w.mu.Lock()
		w.done = true
		w.mu.Unlock()
	})
	return w
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return len(p), nil
	}
	w.t.Logf("%s", p)
	return len(p), nil
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type testLink struct {
	t      *testing.T
	srv    *agentlink.Server
	client agentlinkpb.AgentLinkServiceClient
	idp    *agentlinktest.IDP
	clock  *clock
}

var testSeal = agenticrun.Executor{
	Namespace: testNamespace, ServiceAccount: testSA, PodName: testPod, PodUID: testPodUID,
}

func newTestLink(t *testing.T, hbInterval time.Duration, missLimit uint32) *testLink {
	t.Helper()
	idp := agentlinktest.NewIDP(t)
	clk := &clock{t: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)}
	srv, err := agentlink.New(idp.Verifier(t),
		agentlink.WithHeartbeatInterval(hbInterval),
		agentlink.WithHeartbeatMissLimit(missLimit),
		agentlink.WithLogger(testLogger(t)),
		agentlink.WithNow(clk.now),
	)
	if err != nil {
		t.Fatalf("agentlink.New: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &testLink{t: t, srv: srv, client: agentlinkpb.NewAgentLinkServiceClient(conn), idp: idp, clock: clk}
}

func (g *testLink) ctx(parent context.Context, runID string, claims map[string]any) context.Context {
	g.t.Helper()
	extra := agentlinktest.Claims(runID, testSeal)
	for k, v := range claims {
		extra[k] = v
	}
	return metadata.AppendToOutgoingContext(parent, agentlink.AssertionMetadataKey, g.idp.Sign(g.t, extra))
}

func (g *testLink) attach(ctx context.Context, attempt uint32, agentRunning bool) (agentlinkpb.AgentLinkService_AttachClient, *agentlinkpb.ManagerHelloAck, error) {
	g.t.Helper()
	stream, err := g.client.Attach(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.Send(&agentlinkpb.SidecarFrame{
		Msg: &agentlinkpb.SidecarFrame_Hello{Hello: &agentlinkpb.SidecarHello{
			ProtocolVersion: agentlink.ProtocolVersion, Attempt: attempt, AgentRunning: agentRunning,
		}},
	}); err != nil {
		return nil, nil, err
	}
	frame, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	ack := frame.GetHelloAck()
	if ack == nil {
		g.t.Fatalf("first manager frame was %v, want HelloAck", frame)
	}
	return stream, ack, nil
}

func codeOf(err error) codes.Code { return status.Code(err) }

func TestAttachUnknownRunIsNotFound(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := g.attach(g.ctx(ctx, "no-such-run", nil), 1, false)
	if codeOf(err) != codes.NotFound {
		t.Fatalf("err = %v (code %s), want NotFound", err, codeOf(err))
	}
}

func TestAttachSealMismatchIsFailedPrecondition(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, err := g.srv.Expect("run-seal", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = g.attach(g.ctx(ctx, "run-seal", map[string]any{"act.kubernetes.io.pod.uid": "someone-else"}), 1, false)
	if codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
}

func TestAttachWithoutAssertionIsRejected(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	if _, err := g.srv.Expect("run-noassert", testSeal, nil); err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget("run-noassert")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := g.attach(ctx, 1, false)
	if codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
}

func TestExpectRejectsDuplicateAndForgetClearsRegistry(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	if _, err := g.srv.Expect("run-dup", testSeal, nil); err != nil {
		t.Fatalf("Expect: %v", err)
	}
	if _, err := g.srv.Expect("run-dup", testSeal, nil); err == nil {
		t.Error("a second Expect for the same run should fail")
	}
	if _, err := g.srv.Expect("", testSeal, nil); err == nil {
		t.Error("Expect should reject an empty run id")
	}
	if _, err := g.srv.Expect("run-badseal", agenticrun.Executor{Namespace: "ns"}, nil); err == nil {
		t.Error("Expect should reject an incomplete seal")
	}
	g.srv.Forget("run-dup")
	if n := g.srv.Expecting(); n != 0 {
		t.Errorf("registry holds %d runs after Forget, want 0", n)
	}
}

func TestForgetReleasesWaiters(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, err := g.srv.Expect("run-forget", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- handle.AwaitAttach(context.Background()) }()
	g.srv.Forget("run-forget")
	select {
	case err := <-done:
		if err == nil {
			t.Error("AwaitAttach should fail once the run is forgotten")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AwaitAttach did not return after Forget")
	}
	if handle.Err() == nil {
		t.Error("Err should report why the run finished")
	}
}

func TestSecondAttachRejectedWhileFirstIsFresh(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, err := g.srv.Expect("run-single", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := g.attach(g.ctx(ctx, "run-single", nil), 1, false); err != nil {
		t.Fatalf("first attach: %v", err)
	}
	if err := handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	_, _, err = g.attach(g.ctx(ctx, "run-single", nil), 2, false)
	if codeOf(err) != codes.AlreadyExists {
		t.Fatalf("second attach err = %v (code %s), want AlreadyExists", err, codeOf(err))
	}
}

func TestSecondAttachEvictsSilentFirst(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	var lost atomic.Int32
	handle, err := g.srv.Expect("run-evict", testSeal, nil, agentlink.WithOnLost(func(error) { lost.Add(1) }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, _, err := g.attach(g.ctx(ctx, "run-evict", nil), 1, false)
	if err != nil {
		t.Fatalf("first attach: %v", err)
	}
	if err := handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}

	g.clock.advance(2 * time.Hour)
	if _, _, err := g.attach(g.ctx(ctx, "run-evict", nil), 2, true); err != nil {
		t.Fatalf("second attach after eviction: %v", err)
	}
	if _, err := first.Recv(); err == nil {
		t.Error("the evicted stream should have been closed")
	}
	time.Sleep(100 * time.Millisecond)
	if n := lost.Load(); n != 0 {
		t.Errorf("OnLost fired %d times for an evicted stream, want 0", n)
	}
}

func TestHeartbeatExpiryFiresOnLostOnce(t *testing.T) {
	const interval = time.Second
	g := newTestLink(t, interval, 1)
	srv, err := agentlink.New(g.idp.Verifier(t),
		agentlink.WithHeartbeatInterval(interval), agentlink.WithHeartbeatMissLimit(1),
		agentlink.WithLogger(testLogger(t)),
	)
	if err != nil {
		t.Fatalf("agentlink.New: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	g.srv, g.client = srv, agentlinkpb.NewAgentLinkServiceClient(conn)

	var lost atomic.Int32
	if _, err := g.srv.Expect("run-hb", testSeal, nil, agentlink.WithOnLost(func(error) { lost.Add(1) })); err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget("run-hb")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, ack, err := g.attach(g.ctx(ctx, "run-hb", nil), 1, false)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if ack.GetHeartbeatSeconds() != 1 || ack.GetHeartbeatMissLimit() != 1 {
		t.Errorf("hello ack: %v", ack)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for lost.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := lost.Load(); n != 1 {
		t.Fatalf("OnLost fired %d times, want exactly 1", n)
	}
	time.Sleep(2 * interval)
	if n := lost.Load(); n != 1 {
		t.Fatalf("OnLost fired %d times after waiting, want exactly 1", n)
	}
}

func TestSidecarErrorReachesOnError(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	errCh := make(chan string, 1)
	handle, err := g.srv.Expect("run-err", testSeal, nil, agentlink.WithOnError(func(reason string, _ error) { errCh <- reason }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, _, err := g.attach(g.ctx(ctx, "run-err", nil), 1, false)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := stream.Send(&agentlinkpb.SidecarFrame{
		Msg: &agentlinkpb.SidecarFrame_Status{Status: &agentlinkpb.Status{
			State: agentlinkpb.Status_STATE_ERROR, Reason: "envoy_exited",
		}},
	}); err != nil {
		t.Fatalf("send status: %v", err)
	}
	select {
	case reason := <-errCh:
		if reason != "envoy_exited" {
			t.Errorf("reason = %q", reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnError never fired")
	}
}

func TestAttachDuringJWKSOutageIsRetryable(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(jwks.Close)
	v, err := agentlink.NewVerifier(idp.Issuer(),
		agentlink.WithAudience(agentlinktest.Audience),
		agentlink.WithJWKSURL(jwks.URL),
		agentlink.WithHTTPClient(jwks.Client()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	srv, err := agentlink.New(v, agentlink.WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("agentlink.New: %v", err)
	}
	handle, err := srv.Expect("run-outage", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer srv.Forget(handle.RunID())
	addr := agentlinktest.Serve(t, srv, func() string { return idp.SignFor(t, "run-outage", testSeal) })
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	g := &testLink{t: t, srv: srv, client: agentlinkpb.NewAgentLinkServiceClient(conn), idp: idp}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for attempt := range uint32(2) {
		_, _, err := g.attach(ctx, attempt+1, false)
		if codeOf(err) != codes.Unavailable {
			t.Fatalf("attempt %d: err = %v (code %s), want Unavailable", attempt+1, err, codeOf(err))
		}
	}
}

func TestSubSecondHeartbeatIsAdvertisedAsWholeSeconds(t *testing.T) {
	g := newTestLink(t, 500*time.Millisecond, 3)
	handle, err := g.srv.Expect("run-short-hb", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, ack, err := g.attach(g.ctx(ctx, "run-short-hb", nil), 1, false)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if got := ack.GetHeartbeatSeconds(); got != 1 {
		t.Fatalf("HelloAck heartbeat = %ds for a 500ms server interval, want 1s", got)
	}
}

func TestSidecarThatStopsReadingIsLost(t *testing.T) {
	g := newTestLink(t, time.Second, 1)
	lost := make(chan error, 1)
	cfg := &agentlinkpb.SandboxConfig{Endpoints: []*agentlinkpb.ProxiedEndpoint{{
		Name: "big", UpstreamUrl: "https://" + strings.Repeat("x", 1<<20),
	}}}
	handle, err := g.srv.Expect("run-unread", testSeal, cfg, agentlink.WithOnLost(func(cause error) { lost <- cause }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := g.client.Attach(g.ctx(ctx, "run-unread", nil))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if err := stream.Send(&agentlinkpb.SidecarFrame{
		Msg: &agentlinkpb.SidecarFrame_Hello{Hello: &agentlinkpb.SidecarHello{
			ProtocolVersion: agentlink.ProtocolVersion, Attempt: 1,
		}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	beat := time.NewTicker(100 * time.Millisecond)
	defer beat.Stop()
	for {
		select {
		case cause := <-lost:
			t.Logf("lost: %v", cause)
			return
		case <-beat.C:
			g.clock.advance(100 * time.Millisecond)
			_ = stream.Send(&agentlinkpb.SidecarFrame{
				Msg: &agentlinkpb.SidecarFrame_Status{Status: &agentlinkpb.Status{State: agentlinkpb.Status_STATE_READY}},
			})
		case <-ctx.Done():
			t.Fatal("OnLost never fired for a sidecar that stopped reading its control stream")
		}
	}
}

func TestReadyWaitsForTheAttachCallback(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	handle, err := g.srv.Expect("run-callback-order", testSeal, nil,
		agentlink.WithOnAttached(func(uint32, bool) {
			close(entered)
			<-release
		}))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, _, err := g.attach(g.ctx(ctx, "run-callback-order", nil), 1, false)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("OnAttached never started")
	}
	if err := stream.Send(&agentlinkpb.SidecarFrame{
		Msg: &agentlinkpb.SidecarFrame_Status{Status: &agentlinkpb.Status{State: agentlinkpb.Status_STATE_READY}},
	}); err != nil {
		t.Fatalf("send ready: %v", err)
	}
	ready := make(chan error, 1)
	go func() { ready <- handle.AwaitReady(ctx) }()
	select {
	case err := <-ready:
		t.Fatalf("READY was exposed before OnAttached returned: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := handle.SpawnAgent(ctx, &agentlinkpb.SessionParams{}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	frames := make(chan *agentlinkpb.ManagerFrame, 1)
	go func() {
		if f, err := stream.Recv(); err == nil {
			frames <- f
		}
	}()
	select {
	case f := <-frames:
		t.Fatalf("%v was sent before OnAttached returned", f)
	case <-time.After(200 * time.Millisecond):
	}
	unblock()
	if err := <-ready; err != nil {
		t.Fatalf("AwaitReady after OnAttached returned: %v", err)
	}
	select {
	case f := <-frames:
		if f.GetSpawn() == nil {
			t.Fatalf("first frame after OnAttached = %v, want Spawn", f)
		}
	case <-ctx.Done():
		t.Fatal("Spawn never arrived after OnAttached returned")
	}
}
