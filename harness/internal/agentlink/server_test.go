package agentlink_test

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
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
	const interval = 60 * time.Millisecond
	g := newTestLink(t, interval, 2)
	srv, err := agentlink.New(g.idp.Verifier(t),
		agentlink.WithHeartbeatInterval(interval), agentlink.WithHeartbeatMissLimit(2),
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
	if ack.GetHeartbeatSeconds() != 0 || ack.GetHeartbeatMissLimit() != 2 {
		t.Logf("hello ack: %v", ack)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for lost.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := lost.Load(); n != 1 {
		t.Fatalf("OnLost fired %d times, want exactly 1", n)
	}
	time.Sleep(4 * interval)
	if n := lost.Load(); n != 1 {
		t.Fatalf("OnLost fired %d times after waiting, want exactly 1", n)
	}
}

func TestAgentIORequiresLiveAttach(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	if _, err := g.srv.Expect("run-noattach", testSeal, nil); err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget("run-noattach")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := g.client.AgentIO(g.ctx(ctx, "run-noattach", nil))
	if err != nil {
		t.Fatalf("AgentIO: %v", err)
	}
	if err := stream.Send(&agentlinkpb.AgentIOFrame{
		Msg: &agentlinkpb.AgentIOFrame_Open{Open: &agentlinkpb.AgentIOOpen{}},
	}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	_, err = stream.Recv()
	if codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
}

type agentIOPeer struct {
	stream   agentlinkpb.AgentLinkService_AgentIOClient
	cancel   context.CancelFunc
	consumed uint64
	got      []byte
}

func (g *testLink) openAgentIO(parent context.Context, runID string, consumed uint64) (*agentIOPeer, error) {
	g.t.Helper()
	ctx, cancel := context.WithCancel(g.ctx(parent, runID, nil))
	stream, err := g.client.AgentIO(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := stream.Send(&agentlinkpb.AgentIOFrame{
		Msg: &agentlinkpb.AgentIOFrame_Open{Open: &agentlinkpb.AgentIOOpen{Consumed: consumed}},
	}); err != nil {
		cancel()
		return nil, err
	}
	frame, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, err
	}
	if frame.GetOpen() == nil {
		cancel()
		return nil, status.Errorf(codes.Internal, "first manager AgentIO frame was %v, want Open", frame)
	}
	return &agentIOPeer{stream: stream, cancel: cancel, consumed: consumed}, nil
}

func TestAgentIORoundTrip(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, err := g.srv.Expect("run-io", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	attach, _, err := g.attach(g.ctx(ctx, "run-io", nil), 1, false)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}

	spawned := make(chan struct{})
	go func() {
		for {
			frame, err := attach.Recv()
			if err != nil {
				return
			}
			if frame.GetSpawn() != nil {
				close(spawned)
				return
			}
		}
	}()
	if err := handle.SpawnAgent(ctx); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	select {
	case <-spawned:
	case <-time.After(5 * time.Second):
		t.Fatal("the spawn directive never arrived")
	}

	peer, err := g.openAgentIO(ctx, "run-io", 0)
	if err != nil {
		t.Fatalf("openAgentIO: %v", err)
	}
	defer peer.cancel()

	stdin, stdout, err := handle.AwaitAgentIO(ctx)
	if err != nil {
		t.Fatalf("AwaitAgentIO: %v", err)
	}

	want := []byte("{\"jsonrpc\":\"2.0\",\"method\":\"initialize\"}\n")
	if _, err := stdin.Write(want); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	for uint64(len(peer.got)) < uint64(len(want)) {
		frame, err := peer.stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if d := frame.GetData(); d != nil {
			peer.got = append(peer.got, d.GetPayload()...)
			peer.consumed = d.GetSeq()
		}
	}
	if string(peer.got) != string(want) {
		t.Errorf("agent stdin = %q, want %q", peer.got, want)
	}

	reply := []byte("{\"jsonrpc\":\"2.0\",\"result\":{}}\n")
	if err := peer.stream.Send(&agentlinkpb.AgentIOFrame{
		Msg: &agentlinkpb.AgentIOFrame_Data{Data: &agentlinkpb.AgentIOData{
			Seq: uint64(len(reply)), Payload: reply,
		}},
	}); err != nil {
		t.Fatalf("send data: %v", err)
	}
	buf := make([]byte, len(reply))
	if _, err := io.ReadFull(stdout, buf); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if string(buf) != string(reply) {
		t.Errorf("agent stdout = %q, want %q", buf, reply)
	}
}

func TestAgentIOResumeIsByteExact(t *testing.T) {
	const payloadSize = 300 << 10
	rng := rand.New(rand.NewSource(20260725))

	for round := range 6 {
		g := newTestLink(t, time.Hour, 3)
		runID := "run-resume"
		handle, err := g.srv.Expect(runID, testSeal, nil)
		if err != nil {
			t.Fatalf("Expect: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if _, _, err := g.attach(g.ctx(ctx, runID, nil), 1, false); err != nil {
			t.Fatalf("attach: %v", err)
		}
		if err := handle.AwaitAttach(ctx); err != nil {
			t.Fatalf("AwaitAttach: %v", err)
		}

		peer, err := g.openAgentIO(ctx, runID, 0)
		if err != nil {
			t.Fatalf("openAgentIO: %v", err)
		}
		stdin, _, err := handle.AwaitAgentIO(ctx)
		if err != nil {
			t.Fatalf("AwaitAgentIO: %v", err)
		}

		want := make([]byte, payloadSize)
		if _, err := rng.Read(want); err != nil {
			t.Fatalf("rand: %v", err)
		}
		writeDone := make(chan error, 1)
		go func() {
			_, err := stdin.Write(want)
			writeDone <- err
		}()

		var transcript []byte
		drops := 2
		nextDrop := rng.Intn(payloadSize/2) + 1
		for len(transcript) < payloadSize {
			frame, err := peer.stream.Recv()
			if err != nil {
				t.Fatalf("round %d: recv after %d bytes: %v", round, len(transcript), err)
			}
			d := frame.GetData()
			if d == nil {
				continue
			}
			transcript = append(transcript, d.GetPayload()...)
			peer.consumed = d.GetSeq()
			if uint64(len(transcript)) != peer.consumed {
				t.Fatalf("round %d: transcript is %d bytes but seq says %d", round, len(transcript), peer.consumed)
			}
			if rng.Intn(2) == 0 {
				if err := peer.stream.Send(&agentlinkpb.AgentIOFrame{
					Msg: &agentlinkpb.AgentIOFrame_Ack{Ack: &agentlinkpb.AgentIOAck{Consumed: peer.consumed}},
				}); err != nil {
					t.Fatalf("round %d: ack: %v", round, err)
				}
			}
			if drops > 0 && len(transcript) >= nextDrop {
				drops--
				consumed := peer.consumed
				peer.cancel()
				nextDrop = len(transcript) + rng.Intn(payloadSize/3) + 1
				if peer, err = g.openAgentIO(ctx, runID, consumed); err != nil {
					t.Fatalf("round %d: reopen at %d: %v", round, consumed, err)
				}
			}
		}
		if err := <-writeDone; err != nil {
			t.Fatalf("round %d: stdin write: %v", round, err)
		}
		if string(transcript) != string(want) {
			t.Fatalf("round %d: transcript differs from what was written (%d vs %d bytes)",
				round, len(transcript), len(want))
		}
		peer.cancel()
		g.srv.Forget(runID)
		cancel()
	}
}

func TestAgentIOUnserviceableResumeIsTerminal(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	var reasons []string
	var mu sync.Mutex
	handle, err := g.srv.Expect("run-badresume", testSeal, nil, agentlink.WithOnError(func(reason string, _ error) { mu.Lock(); reasons = append(reasons, reason); mu.Unlock() }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := g.attach(g.ctx(ctx, "run-badresume", nil), 1, false); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}

	_, err = g.openAgentIO(ctx, "run-badresume", 1)
	if codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != "agentio_resume_invalid" {
		t.Errorf("OnError reasons = %v", reasons)
	}
}

func TestAgentIODataGapIsTerminal(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	var reasons []string
	var mu sync.Mutex
	handle, err := g.srv.Expect("run-gap", testSeal, nil, agentlink.WithOnError(func(reason string, _ error) { mu.Lock(); reasons = append(reasons, reason); mu.Unlock() }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := g.attach(g.ctx(ctx, "run-gap", nil), 1, false); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	peer, err := g.openAgentIO(ctx, "run-gap", 0)
	if err != nil {
		t.Fatalf("openAgentIO: %v", err)
	}
	defer peer.cancel()

	if err := peer.stream.Send(&agentlinkpb.AgentIOFrame{
		Msg: &agentlinkpb.AgentIOFrame_Data{Data: &agentlinkpb.AgentIOData{Seq: 100, Payload: []byte("oops")}},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	for {
		if _, err := peer.stream.Recv(); err != nil {
			if codeOf(err) != codes.FailedPrecondition {
				t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
			}
			break
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != "protocol_violation" {
		t.Errorf("OnError reasons = %v", reasons)
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

func TestAgentExitReachesOnAgentExit(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	exitCh := make(chan int32, 1)
	handle, err := g.srv.Expect("run-exit", testSeal, nil, agentlink.WithOnAgentExit(func(code int32) { exitCh <- code }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget(handle.RunID())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, _, err := g.attach(g.ctx(ctx, "run-exit", nil), 1, false)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := stream.Send(&agentlinkpb.SidecarFrame{
		Msg: &agentlinkpb.SidecarFrame_Exited{Exited: &agentlinkpb.AgentExited{ExitCode: 3}},
	}); err != nil {
		t.Fatalf("send exited: %v", err)
	}
	select {
	case code := <-exitCh:
		if code != 3 {
			t.Errorf("exit code = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnAgentExit never fired")
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
