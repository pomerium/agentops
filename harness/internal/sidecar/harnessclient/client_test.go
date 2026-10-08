package harnessclient_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	"github.com/pomerium/agentops/harness/internal/runner"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
	"github.com/pomerium/agentops/harness/internal/runner/runnertest"
	"github.com/pomerium/agentops/harness/internal/sidecar/harnessclient"
)

func TestMain(m *testing.M) {
	runnertest.RunIfRequested()
	os.Exit(m.Run())
}

const runID = "run-hermetic"

var seal = agenticrun.Executor{
	Namespace: "agentops", ServiceAccount: "sandbox-agent", PodName: "smc-pod", PodUID: "uid-1",
}

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

type staticToken struct {
	mu        sync.Mutex
	bearer    string
	refreshes int
}

func (s *staticToken) Bearer() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bearer
}

func (s *staticToken) set(bearer string) {
	s.mu.Lock()
	s.bearer = bearer
	s.mu.Unlock()
}

func (s *staticToken) Refresh() {
	s.mu.Lock()
	s.refreshes++
	s.mu.Unlock()
}

type runnerServer struct {
	socket string
	gs     *grpc.Server
	svc    *runner.Service
}

func startRunner(t *testing.T) *runnerServer {
	t.Helper()
	dir, err := os.MkdirTemp("", "rnr")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "r.sock")

	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen on %s: %v", socket, err)
	}
	svc := runner.New(
		runner.WithCommand(runnertest.Command()),
		runner.WithKillDelay(time.Second),
		runner.WithLogger(testLogger(t)),
	)
	gs := grpc.NewServer()
	svc.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		svc.Close()
	})
	return &runnerServer{socket: socket, gs: gs, svc: svc}
}

func (r *runnerServer) client(t *testing.T) runnerpb.AgentRunnerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("unix:"+r.socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return runnerpb.NewAgentRunnerServiceClient(conn)
}

type rig struct {
	t          *testing.T
	link       *agentlink.Server
	handle     *agentlink.RunHandle
	client     *harnessclient.Client
	token      *staticToken
	runErr     chan error
	configured chan *agentlinkpb.SandboxConfig
	runner     *runnerServer
	addr       string
}

func sessionConfig() *agentlinkpb.SandboxConfig {
	return &agentlinkpb.SandboxConfig{Endpoints: []*agentlinkpb.ProxiedEndpoint{{
		Name: "mcp-gke", ListenPort: 9101,
		UpstreamUrl: "https://gke.example.com/mcp",
	}}}
}

type serveFunc func(t *testing.T, srv *agentlink.Server, assertion func() string) string

func newRig(t *testing.T, opts ...agentlink.ExpectOption) *rig {
	t.Helper()
	return newRigWith(t, agentlinktest.Serve, nil, opts...)
}

func newRigWith(t *testing.T, serve serveFunc, agentRunner harnessclient.Runner, opts ...agentlink.ExpectOption) *rig {
	t.Helper()
	idp := agentlinktest.NewIDP(t)
	srv, err := agentlink.New(idp.Verifier(t),
		agentlink.WithHeartbeatInterval(200*time.Millisecond),
		agentlink.WithHeartbeatMissLimit(3),
		agentlink.WithLogger(testLogger(t)),
	)
	if err != nil {
		t.Fatalf("agentlink.New: %v", err)
	}
	addr := serve(t, srv, func() string { return idp.SignFor(t, runID, seal) })

	handle, err := srv.Expect(runID, seal, sessionConfig(), opts...)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}

	r := &rig{t: t, link: srv, handle: handle, runErr: make(chan error, 1), addr: addr}
	if agentRunner == nil {
		r.runner = startRunner(t)
		agentRunner = r.udsRunner(t)
	}
	r.token = &staticToken{bearer: "Bearer pom_art_test"}
	r.configured = make(chan *agentlinkpb.SandboxConfig, 1)
	r.client = r.newClient(t, agentRunner)
	t.Cleanup(r.client.Close)
	return r
}

func (r *rig) udsRunner(t *testing.T) *harnessclient.UDSRunner {
	t.Helper()
	uds, err := harnessclient.NewUDSRunner(r.runner.socket, testLogger(t))
	if err != nil {
		t.Fatalf("NewUDSRunner: %v", err)
	}
	t.Cleanup(uds.Close)
	return uds
}

func (r *rig) newClient(t *testing.T, agentRunner harnessclient.Runner) *harnessclient.Client {
	t.Helper()
	client, err := harnessclient.New(harnessclient.Config{
		URL:                "http://" + r.addr,
		Insecure:           true,
		Token:              r.token,
		Runner:             agentRunner,
		HeartbeatInterval:  200 * time.Millisecond,
		HeartbeatMissLimit: 3,
		BaseBackoff:        20 * time.Millisecond,
		MaxBackoff:         100 * time.Millisecond,
		Logger:             testLogger(t),
		Configure: func(cfg *agentlinkpb.SandboxConfig) error {
			select {
			case r.configured <- cfg:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("harnessclient.New: %v", err)
	}
	return client
}

func (r *rig) start(ctx context.Context) {
	go func() { r.runErr <- r.client.Run(ctx) }()
}

func (r *rig) spawn(ctx context.Context) {
	r.t.Helper()
	if err := r.handle.AwaitAttach(ctx); err != nil {
		r.t.Fatalf("AwaitAttach: %v", err)
	}
	if err := r.handle.SpawnAgent(ctx, &agentlinkpb.SessionParams{Cwd: "/tmp"}); err != nil {
		r.t.Fatalf("SpawnAgent: %v", err)
	}
}

func (r *rig) next() *agentlinkpb.AgentIOFrame {
	r.t.Helper()
	select {
	case f := <-r.handle.Inbox():
		return f
	case <-time.After(15 * time.Second):
		r.t.Fatal("nothing reached the manager's inbox")
		return nil
	}
}

func (r *rig) nextEvent() *agentlinkpb.AgentEvent {
	r.t.Helper()
	for {
		if ev := r.next().GetEvent(); ev != nil {
			return ev
		}
	}
}

func (r *rig) eventsUntil(done func(*agentlinkpb.AgentEvent) bool) []*agentlinkpb.AgentEvent {
	r.t.Helper()
	var out []*agentlinkpb.AgentEvent
	for {
		ev := r.nextEvent()
		out = append(out, ev)
		r.handle.Ack(ev.GetSeq())
		if done(ev) {
			return out
		}
	}
}

func (r *rig) prompt(turnID string, seq uint64, text string) {
	r.handle.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Prompt{Prompt: &agentlinkpb.Prompt{
		TurnId: turnID, TurnSeq: seq, Text: text,
	}}})
}

func turnFinished(turnID string) func(*agentlinkpb.AgentEvent) bool {
	return func(ev *agentlinkpb.AgentEvent) bool {
		return ev.GetTurnFinished() != nil && ev.GetTurnId() == turnID
	}
}

func assertContiguous(t *testing.T, evs []*agentlinkpb.AgentEvent, first uint64) {
	t.Helper()
	for i, ev := range evs {
		if want := first + uint64(i); ev.GetSeq() != want {
			t.Fatalf("event %d has seq %d, want %d", i, ev.GetSeq(), want)
		}
	}
}

func messages(evs []*agentlinkpb.AgentEvent) string {
	var b strings.Builder
	for _, ev := range evs {
		b.WriteString(ev.GetMessage().GetText())
	}
	return b.String()
}

func TestTheSidecarSpeaksTheManagersProtocolVersion(t *testing.T) {
	if harnessclient.ProtocolVersion != agentlink.ProtocolVersion {
		t.Fatalf("sidecar protocol %d, manager protocol %d", harnessclient.ProtocolVersion, agentlink.ProtocolVersion)
	}
}

func TestAttachDeliversTheSessionConfiguration(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)
	if err := r.handle.AwaitReady(ctx); err != nil {
		t.Fatalf("AwaitReady: %v", err)
	}
	var got *agentlinkpb.SandboxConfig
	select {
	case got = <-r.configured:
	case <-time.After(5 * time.Second):
		t.Fatal("the sidecar was never handed its configuration")
	}
	if len(got.GetEndpoints()) != 1 || got.GetEndpoints()[0].GetName() != "mcp-gke" {
		t.Errorf("configuration = %v", got)
	}
}

func TestATurnFlowsBetweenTheManagerAndTheRunner(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)
	r.spawn(ctx)

	if st := r.next().GetState(); st == nil {
		t.Fatal("the first inbox frame is not the agent state")
	}
	ready := r.nextEvent()
	if ready.GetSessionReady().GetAcpSessionId() != "fake-session" || ready.GetSeq() != 1 {
		t.Fatalf("first event = %v, want SessionReady", ready)
	}
	r.handle.Ack(1)
	r.prompt("t1", 1, "say hello")
	evs := r.eventsUntil(turnFinished("t1"))
	assertContiguous(t, evs, 2)
	if got := messages(evs); got != "hello" {
		t.Fatalf("reply = %q, want hello", got)
	}
}

func TestATunnelDropReplaysWithoutGapsOrDuplicates(t *testing.T) {
	var mu sync.Mutex
	attached := 0
	r := newRig(t, agentlink.WithOnAttached(func(uint32, bool) { mu.Lock(); attached++; mu.Unlock() }))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r.start(ctx)
	r.spawn(ctx)
	gate := filepath.Join(t.TempDir(), "gate")

	ready := r.nextEvent()
	r.handle.Ack(ready.GetSeq())
	r.prompt("t1", 1, "say one\ntool c1 x\nwait "+gate+"\nsay two")
	before := r.eventsUntil(func(ev *agentlinkpb.AgentEvent) bool { return ev.GetToolCall() != nil })

	r.link.DropStreams(runID)
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	after := r.eventsUntil(turnFinished("t1"))
	all := append(append([]*agentlinkpb.AgentEvent{ready}, before...), after...)
	assertContiguous(t, all, 1)
	if got := messages(all); got != "onetwo" {
		t.Fatalf("reply = %q, want onetwo", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if attached < 2 {
		t.Errorf("the sidecar attached %d times, want a reattach after the drop", attached)
	}
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s ctxStream) Context() context.Context { return s.ctx }

type ioResetter struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

func (r *ioResetter) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
	}
}

func serveIntercepted(intercept grpc.StreamServerInterceptor) serveFunc {
	return func(t *testing.T, srv *agentlink.Server, assertion func() string) string {
		t.Helper()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		stamp := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			md, _ := metadata.FromIncomingContext(ss.Context())
			md = md.Copy()
			md.Set(agentlink.AssertionMetadataKey, assertion())
			return handler(srv, ctxStream{ServerStream: ss, ctx: metadata.NewIncomingContext(ss.Context(), md)})
		}
		gs := grpc.NewServer(grpc.ChainStreamInterceptor(stamp, intercept))
		srv.Register(gs)
		go func() { _ = gs.Serve(lis) }()
		t.Cleanup(gs.Stop)
		return lis.Addr().String()
	}
}

func (r *ioResetter) intercept(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if info.FullMethod != agentlinkpb.AgentLinkService_AgentIO_FullMethodName {
		return handler(srv, ss)
	}
	ctx, cancel := context.WithCancel(ss.Context())
	defer cancel()
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()
	err := handler(srv, ctxStream{ServerStream: ss, ctx: ctx})
	if ctx.Err() != nil && ss.Context().Err() == nil {
		return status.Error(codes.Unavailable, "agent io reset by a proxy")
	}
	return err
}

func TestAnAgentIOResetAloneKeepsTheTurnsFlowing(t *testing.T) {
	resetter := &ioResetter{}
	r := newRigWith(t, serveIntercepted(resetter.intercept), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r.start(ctx)
	r.spawn(ctx)
	ready := r.nextEvent()
	r.handle.Ack(ready.GetSeq())

	r.prompt("t1", 1, "say first")
	first := r.eventsUntil(turnFinished("t1"))
	resetter.reset()
	r.prompt("t2", 2, "say second")
	second := r.eventsUntil(turnFinished("t2"))
	if messages(first) != "first" || messages(second) != "second" {
		t.Fatalf("replies = %q, %q", messages(first), messages(second))
	}
	assertContiguous(t, append(first, second...), ready.GetSeq()+1)
}

func TestARestartedSidecarJoinsTheAgentThatRuns(t *testing.T) {
	var mu sync.Mutex
	var running []bool
	r := newRig(t, agentlink.WithOnAttached(func(_ uint32, agentRunning bool) {
		mu.Lock()
		running = append(running, agentRunning)
		mu.Unlock()
	}))
	gate := filepath.Join(t.TempDir(), "gate")
	firstCtx, crash := context.WithCancel(context.Background())
	defer crash()
	r.start(firstCtx)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r.spawn(ctx)
	ready := r.nextEvent()
	r.handle.Ack(ready.GetSeq())
	r.prompt("t1", 1, "say one\ntool c1 x\nwait "+gate+"\nsay two")
	before := r.eventsUntil(func(ev *agentlinkpb.AgentEvent) bool { return ev.GetToolCall() != nil })

	crash()
	<-r.runErr

	second := r.newClient(t, r.udsRunner(t))
	t.Cleanup(second.Close)
	go func() { _ = second.Run(ctx) }()
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	after := r.eventsUntil(turnFinished("t1"))
	assertContiguous(t, append(append([]*agentlinkpb.AgentEvent{ready}, before...), after...), 1)
	if got := messages(append(before, after...)); got != "onetwo" {
		t.Fatalf("reply = %q, want onetwo", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(running) < 2 || !running[len(running)-1] {
		t.Fatalf("agent_running on each attach = %v, want true for the restarted sidecar", running)
	}
}

func TestShutdownStopsTheAgent(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)
	r.spawn(ctx)
	r.nextEvent()
	r.handle.Shutdown("session_end")

	select {
	case err := <-r.runErr:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after Shutdown", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the client did not return after Shutdown")
	}

	stream, err := r.runner.client(t).Run(ctx)
	if err != nil {
		t.Fatalf("open runner stream: %v", err)
	}
	if err := stream.Send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Join{Join: &runnerpb.Join{}}}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if f, err := stream.Recv(); err != nil || f.GetStarted() == nil {
		t.Fatalf("join = %v, %v", f, err)
	}
	if err := stream.Send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Replay{Replay: &runnerpb.Replay{}}}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	for {
		f, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if f.GetEvent().GetExited() != nil {
			return
		}
	}
}

func TestLosingTheRunnerIsTerminal(t *testing.T) {
	reasons := make(chan string, 4)
	r := newRig(t, agentlink.WithOnError(func(reason string, _ error) { reasons <- reason }))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)
	r.spawn(ctx)
	r.nextEvent()

	r.runner.gs.Stop()
	select {
	case err := <-r.runErr:
		var terminal *harnessclient.TerminalError
		if !errors.As(err, &terminal) || terminal.Reason != harnessclient.ReasonRunnerLost {
			t.Fatalf("Run = %v, want a terminal %q", err, harnessclient.ReasonRunnerLost)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the client did not notice that the runner went away")
	}
	select {
	case reason := <-reasons:
		if reason != harnessclient.ReasonRunnerLost {
			t.Errorf("manager OnError reason = %q", reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the manager never heard about the lost runner")
	}
}

func TestUnknownRunIsTerminal(t *testing.T) {
	r := newRig(t)
	r.link.Forget(runID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)
	select {
	case err := <-r.runErr:
		var terminal *harnessclient.TerminalError
		if !errors.As(err, &terminal) || terminal.Reason != harnessclient.ReasonUnknownRun {
			t.Fatalf("err = %v, want a terminal %q", err, harnessclient.ReasonUnknownRun)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the client did not go terminal on an unknown run")
	}
}

type stalledRunner struct {
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (r *stalledRunner) Spawn(ctx context.Context, _ []byte, _ *agentlinkpb.SessionParams) (*harnessclient.AgentSession, error) {
	close(r.entered)
	select {
	case <-ctx.Done():
		close(r.canceled)
		return nil, ctx.Err()
	case <-r.release:
		return nil, errors.New("test released the stalled runner")
	}
}

func (r *stalledRunner) Join(context.Context) (*harnessclient.AgentSession, error) {
	return nil, harnessclient.ErrNoAgent
}

func TestShutdownDuringAStalledSpawnEndsCleanly(t *testing.T) {
	stalled := &stalledRunner{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(stalled.release) })
	r := newRigWith(t, agentlinktest.Serve, stalled)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)
	r.spawn(ctx)
	select {
	case <-stalled.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the runner was never asked to spawn")
	}
	r.handle.Shutdown("session_end")
	select {
	case err := <-r.runErr:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after Shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a stalled agent start kept the control loop from honoring Shutdown")
	}
	select {
	case <-stalled.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("ending the session never canceled the stalled agent start")
	}
}

type silentRunner struct {
	runnerpb.UnimplementedAgentRunnerServiceServer
	entered chan struct{}
}

func (s *silentRunner) Run(stream runnerpb.AgentRunnerService_RunServer) error {
	s.entered <- struct{}{}
	<-stream.Context().Done()
	return nil
}

func TestUDSRunnerSpawnStopsWaitingWhenCanceled(t *testing.T) {
	dir, err := os.MkdirTemp("", "rnr")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "r.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen on %s: %v", socket, err)
	}
	silent := &silentRunner{entered: make(chan struct{}, 1)}
	gs := grpc.NewServer()
	runnerpb.RegisterAgentRunnerServiceServer(gs, silent)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	agentRunner, err := harnessclient.NewUDSRunner(socket, testLogger(t))
	if err != nil {
		t.Fatalf("NewUDSRunner: %v", err)
	}
	t.Cleanup(agentRunner.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := agentRunner.Spawn(ctx, []byte("s"), &agentlinkpb.SessionParams{})
		done <- err
	}()
	select {
	case <-silent.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the spawn never reached the runner")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Spawn returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled spawn still waits for the runner to report Started")
	}
}

func TestJoinWithoutAnAgentReportsNoAgent(t *testing.T) {
	rs := startRunner(t)
	uds, err := harnessclient.NewUDSRunner(rs.socket, testLogger(t))
	if err != nil {
		t.Fatalf("NewUDSRunner: %v", err)
	}
	t.Cleanup(uds.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := uds.Join(ctx); !errors.Is(err, harnessclient.ErrNoAgent) {
		t.Fatalf("Join = %v, want ErrNoAgent", err)
	}
}

func denyAll(_ any, _ grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
	return status.Error(codes.PermissionDenied, "denied by policy")
}

func TestPermissionDeniedRefreshesThenGivesUp(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	srv, err := agentlink.New(idp.Verifier(t), agentlink.WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("agentlink.New: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.StreamInterceptor(denyAll))
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	rs := startRunner(t)
	agentRunner, err := harnessclient.NewUDSRunner(rs.socket, testLogger(t))
	if err != nil {
		t.Fatalf("NewUDSRunner: %v", err)
	}
	defer agentRunner.Close()

	token := &staticToken{bearer: "Bearer pom_art_test"}
	client, err := harnessclient.New(harnessclient.Config{
		URL: "http://" + lis.Addr().String(), Insecure: true,
		Token: token, Runner: agentRunner,
		BaseBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
		TokenRefreshTimeout: 100 * time.Millisecond,
		Logger:              testLogger(t),
	})
	if err != nil {
		t.Fatalf("harnessclient.New: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- client.Run(ctx) }()
	select {
	case err := <-errCh:
		var terminal *harnessclient.TerminalError
		if !errors.As(err, &terminal) || terminal.Reason != harnessclient.ReasonAttachDenied {
			t.Fatalf("err = %v, want a terminal %q", err, harnessclient.ReasonAttachDenied)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the client did not give up on a persistent denial")
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.refreshes == 0 {
		t.Error("a denial should have forced a token refresh")
	}
}

func TestDeniedAttachWaitsForTheRefreshedToken(t *testing.T) {
	const stale = "Bearer pom_art_stale"
	denials := make(chan struct{}, 64)
	denyStale := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		if auth := md.Get("authorization"); len(auth) > 0 && auth[0] == stale {
			select {
			case denials <- struct{}{}:
			default:
			}
			return status.Error(codes.PermissionDenied, "the run token expired")
		}
		return handler(srv, ss)
	}
	r := newRigWith(t, serveIntercepted(denyStale), nil)
	fresh := r.token.Bearer()
	r.token.set(stale)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)
	for range 6 {
		select {
		case <-denials:
		case err := <-r.runErr:
			t.Fatalf("Run gave up while the token refresh was still in flight: %v", err)
		case <-ctx.Done():
			t.Fatal("the client stopped retrying the stale token")
		}
	}
	r.token.set(fresh)
	if err := r.handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach with the refreshed token: %v", err)
	}
}

func TestAnIdleLinkWaitsForTheManagersAdvertisedHeartbeat(t *testing.T) {
	lost := make(chan error, 16)
	r := newRig(t, agentlink.WithOnLost(func(err error) { lost <- err }))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.start(ctx)
	if err := r.handle.AwaitReady(ctx); err != nil {
		t.Fatalf("AwaitReady: %v", err)
	}
	select {
	case err := <-lost:
		t.Fatalf("a healthy idle link was dropped: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
}

type flakyJoin struct {
	harnessclient.Runner
	mu       sync.Mutex
	failures int
}

func (r *flakyJoin) Join(ctx context.Context) (*harnessclient.AgentSession, error) {
	r.mu.Lock()
	fail := r.failures > 0
	if fail {
		r.failures--
	}
	r.mu.Unlock()
	if fail {
		return nil, status.Error(codes.Unavailable, "the runner is restarting")
	}
	return r.Runner.Join(ctx)
}

func TestARestartedSidecarRetriesAJoinThatFailsBeforeItAttaches(t *testing.T) {
	running := make(chan bool, 8)
	r := newRig(t, agentlink.WithOnAttached(func(_ uint32, agentRunning bool) { running <- agentRunning }))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	firstCtx, crash := context.WithCancel(ctx)
	defer crash()
	r.start(firstCtx)
	r.spawn(ctx)
	r.nextEvent()
	<-running

	crash()
	<-r.runErr

	second := r.newClient(t, &flakyJoin{Runner: r.udsRunner(t), failures: 2})
	t.Cleanup(second.Close)
	go func() { _ = second.Run(ctx) }()
	select {
	case agentRunning := <-running:
		if !agentRunning {
			t.Fatal("the restarted sidecar reported no agent after a Join that failed for a moment")
		}
	case <-ctx.Done():
		t.Fatal("the restarted sidecar never attached")
	}
}

type stoppedReader struct {
	runnerpb.UnimplementedAgentRunnerServiceServer
}

func (*stoppedReader) Run(s runnerpb.AgentRunnerService_RunServer) error {
	if _, err := s.Recv(); err != nil {
		return err
	}
	if err := s.Send(&runnerpb.RunnerServerFrame{
		Msg: &runnerpb.RunnerServerFrame_Started{
			Started: &runnerpb.Started{StreamId: []byte("s")},
		},
	}); err != nil {
		return err
	}
	<-s.Context().Done()
	return s.Context().Err()
}

func TestCloseInterruptsABlockedRunnerSend(t *testing.T) {
	dir, err := os.MkdirTemp("", "rnr")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "r.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	runnerpb.RegisterAgentRunnerServiceServer(gs, &stoppedReader{})
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	uds, err := harnessclient.NewUDSRunner(socket, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer uds.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ag, err := uds.Join(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for range 16 {
			if ag.Send(&runnerpb.RunnerClientFrame{
				Msg: &runnerpb.RunnerClientFrame_Prompt{
					Prompt: &agentlinkpb.Prompt{
						Text: strings.Repeat("x", 1<<20),
					},
				},
			}) != nil {
				return
			}
		}
	}()
	select {
	case <-sent:
		t.Fatal("expected the unread commands to block Send")
	case <-time.After(200 * time.Millisecond):
	}
	closed := make(chan struct{})
	go func() { ag.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close could not interrupt Send")
	}
}
