package harnessclient_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/agentlink/agentlinktest"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/runner"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
	"github.com/pomerium/agentops/harness/internal/sidecar/harnessclient"
)

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

func stubAgent(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.sh")
	script := `#!/bin/sh
echo "agent-ready" >&2
while IFS= read -r line; do
  case "$line" in
    quit) exit 7 ;;
  esac
  printf 'echo:%s\n' "$line"
done
exit 0
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write stub agent: %v", err)
	}
	return path
}

func startRunner(t *testing.T, agentPath string) string {
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
		runner.WithCommand([]string{"/bin/sh", "-c", "exec " + agentPath}),
		runner.WithKillDelay(time.Second),
		runner.WithLogger(testLogger(t)),
	)
	gs := grpc.NewServer()
	svc.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return socket
}

type rig struct {
	link       *agentlink.Server
	handle     *agentlink.RunHandle
	client     *harnessclient.Client
	token      *staticToken
	runErr     chan error
	configured chan *agentlinkpb.SandboxConfig
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

	if agentRunner == nil {
		uds, err := harnessclient.NewUDSRunner(startRunner(t, stubAgent(t)), testLogger(t))
		if err != nil {
			t.Fatalf("NewUDSRunner: %v", err)
		}
		t.Cleanup(uds.Close)
		agentRunner = uds
	}

	token := &staticToken{bearer: "Bearer pom_art_test"}
	configured := make(chan *agentlinkpb.SandboxConfig, 1)
	client, err := harnessclient.New(harnessclient.Config{
		URL:                "http://" + addr,
		Insecure:           true,
		Token:              token,
		Runner:             agentRunner,
		HeartbeatInterval:  200 * time.Millisecond,
		HeartbeatMissLimit: 3,
		BaseBackoff:        20 * time.Millisecond,
		MaxBackoff:         100 * time.Millisecond,
		Logger:             testLogger(t),
		Configure: func(cfg *agentlinkpb.SandboxConfig) error {
			select {
			case configured <- cfg:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("harnessclient.New: %v", err)
	}
	t.Cleanup(client.Close)

	return &rig{
		link: srv, handle: handle, client: client, token: token,
		runErr: make(chan error, 1), configured: configured,
	}
}

func (r *rig) start(ctx context.Context) {
	go func() { r.runErr <- r.client.Run(ctx) }()
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
	if len(got.GetEndpoints()) != 1 {
		t.Fatalf("configured with %d endpoints, want 1: %+v", len(got.GetEndpoints()), got.GetEndpoints())
	}
	ep := got.GetEndpoints()[0]
	if ep.GetName() != "mcp-gke" || ep.GetListenPort() != 9101 ||
		ep.GetUpstreamUrl() != "https://gke.example.com/mcp" {
		t.Errorf("endpoint crossed the wire as %+v", ep)
	}
}

func TestHappyPath(t *testing.T) {
	exited := make(chan int32, 1)
	r := newRig(t, agentlink.WithOnAgentExit(func(code int32) { exited <- code }))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)

	if err := r.handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	if err := r.handle.SpawnAgent(ctx); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	stdin, stdout, err := r.handle.AwaitAgentIO(ctx)
	if err != nil {
		t.Fatalf("AwaitAgentIO: %v", err)
	}

	reader := bufio.NewReader(stdout)
	for i := range 5 {
		line := fmt.Sprintf("line-%d\n", i)
		if _, err := stdin.Write([]byte(line)); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if want := "echo:line-" + fmt.Sprint(i) + "\n"; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}

	if _, err := stdin.Write([]byte("quit\n")); err != nil {
		t.Fatalf("write quit: %v", err)
	}
	select {
	case code := <-exited:
		if code != 7 {
			t.Errorf("agent exit code = %d, want 7", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the agent exit was never reported")
	}
}

func TestResumeAcrossTunnelDrop(t *testing.T) {
	var lost, attached int
	var mu sync.Mutex
	r := newRig(t,
		agentlink.WithOnLost(func(error) { mu.Lock(); lost++; mu.Unlock() }),
		agentlink.WithOnAttached(func(uint32, bool) { mu.Lock(); attached++; mu.Unlock() }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r.start(ctx)

	if err := r.handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	if err := r.handle.SpawnAgent(ctx); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	stdin, stdout, err := r.handle.AwaitAgentIO(ctx)
	if err != nil {
		t.Fatalf("AwaitAgentIO: %v", err)
	}
	reader := bufio.NewReader(stdout)

	exchange := func(i int) {
		t.Helper()
		line := fmt.Sprintf("turn-%d\n", i)
		if _, err := stdin.Write([]byte(line)); err != nil {
			t.Fatalf("turn %d: write: %v", i, err)
		}
		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("turn %d: read: %v", i, err)
		}
		if want := fmt.Sprintf("echo:turn-%d\n", i); got != want {
			t.Fatalf("turn %d: got %q, want %q", i, got, want)
		}
	}

	exchange(0)

	r.link.DropStreams(runID)

	deadline := time.Now().Add(30 * time.Second)
	for {
		mu.Lock()
		reattached := attached >= 2
		mu.Unlock()
		if reattached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the sidecar never re-attached after the drop")
		}
		time.Sleep(20 * time.Millisecond)
	}

	for i := 1; i < 4; i++ {
		exchange(i)
	}

	mu.Lock()
	defer mu.Unlock()
	if lost == 0 {
		t.Error("the dropped control stream should have been reported lost")
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

func TestAgentIOResetAloneKeepsTheConversationFlowing(t *testing.T) {
	resetter := &ioResetter{}
	r := newRigWith(t, serveIntercepted(resetter.intercept), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r.start(ctx)

	if err := r.handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	if err := r.handle.SpawnAgent(ctx); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	stdin, stdout, err := r.handle.AwaitAgentIO(ctx)
	if err != nil {
		t.Fatalf("AwaitAgentIO: %v", err)
	}
	lines := make(chan string)
	go func() {
		reader := bufio.NewReader(stdout)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			select {
			case lines <- line:
			case <-ctx.Done():
				return
			}
		}
	}()
	exchange := func(i int) {
		t.Helper()
		if _, err := fmt.Fprintf(stdin, "turn-%d\n", i); err != nil {
			t.Fatalf("turn %d: write: %v", i, err)
		}
		select {
		case got := <-lines:
			if want := fmt.Sprintf("echo:turn-%d\n", i); got != want {
				t.Fatalf("turn %d: got %q, want %q", i, got, want)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("turn %d: no reply after the agent io reset", i)
		}
	}

	exchange(0)
	resetter.reset()
	for i := 1; i < 4; i++ {
		exchange(i)
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
		if !errors.As(err, &terminal) {
			t.Fatalf("err = %v, want a TerminalError", err)
		}
		if terminal.Reason != harnessclient.ReasonUnknownRun {
			t.Errorf("reason = %q, want %q", terminal.Reason, harnessclient.ReasonUnknownRun)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the client did not go terminal on an unknown run")
	}
}

func TestShutdownEndsCleanly(t *testing.T) {
	r := newRig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)

	if err := r.handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	if err := r.handle.SpawnAgent(ctx); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if _, _, err := r.handle.AwaitAgentIO(ctx); err != nil {
		t.Fatalf("AwaitAgentIO: %v", err)
	}
	r.handle.Shutdown("session_end")

	select {
	case err := <-r.runErr:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after Shutdown", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the client did not exit after Shutdown")
	}
}

type stalledRunner struct {
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (r *stalledRunner) Spawn(ctx context.Context) (*harnessclient.AgentSession, error) {
	close(r.entered)
	select {
	case <-ctx.Done():
		close(r.canceled)
		return nil, ctx.Err()
	case <-r.release:
		return nil, errors.New("test released the stalled runner")
	}
}

func TestShutdownDuringAStalledSpawnEndsCleanly(t *testing.T) {
	stalled := &stalledRunner{
		entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}),
	}
	t.Cleanup(func() { close(stalled.release) })
	r := newRigWith(t, agentlinktest.Serve, stalled)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)

	if err := r.handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	if err := r.handle.SpawnAgent(ctx); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
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
		_, err := agentRunner.Spawn(ctx)
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

	socket := startRunner(t, stubAgent(t))
	agentRunner, err := harnessclient.NewUDSRunner(socket, testLogger(t))
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

func denyAll(_ any, _ grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
	return status.Error(codes.PermissionDenied, "denied by policy")
}

func TestStderrNeverEntersACP(t *testing.T) {
	r := newRig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.start(ctx)

	if err := r.handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	if err := r.handle.SpawnAgent(ctx); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	stdin, stdout, err := r.handle.AwaitAgentIO(ctx)
	if err != nil {
		t.Fatalf("AwaitAgentIO: %v", err)
	}
	if _, err := stdin.Write([]byte("hello\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Contains(got, "agent-ready") {
		t.Fatalf("stderr leaked into the ACP stream: %q", got)
	}
	if got != "echo:hello\n" {
		t.Fatalf("got %q", got)
	}
}
