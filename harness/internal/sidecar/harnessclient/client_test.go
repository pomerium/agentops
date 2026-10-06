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
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/agentlink/agentlinktest"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/runner"
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

func newRig(t *testing.T, opts ...agentlink.ExpectOption) *rig {
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
	addr := agentlinktest.Serve(t, srv, func() string { return idp.SignFor(t, runID, seal) })

	handle, err := srv.Expect(runID, seal, sessionConfig(), opts...)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}

	socket := startRunner(t, stubAgent(t))
	agentRunner, err := harnessclient.NewUDSRunner(socket, testLogger(t))
	if err != nil {
		t.Fatalf("NewUDSRunner: %v", err)
	}
	t.Cleanup(agentRunner.Close)

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
		Logger: testLogger(t),
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
