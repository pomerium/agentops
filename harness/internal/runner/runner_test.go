package runner_test

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/runner"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
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

func serve(t *testing.T, command []string) runnerpb.AgentRunnerServiceClient {
	t.Helper()
	dir, err := os.MkdirTemp("", "rnr")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "r.sock")

	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	runner.New(runner.WithCommand(command), runner.WithKillDelay(500*time.Millisecond), runner.WithLogger(testLogger(t))).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("unix:"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return runnerpb.NewAgentRunnerServiceClient(conn)
}

func spawn(t *testing.T, ctx context.Context, client runnerpb.AgentRunnerServiceClient) (runnerpb.AgentRunnerService_RunClient, int64) {
	t.Helper()
	stream, err := client.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := stream.Send(&runnerpb.RunnerClientFrame{
		Msg: &runnerpb.RunnerClientFrame_Spawn{Spawn: &runnerpb.Spawn{}},
	}); err != nil {
		t.Fatalf("send spawn: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv started: %v", err)
	}
	started := first.GetStarted()
	if started == nil {
		t.Fatalf("first frame was %v, want Started", first)
	}
	return stream, started.GetPid()
}

func TestRunBridgesStdioAndReportsExit(t *testing.T) {
	client := serve(t, []string{"/bin/sh", "-c", `
echo "note" >&2
IFS= read -r line
printf 'got:%s\n' "$line"
exit 5`})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream, pid := spawn(t, ctx, client)
	if pid <= 0 {
		t.Errorf("pid = %d", pid)
	}
	if err := stream.Send(&runnerpb.RunnerClientFrame{
		Msg: &runnerpb.RunnerClientFrame_Stdin{Stdin: []byte("hello\n")},
	}); err != nil {
		t.Fatalf("send stdin: %v", err)
	}

	var stdout, stderr []byte
	var code int32 = -99
	for code == -99 {
		frame, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		switch {
		case frame.GetStdout() != nil:
			stdout = append(stdout, frame.GetStdout()...)
		case frame.GetStderr() != nil:
			stderr = append(stderr, frame.GetStderr()...)
		case frame.GetExited() != nil:
			code = frame.GetExited().GetExitCode()
		}
	}
	if string(stdout) != "got:hello\n" {
		t.Errorf("stdout = %q", stdout)
	}
	if string(stderr) != "note\n" {
		t.Errorf("stderr = %q", stderr)
	}
	if code != 5 {
		t.Errorf("exit code = %d, want 5", code)
	}
}

func TestSecondConcurrentRunIsRejected(t *testing.T) {
	client := serve(t, []string{"/bin/sh", "-c", "sleep 30"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	first, _ := spawn(t, ctx, client)
	defer func() { _ = first.CloseSend() }()

	second, err := client.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := second.Send(&runnerpb.RunnerClientFrame{
		Msg: &runnerpb.RunnerClientFrame_Spawn{Spawn: &runnerpb.Spawn{}},
	}); err != nil {
		t.Fatalf("send spawn: %v", err)
	}
	if _, err := second.Recv(); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second Run err = %v (code %s), want AlreadyExists", err, status.Code(err))
	}
}

func TestStreamDropKillsTheAgent(t *testing.T) {
	client := serve(t, []string{"/bin/sh", "-c", "sleep 60"})
	ctx, cancel := context.WithCancel(context.Background())

	_, pid := spawn(t, ctx, client)
	if !processAlive(pid) {
		t.Fatalf("agent %d is not running", pid)
	}
	cancel()

	deadline := time.Now().Add(10 * time.Second)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("agent %d survived the dropped stream", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestStreamDropKillsAnAgentThatStoppedReadingStdin(t *testing.T) {
	client := serve(t, []string{"/bin/sh", "-c", "head -c 1 >/dev/null; echo reading; exec sleep 60"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, pid := spawn(t, ctx, client)
	defer func() { _ = syscall.Kill(-int(pid), syscall.SIGKILL) }()
	if err := stream.Send(&runnerpb.RunnerClientFrame{
		Msg: &runnerpb.RunnerClientFrame_Stdin{Stdin: make([]byte, 1<<20)},
	}); err != nil {
		t.Fatalf("send stdin: %v", err)
	}
	frame, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if string(frame.GetStdout()) != "reading\n" {
		t.Fatalf("frame = %v, want stdout %q", frame, "reading\n")
	}
	cancel()

	deadline := time.Now().Add(10 * time.Second)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("agent %d survived the dropped stream while its stdin was full", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func processAlive(pid int64) bool {
	proc, err := os.FindProcess(int(pid))
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
