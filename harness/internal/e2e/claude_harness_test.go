//go:build e2e

package e2e

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/runner"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

const (
	agnoMCPName = "agno"

	agnoMCPURL = "https://docs.agno.com/mcp"

	agnoTurnID = "agno-turn-1"
)

func TestClaudeHarnessConnectsAgnoMCP(t *testing.T) {
	if os.Getenv(optInEnv) == "" {
		t.Skipf("opt-in e2e test; set %s=1 to run", optInEnv)
	}
	apiKey := resolveAnthropicKey(t)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	ctr := startHarness(ctx, t, apiKey)

	client := serveRunner(t, append([]string{"docker", "exec", "-i", ctr.GetContainerID()}, runner.AgentCommand...))

	runCtx, runCancel := context.WithTimeout(ctx, 6*time.Minute)
	defer runCancel()
	stream, err := client.Run(runCtx)
	if err != nil {
		t.Fatalf("open runner stream: %v", err)
	}
	run := &agentRun{t: t, stream: stream}

	started := run.spawn([]byte("e2e-agno"), &agentlinkpb.SessionParams{
		Cwd:        "/workspace",
		McpServers: []*agentlinkpb.McpServer{{Name: agnoMCPName, Url: agnoMCPURL}},
	})
	t.Logf("runner started the agent: pid=%d", started.GetPid())

	run.replay(0)
	ready := run.until(func(ev *agentlinkpb.AgentEvent) bool { return ev.GetSessionReady() != nil })
	t.Logf("ACP session ready: id=%q resumable=%v", ready.GetSessionReady().GetAcpSessionId(), ready.GetSessionReady().GetResumable())

	const prompt = "You have an MCP server named \"agno\" connected that exposes a tool to " +
		"search the Agno documentation. " +
		"Call that MCP tool now to search for \"agent sessions\", then reply with a " +
		"one-sentence summary. You MUST call the MCP tool; do not answer from memory."

	run.prompt(agnoTurnID, 1, prompt)
	done := run.until(func(ev *agentlinkpb.AgentEvent) bool {
		return ev.GetTurnFinished() != nil && ev.GetTurnId() == agnoTurnID
	}).GetTurnFinished()
	t.Logf("turn finished: stop_reason=%q", done.GetStopReason())
	run.dump()
	if done.GetError() != "" {
		t.Fatalf("Prompt: %s", done.GetError())
	}

	if !run.sawAgnoToolCall() {
		t.Fatalf("harness did not call the Agno MCP tool — it likely sees ZERO connected MCP servers.\n"+
			"  tool calls observed: %d\n  agent text: %q",
			len(run.toolCalls), strings.Join(run.messages, ""))
	}

	if n := len(run.permissions); n != 0 {
		t.Fatalf("expected 0 permission requests (unattended bypassPermissions), got %d — "+
			"the harness likely runs as root, where bypassPermissions is disabled", n)
	}

	run.stop()
	exited := run.until(func(ev *agentlinkpb.AgentEvent) bool { return ev.GetExited() != nil })
	t.Logf("agent exited: code=%d", exited.GetExited().GetExitCode())
}

func resolveAnthropicKey(t *testing.T) string {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("cannot resolve home dir for key file: %v", err)
	}
	path := filepath.Join(home, "tmp", "keys", "claude_api_key.txt")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("no Anthropic key: set ANTHROPIC_API_KEY or place a key at %s (%v)", path, err)
	}
	key := strings.TrimSpace(string(b))
	if key == "" {
		t.Fatalf("Anthropic key file %s is empty", path)
	}
	return key
}

func startHarness(ctx context.Context, t *testing.T, apiKey string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:        filepath.Join(repoRoot(t), "..", "deploy", "harness"),
			Dockerfile:     "claude-code/Dockerfile",
			KeepImage:      true,
			BuildLogWriter: newLineLogger(t, "[build]"),
		},
		Env: map[string]string{"ANTHROPIC_API_KEY": apiKey},

		WaitingFor: wait.ForExec([]string{"sh", "-lc", `command -v "$ACP_AGENT_CMD"`}).WithExitCode(0).WithStartupTimeout(3 * time.Minute),
	}

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start harness container: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_ = ctr.Terminate(cctx)
	})
	return ctr
}

func serveRunner(t *testing.T, command []string) runnerpb.AgentRunnerServiceClient {
	t.Helper()
	dir, err := os.MkdirTemp("", "e2e-runner")
	if err != nil {
		t.Fatalf("runner socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "runner.sock")

	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen on the runner socket: %v", err)
	}
	log := slog.New(slog.NewTextHandler(newLineLogger(t, "[runner]"), &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc := runner.New(runner.WithCommand(command), runner.WithLogger(log))
	gs := grpc.NewServer()
	svc.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		svc.Close()
	})

	conn, err := grpc.NewClient("unix:"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial the runner: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return runnerpb.NewAgentRunnerServiceClient(conn)
}

type agentRun struct {
	t           *testing.T
	stream      runnerpb.AgentRunnerService_RunClient
	messages    []string
	thoughts    int
	toolCalls   []*agentlinkpb.ToolCall
	permissions []*agentlinkpb.PermissionRequest
}

func (r *agentRun) send(f *runnerpb.RunnerClientFrame) {
	r.t.Helper()
	if err := r.stream.Send(f); err != nil {
		r.t.Fatalf("send %T to the runner: %v", f.GetMsg(), err)
	}
}

func (r *agentRun) recv() *runnerpb.RunnerServerFrame {
	r.t.Helper()
	f, err := r.stream.Recv()
	if err != nil {
		r.dump()
		r.t.Fatalf("receive from the runner: %v", err)
	}
	return f
}

func (r *agentRun) spawn(streamID []byte, params *agentlinkpb.SessionParams) *runnerpb.Started {
	r.t.Helper()
	r.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Spawn{Spawn: &runnerpb.Spawn{
		StreamId: streamID, Session: params,
	}}})
	f := r.recv()
	if f.GetStarted() == nil {
		r.t.Fatalf("first runner frame = %v, want Started", f)
	}
	return f.GetStarted()
}

func (r *agentRun) replay(after uint64) {
	r.t.Helper()
	r.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Replay{Replay: &runnerpb.Replay{After: after}}})
	if f := r.recv(); f.GetReplayStart() == nil {
		r.t.Fatalf("first frame after Replay = %v, want ReplayStart", f)
	}
}

func (r *agentRun) prompt(turnID string, seq uint64, text string) {
	r.t.Helper()
	r.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Prompt{Prompt: &agentlinkpb.Prompt{
		TurnId: turnID, TurnSeq: seq, Text: text,
	}}})
}

func (r *agentRun) stop() {
	r.t.Helper()
	r.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Stop{Stop: &runnerpb.Stop{}}})
}

func (r *agentRun) until(done func(*agentlinkpb.AgentEvent) bool) *agentlinkpb.AgentEvent {
	r.t.Helper()
	for {
		f := r.recv()
		ev := f.GetEvent()
		if ev == nil {
			r.t.Fatalf("runner frame = %v, want an event", f)
		}
		r.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Ack{Ack: &agentlinkpb.AgentIOAck{Consumed: ev.GetSeq()}}})
		r.record(ev)
		if done(ev) {
			return ev
		}
		switch {
		case ev.GetSessionFailed() != nil:
			r.dump()
			r.t.Fatalf("the ACP session did not open: %s", ev.GetSessionFailed().GetReason())
		case ev.GetExited() != nil:
			r.dump()
			r.t.Fatalf("the agent exited early with code %d", ev.GetExited().GetExitCode())
		}
	}
}

func (r *agentRun) record(ev *agentlinkpb.AgentEvent) {
	r.t.Helper()
	switch {
	case ev.GetMessage() != nil:
		r.messages = append(r.messages, ev.GetMessage().GetText())
	case ev.GetThought() != nil:
		r.thoughts++
	case ev.GetToolCall() != nil:
		c := ev.GetToolCall()
		r.toolCalls = append(r.toolCalls, c)
		r.t.Logf("[tool call] id=%q title=%q kind=%q status=%q update=%v", c.GetId(), c.GetTitle(), c.GetKind(), c.GetStatus(), c.GetUpdate())
	case ev.GetUsage() != nil:
		r.t.Logf("[usage] %v", ev.GetUsage())
	case ev.GetPermissionRequest() != nil:
		req := ev.GetPermissionRequest()
		r.permissions = append(r.permissions, req)
		r.t.Logf("[permission requested] tool_call_id=%q summary=%q options=%d", req.GetToolCallId(), req.GetSummary(), len(req.GetOptions()))
		decision := &agentlinkpb.PermissionDecision{RequestId: req.GetRequestId(), Cancelled: true}
		if opts := req.GetOptions(); len(opts) > 0 {
			decision = &agentlinkpb.PermissionDecision{RequestId: req.GetRequestId(), OptionId: opts[0].GetId()}
		}
		r.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Permission{Permission: decision}})
	}
}

func (r *agentRun) sawAgnoToolCall() bool {
	for _, c := range r.toolCalls {
		if strings.HasPrefix(c.GetTitle(), "mcp__"+agnoMCPName+"__") {
			return true
		}
	}
	return false
}

func (r *agentRun) dump() {
	r.t.Helper()
	r.t.Logf("agent messages (%d): %q", len(r.messages), strings.Join(r.messages, ""))
	r.t.Logf("agent thoughts: %d", r.thoughts)
	r.t.Logf("permission requests: %d", len(r.permissions))
	r.t.Logf("tool calls: %d", len(r.toolCalls))
	for i, c := range r.toolCalls {
		r.t.Logf("  [%d] id=%q title=%q kind=%q status=%q", i, c.GetId(), c.GetTitle(), c.GetKind(), c.GetStatus())
	}
}
