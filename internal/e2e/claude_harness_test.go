//go:build e2e

// Package e2e end-to-end test: prove the Claude harness can actually connect to
// an MCP server.
//
// Symptom this guards against: in the live deployment the sandboxed agent talks
// to the LLM fine but sees ZERO connected MCP servers, so no MCP-backed tools
// are ever available. This test drives the *exact* production harness image
// (deploy/harness/claude-code) inside a container via testcontainers, points it
// at a public, no-auth MCP server (https://docs.agno.com/mcp), and asserts the
// agent actually calls an MCP tool. Using a tokenless public server rules out
// OAuth/token misconfiguration: a failure here is the harness/adapter itself.
//
// It reuses the production ACP code path (sandbox.OpenSession + Session.Prompt
// + sandbox.EventSink); only the transport differs — the agent's stdio comes
// from `docker exec -i` instead of a Kubernetes pod-exec SPDY stream.
//
// Run it:
//
//	AGENTOPS_E2E=1 go test -tags e2e -timeout 15m ./internal/e2e/... -run Agno -v
package e2e

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/pomerium/agentops/internal/sandbox"
)

const (
	optInEnv = "AGENTOPS_E2E"

	// agnoMCPName is the server name we register. claude-code-acp prefixes MCP
	// tools with the server name (e.g. mcp__agno__search_agno), so this string
	// is what we look for in the tool-call events.
	agnoMCPName = "agno"
	// agnoMCPURL is a public, no-auth streamable-HTTP MCP server (Mintlify docs
	// MCP). It exposes a "search_agno" tool. No Authorization header required.
	agnoMCPURL = "https://docs.agno.com/mcp"
)

// TestClaudeHarnessConnectsAgnoMCP launches the harness, configures the Agno
// MCP server, asks the agent to call its MCP search tool, and asserts that the
// call actually happened. RED: the agent has no MCP tools (production bug).
// GREEN: an mcp__agno__* tool call is observed.
func TestClaudeHarnessConnectsAgnoMCP(t *testing.T) {
	if os.Getenv(optInEnv) == "" {
		t.Skipf("opt-in e2e test; set %s=1 to run", optInEnv)
	}
	apiKey := resolveAnthropicKey(t)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	ctr := startHarness(ctx, t, apiKey)

	// Exec the ACP agent over stdio, mirroring the prod pod-exec command.
	agent, closeAgent := dockerExecAgent(t, ctr.GetContainerID())

	sink := newRecordingSink(t)

	mcpServers := []acp.McpServer{{
		Http: &acp.McpServerHttpInline{
			Name: agnoMCPName,
			Type: "http",
			Url:  agnoMCPURL,
		},
	}}

	sess, err := sandbox.OpenSession(ctx, nil, sink, agent.stdin, agent.stdout, closeAgent, sandbox.SessionParams{
		Cwd:        "/workspace",
		MCPServers: mcpServers,
	})
	if err != nil {
		_ = closeAgent()
		t.Fatalf("OpenSession: %v", err)
	}
	defer func() { _ = sess.Close() }()

	promptCtx, promptCancel := context.WithTimeout(ctx, 4*time.Minute)
	defer promptCancel()

	const prompt = "You have an MCP server named \"agno\" connected that exposes a tool to " +
		"search the Agno documentation (the tool name contains \"search_agno\"). " +
		"Call that MCP tool now to search for \"agent sessions\", then reply with a " +
		"one-sentence summary. You MUST call the MCP tool; do not answer from memory."

	stop, err := sess.Prompt(promptCtx, prompt)
	if err != nil {
		sink.dump(t)
		t.Fatalf("Prompt: %v", err)
	}
	t.Logf("prompt stop reason: %s", stop)
	sink.dump(t)

	if !sink.sawAgnoToolCall() {
		t.Fatalf("harness did not call the Agno MCP tool — it likely sees ZERO connected MCP servers.\n"+
			"  tool calls observed: %d\n  agent text: %q",
			sink.toolCallCount(), sink.joinedMessages())
	}

	// Unattended operation: with bypassPermissions active the harness must never
	// ask the client to approve a tool. A request here means bypass is off (e.g.
	// the harness runs as root) and every MCP call would block on a Slack prompt.
	if n := sink.permissionCount(); n != 0 {
		t.Fatalf("expected 0 permission requests (unattended bypassPermissions), got %d — "+
			"the harness likely runs as root, where bypassPermissions is disabled", n)
	}
}

// resolveAnthropicKey returns the Anthropic API key from ANTHROPIC_API_KEY, or
// from ~/tmp/keys/claude_api_key.txt, skipping the test if neither is present.
func resolveAnthropicKey(t *testing.T) string {
	if v := strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("cannot resolve home dir for key file: %v", err)
	}
	path := filepath.Join(home, "tmp", "keys", "claude_api_key.txt")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("no Anthropic key: set ANTHROPIC_API_KEY or place a key at %s (%v)", path, err)
	}
	key := strings.TrimSpace(string(b))
	if key == "" {
		t.Skipf("Anthropic key file %s is empty", path)
	}
	return key
}

// startHarness builds the harness image from deploy/harness/claude-code and
// runs it (CMD: sleep infinity, matching prod). The ACP agent is exec'd per
// session.
func startHarness(ctx context.Context, t *testing.T, apiKey string) testcontainers.Container {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed; cannot locate harness build context")
	}
	harnessDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "deploy", "harness")

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:        harnessDir,
			Dockerfile:     "claude-code/Dockerfile",
			KeepImage:      true, // reuse across runs; the npm install is slow
			BuildLogWriter: &lineLogger{t: t, prefix: "[build]"},
		},
		Env: map[string]string{"ANTHROPIC_API_KEY": apiKey},
		// Confirm the ACP adapter (whatever ACP_AGENT_CMD names) is on PATH.
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

// agentStdio is the bidirectional stdio of the exec'd ACP agent process.
type agentStdio struct {
	stdin  io.WriteCloser
	stdout io.Reader
}

// dockerExecAgent execs the ACP agent inside the running container over stdio,
// the testcontainers analog of sandbox.spdyPodExecutor.OpenAgent. It uses
// `docker exec -i` (interactive, no TTY) so Docker does NOT multiplex
// stdout/stderr — ACP reads clean bytes off stdout, while stderr (where the
// adapter reports MCP connection failures) is drained into the test log.
func dockerExecAgent(t *testing.T, containerID string) (*agentStdio, func() error) {
	cmd := exec.Command("docker", "exec", "-i", containerID,
		"/bin/sh", "-lc", "exec ${ACP_AGENT_CMD:-claude-agent-acp}")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("agent stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("agent stdout pipe: %v", err)
	}
	cmd.Stderr = &lineLogger{t: t, prefix: "[agent stderr]"}

	if err := cmd.Start(); err != nil {
		t.Fatalf("docker exec start: %v", err)
	}

	closeFn := func() error {
		_ = stdin.Close() // EOF on stdin tells the adapter to exit
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		return nil
	}
	return &agentStdio{stdin: stdin, stdout: stdout}, closeFn
}

// recordingSink implements sandbox.EventSink, capturing agent output and every
// tool-call event for assertions.
type recordingSink struct {
	t           *testing.T
	mu          sync.Mutex
	messages    []string
	thoughts    []string
	toolCalls   []sandbox.ToolCallEvent
	permissions []sandbox.PermissionRequest
}

var _ sandbox.EventSink = (*recordingSink)(nil)

func newRecordingSink(t *testing.T) *recordingSink { return &recordingSink{t: t} }

func (s *recordingSink) AgentMessage(_ context.Context, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, text)
}

func (s *recordingSink) AgentThought(_ context.Context, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.thoughts = append(s.thoughts, text)
}

func (s *recordingSink) ToolCall(_ context.Context, ev sandbox.ToolCallEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.toolCalls = append(s.toolCalls, ev)
	s.t.Logf("[tool call] id=%q title=%q kind=%q status=%q update=%v", ev.ID, ev.Title, ev.Kind, ev.Status, ev.Update)
}

// Permission records the request and auto-approves. The session is meant to run
// in bypassPermissions (so the harness should never ask); recording lets the
// test assert that — auto-approving is only a safety net so a stray prompt can't
// hang the run. A non-zero count means unattended operation is broken (e.g. the
// harness runs as root, where bypassPermissions is unavailable).
func (s *recordingSink) Permission(_ context.Context, req sandbox.PermissionRequest) (sandbox.PermissionDecision, error) {
	s.mu.Lock()
	s.permissions = append(s.permissions, req)
	s.mu.Unlock()
	s.t.Logf("[permission requested] tool_call_id=%q title=%q options=%d", req.ToolCallID, req.Title, len(req.Options))
	if len(req.Options) > 0 {
		return sandbox.PermissionDecision{OptionID: req.Options[0].ID}, nil
	}
	return sandbox.PermissionDecision{Cancelled: true}, nil
}

// sawAgnoToolCall reports whether any recorded tool call references the Agno MCP
// server. With no other MCP servers configured, this is the unambiguous signal
// that the harness connected to it. Built-in harness tools (Bash, Read, ...) do
// NOT match, so a generic tool call won't produce a false green.
func (s *recordingSink) sawAgnoToolCall() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.toolCalls {
		hay := strings.ToLower(ev.ID + " " + ev.Title + " " + ev.Kind)
		if strings.Contains(hay, agnoMCPName) || strings.Contains(hay, "search_agno") {
			return true
		}
	}
	return false
}

func (s *recordingSink) toolCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.toolCalls)
}

func (s *recordingSink) permissionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.permissions)
}

func (s *recordingSink) joinedMessages() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.messages, "")
}

func (s *recordingSink) dump(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Logf("agent messages (%d): %q", len(s.messages), strings.Join(s.messages, ""))
	t.Logf("agent thoughts: %d", len(s.thoughts))
	t.Logf("permission requests: %d", len(s.permissions))
	t.Logf("tool calls: %d", len(s.toolCalls))
	for i, ev := range s.toolCalls {
		t.Logf("  [%d] id=%q title=%q kind=%q status=%q", i, ev.ID, ev.Title, ev.Kind, ev.Status)
	}
}

// lineLogger is an io.Writer that forwards complete lines to the test log. It is
// concurrency-safe (build logs and the exec'd process's stderr write from their
// own goroutines).
type lineLogger struct {
	t      *testing.T
	prefix string
	mu     sync.Mutex
	buf    []byte
}

func (w *lineLogger) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(w.buf[:i]), "\r")
		w.buf = w.buf[i+1:]
		if line != "" {
			w.t.Logf("%s %s", w.prefix, line)
		}
	}
	return len(p), nil
}
