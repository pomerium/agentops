package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/runner"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
	"github.com/pomerium/agentops/harness/internal/runner/runnertest"
)

func TestMain(m *testing.M) {
	runnertest.RunIfRequested()
	os.Exit(m.Run())
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

func serve(t *testing.T, command []string, opts ...runner.Option) runnerpb.AgentRunnerServiceClient {
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
	opts = append([]runner.Option{
		runner.WithCommand(command), runner.WithKillDelay(500 * time.Millisecond), runner.WithLogger(testLogger(t)),
	}, opts...)
	svc := runner.New(opts...)
	svc.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		svc.Close()
	})

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

type peer struct {
	t      *testing.T
	stream runnerpb.AgentRunnerService_RunClient
	cancel context.CancelFunc
}

func open(t *testing.T, client runnerpb.AgentRunnerServiceClient, first *runnerpb.RunnerClientFrame) (*peer, *runnerpb.Started, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	stream, err := client.Run(ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if err := stream.Send(first); err != nil {
		cancel()
		return nil, nil, err
	}
	f, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if f.GetStarted() == nil {
		cancel()
		t.Fatalf("first runner frame = %v, want Started", f)
	}
	p := &peer{t: t, stream: stream, cancel: cancel}
	t.Cleanup(cancel)
	return p, f.GetStarted(), nil
}

var testStreamID = []byte("stream-A")

func spawnFrame(params *agentlinkpb.SessionParams) *runnerpb.RunnerClientFrame {
	if params == nil {
		params = &agentlinkpb.SessionParams{Cwd: "/tmp"}
	}
	return &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Spawn{Spawn: &runnerpb.Spawn{
		StreamId: testStreamID, Session: params,
	}}}
}

func joinFrame() *runnerpb.RunnerClientFrame {
	return &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Join{Join: &runnerpb.Join{}}}
}

func mustSpawn(t *testing.T, client runnerpb.AgentRunnerServiceClient, params *agentlinkpb.SessionParams) (*peer, *runnerpb.Started) {
	t.Helper()
	p, started, err := open(t, client, spawnFrame(params))
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	return p, started
}

func mustJoin(t *testing.T, client runnerpb.AgentRunnerServiceClient) (*peer, *runnerpb.Started) {
	t.Helper()
	p, started, err := open(t, client, joinFrame())
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	return p, started
}

func (p *peer) send(f *runnerpb.RunnerClientFrame) {
	p.t.Helper()
	if err := p.stream.Send(f); err != nil {
		p.t.Fatalf("send %v: %v", f, err)
	}
}

func (p *peer) replay(after uint64) *agentlinkpb.AgentState {
	p.t.Helper()
	p.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Replay{Replay: &runnerpb.Replay{After: after}}})
	f, err := p.stream.Recv()
	if err != nil {
		p.t.Fatalf("recv after replay: %v", err)
	}
	if f.GetReplayStart() == nil {
		p.t.Fatalf("first frame after Replay = %v, want ReplayStart", f)
	}
	return f.GetReplayStart().GetState()
}

func (p *peer) ack(seq uint64) {
	p.t.Helper()
	p.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Ack{Ack: &agentlinkpb.AgentIOAck{Consumed: seq}}})
}

func (p *peer) prompt(turnID string, seq uint64, text string) {
	p.t.Helper()
	p.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Prompt{Prompt: &agentlinkpb.Prompt{
		TurnId: turnID, TurnSeq: seq, Text: text,
	}}})
}

func (p *peer) decide(requestID, option string) {
	p.t.Helper()
	p.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Permission{Permission: &agentlinkpb.PermissionDecision{
		RequestId: requestID, OptionId: option,
	}}})
}

func (p *peer) stop() {
	p.t.Helper()
	p.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Stop{Stop: &runnerpb.Stop{}}})
}

func (p *peer) event() *agentlinkpb.AgentEvent {
	p.t.Helper()
	f, err := p.stream.Recv()
	if err != nil {
		p.t.Fatalf("recv event: %v", err)
	}
	if f.GetEvent() == nil {
		p.t.Fatalf("frame = %v, want an event", f)
	}
	return f.GetEvent()
}

func (p *peer) until(done func(*agentlinkpb.AgentEvent) bool) []*agentlinkpb.AgentEvent {
	p.t.Helper()
	var out []*agentlinkpb.AgentEvent
	for {
		ev := p.event()
		out = append(out, ev)
		if done(ev) {
			return out
		}
	}
}

func finished(turnID string) func(*agentlinkpb.AgentEvent) bool {
	return func(ev *agentlinkpb.AgentEvent) bool {
		return ev.GetTurnFinished() != nil && ev.GetTurnId() == turnID
	}
}

func exited(ev *agentlinkpb.AgentEvent) bool { return ev.GetExited() != nil }

func describe(evs []*agentlinkpb.AgentEvent) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		var s string
		switch {
		case ev.GetSessionReady() != nil:
			s = "ready " + ev.GetSessionReady().GetAcpSessionId()
		case ev.GetSessionFailed() != nil:
			s = fmt.Sprintf("failed resume_unavailable=%v", ev.GetSessionFailed().GetResumeUnavailable())
		case ev.GetMessage() != nil:
			m := ev.GetMessage()
			s = fmt.Sprintf("message %s %q final=%v", m.GetPartId(), m.GetText(), m.GetFinal())
		case ev.GetThought() != nil:
			s = fmt.Sprintf("thought %q", ev.GetThought().GetText())
		case ev.GetToolCall() != nil:
			c := ev.GetToolCall()
			s = fmt.Sprintf("tool %s %q %s %s update=%v", c.GetId(), c.GetTitle(), c.GetKind(), c.GetStatus(), c.GetUpdate())
		case ev.GetUsage() != nil:
			s = fmt.Sprintf("usage %d", ev.GetUsage().GetTotalTokens())
		case ev.GetPermissionRequest() != nil:
			s = "permission " + ev.GetPermissionRequest().GetRequestId()
		case ev.GetTurnFinished() != nil:
			f := ev.GetTurnFinished()
			s = fmt.Sprintf("finished stop=%q error=%v", f.GetStopReason(), f.GetError() != "")
		case ev.GetExited() != nil:
			s = fmt.Sprintf("exited %d", ev.GetExited().GetExitCode())
		}
		if ev.GetTurnId() != "" {
			s = ev.GetTurnId() + ": " + s
		}
		out = append(out, s)
	}
	return out
}

func assertSeqs(t *testing.T, evs []*agentlinkpb.AgentEvent, first uint64) {
	t.Helper()
	for i, ev := range evs {
		if want := first + uint64(i); ev.GetSeq() != want {
			t.Fatalf("event %d has seq %d, want %d (events %v)", i, ev.GetSeq(), want, describe(evs))
		}
	}
}

func assertEvents(t *testing.T, evs []*agentlinkpb.AgentEvent, want ...string) {
	t.Helper()
	got := describe(evs)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func readyFrom(t *testing.T, p *peer) *agentlinkpb.AgentEvent {
	t.Helper()
	ev := p.event()
	if ev.GetSessionReady() == nil {
		t.Fatalf("first event = %v, want SessionReady", describe([]*agentlinkpb.AgentEvent{ev}))
	}
	return ev
}

func TestSpawnOpensTheACPSessionWithTheManagersParameters(t *testing.T) {
	record := filepath.Join(t.TempDir(), "acp.jsonl")
	client := serve(t, runnertest.Command(runnertest.EnvRecord+"="+record, runnertest.EnvOptions+"=model"))
	p, started := mustSpawn(t, client, &agentlinkpb.SessionParams{
		Cwd:          "/workspace",
		SystemPrompt: "be terse",
		McpServers:   []*agentlinkpb.McpServer{{Name: "github", Url: "http://127.0.0.1:7001/mcp"}},
		Config:       map[string]string{"model": "opus"},
	})
	if started.GetPid() <= 0 || string(started.GetStreamId()) != string(testStreamID) {
		t.Fatalf("Started = %v, want a pid and stream id %q", started, testStreamID)
	}
	if state := p.replay(0); state.GetLastTurnSeq() != 0 || len(state.GetOutstandingTurnIds()) != 0 {
		t.Fatalf("state of a new session = %v, want empty", state)
	}
	ready := readyFrom(t, p)
	if ready.GetSeq() != 1 || ready.GetSessionReady().GetAcpSessionId() != "fake-session" || !ready.GetSessionReady().GetResumable() {
		t.Fatalf("ready = %v, want seq 1, session fake-session, resumable", ready)
	}

	reqs, err := runnertest.ReadRecord(record)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var methods []string
	var newSession struct {
		Cwd        string            `json:"cwd"`
		Meta       map[string]any    `json:"_meta"`
		McpServers []json.RawMessage `json:"mcpServers"`
	}
	var setMode, setConfig string
	for _, r := range reqs {
		methods = append(methods, r.Method)
		switch r.Method {
		case "session/new":
			if err := json.Unmarshal(r.Params, &newSession); err != nil {
				t.Fatalf("decode session/new: %v", err)
			}
		case "session/set_mode":
			setMode = string(r.Params)
		case "session/set_config_option":
			setConfig = string(r.Params)
		}
	}
	if strings.Join(methods, ",") != "initialize,session/new,session/set_mode,session/set_config_option" {
		t.Fatalf("ACP requests = %v", methods)
	}
	if newSession.Cwd != "/workspace" || len(newSession.McpServers) != 1 ||
		!strings.Contains(string(newSession.McpServers[0]), `"url":"http://127.0.0.1:7001/mcp"`) {
		t.Errorf("session/new = %+v, mcp %s", newSession, newSession.McpServers)
	}
	if sp, _ := newSession.Meta["systemPrompt"].(map[string]any); sp["append"] != "be terse" {
		t.Errorf("session/new _meta = %v, want the system prompt appended", newSession.Meta)
	}
	if !strings.Contains(setMode, "bypassPermissions") {
		t.Errorf("set_mode = %s, want bypassPermissions", setMode)
	}
	if !strings.Contains(setConfig, `"value":"opus"`) {
		t.Errorf("set_config_option = %s, want model=opus", setConfig)
	}
}

func TestATurnStreamsAggregatedEventsAndFinishes(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)

	p.prompt("t1", 1, "think hmm\nsay hello\nsay  world\ntool c1 ls\ntooldone c1\nsay bye")
	evs := p.until(finished("t1"))
	assertSeqs(t, evs, 2)
	assertEvents(t, evs,
		`t1: thought "hmm"`,
		`t1: message t1.1 "hello world" final=false`,
		`t1: tool c1 "ls" execute pending update=false`,
		`t1: tool c1 "ls"  completed update=true`,
		`t1: message t1.2 "bye" final=true`,
		`t1: usage 30`,
		`t1: finished stop="end_turn" error=false`,
	)
	var input map[string]any
	if err := json.Unmarshal(evs[2].GetToolCall().GetRawInput(), &input); err != nil || input["command"] != "ls" {
		t.Errorf("tool raw input = %s (%v), want {\"command\":\"ls\"}", evs[2].GetToolCall().GetRawInput(), err)
	}
}

func TestTurnsRunInOrderAndARepeatedPromptIsIgnored(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)

	p.prompt("t1", 1, "say one")
	p.prompt("t1", 1, "say one")
	p.prompt("t2", 2, "say two")
	evs := p.until(finished("t2"))
	assertEvents(t, evs,
		`t1: message t1.1 "one" final=true`,
		`t1: usage 30`,
		`t1: finished stop="end_turn" error=false`,
		`t2: message t2.1 "two" final=true`,
		`t2: usage 30`,
		`t2: finished stop="end_turn" error=false`,
	)
	p2, _ := mustJoin(t, client)
	if state := p2.replay(evs[len(evs)-1].GetSeq()); state.GetLastTurnSeq() != 2 || len(state.GetOutstandingTurnIds()) != 0 {
		t.Errorf("state = %v, want last turn seq 2 and nothing outstanding", state)
	}
}

func TestReplaySendsTheEventsAfterThePointAndAcksTrimThem(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)
	p.prompt("t1", 1, "say a\ntool c1 x\nsay b")
	all := p.until(finished("t1"))
	last := all[len(all)-1].GetSeq()

	p2, _ := mustJoin(t, client)
	p2.replay(3)
	again := p2.until(finished("t1"))
	assertSeqs(t, again, 4)
	if len(again) != int(last-3) {
		t.Fatalf("replay after 3 sent %v, want seqs 4..%d", describe(again), last)
	}

	p2.ack(last)
	p3, _ := mustJoin(t, client)
	p3.replay(last)
	p3.prompt("t2", 2, "say c")
	next := p3.event()
	if next.GetSeq() != last+1 || next.GetTurnId() != "t2" {
		t.Fatalf("first event after the ack = %v, want seq %d of t2", describe([]*agentlinkpb.AgentEvent{next}), last+1)
	}

	p4, _ := mustJoin(t, client)
	p4.send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Replay{Replay: &runnerpb.Replay{After: 1}}})
	if _, err := p4.stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replay before the acked point: err = %v, want FailedPrecondition", err)
	}
}

func TestTheAgentKeepsWorkingWhileNoStreamIsJoined(t *testing.T) {
	gate := filepath.Join(t.TempDir(), "go")
	client := serve(t, runnertest.Command())
	p, started := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)
	p.prompt("t1", 1, "say before\ntool c1 x\nwait "+gate+"\nsay after")
	seen := p.until(func(ev *agentlinkpb.AgentEvent) bool { return ev.GetToolCall() != nil })
	p.cancel()

	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	p2, joined := mustJoin(t, client)
	if joined.GetPid() != started.GetPid() || string(joined.GetStreamId()) != string(testStreamID) {
		t.Fatalf("Join = %v, want the running agent %d", joined, started.GetPid())
	}
	state := p2.replay(seen[len(seen)-1].GetSeq())
	if state.GetLastTurnSeq() != 1 {
		t.Fatalf("state = %v, want last turn seq 1", state)
	}
	evs := p2.until(finished("t1"))
	assertEvents(t, evs,
		`t1: message t1.2 "after" final=true`,
		`t1: usage 30`,
		`t1: finished stop="end_turn" error=false`,
	)
	if !processAlive(started.GetPid()) {
		t.Errorf("agent %d is gone after the stream closed", started.GetPid())
	}
}

func TestAPermissionRequestWaitsForTheDecision(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)
	p.prompt("t1", 1, "tool c9 rm -rf\nsleep 100ms\nask c9")

	evs := p.until(func(ev *agentlinkpb.AgentEvent) bool { return ev.GetPermissionRequest() != nil })
	req := evs[len(evs)-1].GetPermissionRequest()
	if req.GetRequestId() != "c9" || req.GetToolCallId() != "c9" || req.GetSummary() != "rm -rf" ||
		req.GetTurnId() != "t1" || len(req.GetOptions()) != 2 || req.GetOptions()[0].GetId() != "allow" {
		t.Fatalf("permission request = %v", req)
	}

	p2, _ := mustJoin(t, client)
	state := p2.replay(evs[len(evs)-1].GetSeq())
	if len(state.GetPendingPermissions()) != 1 || state.GetPendingPermissions()[0].GetRequestId() != "c9" ||
		len(state.GetOutstandingTurnIds()) != 1 || state.GetOutstandingTurnIds()[0] != "t1" {
		t.Fatalf("state while waiting = %v", state)
	}
	p2.decide("unknown", "allow")
	p2.decide("c9", "allow")
	p2.decide("c9", "reject")
	rest := p2.until(finished("t1"))
	assertEvents(t, rest,
		`t1: message t1.1 "permission c9: allow" final=true`,
		`t1: usage 30`,
		`t1: finished stop="end_turn" error=false`,
	)
}

func TestStopEndsTheRunningTurnAndReportsExited(t *testing.T) {
	gate := filepath.Join(t.TempDir(), "never")
	client := serve(t, runnertest.Command())
	p, started := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)
	p.prompt("t1", 1, "wait "+gate)
	p.prompt("t2", 2, "say never")
	p.stop()

	evs := p.until(exited)
	got := describe(evs)
	want := []string{`t1: finished stop="" error=true`, `t2: finished stop="" error=true`}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("events after Stop = %v, want the two turns failed, then exited", got)
	}
	deadline := time.Now().Add(10 * time.Second)
	for processAlive(started.GetPid()) {
		if time.Now().After(deadline) {
			t.Fatalf("agent %d survived Stop", started.GetPid())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestAnAgentThatExitsMidTurnFailsItsTurnsThenReportsExited(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)
	p.prompt("t1", 1, "say partial\nexit 3")
	p.prompt("t2", 2, "say never")

	var rest []*agentlinkpb.AgentEvent
	for _, ev := range p.until(exited) {
		if ev.GetMessage() == nil {
			rest = append(rest, ev)
		}
	}
	assertEvents(t, rest,
		`t1: finished stop="" error=true`,
		`t2: finished stop="" error=true`,
		`exited 3`,
	)
}

func TestAnUnresumableSessionFailsAndTheAgentStops(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, &agentlinkpb.SessionParams{Cwd: "/tmp", ResumeSessionId: "gone"})
	p.replay(0)
	evs := p.until(exited)
	if len(evs) != 2 || evs[0].GetSessionFailed() == nil || !evs[0].GetSessionFailed().GetResumeUnavailable() {
		t.Fatalf("events = %v, want SessionFailed (resume unavailable) then exited", describe(evs))
	}
}

func TestResumingAKnownSessionSkipsSessionNew(t *testing.T) {
	record := filepath.Join(t.TempDir(), "acp.jsonl")
	client := serve(t, runnertest.Command(runnertest.EnvRecord+"="+record, runnertest.EnvKnown+"=earlier"))
	p, _ := mustSpawn(t, client, &agentlinkpb.SessionParams{Cwd: "/tmp", ResumeSessionId: "earlier"})
	p.replay(0)
	ready := readyFrom(t, p)
	if ready.GetSessionReady().GetAcpSessionId() != "earlier" {
		t.Fatalf("ready = %v, want the resumed session", ready)
	}
	reqs, _ := runnertest.ReadRecord(record)
	var methods []string
	for _, r := range reqs {
		methods = append(methods, r.Method)
	}
	if strings.Join(methods, ",") != "initialize,session/list,session/resume,session/set_mode" {
		t.Fatalf("ACP requests = %v", methods)
	}
}

func TestAnUnknownConfigOptionFailsTheSession(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, &agentlinkpb.SessionParams{Cwd: "/tmp", Config: map[string]string{"model": "opus"}})
	p.replay(0)
	evs := p.until(exited)
	if len(evs) != 2 || evs[0].GetSessionFailed() == nil || !strings.Contains(evs[0].GetSessionFailed().GetReason(), "model") {
		t.Fatalf("events = %v, want SessionFailed naming the option, then exited", describe(evs))
	}
}

func TestOnlyOneAgentSessionRuns(t *testing.T) {
	client := serve(t, runnertest.Command())
	if _, _, err := open(t, client, joinFrame()); status.Code(err) != codes.NotFound {
		t.Fatalf("join without a session: err = %v, want NotFound", err)
	}
	p, started := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)

	_, again, err := open(t, client, spawnFrame(nil))
	if err != nil || again.GetPid() != started.GetPid() {
		t.Fatalf("spawn with the same stream id = %v, %v; want it to join pid %d", again, err, started.GetPid())
	}
	other := &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Spawn{Spawn: &runnerpb.Spawn{
		StreamId: []byte("stream-B"), Session: &agentlinkpb.SessionParams{Cwd: "/tmp"},
	}}}
	if _, _, err := open(t, client, other); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("spawn with another stream id: err = %v, want AlreadyExists", err)
	}
}

func TestANewStreamReplacesTheOldOne(t *testing.T) {
	client := serve(t, runnertest.Command())
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	readyFrom(t, p)
	p2, _ := mustJoin(t, client)
	p2.replay(1)
	if _, err := p.stream.Recv(); err == nil {
		t.Fatal("the replaced stream is still open")
	}
}

func TestAnAgentThatIsNotACPFailsTheSession(t *testing.T) {
	client := serve(t, []string{"/bin/sh", "-c", "exit 4"})
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	evs := p.until(exited)
	assertEvents(t, evs, `failed resume_unavailable=false`, `exited 4`)
}

func TestAgentExitStopsTheChildrenItLeft(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child")
	client := serve(t, []string{"/bin/sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $! > " + pidFile + "; exit 0"})
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	p.until(exited)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	child, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		t.Fatalf("child pid from %q: %v", data, err)
	}
	defer func() { _ = syscall.Kill(int(child), syscall.SIGKILL) }()
	deadline := time.Now().Add(10 * time.Second)
	for processAlive(child) {
		if time.Now().After(deadline) {
			t.Fatalf("child %d survived the agent's exit", child)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestAnEscapedGrandchildHoldingStdoutDoesNotHoldUpExited(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is not installed")
	}
	pidFile := filepath.Join(t.TempDir(), "holder")
	client := serve(t, []string{"/bin/sh", "-c", "setsid sleep 60 & echo $! > " + pidFile + "; exit 3"})
	p, _ := mustSpawn(t, client, nil)
	p.replay(0)
	evs := p.until(exited)
	if data, err := os.ReadFile(pidFile); err == nil {
		if holder, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
			_ = syscall.Kill(holder, syscall.SIGKILL)
		}
	}
	if code := evs[len(evs)-1].GetExited().GetExitCode(); code != 3 {
		t.Errorf("exit code = %d, want 3", code)
	}
}

func processAlive(pid int64) bool {
	if stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		_, state, _ := strings.Cut(string(stat), ") ")
		return !strings.HasPrefix(state, "Z")
	}
	proc, err := os.FindProcess(int(pid))
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
