package agentlink_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/pomerium/agentops/harness/internal/agentlink"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

type ioPeer struct {
	t      *testing.T
	stream agentlinkpb.AgentLinkService_AgentIOClient
	cancel context.CancelFunc
	open   *agentlinkpb.AgentIOOpen
}

func (g *testLink) openIO(parent context.Context, runID string, streamID []byte) (*ioPeer, error) {
	g.t.Helper()
	ctx, cancel := context.WithCancel(g.ctx(parent, runID, nil))
	stream, err := g.client.AgentIO(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := stream.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Open{Open: &agentlinkpb.AgentIOOpen{StreamId: streamID}}}); err != nil && !errors.Is(err, io.EOF) {
		cancel()
		return nil, err
	}
	f, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, err
	}
	if f.GetOpen() == nil {
		cancel()
		g.t.Fatalf("first manager AgentIO frame = %v, want Open", f)
	}
	return &ioPeer{t: g.t, stream: stream, cancel: cancel, open: f.GetOpen()}, nil
}

func (p *ioPeer) send(f *agentlinkpb.AgentIOFrame) {
	p.t.Helper()
	if err := p.stream.Send(f); err != nil {
		p.t.Fatalf("send: %v", err)
	}
}

func (p *ioPeer) state(st *agentlinkpb.AgentState) {
	p.t.Helper()
	p.send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_State{State: st}})
}

func (p *ioPeer) event(seq uint64, text string) {
	p.t.Helper()
	p.send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Event{Event: &agentlinkpb.AgentEvent{
		Seq: seq, TurnId: "t1", Payload: &agentlinkpb.AgentEvent_Thought{Thought: &agentlinkpb.AgentThought{Text: text}},
	}}})
}

func (p *ioPeer) recv() *agentlinkpb.AgentIOFrame {
	p.t.Helper()
	f, err := p.stream.Recv()
	if err != nil {
		p.t.Fatalf("recv: %v", err)
	}
	return f
}

func inbox(t *testing.T, h *agentlink.RunHandle) *agentlinkpb.AgentIOFrame {
	t.Helper()
	select {
	case f := <-h.Inbox():
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("nothing reached the inbox")
		return nil
	}
}

func assertInboxEmpty(t *testing.T, h *agentlink.RunHandle) {
	t.Helper()
	select {
	case f := <-h.Inbox():
		t.Fatalf("unexpected inbox frame %v", f)
	case <-time.After(100 * time.Millisecond):
	}
}

func expectAttached(t *testing.T, g *testLink, runID string, opts ...agentlink.ExpectOption) (*agentlink.RunHandle, context.Context) {
	t.Helper()
	handle, err := g.srv.Expect(runID, testSeal, nil, opts...)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	t.Cleanup(func() { g.srv.Forget(runID) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	if _, _, err := g.attach(g.ctx(ctx, runID, nil), 1, false); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	return handle, ctx
}

func TestAgentIORequiresALiveAttach(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, err := g.srv.Expect("run-noattach", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget("run-noattach")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := g.openIO(ctx, "run-noattach", handle.StreamID()); codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
}

func TestSpawnAgentCarriesTheStreamAndTheSessionParameters(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, err := g.srv.Expect("run-spawn", testSeal, nil)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	defer g.srv.Forget("run-spawn")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	attach, _, err := g.attach(g.ctx(ctx, "run-spawn", nil), 1, false)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := handle.AwaitAttach(ctx); err != nil {
		t.Fatalf("AwaitAttach: %v", err)
	}
	params := &agentlinkpb.SessionParams{Cwd: "/workspace", ResumeSessionId: "acp-1"}
	if err := handle.SpawnAgent(ctx, params); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	for {
		f, err := attach.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if spawn := f.GetSpawn(); spawn != nil {
			if len(handle.StreamID()) == 0 || !bytes.Equal(spawn.GetStreamId(), handle.StreamID()) {
				t.Errorf("SpawnAgent stream_id = %q, want %q", spawn.GetStreamId(), handle.StreamID())
			}
			if spawn.GetSession().GetCwd() != "/workspace" || spawn.GetSession().GetResumeSessionId() != "acp-1" {
				t.Errorf("SpawnAgent session = %v", spawn.GetSession())
			}
			return
		}
	}
}

func TestAgentIODeliversTheStateThenTheEventsInOrder(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, ctx := expectAttached(t, g, "run-order")
	p, err := g.openIO(ctx, "run-order", handle.StreamID())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if p.open.GetConsumed() != 0 || !bytes.Equal(p.open.GetStreamId(), handle.StreamID()) {
		t.Fatalf("manager Open = %v, want consumed 0 and the run's stream", p.open)
	}
	p.state(&agentlinkpb.AgentState{LastTurnSeq: 4})
	p.event(1, "a")
	p.event(2, "b")
	if st := inbox(t, handle).GetState(); st.GetLastTurnSeq() != 4 {
		t.Fatalf("first inbox frame = %v, want the state", st)
	}
	for _, want := range []uint64{1, 2} {
		if ev := inbox(t, handle).GetEvent(); ev.GetSeq() != want {
			t.Fatalf("inbox event = %v, want seq %d", ev, want)
		}
	}
}

func TestAReopenedAgentIOResumesAfterTheDeliveredEvents(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, ctx := expectAttached(t, g, "run-resume")
	p, err := g.openIO(ctx, "run-resume", handle.StreamID())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	p.state(&agentlinkpb.AgentState{})
	p.event(1, "a")
	p.event(2, "b")
	inbox(t, handle)
	inbox(t, handle)
	inbox(t, handle)
	p.cancel()

	p2, err := g.openIO(ctx, "run-resume", handle.StreamID())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if p2.open.GetConsumed() != 2 {
		t.Fatalf("manager Open on reopen = %v, want consumed 2", p2.open)
	}
	p2.state(&agentlinkpb.AgentState{})
	p2.event(2, "b again")
	p2.event(3, "c")
	if inbox(t, handle).GetState() == nil {
		t.Fatal("the reopen did not deliver its state first")
	}
	if ev := inbox(t, handle).GetEvent(); ev.GetSeq() != 3 {
		t.Fatalf("inbox event = %v, want seq 3 (2 was delivered before)", ev)
	}
}

func TestAnEventGapIsTerminal(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	var mu sync.Mutex
	var reasons []string
	handle, ctx := expectAttached(t, g, "run-gap", agentlink.WithOnError(func(reason string, _ error) {
		mu.Lock()
		reasons = append(reasons, reason)
		mu.Unlock()
	}))
	p, err := g.openIO(ctx, "run-gap", handle.StreamID())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	p.state(&agentlinkpb.AgentState{})
	p.event(1, "a")
	p.event(3, "c")
	if _, err := p.stream.Recv(); codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != "protocol_violation" {
		t.Errorf("OnError reasons = %v", reasons)
	}
}

func TestAgentIOForAnotherStreamIsTerminal(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	var mu sync.Mutex
	var reasons []string
	_, ctx := expectAttached(t, g, "run-stale", agentlink.WithOnError(func(reason string, _ error) {
		mu.Lock()
		reasons = append(reasons, reason)
		mu.Unlock()
	}))
	if _, err := g.openIO(ctx, "run-stale", []byte("an earlier stream")); codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != "agentio_stream_mismatch" {
		t.Errorf("OnError reasons = %v", reasons)
	}
}

func TestCommandsWaitForAStreamAndAcksReachTheSidecar(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, ctx := expectAttached(t, g, "run-cmd")
	handle.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Prompt{Prompt: &agentlinkpb.Prompt{
		TurnId: "t1", TurnSeq: 1, Text: "hello",
	}}})
	p, err := g.openIO(ctx, "run-cmd", handle.StreamID())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if prompt := p.recv().GetPrompt(); prompt.GetTurnId() != "t1" || prompt.GetText() != "hello" {
		t.Fatalf("first frame after Open = %v, want the queued prompt", prompt)
	}
	p.state(&agentlinkpb.AgentState{})
	p.event(1, "a")
	inbox(t, handle)
	inbox(t, handle)
	handle.Ack(1)
	if ack := p.recv().GetAck(); ack.GetConsumed() != 1 {
		t.Fatalf("frame after Ack(1) = %v, want ack 1", ack)
	}
	handle.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Permission{Permission: &agentlinkpb.PermissionDecision{
		RequestId: "c1", OptionId: "allow",
	}}})
	if d := p.recv().GetPermission(); d.GetRequestId() != "c1" {
		t.Fatalf("frame after Send = %v, want the decision", d)
	}
}

func TestExpectCanResumeAKnownStreamAfterAPoint(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	stream := []byte("persisted-stream")
	handle, ctx := expectAttached(t, g, "run-adopt", agentlink.WithStreamID(stream), agentlink.WithResumeAfter(7))
	if !bytes.Equal(handle.StreamID(), stream) {
		t.Fatalf("StreamID = %q, want %q", handle.StreamID(), stream)
	}
	p, err := g.openIO(ctx, "run-adopt", stream)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if p.open.GetConsumed() != 7 {
		t.Fatalf("manager Open = %v, want consumed 7", p.open)
	}
	p.state(&agentlinkpb.AgentState{})
	p.event(7, "old")
	p.event(8, "new")
	inbox(t, handle)
	if ev := inbox(t, handle).GetEvent(); ev.GetSeq() != 8 {
		t.Fatalf("inbox event = %v, want seq 8", ev)
	}
}

func TestANewAgentIOReplacesTheOldOne(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, ctx := expectAttached(t, g, "run-replace")
	first, err := g.openIO(ctx, "run-replace", handle.StreamID())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	second, err := g.openIO(ctx, "run-replace", handle.StreamID())
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if _, err := first.stream.Recv(); err == nil {
		t.Fatal("the replaced AgentIO stream is still open")
	}
	second.state(&agentlinkpb.AgentState{})
	if inbox(t, handle).GetState() == nil {
		t.Fatal("the new stream does not deliver")
	}
	assertInboxEmpty(t, handle)
}

func TestACommandFromTheSidecarIsAProtocolError(t *testing.T) {
	g := newTestLink(t, time.Hour, 3)
	handle, ctx := expectAttached(t, g, "run-proto")
	p, err := g.openIO(ctx, "run-proto", handle.StreamID())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	p.send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Prompt{Prompt: &agentlinkpb.Prompt{TurnId: "t9"}}})
	if _, err := p.stream.Recv(); codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FailedPrecondition", err, codeOf(err))
	}
}
