package harnessapi_test

import (
	"context"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
)

func restart(t *testing.T, h *harness) *harness {
	t.Helper()
	h.svc.Shutdown()
	return reconcileWith(t, h)
}

func reconcileWith(t *testing.T, h *harness) *harness {
	t.Helper()
	h2 := *h
	h2.svc = harnessapi.New(h.store, harnessapi.NewEventLog(h.store), h.launcher, h.tmpl, h.runs,
		harnessapi.WithLogger(testLogger(t)))
	<-h2.svc.ReconcileOnStartup(context.Background())
	return &h2
}

func history(t *testing.T, h *harness, ref *pb.SessionRef) []*pb.Event {
	t.Helper()
	page, err := h.svc.ListEvents(as(stubClient), &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	return page.GetEvents()
}

func waitForEvent(t *testing.T, h *harness, ref *pb.SessionRef, match func(*pb.Event) bool) *pb.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, ev := range history(t, h, ref) {
			if match(ev) {
				return ev
			}
		}
		if time.Now().After(deadline) {
			var kinds []string
			for _, ev := range history(t, h, ref) {
				kinds = append(kinds, api.Kind(ev))
			}
			t.Fatalf("the event never arrived; history %v", kinds)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func kindOf(kind, turnID string) func(*pb.Event) bool {
	return func(ev *pb.Event) bool { return api.Kind(ev) == kind && (turnID == "" || ev.GetTurnId() == turnID) }
}

func countKind(evs []*pb.Event, kind, turnID string) int {
	n := 0
	for _, ev := range evs {
		if kindOf(kind, turnID)(ev) {
			n++
		}
	}
	return n
}

func turnText(evs []*pb.Event, turnID string) string {
	var out string
	for _, ev := range evs {
		if ev.GetTurnId() == turnID && ev.GetAgentMessage() != nil {
			out += ev.GetAgentMessage().GetText()
		}
	}
	return out
}

func TestARestartKeepsARunningTurn(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	started, release := make(chan struct{}), make(chan struct{})
	h.launcher.session.setScript(func(ctx context.Context, sink *fakeAgent, _ string) (acp.StopReason, error) {
		sink.AgentMessage(ctx, "before")
		sink.ToolCall(ctx, toolCallEvent{ID: "tc-1", Title: "build", Kind: "execute", Status: "in_progress"})
		close(started)
		<-release
		sink.AgentMessage(ctx, "after")
		return acp.StopReasonEndTurn, nil
	})
	res, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "build it"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	turn := res.GetTurnId()
	<-started
	waitForEvent(t, h, ref, kindOf("tool_call", turn))

	h2 := restart(t, h)
	close(release)
	waitForEvent(t, h2, ref, kindOf("turn_completed", turn))

	evs := history(t, h2, ref)
	if got := turnText(evs, turn); got != "beforeafter" {
		t.Errorf("the turn's reply = %q, want each part once: beforeafter", got)
	}
	if n := countKind(evs, "tool_call", turn); n != 1 {
		t.Errorf("tool_call recorded %d times, want once", n)
	}
	if n := countKind(evs, "session_ended", ""); n != 0 {
		t.Error("the restart ended the session")
	}
	h2.launcher.mu.Lock()
	adopted := h2.launcher.adopted
	h2.launcher.mu.Unlock()
	if len(adopted) != 1 || adopted[0].RunID == "" || string(adopted[0].StreamID) != "fake-stream" || adopted[0].ResumeAfter < 3 {
		t.Fatalf("adopted = %+v, want the session's run and stream, resuming after the recorded events", adopted)
	}
	waitForStoredState(t, h2, ref, api.StateRunning)
	if r := h2.launcher.session.current(); r.ackedSeq() < uint64(adopted[0].ResumeAfter)+2 {
		t.Errorf("acked %d, want the events after the restart acknowledged", r.ackedSeq())
	}

	h2.launcher.session.setScript(nil)
	next, err := h2.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "again"})
	if err != nil {
		t.Fatalf("Prompt after the restart: %v", err)
	}
	waitForEvent(t, h2, ref, kindOf("turn_completed", next.GetTurnId()))
}

func TestEventsNoHarnessRecordedAreReplayedOnce(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	started, release := make(chan struct{}), make(chan struct{})
	h.launcher.session.setScript(func(ctx context.Context, sink *fakeAgent, _ string) (acp.StopReason, error) {
		sink.AgentMessage(ctx, "one")
		sink.ToolCall(ctx, toolCallEvent{ID: "tc-1", Title: "build", Kind: "execute", Status: "in_progress"})
		close(started)
		<-release
		sink.AgentMessage(ctx, "two")
		return acp.StopReasonEndTurn, nil
	})
	res, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "build it"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	turn := res.GetTurnId()
	<-started
	waitForEvent(t, h, ref, kindOf("tool_call", turn))

	h.svc.Shutdown()
	close(release)
	r := h.launcher.session.current()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.Lock()
		done := len(r.log) > 0 && r.log[len(r.log)-1].GetTurnFinished() != nil
		r.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the agent never finished the turn while the harness was down")
		}
		time.Sleep(5 * time.Millisecond)
	}

	h2 := reconcileWith(t, h)
	waitForEvent(t, h2, ref, kindOf("turn_completed", turn))
	evs := history(t, h2, ref)
	if got := turnText(evs, turn); got != "onetwo" {
		t.Errorf("the turn's reply = %q, want onetwo with no part repeated", got)
	}
	if n := countKind(evs, "turn_completed", turn); n != 1 {
		t.Errorf("turn_completed recorded %d times, want once", n)
	}
}

func TestATurnTheAgentNeverGotIsSentAgainAfterARestart(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	r := h.launcher.session.current()
	r.setDrops(true, false)
	res, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "hello"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	r.setDrops(false, false)

	h2 := restart(t, h)
	waitForEvent(t, h2, ref, kindOf("turn_completed", res.GetTurnId()))
	if got := turnText(history(t, h2, ref), res.GetTurnId()); got != "done: hello" {
		t.Errorf("reply = %q, want the resent turn's reply", got)
	}
}

func TestAPermissionAnswerTheAgentNeverGotIsSentAgainAfterARestart(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.launcher.session.setScript(func(ctx context.Context, sink *fakeAgent, _ string) (acp.StopReason, error) {
		d, err := sink.Permission(ctx, permissionRequest{
			ToolCallID: "tc-1", Title: "Write main.go",
			Options: []permissionOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}, {ID: "deny", Name: "Deny", Kind: "reject_once"}},
		})
		if err != nil {
			return "", err
		}
		sink.AgentMessage(ctx, "decided: "+d.OptionID)
		return acp.StopReasonEndTurn, nil
	})
	res, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "edit"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	waitForEvent(t, h, ref, kindOf("permission_request", res.GetTurnId()))
	r := h.launcher.session.current()
	r.setDrops(false, true)
	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{Ref: ref, RequestId: "tc-1", OptionId: "allow"}); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}
	r.setDrops(false, false)

	h2 := restart(t, h)
	waitForEvent(t, h2, ref, kindOf("turn_completed", res.GetTurnId()))
	evs := history(t, h2, ref)
	if got := turnText(evs, res.GetTurnId()); got != "decided: allow" {
		t.Errorf("reply = %q, want the saved answer to reach the agent", got)
	}
	if n := countKind(evs, "permission_resolved", res.GetTurnId()); n != 1 {
		t.Errorf("permission_resolved recorded %d times, want once", n)
	}
}

func TestAPermissionStillWaitingAtARestartCanBeAnsweredAfterIt(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.launcher.session.setScript(func(ctx context.Context, sink *fakeAgent, _ string) (acp.StopReason, error) {
		d, err := sink.Permission(ctx, permissionRequest{
			ToolCallID: "tc-1", Title: "Write main.go",
			Options: []permissionOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}},
		})
		if err != nil {
			return "", err
		}
		sink.AgentMessage(ctx, "decided: "+d.OptionID)
		return acp.StopReasonEndTurn, nil
	})
	res, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "edit"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	waitForEvent(t, h, ref, kindOf("permission_request", res.GetTurnId()))

	h2 := restart(t, h)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := h2.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{Ref: ref, RequestId: "tc-1", OptionId: "allow"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("RespondPermission after the restart: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	waitForEvent(t, h2, ref, kindOf("turn_completed", res.GetTurnId()))
	evs := history(t, h2, ref)
	if got := turnText(evs, res.GetTurnId()); got != "decided: allow" {
		t.Errorf("reply = %q", got)
	}
	if n := countKind(evs, "permission_request", res.GetTurnId()); n != 1 {
		t.Errorf("permission_request recorded %d times, want once", n)
	}
}

func TestAnAdoptedSessionWhoseSandboxNeverReturnsEnds(t *testing.T) {
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h2 := restart(t, h)
	h2.launcher.down("tunnel_lost")
	waitForStoredState(t, h2, ref, api.StateEnded)
	ended := waitForEvent(t, h2, ref, kindOf("session_ended", ""))
	if ended.GetSessionEnded().GetReason() != api.EndTunnelLost {
		t.Errorf("end reason = %v, want %v", ended.GetSessionEnded().GetReason(), api.EndTunnelLost)
	}
}

func TestAnAgentExitEndsTheSession(t *testing.T) {
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.launcher.session.current().exit(3)
	waitForStoredState(t, h, ref, api.StateEnded)
	ended := waitForEvent(t, h, ref, kindOf("session_ended", ""))
	if ended.GetSessionEnded().GetReason() != api.EndAgentExit {
		t.Errorf("end reason = %v, want %v", ended.GetSessionEnded().GetReason(), api.EndAgentExit)
	}
}

