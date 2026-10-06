package harnessapi_test

import (
	"context"
	"errors"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func TestStubClientDrivesFullLifecycle(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t, harnessapi.WithPermissionTimeout(5*time.Second))
	h.launcher.gate = make(chan struct{})

	created, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template:        "deploy",
		ConversationRef: "stub:conv-1",
		ApprovalPrompt:  "ship the thing",
		InitialPrompt:   "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	view := created.GetSession()
	if view.GetState() != api.StatePending {
		t.Errorf("new session state = %q, want pending", view.GetState())
	}

	ref := byID(view.GetId())
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	ev, at := rec.waitFor("approval_required", 0)
	approval := ev.GetApprovalRequired()
	if approval.ApprovalUrl == "" {
		t.Error("approval_required carried no URL")
	}
	h.launcher.mu.Lock()
	expected := len(h.launcher.expectedRuns)
	h.launcher.mu.Unlock()
	if expected == 0 {
		t.Error("the Agent Link expectation must be registered before the approval link is published")
	}

	h.launcher.openGate()

	approvedEv, at := rec.waitFor("approved", at)
	approved := approvedEv.GetApproved()
	if approved.ApproverSubject != testApproverSubject {
		t.Errorf("approver_subject = %q, want the raw IdP subject %q",
			approved.ApproverSubject, testApproverSubject)
	}
	at = waitForState(t, rec, at, api.StateRunning)

	_, at = rec.waitFor("turn_completed", at)

	agent := h.launcher.session
	agent.setScript(func(ctx context.Context, sink sandbox.EventSink, text string) (acp.StopReason, error) {
		sink.AgentMessage(ctx, "looking at it")
		sink.ToolCall(ctx, sandbox.ToolCallEvent{ID: "tc-1", Title: "Read file", Kind: "read", Status: "in_progress"})
		decision, err := sink.Permission(ctx, sandbox.PermissionRequest{
			ToolCallID: "tc-1",
			Title:      "Write to main.go",
			Options: []sandbox.PermissionOption{
				{ID: "allow", Name: "Allow", Kind: "allow_once"},
				{ID: "deny", Name: "Deny", Kind: "reject_once"},
			},
		})
		if err != nil {
			return "", err
		}
		if decision.Cancelled {
			sink.AgentMessage(ctx, "stopped")
			return acp.StopReasonCancelled, nil
		}
		sink.Usage(ctx, sandbox.UsageEvent{InputTokens: 100, OutputTokens: 20, TotalTokens: 120})
		sink.AgentMessage(ctx, "decided: "+decision.OptionID)
		return acp.StopReasonEndTurn, nil
	})

	res, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "edit main.go"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if res.GetTurnId() == "" {
		t.Error("Prompt returned no turn id")
	}

	msgEv, at := rec.waitFor("agent_message", at)
	msg := msgEv.GetAgentMessage()
	if msg.Text != "looking at it" || msg.Final || msg.PartId == "" {
		t.Errorf("first segment = %+v, want the non-final narration with a part id", msg)
	}
	if msgEv.TurnId != res.GetTurnId() {
		t.Errorf("agent_message turn_id = %q, want %q", msgEv.TurnId, res.GetTurnId())
	}

	toolEv, at := rec.waitFor("tool_call", at)
	tool := toolEv.GetToolCall()
	if tool.Id != "tc-1" || tool.Title != "Read file" || tool.Status != api.ToolCallInProgress {
		t.Errorf("tool_call = %+v, want the published closed-enum status", tool)
	}

	permEv, at := rec.waitFor("permission_request", at)
	perm := permEv.GetPermissionRequest()
	if len(perm.Options) != 2 || perm.GetDeadline() == nil {
		t.Errorf("permission_request = %+v, want two options and a deadline", perm)
	}

	_, err = h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: "nope", OptionId: "allow",
	})
	if !errors.Is(err, api.ErrUnknownRequest) {
		t.Errorf("responding to an unknown request: got %v, want ErrUnknownRequest", err)
	}

	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: perm.RequestId, OptionId: "allow",
	}); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}

	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: perm.RequestId, OptionId: "allow",
	}); err != nil {
		t.Errorf("retrying a decision that already landed: got %v, want it accepted", err)
	}

	resolvedEv, at := rec.waitFor("permission_resolved", at)
	resolved := resolvedEv.GetPermissionResolved()
	if resolved.GetOptionId() != "allow" || resolved.RequestId != perm.RequestId {
		t.Errorf("permission_resolved = %+v, want the chosen option", resolved)
	}

	usageEv, at := rec.waitFor("usage", at)
	usage := usageEv.GetUsage()
	if usage.TotalTokens != 120 || usageEv.TurnId != res.GetTurnId() {
		t.Errorf("usage = %+v on turn %q, want 120 tokens on %q", usage, usageEv.TurnId, res.GetTurnId())
	}

	completedEv, at := rec.waitFor("turn_completed", at)
	completed := completedEv.GetTurnCompleted()
	if completed.StopReason != string(acp.StopReasonEndTurn) {
		t.Errorf("stop_reason = %q", completed.StopReason)
	}

	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	at = waitForState(t, rec, at, api.StateSuspended)
	_, at = rec.waitFor("suspended", at)

	_, suspends, teardowns, _, _ := h.launcher.snapshot()
	if len(suspends) != 1 {
		t.Errorf("suspends = %v, want exactly one", suspends)
	}
	if len(teardowns) != 0 {
		t.Errorf("a suspend must not tear the workspace down; teardowns = %v", teardowns)
	}

	agent.setScript(nil)
	res2, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"})
	if err != nil {
		t.Fatalf("Prompt on a suspended session: %v", err)
	}
	if res2.GetTurnId() == res.GetTurnId() {
		t.Errorf("turn ids restarted across the revive: %q then %q", res.GetTurnId(), res2.GetTurnId())
	}

	reapproval, at := rec.waitFor("approval_required", at)
	approval = reapproval.GetApprovalRequired()
	if approval.ApprovalUrl == "" {
		t.Error("a revive must solicit its own approval")
	}
	at = waitForState(t, rec, at, api.StateRunning)
	_, at = rec.waitFor("revived", at)

	revives, _, _, resumeID, _ := h.launcher.snapshot()
	if len(revives) != 1 || revives[0] != "claim-1" {
		t.Errorf("revives = %v, want the original claim", revives)
	}
	if resumeID != "acp-sess" {
		t.Errorf("resumed ACP session = %q, want the persisted one", resumeID)
	}
	reqs := h.runs.requests()
	if len(reqs) != 2 {
		t.Fatalf("CreateRun calls = %d, want 2 (a revive is a new run)", len(reqs))
	}
	if reqs[1].ExpectedSubject != testApproverSubject {
		t.Errorf("the revive's run is pinned to %q, want %q", reqs[1].ExpectedSubject, testApproverSubject)
	}
	if reqs[0].ExpectedSubject != "" {
		t.Errorf("a first launch with no nominated principal must not be pinned, got %q", reqs[0].ExpectedSubject)
	}

	if _, err := h.svc.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	endedEv, _ := rec.waitFor("session_ended", at)
	ended := endedEv.GetSessionEnded()
	if ended.Reason != api.EndEnded {
		t.Errorf("session_ended reason = %q, want %q", ended.Reason, api.EndEnded)
	}

	final, err := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref})
	if err != nil {
		t.Fatalf("GetSession after end: %v", err)
	}
	if got := final.GetSession().GetState(); got != api.StateEnded {
		t.Errorf("final state = %q", got)
	}
	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "hello?"}); !errors.Is(err, api.ErrInvalidState) {
		t.Errorf("prompt to an ended session: got %v, want ErrInvalidState", err)
	}

	if got := agent.promptList(); len(got) != 3 {
		t.Errorf("agent saw %v, want the opening prompt, the tool turn and the continuation", got)
	}
}

func waitForState(t *testing.T, rec *recorder, from int, want api.SessionState) int {
	t.Helper()
	at := from
	for {
		ev, i := rec.waitFor("state_changed", at)
		p := ev.GetStateChanged()
		if p.New == want {
			return i
		}
		at = i + 1
	}
}

func TestCreateSessionConversationConflict(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)

	if _, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-1",
		ApprovalPrompt: "ship the thing",
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	_, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-1",
		ApprovalPrompt: "ship the thing",
	})
	if !errors.Is(err, api.ErrConflict) {
		t.Errorf("a second live session for one conversation: got %v, want ErrConflict", err)
	}
}

func TestSupervisionEndsTheSession(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	h.launcher.down("tunnel_lost")

	ev, _ := rec.waitFor("session_ended", 0)
	ended := ev.GetSessionEnded()
	if ended.Reason != api.EndTunnelLost {
		t.Errorf("session_ended reason = %q, want %q", ended.Reason, api.EndTunnelLost)
	}
}

func launchRunning(t *testing.T, h *harness, conversationRef string) *pb.SessionView {
	t.Helper()
	ctx := as(stubClient)
	created, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: conversationRef,
		ApprovalPrompt: "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	get := &pb.GetSessionRequest{Ref: byID(created.GetSession().GetId())}
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := h.svc.GetSession(ctx, get)
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		got := res.GetSession()
		if got.GetState() == api.StateRunning {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("session never reached running; stuck at %q", got.GetState())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestActingVerbsIgnoreIncludeTerminal(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	if _, err := h.svc.EndSession(ctx, &pb.EndSessionRequest{
		Ref: byID(view.GetId()),
	}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	waitForStoredState(t, h, byID(view.GetId()), api.StateEnded)

	ref := &pb.SessionRef{ConversationRef: "stub:conv-1", IncludeTerminal: true}
	if _, err := h.svc.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("EndSession on an ended conversation: %v, want ErrNotFound", err)
	}
	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "hi"}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Prompt on an ended conversation: %v, want ErrNotFound", err)
	}
	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: "tc-1", OptionId: "allow",
	}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("RespondPermission on an ended conversation: %v, want ErrNotFound", err)
	}
	if got, err := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref}); err != nil || got.GetSession().GetId() != view.GetId() {
		t.Errorf("GetSession with include_terminal: %q, %v", got.GetSession().GetId(), err)
	}
}

func TestOverlappingPromptsKeepTheirReplies(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	ref := byID(launchRunning(t, h, "stub:conv-1").GetId())

	firstEntered, secondEntered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h.launcher.session.setScript(func(ctx context.Context, sink sandbox.EventSink, text string) (acp.StopReason, error) {
		sink.AgentMessage(ctx, text+" reply")
		if text == "first" {
			close(firstEntered)
			<-release
		} else {
			close(secondEntered)
		}
		return acp.StopReasonEndTurn, nil
	})
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	first, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "first"})
	if err != nil {
		t.Fatalf("Prompt (first): %v", err)
	}
	<-firstEntered
	second, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "second"})
	if err != nil {
		t.Fatalf("Prompt (second): %v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	_, at := rec.waitFor("turn_completed", 0)
	rec.waitFor("turn_completed", at+1)

	page, err := h.svc.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	replies := map[string]string{}
	for _, ev := range page.GetEvents() {
		if msg := ev.GetAgentMessage(); msg != nil {
			replies[ev.GetTurnId()] += msg.GetText()
		}
	}
	if got := replies[first.GetTurnId()]; got != "first reply" {
		t.Errorf("the first turn's reply is %q, want %q", got, "first reply")
	}
	if got := replies[second.GetTurnId()]; got != "second reply" {
		t.Errorf("the second turn's reply is %q, want %q", got, "second reply")
	}
}
