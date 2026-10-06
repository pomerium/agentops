package harnessapi_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

func bind(h *harness, clientID string, templates []string, q *v1alpha1.ClientQuotas) {
	if h.tmpl.bindings == nil {
		h.tmpl.bindings = map[string]*v1alpha1.ClientBinding{}
	}
	h.tmpl.bindings[clientID] = &v1alpha1.ClientBinding{
		Spec: v1alpha1.ClientBindingSpec{Subject: clientID, Templates: templates, Quotas: q},
	}
}

func TestUnregisteredClientIsRefused(t *testing.T) {
	h := newHarness(t)

	owned := launchRunning(t, h, "stub:conv-1")

	stranger := as("stranger")
	ref := byID(owned.GetId())
	if _, err := h.svc.CreateSession(stranger, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "conv-x",
		ApprovalPrompt: "ship the thing",
	}); !errors.Is(err, api.ErrForbidden) {
		t.Errorf("CreateSession by an unregistered client: got %v, want ErrForbidden", err)
	}
	if _, err := h.svc.GetSession(stranger, &pb.GetSessionRequest{Ref: ref}); !errors.Is(err, api.ErrForbidden) {
		t.Errorf("GetSession by an unregistered client: got %v, want ErrForbidden", err)
	}
	if _, err := h.svc.ListSessions(stranger, &pb.ListSessionsRequest{}); !errors.Is(err, api.ErrForbidden) {
		t.Errorf("ListSessions by an unregistered client: got %v, want ErrForbidden", err)
	}
	if _, err := h.svc.ListTemplates(stranger, &pb.ListTemplatesRequest{}); !errors.Is(err, api.ErrForbidden) {
		t.Errorf("ListTemplates by an unregistered client: got %v, want ErrForbidden", err)
	}
	if _, err := h.svc.EndSession(stranger, &pb.EndSessionRequest{Ref: ref}); !errors.Is(err, api.ErrForbidden) {
		t.Errorf("EndSession by an unregistered client: got %v, want ErrForbidden", err)
	}
}

func TestMaxLiveSessionsQuota(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	bind(h, stubClient, []string{"deploy"}, &v1alpha1.ClientQuotas{MaxLiveSessions: 2})

	first := launchRunning(t, h, "stub:conv-1")
	launchRunning(t, h, "stub:conv-2")

	_, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-3",
		ApprovalPrompt: "ship the thing",
	})
	if !errors.Is(err, api.ErrQuotaExceeded) {
		t.Fatalf("the third session: got %v, want ErrQuotaExceeded", err)
	}

	if _, err := h.svc.EndSession(ctx, &pb.EndSessionRequest{
		Ref: byID(first.GetId()),
	}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	if _, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-3",
		ApprovalPrompt: "ship the thing",
	}); err != nil {
		t.Errorf("a session freed by ending another: %v", err)
	}
}

func TestMaxPendingApprovalsQuota(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	h.launcher.gate = make(chan struct{})
	bind(h, stubClient, []string{"deploy"}, &v1alpha1.ClientQuotas{MaxPendingApprovals: 1})

	if _, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-1",
		ApprovalPrompt: "ship the thing",
	}); err != nil {
		t.Fatalf("the first session: %v", err)
	}
	waitForSessionState(t, h, "stub:conv-1", api.StateAwaitingApproval)

	_, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-2",
		ApprovalPrompt: "ship the thing",
	})
	if !errors.Is(err, api.ErrQuotaExceeded) {
		t.Fatalf("a second outstanding approval: got %v, want ErrQuotaExceeded", err)
	}
}

func TestCreateRateQuota(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	bind(h, stubClient, []string{"deploy"}, &v1alpha1.ClientQuotas{CreateRatePerMinute: 3})

	for i := range 3 {
		if _, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
			Template:        "deploy",
			ConversationRef: fmt.Sprintf("stub:conv-%d", i),
			ApprovalPrompt:  "ship the thing",
		}); err != nil {
			t.Fatalf("session %d within the rate: %v", i, err)
		}
	}
	_, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-over",
		ApprovalPrompt: "ship the thing",
	})
	if !errors.Is(err, api.ErrQuotaExceeded) {
		t.Fatalf("past the rate: got %v, want ErrQuotaExceeded", err)
	}
}

func waitForSessionState(t *testing.T, h *harness, conversationRef string, want api.SessionState) {
	t.Helper()
	ctx := as(stubClient)
	get := &pb.GetSessionRequest{Ref: &pb.SessionRef{ConversationRef: conversationRef}}
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := h.svc.GetSession(ctx, get)
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		got := res.GetSession()
		if got.GetState() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session never reached %q; stuck at %q", want, got.GetState())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRespondPermissionIsIdempotentPastTheSession(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t, harnessapi.WithPermissionTimeout(5*time.Second))

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	h.launcher.session.setScript(func(ctx context.Context, sink sandbox.EventSink, _ string) (acp.StopReason, error) {
		decision, err := sink.Permission(ctx, sandbox.PermissionRequest{
			ToolCallID: "tc-1",
			Title:      "Write to main.go",
			Options:    []sandbox.PermissionOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}},
		})
		if err != nil {
			return "", err
		}
		sink.AgentMessage(ctx, "decided: "+decision.OptionID)
		return acp.StopReasonEndTurn, nil
	})
	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "do the thing"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	permEv, _ := rec.waitFor("permission_request", 0)
	perm := permEv.GetPermissionRequest()
	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: perm.RequestId, OptionId: "allow",
	}); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}

	if _, err := h.svc.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: perm.RequestId, OptionId: "allow",
	}); err != nil {
		t.Errorf("retrying a decision after the session ended: got %v, want it accepted", err)
	}
	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: "never-asked", OptionId: "allow",
	}); !errors.Is(err, api.ErrUnknownRequest) {
		t.Errorf("an unknown request after the session ended: got %v, want ErrUnknownRequest", err)
	}
}

func TestCreateSessionRequiresAnApprovalPrompt(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	for _, prompt := range []string{"", "   ", "\n\t"} {
		_, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
			Template:        "deploy",
			ConversationRef: "stub:conv-" + prompt, ApprovalPrompt: prompt,
		})
		if !errors.Is(err, api.ErrInvalidArgument) {
			t.Errorf("CreateSession with approval_prompt %q: got %v, want ErrInvalidArgument", prompt, err)
		}
	}
}

func TestReviveIsChargedLikeALaunch(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	bind(h, stubClient, []string{"deploy"}, &v1alpha1.ClientQuotas{CreateRatePerMinute: 1})

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForSessionState(t, h, "stub:conv-1", api.StateSuspended)

	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"}); !errors.Is(err, api.ErrQuotaExceeded) {
		t.Fatalf("a revive past the rate cap: got %v, want ErrQuotaExceeded", err)
	}
}

func TestReviveIsNotChargedAgainstLiveSessions(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	bind(h, stubClient, []string{"deploy"}, &v1alpha1.ClientQuotas{MaxLiveSessions: 1})

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForSessionState(t, h, "stub:conv-1", api.StateSuspended)

	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"}); err != nil {
		t.Errorf("reviving the client's own only session: got %v, want it accepted", err)
	}
}

func TestUnofferedPermissionChoiceIsRefused(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t, harnessapi.WithPermissionTimeout(5*time.Second))

	ref := byID(launchRunning(t, h, "stub:conv-1").GetId())
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	h.launcher.session.setScript(func(ctx context.Context, sink sandbox.EventSink, _ string) (acp.StopReason, error) {
		decision, err := sink.Permission(ctx, sandbox.PermissionRequest{
			ToolCallID: "tc-1",
			Options:    []sandbox.PermissionOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}},
		})
		if err != nil {
			return "", err
		}
		sink.AgentMessage(ctx, "decided: "+decision.OptionID)
		return acp.StopReasonEndTurn, nil
	})
	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "do the thing"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	permEv, _ := rec.waitFor("permission_request", 0)
	requestID := permEv.GetPermissionRequest().GetRequestId()

	for _, option := range []string{"", "not-offered"} {
		if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
			Ref: ref, RequestId: requestID, OptionId: option,
		}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Errorf("answering with option %q: got %v, want ErrInvalidArgument", option, err)
		}
	}
	if _, err := h.svc.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: requestID, OptionId: "allow",
	}); err != nil {
		t.Fatalf("the offered option after a refused one: %v", err)
	}
	ev, _ := rec.waitFor("agent_message", 0)
	if got := ev.GetAgentMessage().GetText(); got != "decided: allow" {
		t.Errorf("the agent got %q, want the offered option", got)
	}
}

type listBarrier struct {
	sessionstore.Sessions
	mu     sync.Mutex
	calls  int
	second chan struct{}
}

func (b *listBarrier) ListSessionsByClient(ctx context.Context, clientID string, liveOnly bool, since time.Time) ([]sessionstore.Session, error) {
	rows, err := b.Sessions.ListSessionsByClient(ctx, clientID, liveOnly, since)
	b.mu.Lock()
	b.calls++
	call := b.calls
	b.mu.Unlock()
	switch call {
	case 1:
		select {
		case <-b.second:
		case <-time.After(200 * time.Millisecond):
		}
	case 2:
		close(b.second)
	}
	return rows, err
}

func TestConcurrentCreatesRespectTheLiveCap(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	h.launcher.gate = make(chan struct{})
	defer h.launcher.openGate()
	bind(h, stubClient, []string{"deploy"}, &v1alpha1.ClientQuotas{MaxLiveSessions: 1})
	barrier := &listBarrier{Sessions: h.store, second: make(chan struct{})}
	svc := harnessapi.New(barrier, harnessapi.NewEventLog(h.store), h.launcher, h.tmpl, h.runs,
		harnessapi.WithLogger(testLogger(t)))

	results := make(chan error, 2)
	for _, conv := range []string{"stub:conv-1", "stub:conv-2"} {
		go func() {
			_, err := svc.CreateSession(ctx, &pb.CreateSessionRequest{
				Template: "deploy", ConversationRef: conv, ApprovalPrompt: "ship the thing",
			})
			results <- err
		}()
	}
	accepted := 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			accepted++
		case !errors.Is(err, api.ErrQuotaExceeded):
			t.Errorf("the refused create: got %v, want ErrQuotaExceeded", err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d sessions with a cap of 1", accepted)
	}
}

func TestLaunchesBeforeApprovalCountAsPending(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	h.launcher.prepareGate = make(chan struct{})
	defer close(h.launcher.prepareGate)
	bind(h, stubClient, []string{"deploy"}, &v1alpha1.ClientQuotas{MaxPendingApprovals: 1})

	if _, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-1", ApprovalPrompt: "ship the thing",
	}); err != nil {
		t.Fatalf("the first session: %v", err)
	}
	_, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:conv-2", ApprovalPrompt: "ship the thing",
	})
	if !errors.Is(err, api.ErrQuotaExceeded) {
		t.Fatalf("a second launch while the first awaits its approval page: got %v, want ErrQuotaExceeded", err)
	}
}
