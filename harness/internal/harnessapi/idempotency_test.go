package harnessapi_test

import (
	"errors"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

func TestPromptKeyStartsOneTurn(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())

	first, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "deploy", IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	again, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "deploy, caught up", IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("Prompt (repeat): %v", err)
	}
	if again.GetTurnId() != first.GetTurnId() {
		t.Errorf("the repeat got turn %q, want the first prompt's %q", again.GetTurnId(), first.GetTurnId())
	}
	other, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "and test", IdempotencyKey: "k2"})
	if err != nil {
		t.Fatalf("Prompt (new key): %v", err)
	}
	if other.GetTurnId() == first.GetTurnId() {
		t.Errorf("a new key reused turn %q", first.GetTurnId())
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(h.launcher.session.promptList()) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the agent saw %v, want two prompts", h.launcher.session.promptList())
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, p := range h.launcher.session.promptList() {
		if p == "deploy, caught up" {
			t.Errorf("the repeat reached the agent: %v", h.launcher.session.promptList())
		}
	}
}

func TestPromptKeyCoversARevive(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	h.launcher.mu.Lock()
	h.launcher.gate = make(chan struct{})
	h.launcher.mu.Unlock()

	first, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on", IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("Prompt (revive): %v", err)
	}
	waitForStoredState(t, h, ref, api.StateAwaitingApproval)

	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"}); !errors.Is(err, api.ErrInvalidState) {
		t.Fatalf("an unkeyed repeat = %v, want ErrInvalidState", err)
	}
	again, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on", IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("a keyed repeat during the revive = %v, want the first turn back", err)
	}
	if again.GetTurnId() != first.GetTurnId() {
		t.Errorf("the repeat got turn %q, want the revive's %q", again.GetTurnId(), first.GetTurnId())
	}

	h.launcher.openGate()
	waitForStoredState(t, h, ref, api.StateRunning)
	if revives, _, _, _, _ := h.launcher.snapshot(); len(revives) != 1 {
		t.Errorf("one keyed prompt revived the session %d times", len(revives))
	}
}
