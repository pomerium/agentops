package harnessapi_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

func TestReviveRunsTheStoredTemplate(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	h.tmpl.tmpl = &v1alpha1.AgentTemplate{
		ObjectMeta: h.tmpl.tmpl.ObjectMeta,
		Spec: v1alpha1.AgentTemplateSpec{
			SystemPrompt: "do something else",
			RequiredMCPServers: []v1alpha1.MCPServerRef{
				{Name: "attacker", URL: "https://mcp.attacker.example/"},
			},
		},
	}

	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"}); err != nil {
		t.Fatalf("Prompt (revive): %v", err)
	}
	waitForStoredState(t, h, ref, api.StateRunning)

	_, _, _, _, revived := h.launcher.snapshot()
	if revived == nil {
		t.Fatal("the revive ran no template at all")
	}
	if len(revived.Spec.RequiredMCPServers) != 1 || revived.Spec.RequiredMCPServers[0].URL != "https://mcp.example/github" {
		t.Errorf("the revive ran %v; it must run the snapshot the approver consented to",
			revived.Spec.RequiredMCPServers)
	}
	if revived.Spec.SystemPrompt != "be helpful" {
		t.Errorf("the revive ran system prompt %q, want the stored one", revived.Spec.SystemPrompt)
	}
	reqs := h.runs.requests()
	last := reqs[len(reqs)-1]
	if len(last.MCPServers) != 1 || last.MCPServers[0] != "https://mcp.example/github" {
		t.Errorf("the revive's consent page discloses %v, want the stored upstreams", last.MCPServers)
	}
}

func TestReviveRefusedWithoutAnApprover(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	h.runs.status = &agenticrun.RunStatus{State: "approved"}

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	_, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"})
	if !errors.Is(err, api.ErrNotRevivable) {
		t.Errorf("reviving a session with no recorded approver: got %v, want ErrNotRevivable", err)
	}
	if revives, _, _, _, _ := h.launcher.snapshot(); len(revives) != 0 {
		t.Errorf("nothing should have been revived, got %v", revives)
	}
}

func TestFailedReviveKeepsTheWorkspace(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	res, err := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	before := res.GetSession()
	stored, err := h.store.GetSession(ctx, view.GetId())
	if err != nil {
		t.Fatalf("store.GetSession: %v", err)
	}

	h.launcher.mu.Lock()
	h.launcher.resumeErr = errors.New("the pod would not come back")
	h.launcher.mu.Unlock()

	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref, AfterSeq: before.GetLastSeq()})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	turn, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"})
	if err != nil {
		t.Fatalf("Prompt (revive): %v", err)
	}
	ev, _ := rec.waitFor("turn_failed", 0)
	if ev.TurnId != turn.GetTurnId() {
		t.Errorf("turn_failed is for %q, want the turn the prompt opened (%q)", ev.TurnId, turn.GetTurnId())
	}
	if got := suspendReason(rec); got != api.ReasonReviveFailed {
		t.Errorf("a revive that may succeed later reported %v, want %v", got, api.ReasonReviveFailed)
	}

	waitForStoredState(t, h, ref, api.StateSuspended)
	after, err := h.store.GetSession(ctx, view.GetId())
	if err != nil {
		t.Fatalf("store.GetSession: %v", err)
	}
	if !after.SuspendedAt.Equal(stored.SuspendedAt) {
		t.Errorf("the retention clock was restamped by a failed revive: %v then %v",
			stored.SuspendedAt, after.SuspendedAt)
	}
	if _, _, teardowns, _, _ := h.launcher.snapshot(); len(teardowns) != 0 {
		t.Errorf("a failed revive tore the workspace down: %v", teardowns)
	}
}

func waitForStoredState(t *testing.T, h *harness, ref *pb.SessionRef, want api.SessionState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := h.svc.GetSession(as(stubClient), &pb.GetSessionRequest{Ref: ref})
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

func TestUnresumableReviveSaysSo(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)

	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	h.launcher.mu.Lock()
	h.launcher.activateErr = sandbox.ErrResumeUnavailable
	h.launcher.mu.Unlock()

	res, err := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref, AfterSeq: res.GetSession().GetLastSeq()})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"}); err != nil {
		t.Fatalf("Prompt (revive): %v", err)
	}
	if got := suspendReason(rec); got != api.ReasonResumeUnavailable {
		t.Errorf("an unresumable revive reported %v, want %v", got, api.ReasonResumeUnavailable)
	}
	waitForStoredState(t, h, ref, api.StateSuspended)
	if _, _, teardowns, _, _ := h.launcher.snapshot(); len(teardowns) != 0 {
		t.Errorf("an unresumable revive tore the workspace down: %v", teardowns)
	}
}

func suspendReason(rec *recorder) api.Reason {
	for from := 0; ; {
		ev, i := rec.waitFor("state_changed", from)
		if sc := ev.GetStateChanged(); sc.GetNew() == api.StateSuspended {
			return sc.GetReason()
		}
		from = i + 1
	}
}

type readBarrier struct {
	sessionstore.Sessions
	mu      sync.Mutex
	waiting int
	arrived chan struct{}
	release chan struct{}
}

func (b *readBarrier) GetSession(ctx context.Context, id string) (sessionstore.Session, error) {
	sess, err := b.Sessions.GetSession(ctx, id)
	b.mu.Lock()
	hold := b.waiting > 0
	if hold {
		b.waiting--
	}
	b.mu.Unlock()
	if hold {
		b.arrived <- struct{}{}
		<-b.release
	}
	return sess, err
}

func TestConcurrentRevivesAcceptOneTurn(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	ref := byID(launchRunning(t, h, "stub:conv-1").GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	h.launcher.gate = make(chan struct{})
	defer h.launcher.openGate()
	barrier := &readBarrier{
		Sessions: h.store, waiting: 2,
		arrived: make(chan struct{}, 2), release: make(chan struct{}),
	}
	svc := harnessapi.New(barrier, harnessapi.NewEventLog(h.store), h.launcher, h.tmpl, h.runs,
		harnessapi.WithLogger(testLogger(t)))

	results := make(chan error, 2)
	for _, content := range []string{"one", "two"} {
		go func() {
			_, err := svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: content})
			results <- err
		}()
	}
	<-barrier.arrived
	<-barrier.arrived
	close(barrier.release)

	accepted := 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			accepted++
		case !errors.Is(err, api.ErrInvalidState):
			t.Errorf("the losing revive: got %v, want ErrInvalidState", err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d turns, but only one revive can run", accepted)
	}
}
