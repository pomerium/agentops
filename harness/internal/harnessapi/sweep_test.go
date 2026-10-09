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

func TestIdleSweepWarnsThenSuspends(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t,
		harnessapi.WithSessionIdleTTL(40*time.Millisecond),
		harnessapi.WithIdleWarnLead(30*time.Millisecond),
		harnessapi.WithSessionTTL(time.Hour),
	)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())

	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref, AfterSeq: view.GetLastSeq()})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	time.Sleep(15 * time.Millisecond)
	h.svc.SweepExpired(ctx)
	ev, at := rec.waitFor("idle_warning", 0)
	warn := ev.GetIdleWarning()
	if warn.GetLead().AsDuration() != 30*time.Millisecond {
		t.Errorf("idle_warning lead = %v, want the configured lead", warn.GetLead().AsDuration())
	}
	if got, _ := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref}); got.GetSession().GetState() != api.StateRunning {
		t.Fatalf("a warned session was suspended too early: %q", got.GetSession().GetState())
	}

	time.Sleep(45 * time.Millisecond)
	h.svc.SweepExpired(ctx)
	_, _ = rec.waitFor("suspended", at)
	waitForStoredState(t, h, ref, api.StateSuspended)

	_, suspends, teardowns, _, _ := h.launcher.snapshot()
	if len(suspends) != 1 {
		t.Errorf("suspends = %v, want one", suspends)
	}
	if len(teardowns) != 0 {
		t.Errorf("the idle sweep must keep the workspace; teardowns = %v", teardowns)
	}
}

func TestIdleSweepNeverReapsMidTurn(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t, harnessapi.WithSessionIdleTTL(20*time.Millisecond), harnessapi.WithSessionTTL(time.Hour))
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())

	release := make(chan struct{})
	started := make(chan struct{})
	h.launcher.session.setScript(func(ctx context.Context, sink *fakeAgent, text string) (acp.StopReason, error) {
		close(started)
		<-release
		sink.AgentMessage(ctx, "eventually: "+text)
		return acp.StopReasonEndTurn, nil
	})
	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "think hard"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	<-started

	time.Sleep(30 * time.Millisecond)
	h.svc.SweepExpired(ctx)
	if got, _ := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref}); got.GetSession().GetState() != api.StateRunning {
		t.Errorf("a session mid-turn was reaped: %q", got.GetSession().GetState())
	}
	close(release)
}

func TestSuspendedSweepReleasesTheWorkspace(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t,
		harnessapi.WithSessionTTL(time.Hour),
		harnessapi.WithSuspendedTTL(10*time.Millisecond),
	)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())

	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref, AfterSeq: view.GetLastSeq()})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	time.Sleep(1100 * time.Millisecond)
	h.svc.SweepExpired(ctx)

	_, at := rec.waitFor("released", 0)
	ev, _ := rec.waitFor("session_ended", at)
	ended := ev.GetSessionEnded()
	if ended.Reason != api.EndExpired {
		t.Errorf("released session ended with reason %q, want %q", ended.Reason, api.EndExpired)
	}
	if _, _, teardowns, _, _ := h.launcher.snapshot(); len(teardowns) != 1 {
		t.Errorf("teardowns = %v, want the released claim", teardowns)
	}
}

func TestAbsoluteLifetimeRestartsWithEachLaunch(t *testing.T) {
	ctx := as(stubClient)
	const ttl = 1500 * time.Millisecond
	h := newHarness(t, harnessapi.WithSessionTTL(ttl))
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())
	h.svc.SuspendForTest(ctx, ref.GetSessionId())
	waitForStoredState(t, h, ref, api.StateSuspended)

	time.Sleep(ttl + 100*time.Millisecond)
	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "carry on"}); err != nil {
		t.Fatalf("Prompt (revive): %v", err)
	}
	waitForStoredState(t, h, ref, api.StateRunning)

	h.svc.SweepExpired(ctx)
	if got, _ := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref}); got.GetSession().GetState() != api.StateRunning {
		t.Fatalf("a continuation was swept by the first launch's lifetime: %q", got.GetSession().GetState())
	}
	if _, _, teardowns, _, _ := h.launcher.snapshot(); len(teardowns) != 0 {
		t.Fatalf("the sweep tore down a continued workspace: %v", teardowns)
	}

	time.Sleep(ttl + 100*time.Millisecond)
	h.svc.SweepExpired(ctx)
	waitForStoredState(t, h, ref, api.StateEnded)
	if _, _, teardowns, _, _ := h.launcher.snapshot(); len(teardowns) != 1 {
		t.Errorf("teardowns = %v, want the expired continuation's claim", teardowns)
	}
}
