package harnessapi

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

type stagedRuns struct {
	agenticrun.RunClient
	mu       sync.Mutex
	approved bool
	polls    int
	seen     int
}

func (r *stagedRuns) GetRun(context.Context, string) (*agenticrun.RunStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.polls++
	if !r.approved {
		return &agenticrun.RunStatus{State: "pending"}, nil
	}
	r.seen++
	return &agenticrun.RunStatus{State: "approved"}, nil
}

func (r *stagedRuns) approve() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.approved = true
}

func (r *stagedRuns) approvedPolls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen
}

func awaitingService(t *testing.T, runs agenticrun.RunClient) *Service {
	t.Helper()
	st := openStore(t)
	if err := st.CreateSession(context.Background(), sessionstore.Session{
		ID: "s1", ClientID: "client", ConversationRef: "conversation",
		TemplateName: "deploy", Status: api.StateAwaitingApproval,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	svc := New(st, NewEventLog(st), quietLauncher{}, nil, runs, WithLogger(slog.New(slog.DiscardHandler)))
	svc.cfg.runPollInterval = time.Millisecond
	return svc
}

func stallReports(t *testing.T, svc *Service) int {
	t.Helper()
	events, err := svc.events.History(context.Background(), "s1", 0, 100)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	n := 0
	for _, ev := range events {
		if ev.GetLaunchStalled() != nil {
			n++
		}
	}
	return n
}

func awaitApprovedPolls(t *testing.T, runs *stagedRuns, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runs.approvedPolls() < n {
		if time.Now().After(deadline) {
			t.Fatalf("the run was polled %d times after approval, want %d", runs.approvedPolls(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAnApprovalAfterTheAttachWarningReportsTheStall(t *testing.T) {
	ctx := context.Background()
	runs := &stagedRuns{}
	svc := awaitingService(t, runs)
	outcome := &runOutcome{}
	polling, cancel := context.WithCancel(ctx)
	defer cancel()
	go svc.pollRun(polling, cancel, "s1", "run-1", outcome, func() bool { return false })

	svc.reportAttachDelayed(ctx, "s1", "run-1", outcome, 90*time.Second)
	if n := stallReports(t, svc); n != 0 {
		t.Fatalf("reported %d stalls before anyone approved", n)
	}

	runs.approve()
	awaitApprovedPolls(t, runs, 5)
	if n := stallReports(t, svc); n != 1 {
		t.Errorf("an approved sandbox that never attached was reported stalled %d times, want once", n)
	}
}

func TestASandboxThatAttachesJustAfterALateApprovalIsNotReportedStalled(t *testing.T) {
	ctx := context.Background()
	runs := &stagedRuns{}
	svc := awaitingService(t, runs)
	outcome := &runOutcome{}
	polling, cancel := context.WithCancel(ctx)
	defer cancel()
	attached := func() bool { return runs.approvedPolls() >= 2 }
	go svc.pollRun(polling, cancel, "s1", "run-1", outcome, attached)

	svc.reportAttachDelayed(ctx, "s1", "run-1", outcome, 90*time.Second)
	runs.approve()
	awaitApprovedPolls(t, runs, 5)
	if n := stallReports(t, svc); n != 0 {
		t.Errorf("a sandbox that attached a poll after the approval was reported stalled %d times", n)
	}
}

func TestAStallBeforeTheAttachWarningIsNotReported(t *testing.T) {
	ctx := context.Background()
	runs := &stagedRuns{}
	svc := awaitingService(t, runs)
	outcome := &runOutcome{}
	polling, cancel := context.WithCancel(ctx)
	defer cancel()
	go svc.pollRun(polling, cancel, "s1", "run-1", outcome, func() bool { return false })

	runs.approve()
	awaitApprovedPolls(t, runs, 5)
	if n := stallReports(t, svc); n != 0 {
		t.Errorf("a sandbox still inside its attach window was reported stalled %d times", n)
	}
	svc.reportAttachDelayed(ctx, "s1", "run-1", outcome, 90*time.Second)
	awaitApprovedPolls(t, runs, 10)
	if n := stallReports(t, svc); n != 1 {
		t.Errorf("an approved sandbox past its attach window was reported stalled %d times, want once", n)
	}
}
