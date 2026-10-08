package harnessapi

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/pomerium/agentops/harness/api/pb"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

type flakyPodLog struct {
	EventLog
	failures atomic.Int32
}

func (l *flakyPodLog) AppendFromPod(ctx context.Context, ev *pb.Event, podSeq uint64) error {
	if l.failures.Load() != 0 {
		l.failures.Add(-1)
		return errors.New("database unavailable")
	}
	return l.EventLog.AppendFromPod(ctx, ev, podSeq)
}

type ackProbe struct {
	idleSession
	acked atomic.Uint64
}

func (p *ackProbe) Ack(seq uint64) { p.acked.Store(seq) }

func turnFinished(seq uint64, turnID string) *agentlinkpb.AgentEvent {
	return &agentlinkpb.AgentEvent{
		Seq: seq, TurnId: turnID,
		Payload: &agentlinkpb.AgentEvent_TurnFinished{TurnFinished: &agentlinkpb.TurnFinished{StopReason: "end_turn"}},
	}
}

func TestAnUnrecordedPodEventIsNotAcked(t *testing.T) {
	svc, _ := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	live := &ackProbe{}
	b.session = live
	podLog := &flakyPodLog{EventLog: svc.events}
	podLog.failures.Store(-1)
	svc.events = podLog
	b.enter("t1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.handleEvent(context.Background(), b, turnFinished(2, "t1"))
	}()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
	}
	if got := live.acked.Load(); got != 0 {
		t.Fatalf("acked event %d that the log did not record", got)
	}
	if !slices.Contains(b.outstanding(), "t1") {
		t.Fatal("forgot a turn whose end was not recorded")
	}

	b.close(0)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer kept retrying after the binding closed")
	}
	if got := live.acked.Load(); got != 0 {
		t.Fatalf("acked event %d after giving up on it", got)
	}
}

func TestAPodEventIsAckedOnceTheLogRecordsIt(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	live := &ackProbe{}
	b.session = live
	podLog := &flakyPodLog{EventLog: svc.events}
	podLog.failures.Store(2)
	svc.events = podLog
	b.enter("t1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.handleEvent(ctx, b, turnFinished(2, "t1"))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the event was never recorded")
	}
	if got := live.acked.Load(); got != 2 {
		t.Errorf("acked = %d, want 2", got)
	}
	if slices.Contains(b.outstanding(), "t1") {
		t.Error("the turn is still outstanding after its end was recorded")
	}
	sess, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.PodSeq != 2 {
		t.Errorf("pod seq = %d, want 2", sess.PodSeq)
	}
}
