package harnessapi

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
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

type promptProbe struct {
	idleSession
	mu       sync.Mutex
	last     uint64
	accepted []string
}

func (p *promptProbe) Prompt(turnID string, turnSeq uint64, _ string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if turnSeq > p.last {
		p.last = turnSeq
		p.accepted = append(p.accepted, turnID)
	}
}

func (p *promptProbe) acceptedTurns() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.accepted)
}

type commandsHook struct {
	Store
	hook func()
}

func (s commandsHook) ListPodCommands(ctx context.Context, sessionID string) ([]sessionstore.PodCommand, error) {
	cmds, err := s.Store.ListPodCommands(ctx, sessionID)
	s.hook()
	return cmds, err
}

func TestAReconnectSendsTheOlderTurnBeforeANewOne(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})
	b := svc.lookup("s1")

	id, n, err := svc.nextTurn(ctx, "s1")
	if err != nil {
		t.Fatalf("nextTurn: %v", err)
	}
	b.enter(id)
	svc.sendPrompt(ctx, b, id, uint64(n), "first")

	live := &promptProbe{}
	b.session = live
	sess, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	newer := make(chan string, 1)
	svc.store = commandsHook{Store: st, hook: func() {
		sent := make(chan struct{})
		go func() {
			defer close(sent)
			turnID, err := svc.startTurn(ctx, sess, "second")
			if err != nil {
				t.Errorf("startTurn: %v", err)
			}
			newer <- turnID
		}()
		select {
		case <-sent:
		case <-time.After(200 * time.Millisecond):
		}
	}}

	svc.reconcileState(ctx, b, &agentlinkpb.AgentState{})
	var second string
	select {
	case second = <-newer:
	case <-time.After(5 * time.Second):
		t.Fatal("the newer turn was never sent")
	}

	if got, want := live.acceptedTurns(), []string{id, second}; !slices.Equal(got, want) {
		t.Errorf("turns the agent accepted = %v, want %v", got, want)
	}
	if !slices.Contains(b.outstanding(), second) {
		t.Errorf("outstanding = %v, want it to keep %s", b.outstanding(), second)
	}
}

type decisionProbe struct {
	idleSession
	decisions chan bool
}

func (p *decisionProbe) Decide(_, _ string, cancelled bool) { p.decisions <- cancelled }

func TestShutdownStopsThePermissionTimers(t *testing.T) {
	svc, _ := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	live := &decisionProbe{decisions: make(chan bool, 1)}
	b.session = live
	b.sink.permTimeout = time.Hour
	b.sink.await(&agentlinkpb.PermissionRequest{RequestId: "request", TurnId: "t1"}, func(id string) {
		svc.expirePermission(context.Background(), b, id)
	})

	svc.Shutdown()

	b.sink.mu.Lock()
	if w := b.sink.waiters["request"]; w != nil {
		w.timer.Reset(time.Millisecond)
	}
	b.sink.mu.Unlock()

	select {
	case cancelled := <-live.decisions:
		t.Fatalf("a retired manager sent a decision (cancelled=%v)", cancelled)
	case <-time.After(100 * time.Millisecond):
	}
}

type flakyCommands struct {
	Store
	failures atomic.Int32
}

func (s *flakyCommands) ListPodCommands(ctx context.Context, sessionID string) ([]sessionstore.PodCommand, error) {
	if s.failures.Load() > 0 {
		s.failures.Add(-1)
		return nil, errors.New("database unavailable")
	}
	return s.Store.ListPodCommands(ctx, sessionID)
}

func TestAFailedOutboxReadHoldsNewTurnsUntilTheReplayIsDone(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	older, n, err := svc.nextTurn(ctx, "s1")
	if err != nil {
		t.Fatalf("nextTurn: %v", err)
	}
	b.enter(older)
	svc.sendPrompt(ctx, b, older, uint64(n), "first")

	live := &promptProbe{}
	b.session = live
	sess, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	commands := &flakyCommands{Store: st}
	commands.failures.Store(3)
	svc.store = commands

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.reconcileState(ctx, b, &agentlinkpb.AgentState{})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for commands.failures.Load() == 3 {
		if time.Now().After(deadline) {
			t.Fatal("the outbox was never read")
		}
		time.Sleep(time.Millisecond)
	}
	newer, err := svc.startTurn(ctx, sess, "second")
	if err != nil {
		t.Fatalf("startTurn: %v", err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the replay never finished")
	}

	if got, want := live.acceptedTurns(), []string{older, newer}; !slices.Equal(got, want) {
		t.Errorf("turns the agent accepted = %v, want %v", got, want)
	}
	for _, id := range []string{older, newer} {
		if !slices.Contains(b.outstanding(), id) {
			t.Errorf("outstanding = %v, want it to keep %s", b.outstanding(), id)
		}
	}
}

type refusedOutbox struct {
	Store
	refuse atomic.Bool
}

func (s *refusedOutbox) PutPodCommand(ctx context.Context, cmd sessionstore.PodCommand) error {
	if s.refuse.Load() {
		return errors.New("database unavailable")
	}
	return s.Store.PutPodCommand(ctx, cmd)
}

func TestAnUnsavedPromptIsNotAccepted(t *testing.T) {
	ctx := context.Background()
	svc, st := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	live := &promptProbe{}
	b.session = live
	outbox := &refusedOutbox{Store: st}
	outbox.refuse.Store(true)
	svc.store = outbox
	sess, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	if _, err := svc.startTurn(ctx, sess, "hello"); !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("startTurn with a failed save: err = %v, want %v", err, api.ErrUnavailable)
	}
	if got := live.acceptedTurns(); len(got) != 0 {
		t.Errorf("the agent got %v for a turn that was not saved", got)
	}
	if got := b.outstanding(); len(got) != 0 {
		t.Errorf("outstanding = %v, want no turn", got)
	}
}

type decisionLog struct {
	idleSession
	mu        sync.Mutex
	decisions []*agentlinkpb.PermissionDecision
}

func (p *decisionLog) Decide(requestID, optionID string, cancelled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.decisions = append(p.decisions, &agentlinkpb.PermissionDecision{RequestId: requestID, OptionId: optionID, Cancelled: cancelled})
}

func (p *decisionLog) sent() []*agentlinkpb.PermissionDecision {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.decisions)
}

type anyClient struct{ Templates }

func (anyClient) ClientBinding(context.Context, string) (*v1alpha1.ClientBinding, error) {
	return &v1alpha1.ClientBinding{}, nil
}

func TestAnUnsavedPermissionChoiceStaysAnswerable(t *testing.T) {
	ctx := apiserver.WithClientID(context.Background(), "client")
	svc, st := runningService(t, quietLauncher{})
	svc.templates = anyClient{}
	b := svc.lookup("s1")
	live := &decisionLog{}
	b.session = live
	b.sink.await(&agentlinkpb.PermissionRequest{
		RequestId: "request", TurnId: "t1", Options: []*agentlinkpb.PermissionOption{{Id: "allow"}},
	}, func(id string) { svc.expirePermission(context.Background(), b, id) })
	outbox := &refusedOutbox{Store: st}
	outbox.refuse.Store(true)
	svc.store = outbox
	req := &pb.RespondPermissionRequest{Ref: &pb.SessionRef{SessionId: "s1"}, RequestId: "request", OptionId: "allow"}

	if _, err := svc.RespondPermission(ctx, req); !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("RespondPermission with a failed save: err = %v, want %v", err, api.ErrUnavailable)
	}
	if got := live.sent(); len(got) != 0 {
		t.Fatalf("sent %v for a choice that was not saved", got)
	}

	outbox.refuse.Store(false)
	if _, err := svc.RespondPermission(ctx, req); err != nil {
		t.Fatalf("RespondPermission after the store recovers: %v", err)
	}
	if got := live.sent(); len(got) != 1 || got[0].GetOptionId() != "allow" || got[0].GetCancelled() {
		t.Errorf("decisions sent = %v, want one allow", got)
	}
}

func TestAnExpiryThatCannotBeSavedIsRetried(t *testing.T) {
	svc, st := runningService(t, quietLauncher{})
	b := svc.lookup("s1")
	live := &decisionLog{}
	b.session = live
	outbox := &refusedOutbox{Store: st}
	outbox.refuse.Store(true)
	svc.store = outbox
	b.sink.permTimeout = 10 * time.Millisecond
	b.sink.expireRetry = 10 * time.Millisecond
	b.sink.await(&agentlinkpb.PermissionRequest{RequestId: "request", TurnId: "t1"}, func(id string) {
		svc.expirePermission(context.Background(), b, id)
	})

	time.Sleep(100 * time.Millisecond)
	if got := live.sent(); len(got) != 0 {
		t.Fatalf("sent %v for an expiry that was not saved", got)
	}
	outbox.refuse.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for len(live.sent()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the expiry was never sent after the store recovered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := live.sent(); len(got) != 1 || !got[0].GetCancelled() {
		t.Errorf("decisions sent = %v, want one cancellation", got)
	}
}
