package harnessapi_test

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pomerium/agentops/harness/api"
	apiclient "github.com/pomerium/agentops/harness/api/client"
	pb "github.com/pomerium/agentops/harness/api/pb"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/agenttemplate"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
	"github.com/pomerium/agentops/harness/internal/runner"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/sessionstore/sqlite"
)

const testApproverSubject = "auth0|6b1f0c2e9d"

const stubClient = "stub"

type fakeTemplates struct {
	tmpl     *v1alpha1.AgentTemplate
	bindings map[string]*v1alpha1.ClientBinding
}

func testTemplate() *v1alpha1.AgentTemplate {
	return &v1alpha1.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "deploy"},
		Spec: v1alpha1.AgentTemplateSpec{
			SystemPrompt: "be helpful",
			RequiredMCPServers: []v1alpha1.MCPServerRef{
				{Name: "github", URL: "https://mcp.example/github"},
			},
			WarmPoolRef: v1alpha1.SandboxWarmPoolReference{Name: "claude-code"},
		},
	}
}

func (f *fakeTemplates) Resolve(_ context.Context, name string) (*v1alpha1.AgentTemplate, error) {
	if f.tmpl != nil && f.tmpl.Name == name {
		return f.tmpl, nil
	}
	return nil, fmt.Errorf("%w: %q", agenttemplate.ErrNotFound, name)
}

func (f *fakeTemplates) List(context.Context) ([]v1alpha1.AgentTemplate, error) {
	if f.tmpl == nil {
		return nil, nil
	}
	return []v1alpha1.AgentTemplate{*f.tmpl}, nil
}

func (f *fakeTemplates) ClientBinding(_ context.Context, subject string) (*v1alpha1.ClientBinding, error) {
	if b, ok := f.bindings[subject]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("%w: %q", agenttemplate.ErrNoBinding, subject)
}

type toolCallEvent struct {
	ID       string
	Title    string
	Status   string
	Kind     string
	RawInput any
	Update   bool
}

type usageEvent struct {
	InputTokens       int64
	OutputTokens      int64
	CachedReadTokens  int64
	CachedWriteTokens int64
	ThoughtTokens     int64
	TotalTokens       int64
	ContextWindow     int64
	ContextUsed       int64
	CostUSD           float64
}

type permissionOption struct {
	ID   string
	Name string
	Kind string
}

type permissionRequest struct {
	ToolCallID string
	Title      string
	Options    []permissionOption
}

type permissionDecision struct {
	OptionID  string
	Cancelled bool
}

type fakeAgent struct {
	sess   *fakeLiveSession
	run    *fakeRun
	turnID string
	agg    *runner.Aggregator
}

func (a *fakeAgent) AgentMessage(_ context.Context, text string) {
	a.agg.Update(acp.UpdateAgentMessageText(text))
}

func (a *fakeAgent) AgentThought(_ context.Context, text string) {
	a.agg.Update(acp.UpdateAgentThoughtText(text))
}

func (a *fakeAgent) ToolCall(_ context.Context, ev toolCallEvent) {
	if ev.Update {
		opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(acp.ToolCallStatus(ev.Status))}
		if ev.Title != "" {
			opts = append(opts, acp.WithUpdateTitle(ev.Title))
		}
		a.agg.Update(acp.UpdateToolCall(acp.ToolCallId(ev.ID), opts...))
		return
	}
	opts := []acp.ToolCallStartOpt{acp.WithStartKind(acp.ToolKind(ev.Kind)), acp.WithStartStatus(acp.ToolCallStatus(ev.Status))}
	if ev.RawInput != nil {
		opts = append(opts, acp.WithStartRawInput(ev.RawInput))
	}
	a.agg.Update(acp.StartToolCall(acp.ToolCallId(ev.ID), ev.Title, opts...))
}

func (a *fakeAgent) Usage(_ context.Context, ev usageEvent) {
	a.run.emit(a.turnID, &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Usage{Usage: &agentlinkpb.Usage{
		InputTokens: ev.InputTokens, OutputTokens: ev.OutputTokens, CachedReadTokens: ev.CachedReadTokens,
		CachedWriteTokens: ev.CachedWriteTokens, ThoughtTokens: ev.ThoughtTokens, TotalTokens: ev.TotalTokens,
		ContextWindow: ev.ContextWindow, ContextUsed: ev.ContextUsed, CostUsd: ev.CostUSD,
	}}})
}

func (a *fakeAgent) Permission(ctx context.Context, req permissionRequest) (permissionDecision, error) {
	summary := req.Title
	if summary == "" {
		summary = a.agg.Title(req.ToolCallID)
	}
	pr := &agentlinkpb.PermissionRequest{
		RequestId: req.ToolCallID, ToolCallId: req.ToolCallID, Summary: summary, TurnId: a.turnID,
	}
	for _, o := range req.Options {
		pr.Options = append(pr.Options, &agentlinkpb.PermissionOption{Id: o.ID, Name: o.Name, Kind: o.Kind})
	}
	answer := a.run.awaitDecision(pr)
	a.run.emit(a.turnID, &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_PermissionRequest{PermissionRequest: pr}})
	select {
	case d := <-answer:
		return permissionDecision{OptionID: d.GetOptionId(), Cancelled: d.GetCancelled()}, nil
	case <-ctx.Done():
		return permissionDecision{Cancelled: true}, ctx.Err()
	}
}

type fakeLiveSession struct {
	mu      sync.Mutex
	prompts []string
	script  func(ctx context.Context, sink *fakeAgent, text string) (acp.StopReason, error)
	id      string
	closed  bool
	run     *fakeRun
}

type fakeRun struct {
	sess     *fakeLiveSession
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan *agentlinkpb.Prompt
	readySeq uint64
	stream   []byte

	mu            sync.Mutex
	seq           uint64
	acked         uint64
	lastTurn      uint64
	running       string
	queued        []string
	pending       []*agentlinkpb.PermissionRequest
	decisions     map[string]chan *agentlinkpb.PermissionDecision
	log           []*agentlinkpb.AgentEvent
	inbox         chan *agentlinkpb.AgentIOFrame
	done          chan struct{}
	dropPrompts   bool
	dropDecisions bool
	closeOnce     sync.Once
}

func (s *fakeLiveSession) start(readySeq uint64, stream []byte) *fakeRun {
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeRun{
		sess: s, ctx: ctx, cancel: cancel, queue: make(chan *agentlinkpb.Prompt, 64),
		readySeq: readySeq, seq: readySeq, stream: stream,
		decisions: map[string]chan *agentlinkpb.PermissionDecision{},
	}
	r.attach(readySeq)
	s.mu.Lock()
	s.run, s.closed = r, false
	s.mu.Unlock()
	go r.work()
	return r
}

func (r *fakeRun) attach(after uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inbox := make(chan *agentlinkpb.AgentIOFrame, 4096)
	state := &agentlinkpb.AgentState{LastTurnSeq: r.lastTurn, PendingPermissions: append([]*agentlinkpb.PermissionRequest(nil), r.pending...)}
	if r.running != "" {
		state.OutstandingTurnIds = append(state.OutstandingTurnIds, r.running)
	}
	state.OutstandingTurnIds = append(state.OutstandingTurnIds, r.queued...)
	inbox <- &agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_State{State: state}}
	for _, ev := range r.log {
		if ev.GetSeq() > after {
			inbox <- &agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Event{Event: ev}}
		}
	}
	r.inbox, r.done = inbox, make(chan struct{})
}

func (r *fakeRun) work() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case p := <-r.queue:
			r.mu.Lock()
			r.running = p.GetTurnId()
			r.queued = r.queued[1:]
			r.mu.Unlock()
			r.runTurn(p)
			r.mu.Lock()
			r.running = ""
			r.mu.Unlock()
		}
	}
}

func (r *fakeRun) runTurn(p *agentlinkpb.Prompt) {
	r.sess.mu.Lock()
	r.sess.prompts = append(r.sess.prompts, p.GetText())
	script := r.sess.script
	r.sess.mu.Unlock()
	agent := &fakeAgent{sess: r.sess, run: r, turnID: p.GetTurnId()}
	agent.agg = runner.NewAggregator(func(turnID string, ev *agentlinkpb.AgentEvent) { r.emit(turnID, ev) })
	agent.agg.Begin(p.GetTurnId())
	var stop acp.StopReason
	var err error
	if script != nil {
		stop, err = script(r.ctx, agent, p.GetText())
	} else {
		agent.AgentMessage(r.ctx, "done: "+p.GetText())
		stop = acp.StopReasonEndTurn
	}
	agent.agg.End()
	finished := &agentlinkpb.TurnFinished{StopReason: string(stop)}
	if err != nil {
		finished = &agentlinkpb.TurnFinished{Error: err.Error()}
	}
	r.emit(p.GetTurnId(), &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_TurnFinished{TurnFinished: finished}})
}

func (r *fakeRun) emit(turnID string, ev *agentlinkpb.AgentEvent) {
	r.mu.Lock()
	r.seq++
	ev.Seq, ev.TurnId = r.seq, turnID
	r.log = append(r.log, ev)
	inbox := r.inbox
	r.mu.Unlock()
	select {
	case inbox <- &agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Event{Event: ev}}:
	case <-r.ctx.Done():
	}
}

func (r *fakeRun) awaitDecision(req *agentlinkpb.PermissionRequest) <-chan *agentlinkpb.PermissionDecision {
	ch := make(chan *agentlinkpb.PermissionDecision, 1)
	r.mu.Lock()
	r.decisions[req.GetRequestId()] = ch
	r.pending = append(r.pending, req)
	r.mu.Unlock()
	return ch
}

func (r *fakeRun) exit(code int32) {
	r.emit("", &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Exited{Exited: &agentlinkpb.AgentExited{ExitCode: code}}})
}

func (r *fakeRun) ackedSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acked
}

func (r *fakeRun) setDrops(prompts, decisions bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropPrompts, r.dropDecisions = prompts, decisions
}

func (s *fakeLiveSession) current() *fakeRun {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.run
}

func (s *fakeLiveSession) ID() string {
	if s.id == "" {
		return "acp-sess"
	}
	return s.id
}

func (s *fakeLiveSession) StreamID() []byte { return s.current().stream }

func (s *fakeLiveSession) ReadySeq() uint64 { return s.current().readySeq }

func (s *fakeLiveSession) Inbox() <-chan *agentlinkpb.AgentIOFrame {
	r := s.current()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inbox
}

func (s *fakeLiveSession) Done() <-chan struct{} {
	r := s.current()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

func (s *fakeLiveSession) Ack(seq uint64) {
	r := s.current()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acked = max(r.acked, seq)
}

func (s *fakeLiveSession) Prompt(turnID string, turnSeq uint64, text string) {
	r := s.current()
	r.mu.Lock()
	if r.dropPrompts || turnSeq <= r.lastTurn {
		r.mu.Unlock()
		return
	}
	r.lastTurn = turnSeq
	r.queued = append(r.queued, turnID)
	r.mu.Unlock()
	r.queue <- &agentlinkpb.Prompt{TurnId: turnID, TurnSeq: turnSeq, Text: text}
}

func (s *fakeLiveSession) Decide(requestID, optionID string, cancelled bool) {
	r := s.current()
	r.mu.Lock()
	if r.dropDecisions {
		r.mu.Unlock()
		return
	}
	ch := r.decisions[requestID]
	delete(r.decisions, requestID)
	for i, p := range r.pending {
		if p.GetRequestId() == requestID {
			r.pending = append(r.pending[:i:i], r.pending[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
	if ch != nil {
		ch <- &agentlinkpb.PermissionDecision{RequestId: requestID, OptionId: optionID, Cancelled: cancelled}
	}
}

func (s *fakeLiveSession) Close() error {
	s.mu.Lock()
	s.closed = true
	r := s.run
	s.mu.Unlock()
	if r != nil {
		r.closeOnce.Do(func() {
			r.cancel()
			r.mu.Lock()
			close(r.done)
			r.mu.Unlock()
		})
	}
	return nil
}

func (s *fakeLiveSession) setScript(f func(ctx context.Context, sink *fakeAgent, text string) (acp.StopReason, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.script = f
}

func (s *fakeLiveSession) promptList() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.prompts...)
}

type fakeLauncher struct {
	mu       sync.Mutex
	session  *fakeLiveSession
	prepares int
	revives  []string

	resumeSessionID string

	resumeTemplate *v1alpha1.AgentTemplate
	suspends       []string
	teardowns      []string
	expectedRuns   []string
	sup            sandbox.Supervision

	gate chan struct{}

	prepareGate chan struct{}

	suspendEntered chan struct{}
	suspendGate    chan struct{}

	resumeErr   error
	activateErr error
}

func newFakeLauncher() *fakeLauncher {
	return &fakeLauncher{session: &fakeLiveSession{}}
}

func (l *fakeLauncher) Prepare(_ context.Context, spec sandbox.LaunchSpec) (*sandbox.Prepared, error) {
	l.mu.Lock()
	prepareGate := l.prepareGate
	l.mu.Unlock()
	if prepareGate != nil {
		<-prepareGate
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prepares++
	return &sandbox.Prepared{
		ClaimName:   "claim-1",
		SandboxName: "sbx-1",
		LeaseUntil:  time.Now().Add(time.Hour),
		Executor: agenticrun.Executor{
			Namespace: "ns", ServiceAccount: "sandbox-agent", PodName: "sbx-1", PodUID: "uid-A",
		},
	}, nil
}

func (l *fakeLauncher) Expect(runID string, _ *sandbox.Prepared, opts ...sandbox.SupervisionOption) (*sandbox.Attachment, error) {
	var sup sandbox.Supervision
	for _, opt := range opts {
		opt(&sup)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expectedRuns = append(l.expectedRuns, runID)
	l.sup = sup
	return &sandbox.Attachment{}, nil
}

func (l *fakeLauncher) Activate(ctx context.Context, _ *sandbox.Prepared, _ *sandbox.Attachment) (harnessapi.LiveSession, error) {
	l.mu.Lock()
	gate, activateErr := l.gate, l.activateErr
	l.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if activateErr != nil {
		return nil, activateErr
	}
	l.session.start(1, []byte("fake-stream"))
	return l.session, nil
}

func (l *fakeLauncher) Teardown(_ context.Context, claimName string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.teardowns = append(l.teardowns, claimName)
	return nil
}

func (l *fakeLauncher) Suspend(_ context.Context, claimName string) error {
	l.mu.Lock()
	entered, gate := l.suspendEntered, l.suspendGate
	l.suspendEntered, l.suspendGate = nil, nil
	l.mu.Unlock()
	if gate != nil {
		close(entered)
		<-gate
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.suspends = append(l.suspends, claimName)
	return nil
}

func (l *fakeLauncher) Revive(_ context.Context, claimName string, spec sandbox.LaunchSpec) (*sandbox.Prepared, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.resumeErr != nil {
		return nil, l.resumeErr
	}
	l.revives = append(l.revives, claimName)
	l.resumeSessionID = spec.ResumeACPSessionID
	l.resumeTemplate = spec.Template
	return &sandbox.Prepared{
		ClaimName:   claimName,
		SandboxName: "sbx-1",
		Resumed:     true,
		LeaseUntil:  time.Now().Add(time.Hour),
		Executor: agenticrun.Executor{
			Namespace: "ns", ServiceAccount: "sandbox-agent", PodName: "sbx-1", PodUID: "uid-REVIVED",
		},
	}, nil
}

func (l *fakeLauncher) ExtendLease(_ context.Context, _ string) (time.Time, error) {
	return time.Now().Add(time.Hour), nil
}

func (l *fakeLauncher) LeaseLength() time.Duration { return time.Hour }

func (l *fakeLauncher) openGate() {
	l.mu.Lock()
	gate := l.gate
	l.gate = nil
	l.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (l *fakeLauncher) down(cause string) {
	l.mu.Lock()
	f := l.sup.OnDown
	l.mu.Unlock()
	if f != nil {
		f(cause)
	}
}

func (l *fakeLauncher) snapshot() (revives, suspends, teardowns []string, resumeID string, resumeTmpl *v1alpha1.AgentTemplate) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.revives...), append([]string(nil), l.suspends...),
		append([]string(nil), l.teardowns...), l.resumeSessionID, l.resumeTemplate
}

type fakeRunClient struct {
	mu      sync.Mutex
	reqs    []agenticrun.CreateRunRequest
	created int
	status  *agenticrun.RunStatus
	revoked bool
}

func (f *fakeRunClient) CreateRun(_ context.Context, req agenticrun.CreateRunRequest) (*agenticrun.CreateRunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	f.created++
	return &agenticrun.CreateRunResult{
		RunID:       fmt.Sprintf("run-%d", f.created),
		ApprovalURL: fmt.Sprintf("https://pom.example/agentic/approve?run_id=run-%d", f.created),
		ExpiresAt:   time.Now().Add(15 * time.Minute),
	}, nil
}

func (f *fakeRunClient) GetRun(context.Context, string) (*agenticrun.RunStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status != nil {
		return f.status, nil
	}
	return &agenticrun.RunStatus{
		State: "approved", ApproverSubject: testApproverSubject, Revoked: f.revoked,
	}, nil
}

func (f *fakeRunClient) requests() []agenticrun.CreateRunRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]agenticrun.CreateRunRequest(nil), f.reqs...)
}

type harness struct {
	t        *testing.T
	svc      *harnessapi.Service
	store    sessionstore.Store
	launcher *fakeLauncher
	runs     *fakeRunClient
	tmpl     *fakeTemplates
	dbPath   string
}

func newHarness(t *testing.T, opts ...harnessapi.Option) *harness {
	t.Helper()
	return newHarnessAt(t, filepath.Join(t.TempDir(), "test.db"), opts...)
}

func newHarnessAt(t *testing.T, dbPath string, opts ...harnessapi.Option) *harness {
	t.Helper()
	st, err := sqlite.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	h := &harness{
		t:        t,
		store:    st,
		launcher: newFakeLauncher(),
		runs:     &fakeRunClient{},
		tmpl:     &fakeTemplates{tmpl: testTemplate()},
		dbPath:   dbPath,
	}
	for _, client := range []string{stubClient, "other", "admin", "verified-client"} {
		bind(h, client, []string{"deploy"}, nil)
	}
	opts = append([]harnessapi.Option{harnessapi.WithLogger(testLogger(t))}, opts...)
	h.svc = harnessapi.New(st, harnessapi.NewEventLog(st), h.launcher, h.tmpl, h.runs, opts...)
	return h
}

func as(clientID string) context.Context {
	return apiserver.WithClientID(context.Background(), clientID)
}

func byID(id string) *pb.SessionRef { return &pb.SessionRef{SessionId: id} }

func (h *harness) subscribe(ctx context.Context, req *pb.SubscribeRequest) (*apiclient.Subscription, error) {
	h.t.Helper()
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	sub, err := apiclient.Subscribe(ctx, serveAPI(h.t, h, clientID), req, apiclient.WithLogger(testLogger(h.t)))
	if err != nil {
		return nil, err
	}

	h.t.Cleanup(sub.Close)
	return sub, nil
}

type testWriter struct {
	mu   sync.Mutex
	t    *testing.T
	done bool
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(newTestWriter(t), &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestWriter(t *testing.T) *testWriter {
	w := &testWriter{t: t}
	t.Cleanup(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.done = true
	})
	return w
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.done {
		w.t.Logf("%s", p)
	}
	return len(p), nil
}

type recorder struct {
	t   *testing.T
	mu  sync.Mutex
	c   *sync.Cond
	seq int64
	evs []*pb.Event
}

type feed interface {
	Events() <-chan *pb.Event
}

func record(t *testing.T, sub feed) *recorder {
	t.Helper()
	r := &recorder{t: t}
	r.c = sync.NewCond(&r.mu)
	go func() {
		for ev := range sub.Events() {
			r.mu.Lock()
			if ev.Seq <= r.seq {
				r.mu.Unlock()
				continue
			}
			r.seq = ev.Seq
			r.evs = append(r.evs, ev)
			r.c.Broadcast()
			r.mu.Unlock()
		}
	}()
	return r
}

func (r *recorder) waitFor(kind string, from int) (*pb.Event, int) {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				r.mu.Lock()
				r.c.Broadcast()
				r.mu.Unlock()
			}
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		for i := from; i < len(r.evs); i++ {
			if api.Kind(r.evs[i]) == kind {
				return r.evs[i], i
			}
		}
		if time.Now().After(deadline) {
			var seen []string
			for _, ev := range r.evs {
				seen = append(seen, api.Kind(ev))
			}
			r.t.Fatalf("timed out waiting for %s after index %d; saw %v", kind, from, seen)
			return nil, 0
		}
		r.c.Wait()
	}
}

func (r *recorder) kinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.evs))
	for _, ev := range r.evs {
		out = append(out, api.Kind(ev))
	}
	return out
}
