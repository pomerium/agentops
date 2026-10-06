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
	"github.com/pomerium/agentops/harness/internal/agenttemplate"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
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

type fakeLiveSession struct {
	mu      sync.Mutex
	prompts []string
	script  func(ctx context.Context, sink sandbox.EventSink, text string) (acp.StopReason, error)
	sink    sandbox.EventSink
	id      string
	closed  bool
}

func (s *fakeLiveSession) ID() string {
	if s.id == "" {
		return "acp-sess"
	}
	return s.id
}

func (s *fakeLiveSession) Prompt(ctx context.Context, text string) (acp.StopReason, error) {
	s.mu.Lock()
	s.prompts = append(s.prompts, text)
	script, sink := s.script, s.sink
	s.mu.Unlock()
	if script != nil {
		return script(ctx, sink, text)
	}

	sink.AgentMessage(ctx, "done: "+text)
	return acp.StopReasonEndTurn, nil
}

func (s *fakeLiveSession) Cancel(context.Context) error { return nil }

func (s *fakeLiveSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeLiveSession) setScript(f func(ctx context.Context, sink sandbox.EventSink, text string) (acp.StopReason, error)) {
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

	resumeErr   error
	activateErr error
}

func newFakeLauncher() *fakeLauncher {
	return &fakeLauncher{session: &fakeLiveSession{}}
}

func (l *fakeLauncher) Prepare(_ context.Context, spec sandbox.LaunchSpec) (*sandbox.Prepared, error) {
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

func (l *fakeLauncher) Activate(_ context.Context, sink sandbox.EventSink, _ *sandbox.Prepared, _ *sandbox.Attachment) (harnessapi.LiveSession, error) {
	l.mu.Lock()
	gate, activateErr := l.gate, l.activateErr
	l.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if activateErr != nil {
		return nil, activateErr
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.session.mu.Lock()
	l.session.sink = sink
	l.session.mu.Unlock()
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
