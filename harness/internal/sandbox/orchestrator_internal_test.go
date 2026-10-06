package sandbox

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

type fakeClaims struct {
	mu      sync.Mutex
	claims  map[string]*sbxv1.SandboxClaim
	deleted []string
}

func newFakeClaims() *fakeClaims {
	return &fakeClaims{claims: map[string]*sbxv1.SandboxClaim{}}
}

func (f *fakeClaims) Create(_ context.Context, claim *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := claim.DeepCopy()
	c.Status.SandboxStatus.Name = "pod-" + claim.Name
	c.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: "True"}}
	f.claims[c.Name] = c
	return c, nil
}

func (f *fakeClaims) Get(_ context.Context, name string) (*sbxv1.SandboxClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims[name], nil
}

func (f *fakeClaims) Delete(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, name)
	delete(f.claims, name)
	return nil
}

func (f *fakeClaims) deletedNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

type fakeAgentLink struct {
	mu        sync.Mutex
	expected  map[string]*fakeHandle
	configs   map[string]*agentlinkpb.SandboxConfig
	forgotten []string
	expectErr error
}

func newFakeAgentLink() *fakeAgentLink {
	return &fakeAgentLink{expected: map[string]*fakeHandle{}, configs: map[string]*agentlinkpb.SandboxConfig{}}
}

func (g *fakeAgentLink) Expect(runID string, _ agenticrun.Executor, cfg *agentlinkpb.SandboxConfig, opts ...agentlink.ExpectOption) (AttachHandle, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.expectErr != nil {
		return nil, g.expectErr
	}
	var callbacks agentlink.ExpectCallbacks
	for _, opt := range opts {
		opt(&callbacks)
	}
	h := &fakeHandle{
		opts:     callbacks,
		attached: make(chan struct{}),
		ready:    make(chan struct{}),
		ioReady:  make(chan struct{}),
		spawned:  make(chan struct{}, 1),
	}
	g.expected[runID] = h
	g.configs[runID] = cfg
	return h, nil
}

func (g *fakeAgentLink) Forget(runID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.expected, runID)
	g.forgotten = append(g.forgotten, runID)
}

func (g *fakeAgentLink) live() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.expected)
}

func (g *fakeAgentLink) handle(runID string) *fakeHandle {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.expected[runID]
}

func (g *fakeAgentLink) config(runID string) *agentlinkpb.SandboxConfig {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.configs[runID]
}

type fakeHandle struct {
	opts     agentlink.ExpectCallbacks
	attached chan struct{}
	ready    chan struct{}
	ioReady  chan struct{}
	spawned  chan struct{}

	mu           sync.Mutex
	shutdownWith string
}

func (h *fakeHandle) AwaitAttach(ctx context.Context) error {
	select {
	case <-h.attached:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *fakeHandle) AwaitReady(ctx context.Context) error {
	select {
	case <-h.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *fakeHandle) SpawnAgent(context.Context) error {
	select {
	case h.spawned <- struct{}{}:
	default:
	}
	close(h.ioReady)
	return nil
}

func (h *fakeHandle) AwaitAgentIO(ctx context.Context) (io.Writer, io.Reader, error) {
	select {
	case <-h.ioReady:
		return io.Discard, &blockingReader{}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (h *fakeHandle) Shutdown(reason string) {
	h.mu.Lock()
	h.shutdownWith = reason
	h.mu.Unlock()
}

func (h *fakeHandle) shutdownReason() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.shutdownWith
}

func (h *fakeHandle) attach(attempt uint32, agentRunning bool) {
	if h.opts.OnAttached != nil {
		h.opts.OnAttached(attempt, agentRunning)
	}
	close(h.attached)
	close(h.ready)
}

func testTemplate() *v1alpha1.AgentTemplate {
	return &v1alpha1.AgentTemplate{
		Spec: v1alpha1.AgentTemplateSpec{
			WarmPoolRef:   v1alpha1.SandboxWarmPoolReference{Name: "claude-code"},
			SessionConfig: map[string]string{"model": "opus"},
		},
	}
}

func stubOpenSession(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, closeFn func() error, _ SessionParams) (*Session, error) {
	return &Session{close: closeFn}, nil
}

func testPods() PodGetter {
	return &fakePodGetter{pods: []*corev1.Pod{pod("ns", "sandbox-agent", "uid-1")}}
}

func newTestOrchestrator(t *testing.T, claims ClaimClient, link AgentLink) *Orchestrator {
	t.Helper()
	o := New(claims, testPods(), nil, link, WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"))
	o.openSession = stubOpenSession
	return o
}

func TestPrepareCreatesAnAdoptableClaim(t *testing.T) {
	claims := newFakeClaims()
	o := newTestOrchestrator(t, claims, newFakeAgentLink())

	if _, err := o.Prepare(context.Background(), LaunchSpec{
		SessionID: "s1", Template: testTemplate(),
		Endpoints: []ProxiedEndpoint{{
			Name: "agno", ListenPort: 9100, UpstreamURL: "https://docs.agno.com/mcp",
		}},
	}); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	claim, err := claims.Get(context.Background(), ClaimName("s1"))
	if err != nil || claim == nil {
		t.Fatalf("claim not created: %v", err)
	}
	if len(claim.Spec.Env) != 0 {
		t.Errorf("claim carries env, which forfeits adoption: %+v", claim.Spec.Env)
	}
}

func TestExpectCarriesTheSessionEndpoints(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{
		SessionID: "s1", Template: testTemplate(),
		Endpoints: []ProxiedEndpoint{
			{Name: "agno", ListenPort: 9100, UpstreamURL: "https://docs.agno.com/mcp"},
			{
				Name: "gke", ListenPort: 9101,
				UpstreamURL: "https://gke.example.com/mcp",
				DialAddress: "pomerium.ns.svc:443",
			},
		},
	})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := o.Expect("run-1", prepared); err != nil {
		t.Fatalf("Expect: %v", err)
	}

	cfg := link.config("run-1")
	if got := len(cfg.GetEndpoints()); got != 2 {
		t.Fatalf("config carries %d endpoints, want 2: %+v", got, cfg.GetEndpoints())
	}
	byPort := map[uint32]*agentlinkpb.ProxiedEndpoint{}
	for _, ep := range cfg.GetEndpoints() {
		byPort[ep.GetListenPort()] = ep
	}
	agno := byPort[9100]
	if agno.GetName() != SidecarEndpointName("agno") {
		t.Errorf("endpoint name = %q, want it namespaced as %q", agno.GetName(), SidecarEndpointName("agno"))
	}
	if agno.GetUpstreamUrl() != "https://docs.agno.com/mcp" {
		t.Errorf("agno endpoint = %+v", agno)
	}
	gke := byPort[9101]
	if gke.GetDialAddress() != "pomerium.ns.svc:443" {
		t.Errorf("gke endpoint lost a field: %+v", gke)
	}
}

func TestActivateWaitsForReadyBeforeSpawning(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	att, err := o.Expect("run-1", prepared)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	h := link.handle("run-1")

	done := make(chan error, 1)
	go func() {
		_, err := o.Activate(context.Background(), nil, prepared, att)
		done <- err
	}()

	close(h.attached)
	if h.opts.OnAttached != nil {
		h.opts.OnAttached(1, false)
	}
	select {
	case <-h.spawned:
		t.Fatal("the agent was spawned before the sandbox reported ready")
	case <-time.After(100 * time.Millisecond):
	}

	close(h.ready)
	select {
	case <-h.spawned:
	case <-time.After(2 * time.Second):
		t.Fatal("the agent was never spawned after the sandbox reported ready")
	}
	if err := <-done; err != nil {
		t.Fatalf("Activate: %v", err)
	}
}

func TestActivateOpensSessionOverTheTunnel(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)

	var gotParams SessionParams
	o.openSession = func(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, closeFn func() error, params SessionParams) (*Session, error) {
		gotParams = params
		return &Session{close: closeFn}, nil
	}

	endpoints := []ProxiedEndpoint{{
		Name: "agno", ListenPort: 9100, UpstreamURL: "https://docs.agno.com/mcp",
	}}
	prepared, err := o.Prepare(context.Background(), LaunchSpec{
		SessionID: "s1", Template: testTemplate(), Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	att, err := o.Expect("run-1", prepared)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		link.handle("run-1").attach(1, false)
	}()

	sess, err := o.Activate(context.Background(), nil, prepared, att)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	select {
	case <-link.handle("run-1").spawned:
	default:
		t.Error("the agent was never spawned")
	}

	if len(gotParams.MCPServers) != 1 {
		t.Fatalf("session MCP servers = %+v, want 1", gotParams.MCPServers)
	}
	http := gotParams.MCPServers[0].Http
	if http == nil || http.Url != "http://127.0.0.1:9100/mcp" {
		t.Errorf("session MCP server = %+v, want the rewritten loopback URL", gotParams.MCPServers[0])
	}
	if len(http.Headers) != 0 {
		t.Errorf("session MCP server carries headers: %+v", http.Headers)
	}
	if got := gotParams.SessionConfig["model"]; got != "opus" {
		t.Errorf("session config model = %q, want opus", got)
	}

	_ = sess.Close()
	if reason := link.handle("run-1"); reason != nil {
		t.Error("the expectation survived the session close")
	}
	if link.live() != 0 {
		t.Errorf("the Agent Link still holds %d expectations", link.live())
	}
}

func TestActivateFailureForgetsAndTearsDown(t *testing.T) {
	link := newFakeAgentLink()
	claims := newFakeClaims()
	o := New(claims, testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachTimeout(50*time.Millisecond),
	)
	o.openSession = func(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, closeFn func() error, _ SessionParams) (*Session, error) {
		t.Error("openSession must not run when the sandbox never attached")
		return nil, errors.New("unreachable")
	}

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	att, err := o.Expect("run-1", prepared)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}

	if _, err := o.Activate(context.Background(), nil, prepared, att); err == nil {
		t.Fatal("Activate succeeded although the sandbox never attached")
	}
	if link.live() != 0 {
		t.Errorf("the expectation leaked: %d still registered", link.live())
	}
	if names := claims.deletedNames(); len(names) != 1 {
		t.Errorf("claim not torn down: %v", names)
	}
}

func TestFailedReviveActivationKeepsTheClaim(t *testing.T) {
	for _, tc := range []struct {
		name    string
		attach  bool
		openErr error
	}{
		{name: "attach timeout"},
		{name: "acp setup error", attach: true, openErr: errors.New("initialize failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			link := newFakeAgentLink()
			claims := newFakeClaims()
			o := New(claims, testPods(), nil, link,
				WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
				WithAttachTimeout(50*time.Millisecond),
			)
			o.openSession = func(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, _ func() error, _ SessionParams) (*Session, error) {
				return nil, tc.openErr
			}

			prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
			if err != nil {
				t.Fatalf("Prepare: %v", err)
			}
			prepared.Resumed = true
			att, err := o.Expect("run-1", prepared)
			if err != nil {
				t.Fatalf("Expect: %v", err)
			}
			if tc.attach {
				link.handle("run-1").attach(1, false)
			}

			if _, err := o.Activate(context.Background(), nil, prepared, att); err == nil {
				t.Fatal("Activate succeeded")
			}
			if names := claims.deletedNames(); len(names) != 0 {
				t.Errorf("a failed revive deleted the retained claim: %v", names)
			}
			if link.live() != 0 {
				t.Errorf("the expectation leaked: %d still registered", link.live())
			}
		})
	}
}

func TestGraceWindowCancelledByReattach(t *testing.T) {
	link := newFakeAgentLink()
	o := New(newFakeClaims(), testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachGrace(300*time.Millisecond),
	)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	var downs []string
	var mu sync.Mutex
	if _, err := o.Expect("run-1", prepared, WithOnDown(func(cause string) {
		mu.Lock()
		downs = append(downs, cause)
		mu.Unlock()
	})); err != nil {
		t.Fatalf("Expect: %v", err)
	}
	h := link.handle("run-1")

	h.opts.OnLost(errors.New("idle timeout"))
	time.Sleep(50 * time.Millisecond)
	h.opts.OnAttached(2, true)

	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(downs) != 0 {
		t.Errorf("the session was closed despite a re-attach inside the grace window: %v", downs)
	}
}

func TestGraceWindowExpiryClosesOnce(t *testing.T) {
	link := newFakeAgentLink()
	o := New(newFakeClaims(), testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachGrace(50*time.Millisecond),
	)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	downs := make(chan string, 4)
	if _, err := o.Expect("run-1", prepared, WithOnDown(func(cause string) { downs <- cause })); err != nil {
		t.Fatalf("Expect: %v", err)
	}
	h := link.handle("run-1")

	h.opts.OnLost(errors.New("first"))
	h.opts.OnLost(errors.New("second"))
	select {
	case cause := <-downs:
		if cause != "tunnel_lost" {
			t.Errorf("cause = %q, want tunnel_lost", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the grace window never expired")
	}
	h.opts.OnAgentExit(1)
	h.opts.OnError("envoy_exited", errors.New("boom"))
	time.Sleep(100 * time.Millisecond)
	if len(downs) != 0 {
		t.Errorf("the session was closed more than once: %d extra", len(downs))
	}
}

func TestForgottenAttachmentNeverReportsDown(t *testing.T) {
	link := newFakeAgentLink()
	o := New(newFakeClaims(), testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachGrace(10*time.Millisecond),
	)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	downs := make(chan string, 4)
	att, err := o.Expect("run-1", prepared, WithOnDown(func(cause string) { downs <- cause }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	h := link.handle("run-1")

	h.opts.OnLost(errors.New("idle timeout"))
	att.Forget()
	select {
	case cause := <-downs:
		t.Fatalf("a forgotten attachment's grace timer reported down: %s", cause)
	case <-time.After(200 * time.Millisecond):
	}

	h.opts.OnAgentExit(1)
	select {
	case cause := <-downs:
		t.Fatalf("a forgotten attachment reported down: %s", cause)
	default:
	}
}

func TestReattachWithoutAgentClosesTheSession(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	downs := make(chan string, 1)
	att, err := o.Expect("run-1", prepared, WithOnDown(func(cause string) { downs <- cause }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	h := link.handle("run-1")

	h.attach(1, false)
	if _, err := o.Activate(context.Background(), nil, prepared, att); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	select {
	case cause := <-downs:
		t.Fatalf("the first attach closed the session (%s)", cause)
	default:
	}

	h.opts.OnAttached(2, false)
	select {
	case cause := <-downs:
		if cause != "sidecar_restarted" {
			t.Errorf("cause = %q, want sidecar_restarted", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a re-attach without the agent did not close the session")
	}
}

func TestFirstAttachAfterRetriesKeepsSupervision(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	downs := make(chan string, 4)
	att, err := o.Expect("run-1", prepared, WithOnDown(func(cause string) { downs <- cause }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	h := link.handle("run-1")

	h.attach(3, false)
	if _, err := o.Activate(context.Background(), nil, prepared, att); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	h.opts.OnAgentExit(1)
	select {
	case cause := <-downs:
		if cause != "agent_exited" {
			t.Errorf("cause = %q, want agent_exited", cause)
		}
	default:
		t.Fatal("the agent exit was never reported")
	}
}

func TestExpectFailureIsReported(t *testing.T) {
	link := newFakeAgentLink()
	link.expectErr = errors.New("already expected")
	o := newTestOrchestrator(t, newFakeClaims(), link)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := o.Expect("run-1", prepared); err == nil {
		t.Fatal("Expect succeeded although the Agent Link refused")
	}
}

type blockingReader struct {
	once sync.Once
	ch   chan struct{}
}

func (b *blockingReader) Read([]byte) (int, error) {
	b.once.Do(func() { b.ch = make(chan struct{}) })
	<-b.ch
	return 0, io.EOF
}
