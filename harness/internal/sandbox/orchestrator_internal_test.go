package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	agentsv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
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
	mu         sync.Mutex
	expected   map[string]*fakeHandle
	configs    map[string]*agentlinkpb.SandboxConfig
	forgotten  []string
	expectErr  error
	spawnReply func(h *fakeHandle)
	onExpect   func(h *fakeHandle)
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
		spawned:  make(chan *agentlinkpb.SessionParams, 1),
		inbox:    make(chan *agentlinkpb.AgentIOFrame, 16),
		done:     make(chan struct{}),
		reply:    g.spawnReply,
	}
	g.expected[runID] = h
	g.configs[runID] = cfg
	if g.onExpect != nil {
		g.onExpect(h)
	}
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
	spawned  chan *agentlinkpb.SessionParams
	inbox    chan *agentlinkpb.AgentIOFrame
	done     chan struct{}
	reply    func(h *fakeHandle)

	mu           sync.Mutex
	shutdownWith string
	sent         []*agentlinkpb.AgentIOFrame
	acked        uint64
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

func (h *fakeHandle) SpawnAgent(_ context.Context, params *agentlinkpb.SessionParams) error {
	select {
	case h.spawned <- params:
	default:
	}
	if h.reply != nil {
		h.reply(h)
	} else {
		h.push(&agentlinkpb.AgentEvent{Seq: 1, Payload: &agentlinkpb.AgentEvent_SessionReady{
			SessionReady: &agentlinkpb.SessionReady{AcpSessionId: "acp-1", Resumable: true},
		}})
	}
	return nil
}

func (h *fakeHandle) push(ev *agentlinkpb.AgentEvent) {
	h.inbox <- &agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Event{Event: ev}}
}

func (h *fakeHandle) StreamID() []byte {
	if len(h.opts.StreamID) > 0 {
		return h.opts.StreamID
	}
	return []byte("fake-stream")
}

func (h *fakeHandle) Inbox() <-chan *agentlinkpb.AgentIOFrame { return h.inbox }

func (h *fakeHandle) Done() <-chan struct{} { return h.done }

func (h *fakeHandle) Ack(seq uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.acked = max(h.acked, seq)
}

func (h *fakeHandle) Send(f *agentlinkpb.AgentIOFrame) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sent = append(h.sent, f)
}

func (h *fakeHandle) sentFrames() []*agentlinkpb.AgentIOFrame {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*agentlinkpb.AgentIOFrame(nil), h.sent...)
}

func (h *fakeHandle) ackedSeq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.acked
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

func testPods() PodGetter {
	return &fakePodGetter{pods: []*corev1.Pod{pod("ns", "sandbox-agent", "uid-1")}}
}

func newTestOrchestrator(t *testing.T, claims ClaimClient, link AgentLink) *Orchestrator {
	t.Helper()
	return New(claims, testPods(), nil, link, WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"))
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
		_, err := o.Activate(context.Background(), prepared, att)
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

func TestActivateSpawnsWithTheSessionParametersAndWaitsForReady(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)

	endpoints := []ProxiedEndpoint{{
		Name: "agno", ListenPort: 9100, UpstreamURL: "https://docs.agno.com/mcp",
	}}
	prepared, err := o.Prepare(context.Background(), LaunchSpec{
		SessionID: "s1", Template: testTemplate(), Endpoints: endpoints,
		SystemPrompt: "be terse", ResumeACPSessionID: "acp-0",
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

	sess, err := o.Activate(context.Background(), prepared, att)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if sess.ID() != "acp-1" || sess.ReadySeq() != 1 {
		t.Errorf("session = %q ready at %d, want acp-1 at 1", sess.ID(), sess.ReadySeq())
	}

	var params *agentlinkpb.SessionParams
	select {
	case params = <-link.handle("run-1").spawned:
	default:
		t.Fatal("the agent was never spawned")
	}
	if params.GetCwd() != "/workspace" || params.GetSystemPrompt() != "be terse" || params.GetResumeSessionId() != "acp-0" {
		t.Errorf("session params = %v", params)
	}
	if len(params.GetMcpServers()) != 1 || params.GetMcpServers()[0].GetUrl() != "http://127.0.0.1:9100/mcp" {
		t.Errorf("session MCP servers = %v, want the rewritten loopback URL", params.GetMcpServers())
	}
	if got := params.GetConfig()["model"]; got != "opus" {
		t.Errorf("session config model = %q, want opus", got)
	}

	h := link.handle("run-1")
	_ = sess.Close()
	if h.shutdownReason() != "session_end" {
		t.Errorf("closing the session sent shutdown %q, want session_end", h.shutdownReason())
	}
	if link.live() != 0 {
		t.Errorf("the Agent Link still holds %d expectations", link.live())
	}
}

func TestActivateReportsAFailedSession(t *testing.T) {
	for _, tc := range []struct {
		name              string
		resumeUnavailable bool
		resumed           bool
		wantTeardown      bool
	}{
		{name: "launch", wantTeardown: true},
		{name: "unresumable revive", resumeUnavailable: true, resumed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			link := newFakeAgentLink()
			link.spawnReply = func(h *fakeHandle) {
				h.push(&agentlinkpb.AgentEvent{Seq: 1, Payload: &agentlinkpb.AgentEvent_SessionFailed{
					SessionFailed: &agentlinkpb.SessionFailed{Reason: "nope", ResumeUnavailable: tc.resumeUnavailable},
				}})
			}
			claims := newFakeClaims()
			o := newTestOrchestrator(t, claims, link)
			prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
			if err != nil {
				t.Fatalf("Prepare: %v", err)
			}
			prepared.Resumed = tc.resumed
			att, err := o.Expect("run-1", prepared)
			if err != nil {
				t.Fatalf("Expect: %v", err)
			}
			link.handle("run-1").attach(1, false)
			_, err = o.Activate(context.Background(), prepared, att)
			if err == nil {
				t.Fatal("Activate succeeded although the session failed")
			}
			if errors.Is(err, ErrResumeUnavailable) != tc.resumeUnavailable {
				t.Errorf("err = %v; resume unavailable = %v, want %v", err, errors.Is(err, ErrResumeUnavailable), tc.resumeUnavailable)
			}
			if got := len(claims.deletedNames()) == 1; got != tc.wantTeardown {
				t.Errorf("claim torn down = %v, want %v", got, tc.wantTeardown)
			}
			if link.live() != 0 {
				t.Errorf("the expectation leaked: %d still registered", link.live())
			}
		})
	}
}

func TestSessionCommandsAndAcksReachTheLink(t *testing.T) {
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
	h.attach(1, false)
	sess, err := o.Activate(context.Background(), prepared, att)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	sess.Prompt("t1", 1, "hello")
	sess.Decide("c1", "allow", false)
	sess.Ack(5)
	sent := h.sentFrames()
	if len(sent) != 2 || sent[0].GetPrompt().GetTurnId() != "t1" || sent[0].GetPrompt().GetTurnSeq() != 1 ||
		sent[1].GetPermission().GetRequestId() != "c1" || sent[1].GetPermission().GetOptionId() != "allow" {
		t.Fatalf("frames sent = %v", sent)
	}
	if h.ackedSeq() != 5 {
		t.Errorf("acked = %d, want 5", h.ackedSeq())
	}
}

func TestAdoptExpectsTheRunAgainAtItsResumePoint(t *testing.T) {
	link := newFakeAgentLink()
	o := New(newFakeClaims(), testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachGrace(50*time.Millisecond),
	)
	downs := make(chan string, 4)
	sess, err := o.Adopt(context.Background(), AdoptSpec{
		RunID: "run-9", ClaimName: "claim-9", SandboxName: "sandbox-agent",
		Executor:     agenticrun.Executor{Namespace: "ns", ServiceAccount: "sandbox-agent", PodName: "sandbox-agent", PodUID: "uid-1"},
		Launch:       LaunchSpec{SessionID: "s9", Template: testTemplate()},
		ACPSessionID: "acp-9", StreamID: []byte("stream-9"), ResumeAfter: 42,
	}, WithOnDown(func(cause string) { downs <- cause }))
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	h := link.handle("run-9")
	if h == nil {
		t.Fatal("Adopt did not expect the run")
	}
	if string(h.opts.StreamID) != "stream-9" || h.opts.ResumeAfter != 42 {
		t.Errorf("expect options = stream %q resume %d, want stream-9 and 42", h.opts.StreamID, h.opts.ResumeAfter)
	}
	if sess.ID() != "acp-9" || string(sess.StreamID()) != "stream-9" {
		t.Errorf("adopted session = %q %q", sess.ID(), sess.StreamID())
	}
	select {
	case cause := <-downs:
		if cause != "tunnel_lost" {
			t.Errorf("cause = %q, want tunnel_lost", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("an adopted run that never attached again was never closed")
	}
}

func TestAdoptKeepsASidecarThatAttachesBeforeAdoptReturns(t *testing.T) {
	link := newFakeAgentLink()
	link.onExpect = func(h *fakeHandle) { h.attach(1, true) }
	o := New(newFakeClaims(), testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachGrace(50*time.Millisecond),
	)
	downs := make(chan string, 4)
	if _, err := o.Adopt(context.Background(), AdoptSpec{
		RunID: "run-9", ClaimName: "claim-9", SandboxName: "sandbox-agent",
		Executor: agenticrun.Executor{Namespace: "ns", ServiceAccount: "sandbox-agent", PodName: "sandbox-agent", PodUID: "uid-1"},
		Launch:   LaunchSpec{SessionID: "s9", Template: testTemplate()},
	}, WithOnDown(func(cause string) { downs <- cause })); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	select {
	case cause := <-downs:
		t.Fatalf("an adopted run that attached during Adopt was closed: %s", cause)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestAdoptKeepsTheSessionWhenTheSidecarReturns(t *testing.T) {
	link := newFakeAgentLink()
	o := New(newFakeClaims(), testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachGrace(100*time.Millisecond),
	)
	downs := make(chan string, 4)
	if _, err := o.Adopt(context.Background(), AdoptSpec{
		RunID: "run-9", ClaimName: "claim-9", SandboxName: "sandbox-agent",
		Executor: agenticrun.Executor{Namespace: "ns", ServiceAccount: "sandbox-agent", PodName: "sandbox-agent", PodUID: "uid-1"},
		Launch:   LaunchSpec{SessionID: "s9", Template: testTemplate()},
	}, WithOnDown(func(cause string) { downs <- cause })); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	link.handle("run-9").attach(1, true)
	select {
	case cause := <-downs:
		t.Fatalf("an adopted run that attached again was closed: %s", cause)
	case <-time.After(300 * time.Millisecond):
	}
	link.handle("run-9").opts.OnAttached(2, false)
	select {
	case cause := <-downs:
		if cause != "sidecar_restarted" {
			t.Errorf("cause = %q, want sidecar_restarted", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("an adopted run whose agent is gone was not closed")
	}
}

func TestAdoptRefusesAReplacedPod(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)
	if _, err := o.Adopt(context.Background(), AdoptSpec{
		RunID: "run-9", SandboxName: "sandbox-agent",
		Executor: agenticrun.Executor{Namespace: "ns", ServiceAccount: "sandbox-agent", PodName: "sandbox-agent", PodUID: "uid-OLD"},
		Launch:   LaunchSpec{SessionID: "s9", Template: testTemplate()},
	}); err == nil {
		t.Fatal("Adopt succeeded for a pod that was replaced")
	}
	if link.live() != 0 {
		t.Errorf("a refused adoption left %d expectations", link.live())
	}
}

func TestActivateFailureForgetsAndTearsDown(t *testing.T) {
	link := newFakeAgentLink()
	claims := newFakeClaims()
	o := New(claims, testPods(), nil, link,
		WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
		WithAttachTimeout(50*time.Millisecond),
	)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	att, err := o.Expect("run-1", prepared)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}

	if _, err := o.Activate(context.Background(), prepared, att); err == nil {
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
		name   string
		attach bool
	}{
		{name: "attach timeout"},
		{name: "acp setup error", attach: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			link := newFakeAgentLink()
			link.spawnReply = func(h *fakeHandle) {
				h.push(&agentlinkpb.AgentEvent{Seq: 1, Payload: &agentlinkpb.AgentEvent_SessionFailed{
					SessionFailed: &agentlinkpb.SessionFailed{Reason: "initialize failed"},
				}})
			}
			claims := newFakeClaims()
			o := New(claims, testPods(), nil, link,
				WithNamespace("ns"), WithHarnessRoute("https://harness.example.com"),
				WithAttachTimeout(50*time.Millisecond),
			)

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

			if _, err := o.Activate(context.Background(), prepared, att); err == nil {
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

	h.opts.OnError("envoy_exited", errors.New("boom"))
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
	if _, err := o.Activate(context.Background(), prepared, att); err != nil {
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
	if _, err := o.Activate(context.Background(), prepared, att); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	h.opts.OnError("envoy_exited", errors.New("boom"))
	select {
	case cause := <-downs:
		if cause != "envoy_exited" {
			t.Errorf("cause = %q, want envoy_exited", cause)
		}
	default:
		t.Fatal("the sidecar error was never reported")
	}
}

type cancelOnCreate struct {
	*fakeClaims
	cancel context.CancelFunc
}

func (f *cancelOnCreate) Create(ctx context.Context, claim *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error) {
	defer f.cancel()
	return f.fakeClaims.Create(ctx, claim)
}

func (f *cancelOnCreate) Get(ctx context.Context, name string) (*sbxv1.SandboxClaim, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.fakeClaims.Get(ctx, name)
}

func (f *cancelOnCreate) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.fakeClaims.Delete(ctx, name)
}

func TestCancelledPrepareStillDeletesTheClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claims := &cancelOnCreate{fakeClaims: newFakeClaims(), cancel: cancel}
	o := newTestOrchestrator(t, claims, newFakeAgentLink())

	if _, err := o.Prepare(ctx, LaunchSpec{SessionID: "s1", Template: testTemplate()}); err == nil {
		t.Fatal("Prepare succeeded although its context was cancelled")
	}
	if names := claims.deletedNames(); len(names) != 1 {
		t.Errorf("a cancelled prepare leaked its claim: deleted %v", names)
	}
}

type ctxClaims struct{ *fakeClaims }

func (f ctxClaims) Get(ctx context.Context, name string) (*sbxv1.SandboxClaim, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.fakeClaims.Get(ctx, name)
}

type cancelOnResume struct {
	cancel context.CancelFunc

	mu   sync.Mutex
	mode string
}

func (f *cancelOnResume) Get(context.Context, string) (*agentsv1.Sandbox, error) { return nil, nil }

func (f *cancelOnResume) Patch(ctx context.Context, _ string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var patch struct {
		Spec struct {
			OperatingMode string `json:"operatingMode"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return err
	}
	f.mu.Lock()
	f.mode = patch.Spec.OperatingMode
	f.mu.Unlock()
	if patch.Spec.OperatingMode == string(agentsv1.SandboxOperatingModeRunning) {
		f.cancel()
	}
	return nil
}

func (f *cancelOnResume) operatingMode() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mode
}

func TestCancelledReviveRestoresSuspension(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claims := newFakeClaims()
	spec := LaunchSpec{SessionID: "s1", Template: testTemplate()}
	claim, err := claims.Create(context.Background(), BuildSandboxClaim("ns", spec))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sandboxes := &cancelOnResume{cancel: cancel, mode: string(agentsv1.SandboxOperatingModeSuspended)}
	o := New(ctxClaims{claims}, testPods(), sandboxes, newFakeAgentLink(), WithNamespace("ns"))

	if _, err := o.Revive(ctx, claim.Name, spec); err == nil {
		t.Fatal("Revive succeeded although its context was cancelled")
	}
	if got := sandboxes.operatingMode(); got != string(agentsv1.SandboxOperatingModeSuspended) {
		t.Errorf("a cancelled revive left the sandbox %s", got)
	}
}

func TestSupervisionAcrossReattaches(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after func(h *fakeHandle)
		want  []string
	}{
		{
			name: "agent io re-attach keeps the session",
			after: func(h *fakeHandle) {
				h.opts.OnLost(errors.New("agent io lost"))
				h.opts.OnAttached(2, true)
			},
		},
		{
			name: "an error then a re-attach reports the error once",
			after: func(h *fakeHandle) {
				h.opts.OnError("envoy_exited", errors.New("boom"))
				h.opts.OnAttached(2, false)
			},
			want: []string{"envoy_exited"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			link := newFakeAgentLink()
			o := newTestOrchestrator(t, newFakeClaims(), link)
			o.cfg.attachGrace = 20 * time.Millisecond

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
			h.attach(1, false)
			if _, err := o.Activate(context.Background(), prepared, att); err != nil {
				t.Fatalf("Activate: %v", err)
			}

			tc.after(h)
			time.Sleep(200 * time.Millisecond)
			close(downs)
			var got []string
			for cause := range downs {
				got = append(got, cause)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("downs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRevivedAttachmentStartsUnspawned(t *testing.T) {
	link := newFakeAgentLink()
	o := newTestOrchestrator(t, newFakeClaims(), link)

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	oldDowns := make(chan string, 4)
	old, err := o.Expect("run-1", prepared, WithOnDown(func(cause string) { oldDowns <- cause }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	oldHandle := link.handle("run-1")
	oldHandle.attach(1, false)
	if _, err := o.Activate(context.Background(), prepared, old); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	old.Forget()

	prepared.Resumed = true
	downs := make(chan string, 4)
	att, err := o.Expect("run-2", prepared, WithOnDown(func(cause string) { downs <- cause }))
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	h := link.handle("run-2")
	h.attach(1, false)
	oldHandle.opts.OnAttached(2, false)
	select {
	case cause := <-downs:
		t.Fatalf("the revived attachment reported down before its agent ran: %s", cause)
	case cause := <-oldDowns:
		t.Fatalf("the forgotten attachment reported down: %s", cause)
	default:
	}
	if _, err := o.Activate(context.Background(), prepared, att); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	h.opts.OnError("envoy_exited", errors.New("boom"))
	select {
	case cause := <-downs:
		if cause != "envoy_exited" {
			t.Errorf("cause = %q, want envoy_exited", cause)
		}
	default:
		t.Fatal("the revived sandbox's error was never reported")
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
