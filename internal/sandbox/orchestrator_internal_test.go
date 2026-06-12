package sandbox

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	sidecarpb "github.com/pomerium/agentops/internal/sidecar/pb"
	"github.com/pomerium/agentops/internal/sidecar/stdiorpc"
	"github.com/pomerium/agentops/internal/telemetry"
)

// fakeClaims is an in-memory ClaimClient whose claims become ready instantly.
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

// execCall records one OpenAgent invocation.
type execCall struct {
	container string
	command   []string
}

// fakeExecutor hands out pre-wired AgentConns per container name.
type fakeExecutor struct {
	mu    sync.Mutex
	calls []execCall
	conns map[string]*AgentConn // by container
}

func (f *fakeExecutor) OpenAgent(_ context.Context, _, _, container string, command []string) (*AgentConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, execCall{container: container, command: command})
	conn, ok := f.conns[container]
	if !ok {
		return nil, errors.New("no conn for container " + container)
	}
	return conn, nil
}

// fakeSidecarService runs a SidecarControlService over pipes and records the
// Configure it receives. answer controls the status it replies with. If
// postReady is non-nil, a status pushed onto it after the READY handshake is
// sent on the stream — modelling envoy dying mid-session.
type fakeSidecarService struct {
	sidecarpb.UnimplementedSidecarControlServiceServer
	answer     sidecarpb.Status_State
	postReady  chan *sidecarpb.Status
	mu         sync.Mutex
	configured *sidecarpb.Configure
}

func (f *fakeSidecarService) Control(stream grpc.BidiStreamingServer[sidecarpb.ControlRequest, sidecarpb.ControlResponse]) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.configured = req.GetConfigure()
	f.mu.Unlock()
	if err := stream.Send(&sidecarpb.ControlResponse{
		Msg: &sidecarpb.ControlResponse_Status{Status: &sidecarpb.Status{State: f.answer}},
	}); err != nil {
		return err
	}
	// Stay on the stream like the real sidecar, optionally emitting a
	// post-READY status (e.g. STATE_ERROR when envoy dies).
	recvErr := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				recvErr <- err
				return
			}
		}
	}()
	for {
		select {
		case <-recvErr:
			return nil
		case st := <-f.postReady:
			if err := stream.Send(&sidecarpb.ControlResponse{
				Msg: &sidecarpb.ControlResponse_Status{Status: st},
			}); err != nil {
				return err
			}
		}
	}
}

// sidecarConn builds the client-side AgentConn for a fake sidecar service.
func sidecarConn(t *testing.T, svc *fakeSidecarService) *AgentConn {
	t.Helper()
	clientToServerR, clientToServerW := io.Pipe()
	serverToClientR, serverToClientW := io.Pipe()

	gsrv := grpc.NewServer()
	sidecarpb.RegisterSidecarControlServiceServer(gsrv, svc)
	go func() {
		_ = gsrv.Serve(stdiorpc.ListenOnce(stdiorpc.NewConn(clientToServerR, serverToClientW, nil)))
	}()
	t.Cleanup(gsrv.Stop)

	return &AgentConn{Stdin: clientToServerW, Stdout: serverToClientR}
}

func TestConfigDefaultsContainerNames(t *testing.T) {
	// Multi-container pods reject exec without an explicit container name
	// ("a container name must be specified"), so both containers must have
	// non-empty defaults matching the SandboxTemplate.
	o := New(newFakeClaims(), &fakeExecutor{}, Config{Namespace: "ns"}, nil)
	if o.cfg.AgentContainer != "agent" {
		t.Errorf("default AgentContainer = %q, want agent", o.cfg.AgentContainer)
	}
	if o.cfg.SidecarContainer != "sidecar" {
		t.Errorf("default SidecarContainer = %q, want sidecar", o.cfg.SidecarContainer)
	}
}

func testTemplate() *v1alpha1.AgentTemplate {
	return &v1alpha1.AgentTemplate{
		Spec: v1alpha1.AgentTemplateSpec{
			SandboxTemplateRef: v1alpha1.SandboxTemplateReference{Name: "claude-code"},
			SessionConfig:      map[string]string{"model": "opus"},
		},
	}
}

func TestLaunchConfiguresSidecarThenAgent(t *testing.T) {
	svc := &fakeSidecarService{answer: sidecarpb.Status_STATE_READY}
	exec := &fakeExecutor{conns: map[string]*AgentConn{
		"sidecar": sidecarConn(t, svc),
		"agent":   {Stdin: nopWriteCloser{}, Stdout: &blockingReader{}},
	}}
	claims := newFakeClaims()

	var gotSession SessionParams
	o := New(claims, exec, Config{
		Namespace:        "ns",
		AgentContainer:   "agent",
		SidecarContainer: "sidecar",
	}, nil)
	o.openSession = func(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, closeFn func() error, params SessionParams) (*Session, error) {
		gotSession = params
		return &Session{close: closeFn}, nil
	}

	endpoints := []ProxiedEndpoint{{
		Name:        "agno",
		ListenPort:  9100,
		UpstreamURL: "https://docs.agno.com/mcp",
		Headers:     map[string]string{"authorization": "Bearer tok"},
	}}
	sess, _, err := o.Launch(context.Background(), nil, LaunchSpec{
		SessionID: "s1", Template: testTemplate(),
	}, endpoints)
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	defer func() { _ = sess.Close() }()

	// Exec order: sidecar first, then agent.
	if len(exec.calls) != 2 {
		t.Fatalf("got %d exec calls, want 2: %+v", len(exec.calls), exec.calls)
	}
	if exec.calls[0].container != "sidecar" {
		t.Errorf("first exec container = %q, want sidecar", exec.calls[0].container)
	}
	if exec.calls[1].container != "agent" {
		t.Errorf("second exec container = %q, want agent", exec.calls[1].container)
	}

	// The sidecar received the endpoint with its secret header.
	svc.mu.Lock()
	cfg := svc.configured
	svc.mu.Unlock()
	if cfg == nil || len(cfg.Endpoints) != 1 {
		t.Fatalf("sidecar Configure = %v, want 1 endpoint", cfg)
	}
	ep := cfg.Endpoints[0]
	// The sidecar registers the endpoint under its namespaced name (so an
	// agent-template MCP server can't collide with a SandboxTemplate env
	// endpoint); the agent still reaches it by port under the bare name.
	if ep.Name != SidecarEndpointName("agno") || ep.ListenPort != 9100 || ep.UpstreamUrl != "https://docs.agno.com/mcp" {
		t.Errorf("unexpected endpoint: %+v", ep)
	}
	if len(ep.Headers) != 1 || ep.Headers[0].Name != "authorization" || ep.Headers[0].Value != "Bearer tok" {
		t.Errorf("unexpected endpoint headers: %+v", ep.Headers)
	}

	// The ACP session got the rewritten localhost URL and no credentials.
	if len(gotSession.MCPServers) != 1 {
		t.Fatalf("session MCP servers = %+v, want 1", gotSession.MCPServers)
	}
	http := gotSession.MCPServers[0].Http
	if http == nil || http.Url != "http://127.0.0.1:9100/mcp" {
		t.Errorf("session MCP server = %+v, want rewritten localhost URL", gotSession.MCPServers[0])
	}
	if len(http.Headers) != 0 {
		t.Errorf("session MCP server carries headers: %+v", http.Headers)
	}

	// The template's session config rides into the ACP session unchanged.
	if got := gotSession.SessionConfig["model"]; got != "opus" {
		t.Errorf("session config model = %q, want opus (full config: %v)", got, gotSession.SessionConfig)
	}
}

func TestLaunchFailsAndTearsDownWhenSidecarErrors(t *testing.T) {
	svc := &fakeSidecarService{answer: sidecarpb.Status_STATE_ERROR}
	exec := &fakeExecutor{conns: map[string]*AgentConn{
		"sidecar": sidecarConn(t, svc),
	}}
	claims := newFakeClaims()

	o := New(claims, exec, Config{
		Namespace:        "ns",
		AgentContainer:   "agent",
		SidecarContainer: "sidecar",
	}, nil)
	o.openSession = func(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, _ func() error, _ SessionParams) (*Session, error) {
		t.Error("openSession must not be called when the sidecar fails")
		return nil, errors.New("unreachable")
	}

	_, _, err := o.Launch(context.Background(), nil, LaunchSpec{
		SessionID: "s1", Template: testTemplate(),
	}, []ProxiedEndpoint{{Name: "agno", ListenPort: 9100, UpstreamURL: "https://x"}})
	if err == nil {
		t.Fatal("Launch succeeded although the sidecar reported ERROR")
	}
	// Only the sidecar exec may have happened; never the agent's.
	for _, c := range exec.calls {
		if c.container == "agent" {
			t.Error("agent was exec'd although the sidecar failed")
		}
	}
	if len(claims.deleted) != 1 {
		t.Errorf("claim not torn down after sidecar failure: %v", claims.deleted)
	}
}

// Finding #4: after the READY handshake nothing read the control stream, so a
// STATE_ERROR the sidecar sends when envoy dies mid-session was never received
// and the session lingered until its TTL. The orchestrator must supervise the
// stream and close the session when the sidecar reports a terminal error.
func TestLaunchSupervisesSidecarAfterReady(t *testing.T) {
	svc := &fakeSidecarService{
		answer:    sidecarpb.Status_STATE_READY,
		postReady: make(chan *sidecarpb.Status, 1),
	}
	exec := &fakeExecutor{conns: map[string]*AgentConn{
		"sidecar": sidecarConn(t, svc),
		"agent":   {Stdin: nopWriteCloser{}, Stdout: &blockingReader{}},
	}}
	o := New(newFakeClaims(), exec, Config{
		Namespace:        "ns",
		AgentContainer:   "agent",
		SidecarContainer: "sidecar",
	}, nil)

	closedCh := make(chan struct{})
	var once sync.Once
	o.openSession = func(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, closeFn func() error, _ SessionParams) (*Session, error) {
		return &Session{close: func() error {
			var err error
			once.Do(func() { close(closedCh); err = closeFn() })
			return err
		}}, nil
	}

	sess, _, err := o.Launch(context.Background(), nil, LaunchSpec{
		SessionID: "s1", Template: testTemplate(),
	}, []ProxiedEndpoint{{Name: "agno", ListenPort: 9100, UpstreamURL: "https://x"}})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	defer func() { _ = sess.Close() }()

	// Envoy dies mid-session: the sidecar emits a terminal STATE_ERROR.
	svc.postReady <- &sidecarpb.Status{State: sidecarpb.Status_STATE_ERROR, Message: "proxy exited"}

	select {
	case <-closedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("session not closed after the sidecar reported STATE_ERROR post-READY")
	}
}

func TestLaunchSidecarReadyTimeout(t *testing.T) {
	// A sidecar that never answers: the conn is wired to pipes nobody serves.
	clientToServerR, clientToServerW := io.Pipe()
	serverToClientR, _ := io.Pipe()
	t.Cleanup(func() { _ = clientToServerR.Close() })

	exec := &fakeExecutor{conns: map[string]*AgentConn{
		"sidecar": {Stdin: clientToServerW, Stdout: serverToClientR},
	}}
	claims := newFakeClaims()

	o := New(claims, exec, Config{
		Namespace:           "ns",
		AgentContainer:      "agent",
		SidecarContainer:    "sidecar",
		SidecarReadyTimeout: 200 * time.Millisecond,
	}, nil)

	_, _, err := o.Launch(context.Background(), nil, LaunchSpec{
		SessionID: "s1", Template: testTemplate(),
	}, []ProxiedEndpoint{{Name: "agno", ListenPort: 9100, UpstreamURL: "https://x"}})
	if err == nil {
		t.Fatal("Launch succeeded although the sidecar never became ready")
	}
	if len(claims.deleted) != 1 {
		t.Errorf("claim not torn down after sidecar timeout: %v", claims.deleted)
	}
}

func TestLaunchSkipsSidecarWithoutEndpoints(t *testing.T) {
	// No proxied endpoints (no MCP servers): the orchestrator still execs the
	// sidecar? No — nothing to proxy via gRPC; env-defined endpoints (LLM key)
	// still need the sidecar. So the sidecar is always configured, possibly
	// with zero endpoints.
	svc := &fakeSidecarService{answer: sidecarpb.Status_STATE_READY}
	exec := &fakeExecutor{conns: map[string]*AgentConn{
		"sidecar": sidecarConn(t, svc),
		"agent":   {Stdin: nopWriteCloser{}, Stdout: &blockingReader{}},
	}}
	o := New(newFakeClaims(), exec, Config{
		Namespace:        "ns",
		AgentContainer:   "agent",
		SidecarContainer: "sidecar",
	}, nil)
	o.openSession = func(_ context.Context, _ *telemetry.Component, _ EventSink, _ io.Writer, _ io.Reader, closeFn func() error, _ SessionParams) (*Session, error) {
		return &Session{close: closeFn}, nil
	}

	sess, _, err := o.Launch(context.Background(), nil, LaunchSpec{
		SessionID: "s1", Template: testTemplate(),
	}, nil)
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if len(exec.calls) != 2 || exec.calls[0].container != "sidecar" {
		t.Errorf("expected sidecar+agent execs, got %+v", exec.calls)
	}
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }

// blockingReader blocks forever (like an idle agent stdout).
type blockingReader struct {
	once sync.Once
	ch   chan struct{}
}

func (b *blockingReader) Read([]byte) (int, error) {
	b.once.Do(func() { b.ch = make(chan struct{}) })
	<-b.ch
	return 0, io.EOF
}
