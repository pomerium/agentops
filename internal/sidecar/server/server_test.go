package server_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/pomerium/agentops/internal/sidecar/envoyconfig"
	"github.com/pomerium/agentops/internal/sidecar/envparse"
	sidecarpb "github.com/pomerium/agentops/internal/sidecar/pb"
	"github.com/pomerium/agentops/internal/sidecar/server"
	"github.com/pomerium/agentops/internal/sidecar/stdiorpc"
)

// fakeProxy records Start/Stop calls and lets tests control envoy's lifetime.
type fakeProxy struct {
	mu       sync.Mutex
	started  [][]envoyconfig.Endpoint
	startErr error
	exited   chan error
	stopped  chan struct{}
}

func newFakeProxy() *fakeProxy {
	return &fakeProxy{exited: make(chan error, 1), stopped: make(chan struct{})}
}

func (f *fakeProxy) Start(_ context.Context, eps []envoyconfig.Endpoint) (server.Process, error) {
	f.mu.Lock()
	f.started = append(f.started, eps)
	f.mu.Unlock()
	if f.startErr != nil {
		return nil, f.startErr
	}
	return f, nil
}

func (f *fakeProxy) Exited() <-chan error { return f.exited }

func (f *fakeProxy) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.stopped:
	default:
		close(f.stopped)
	}
}

func (f *fakeProxy) startCalls() [][]envoyconfig.Endpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started
}

// startControl serves a server.Server over in-memory pipes and returns an open
// Control stream plus a cleanup func.
func startControl(t *testing.T, srv *server.Server) sidecarpb.SidecarControlService_ControlClient {
	t.Helper()
	clientToServerR, clientToServerW := io.Pipe()
	serverToClientR, serverToClientW := io.Pipe()
	var clientConn net.Conn = stdiorpc.NewConn(serverToClientR, clientToServerW, nil)
	serverConn := stdiorpc.NewConn(clientToServerR, serverToClientW, nil)

	gsrv := grpc.NewServer()
	sidecarpb.RegisterSidecarControlServiceServer(gsrv, srv)
	go func() { _ = gsrv.Serve(stdiorpc.ListenOnce(serverConn)) }()
	t.Cleanup(gsrv.Stop)

	cc, err := stdiorpc.Dial(clientConn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := sidecarpb.NewSidecarControlServiceClient(cc).Control(ctx)
	if err != nil {
		t.Fatalf("Control: %v", err)
	}
	return stream
}

func sendConfigure(t *testing.T, stream sidecarpb.SidecarControlService_ControlClient, eps ...*sidecarpb.HttpEndpoint) {
	t.Helper()
	err := stream.Send(&sidecarpb.ControlRequest{
		Msg: &sidecarpb.ControlRequest_Configure{Configure: &sidecarpb.Configure{Endpoints: eps}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func recvStatus(t *testing.T, stream sidecarpb.SidecarControlService_ControlClient) *sidecarpb.Status {
	t.Helper()
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if resp.GetStatus() == nil {
		t.Fatalf("expected status, got %v", resp)
	}
	return resp.GetStatus()
}

func TestConfigureMergesEnvAndStreamEndpoints(t *testing.T) {
	t.Parallel()
	proxy := newFakeProxy()
	srv := server.New(server.Config{
		Proxy: proxy,
		EnvEndpoints: []envparse.Endpoint{{
			Name: "anthropic", ListenPort: 9999, UpstreamURL: "https://api.anthropic.com",
			Headers: map[string]string{"x-api-key": "sk"},
		}},
	})
	stream := startControl(t, srv)

	sendConfigure(t, stream, &sidecarpb.HttpEndpoint{
		Name: "mcp-0", ListenPort: 9100, UpstreamUrl: "https://mcp.example.com",
		Headers: []*sidecarpb.Header{{Name: "authorization", Value: "Bearer tok"}},
	})

	status := recvStatus(t, stream)
	if status.State != sidecarpb.Status_STATE_READY {
		t.Fatalf("state = %v (%s), want READY", status.State, status.Message)
	}
	if len(status.Endpoints) != 2 {
		t.Fatalf("status endpoints = %d, want 2", len(status.Endpoints))
	}
	byName := map[string]*sidecarpb.EndpointStatus{}
	for _, es := range status.Endpoints {
		byName[es.Name] = es
	}
	if es := byName["anthropic"]; es == nil || !es.EnvDefined || es.ListenPort != 9999 {
		t.Errorf("anthropic status = %v, want env_defined on port 9999", es)
	}
	if es := byName["mcp-0"]; es == nil || es.EnvDefined || es.ListenPort != 9100 {
		t.Errorf("mcp-0 status = %v, want stream-defined on port 9100", es)
	}

	calls := proxy.startCalls()
	if len(calls) != 1 || len(calls[0]) != 2 {
		t.Fatalf("proxy started %d times with %v", len(calls), calls)
	}
	var gotAuth string
	for _, ep := range calls[0] {
		if ep.Name == "mcp-0" {
			gotAuth = ep.Headers["authorization"]
		}
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("mcp-0 authorization header = %q, want Bearer tok", gotAuth)
	}
}

func TestConfigureCollisionReportsError(t *testing.T) {
	t.Parallel()
	for name, ep := range map[string]*sidecarpb.HttpEndpoint{
		"name collision": {Name: "anthropic", ListenPort: 9100, UpstreamUrl: "https://x"},
		"port collision": {Name: "other", ListenPort: 9999, UpstreamUrl: "https://x"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			proxy := newFakeProxy()
			srv := server.New(server.Config{
				Proxy: proxy,
				EnvEndpoints: []envparse.Endpoint{{
					Name: "anthropic", ListenPort: 9999, UpstreamURL: "https://api.anthropic.com",
				}},
			})
			stream := startControl(t, srv)
			sendConfigure(t, stream, ep)

			status := recvStatus(t, stream)
			if status.State != sidecarpb.Status_STATE_ERROR {
				t.Fatalf("state = %v, want ERROR", status.State)
			}
			if len(proxy.startCalls()) != 0 {
				t.Error("proxy must not start on collision")
			}
		})
	}
}

func TestProxyStartFailureReportsError(t *testing.T) {
	t.Parallel()
	proxy := newFakeProxy()
	proxy.startErr = errors.New("envoy refused to start")
	srv := server.New(server.Config{Proxy: proxy})
	stream := startControl(t, srv)
	sendConfigure(t, stream, &sidecarpb.HttpEndpoint{Name: "a", ListenPort: 9100, UpstreamUrl: "https://x"})

	status := recvStatus(t, stream)
	if status.State != sidecarpb.Status_STATE_ERROR {
		t.Fatalf("state = %v, want ERROR", status.State)
	}
}

func TestProxyExitReportsError(t *testing.T) {
	t.Parallel()
	proxy := newFakeProxy()
	srv := server.New(server.Config{Proxy: proxy})
	stream := startControl(t, srv)
	sendConfigure(t, stream, &sidecarpb.HttpEndpoint{Name: "a", ListenPort: 9100, UpstreamUrl: "https://x"})

	if got := recvStatus(t, stream).State; got != sidecarpb.Status_STATE_READY {
		t.Fatalf("state = %v, want READY", got)
	}

	proxy.exited <- errors.New("envoy crashed")

	status := recvStatus(t, stream)
	if status.State != sidecarpb.Status_STATE_ERROR {
		t.Fatalf("state = %v, want ERROR after envoy exit", status.State)
	}
}

func TestStreamCloseStopsProxy(t *testing.T) {
	t.Parallel()
	proxy := newFakeProxy()
	srv := server.New(server.Config{Proxy: proxy})
	stream := startControl(t, srv)
	sendConfigure(t, stream, &sidecarpb.HttpEndpoint{Name: "a", ListenPort: 9100, UpstreamUrl: "https://x"})

	if got := recvStatus(t, stream).State; got != sidecarpb.Status_STATE_READY {
		t.Fatalf("state = %v, want READY", got)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	select {
	case <-proxy.stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("proxy was not stopped after the control stream closed")
	}

	// Done lets the sidecar main exit once the control session is over.
	select {
	case <-srv.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("server Done did not fire after the control stream closed")
	}
}
