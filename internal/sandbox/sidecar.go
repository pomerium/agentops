package sandbox

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync/atomic"
	"time"

	acp "github.com/coder/acp-go-sdk"

	sidecarpb "github.com/pomerium/agentops/internal/sidecar/pb"
	"github.com/pomerium/agentops/internal/sidecar/stdiorpc"
	"github.com/pomerium/agentops/internal/telemetry"
)

// MCPPortBase is the first sidecar listener port assigned to MCP servers; the
// i-th required MCP server listens on MCPPortBase+i. Ports 9900-9999 are
// reserved for endpoints defined in the SandboxTemplate via SIDECAR_HTTP_*
// env vars (e.g. the LLM API), and 9901 is the sidecar's envoy admin port.
const MCPPortBase uint32 = 9100

// MCPListenPort returns the sidecar listener port for the i-th MCP server.
func MCPListenPort(i int) uint32 { return MCPPortBase + uint32(i) }

// mcpEndpointPrefix namespaces agent-template MCP endpoints inside the
// sidecar's flat endpoint namespace. SandboxTemplate env endpoints
// (SIDECAR_HTTP_<NAME>) are lowercased and can never contain a dash, so a
// dash-bearing prefix guarantees an agent-template MCP server (e.g. one named
// "anthropic") never collides with an env-defined endpoint of the same name.
// The agent never sees this name — it reaches each server by its loopback
// port — so the agent-facing ACP server name stays exactly what the agent
// template declared.
const mcpEndpointPrefix = "mcp-"

// SidecarEndpointName returns the namespaced name an agent-template MCP
// endpoint is registered under in the sidecar, distinct from any env-defined
// endpoint.
func SidecarEndpointName(name string) string { return mcpEndpointPrefix + name }

// ProxiedEndpoint is one localhost→upstream route the sandbox sidecar serves
// for the agent. Headers hold the secret credentials envoy injects on the way
// to the upstream — they must never be logged and never reach the agent.
type ProxiedEndpoint struct {
	Name        string
	ListenPort  uint32
	UpstreamURL string
	// DialAddress optionally overrides the host[:port] the sidecar's envoy dials,
	// while SNI and the Host header stay derived from UpstreamURL. Empty dials the
	// host:port parsed from UpstreamURL.
	DialAddress string
	Headers     map[string]string
}

// RewriteMCPServers maps proxied endpoints to the credential-free ACP MCP
// server configs the agent receives: the upstream URL's path and query on the
// sidecar's loopback listener, with an empty (never nil — nil marshals to
// JSON null, which harnesses reject) header list.
func RewriteMCPServers(endpoints []ProxiedEndpoint) []acp.McpServer {
	servers := make([]acp.McpServer, 0, len(endpoints))
	for _, ep := range endpoints {
		localURL := fmt.Sprintf("http://127.0.0.1:%d", ep.ListenPort)
		if u, err := url.Parse(ep.UpstreamURL); err == nil {
			localURL += u.RequestURI()
			if u.Path == "" && u.RawQuery == "" {
				localURL = fmt.Sprintf("http://127.0.0.1:%d", ep.ListenPort)
			}
		}
		servers = append(servers, acp.McpServer{
			Http: &acp.McpServerHttpInline{
				Name:    ep.Name,
				Type:    "http",
				Url:     localURL,
				Headers: []acp.HttpHeader{},
			},
		})
	}
	return servers
}

// sidecarControl is a live control stream to the sidecar after a successful
// READY handshake. Closing it ends the stream, which makes the sidecar stop
// envoy and exit.
type sidecarControl struct {
	stream  sidecarpb.SidecarControlService_ControlClient
	cancel  context.CancelFunc
	cc      io.Closer
	closing atomic.Bool
}

// Close ends the control stream. It is safe to call more than once.
func (s *sidecarControl) Close() error {
	s.closing.Store(true)
	s.cancel()
	return s.cc.Close()
}

// Supervise reads the control stream until it ends. If the sidecar reports a
// terminal STATE_ERROR (envoy died) or the stream fails for any reason other
// than our own Close, it invokes onDown exactly once. Without this, envoy
// dying mid-session would go unnoticed: the agent's loopback calls would fail
// while the session lingered until its TTL.
func (s *sidecarControl) Supervise(ctx context.Context, tel *telemetry.Component, onDown func(error)) {
	go func() {
		for {
			resp, err := s.stream.Recv()
			if err != nil {
				if !s.closing.Load() {
					onDown(fmt.Errorf("sidecar control stream ended: %w", err))
				}
				return
			}
			if status := resp.GetStatus(); status != nil && status.State == sidecarpb.Status_STATE_ERROR {
				tel.Error(ctx, "sidecar reported failure mid-session", "message", status.Message)
				onDown(fmt.Errorf("sidecar reported error: %s", status.Message))
				return
			}
		}
	}()
}

// configureSidecar speaks SidecarControlService over the exec'd sidecar
// process's stdio: it sends the endpoint configuration and waits (bounded by
// timeout) for the sidecar to report READY, meaning every listener is live.
// On success it returns a live control handle; the caller must Supervise it for
// the session's lifetime and Close it when the session ends.
func configureSidecar(ctx context.Context, tel *telemetry.Component, conn *AgentConn, endpoints []ProxiedEndpoint, timeout time.Duration) (*sidecarControl, error) {
	cc, err := stdiorpc.Dial(stdiorpc.NewConn(conn.Stdout, conn.Stdin, conn.Close))
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dial sidecar: %w", err)
	}

	// The control stream must stay open for the whole session (the sidecar
	// kills envoy when it closes), so it runs under its own cancelable
	// context independent of the launch ctx.
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	closeAll := func() error {
		cancel()
		return cc.Close()
	}

	// The whole handshake — opening the stream, sending the config, awaiting
	// READY — runs in a goroutine so timeout covers it all; the cancel aborts
	// whichever step is in flight.
	type result struct {
		stream sidecarpb.SidecarControlService_ControlClient
		err    error
	}
	handshake := make(chan result, 1)
	go func() {
		stream, err := sidecarpb.NewSidecarControlServiceClient(cc).Control(streamCtx)
		if err != nil {
			handshake <- result{err: fmt.Errorf("open sidecar control stream: %w", err)}
			return
		}

		pbEndpoints := make([]*sidecarpb.HttpEndpoint, 0, len(endpoints))
		for _, ep := range endpoints {
			pbEp := &sidecarpb.HttpEndpoint{
				Name:        SidecarEndpointName(ep.Name),
				ListenPort:  ep.ListenPort,
				UpstreamUrl: ep.UpstreamURL,
				DialAddress: ep.DialAddress,
			}
			for name, value := range ep.Headers {
				pbEp.Headers = append(pbEp.Headers, &sidecarpb.Header{Name: name, Value: value})
			}
			pbEndpoints = append(pbEndpoints, pbEp)
		}
		err = stream.Send(&sidecarpb.ControlRequest{
			Msg: &sidecarpb.ControlRequest_Configure{Configure: &sidecarpb.Configure{Endpoints: pbEndpoints}},
		})
		if err != nil {
			handshake <- result{err: fmt.Errorf("send sidecar configure: %w", err)}
			return
		}

		resp, err := stream.Recv()
		if err != nil {
			handshake <- result{err: fmt.Errorf("sidecar control stream: %w", err)}
			return
		}
		status := resp.GetStatus()
		switch {
		case status == nil:
			handshake <- result{err: fmt.Errorf("unexpected sidecar response: %v", resp)}
		case status.State != sidecarpb.Status_STATE_READY:
			handshake <- result{err: fmt.Errorf("sidecar reported %s: %s", status.State, status.Message)}
		default:
			tel.Debug(ctx, "sidecar ready", "endpoints", len(status.Endpoints))
			handshake <- result{stream: stream}
		}
	}()

	select {
	case r := <-handshake:
		if r.err != nil {
			_ = closeAll()
			return nil, r.err
		}
		return &sidecarControl{stream: r.stream, cancel: cancel, cc: cc}, nil
	case <-time.After(timeout):
		_ = closeAll()
		return nil, fmt.Errorf("sidecar not ready within %s", timeout)
	case <-ctx.Done():
		_ = closeAll()
		return nil, ctx.Err()
	}
}
