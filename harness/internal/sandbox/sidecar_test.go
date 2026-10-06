package sandbox_test

import (
	"strings"
	"testing"

	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func TestRewriteMCPServers(t *testing.T) {
	servers := sandbox.RewriteMCPServers([]sandbox.ProxiedEndpoint{
		{
			Name:        "agno",
			ListenPort:  9100,
			UpstreamURL: "https://docs.agno.com/mcp",
		},
		{
			Name:        "rooty",
			ListenPort:  9101,
			UpstreamURL: "https://mcp.example.com",
		},
		{
			Name:        "querty",
			ListenPort:  9102,
			UpstreamURL: "https://mcp.example.com/base/path?tenant=t1",
		},
	})

	if len(servers) != 3 {
		t.Fatalf("got %d servers, want 3", len(servers))
	}

	wantURLs := map[string]string{
		"agno":   "http://127.0.0.1:9100/mcp",
		"rooty":  "http://127.0.0.1:9101",
		"querty": "http://127.0.0.1:9102/base/path?tenant=t1",
	}
	for _, s := range servers {
		if s.Http == nil {
			t.Fatalf("server %+v is not HTTP", s)
		}
		want, ok := wantURLs[s.Http.Name]
		if !ok {
			t.Fatalf("unexpected server name %q", s.Http.Name)
		}
		if s.Http.Url != want {
			t.Errorf("server %s url = %q, want %q", s.Http.Name, s.Http.Url, want)
		}
		if s.Http.Headers == nil {
			t.Errorf("server %s headers are nil; want empty slice", s.Http.Name)
		}
		if len(s.Http.Headers) != 0 {
			t.Errorf("server %s carries %d headers into the agent: %v", s.Http.Name, len(s.Http.Headers), s.Http.Headers)
		}
	}
}

func TestSidecarEndpointNameIsNamespaced(t *testing.T) {
	ep := sandbox.ProxiedEndpoint{Name: "anthropic", ListenPort: 9100, UpstreamURL: "https://x"}

	got := sandbox.SidecarEndpointName(ep.Name)
	if got == "anthropic" {
		t.Errorf("sidecar endpoint name = %q; must not collide with a same-named env endpoint", got)
	}
	if !strings.Contains(got, "-") {
		t.Errorf("sidecar endpoint name = %q; want a dash-namespaced form", got)
	}

	servers := sandbox.RewriteMCPServers([]sandbox.ProxiedEndpoint{ep})
	if servers[0].Http.Name != "anthropic" {
		t.Errorf("agent-facing MCP server name = %q, want anthropic (unchanged)", servers[0].Http.Name)
	}
}

func TestMCPPortAllocationIsDeterministic(t *testing.T) {
	if got := sandbox.MCPListenPort(0); got != 9100 {
		t.Errorf("MCPListenPort(0) = %d, want 9100", got)
	}
	if got := sandbox.MCPListenPort(3); got != 9103 {
		t.Errorf("MCPListenPort(3) = %d, want 9103", got)
	}
}
