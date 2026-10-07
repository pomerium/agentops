package server_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/pomerium/agentops/harness/internal/sidecar/envoyconfig"
	"github.com/pomerium/agentops/harness/internal/sidecar/envparse"
	"github.com/pomerium/agentops/harness/internal/sidecar/server"
)

func TestEndpointsFromEnvMapsEveryField(t *testing.T) {
	got, err := server.Endpoints(server.EndpointsFromEnv([]envparse.Endpoint{
		{
			Name: "mcp_gke", ListenPort: 9100,
			UpstreamURL: "https://gke.example.com/mcp", DialAddress: "pomerium.ns.svc:443",
			Headers: map[string]string{"authorization": "Bearer tok"}, InjectRunToken: true,
		},
		{Name: "anthropic", ListenPort: 9999, UpstreamURL: "https://anthropic.example.com"},
	}))
	if err != nil {
		t.Fatalf("Endpoints: %v", err)
	}
	want := []envoyconfig.Endpoint{
		{Name: "anthropic", ListenPort: 9999, UpstreamURL: "https://anthropic.example.com"},
		{
			Name: "mcp_gke", ListenPort: 9100,
			UpstreamURL: "https://gke.example.com/mcp", DialAddress: "pomerium.ns.svc:443",
			Headers: map[string]string{"authorization": "Bearer tok"}, InjectRunToken: true,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mapping mismatch (-want +got):\n%s", diff)
	}
}

func TestEndpointsRejectsCollisions(t *testing.T) {
	_, err := server.Endpoints(server.EndpointsFromEnv([]envparse.Endpoint{
		{Name: "dup", ListenPort: 9100, UpstreamURL: "https://a.example.com"},
		{Name: "dup", ListenPort: 9101, UpstreamURL: "https://b.example.com"},
	}))
	if err == nil {
		t.Error("a duplicate name must be rejected")
	}

	_, err = server.Endpoints(server.EndpointsFromEnv([]envparse.Endpoint{
		{Name: "a", ListenPort: 9100, UpstreamURL: "https://a.example.com"},
		{Name: "b", ListenPort: 9100, UpstreamURL: "https://b.example.com"},
	}))
	if err == nil {
		t.Error("a shared listen port must be rejected")
	}

	_, err = server.Endpoints(
		[]envoyconfig.Endpoint{{Name: "anthropic", ListenPort: 9999, UpstreamURL: "https://a.example.com"}},
		[]envoyconfig.Endpoint{{Name: "mcp-gke", ListenPort: 9999, UpstreamURL: "https://b.example.com"}},
	)
	if err == nil {
		t.Error("a listen port shared across sources must be rejected")
	}
}
