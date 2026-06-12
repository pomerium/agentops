package envoyconfig_test

import (
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/pomerium/agentops/internal/sidecar/envoyconfig"
)

func mustBuild(t *testing.T, eps []envoyconfig.Endpoint) ( //nolint:unparam
	listeners []*listenerv3.Listener, clusters []*clusterv3.Cluster,
) {
	t.Helper()
	b, err := envoyconfig.BuildBootstrap(eps, "/tmp/sidecar-admin.sock")
	if err != nil {
		t.Fatalf("BuildBootstrap: %v", err)
	}
	return b.StaticResources.Listeners, b.StaticResources.Clusters
}

func unpackHCM(t *testing.T, l *listenerv3.Listener) *hcmv3.HttpConnectionManager {
	t.Helper()
	if len(l.FilterChains) != 1 || len(l.FilterChains[0].Filters) != 1 {
		t.Fatalf("listener %s: expected exactly one filter chain with one filter", l.Name)
	}
	var hcm hcmv3.HttpConnectionManager
	if err := l.FilterChains[0].Filters[0].GetTypedConfig().UnmarshalTo(&hcm); err != nil {
		t.Fatalf("unpack HCM: %v", err)
	}
	return &hcm
}

func soleRoute(t *testing.T, hcm *hcmv3.HttpConnectionManager) *routev3.Route {
	t.Helper()
	rc := hcm.GetRouteConfig()
	if rc == nil {
		t.Fatal("HCM has no inline route config")
	}
	if len(rc.VirtualHosts) != 1 {
		t.Fatalf("expected 1 virtual host, got %d", len(rc.VirtualHosts))
	}
	vh := rc.VirtualHosts[0]
	if len(vh.Domains) != 1 || vh.Domains[0] != "*" {
		t.Errorf("vhost domains = %v, want [*]", vh.Domains)
	}
	if len(vh.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(vh.Routes))
	}
	return vh.Routes[0]
}

func TestBuildBootstrapHTTPSUpstream(t *testing.T) {
	t.Parallel()
	listeners, clusters := mustBuild(t, []envoyconfig.Endpoint{{
		Name:        "anthropic",
		ListenPort:  9999,
		UpstreamURL: "https://api.anthropic.com",
		Headers:     map[string]string{"x-api-key": "sk-secret", "anthropic-beta": "b"},
	}})

	if len(listeners) != 1 || len(clusters) != 1 {
		t.Fatalf("got %d listeners, %d clusters; want 1 each", len(listeners), len(clusters))
	}

	// Listener binds to loopback only on the requested port.
	addr := listeners[0].Address.GetSocketAddress()
	if addr.GetAddress() != "127.0.0.1" {
		t.Errorf("listener address = %q, want 127.0.0.1", addr.GetAddress())
	}
	if addr.GetPortValue() != 9999 {
		t.Errorf("listener port = %d, want 9999", addr.GetPortValue())
	}

	hcm := unpackHCM(t, listeners[0])
	// The router filter is the terminal http filter; without it requests are
	// accepted and then hang forever (envoy does not reject the config).
	if n := len(hcm.HttpFilters); n != 1 {
		t.Fatalf("got %d http filters, want 1 (router)", n)
	}
	if hcm.HttpFilters[0].Name != "envoy.filters.http.router" {
		t.Errorf("http filter = %q, want envoy.filters.http.router", hcm.HttpFilters[0].Name)
	}
	if hcm.HttpFilters[0].GetTypedConfig() == nil {
		t.Error("router filter missing typed config")
	}

	route := soleRoute(t, hcm)
	if route.Match.GetPrefix() != "/" {
		t.Errorf("route match prefix = %q, want /", route.Match.GetPrefix())
	}
	action := route.GetRoute()
	if action.GetCluster() != clusters[0].Name {
		t.Errorf("route cluster = %q, want %q", action.GetCluster(), clusters[0].Name)
	}
	if action.GetHostRewriteLiteral() != "api.anthropic.com" {
		t.Errorf("host rewrite = %q, want api.anthropic.com", action.GetHostRewriteLiteral())
	}
	// Zero timeouts are load-bearing: envoy's 15s default kills SSE MCP
	// streams and streaming LLM responses.
	if action.Timeout == nil || action.Timeout.AsDuration() != 0 {
		t.Errorf("route timeout = %v, want explicit 0s", action.Timeout)
	}
	if action.IdleTimeout == nil || action.IdleTimeout.AsDuration() != 0 {
		t.Errorf("route idle timeout = %v, want explicit 0s", action.IdleTimeout)
	}

	// Secret headers are injected, overwriting anything the agent sent, in
	// deterministic (sorted) order.
	hdrs := route.RequestHeadersToAdd
	if len(hdrs) != 2 {
		t.Fatalf("got %d injected headers, want 2", len(hdrs))
	}
	if hdrs[0].Header.Key != "anthropic-beta" || hdrs[1].Header.Key != "x-api-key" {
		t.Errorf("header order = [%s %s], want sorted [anthropic-beta x-api-key]", hdrs[0].Header.Key, hdrs[1].Header.Key)
	}
	if hdrs[1].Header.Value != "sk-secret" {
		t.Errorf("x-api-key value = %q, want sk-secret", hdrs[1].Header.Value)
	}
	for _, h := range hdrs {
		if h.AppendAction != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
			t.Errorf("header %s append action = %v, want OVERWRITE_IF_EXISTS_OR_ADD", h.Header.Key, h.AppendAction)
		}
	}

	// Cluster: logical DNS to api.anthropic.com:443 with TLS+SNI.
	c := clusters[0]
	if c.GetType() != clusterv3.Cluster_LOGICAL_DNS {
		t.Errorf("cluster type = %v, want LOGICAL_DNS", c.GetType())
	}
	ep := c.LoadAssignment.Endpoints[0].LbEndpoints[0].GetEndpoint().Address.GetSocketAddress()
	if ep.GetAddress() != "api.anthropic.com" || ep.GetPortValue() != 443 {
		t.Errorf("upstream = %s:%d, want api.anthropic.com:443", ep.GetAddress(), ep.GetPortValue())
	}
	ts := c.TransportSocket
	if ts == nil {
		t.Fatal("https upstream must have a TLS transport socket")
	}
	var tlsCtx tlsv3.UpstreamTlsContext
	if err := ts.GetTypedConfig().UnmarshalTo(&tlsCtx); err != nil {
		t.Fatalf("unpack tls context: %v", err)
	}
	if tlsCtx.Sni != "api.anthropic.com" {
		t.Errorf("SNI = %q, want api.anthropic.com", tlsCtx.Sni)
	}
	if tlsCtx.GetCommonTlsContext().GetValidationContext() == nil {
		t.Error("upstream TLS must validate the server certificate")
	}
}

func TestBuildBootstrapHTTPUpstreamWithPort(t *testing.T) {
	t.Parallel()
	listeners, clusters := mustBuild(t, []envoyconfig.Endpoint{{
		Name:        "echo",
		ListenPort:  9100,
		UpstreamURL: "http://echo.test.svc:8080",
	}})

	c := clusters[0]
	if c.TransportSocket != nil {
		t.Error("plain http upstream must not have a TLS transport socket")
	}
	ep := c.LoadAssignment.Endpoints[0].LbEndpoints[0].GetEndpoint().Address.GetSocketAddress()
	if ep.GetAddress() != "echo.test.svc" || ep.GetPortValue() != 8080 {
		t.Errorf("upstream = %s:%d, want echo.test.svc:8080", ep.GetAddress(), ep.GetPortValue())
	}
	if got := listeners[0].Address.GetSocketAddress().GetPortValue(); got != 9100 {
		t.Errorf("listener port = %d, want 9100", got)
	}
}

func TestBuildBootstrapHTTPDefaultPort(t *testing.T) {
	t.Parallel()
	_, clusters := mustBuild(t, []envoyconfig.Endpoint{{
		Name: "plain", ListenPort: 9101, UpstreamURL: "http://example.com",
	}})
	ep := clusters[0].LoadAssignment.Endpoints[0].LbEndpoints[0].GetEndpoint().Address.GetSocketAddress()
	if ep.GetPortValue() != 80 {
		t.Errorf("default http port = %d, want 80", ep.GetPortValue())
	}
}

func TestBuildBootstrapAdminAndMarshal(t *testing.T) {
	t.Parallel()
	b, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "a", ListenPort: 9100, UpstreamURL: "https://a.example.com"},
		{Name: "b", ListenPort: 9101, UpstreamURL: "http://b.example.com:8080"},
	}, "/run/sidecar/admin.sock")
	if err != nil {
		t.Fatalf("BuildBootstrap: %v", err)
	}
	// Finding #2: the admin interface must bind a unix socket on the sidecar's
	// own filesystem, NOT a loopback TCP port — the agent container shares the
	// pod network namespace and could otherwise curl /config_dump to read the
	// injected secret headers, or /quitquitquit to kill the proxy.
	if b.Admin.Address.GetSocketAddress() != nil {
		t.Error("admin bound a TCP socket; want a unix-domain pipe unreachable over the pod network")
	}
	if got := b.Admin.Address.GetPipe().GetPath(); got != "/run/sidecar/admin.sock" {
		t.Errorf("admin pipe path = %q, want /run/sidecar/admin.sock", got)
	}
	// The bootstrap must be serializable to JSON envoy accepts (validates the
	// typed-config Any packing).
	data, err := protojson.Marshal(b)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty bootstrap JSON")
	}
}

func TestBuildBootstrapErrors(t *testing.T) {
	t.Parallel()
	for name, eps := range map[string][]envoyconfig.Endpoint{
		"bad scheme":     {{Name: "a", ListenPort: 9100, UpstreamURL: "ftp://x"}},
		"no host":        {{Name: "a", ListenPort: 9100, UpstreamURL: "https://"}},
		"zero port":      {{Name: "a", ListenPort: 0, UpstreamURL: "https://x"}},
		"empty name":     {{Name: "", ListenPort: 9100, UpstreamURL: "https://x"}},
		"duplicate name": {{Name: "a", ListenPort: 9100, UpstreamURL: "https://x"}, {Name: "a", ListenPort: 9101, UpstreamURL: "https://y"}},
		"duplicate port": {{Name: "a", ListenPort: 9100, UpstreamURL: "https://x"}, {Name: "b", ListenPort: 9100, UpstreamURL: "https://y"}},
	} {
		if _, err := envoyconfig.BuildBootstrap(eps, "/tmp/admin.sock"); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// Finding #1: an empty CertificateValidationContext disables upstream
// certificate verification (envoy only verifies when trusted_ca or
// system_root_certs is set). The injected secret headers would then be exposed
// to a MITM presenting any certificate. The validation context must opt into a
// real trust store.
func TestBuildBootstrapHTTPSUpstreamVerifiesCert(t *testing.T) {
	t.Parallel()
	_, clusters := mustBuild(t, []envoyconfig.Endpoint{{
		Name: "a", ListenPort: 9100, UpstreamURL: "https://api.anthropic.com",
		Headers: map[string]string{"x-api-key": "sk-secret"},
	}})
	ts := clusters[0].TransportSocket
	if ts == nil {
		t.Fatal("https upstream must have a TLS transport socket")
	}
	var tlsCtx tlsv3.UpstreamTlsContext
	if err := ts.GetTypedConfig().UnmarshalTo(&tlsCtx); err != nil {
		t.Fatalf("unpack tls context: %v", err)
	}
	vc := tlsCtx.GetCommonTlsContext().GetValidationContext()
	if vc == nil {
		t.Fatal("upstream TLS must have a validation context")
	}
	// An empty validation context means "verify nothing". Require an explicit
	// trust store: either a trusted_ca bundle or the system root certs.
	if vc.GetTrustedCa() == nil && vc.GetSystemRootCerts() == nil {
		t.Error("validation context verifies no CA: set trusted_ca or system_root_certs")
	}
}

// Finding #5: the Host header rewrite dropped the port for upstreams on
// non-default ports, so a virtual-host-routed gateway on :8443 receives
// "Host: mcp.internal" and 404s/421s.
func TestBuildBootstrapHostRewriteKeepsNonDefaultPort(t *testing.T) {
	t.Parallel()
	listeners, _ := mustBuild(t, []envoyconfig.Endpoint{{
		Name: "a", ListenPort: 9100, UpstreamURL: "https://mcp.internal:8443/sse",
	}})
	hcm := unpackHCM(t, listeners[0])
	got := soleRoute(t, hcm).GetRoute().GetHostRewriteLiteral()
	if got != "mcp.internal:8443" {
		t.Errorf("host rewrite = %q, want mcp.internal:8443 (port preserved)", got)
	}
}

// Finding #5 (companion): default ports stay omitted from the Host header per
// HTTP convention.
func TestBuildBootstrapHostRewriteOmitsDefaultPort(t *testing.T) {
	t.Parallel()
	listeners, _ := mustBuild(t, []envoyconfig.Endpoint{{
		Name: "a", ListenPort: 9100, UpstreamURL: "https://api.anthropic.com:443",
	}})
	hcm := unpackHCM(t, listeners[0])
	got := soleRoute(t, hcm).GetRoute().GetHostRewriteLiteral()
	if got != "api.anthropic.com" {
		t.Errorf("host rewrite = %q, want api.anthropic.com (default port omitted)", got)
	}
}
