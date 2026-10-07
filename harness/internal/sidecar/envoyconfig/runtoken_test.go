package envoyconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	credinjv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/credential_injector/v3"
	genericv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/http/injected_credentials/generic/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/pomerium/agentops/harness/internal/sidecar/envoyconfig"
)

func TestBuildBootstrap_InjectRunTokenPrependsCredentialInjector(t *testing.T) {
	t.Parallel()
	const sdsPath = "/run/sidecar/sds/run_token.yaml"
	b, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "gke", ListenPort: 9100, UpstreamURL: "https://gke.example.com", InjectRunToken: true},
	}, "/tmp/admin.sock", sdsPath, "")
	require.NoError(t, err)

	hcm := unpackHCM(t, b.StaticResources.Listeners[0])
	require.Len(t, hcm.HttpFilters, 2, "credential_injector must be prepended before the router")
	assert.Equal(t, "envoy.filters.http.credential_injector", hcm.HttpFilters[0].Name)
	assert.Equal(t, "envoy.filters.http.router", hcm.HttpFilters[1].Name)

	var inj credinjv3.CredentialInjector
	require.NoError(t, hcm.HttpFilters[0].GetTypedConfig().UnmarshalTo(&inj))
	assert.True(t, inj.Overwrite, "must overwrite any client-sent Authorization")
	assert.False(t, inj.AllowRequestWithoutCredential, "must fail closed (missing secret => 401)")

	var generic genericv3.Generic
	require.NoError(t, inj.GetCredential().GetTypedConfig().UnmarshalTo(&generic))
	sds := generic.GetCredential()
	assert.Equal(t, "run_token", sds.GetName())
	pcs := sds.GetSdsConfig().GetPathConfigSource()
	require.NotNil(t, pcs, "SDS must use a filesystem path_config_source")
	assert.Equal(t, sdsPath, pcs.GetPath())
	assert.Equal(t, filepath.Dir(sdsPath), pcs.GetWatchedDirectory().GetPath(),
		"a watched directory is required so an atomic rename hot-reloads the secret")
}

func TestBuildBootstrap_InjectRunTokenConflictsWithStaticAuth(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Authorization", "authorization"} {
		_, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
			{Name: "x", ListenPort: 9100, UpstreamURL: "https://x", InjectRunToken: true, Headers: map[string]string{name: "Bearer static"}},
		}, "/tmp/admin.sock", "/run/sds/run_token.yaml", "")
		require.Errorf(t, err, "header %q must conflict with inject_run_token", name)
		assert.Contains(t, err.Error(), "inject_run_token")
	}
}

func TestBuildBootstrap_InjectRunTokenRequiresSDSPath(t *testing.T) {
	t.Parallel()
	_, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "x", ListenPort: 9100, UpstreamURL: "https://x", InjectRunToken: true},
	}, "/tmp/admin.sock", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SDS path")
}

func TestBuildBootstrap_InjectRunTokenRequiresHTTPS(t *testing.T) {
	t.Parallel()
	_, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "x", ListenPort: 9100, UpstreamURL: "http://x", InjectRunToken: true},
	}, "/tmp/admin.sock", "/run/sds/run_token.yaml", "")
	require.Error(t, err, "a run token must not be sent without TLS")
	assert.Contains(t, err.Error(), "https")

	_, err = envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "x", ListenPort: 9100, UpstreamURL: "http://x"},
	}, "/tmp/admin.sock", "/run/sds/run_token.yaml", "")
	require.NoError(t, err, "a plaintext upstream without a run token stays allowed")
}

func TestBuildBootstrap_NonInjectEndpointHasNoCredentialInjector(t *testing.T) {
	t.Parallel()
	b, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "plain", ListenPort: 9100, UpstreamURL: "https://x"},
	}, "/tmp/admin.sock", "/run/sds/run_token.yaml", "")
	require.NoError(t, err)
	hcm := unpackHCM(t, b.StaticResources.Listeners[0])
	require.Len(t, hcm.HttpFilters, 1)
	assert.Equal(t, "envoy.filters.http.router", hcm.HttpFilters[0].Name)
}

func upstreamTrustedCA(t *testing.T, c *clusterv3.Cluster) string {
	t.Helper()
	require.NotNil(t, c.TransportSocket, "https upstream must have a TLS transport socket")
	var tlsCtx tlsv3.UpstreamTlsContext
	require.NoError(t, c.TransportSocket.GetTypedConfig().UnmarshalTo(&tlsCtx))
	return tlsCtx.GetCommonTlsContext().GetValidationContext().GetTrustedCa().GetFilename()
}

func TestBuildBootstrap_InjectEndpointTrustsRunTokenCA(t *testing.T) {
	t.Parallel()
	b, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "gke", ListenPort: 9100, UpstreamURL: "https://gke.example.com", InjectRunToken: true},
	}, "/tmp/admin.sock", "/run/sds/run_token.yaml", "/etc/agentic-ca/rootCA.pem")
	require.NoError(t, err)
	assert.Equal(t, "/etc/agentic-ca/rootCA.pem", upstreamTrustedCA(t, b.StaticResources.Clusters[0]),
		"an inject_run_token endpoint validates its Pomerium upstream against the run-token CA")
}

func TestBuildBootstrap_InjectEndpointFallsBackToSystemCA(t *testing.T) {
	t.Parallel()
	b, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "gke", ListenPort: 9100, UpstreamURL: "https://gke.example.com", InjectRunToken: true},
	}, "/tmp/admin.sock", "/run/sds/run_token.yaml", "")
	require.NoError(t, err)
	assert.Equal(t, "/etc/ssl/certs/ca-certificates.crt", upstreamTrustedCA(t, b.StaticResources.Clusters[0]))
}

func TestBuildBootstrap_NonInjectEndpointAlwaysSystemCA(t *testing.T) {
	t.Parallel()
	b, err := envoyconfig.BuildBootstrap([]envoyconfig.Endpoint{
		{Name: "anthropic", ListenPort: 9999, UpstreamURL: "https://api.anthropic.com"},
	}, "/tmp/admin.sock", "/run/sds/run_token.yaml", "/etc/agentic-ca/rootCA.pem")
	require.NoError(t, err)
	assert.Equal(t, "/etc/ssl/certs/ca-certificates.crt", upstreamTrustedCA(t, b.StaticResources.Clusters[0]))
}

func TestRenderRunTokenSecret(t *testing.T) {
	t.Parallel()
	data, err := envoyconfig.RenderRunTokenSecret("Bearer pom_art_abc")
	require.NoError(t, err)

	var resp discoveryv3.DiscoveryResponse
	require.NoError(t, protojson.Unmarshal(data, &resp))
	require.Len(t, resp.Resources, 1)

	var secret tlsv3.Secret
	require.NoError(t, resp.Resources[0].UnmarshalTo(&secret))
	assert.Equal(t, "run_token", secret.GetName())
	ds := secret.GetGenericSecret().GetSecret()
	assert.Equal(t, "Bearer pom_art_abc", ds.GetSpecifier().(*corev3.DataSource_InlineString).InlineString)
}

func TestWriteRunTokenSecretAtomic(t *testing.T) {
	t.Parallel()
	configDir := t.TempDir()
	watchedDir := filepath.Join(configDir, "sds")
	require.NoError(t, os.MkdirAll(watchedDir, 0o700))
	sdsPath := filepath.Join(watchedDir, "run_token.yaml")

	require.NoError(t, envoyconfig.WriteRunTokenSecret(sdsPath, "Bearer pom_art_1"))

	got, err := os.ReadFile(sdsPath)
	require.NoError(t, err)
	var resp discoveryv3.DiscoveryResponse
	require.NoError(t, protojson.Unmarshal(got, &resp))
	var secret tlsv3.Secret
	require.NoError(t, resp.Resources[0].UnmarshalTo(&secret))
	assert.Equal(t, "Bearer pom_art_1", secret.GetGenericSecret().GetSecret().GetInlineString())

	entries, err := os.ReadDir(watchedDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "run_token.yaml", entries[0].Name())

	require.NoError(t, envoyconfig.WriteRunTokenSecret(sdsPath, "Bearer pom_art_2"))
	got2, err := os.ReadFile(sdsPath)
	require.NoError(t, err)
	require.NoError(t, protojson.Unmarshal(got2, &resp))
	require.NoError(t, resp.Resources[0].UnmarshalTo(&secret))
	assert.Equal(t, "Bearer pom_art_2", secret.GetGenericSecret().GetSecret().GetInlineString())
	entries, err = os.ReadDir(watchedDir)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}
