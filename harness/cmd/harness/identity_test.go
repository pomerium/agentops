package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/agentlink/agentlinktest"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/apistub"
)

func TestAssertionIdentityDuringAJWKSOutageIsRetryable(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "jwks unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)
	v, err := agentlink.NewVerifier(idp.Issuer(),
		agentlink.WithJWKSURL(down.URL), agentlink.WithVerifierLogger(slog.New(slog.DiscardHandler)))
	require.NoError(t, err)

	srv, err := apiserver.New(apistub.New(), assertionIdentity(v), apiserver.WithLogger(slog.New(slog.DiscardHandler)))
	require.NoError(t, err)
	api := httptest.NewServer(srv.Handler())
	t.Cleanup(api.Close)

	req, err := http.NewRequest(http.MethodPost, api.URL+"/harnessapi.v1.HarnessAPIService/ListTemplates", strings.NewReader("{}"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(agentlink.AssertionMetadataKey, idp.Sign(t, map[string]any{"sub": "client-a"}))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"a client was refused as unauthenticated while the signing keys could not be fetched")
}
