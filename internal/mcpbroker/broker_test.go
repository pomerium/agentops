package mcpbroker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pomerium/agentops/internal/chatops/store"
	"github.com/pomerium/agentops/internal/mcpbroker"
)

// fakeProvider is an in-memory OAuth authorization server + protected resource
// implementing just enough of RFC 7591 (DCR), RFC 8414 (AS metadata), RFC 9728
// (protected-resource metadata) and the auth-code + refresh token grants for
// the broker to exercise its full flow.
type fakeProvider struct {
	srv          *httptest.Server
	issuedCodes  map[string]bool
	refreshCount atomic.Int64
}

func newFakeProvider(t *testing.T) *fakeProvider {
	t.Helper()
	fp := &fakeProvider{issuedCodes: map[string]bool{}}
	mux := http.NewServeMux()

	// MCP endpoint: unauthenticated requests get a 401 advertising the
	// protected-resource metadata document.
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate",
			fmt.Sprintf(`Bearer resource_metadata="%s/.well-known/oauth-protected-resource"`, fp.srv.URL))
		w.WriteHeader(http.StatusUnauthorized)
	})

	mux.HandleFunc("/.well-known/oauth-protected-resource", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"resource":              fp.srv.URL + "/mcp",
			"authorization_servers": []string{fp.srv.URL},
		})
	})

	asMeta := func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"issuer":                                fp.srv.URL,
			"authorization_endpoint":                fp.srv.URL + "/authorize",
			"token_endpoint":                        fp.srv.URL + "/token",
			"registration_endpoint":                 fp.srv.URL + "/register",
			"response_types_supported":              []string{"code"},
			"grant_types_supported":                 []string{"authorization_code", "refresh_token"},
			"code_challenge_methods_supported":      []string{"S256"},
			"token_endpoint_auth_methods_supported": []string{"client_secret_basic", "none"},
			"scopes_supported":                      []string{"read", "write"},
		})
	}
	mux.HandleFunc("/.well-known/oauth-authorization-server", asMeta)
	mux.HandleFunc("/.well-known/openid-configuration", asMeta)

	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, map[string]any{
			"client_id":     "dcr-client-id",
			"client_secret": "dcr-client-secret",
			"redirect_uris": []string{"placeholder"},
		})
	})

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.Form.Get("grant_type") {
		case "refresh_token":
			fp.refreshCount.Add(1)
			writeJSON(w, map[string]any{
				"access_token":  "refreshed-access-token",
				"token_type":    "Bearer",
				"refresh_token": "refresh-token-2",
				"expires_in":    3600,
			})
		default: // authorization_code
			writeJSON(w, map[string]any{
				"access_token":  "first-access-token",
				"token_type":    "Bearer",
				"refresh_token": "refresh-token-1",
				"expires_in":    3600,
			})
		}
	})

	fp.srv = httptest.NewServer(mux)
	t.Cleanup(fp.srv.Close)
	return fp
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), t.TempDir()+"/test.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// flowIDFromConnectURL extracts the trailing flow id from a short connect URL
// like https://app.example/connect/<flow_id>.
func flowIDFromConnectURL(t *testing.T, connectURL string) string {
	t.Helper()
	const marker = "/connect/"
	i := strings.LastIndex(connectURL, marker)
	if i < 0 {
		t.Fatalf("connect URL %q missing %q", connectURL, marker)
	}
	return connectURL[i+len(marker):]
}

func TestStartAuthFlowReturnsShortConnectURL(t *testing.T) {
	ctx := context.Background()
	fp := newFakeProvider(t)
	s := newTestStore(t)

	b := mcpbroker.New(s, mcpbroker.Options{
		HTTPClient:      fp.srv.Client(),
		RedirectBaseURL: "https://app.example",
		ClientName:      "agentops-test",
	})

	connectURL, err := b.StartAuthFlow(ctx, mcpbroker.StartAuthRequest{
		SlackUserID:    "U1",
		SlackChannelID: "C1",
		SlackThreadTS:  "1.2",
		ServerName:     "github",
		ServerURL:      fp.srv.URL + "/mcp",
	})
	if err != nil {
		t.Fatalf("StartAuthFlow: %v", err)
	}

	// StartAuthFlow returns a short app-domain URL, NOT the provider authorize
	// URL (which would leak PKCE/state/client_id into the Slack button tooltip).
	if !strings.HasPrefix(connectURL, "https://app.example/connect/") {
		t.Fatalf("connect URL = %q, want https://app.example/connect/<id>", connectURL)
	}
	if strings.Contains(connectURL, "code_challenge") || strings.Contains(connectURL, "client_id") {
		t.Errorf("connect URL must not expose oauth params: %q", connectURL)
	}
	flowID := flowIDFromConnectURL(t, connectURL)

	// AuthorizeURL resolves the flow id back to the real provider authorize URL.
	authURL, err := b.AuthorizeURL(ctx, flowID)
	if err != nil {
		t.Fatalf("AuthorizeURL: %v", err)
	}
	u, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("authURL not a URL: %v", err)
	}
	q := u.Query()
	if q.Get("client_id") != "dcr-client-id" {
		t.Errorf("client_id = %q, want dcr-client-id", q.Get("client_id"))
	}
	if q.Get("code_challenge") == "" || q.Get("code_challenge_method") != "S256" {
		t.Errorf("missing PKCE challenge: %v", q)
	}
	if q.Get("redirect_uri") != "https://app.example/oauth/callback" {
		t.Errorf("redirect_uri = %q", q.Get("redirect_uri"))
	}
	state := q.Get("state")
	if state == "" {
		t.Fatal("missing state")
	}
	// A flow row must have been persisted, keyed by state.
	flow, err := s.GetOAuthFlowByState(ctx, state)
	if err != nil {
		t.Fatalf("flow not persisted: %v", err)
	}
	if flow.SlackUserID != "U1" || flow.ServerName != "github" {
		t.Errorf("flow fields wrong: %+v", flow)
	}
	if flow.PKCEVerifier == "" {
		t.Error("flow missing PKCE verifier")
	}
}

func TestAuthorizeURLUnknownFlow(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	b := mcpbroker.New(s, mcpbroker.Options{RedirectBaseURL: "https://app.example"})
	if _, err := b.AuthorizeURL(ctx, "does-not-exist"); err == nil {
		t.Error("expected error resolving unknown flow id")
	}
}

func TestHandleCallbackExchangesAndStores(t *testing.T) {
	ctx := context.Background()
	fp := newFakeProvider(t)
	s := newTestStore(t)
	b := mcpbroker.New(s, mcpbroker.Options{
		HTTPClient:      fp.srv.Client(),
		RedirectBaseURL: "https://app.example",
	})

	connectURL, err := b.StartAuthFlow(ctx, mcpbroker.StartAuthRequest{
		SlackUserID: "U1", SlackChannelID: "C1", SlackThreadTS: "1.2",
		ServerName: "github", ServerURL: fp.srv.URL + "/mcp", WorkflowName: "deploy",
	})
	if err != nil {
		t.Fatalf("StartAuthFlow: %v", err)
	}
	authURL, err := b.AuthorizeURL(ctx, flowIDFromConnectURL(t, connectURL))
	if err != nil {
		t.Fatalf("AuthorizeURL: %v", err)
	}
	state := mustQuery(t, authURL, "state")

	res, err := b.HandleCallback(ctx, "auth-code-123", state)
	if err != nil {
		t.Fatalf("HandleCallback: %v", err)
	}
	if res.SlackUserID != "U1" || res.ServerName != "github" || res.WorkflowName != "deploy" {
		t.Errorf("callback result wrong: %+v", res)
	}

	cred, err := s.GetMCPCredential(ctx, "U1", "github")
	if err != nil {
		t.Fatalf("credential not stored: %v", err)
	}
	if cred.AccessToken != "first-access-token" || cred.RefreshToken != "refresh-token-1" {
		t.Errorf("stored token wrong: %+v", cred)
	}
	if cred.ClientID != "dcr-client-id" || cred.TokenURL != fp.srv.URL+"/token" {
		t.Errorf("stored registration wrong: %+v", cred)
	}
	// flow must be consumed
	if _, err := s.GetOAuthFlowByState(ctx, state); err == nil {
		t.Error("expected flow to be deleted after callback")
	}
}

func TestAccessTokenRefreshesWhenExpired(t *testing.T) {
	ctx := context.Background()
	fp := newFakeProvider(t)
	s := newTestStore(t)
	b := mcpbroker.New(s, mcpbroker.Options{HTTPClient: fp.srv.Client(), RedirectBaseURL: "https://app.example"})

	// Seed an already-expired credential pointing at the fake provider.
	if err := s.SaveMCPCredential(ctx, store.MCPCredential{
		SlackUserID:  "U1",
		ServerName:   "github",
		ServerURL:    fp.srv.URL + "/mcp",
		AccessToken:  "stale",
		RefreshToken: "refresh-token-1",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-time.Hour),
		TokenURL:     fp.srv.URL + "/token",
		AuthURL:      fp.srv.URL + "/authorize",
		ClientID:     "dcr-client-id",
		ClientSecret: "dcr-client-secret",
	}); err != nil {
		t.Fatalf("seed credential: %v", err)
	}

	tok, err := b.AccessToken(ctx, "U1", "github", fp.srv.URL+"/mcp")
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if tok != "refreshed-access-token" {
		t.Errorf("expected refreshed token, got %q", tok)
	}
	if fp.refreshCount.Load() == 0 {
		t.Error("expected a refresh call to the token endpoint")
	}
	// The refreshed token must be persisted back to the store.
	cred, _ := s.GetMCPCredential(ctx, "U1", "github")
	if cred.AccessToken != "refreshed-access-token" {
		t.Errorf("refreshed token not persisted: %q", cred.AccessToken)
	}
}

func TestAccessTokenNotConnected(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	b := mcpbroker.New(s, mcpbroker.Options{RedirectBaseURL: "https://app.example"})
	if _, err := b.AccessToken(ctx, "nobody", "nothing", "https://x/mcp"); err == nil {
		t.Error("expected error for missing credential")
	}
}

func TestAccessTokenRejectsWrongServerURL(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	b := mcpbroker.New(s, mcpbroker.Options{RedirectBaseURL: "https://app.example"})
	if err := s.SaveMCPCredential(ctx, store.MCPCredential{
		SlackUserID: "U1", ServerName: "github", ServerURL: "https://gh-a.example/mcp",
		AccessToken: "tok", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A credential stored for gh-a must not be handed out for gh-b under the same name.
	if _, err := b.AccessToken(ctx, "U1", "github", "https://gh-b.example/mcp"); err != mcpbroker.ErrNotConnected {
		t.Errorf("expected ErrNotConnected for mismatched server URL, got %v", err)
	}
	// The matching URL still works (trailing slash tolerated).
	if _, err := b.AccessToken(ctx, "U1", "github", "https://gh-a.example/mcp/"); err != nil {
		t.Errorf("expected token for matching URL, got %v", err)
	}
}

func TestConnectionStatus(t *testing.T) {
	ctx := context.Background()
	fp := newFakeProvider(t)
	s := newTestStore(t)

	var gotToken string
	connect := func(ctx context.Context, serverURL string, hc *http.Client) error {
		// Capture the bearer token the broker would inject.
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, serverURL, nil)
		resp, err := hc.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		gotToken = resp.Request.Header.Get("Authorization")
		return nil
	}

	b := mcpbroker.New(s, mcpbroker.Options{
		HTTPClient:      fp.srv.Client(),
		RedirectBaseURL: "https://app.example",
		Connect:         connect,
	})

	// No credential -> not connected, no error.
	connected, err := b.ConnectionStatus(ctx, "U1", "github", fp.srv.URL+"/mcp")
	if err != nil {
		t.Fatalf("ConnectionStatus (no cred): %v", err)
	}
	if connected {
		t.Error("expected not connected without a credential")
	}

	// With a valid credential -> connected, and the injected client carries the bearer.
	if err := s.SaveMCPCredential(ctx, store.MCPCredential{
		SlackUserID: "U1", ServerName: "github", ServerURL: fp.srv.URL + "/mcp",
		AccessToken: "first-access-token", TokenType: "Bearer",
		Expiry: time.Now().Add(time.Hour), TokenURL: fp.srv.URL + "/token",
		ClientID: "dcr-client-id", ClientSecret: "dcr-client-secret",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	connected, err = b.ConnectionStatus(ctx, "U1", "github", fp.srv.URL+"/mcp")
	if err != nil {
		t.Fatalf("ConnectionStatus (valid): %v", err)
	}
	if !connected {
		t.Error("expected connected with a valid credential")
	}
	if !strings.Contains(gotToken, "first-access-token") {
		t.Errorf("expected bearer token to be injected, got %q", gotToken)
	}
}

func mustQuery(t *testing.T, rawURL, key string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse %q: %v", rawURL, err)
	}
	return u.Query().Get(key)
}
