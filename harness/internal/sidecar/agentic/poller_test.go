package agentic

import (
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStringLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func writeTokenFile(t *testing.T, val string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(p, []byte(val), 0o600))
	return p
}

func writeCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	return p
}

func newTestPoller(t *testing.T, srv *httptest.Server, tokenFile string) *HTTPPoller {
	t.Helper()
	p, err := NewHTTPPoller(HTTPPollerConfig{
		BaseURL:   srv.URL,
		DialAddr:  srv.Listener.Addr().String(),
		TokenFile: tokenFile,
		CAFile:    writeCA(t, srv),
	})
	require.NoError(t, err)
	return p
}

func TestHTTPPoller_OK(t *testing.T) {
	t.Parallel()
	var gotAuth, gotPath, gotBody, gotMethod string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = io.WriteString(w, `{"access_token":"pom_art_abc","token_type":"Bearer","expires_in":3600,"run_id":"run-9"}`)
	}))
	defer srv.Close()

	res := newTestPoller(t, srv, writeTokenFile(t, "sa-jwt")).Poll(context.Background())
	require.Equal(t, PollOk, res.Kind)
	assert.Equal(t, "Bearer pom_art_abc", res.Token.Bearer)
	assert.Equal(t, "run-9", res.Token.RunID)
	assert.Equal(t, time.Hour, res.Token.ExpiresIn)

	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/agentic/token", gotPath)
	assert.Equal(t, "Bearer sa-jwt", gotAuth)
	assert.Empty(t, gotBody)
}

func TestHTTPPoller_ReReadsTokenFile(t *testing.T) {
	t.Parallel()
	var seen []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, `{"access_token":"pom_art_x","token_type":"Bearer","expires_in":3600,"run_id":"r"}`)
	}))
	defer srv.Close()

	tok := writeTokenFile(t, "A")
	p := newTestPoller(t, srv, tok)
	p.Poll(context.Background())
	require.NoError(t, os.WriteFile(tok, []byte("B"), 0o600))
	p.Poll(context.Background())
	assert.Equal(t, []string{"Bearer A", "Bearer B"}, seen)
}

func TestHTTPPoller_Classification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		status     int
		body       string
		wantKind   PollKind
		wantReason string
	}{
		{"pending", http.StatusBadRequest, `{"error":"authorization_pending"}`, PollPending, ""},
		{"access_denied", http.StatusForbidden, `{"error":"access_denied"}`, PollTerminal, ReasonRevokedOrExpired},
		{"revoked_plain", http.StatusForbidden, `revoked`, PollTerminal, ReasonRevokedOrExpired},
		{"bad_sa_token", http.StatusUnauthorized, `unauthorized`, PollTerminal, ReasonConfigError},
		{"databroker", http.StatusServiceUnavailable, `unavailable`, PollRetryable, ""},
		{"bad_gateway", http.StatusBadGateway, `bad gateway`, PollRetryable, ""},
		{"gateway_timeout", http.StatusGatewayTimeout, `gateway timeout`, PollRetryable, ""},
		{"internal_error", http.StatusInternalServerError, `boom`, PollTerminal, ReasonConfigError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			res := newTestPoller(t, srv, writeTokenFile(t, "t")).Poll(context.Background())
			assert.Equal(t, tc.wantKind, res.Kind)
			if tc.wantReason != "" {
				assert.Equal(t, tc.wantReason, res.Reason)
			}
		})
	}
}

func TestHTTPPoller_NetworkErrorIsRetryable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.Listener.Addr().String()
	srv.Close()

	p, err := NewHTTPPoller(HTTPPollerConfig{BaseURL: "https://as.invalid", DialAddr: addr, TokenFile: writeTokenFile(t, "t")})
	require.NoError(t, err)
	res := p.Poll(context.Background())
	assert.Equal(t, PollRetryable, res.Kind)
}

func TestHTTPPoller_NeverLogsToken(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"access_token":"pom_art_SECRET","token_type":"Bearer","expires_in":3600,"run_id":"r"}`)
	}))
	defer srv.Close()

	var buf strings.Builder
	p, err := NewHTTPPoller(HTTPPollerConfig{
		BaseURL: srv.URL, DialAddr: srv.Listener.Addr().String(), TokenFile: writeTokenFile(t, "sa"),
		CAFile: writeCA(t, srv), Logger: newStringLogger(&buf),
	})
	require.NoError(t, err)
	res := p.Poll(context.Background())
	require.Equal(t, PollOk, res.Kind)
	assert.NotContains(t, buf.String(), "pom_art_SECRET")
}

func TestHTTPPoller_RejectsNonPositiveLifetime(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"missing":  `{"access_token":"pom_art_x","token_type":"Bearer","run_id":"r"}`,
		"zero":     `{"access_token":"pom_art_x","token_type":"Bearer","expires_in":0,"run_id":"r"}`,
		"negative": `{"access_token":"pom_art_x","token_type":"Bearer","expires_in":-1,"run_id":"r"}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer srv.Close()
			res := newTestPoller(t, srv, writeTokenFile(t, "sa")).Poll(context.Background())
			assert.Equal(t, PollTerminal, res.Kind)
			assert.Equal(t, ReasonConfigError, res.Reason)
			assert.Nil(t, res.Token)
		})
	}
}

func TestNewHTTPPoller_RejectsPlaintext(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"http://as.example", "as.example", "https://", "://bad"} {
		_, err := NewHTTPPoller(HTTPPollerConfig{BaseURL: raw, TokenFile: writeTokenFile(t, "sa-secret")})
		assert.Errorf(t, err, "%q must be rejected", raw)
	}
}

func TestHTTPPoller_DoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	leaked := make(chan string, 1)
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked <- r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"access_token":"pom_art_x","token_type":"Bearer","expires_in":3600,"run_id":"r"}`)
	}))
	defer plain.Close()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	p, err := NewHTTPPoller(HTTPPollerConfig{BaseURL: srv.URL, TokenFile: writeTokenFile(t, "sa-secret"), CAFile: writeCA(t, srv)})
	require.NoError(t, err)
	res := p.Poll(context.Background())
	assert.Equal(t, PollTerminal, res.Kind)
	select {
	case got := <-leaked:
		t.Fatalf("redirect was followed to %s with Authorization %q", plain.URL, got)
	default:
	}
}
