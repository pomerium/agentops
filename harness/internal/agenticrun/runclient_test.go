package agenticrun

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRuns struct {
	gotReq CreateRunRequest
	result *CreateRunResult
	status *RunStatus
	err    error
}

var _ RunClient = (*fakeRuns)(nil)

func (f *fakeRuns) CreateRun(_ context.Context, req CreateRunRequest) (*CreateRunResult, error) {
	f.gotReq = req
	if f.err != nil {
		return nil, f.err
	}
	if f.result != nil {
		return f.result, nil
	}
	return &CreateRunResult{
		RunID:       "run-123",
		ApprovalURL: "https://pomerium.example/agentic/approve?run_id=run-123",
	}, nil
}

func (f *fakeRuns) GetRun(context.Context, string) (*RunStatus, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.status != nil {
		return f.status, nil
	}
	return &RunStatus{State: "pending_approval"}, nil
}

func testClient(t *testing.T, srv *httptest.Server, tokenFile string, opts ...Option) *HTTPRunClient {
	t.Helper()
	opts = append([]Option{WithCAFile(writeCA(t, srv))}, opts...)
	c, err := NewHTTPRunClient(srv.URL, srv.Listener.Addr().String(), tokenFile, opts...)
	require.NoError(t, err)
	return c
}

func writeCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	return p
}

func writeToken(t *testing.T, val string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(p, []byte(val), 0o600))
	return p
}

func minimalCreate() CreateRunRequest {
	return CreateRunRequest{
		Prompt:     "do it",
		MCPServers: []string{"https://mcp.example/x"},
		TTL:        15 * time.Minute,
		Executor:   map[string]string{"kubernetes.io.namespace": "ns"},
	}
}

func TestCreateRun_SendsRequestAndParsesResponse(t *testing.T) {
	t.Parallel()
	var gotAuth, gotPath, gotBody string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"run_id":"run-9","approval_url":"https://pub.example/agentic/approve?run_id=run-9","expires_at":"2026-07-20T12:00:00Z"}`)
	}))
	defer srv.Close()

	c := testClient(t, srv, writeToken(t, "wjwt-1"))
	res, err := c.CreateRun(context.Background(), minimalCreate())
	require.NoError(t, err)

	assert.Equal(t, "run-9", res.RunID)
	assert.Contains(t, res.ApprovalURL, "run_id=run-9")
	assert.Equal(t, time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC), res.ExpiresAt)

	assert.Equal(t, "/agentic/runs", gotPath)
	assert.Equal(t, "Bearer wjwt-1", gotAuth)
	assert.Contains(t, gotBody, `"ttl_seconds":900`)
	assert.Contains(t, gotBody, `"mcp_servers":["https://mcp.example/x"]`)
}

func TestCreateRun_ReReadsTokenFileEachCall(t *testing.T) {
	t.Parallel()
	var seen []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"run_id":"r","approval_url":"","expires_at":""}`)
	}))
	defer srv.Close()

	tok := writeToken(t, "A")
	c := testClient(t, srv, tok)
	_, err := c.CreateRun(context.Background(), minimalCreate())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tok, []byte("B"), 0o600))
	_, err = c.CreateRun(context.Background(), minimalCreate())
	require.NoError(t, err)

	assert.Equal(t, []string{"Bearer A", "Bearer B"}, seen)
}

func TestCreateRun_ErrorMapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status    int
		retryable bool
	}{
		{http.StatusServiceUnavailable, true},
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusInternalServerError, false},
	}
	for _, tc := range cases {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, "boom")
		}))
		c := testClient(t, srv, writeToken(t, "t"))
		_, err := c.CreateRun(context.Background(), minimalCreate())
		require.Error(t, err)
		assert.Equalf(t, tc.retryable, IsRetryable(err), "status %d retryable", tc.status)
		srv.Close()
	}
}

func TestGetRun_ParsesFields(t *testing.T) {
	t.Parallel()
	var gotPath string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		_, _ = io.WriteString(w, `{"run_id":"run 1","state":"approved","bound":true,"revoked":false,"expires_at":"2026-07-20T13:00:00Z"}`)
	}))
	defer srv.Close()

	st, err := testClient(t, srv, writeToken(t, "t")).GetRun(context.Background(), "run 1")
	require.NoError(t, err)
	assert.Equal(t, "approved", st.State)
	assert.True(t, st.Bound)
	assert.False(t, st.Revoked)
	assert.Equal(t, time.Date(2026, 7, 20, 13, 0, 0, 0, time.UTC), st.ExpiresAt)
	assert.Equal(t, "/agentic/runs/run%201", gotPath)
}

func TestGetRun_RetryableOn503(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	_, err := testClient(t, srv, writeToken(t, "t")).GetRun(context.Background(), "run-1")
	require.Error(t, err)
	assert.True(t, IsRetryable(err))
}

func TestTLSVerification(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"state":"approved"}`)
	}))
	defer srv.Close()

	untrusted, err := NewHTTPRunClient(srv.URL, srv.Listener.Addr().String(), writeToken(t, "t"))
	require.NoError(t, err)
	_, err = untrusted.GetRun(context.Background(), "run-1")
	require.Error(t, err, "an unknown CA must be rejected (no InsecureSkipVerify)")

	st, err := testClient(t, srv, writeToken(t, "t")).GetRun(context.Background(), "run-1")
	require.NoError(t, err)
	assert.Equal(t, "approved", st.State)
}

func TestNewHTTPRunClient_BadCAFile(t *testing.T) {
	t.Parallel()
	_, err := NewHTTPRunClient("https://as.example", "", "/tmp/t", WithCAFile("/nonexistent/ca.pem"))
	require.Error(t, err)
}

func TestClampPrompt(t *testing.T) {
	t.Parallel()
	const marker = "… [truncated]"

	assert.Equal(t, "hello", ClampPrompt("hello"))

	exact := strings.Repeat("a", 4096)
	assert.Equal(t, exact, ClampPrompt(exact))
	assert.Len(t, ClampPrompt(exact), 4096)

	over := strings.Repeat("a", 4097)
	got := ClampPrompt(over)
	assert.LessOrEqual(t, len(got), 4096)
	assert.Equal(t, 4096, len(got))
	assert.True(t, strings.HasSuffix(got, marker))

	multi := strings.Repeat("a", 4080) + strings.Repeat("€", 20)
	gotMulti := ClampPrompt(multi)
	assert.True(t, utf8.ValidString(gotMulti), "clamped prompt must be valid UTF-8")
	assert.True(t, strings.HasSuffix(gotMulti, marker))
	assert.LessOrEqual(t, len(gotMulti), 4096)
	assert.Equal(t, strings.Repeat("a", 4080)+marker, gotMulti)
}

func TestNewHTTPRunClient_RejectsPlaintext(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"http://as.example", "as.example", "https://", "://bad"} {
		_, err := NewHTTPRunClient(raw, "", writeToken(t, "workload-secret"))
		assert.Errorf(t, err, "%q must be rejected", raw)
	}
}

func TestRunClient_DoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	leaked := make(chan string, 1)
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked <- r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"state":"approved"}`)
	}))
	defer plain.Close()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	c, err := NewHTTPRunClient(srv.URL, "", writeToken(t, "workload-secret"), WithCAFile(writeCA(t, srv)))
	require.NoError(t, err)
	_, err = c.GetRun(context.Background(), "run-1")
	require.Error(t, err)
	select {
	case got := <-leaked:
		t.Fatalf("redirect was followed to %s with Authorization %q", plain.URL, got)
	default:
	}
}
