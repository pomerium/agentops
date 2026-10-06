package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pomerium/agentops/harness/internal/sidecar/agentic"
	"github.com/pomerium/agentops/harness/internal/sidecar/envoyconfig"
)

func workloadJWT(t *testing.T, ttl time.Duration) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"aud": "pomerium-egress", "exp": time.Now().Add(ttl).Unix()})
	require.NoError(t, err)
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256"}`)) + "." + enc(payload) + ".sig"
}

func newWorkloadSidecar(t *testing.T, envoyScript string) *sidecar {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "scw")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	lis, err := net.Listen("unix", filepath.Join(dir, "admin.sock"))
	require.NoError(t, err)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			fmt.Fprintln(w, "LIVE")
			return
		}
		http.NotFound(w, r)
	}))
	_ = srv.Listener.Close()
	srv.Listener = lis
	srv.Start()
	t.Cleanup(srv.Close)

	envoy := filepath.Join(dir, "fake-envoy")
	require.NoError(t, os.WriteFile(envoy, []byte("#!/bin/sh\n"+envoyScript), 0o755))
	s, err := newSidecar(slog.New(slog.DiscardHandler),
		[]envoyconfig.Endpoint{{Name: "llm", ListenPort: 9999, UpstreamURL: "https://llm.example.com", InjectRunToken: true}},
		envoy, dir)
	require.NoError(t, err)
	return s
}

func runWorkload(ctx context.Context, s *sidecar, cfg workloadConfig) <-chan error {
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, config{Identity: workloadIdentity, Workload: cfg}) }()
	return done
}

func await(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return")
		return nil
	}
}

func TestServeWorkload_InjectsTokenAndStopsOnSignal(t *testing.T) {
	t.Parallel()
	s := newWorkloadSidecar(t, "sleep 30\n")
	jwt := workloadJWT(t, time.Hour)
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(jwt), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	done := runWorkload(ctx, s, workloadConfig{TokenFile: tokenFile, Audience: "pomerium-egress"})

	bootstrap := filepath.Join(s.configDir, "bootstrap.json")
	require.Eventually(t, func() bool { _, err := os.Stat(bootstrap); return err == nil }, 5*time.Second, 20*time.Millisecond)
	want, err := envoyconfig.RenderRunTokenSecret("Bearer " + jwt)
	require.NoError(t, err)
	got, err := os.ReadFile(s.sdsPath)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
	boot, err := os.ReadFile(bootstrap)
	require.NoError(t, err)
	assert.Contains(t, string(boot), "credential_injector")
	assert.Contains(t, string(boot), `"portValue":9999`)

	cancel()
	assert.NoError(t, await(t, done))
}

func TestServeWorkload_EnvoyExitEndsTheProcess(t *testing.T) {
	t.Parallel()
	s := newWorkloadSidecar(t, "sleep 0.3; exit 3\n")
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(workloadJWT(t, time.Hour)), 0o600))

	err := await(t, runWorkload(context.Background(), s, workloadConfig{TokenFile: tokenFile, Audience: "pomerium-egress"}))
	assert.ErrorContains(t, err, "proxy exited")
}

func TestServeWorkload_TerminalTokenEndsTheProcess(t *testing.T) {
	t.Parallel()
	s := newWorkloadSidecar(t, "sleep 30\n")
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(workloadJWT(t, 7*time.Second)), 0o600))

	done := runWorkload(context.Background(), s, workloadConfig{TokenFile: tokenFile, Audience: "pomerium-egress"})
	require.Eventually(t, func() bool { _, err := os.Stat(s.sdsPath); return err == nil }, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, os.Remove(tokenFile))

	err := await(t, done)
	var te *agentic.TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, agentic.ReasonConfigError, te.Reason)
}

func TestServeWorkload_BadTokenNeverStartsEnvoy(t *testing.T) {
	t.Parallel()
	s := newWorkloadSidecar(t, "sleep 30\n")
	err := await(t, runWorkload(context.Background(), s, workloadConfig{TokenFile: filepath.Join(t.TempDir(), "absent"), Audience: "pomerium-egress"}))
	assert.ErrorContains(t, err, "read projected token")
	_, statErr := os.Stat(filepath.Join(s.configDir, "bootstrap.json"))
	assert.True(t, os.IsNotExist(statErr))
}

func TestServeWorkload_NeedsEndpoints(t *testing.T) {
	t.Parallel()
	s := newWorkloadSidecar(t, "sleep 30\n")
	s.bakedIn = nil
	err := await(t, runWorkload(context.Background(), s, workloadConfig{TokenFile: "/x", Audience: "pomerium-egress"}))
	assert.ErrorContains(t, err, "workload-identity needs at least one SIDECAR_HTTP_* endpoint")
}
