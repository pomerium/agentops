package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pomerium/agentops/internal/sidecar/envoyconfig"
	"github.com/pomerium/agentops/internal/sidecar/server"
)

// fakeBinary writes an executable shell script to a temp dir and returns its
// path. It stands in for the envoy binary in runner tests.
func fakeBinary(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-envoy")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// shortSocketDir returns a short-pathed temp dir. The default per-test temp dir
// on macOS (/var/folders/...) overflows the unix-socket sun_path limit (~104
// bytes); production uses the short /run/sidecar, so this is a test-only quirk.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "smc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// readySocket runs an HTTP server answering /ready on a unix-domain socket
// (mirroring envoy's admin socket) and returns its path.
func readySocket(t *testing.T) string {
	t.Helper()
	path := filepath.Join(shortSocketDir(t), "admin.sock")
	lis, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
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
	return path
}

// unusedSocket returns a unix-socket path with nothing listening on it.
func unusedSocket(t *testing.T) string {
	t.Helper()
	return filepath.Join(shortSocketDir(t), "absent.sock")
}

var testEndpoints = []envoyconfig.Endpoint{
	{Name: "a", ListenPort: 9100, UpstreamURL: "https://upstream.example.com"},
}

func TestEnvoyStartBecomesReadyAndStops(t *testing.T) {
	t.Parallel()
	argsFile := filepath.Join(t.TempDir(), "args")
	envoy := server.NewEnvoy(server.EnvoyConfig{
		Path:        fakeBinary(t, fmt.Sprintf(`echo "$@" > %q; sleep 60`, argsFile)),
		AdminSocket: readySocket(t),
	})

	proc, err := envoy.Start(context.Background(), testEndpoints)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The fake envoy must have been given a config file containing our
	// listener. Start can return before the shell script reaches echo (the
	// ready server here is independent of the process), so poll briefly.
	var args []byte
	deadline := time.Now().Add(5 * time.Second)
	for {
		args, err = os.ReadFile(argsFile)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	fields := strings.Fields(string(args))
	cfgPath := ""
	for i, f := range fields {
		if f == "-c" && i+1 < len(fields) {
			cfgPath = fields[i+1]
		}
	}
	if cfgPath == "" {
		t.Fatalf("fake envoy not invoked with -c <config>: %q", args)
	}
	cfg, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(cfg), "9100") || !strings.Contains(string(cfg), "upstream.example.com") {
		t.Errorf("bootstrap config missing endpoint data")
	}

	proc.Stop()
	select {
	case <-proc.Exited():
	case <-time.After(10 * time.Second):
		t.Fatal("process did not exit after Stop")
	}
}

func TestEnvoyStartFailsWhenProcessExitsEarly(t *testing.T) {
	t.Parallel()
	envoy := server.NewEnvoy(server.EnvoyConfig{
		Path:         fakeBinary(t, "exit 3"),
		AdminSocket:  unusedSocket(t), // never becomes ready; exit must win
		ReadyTimeout: 5 * time.Second,
	})
	if _, err := envoy.Start(context.Background(), testEndpoints); err == nil {
		t.Fatal("Start succeeded although the process exited immediately")
	}
}

func TestEnvoyStartFailsWhenNeverReady(t *testing.T) {
	t.Parallel()
	envoy := server.NewEnvoy(server.EnvoyConfig{
		Path:         fakeBinary(t, "sleep 60"),
		AdminSocket:  unusedSocket(t),
		ReadyTimeout: time.Second,
	})
	if _, err := envoy.Start(context.Background(), testEndpoints); err == nil {
		t.Fatal("Start succeeded although the admin endpoint never became ready")
	}
}

func TestEnvoyStartRejectsBadEndpoints(t *testing.T) {
	t.Parallel()
	envoy := server.NewEnvoy(server.EnvoyConfig{Path: "/nonexistent"})
	_, err := envoy.Start(context.Background(), []envoyconfig.Endpoint{
		{Name: "a", ListenPort: 9100, UpstreamURL: "ftp://nope"},
	})
	if err == nil {
		t.Fatal("Start succeeded with an invalid endpoint")
	}
}
