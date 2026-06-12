package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/pomerium/agentops/internal/sidecar/envoyconfig"
)

// EnvoyConfig configures the envoy subprocess runner.
type EnvoyConfig struct {
	// Path to the envoy binary.
	Path string
	// AdminSocket is the filesystem path of envoy's admin unix-domain socket,
	// used for the readiness poll. It must live on the sidecar's own filesystem
	// (not shared with the agent container). Defaults to <ConfigDir>/admin.sock.
	AdminSocket string
	// ReadyTimeout bounds how long Start waits for envoy's admin /ready.
	// Defaults to 15s.
	ReadyTimeout time.Duration
	// PollInterval is the readiness poll cadence. Defaults to 100ms.
	PollInterval time.Duration
	// ConfigDir is where the bootstrap file is written. Defaults to a fresh
	// temp directory.
	ConfigDir string
	// LogLevel is envoy's --log-level. Defaults to "warn".
	LogLevel string
	// Logger receives envoy output, line by line, on stderr.
	Logger *slog.Logger
}

// Envoy runs an envoy subprocess from a statically generated bootstrap. It
// implements Proxy.
type Envoy struct {
	cfg EnvoyConfig
	log *slog.Logger
}

// NewEnvoy constructs an Envoy runner.
func NewEnvoy(cfg EnvoyConfig) *Envoy {
	if cfg.ReadyTimeout == 0 {
		cfg.ReadyTimeout = 15 * time.Second
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "warn"
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Envoy{cfg: cfg, log: log}
}

type envoyProcess struct {
	cmd    *exec.Cmd
	exited chan error
}

func (p *envoyProcess) Exited() <-chan error { return p.exited }

func (p *envoyProcess) Stop() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
}

// Start renders the bootstrap, launches envoy, and waits until its admin
// endpoint reports ready (or the process dies / the timeout elapses).
func (e *Envoy) Start(ctx context.Context, endpoints []envoyconfig.Endpoint) (Process, error) {
	dir := e.cfg.ConfigDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "sidecar-envoy")
		if err != nil {
			return nil, fmt.Errorf("create config dir: %w", err)
		}
	}
	// The admin socket lives on the sidecar's own filesystem so the agent
	// container (which shares only the network namespace) cannot reach it.
	adminSocket := e.cfg.AdminSocket
	if adminSocket == "" {
		adminSocket = filepath.Join(dir, "admin.sock")
	}

	bootstrap, err := envoyconfig.BuildBootstrap(endpoints, adminSocket)
	if err != nil {
		return nil, fmt.Errorf("build bootstrap: %w", err)
	}
	data, err := protojson.Marshal(bootstrap)
	if err != nil {
		return nil, fmt.Errorf("marshal bootstrap: %w", err)
	}

	cfgPath := filepath.Join(dir, "bootstrap.json")
	if err := os.WriteFile(cfgPath, data, 0o600); err != nil {
		return nil, fmt.Errorf("write bootstrap: %w", err)
	}

	// Envoy writes its application log to stderr by default; route stdout
	// there too — our stdout carries the gRPC control stream.
	cmd := exec.Command(e.cfg.Path, "-c", cfgPath, "--log-level", e.cfg.LogLevel)
	stdout := newLineWriter(e.log, "stdout")
	stderr := newLineWriter(e.log, "stderr")
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Don't let descendants holding the output pipes open block Wait after
	// the process itself has died.
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start envoy: %w", err)
	}
	proc := &envoyProcess{cmd: cmd, exited: make(chan error, 1)}
	go func() {
		err := cmd.Wait()
		// Wait returns only after exec's output-copy goroutines finish, so the
		// writers are quiescent here; flush any trailing unterminated line.
		stdout.Flush()
		stderr.Flush()
		proc.exited <- err
	}()

	if err := e.awaitReady(ctx, proc, adminSocket); err != nil {
		proc.Stop()
		return nil, err
	}
	return proc, nil
}

func (e *Envoy) awaitReady(ctx context.Context, proc *envoyProcess, adminSocket string) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.ReadyTimeout)
	defer cancel()

	// Dial the admin unix-domain socket; the URL host is ignored.
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", adminSocket)
		},
	}}
	defer client.CloseIdleConnections()
	const url = "http://admin/ready"
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case exitErr := <-proc.exited:
			// Re-deliver for any later Exited() reader, then fail Start.
			proc.exited <- exitErr
			return fmt.Errorf("envoy exited before becoming ready: %w", exitErr)
		case <-ctx.Done():
			return fmt.Errorf("envoy not ready within %s: %w", e.cfg.ReadyTimeout, ctx.Err())
		case <-ticker.C:
		}
	}
}

// maxLogLine bounds how many bytes lineWriter buffers for a single
// newline-terminated line. A longer line is logged truncated and the rest
// discarded until the next newline, so a pathological newline-free stream
// can't grow the buffer without bound or block the writer.
const maxLogLine = 1 << 20 // 1 MiB

// lineWriter is an io.Writer that logs its input one line at a time. It writes
// synchronously (no background goroutine, no pipe) so it can never leak a
// goroutine or block the exec output copier, and it bounds the per-line buffer.
type lineWriter struct {
	log      *slog.Logger
	stream   string
	mu       sync.Mutex
	buf      []byte
	dropping bool // discarding the tail of an overlong line until the next '\n'
}

func newLineWriter(log *slog.Logger, stream string) *lineWriter {
	return &lineWriter{log: log, stream: stream}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	for {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			w.append(p)
			return n, nil
		}
		w.append(p[:i])
		w.emitLocked()
		p = p[i+1:]
	}
}

// append adds a chunk to the current line buffer, dropping bytes once the line
// exceeds maxLogLine (and remembering to keep dropping until the next newline).
func (w *lineWriter) append(p []byte) {
	if w.dropping {
		return
	}
	if len(w.buf)+len(p) > maxLogLine {
		take := maxLogLine - len(w.buf)
		w.buf = append(w.buf, p[:take]...)
		w.dropping = true
		return
	}
	w.buf = append(w.buf, p...)
}

// emitLocked logs the buffered line (if any) and resets for the next one.
func (w *lineWriter) emitLocked() {
	if len(w.buf) > 0 || w.dropping {
		line := string(w.buf)
		if w.dropping {
			line += "…(truncated)"
		}
		w.log.Info("envoy", "stream", w.stream, "line", line)
	}
	w.buf = w.buf[:0]
	w.dropping = false
}

// Flush logs any buffered trailing line that was never newline-terminated.
func (w *lineWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.emitLocked()
}

// buffered reports the current buffered byte count (used in tests).
func (w *lineWriter) buffered() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buf)
}
