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

	"github.com/pomerium/agentops/harness/internal/sidecar/envoyconfig"
)

type EnvoyConfig struct {
	Path            string
	AdminSocket     string
	ReadyTimeout    time.Duration
	PollInterval    time.Duration
	ConfigDir       string
	RunTokenSDSPath string
	RunTokenCAFile  string
	LogLevel        string
	Logger          *slog.Logger
}

type Envoy struct {
	cfg EnvoyConfig
	log *slog.Logger
}

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

func (e *Envoy) Start(ctx context.Context, endpoints []envoyconfig.Endpoint) (Process, error) {
	dir := e.cfg.ConfigDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "sidecar-envoy")
		if err != nil {
			return nil, fmt.Errorf("create config dir: %w", err)
		}
	}
	adminSocket := e.cfg.AdminSocket
	if adminSocket == "" {
		adminSocket = filepath.Join(dir, "admin.sock")
	}

	bootstrap, err := envoyconfig.BuildBootstrap(endpoints, adminSocket, e.cfg.RunTokenSDSPath, e.cfg.RunTokenCAFile)
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

	cmd := exec.Command(e.cfg.Path, "-c", cfgPath, "--log-level", e.cfg.LogLevel)
	stdout := newLineWriter(e.log, "stdout")
	stderr := newLineWriter(e.log, "stderr")
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start envoy: %w", err)
	}
	proc := &envoyProcess{cmd: cmd, exited: make(chan error, 1)}
	go func() {
		err := cmd.Wait()
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
			proc.exited <- exitErr
			return fmt.Errorf("envoy exited before becoming ready: %w", exitErr)
		case <-ctx.Done():
			return fmt.Errorf("envoy not ready within %s: %w", e.cfg.ReadyTimeout, ctx.Err())
		case <-ticker.C:
		}
	}
}

const maxLogLine = 1 << 20

type lineWriter struct {
	log      *slog.Logger
	stream   string
	mu       sync.Mutex
	buf      []byte
	dropping bool
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

func (w *lineWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.emitLocked()
}
