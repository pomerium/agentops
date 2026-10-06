package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/sidecar/agentic"
	"github.com/pomerium/agentops/harness/internal/sidecar/envoyconfig"
	"github.com/pomerium/agentops/harness/internal/sidecar/envparse"
	"github.com/pomerium/agentops/harness/internal/sidecar/harnessclient"
	"github.com/pomerium/agentops/harness/internal/sidecar/server"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	if len(os.Args) != 3 || os.Args[1] != "serve" {
		fmt.Fprintf(os.Stderr, "usage: %s serve %s|%s\n", os.Args[0], userIdentity, workloadIdentity)
		os.Exit(2)
	}
	if err := serve(log, identity(os.Args[2])); err != nil {
		log.Error("sidecar serve failed", "err", err)
		os.Exit(1)
	}
}

type sidecar struct {
	log       *slog.Logger
	bakedIn   []envoyconfig.Endpoint
	envoyPath string
	configDir string
	sdsPath   string
}

func newSidecar(log *slog.Logger, bakedIn []envoyconfig.Endpoint, envoyPath, configDir string) (*sidecar, error) {
	sdsDir := filepath.Join(configDir, "sds")
	if err := os.MkdirAll(sdsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create SDS directory: %w", err)
	}
	return &sidecar{
		log:       log,
		bakedIn:   bakedIn,
		envoyPath: envoyPath,
		configDir: configDir,
		sdsPath:   filepath.Join(sdsDir, "run_token.yaml"),
	}, nil
}

func (s *sidecar) sink(t *agentic.Token) error {
	return envoyconfig.WriteRunTokenSecret(s.sdsPath, t.Bearer)
}

func (s *sidecar) proxy(caFile string) *server.Envoy {
	return server.NewEnvoy(server.EnvoyConfig{
		Path:            s.envoyPath,
		ConfigDir:       s.configDir,
		RunTokenSDSPath: s.sdsPath,
		RunTokenCAFile:  caFile,
		LogLevel:        os.Getenv("SIDECAR_ENVOY_LOG_LEVEL"),
		Logger:          s.log,
	})
}

func awaitToken(ctx context.Context, loop *agentic.Loop) (<-chan error, <-chan struct{}, error) {
	loopErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		loopErr <- loop.Run(ctx)
	}()
	select {
	case <-loop.Ready():
		return loopErr, done, nil
	case err := <-loopErr:
		return nil, done, err
	case <-ctx.Done():
		return nil, done, ctx.Err()
	}
}

func (s *sidecar) tokenLoop(cfg config) (*agentic.Loop, string, error) {
	if !cfg.managed() {
		w := cfg.Workload
		poller, err := agentic.NewFilePoller(agentic.FilePollerConfig{TokenFile: w.TokenFile, Audience: w.Audience})
		if err != nil {
			return nil, "", fmt.Errorf("build workload-token poller: %w", err)
		}
		loop := agentic.NewLoop(agentic.LoopConfig{Poll: poller, Sink: s.sink, Label: "workload token", Logger: s.log})
		return loop, w.CAFile, nil
	}

	u := cfg.User
	poller, err := agentic.NewHTTPPoller(agentic.HTTPPollerConfig{
		BaseURL:   u.ASURL,
		DialAddr:  u.ASDialAddr,
		TokenFile: u.TokenFile,
		CAFile:    u.ASCAFile,
		Logger:    s.log,
	})
	if err != nil {
		return nil, "", fmt.Errorf("build run-token poller: %w", err)
	}
	return agentic.NewLoop(agentic.LoopConfig{
		Poll:             poller,
		Sink:             s.sink,
		RotationInterval: u.Rotation,
		Logger:           s.log,
	}), u.ASCAFile, nil
}

func serve(log *slog.Logger, id identity) error {
	parsed, err := envparse.Parse(os.Environ())
	if err != nil {
		return fmt.Errorf("parse SIDECAR_HTTP_* env: %w", err)
	}
	cfg, err := loadConfig(id, os.Getenv)
	if err != nil {
		return err
	}

	configDir := os.Getenv("SIDECAR_CONFIG_DIR")
	if configDir == "" {
		if configDir, err = os.MkdirTemp("", "sidecar"); err != nil {
			return fmt.Errorf("create config dir: %w", err)
		}
	}
	envoyPath := cmp.Or(os.Getenv("SIDECAR_ENVOY_PATH"), "/usr/local/bin/envoy")
	s, err := newSidecar(log, server.EndpointsFromEnv(parsed), envoyPath, configDir)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log.Info("sidecar: starting", "identity", cfg.Identity, "baked_in_endpoints", len(s.bakedIn))
	err = s.serve(ctx, cfg)
	if ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *sidecar) serve(ctx context.Context, cfg config) error {
	log := s.log
	if !cfg.managed() && len(s.bakedIn) == 0 {
		return errors.New(string(workloadIdentity) + " needs at least one SIDECAR_HTTP_* endpoint: nothing else would ever start")
	}
	loop, caFile, err := s.tokenLoop(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	loopErr, loopDone, err := awaitToken(ctx, loop)
	defer func() {
		cancel()
		<-loopDone
	}()
	if err != nil {
		return err
	}

	proxy := s.proxy(caFile)
	started := make(chan server.Process, 1)
	var proxyMu sync.Mutex
	var running server.Process
	proxyStopped := false
	defer func() {
		cancel()
		proxyMu.Lock()
		defer proxyMu.Unlock()
		proxyStopped = true
		if running != nil {
			running.Stop()
		}
	}()
	startProxy := func(endpoints []envoyconfig.Endpoint) error {
		proxyMu.Lock()
		defer proxyMu.Unlock()
		if proxyStopped {
			return errors.New("start proxy: the sidecar is shutting down")
		}
		proc, err := proxy.Start(ctx, endpoints)
		if err != nil {
			return fmt.Errorf("start proxy: %w", err)
		}
		running = proc
		started <- proc
		log.Info("sidecar: proxy ready", "endpoints", len(endpoints))
		return nil
	}

	var client *harnessclient.Client
	var attachErr chan error
	if cfg.managed() {
		u := cfg.User
		agentRunner, err := harnessclient.NewUDSRunner(u.RunnerSocket, log)
		if err != nil {
			return err
		}
		defer agentRunner.Close()

		client, err = harnessclient.New(harnessclient.Config{
			URL:         u.HarnessURL,
			DialAddress: u.HarnessDialAddr,
			CAFile:      u.HarnessCAFile,
			Insecure:    u.HarnessInsecure,
			Token:       loop,
			Runner:      agentRunner,
			Logger:      log,
			Configure: func(sc *agentlinkpb.SandboxConfig) error {
				endpoints, err := server.Endpoints(s.bakedIn, sessionEndpoints(sc))
				if err != nil {
					return fmt.Errorf("sidecar endpoints: %w", err)
				}
				return startProxy(endpoints)
			},
		})
		if err != nil {
			return err
		}
		defer client.Close()

		attachErr = make(chan error, 1)
		go func() { attachErr <- client.Run(ctx) }()
		log.Info("sidecar: attaching to the agent link",
			"target", harnessclient.DialTargetFor(u.HarnessURL, u.HarnessDialAddr),
			"runner_socket", u.RunnerSocket)
	} else if err := startProxy(s.bakedIn); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	fail := func(reason string) {
		if client != nil {
			client.Fail(reason)
			awaitAttach(attachErr, log)
		}
	}

	var proxyExited <-chan error
	for {
		select {
		case proc := <-started:
			proxyExited = proc.Exited()
		case exitErr := <-proxyExited:
			log.Error("sidecar: envoy exited", "err", exitErr)
			fail("envoy_exited")
			return fmt.Errorf("proxy exited: %w", exitErr)
		case err := <-loopErr:
			if ctx.Err() != nil {
				return nil
			}
			log.Error("sidecar: token terminal", "err", err)
			fail("token_terminal:" + reasonOf(err))
			return err
		case err := <-attachErr:
			if err == nil {
				log.Info("sidecar: session ended cleanly")
				return nil
			}
			return err
		case <-ctx.Done():
			log.Info("sidecar: signal received; shutting down")
			return nil
		}
	}
}

func sessionEndpoints(cfg *agentlinkpb.SandboxConfig) []envoyconfig.Endpoint {
	eps := cfg.GetEndpoints()
	out := make([]envoyconfig.Endpoint, 0, len(eps))
	for _, ep := range eps {
		out = append(out, envoyconfig.Endpoint{
			Name:           ep.GetName(),
			ListenPort:     ep.GetListenPort(),
			UpstreamURL:    ep.GetUpstreamUrl(),
			DialAddress:    ep.GetDialAddress(),
			InjectRunToken: true,
		})
	}
	return out
}

func awaitAttach(attachErr <-chan error, log *slog.Logger) {
	select {
	case err := <-attachErr:
		log.Debug("sidecar: attach client finished", "err", err)
	case <-time.After(5 * time.Second):
		log.Warn("sidecar: attach client did not report the terminal status in time")
	}
}

func reasonOf(err error) string {
	var te *agentic.TerminalError
	if errors.As(err, &te) {
		return te.Reason
	}
	return "unknown"
}
