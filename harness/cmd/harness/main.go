package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/agenttemplate"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/config"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/sessionstore/sqlite"
)

func main() {
	bootstrap := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(bootstrap)

	if err := run(bootstrap); err != nil {
		slog.Default().Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel})
	log = slog.New(handler)
	slog.SetDefault(log)

	ctrllog.SetLogger(logr.FromSlogHandler(handler))

	log.Info("starting harness",
		"namespace", cfg.Harness.Namespace, "api_addr", cfg.Harness.APIAddr,
		"grpc_addr", cfg.Harness.GRPCAddr, "log_level", cfg.LogLevel.String())

	st, err := sqlite.Open(ctx, cfg.Harness.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	restCfg, err := ctrlconfig.GetConfig()
	if err != nil {
		return err
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	crCache, err := cache.New(restCfg, cache.Options{
		Scheme:            scheme,
		DefaultNamespaces: map[string]cache.Config{cfg.Harness.Namespace: {}},
	})
	if err != nil {
		return err
	}
	go func() {
		if err := crCache.Start(ctx); err != nil {
			log.Error("agents.pomerium.com resource cache stopped", "err", err)
		}
	}()

	if _, err := crCache.GetInformer(ctx, &v1alpha1.AgentTemplate{}); err != nil {
		log.Error("agent template informer failed to start; will retry on first use", "err", err)
	}
	if _, err := crCache.GetInformer(ctx, &v1alpha1.ClientBinding{}); err != nil {
		log.Error("client binding informer failed to start; will retry on first use", "err", err)
	}
	if !crCache.WaitForCacheSync(ctx) {
		return errors.New("agents.pomerium.com resource cache failed to sync")
	}

	registry := agenttemplate.New(crCache, cfg.Harness.Namespace)

	verifier, err := agentlink.NewVerifier(cfg.Harness.AssertionIssuer,
		agentlink.WithAudience(cfg.Harness.AssertionAudience),
		agentlink.WithJWKSURL(cfg.Harness.AssertionJWKSURL),
		agentlink.WithDialAddress(cfg.Harness.AssertionDialAddress),
		agentlink.WithCAFile(cfg.Harness.AssertionCAFile),
		agentlink.WithVerifierLogger(log),
	)
	if err != nil {
		return err
	}
	linkSrv, err := agentlink.New(verifier,
		agentlink.WithHeartbeatInterval(cfg.Harness.HeartbeatInterval),
		agentlink.WithHeartbeatMissLimit(cfg.Harness.HeartbeatMissLimit),
		agentlink.WithLogger(log),
	)
	if err != nil {
		return err
	}
	if advisory := agentlink.BindAdvisory(cfg.Harness.GRPCAddr); advisory != "" {
		log.Warn(advisory)
	}
	stopAgentLink, err := serveAgentLink(linkSrv, cfg.Harness.GRPCAddr, log)
	if err != nil {
		return err
	}
	defer stopAgentLink()

	claimClient, err := sandbox.NewClaimClient(restCfg, cfg.Harness.Namespace)
	if err != nil {
		return err
	}

	podGetter, err := sandbox.NewPodGetter(restCfg, cfg.Harness.Namespace)
	if err != nil {
		return err
	}

	sandboxClient, err := sandbox.NewSandboxClient(restCfg, cfg.Harness.Namespace)
	if err != nil {
		return err
	}
	orch := sandbox.New(claimClient, podGetter, sandboxClient, sandbox.NewAgentLink(linkSrv),
		sandbox.WithNamespace(cfg.Harness.Namespace),
		sandbox.WithHarnessRoute(cfg.Harness.ExternalURL),
		sandbox.WithAttachGrace(cfg.Harness.AttachGrace),
		sandbox.WithAttachWarnAfter(cfg.Harness.AttachWarnAfter),

		sandbox.WithLease(cfg.Harness.SandboxLease),

		sandbox.WithAttachTimeout(cfg.Harness.AgenticRunTTL+2*time.Minute),
		sandbox.WithLogger(log),
	)

	var runOpts []agenticrun.Option
	if cfg.Harness.AgenticCAFile != "" {
		runOpts = append(runOpts, agenticrun.WithCAFile(cfg.Harness.AgenticCAFile))
	}
	runClient, err := agenticrun.NewHTTPRunClient(cfg.Harness.AgenticASURL, cfg.Harness.AgenticASDialAddress, cfg.Harness.AgenticTokenFile, runOpts...)
	if err != nil {
		return err
	}

	api := harnessapi.New(st, harnessapi.NewEventLog(st), harnessapi.NewOrchestratorLauncher(orch), registry, runClient,
		harnessapi.WithSessionTTL(cfg.Harness.SessionTTL),
		harnessapi.WithSessionIdleTTL(cfg.Harness.SessionIdleTTL),
		harnessapi.WithIdleWarnLead(cfg.Harness.SessionIdleWarnLead),
		harnessapi.WithSuspendedTTL(cfg.Harness.SuspendedTTL),
		harnessapi.WithRunTTL(cfg.Harness.AgenticRunTTL),
		harnessapi.WithLogger(log),
	)

	reconciled := api.ReconcileOnStartup(ctx)
	go func() {
		<-reconciled
		runSweeper(ctx, api)
	}()

	stopAPI, err := serveClientAPI(ctx, cfg, api, st, crCache, log)
	if err != nil {
		return err
	}
	defer stopAPI()

	<-ctx.Done()
	log.Info("shutting down")
	return nil
}

func serveClientAPI(ctx context.Context, cfg config.Config, impl *harnessapi.Service,
	st sessionstore.Store, cache cache.Cache, log *slog.Logger,
) (func(), error) {
	verifier, err := agentlink.NewVerifier(cfg.Harness.APIAssertionIssuer,
		agentlink.WithAudience(cfg.Harness.APIAssertionAudience),
		agentlink.WithJWKSURL(cfg.Harness.APIAssertionJWKSURL),
		agentlink.WithDialAddress(cfg.Harness.APIAssertionDialAddress),
		agentlink.WithCAFile(cfg.Harness.APIAssertionCAFile),
		agentlink.WithVerifierLogger(log),
	)
	if err != nil {
		return nil, err
	}
	metrics := apiserver.NewMetrics()
	srv, err := apiserver.New(impl, assertionIdentity(verifier),
		apiserver.WithReady(func(ctx context.Context) error {
			if err := st.Ping(ctx); err != nil {
				return fmt.Errorf("session store: %w", err)
			}

			if !cache.WaitForCacheSync(ctx) {
				return errors.New("the agents.pomerium.com cache is not synced")
			}
			return nil
		}),
		apiserver.WithMetrics(metrics),
		apiserver.WithLogger(log),
	)
	if err != nil {
		return nil, err
	}

	apiSrv := srv.HTTPServer(cfg.Harness.APIAddr)
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", metrics.Handler())
	adminSrv := &http.Server{
		Addr:              cfg.Harness.AdminAddr,
		Handler:           adminMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	servers := []struct {
		name string
		srv  *http.Server
		lis  net.Listener
	}{{name: "harness client API", srv: apiSrv}, {name: "harness admin", srv: adminSrv}}
	for i := range servers {
		lis, err := net.Listen("tcp", servers[i].srv.Addr)
		if err != nil {
			for _, s := range servers[:i] {
				_ = s.lis.Close()
			}
			return nil, fmt.Errorf("listen for the %s on %s: %w", servers[i].name, servers[i].srv.Addr, err)
		}
		servers[i].lis = lis
	}
	for _, s := range servers {
		go func() {
			log.Info(s.name+" listening", "addr", s.lis.Addr().String())
			if err := s.srv.Serve(s.lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error(s.name+" stopped", "err", err)
			}
		}()
	}
	return func() {
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = apiSrv.Shutdown(shutdown)
		_ = adminSrv.Shutdown(shutdown)
	}, nil
}

func serveAgentLink(srv *agentlink.Server, addr string, log *slog.Logger) (func(), error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for the agent link on %s: %w", addr, err)
	}
	minTime, permitWithoutStream := agentlink.KeepaliveEnforcement()
	gs := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             minTime,
		PermitWithoutStream: permitWithoutStream,
	}))
	srv.Register(gs)
	go func() {
		log.Info("agent link listening", "addr", addr)
		if err := gs.Serve(lis); err != nil {
			log.Error("agent link stopped", "err", err)
		}
	}()
	return func() {
		gs.Stop()
	}, nil
}

const sweepInterval = time.Minute

func runSweeper(ctx context.Context, api *harnessapi.Service) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			api.SweepExpired(ctx)
		}
	}
}
