// Command agentops is a stateful Slack bot that runs CRD-defined
// agentic workflows in agent-sandbox pods, brokering per-user, per-MCP-server
// OAuth credentials so each run gets exactly the MCP tokens it needs.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/agenttemplate"
	"github.com/pomerium/agentops/internal/channels/slack/client"
	"github.com/pomerium/agentops/internal/channels/slack/gateway"
	"github.com/pomerium/agentops/internal/chatops/session"
	"github.com/pomerium/agentops/internal/chatops/store"
	"github.com/pomerium/agentops/internal/config"
	"github.com/pomerium/agentops/internal/mcpbroker"
	"github.com/pomerium/agentops/internal/sandbox"
)

func main() {
	// Bootstrap at Info so config errors are visible; the real handler (at the
	// configured LOG_LEVEL) is installed once config is loaded, in run().
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

	// Install the real handler at the configured level (LOG_LEVEL). Set
	// LOG_LEVEL=debug to see the full HTTP + sandbox/ACP comms trace.
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel})
	log = slog.New(handler)
	slog.SetDefault(log)
	// Route controller-runtime's global logr logger into slog (avoids its
	// "log.SetLogger(...) was never called" warning and structures its output).
	ctrllog.SetLogger(logr.FromSlogHandler(handler))

	log.Info("starting agentops",
		"namespace", cfg.Namespace, "http_addr", cfg.HTTPAddr,
		"log_level", cfg.LogLevel.String())

	// Persistence.
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	// Kubernetes config (in-cluster or kubeconfig).
	restCfg, err := ctrlconfig.GetConfig()
	if err != nil {
		return err
	}

	// Controller-runtime cache over the agents.pomerium.com resources the app
	// reads (AgentTemplates and the ChannelConfig singleton), scoped to the
	// namespace.
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	crCache, err := cache.New(restCfg, cache.Options{
		Scheme:            scheme,
		DefaultNamespaces: map[string]cache.Config{cfg.Namespace: {}},
	})
	if err != nil {
		return err
	}
	go func() {
		if err := crCache.Start(ctx); err != nil {
			log.Error("agents.pomerium.com resource cache stopped", "err", err)
		}
	}()
	// Informers start lazily on first read; create both here so a missing CRD
	// or RBAC rule surfaces at boot instead of on the first mention. Non-fatal:
	// a rollout can start this pod before the new CRD is applied, and the cache
	// retries lazily on first use — so log loudly rather than crash-loop.
	if _, err := crCache.GetInformer(ctx, &v1alpha1.AgentTemplate{}); err != nil {
		log.Error("agent template informer failed to start; will retry on first use", "err", err)
	}
	if _, err := crCache.GetInformer(ctx, &v1alpha1.ChannelConfig{}); err != nil {
		log.Error("channel config informer failed to start; will retry on first use", "err", err)
	}
	if !crCache.WaitForCacheSync(ctx) {
		return errors.New("agents.pomerium.com resource cache failed to sync")
	}

	registry := agenttemplate.New(crCache, cfg.Namespace)

	// MCP credential broker.
	broker := mcpbroker.New(st, mcpbroker.Options{
		RedirectBaseURL: cfg.OAuthRedirectBaseURL,
		ClientName:      "agentops",
		Logger:          log,
	})

	// Sandbox orchestrator.
	claimClient, err := sandbox.NewClaimClient(restCfg, cfg.Namespace)
	if err != nil {
		return err
	}
	podExec, err := sandbox.NewPodExecutor(restCfg, log)
	if err != nil {
		return err
	}
	orch := sandbox.New(claimClient, podExec, sandbox.Config{
		Namespace:        cfg.Namespace,
		AgentCommand:     cfg.AgentCommand,
		AgentContainer:   cfg.AgentContainer,
		SidecarCommand:   cfg.SidecarCommand,
		SidecarContainer: cfg.SidecarContainer,
	}, log)

	// Application brain + Slack poster.
	poster := client.NewPoster(cfg.SlackBotToken)
	// Discover the bot's own user id so app_mention text can have the bot's
	// mention stripped before parsing the template name. Non-fatal: an empty id
	// falls back to stripping a leading mention.
	botUserID, err := poster.BotUserID(ctx)
	if err != nil {
		log.Warn("could not determine bot user id via auth.test; mentions in messages can't be detected", "err", err)
	} else {
		log.Info("resolved bot user id", "bot_user_id", botUserID)
	}
	// All app posting goes through the rate limiter: guaranteed delivery for
	// lifecycle/turn output, last-one-wins coalescing for in-progress message
	// replacements (paced by AGENT_STREAM_INTERVAL).
	limits := client.DefaultLimits()
	if cfg.StreamInterval > 0 {
		limits.UpdateStream = rate.Every(cfg.StreamInterval)
	}
	mgr := session.NewManager(session.Config{
		Namespace:  cfg.Namespace,
		SessionTTL: cfg.SessionTTL,
	}, st, broker, registry, session.NewOrchestratorLauncher(orch), client.NewLimitedWith(poster, limits, log), log)

	// Bring persisted state in line with reality (live ACP sessions don't
	// survive a restart), then sweep expired sessions/flows periodically.
	mgr.ReconcileOnStartup(ctx)
	go runSweeper(ctx, mgr, log)

	// HTTP gateway.
	gw := gateway.New(gateway.Config{SigningSecret: cfg.SlackSigningSecret, BotUserID: botUserID}, mgr, log)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// runSweeper periodically ends timed-out sessions and purges expired OAuth
// flows until ctx is cancelled.
func runSweeper(ctx context.Context, mgr *session.Manager, log *slog.Logger) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mgr.SweepExpired(ctx)
		}
	}
}
