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

	"golang.org/x/time/rate"

	"github.com/pomerium/agentops/harness/api/client"
	slackapp "github.com/pomerium/agentops/slackbot/internal/app"
	"github.com/pomerium/agentops/slackbot/internal/channelmap"
	"github.com/pomerium/agentops/slackbot/internal/config"
	"github.com/pomerium/agentops/slackbot/internal/gateway"
	slackclient "github.com/pomerium/agentops/slackbot/internal/slackclient"
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
	log = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)
	log.Info("starting slackbot",
		"harness_api", cfg.Slack.HarnessAPIURL, "http_addr", cfg.Slack.HTTPAddr,
		"log_level", cfg.LogLevel.String())

	platform, err := client.New(cfg.Slack.HarnessAPIURL,
		client.WithTokenFile(cfg.Slack.HarnessAPITokenFile),
		client.WithDialAddress(cfg.Slack.HarnessAPIDialAddress),
		client.WithCAFile(cfg.Slack.HarnessAPICAFile),
	)
	if err != nil {
		return err
	}

	poster := slackclient.NewPoster(cfg.Slack.BotToken)
	botUserID, homeTeamID, err := poster.BotIdentity(ctx)
	if err != nil {
		log.Warn("could not determine bot identity via auth.test; mentions in messages can't be detected", "err", err)
	} else {
		log.Info("resolved bot identity", "bot_user_id", botUserID, "home_team_id", homeTeamID)
	}
	limits := slackclient.DefaultLimits()
	if cfg.Slack.StreamInterval > 0 {
		limits.UpdateStream = rate.Every(cfg.Slack.StreamInterval)
	}

	channels, err := channelmap.New(cfg.Slack.ChannelMapPath, log)
	if err != nil {
		return err
	}
	go channels.Watch(ctx, cfg.Slack.ChannelMapInterval)

	app := slackapp.New(platform, slackclient.NewLimitedWith(poster, limits, log), channels,
		slackapp.WithBotUserID(botUserID),
		slackapp.WithHomeTeamID(homeTeamID),
		slackapp.WithLogger(log),
	)
	defer app.Shutdown()

	go func() {
		app.ReconcileOnStartup(ctx)
		app.RunSweeper(ctx, slackapp.DefaultSweepInterval)
	}()

	gw := gateway.New(cfg.Slack.SigningSecret, app, gateway.WithBotUserID(botUserID), gateway.WithLogger(log))
	srv := &http.Server{
		Addr:              cfg.Slack.HTTPAddr,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.Slack.HTTPAddr)
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
