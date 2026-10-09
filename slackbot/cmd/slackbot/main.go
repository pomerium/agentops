package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/slack-go/slack"
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
	botUserID, homeTeamID, err := botIdentity(ctx, poster.BotIdentity, log)
	if err != nil {
		return fmt.Errorf("determine the bot's Slack identity with auth.test: %w", err)
	}
	log.Info("resolved bot identity", "bot_user_id", botUserID, "home_team_id", homeTeamID)
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
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := gw.Wait(shutdownCtx); err != nil {
		log.Warn("shutting down with Slack actions still running", "err", err)
	}
	return nil
}

const identityLookupTimeout = 15 * time.Second

var rejectedTokens = []string{"invalid_auth", "not_authed", "account_inactive", "token_revoked", "token_expired"}

func botIdentity(ctx context.Context, lookup func(context.Context) (string, string, error), log *slog.Logger, opts ...backoff.RetryOption) (string, string, error) {
	type identity struct{ user, team string }
	id, err := backoff.Retry(ctx, func() (identity, error) {
		attempt, cancel := context.WithTimeout(ctx, identityLookupTimeout)
		defer cancel()
		user, team, err := lookup(attempt)
		var rejected slack.SlackErrorResponse
		switch {
		case errors.As(err, &rejected) && slices.Contains(rejectedTokens, rejected.Err):
			return identity{}, backoff.Permanent(err)
		case err != nil:
			return identity{}, err
		case user == "":
			return identity{}, errors.New("auth.test returned no bot user ID")
		}
		return identity{user: user, team: team}, nil
	}, append([]backoff.RetryOption{
		backoff.WithMaxElapsedTime(2 * time.Minute),
		backoff.WithNotify(func(err error, next time.Duration) {
			log.Warn("could not determine the bot's Slack identity; retrying", "retry_in", next, "err", err)
		}),
	}, opts...)...)
	return id.user, id.team, err
}
