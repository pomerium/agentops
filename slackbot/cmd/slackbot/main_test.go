package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/cenkalti/backoff/v7"
	"github.com/slack-go/slack"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestBotIdentityRetriesUntilSlackAnswers(t *testing.T) {
	calls := 0
	lookup := func(context.Context) (string, string, error) {
		calls++
		if calls < 3 {
			return "", "", errors.New("connection reset")
		}
		return "UBOT", "T1", nil
	}
	user, team, err := botIdentity(context.Background(), lookup, quiet, backoff.WithBackOff(&backoff.ZeroBackOff{}))
	if err != nil || user != "UBOT" || team != "T1" {
		t.Fatalf("got %q/%q/%v after %d calls, want UBOT/T1", user, team, err, calls)
	}
}

func TestBotIdentityGivesUpOnARejectedToken(t *testing.T) {
	calls := 0
	lookup := func(context.Context) (string, string, error) {
		calls++
		return "", "", slack.SlackErrorResponse{Err: "invalid_auth"}
	}
	if _, _, err := botIdentity(context.Background(), lookup, quiet, backoff.WithBackOff(&backoff.ZeroBackOff{})); err == nil {
		t.Fatal("a rejected token must stop the bot")
	}
	if calls != 1 {
		t.Errorf("a rejected token was tried %d times", calls)
	}
}

func TestBotIdentityWithoutAUserIDIsAnError(t *testing.T) {
	lookup := func(context.Context) (string, string, error) { return "", "T1", nil }
	_, _, err := botIdentity(context.Background(), lookup, quiet,
		backoff.WithBackOff(&backoff.ZeroBackOff{}), backoff.WithMaxTries(3))
	if err == nil {
		t.Fatal("serving without a bot user ID ignores every top-level mention; it must be an error")
	}
}
