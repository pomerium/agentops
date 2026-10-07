package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestAnEndedSessionIdlesUntilThePodIsDeleted(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	for _, cause := range []error{nil, errors.New("token revoked")} {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- awaitDeletion(ctx, log, &sessionEnd{cause: cause}) }()
		select {
		case err := <-done:
			t.Fatalf("cause %v: the sidecar exited with %v before SIGTERM; the kubelet would restart it", cause, err)
		case <-time.After(100 * time.Millisecond):
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("cause %v: exit after SIGTERM = %v, want nil", cause, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("cause %v: the sidecar did not exit on SIGTERM", cause)
		}
	}
}

func TestAFailureBeforeTheSessionStillExits(t *testing.T) {
	boot := errors.New("runner socket missing")
	if err := awaitDeletion(context.Background(), slog.New(slog.DiscardHandler), boot); !errors.Is(err, boot) {
		t.Errorf("awaitDeletion = %v, want the boot error back", err)
	}
}
