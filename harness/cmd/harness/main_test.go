package main

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pomerium/agentops/harness/internal/config"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
)

func TestServeClientAPI_OccupiedPort(t *testing.T) {
	t.Parallel()
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })

	for name, cfg := range map[string]config.HarnessConfig{
		"api":   {APIAddr: occupied.Addr().String(), AdminAddr: "127.0.0.1:0"},
		"admin": {APIAddr: "127.0.0.1:0", AdminAddr: occupied.Addr().String()},
	} {
		t.Run(name, func(t *testing.T) {
			cfg.APIAssertionIssuer = "api.example.com"
			stop, err := serveClientAPI(context.Background(), config.Config{Harness: cfg},
				&harnessapi.Service{}, nil, nil, slog.New(slog.DiscardHandler))
			if stop != nil {
				stop()
			}
			assert.ErrorContains(t, err, occupied.Addr().String())
		})
	}
}

func TestShutdownDuringAStalledReconcileDoesNotWaitForIt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	stalled := make(chan struct{})
	done := make(chan bool, 1)
	go func() { done <- awaitReconcile(ctx, stalled) }()
	cancel()
	select {
	case finished := <-done:
		assert.False(t, finished)
	case <-time.After(5 * time.Second):
		t.Fatal("the startup wait ignored the shutdown")
	}

	reconciled := make(chan struct{})
	close(reconciled)
	assert.True(t, awaitReconcile(context.Background(), reconciled))
}
