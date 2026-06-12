// Command sidecar is the secret-isolating proxy that runs alongside the agent
// container in a sandbox pod. The agent reaches MCP servers and the LLM API
// through the sidecar's 127.0.0.1 listeners; envoy injects the secret headers
// on the way to the upstreams, so credentials never enter the agent container.
//
// Subcommands:
//
//	idle   container entrypoint; sleeps until SIGTERM (distroless-friendly
//	       replacement for `sleep infinity`)
//	serve  exec'd by agentops; speaks the SidecarControlService gRPC
//	       protocol over stdin/stdout and supervises envoy
//
// All logging goes to stderr: stdout carries the gRPC control stream.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/pomerium/agentops/internal/sidecar/envparse"
	sidecarpb "github.com/pomerium/agentops/internal/sidecar/pb"
	"github.com/pomerium/agentops/internal/sidecar/server"
	"github.com/pomerium/agentops/internal/sidecar/stdiorpc"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	cmd := ""
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "idle":
		idle()
	case "serve":
		if err := serve(log); err != nil {
			log.Error("sidecar serve failed", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "usage: %s {idle|serve}\n", os.Args[0])
		os.Exit(2)
	}
}

// idle blocks until the container is asked to stop. It is the container's
// main process; the real work happens in `serve`, exec'd on demand.
func idle() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
}

func serve(log *slog.Logger) error {
	envEndpoints, err := envparse.Parse(os.Environ())
	if err != nil {
		return fmt.Errorf("parse SIDECAR_HTTP_* env: %w", err)
	}

	envoyPath := os.Getenv("SIDECAR_ENVOY_PATH")
	if envoyPath == "" {
		envoyPath = "/usr/local/bin/envoy"
	}

	srv := server.New(server.Config{
		Proxy: server.NewEnvoy(server.EnvoyConfig{
			Path: envoyPath,
			// In the distroless image only this directory is writable; empty
			// means "use a temp dir" for local runs.
			ConfigDir: os.Getenv("SIDECAR_CONFIG_DIR"),
			LogLevel:  os.Getenv("SIDECAR_ENVOY_LOG_LEVEL"),
			Logger:    log,
		}),
		EnvEndpoints: envEndpoints,
		Logger:       log,
	})

	gsrv := grpc.NewServer()
	sidecarpb.RegisterSidecarControlServiceServer(gsrv, srv)

	conn := stdiorpc.NewConn(os.Stdin, os.Stdout, nil)
	serveErr := make(chan error, 1)
	go func() { serveErr <- gsrv.Serve(stdiorpc.ListenOnce(conn)) }()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	select {
	case <-srv.Done():
		// Control session over (stream closed or proxy died); shut down.
	case err := <-serveErr:
		return fmt.Errorf("grpc serve: %w", err)
	case <-sigCtx.Done():
		log.Info("signal received; shutting down")
	}
	// Graceful first: a hard Stop can discard the final in-flight status
	// message (e.g. the ERROR explaining why the proxy died).
	done := make(chan struct{})
	go func() { gsrv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		gsrv.Stop()
	}
	return nil
}
