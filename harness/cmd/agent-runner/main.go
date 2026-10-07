package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/pomerium/agentops/harness/internal/runner"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	if len(os.Args) > 1 && os.Args[1] == "install" {
		if len(os.Args) != 3 {
			fmt.Fprintf(os.Stderr, "usage: %s install <destination>\n", os.Args[0])
			os.Exit(2)
		}
		if err := install(os.Args[2]); err != nil {
			log.Error("agent-runner install failed", "err", err)
			os.Exit(1)
		}
		log.Info("agent-runner installed", "path", os.Args[2])
		return
	}

	if err := run(log); err != nil {
		log.Error("agent-runner failed", "err", err)
		os.Exit(1)
	}
}

func install(dest string) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate this binary: %w", err)
	}
	src, err := os.Open(self)
	if err != nil {
		return fmt.Errorf("open %s: %w", self, err)
	}
	defer func() { _ = src.Close() }()

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(dest), err)
	}
	tmp := dest + ".tmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := io.Copy(out, src); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy to %s: %w", tmp, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tmp, dest, err)
	}
	return nil
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	socket := os.Getenv(runner.SocketEnv)
	if socket == "" {
		socket = runner.DefaultSocket
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o755); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale socket %s: %w", socket, err)
	}
	lis, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socket, err)
	}
	if err := os.Chmod(socket, 0o666); err != nil {
		return fmt.Errorf("chmod socket %s: %w", socket, err)
	}

	opts := []runner.Option{runner.WithLogger(log)}
	if os.Getpid() == 1 {
		reaper := runner.NewReaper(log)
		go reaper.Run(ctx)
		opts = append(opts, runner.WithStart(reaper.Start))
		log.Info("agent-runner: running as pid 1; reaping orphans")
	}
	svc := runner.New(opts...)

	gs := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}))
	svc.Register(gs)

	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()
	log.Info("agent-runner: listening", "socket", socket)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		log.Info("agent-runner: signal received; shutting down")
	}
	done := make(chan struct{})
	go func() { gs.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		gs.Stop()
	}
	return nil
}
