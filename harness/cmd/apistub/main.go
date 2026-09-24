// Command apistub serves the Harness API's conformance stub: the real Connect
// transport over a scripted in-memory implementation.
//
// It is what the TypeScript and Python SDK suites run against, and what an
// SDK author outside this repo can run to check their own client. It needs no
// cluster, no Pomerium and no model key, it holds nothing on disk, and it binds
// loopback only — see internal/apistub for why that last one is enforced.
//
//	go run ./cmd/apistub                       # 127.0.0.1 on a free port
//	go run ./cmd/apistub -addr 127.0.0.1:8099  # a fixed port
//
// The bound address is printed on the first line of stdout as
// "listening on <host:port>", so a test harness can read it and connect without
// guessing a port or racing a sleep.
//
// It is not part of any image: Dockerfile.harness builds ./cmd/harness alone.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pomerium/agentops/harness/internal/apistub"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0",
		"loopback address to listen on; port 0 picks a free one")
	verbose := flag.Bool("v", false, "log every admitted client and refusal")
	flag.Parse()

	level := slog.LevelError
	if *verbose {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ln, srv, err := apistub.Serve(*addr, apistub.New(), log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "apistub:", err)
		os.Exit(1)
	}

	// Printed before serving and flushed by the newline, so a parent process can
	// block on this line rather than poll for a port to open.
	fmt.Printf("listening on %s\n", ln.Addr().String())

	// A stub is a test fixture and gets shut down by whatever started it, so an
	// interrupt has to end the process rather than be swallowed by ListenAndServe.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		_ = srv.Close()
	}()

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, "apistub:", err)
		os.Exit(1)
	}
}
