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

	fmt.Printf("listening on %s\n", ln.Addr().String())

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
