package apistub

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/pomerium/agentops/harness/internal/apiserver"
)

func Serve(addr string, stub *Stub, log *slog.Logger) (net.Listener, *http.Server, error) {
	if log == nil {
		log = slog.Default()
	}
	if err := requireLoopback(addr); err != nil {
		return nil, nil, err
	}
	srv, err := apiserver.New(apiserver.Config{
		Service:  stub,
		Identify: HeaderIdentity,
		Logger:   log,
	})
	if err != nil {
		return nil, nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	httpSrv := &http.Server{Handler: newScenarios(srv.Handler())}
	apiserver.EnableH2C(httpSrv)
	return ln, httpSrv, nil
}

func HeaderIdentity(_ context.Context, h http.Header) (string, error) {
	if id := strings.TrimSpace(h.Get(HeaderClient)); id != "" {
		return id, nil
	}
	if bearer, ok := strings.CutPrefix(h.Get("Authorization"), "Bearer "); ok {
		if id := strings.TrimSpace(bearer); id != "" {
			return id, nil
		}
	}
	return DefaultClient, nil
}

func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		return errors.New("apistub: an address must name a loopback host, e.g. 127.0.0.1:0")
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return errors.New("apistub: " + addr + " resolves to the non-loopback address " +
				ip.String() + "; the stub authenticates nobody and must not be reachable off-host")
		}
	}
	return nil
}
