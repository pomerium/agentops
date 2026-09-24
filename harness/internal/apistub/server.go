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

// Serve puts a stub behind the real apiserver on the given address and returns
// the listener it bound and the server running on it.
//
// The address must resolve to a loopback interface, and that is enforced rather
// than documented. This server admits whoever asks and stamps them with the
// identity they name: reachable off-host it is an open door onto every session
// any client of it has, which is not a footgun to leave lying in a repo that
// also ships production manifests.
func Serve(addr string, stub *Stub, log *slog.Logger) (net.Listener, *http.Server, error) {
	if log == nil {
		log = slog.Default()
	}
	if err := requireLoopback(addr); err != nil {
		return nil, nil, err
	}
	srv, err := apiserver.New(apiserver.Config{
		API:      stub,
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

// HeaderIdentity is the stub's Identify: the client id is whatever the caller
// says it is.
//
// That is the one deliberate difference from production, where the id comes from
// an assertion Pomerium verified and a client cannot state its own. It is also
// what makes cross-client isolation testable in a unit test — two identities,
// one process, no IdP — which is a property worth a conformance scenario since
// getting it wrong leaks somebody else's conversation.
//
// A bearer token is accepted as an identity too, so a client that can only send
// an Authorization header (the harness's own Go client, and therefore the
// companion) can pick an identity without a bespoke header.
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

// requireLoopback refuses an address that is not local. A hostname that resolves
// to anything routable is refused too: "localhost" pointing somewhere else is
// exactly the misconfiguration this check is for.
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
