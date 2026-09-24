// Package apiserver serves the Harness API to clients over Connect.
//
// It is the platform's edge: everything a Slack bot, a web app or a CI bot can
// do arrives here, behind a Pomerium route that stamps a verified identity onto
// every request. The package owns three things and no session semantics at all —
// identity (whose call is this), translation (proto in, api types out), and the
// operational surface (health, metrics). The verbs themselves belong to the
// service it wraps.
package apiserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

// Config configures the API server.
type Config struct {
	// API is the implementation every verb is forwarded to.
	API api.API
	// Identify resolves a request's client id. Required: a server that cannot
	// identify its callers would have to invent an identity, and every session in
	// the system is scoped to one.
	Identify Identify
	// Ready reports whether the process can serve: its dependencies are reachable
	// and its caches are warm. A nil Ready reports ready as soon as the server is
	// built, which is right for a test and wrong for a rollout.
	Ready func(ctx context.Context) error
	// Metrics registers the per-verb RED counters. Nil installs none.
	Metrics *Metrics
	Logger  *slog.Logger
}

// Server is the HTTP surface: the Connect service plus liveness and readiness.
type Server struct {
	cfg     Config
	log     *slog.Logger
	handler http.Handler
}

// New builds the server.
func New(cfg Config) (*Server, error) {
	if cfg.API == nil {
		return nil, errors.New("apiserver: an API implementation is required")
	}
	if cfg.Identify == nil {
		return nil, errors.New("apiserver: an Identify function is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Server{cfg: cfg, log: cfg.Logger}

	// Metrics outermost, so a refused request is counted like any other: the
	// per-verb series are what a deployment watches while it works out which
	// subject Pomerium mints, and an unauthenticated call that never reached a
	// counter is invisible exactly when it matters most.
	interceptors := []connect.Interceptor{
		&identityInterceptor{identify: cfg.Identify, log: s.log, seen: &seenClients{}},
	}
	if cfg.Metrics != nil {
		interceptors = append([]connect.Interceptor{cfg.Metrics.interceptor()}, interceptors...)
	}
	path, svc := harnessapipbconnect.NewHarnessAPIServiceHandler(
		&handler{svc: cfg.API}, connect.WithInterceptors(interceptors...))

	mux := http.NewServeMux()
	mux.Handle(path, svc)
	// Health sits outside the Connect surface. It answers a kubelet, which
	// carries no assertion and must not need one: a probe that could fail on an
	// identity problem would report the wrong thing about the process.
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)

	s.handler = mux
	return s, nil
}

// Handler returns the HTTP handler to serve. Serve it from a server EnableH2C
// has configured, or through HTTPServer.
func (s *Server) Handler() http.Handler { return s.handler }

// HTTPServer returns a server ready to listen on addr, cleartext HTTP/2
// included. Timeouts are deliberately absent: Subscribe is a long-lived stream,
// and a write deadline would cut every subscription at the same age.
func (s *Server) HTTPServer(addr string) *http.Server {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	EnableH2C(srv)
	return srv
}

// EnableH2C lets a server speak HTTP/2 over cleartext as well as HTTP/1.1.
//
// It is not optional in this deployment. The Pomerium route in front of the API
// is `to: h2c://`, and stock net/http negotiates HTTP/2 only over TLS — without
// this, every request through the route fails at the connection, before any
// handler runs. Tests configure their servers through this same function so
// they exercise the transport production uses.
func EnableH2C(srv *http.Server) {
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv.Protocols = &p
}

// healthz answers whether the process is alive. It reaches nothing: a liveness
// probe that consults a dependency turns that dependency's outage into a
// restart loop, which is the opposite of what a restart can fix.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// readyz answers whether the process can serve traffic: its store is reachable
// and its caches are warm. This is the one that consults dependencies, because
// taking a pod out of rotation is the right response to a dependency that is
// down and a restart is not.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Ready != nil {
		if err := s.cfg.Ready(r.Context()); err != nil {
			s.log.WarnContext(r.Context(), "readiness check failed", "err", err)
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// seenClients remembers which client ids have already been logged, so the
// admitted-client line (the one a deployment reads its ClientBinding subject
// off) appears once per client rather than once per request.
type seenClients struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func (s *seenClients) first(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids == nil {
		s.ids = map[string]struct{}{}
	}
	if _, ok := s.ids[id]; ok {
		return false
	}
	s.ids[id] = struct{}{}
	return true
}
