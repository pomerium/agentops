package apiserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/api/server"
)

type Config struct {
	Service harnessapipbconnect.HarnessAPIServiceHandler

	Identify Identify

	Ready func(ctx context.Context) error

	Metrics *Metrics
	Logger  *slog.Logger
}

type Server struct {
	cfg     Config
	log     *slog.Logger
	handler http.Handler
}

func New(cfg Config) (*Server, error) {
	if cfg.Service == nil {
		return nil, errors.New("apiserver: a Service implementation is required")
	}
	if cfg.Identify == nil {
		return nil, errors.New("apiserver: an Identify function is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Server{cfg: cfg, log: cfg.Logger}

	interceptors := []connect.Interceptor{
		server.ErrorInterceptor(),
		&identityInterceptor{identify: cfg.Identify, log: s.log, seen: &seenClients{}},
	}
	if cfg.Metrics != nil {
		interceptors = append([]connect.Interceptor{cfg.Metrics.interceptor()}, interceptors...)
	}
	path, svc := harnessapipbconnect.NewHarnessAPIServiceHandler(
		cfg.Service, connect.WithInterceptors(interceptors...))

	mux := http.NewServeMux()
	mux.Handle(path, svc)

	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)

	s.handler = mux
	return s, nil
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) HTTPServer(addr string) *http.Server {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	EnableH2C(srv)
	return srv
}

func EnableH2C(srv *http.Server) {
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv.Protocols = &p
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

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
