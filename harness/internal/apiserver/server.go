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

type Option func(*options)

type options struct {
	ready   func(ctx context.Context) error
	metrics *Metrics
	logger  *slog.Logger
}

func WithReady(f func(ctx context.Context) error) Option { return func(o *options) { o.ready = f } }

func WithMetrics(m *Metrics) Option { return func(o *options) { o.metrics = m } }

func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

type Server struct {
	ready   func(ctx context.Context) error
	log     *slog.Logger
	handler http.Handler
}

func New(service harnessapipbconnect.HarnessAPIServiceHandler, identify Identify, opts ...Option) (*Server, error) {
	if service == nil {
		return nil, errors.New("apiserver: a Service implementation is required")
	}
	if identify == nil {
		return nil, errors.New("apiserver: an Identify function is required")
	}
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	s := &Server{ready: o.ready, log: o.logger}

	interceptors := []connect.Interceptor{
		server.ErrorInterceptor(),
		&identityInterceptor{identify: identify, log: s.log, seen: &seenClients{}},
	}
	if o.metrics != nil {
		interceptors = append([]connect.Interceptor{o.metrics.interceptor()}, interceptors...)
	}
	path, svc := harnessapipbconnect.NewHarnessAPIServiceHandler(service, connect.WithInterceptors(interceptors...))

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
	if s.ready != nil {
		if err := s.ready(r.Context()); err != nil {
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
