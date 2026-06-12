// Package server implements the sidecar's SidecarControlService: it receives
// endpoint configuration over the control stream, merges in env-defined
// endpoints, runs envoy, and reports lifecycle status back to
// agentops.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"

	"google.golang.org/grpc"

	"github.com/pomerium/agentops/internal/sidecar/envoyconfig"
	"github.com/pomerium/agentops/internal/sidecar/envparse"
	sidecarpb "github.com/pomerium/agentops/internal/sidecar/pb"
)

// Process is a running proxy instance.
type Process interface {
	// Exited delivers the proxy's exit error once it terminates on its own.
	Exited() <-chan error
	// Stop terminates the proxy.
	Stop()
}

// Proxy starts a proxy serving the given endpoints, returning once it is
// ready to accept connections.
type Proxy interface {
	Start(ctx context.Context, endpoints []envoyconfig.Endpoint) (Process, error)
}

// Config configures a Server.
type Config struct {
	Proxy Proxy
	// EnvEndpoints are endpoints parsed from SIDECAR_HTTP_* env vars; they are
	// merged with every Configure message.
	EnvEndpoints []envparse.Endpoint
	// Logger receives diagnostics. It must write to stderr: stdout carries the
	// gRPC stream. Defaults to slog.Default().
	Logger *slog.Logger
}

// Server implements sidecarpb.SidecarControlServiceServer.
type Server struct {
	sidecarpb.UnimplementedSidecarControlServiceServer
	cfg      Config
	log      *slog.Logger
	done     chan struct{}
	doneOnce sync.Once
}

// New constructs a Server.
func New(cfg Config) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, log: log, done: make(chan struct{})}
}

// Done is closed once the control session has ended (stream closed, proxy
// exited, or a fatal error); the sidecar process should then exit.
func (s *Server) Done() <-chan struct{} { return s.done }

// Control handles the single bidirectional control stream: it waits for a
// Configure message, starts the proxy, reports READY, and then supervises
// until either the proxy exits (ERROR) or the stream closes (stop the proxy
// and return).
func (s *Server) Control(stream grpc.BidiStreamingServer[sidecarpb.ControlRequest, sidecarpb.ControlResponse]) error {
	defer s.doneOnce.Do(func() { close(s.done) })
	type recvResult struct {
		req *sidecarpb.ControlRequest
		err error
	}
	recvCh := make(chan recvResult)
	go func() {
		for {
			req, err := stream.Recv()
			select {
			case recvCh <- recvResult{req, err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	// Phase 1: await Configure.
	var configure *sidecarpb.Configure
	for configure == nil {
		select {
		case r := <-recvCh:
			if r.err != nil {
				return fmt.Errorf("control stream closed before Configure: %w", r.err)
			}
			configure = r.req.GetConfigure()
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	endpoints, statuses, err := s.merge(configure)
	if err != nil {
		_ = sendStatus(stream, sidecarpb.Status_STATE_ERROR, err.Error(), nil)
		return err
	}

	proc, err := s.cfg.Proxy.Start(stream.Context(), endpoints)
	if err != nil {
		err = fmt.Errorf("start proxy: %w", err)
		_ = sendStatus(stream, sidecarpb.Status_STATE_ERROR, err.Error(), nil)
		return err
	}
	defer proc.Stop()

	if err := sendStatus(stream, sidecarpb.Status_STATE_READY, "", statuses); err != nil {
		return err
	}
	s.log.Info("proxy ready", "endpoints", len(endpoints))

	// Phase 2: supervise.
	for {
		select {
		case exitErr := <-proc.Exited():
			err := fmt.Errorf("proxy exited: %w", exitErr)
			_ = sendStatus(stream, sidecarpb.Status_STATE_ERROR, err.Error(), nil)
			return err
		case r := <-recvCh:
			if errors.Is(r.err, io.EOF) {
				s.log.Info("control stream closed; stopping proxy")
				return nil
			}
			if r.err != nil {
				s.log.Info("control stream error; stopping proxy", "err", r.err)
				return r.err
			}
			// Reconfiguration is not supported yet; the protocol allows it for
			// future use, so log and carry on rather than killing the proxy.
			s.log.Warn("ignoring control message received after startup")
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// merge combines env-defined endpoints with the Configure message, rejecting
// name and listen-port collisions.
func (s *Server) merge(configure *sidecarpb.Configure) ([]envoyconfig.Endpoint, []*sidecarpb.EndpointStatus, error) {
	byName := map[string]bool{}
	byPort := map[uint32]string{}
	var endpoints []envoyconfig.Endpoint
	var statuses []*sidecarpb.EndpointStatus

	add := func(ep envoyconfig.Endpoint, envDefined bool) error {
		if byName[ep.Name] {
			return fmt.Errorf("duplicate endpoint name %q", ep.Name)
		}
		if other, taken := byPort[ep.ListenPort]; taken {
			return fmt.Errorf("endpoint %q: listen port %d already used by %q", ep.Name, ep.ListenPort, other)
		}
		byName[ep.Name] = true
		byPort[ep.ListenPort] = ep.Name
		endpoints = append(endpoints, ep)
		statuses = append(statuses, &sidecarpb.EndpointStatus{
			Name:       ep.Name,
			ListenPort: ep.ListenPort,
			EnvDefined: envDefined,
		})
		return nil
	}

	for _, ep := range s.cfg.EnvEndpoints {
		if err := add(envoyconfig.Endpoint(ep), true); err != nil {
			return nil, nil, err
		}
	}
	for _, ep := range configure.GetEndpoints() {
		headers := make(map[string]string, len(ep.GetHeaders()))
		for _, h := range ep.GetHeaders() {
			headers[h.GetName()] = h.GetValue()
		}
		err := add(envoyconfig.Endpoint{
			Name:        ep.GetName(),
			ListenPort:  ep.GetListenPort(),
			UpstreamURL: ep.GetUpstreamUrl(),
			Headers:     headers,
		}, false)
		if err != nil {
			return nil, nil, err
		}
	}

	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Name < endpoints[j].Name })
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Name < statuses[j].Name })
	return endpoints, statuses, nil
}

func sendStatus(stream grpc.BidiStreamingServer[sidecarpb.ControlRequest, sidecarpb.ControlResponse],
	state sidecarpb.Status_State, message string, endpoints []*sidecarpb.EndpointStatus,
) error {
	return stream.Send(&sidecarpb.ControlResponse{
		Msg: &sidecarpb.ControlResponse_Status{Status: &sidecarpb.Status{
			State:     state,
			Message:   message,
			Endpoints: endpoints,
		}},
	})
}
