package stdiorpc_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	sidecarpb "github.com/pomerium/agentops/internal/sidecar/pb"
	"github.com/pomerium/agentops/internal/sidecar/stdiorpc"
)

// echoControl is a minimal SidecarControlService that answers every Configure
// with a READY status echoing the endpoint names.
type echoControl struct {
	sidecarpb.UnimplementedSidecarControlServiceServer
}

func (echoControl) Control(stream grpc.BidiStreamingServer[sidecarpb.ControlRequest, sidecarpb.ControlResponse]) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		cfg := req.GetConfigure()
		if cfg == nil {
			continue
		}
		statuses := make([]*sidecarpb.EndpointStatus, 0, len(cfg.Endpoints))
		for _, ep := range cfg.Endpoints {
			statuses = append(statuses, &sidecarpb.EndpointStatus{
				Name:       ep.Name,
				ListenPort: ep.ListenPort,
			})
		}
		if err := stream.Send(&sidecarpb.ControlResponse{
			Msg: &sidecarpb.ControlResponse_Status{Status: &sidecarpb.Status{
				State:     sidecarpb.Status_STATE_READY,
				Endpoints: statuses,
			}},
		}); err != nil {
			return err
		}
	}
}

// pipePair builds two stdiorpc.Conns connected back-to-back via io.Pipe, the
// same shape as an exec'd process's stdin/stdout.
func pipePair() (clientConn, serverConn net.Conn) {
	clientToServerR, clientToServerW := io.Pipe()
	serverToClientR, serverToClientW := io.Pipe()
	clientConn = stdiorpc.NewConn(serverToClientR, clientToServerW, nil)
	serverConn = stdiorpc.NewConn(clientToServerR, serverToClientW, nil)
	return clientConn, serverConn
}

func startServer(t *testing.T, conn net.Conn) *grpc.Server {
	t.Helper()
	srv := grpc.NewServer()
	sidecarpb.RegisterSidecarControlServiceServer(srv, echoControl{})
	lis := stdiorpc.ListenOnce(conn)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return srv
}

func TestControlRoundTrip(t *testing.T) {
	t.Parallel()
	clientConn, serverConn := pipePair()
	startServer(t, serverConn)

	cc, err := stdiorpc.Dial(clientConn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := sidecarpb.NewSidecarControlServiceClient(cc).Control(ctx)
	if err != nil {
		t.Fatalf("Control: %v", err)
	}
	err = stream.Send(&sidecarpb.ControlRequest{
		Msg: &sidecarpb.ControlRequest_Configure{Configure: &sidecarpb.Configure{
			Endpoints: []*sidecarpb.HttpEndpoint{
				{Name: "anthropic", ListenPort: 9999, UpstreamUrl: "https://api.anthropic.com"},
				{Name: "mcp-0", ListenPort: 9100, UpstreamUrl: "https://example.com/mcp"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	status := resp.GetStatus()
	if status == nil {
		t.Fatalf("expected status response, got %v", resp)
	}
	if status.State != sidecarpb.Status_STATE_READY {
		t.Errorf("state = %v, want READY", status.State)
	}
	if len(status.Endpoints) != 2 || status.Endpoints[0].Name != "anthropic" || status.Endpoints[1].ListenPort != 9100 {
		t.Errorf("unexpected endpoint statuses: %v", status.Endpoints)
	}
}

func TestClientCloseEndsServerStream(t *testing.T) {
	t.Parallel()
	clientConn, serverConn := pipePair()

	srv := grpc.NewServer()
	done := make(chan error, 1)
	sidecarpb.RegisterSidecarControlServiceServer(srv, &controlFunc{fn: func(stream grpc.BidiStreamingServer[sidecarpb.ControlRequest, sidecarpb.ControlResponse]) error {
		_, err := stream.Recv()
		done <- err
		return nil
	}})
	go func() { _ = srv.Serve(stdiorpc.ListenOnce(serverConn)) }()
	defer srv.Stop()

	cc, err := stdiorpc.Dial(clientConn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sidecarpb.NewSidecarControlServiceClient(cc).Control(ctx); err != nil {
		t.Fatalf("Control: %v", err)
	}

	// Closing the client connection must surface as a stream error on the
	// server side rather than hanging.
	_ = cc.Close()
	_ = clientConn.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Error("server Recv returned nil error after client close")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server stream did not end after client close")
	}
}

func TestCloseFnRunsOnce(t *testing.T) {
	t.Parallel()
	r, w := io.Pipe()
	calls := 0
	conn := stdiorpc.NewConn(r, w, func() error { calls++; return nil })
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if calls != 1 {
		t.Errorf("closeFn ran %d times, want 1", calls)
	}
}

type controlFunc struct {
	sidecarpb.UnimplementedSidecarControlServiceServer
	fn func(grpc.BidiStreamingServer[sidecarpb.ControlRequest, sidecarpb.ControlResponse]) error
}

func (f *controlFunc) Control(stream grpc.BidiStreamingServer[sidecarpb.ControlRequest, sidecarpb.ControlResponse]) error {
	return f.fn(stream)
}
