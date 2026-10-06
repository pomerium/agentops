package agentlink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentio"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

const (
	DefaultHeartbeatInterval  = 20 * time.Second
	DefaultHeartbeatMissLimit = 3
)

const (
	reasonResumeInvalid   = "agentio_resume_invalid"
	reasonProtocol        = "protocol_violation"
	reasonHeartbeatMissed = "heartbeat_missed"
)

type Option func(*options)

type options struct {
	heartbeatInterval  time.Duration
	heartbeatMissLimit uint32
	logger             *slog.Logger
	now                func() time.Time
}

func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *options) { o.heartbeatInterval = d }
}

func WithHeartbeatMissLimit(n uint32) Option { return func(o *options) { o.heartbeatMissLimit = n } }

func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

func WithNow(now func() time.Time) Option { return func(o *options) { o.now = now } }

type Server struct {
	agentlinkpb.UnimplementedAgentLinkServiceServer

	verifier *Verifier
	cfg      options
	log      *slog.Logger
	tel      *telemetry.Component
	now      func() time.Time

	mu   sync.Mutex
	runs map[string]*attachedRun
}

func New(verifier *Verifier, opts ...Option) (*Server, error) {
	if verifier == nil {
		return nil, errors.New("harness: a Verifier is required")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.heartbeatInterval <= 0 {
		o.heartbeatInterval = DefaultHeartbeatInterval
	}
	if o.heartbeatMissLimit == 0 {
		o.heartbeatMissLimit = DefaultHeartbeatMissLimit
	}
	if o.now == nil {
		o.now = time.Now
	}
	log := o.logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		verifier: verifier, cfg: o, log: log, now: o.now,
		tel:  telemetry.New(log, "harness", slog.LevelDebug),
		runs: map[string]*attachedRun{},
	}, nil
}

func (s *Server) Register(gs *grpc.Server) {
	agentlinkpb.RegisterAgentLinkServiceServer(gs, s)
}

func KeepaliveEnforcement() (minTime time.Duration, permitWithoutStream bool) {
	return 10 * time.Second, true
}

func (s *Server) Expect(runID string, seal agenticrun.Executor, cfg *agentlinkpb.SandboxConfig, opts ...ExpectOption) (*RunHandle, error) {
	if runID == "" {
		return nil, errors.New("harness expect: empty run id")
	}
	if err := seal.Validate(); err != nil {
		return nil, fmt.Errorf("harness expect: %w", err)
	}
	run := &attachedRun{
		runID: runID, seal: seal, config: cfg, opts: newExpectCallbacks(opts), log: s.log,
		hbInterval: s.cfg.heartbeatInterval, hbMissLimit: s.cfg.heartbeatMissLimit,
		io:       agentio.New(),
		attached: make(chan struct{}),
		ready:    make(chan struct{}),
		ioReady:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	s.mu.Lock()
	if _, dup := s.runs[runID]; dup {
		s.mu.Unlock()
		return nil, fmt.Errorf("harness expect: run %s is already expected", runID)
	}
	s.runs[runID] = run
	s.mu.Unlock()
	s.log.Debug("harness: expecting attach", "run_id", runID, "pod", seal.PodName)
	return &RunHandle{srv: s, run: run}, nil
}

func (s *Server) Forget(runID string) {
	s.mu.Lock()
	run := s.runs[runID]
	delete(s.runs, runID)
	s.mu.Unlock()
	if run == nil {
		return
	}
	s.log.Debug("harness: forgetting run", "run_id", runID)
	run.finish(errForgotten)
}

func (s *Server) Expecting() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.runs)
}

func (s *Server) DropStreams(runID string) {
	s.mu.Lock()
	run := s.runs[runID]
	s.mu.Unlock()
	if run == nil {
		return
	}
	run.mu.Lock()
	live, ioStream := run.live, run.ioStream
	run.mu.Unlock()
	if live != nil {
		live.close(errors.New("streams dropped"))
	}
	ioStream.close()
}

func (s *Server) lookup(a *Assertion) (*attachedRun, error) {
	s.mu.Lock()
	run := s.runs[a.RunID]
	s.mu.Unlock()
	if run == nil {
		return nil, status.Errorf(codes.NotFound, "no expectation for run %s", a.RunID)
	}
	if a.Executor != run.seal {
		s.log.Error("harness: attach rejected — pod identity does not match the run's executor seal",
			"run_id", a.RunID, "asserted_pod", a.Executor.PodName, "asserted_pod_uid", a.Executor.PodUID,
			"sealed_pod", run.seal.PodName, "sealed_pod_uid", run.seal.PodUID)
		return nil, status.Errorf(codes.FailedPrecondition, "run %s is sealed to a different pod", a.RunID)
	}
	select {
	case <-run.done:
		return nil, status.Errorf(codes.NotFound, "run %s is no longer expected", a.RunID)
	default:
	}
	return run, nil
}

func (s *Server) verify(ctx context.Context) (*Assertion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(AssertionMetadataKey)
	if len(values) == 0 || values[0] == "" {
		s.log.Error("harness: stream carried no " + AssertionMetadataKey +
			" — the route needs pass_identity_headers: true, and nothing but Pomerium should reach this listener")
		return nil, status.Error(codes.FailedPrecondition, "missing identity assertion")
	}
	a, err := s.verifier.Verify(ctx, values[0])
	if err != nil {
		s.log.Error("harness: identity assertion rejected", "err", err)
		return nil, status.Errorf(codes.FailedPrecondition, "identity assertion rejected: %v", err)
	}
	return a, nil
}

func (s *Server) Attach(stream agentlinkpb.AgentLinkService_AttachServer) error {
	ctx := stream.Context()
	a, err := s.verify(ctx)
	if err != nil {
		return err
	}
	run, err := s.lookup(a)
	if err != nil {
		return err
	}

	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "attach closed before Hello: %v", err)
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.FailedPrecondition, "the first Attach frame must be Hello")
	}

	live, err := s.claim(run, hello)
	if err != nil {
		return err
	}
	ctx = telemetry.With(ctx, "run_id", run.runID)
	s.log.Info("harness: sidecar attached", "run_id", run.runID, "pod", run.seal.PodName,
		"attempt", hello.GetAttempt(), "agent_running", hello.GetAgentRunning(),
		"protocol_version", hello.GetProtocolVersion())

	if err := stream.Send(&agentlinkpb.ManagerFrame{
		Msg: &agentlinkpb.ManagerFrame_HelloAck{HelloAck: &agentlinkpb.ManagerHelloAck{
			ProtocolVersion:    ProtocolVersion,
			HeartbeatSeconds:   uint32(run.hbInterval / time.Second),
			HeartbeatMissLimit: run.hbMissLimit,
			Config:             run.config,
		}},
	}); err != nil {
		s.release(run, live, fmt.Errorf("send hello ack: %w", err))
		return err
	}
	run.markAttached()
	if run.opts.OnAttached != nil {
		run.opts.OnAttached(hello.GetAttempt(), hello.GetAgentRunning())
	}

	cause := s.serveAttach(ctx, stream, run, live)
	s.release(run, live, cause)
	return cause
}

func (s *Server) claim(run *attachedRun, hello *agentlinkpb.SidecarHello) (*attachStream, error) {
	now := s.now()
	run.mu.Lock()
	if prev := run.live; prev != nil {
		if prev.silentFor(now) < run.hbInterval {
			run.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "run %s already has a live attach", run.runID)
		}
		s.log.Warn("harness: evicting a silent attach in favor of a fresh one",
			"run_id", run.runID, "silent_for", prev.silentFor(now).String())
		prev.close(errors.New("evicted by a fresh attach"))
		run.live = nil
	}
	live := newAttachStream(now)
	run.live = live
	run.attempts = hello.GetAttempt()
	prevIO := run.ioStream
	run.ioStream = nil
	run.mu.Unlock()
	prevIO.close()
	return live, nil
}

func (s *Server) release(run *attachedRun, live *attachStream, cause error) {
	live.close(cause)
	run.mu.Lock()
	stillOurs := run.live == live
	if stillOurs {
		run.live = nil
	}
	run.mu.Unlock()
	if !stillOurs {
		return
	}
	select {
	case <-run.done:
		return
	default:
	}
	s.log.Warn("harness: control stream lost", "run_id", run.runID, "err", cause)
	if run.opts.OnLost != nil {
		run.opts.OnLost(cause)
	}
}

func (s *Server) serveAttach(ctx context.Context, stream agentlinkpb.AgentLinkService_AttachServer,
	run *attachedRun, live *attachStream,
) error {
	type recvResult struct {
		frame *agentlinkpb.SidecarFrame
		err   error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			f, err := stream.Recv()
			select {
			case recvCh <- recvResult{f, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(run.hbInterval)
	defer ticker.Stop()
	lastSent := s.now()
	deadline := time.Duration(run.hbMissLimit) * run.hbInterval

	for {
		select {
		case r := <-recvCh:
			if r.err != nil {
				return fmt.Errorf("attach stream ended: %w", r.err)
			}
			live.touch(s.now())
			if err := s.handleSidecarFrame(ctx, run, r.frame); err != nil {
				return err
			}
		case f := <-live.send:
			if err := stream.Send(f); err != nil {
				return fmt.Errorf("send directive: %w", err)
			}
			lastSent = s.now()
			if f.GetShutdown() != nil {
				s.tel.Debug(ctx, "shutdown directive sent", "reason", f.GetShutdown().GetReason())
			}
		case <-ticker.C:
			now := s.now()
			if silent := live.silentFor(now); silent > deadline {
				s.log.Warn("harness: sidecar missed the heartbeat deadline",
					"run_id", run.runID, "silent_for", silent.String(), "deadline", deadline.String())
				return status.Errorf(codes.DeadlineExceeded, "%s: no frame for %s", reasonHeartbeatMissed, silent)
			}
			if now.Sub(lastSent) < run.hbInterval {
				continue
			}
			if err := stream.Send(&agentlinkpb.ManagerFrame{
				Msg: &agentlinkpb.ManagerFrame_Heartbeat{Heartbeat: &agentlinkpb.Heartbeat{}},
			}); err != nil {
				return fmt.Errorf("send heartbeat: %w", err)
			}
			lastSent = now
		case <-live.closed:
			return live.err()
		case <-run.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) handleSidecarFrame(ctx context.Context, run *attachedRun, f *agentlinkpb.SidecarFrame) error {
	switch {
	case f.GetStatus() != nil:
		st := f.GetStatus()
		if st.GetState() != agentlinkpb.Status_STATE_ERROR {
			if st.GetState() == agentlinkpb.Status_STATE_READY {
				run.markReady()
			}
			s.tel.Debug(ctx, "sidecar status", "state", st.GetState().String())
			return nil
		}
		reason := st.GetReason()
		s.log.Error("harness: sidecar reported a terminal error", "run_id", run.runID, "reason", reason)
		err := fmt.Errorf("sidecar error: %s", reason)
		if run.opts.OnError != nil {
			run.opts.OnError(reason, err)
		}
		return err
	case f.GetExited() != nil:
		code := f.GetExited().GetExitCode()
		s.log.Info("harness: agent process exited", "run_id", run.runID, "exit_code", code)
		if run.opts.OnAgentExit != nil {
			run.opts.OnAgentExit(code)
		}
		return nil
	case f.GetHello() != nil:
		return status.Error(codes.FailedPrecondition, "a second Hello on one Attach stream")
	default:
		s.tel.Debug(ctx, "ignoring unknown sidecar frame")
		return nil
	}
}

func BindAdvisory(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "*" {
		return "harness gRPC listener is bound to all interfaces (" + addr +
			"); only Pomerium should be able to reach it — restrict ingress with a NetworkPolicy"
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() && !ip.IsPrivate() {
		return "harness gRPC listener is bound to the public address " + addr
	}
	if strings.EqualFold(host, "localhost") {
		return ""
	}
	return ""
}
