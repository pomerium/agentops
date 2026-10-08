package agentlink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

const (
	DefaultHeartbeatInterval  = 20 * time.Second
	DefaultHeartbeatMissLimit = 3
)

const (
	reasonStreamMismatch  = "agentio_stream_mismatch"
	reasonProtocol        = "protocol_violation"
	reasonHeartbeatMissed = "heartbeat_missed"
)

type Option func(*options)

type options struct {
	heartbeatInterval  time.Duration
	heartbeatMissLimit uint32
	logger             *slog.Logger
	now                func() time.Time
	startupHold        bool
}

func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *options) { o.heartbeatInterval = d }
}

func WithHeartbeatMissLimit(n uint32) Option { return func(o *options) { o.heartbeatMissLimit = n } }

func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

func WithNow(now func() time.Time) Option { return func(o *options) { o.now = now } }

func WithStartupHold() Option { return func(o *options) { o.startupHold = true } }

type Server struct {
	agentlinkpb.UnimplementedAgentLinkServiceServer

	verifier *Verifier
	cfg      options
	log      *slog.Logger
	tel      *telemetry.Component
	now      func() time.Time

	holding atomic.Bool

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
	o.heartbeatInterval = max(time.Second, o.heartbeatInterval.Round(time.Second))
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
	s := &Server{
		verifier: verifier, cfg: o, log: log, now: o.now,
		tel:  telemetry.New(log, "harness", slog.LevelDebug),
		runs: map[string]*attachedRun{},
	}
	s.holding.Store(o.startupHold)
	return s, nil
}

func (s *Server) EndStartupHold() { s.holding.Store(false) }

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
	callbacks := newExpectCallbacks(opts)
	streamID := callbacks.StreamID
	if len(streamID) == 0 {
		streamID = NewStreamID()
	}
	run := &attachedRun{
		runID: runID, seal: seal, config: cfg, opts: callbacks, log: s.log,
		hbInterval: s.cfg.heartbeatInterval, hbMissLimit: s.cfg.heartbeatMissLimit,
		streamID:  streamID,
		inbox:     make(chan *agentlinkpb.AgentIOFrame, inboxSize),
		ioTurn:    make(chan struct{}, 1),
		outReady:  make(chan struct{}, 1),
		attached:  make(chan struct{}),
		ready:     make(chan struct{}),
		done:      make(chan struct{}),
		delivered: callbacks.ResumeAfter,
		acked:     callbacks.ResumeAfter,
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
		if s.holding.Load() {
			return nil, status.Errorf(codes.Unavailable, "the harness is starting; run %s is not expected yet", a.RunID)
		}
		return nil, status.Errorf(codes.NotFound, "no expectation for run %s", a.RunID)
	}
	if a.Executor != run.seal {
		s.log.Error("harness: attach rejected — pod identity does not match the run's executor seal",
			"run_id", a.RunID, "asserted_pod", a.Executor.PodName, "asserted_pod_uid", a.Executor.PodUID,
			"sealed_pod", run.seal.PodName, "sealed_pod_uid", run.seal.PodUID)
		return nil, status.Errorf(codes.FailedPrecondition, "run %s is sealed to a different pod", a.RunID)
	}
	if run.finished() {
		return nil, status.Errorf(codes.NotFound, "run %s is no longer expected", a.RunID)
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
	if errors.Is(err, ErrKeysUnavailable) {
		s.log.Warn("harness: identity assertion not checked", "err", err)
		return nil, status.Errorf(codes.Unavailable, "identity assertion not checked: %v", err)
	}
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

	cause := s.serveAttach(ctx, stream, run, live, hello)
	s.release(run, live, cause)
	return cause
}

func (s *Server) claim(run *attachedRun, hello *agentlinkpb.SidecarHello) (*attachStream, error) {
	if v := hello.GetProtocolVersion(); v != ProtocolVersion {
		return nil, status.Errorf(codes.FailedPrecondition, "protocol version %d is not supported; want %d", v, ProtocolVersion)
	}
	now := s.now()
	run.mu.Lock()
	if run.finished() {
		run.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "run %s is no longer expected", run.runID)
	}
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
	run.notifyMu.Lock()
	defer run.notifyMu.Unlock()
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
	run *attachedRun, live *attachStream, hello *agentlinkpb.SidecarHello,
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

	sendCh := make(chan *agentlinkpb.ManagerFrame, 1)
	sentCh := make(chan error, 1)
	go func() {
		for {
			select {
			case f := <-sendCh:
				sentCh <- stream.Send(f)
			case <-ctx.Done():
				return
			}
		}
	}()

	var pending *agentlinkpb.ManagerFrame
	var pendingSince time.Time
	send := func(f *agentlinkpb.ManagerFrame) {
		pending, pendingSince = f, s.now()
		sendCh <- f
	}
	var notified chan struct{}
	var awaitNotified <-chan struct{}
	settled := false
	var held []*agentlinkpb.SidecarFrame
	defer func() {
		if notified != nil {
			<-notified
		}
	}()
	directives := func() <-chan *agentlinkpb.ManagerFrame {
		if pending != nil || !settled {
			return nil
		}
		return live.send
	}

	ticker := time.NewTicker(run.hbInterval)
	defer ticker.Stop()
	lastSent := s.now()
	deadline := time.Duration(run.hbMissLimit) * run.hbInterval

	send(&agentlinkpb.ManagerFrame{
		Msg: &agentlinkpb.ManagerFrame_HelloAck{HelloAck: &agentlinkpb.ManagerHelloAck{
			ProtocolVersion:    ProtocolVersion,
			HeartbeatSeconds:   uint32(run.hbInterval / time.Second),
			HeartbeatMissLimit: run.hbMissLimit,
			Config:             run.config,
		}},
	})

	for {
		select {
		case r := <-recvCh:
			if r.err != nil {
				return fmt.Errorf("attach stream ended: %w", r.err)
			}
			live.touch(s.now())
			if !settled {
				held = append(held, r.frame)
				continue
			}
			if err := s.handleSidecarFrame(ctx, run, r.frame); err != nil {
				return err
			}
		case <-awaitNotified:
			awaitNotified, settled = nil, true
			for _, f := range held {
				if err := s.handleSidecarFrame(ctx, run, f); err != nil {
					return err
				}
			}
			held = nil
		case err := <-sentCh:
			f := pending
			pending = nil
			if err != nil {
				return fmt.Errorf("send %s: %w", frameKind(f), err)
			}
			lastSent = s.now()
			switch {
			case f.GetHelloAck() != nil:
				run.markAttached()
				notified = make(chan struct{})
				awaitNotified = notified
				go func(done chan struct{}) {
					defer close(done)
					run.notifyAttached(hello.GetAttempt(), hello.GetAgentRunning())
				}(notified)
			case f.GetShutdown() != nil:
				s.tel.Debug(ctx, "shutdown directive sent", "reason", f.GetShutdown().GetReason())
			}
		case f := <-directives():
			send(f)
		case <-ticker.C:
			now := s.now()
			if pending != nil {
				if blocked := now.Sub(pendingSince); blocked > deadline {
					s.log.Warn("harness: sidecar stopped reading its control stream",
						"run_id", run.runID, "blocked_for", blocked.String(), "deadline", deadline.String())
					return status.Errorf(codes.DeadlineExceeded, "%s: %s blocked for %s", reasonHeartbeatMissed, frameKind(pending), blocked)
				}
			}
			if silent := live.silentFor(now); silent > deadline {
				s.log.Warn("harness: sidecar missed the heartbeat deadline",
					"run_id", run.runID, "silent_for", silent.String(), "deadline", deadline.String())
				return status.Errorf(codes.DeadlineExceeded, "%s: no frame for %s", reasonHeartbeatMissed, silent)
			}
			if pending != nil || now.Sub(lastSent) < run.hbInterval {
				continue
			}
			send(&agentlinkpb.ManagerFrame{
				Msg: &agentlinkpb.ManagerFrame_Heartbeat{Heartbeat: &agentlinkpb.Heartbeat{}},
			})
		case <-live.closed:
			return live.err()
		case <-run.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func frameKind(f *agentlinkpb.ManagerFrame) string {
	switch {
	case f.GetHelloAck() != nil:
		return "hello ack"
	case f.GetHeartbeat() != nil:
		return "heartbeat"
	default:
		return "directive"
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
