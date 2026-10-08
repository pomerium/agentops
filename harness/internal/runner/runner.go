package runner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

const DefaultSocket = "/var/run/agentops/runner.sock"

const SocketEnv = "SIDECAR_RUNNER_SOCKET"

const defaultKillDelay = 10 * time.Second

var AgentCommand = []string{"/bin/sh", "-lc", "exec ${ACP_AGENT_CMD:-acp-agent}"}

type StartFunc func(cmd *exec.Cmd) (<-chan int, error)

type Option func(*options)

type options struct {
	command   []string
	start     StartFunc
	pipe      func() (*os.File, *os.File, error)
	killDelay time.Duration
	logger    *slog.Logger
	outboxMax int
}

func WithCommand(argv []string) Option { return func(o *options) { o.command = argv } }

func WithStart(f StartFunc) Option { return func(o *options) { o.start = f } }

func WithKillDelay(d time.Duration) Option { return func(o *options) { o.killDelay = d } }

func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

type Service struct {
	runnerpb.UnimplementedAgentRunnerServiceServer
	cfg options
	log *slog.Logger

	mu     sync.Mutex
	sess   *session
	client context.CancelCauseFunc
}

func New(opts ...Option) *Service {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if len(o.command) == 0 {
		o.command = AgentCommand
	}
	if o.start == nil {
		o.start = startAndWait
	}
	if o.pipe == nil {
		o.pipe = os.Pipe
	}
	if o.killDelay <= 0 {
		o.killDelay = defaultKillDelay
	}
	if o.outboxMax <= 0 {
		o.outboxMax = outboxMax
	}
	log := o.logger
	if log == nil {
		log = slog.Default()
	}
	return &Service{cfg: o, log: log}
}

func (s *Service) Register(gs *grpc.Server) {
	runnerpb.RegisterAgentRunnerServiceServer(gs, s)
}

func (s *Service) Close() {
	s.mu.Lock()
	sess := s.sess
	s.mu.Unlock()
	if sess != nil {
		sess.proc.terminate(s.log, s.cfg.killDelay)
	}
}

var errReplaced = errors.New("replaced by a newer runner stream")

func (s *Service) open(first *runnerpb.RunnerClientFrame) (*session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case first.GetSpawn() != nil:
		sp := first.GetSpawn()
		if s.sess != nil {
			if !bytes.Equal(s.sess.streamID, sp.GetStreamId()) {
				return nil, status.Error(codes.AlreadyExists, "an agent session with another stream id is running")
			}
			return s.sess, nil
		}
		proc, err := s.spawn()
		if err != nil {
			s.log.Error("agent-runner: spawn failed", "err", err)
			return nil, status.Errorf(codes.Internal, "spawn agent: %v", err)
		}
		s.log.Info("agent-runner: agent started", "pid", proc.pid(), "command", s.cfg.command)
		s.sess = newSession(s.log, sp.GetStreamId(), proc, s.cfg.killDelay, s.cfg.outboxMax)
		go s.sess.run(sp.GetSession())
		return s.sess, nil
	case first.GetJoin() != nil:
		if s.sess == nil {
			return nil, status.Error(codes.NotFound, "no agent session runs")
		}
		return s.sess, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "the first runner frame must be Spawn or Join")
	}
}

func (s *Service) claimClient(cancel context.CancelCauseFunc) {
	s.mu.Lock()
	prev := s.client
	s.client = cancel
	s.mu.Unlock()
	if prev != nil {
		prev(errReplaced)
	}
}

func (s *Service) Run(stream runnerpb.AgentRunnerService_RunServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "run closed before its first frame: %v", err)
	}
	sess, err := s.open(first)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil)
	s.claimClient(cancel)

	if err := stream.Send(&runnerpb.RunnerServerFrame{Msg: &runnerpb.RunnerServerFrame_Started{Started: &runnerpb.Started{
		Pid: int64(sess.proc.pid()), StreamId: sess.streamID,
	}}}); err != nil {
		return err
	}

	type recvResult struct {
		frame *runnerpb.RunnerClientFrame
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

	var feedCancel context.CancelFunc
	var feedDone chan struct{}
	feedErr := make(chan error, 1)
	stopFeed := func() bool {
		if feedCancel == nil {
			return true
		}
		feedCancel()
		select {
		case <-feedDone:
		case <-ctx.Done():
			return false
		}
		feedCancel, feedDone = nil, nil
		return true
	}
	defer func() {
		if feedCancel != nil {
			feedCancel()
		}
	}()

	for {
		select {
		case r := <-recvCh:
			if r.err != nil {
				s.log.Debug("agent-runner: runner stream closed; the agent keeps running", "err", r.err)
				return nil
			}
			switch f := r.frame; {
			case f.GetReplay() != nil:
				if !stopFeed() {
					continue
				}
				after := f.GetReplay().GetAfter()
				if err := sess.out.check(after); err != nil {
					return status.Errorf(codes.FailedPrecondition, "replay: %v", err)
				}
				fctx, fcancel := context.WithCancel(ctx)
				done := make(chan struct{})
				feedCancel, feedDone = fcancel, done
				go func() {
					defer close(done)
					if err := feed(fctx, stream, sess, after); err != nil && fctx.Err() == nil {
						select {
						case feedErr <- err:
						default:
						}
					}
				}()
			case f.GetAck() != nil:
				sess.out.ack(f.GetAck().GetConsumed())
			case f.GetPrompt() != nil:
				sess.prompt(f.GetPrompt())
			case f.GetPermission() != nil:
				sess.decide(f.GetPermission())
			case f.GetStop() != nil:
				s.log.Info("agent-runner: stop requested", "pid", sess.proc.pid())
				sess.stop()
			case f.GetSpawn() != nil || f.GetJoin() != nil:
				return status.Error(codes.InvalidArgument, "Spawn and Join are only valid as the first frame")
			}
		case err := <-feedErr:
			if errors.Is(err, errReplayGone) {
				return status.Errorf(codes.FailedPrecondition, "replay: %v", err)
			}
			return err
		case <-ctx.Done():
			if errors.Is(context.Cause(ctx), errReplaced) {
				return status.Error(codes.Aborted, errReplaced.Error())
			}
			return ctx.Err()
		}
	}
}

func feed(ctx context.Context, stream runnerpb.AgentRunnerService_RunServer, sess *session, after uint64) error {
	if err := stream.Send(&runnerpb.RunnerServerFrame{Msg: &runnerpb.RunnerServerFrame_ReplayStart{ReplayStart: &runnerpb.ReplayStart{
		State: sess.state(),
	}}}); err != nil {
		return err
	}
	cursor := after
	for {
		evs, err := sess.out.after(ctx, cursor)
		if err != nil {
			return err
		}
		for _, ev := range evs {
			if err := stream.Send(&runnerpb.RunnerServerFrame{Msg: &runnerpb.RunnerServerFrame_Event{Event: ev}}); err != nil {
				return err
			}
			cursor = ev.GetSeq()
		}
	}
}
