package runner

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

const DefaultSocket = "/var/run/agentops/runner.sock"

const SocketEnv = "SIDECAR_RUNNER_SOCKET"

const stdioChunk = 64 << 10

const defaultKillDelay = 10 * time.Second

var AgentCommand = []string{"/bin/sh", "-lc", "exec ${ACP_AGENT_CMD:-acp-agent}"}

type StartFunc func(cmd *exec.Cmd) (<-chan int, error)

type Option func(*options)

type options struct {
	command   []string
	start     StartFunc
	killDelay time.Duration
	logger    *slog.Logger
}

func WithCommand(argv []string) Option { return func(o *options) { o.command = argv } }

func WithStart(f StartFunc) Option { return func(o *options) { o.start = f } }

func WithKillDelay(d time.Duration) Option { return func(o *options) { o.killDelay = d } }

func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

type Service struct {
	runnerpb.UnimplementedAgentRunnerServiceServer
	cfg options
	log *slog.Logger

	mu   sync.Mutex
	busy bool
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
	if o.killDelay <= 0 {
		o.killDelay = defaultKillDelay
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

func (s *Service) claim() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy {
		return false
	}
	s.busy = true
	return true
}

func (s *Service) release() {
	s.mu.Lock()
	s.busy = false
	s.mu.Unlock()
}

func (s *Service) Run(stream runnerpb.AgentRunnerService_RunServer) error {
	if !s.claim() {
		return status.Error(codes.AlreadyExists, "an agent is already running")
	}
	defer s.release()

	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "run closed before Spawn: %v", err)
	}
	if first.GetSpawn() == nil {
		return status.Error(codes.InvalidArgument, "the first runner frame must be Spawn")
	}

	proc, err := s.spawn()
	if err != nil {
		s.log.Error("agent-runner: spawn failed", "err", err)
		return status.Errorf(codes.Internal, "spawn agent: %v", err)
	}
	defer proc.terminate(s.log, s.cfg.killDelay)

	s.log.Info("agent-runner: agent started", "pid", proc.pid(), "command", s.cfg.command)
	return s.bridge(stream, proc)
}

func (s *Service) bridge(stream runnerpb.AgentRunnerService_RunServer, proc *agentProc) error {
	ctx := stream.Context()
	streamDone := make(chan struct{})
	defer close(streamDone)
	out := make(chan *runnerpb.RunnerServerFrame, 16)
	sendDone := make(chan struct{})
	var sendErr error

	push := func(f *runnerpb.RunnerServerFrame) bool {
		select {
		case out <- f:
			return true
		case <-sendDone:
			return false
		}
	}

	go func() {
		defer close(sendDone)
		for {
			select {
			case f := <-out:
				if sendErr = stream.Send(f); sendErr != nil || f.GetExited() != nil {
					return
				}
			case <-streamDone:
				return
			}
		}
	}()

	if !push(&runnerpb.RunnerServerFrame{
		Msg: &runnerpb.RunnerServerFrame_Started{Started: &runnerpb.Started{Pid: int64(proc.pid())}},
	}) {
		return nil
	}

	pumps := &sync.WaitGroup{}
	pumps.Add(2)
	go func() {
		defer pumps.Done()
		s.pump(proc.stdout, push, func(b []byte) *runnerpb.RunnerServerFrame {
			return &runnerpb.RunnerServerFrame{Msg: &runnerpb.RunnerServerFrame_Stdout{Stdout: b}}
		})
	}()
	go func() {
		defer pumps.Done()
		s.pump(proc.stderr, push, func(b []byte) *runnerpb.RunnerServerFrame {
			return &runnerpb.RunnerServerFrame{Msg: &runnerpb.RunnerServerFrame_Stderr{Stderr: b}}
		})
	}()

	type recvResult struct {
		frame *runnerpb.RunnerClientFrame
		err   error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			f, err := stream.Recv()
			if b := f.GetStdin(); err == nil && b != nil {
				if _, err := proc.stdin.Write(b); err != nil {
					s.log.Warn("agent-runner: write to agent stdin failed", "err", err)
				}
				continue
			}
			select {
			case recvCh <- recvResult{f, err}:
			case <-streamDone:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case r := <-recvCh:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					s.log.Info("agent-runner: run stream closed; stopping the agent", "pid", proc.pid())
					return nil
				}
				s.log.Info("agent-runner: run stream failed; stopping the agent", "pid", proc.pid(), "err", r.err)
				return r.err
			}
			if sig := r.frame.GetSignal(); sig != nil {
				proc.signal(s.log, syscall.Signal(sig.GetSignum()))
			}
		case <-ctx.Done():
			s.log.Info("agent-runner: run stream canceled; stopping the agent", "pid", proc.pid())
			return ctx.Err()
		case <-proc.done:
			code := proc.exitCode()
			proc.signal(s.log, syscall.SIGKILL)
			_ = proc.stdout.SetReadDeadline(time.Now())
			_ = proc.stderr.SetReadDeadline(time.Now())
			drained := make(chan struct{})
			go func() { pumps.Wait(); close(drained) }()
			select {
			case <-drained:
			case <-ctx.Done():
				return ctx.Err()
			}
			s.log.Info("agent-runner: agent exited", "pid", proc.pid(), "exit_code", code)
			push(&runnerpb.RunnerServerFrame{
				Msg: &runnerpb.RunnerServerFrame_Exited{Exited: &runnerpb.Exited{ExitCode: int32(code)}},
			})
			select {
			case <-sendDone:
				return sendErr
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-sendDone:
			s.log.Info("agent-runner: send failed; stopping the agent", "pid", proc.pid(), "err", sendErr)
			return sendErr
		}
	}
}

func (s *Service) pump(f *os.File, push func(*runnerpb.RunnerServerFrame) bool, wrap func([]byte) *runnerpb.RunnerServerFrame) {
	buf := make([]byte, stdioChunk)
	emit := func(n int) bool {
		chunk := make([]byte, n)
		copy(chunk, buf[:n])
		return push(wrap(chunk))
	}
	for {
		n, err := f.Read(buf)
		if n > 0 && !emit(n) {
			return
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			break
		}
		if err != nil {
			return
		}
	}
	if err := f.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	rc, err := f.SyscallConn()
	if err != nil {
		return
	}
	var pending int
	var ioctlErr error
	if err := rc.Control(func(fd uintptr) {
		pending, ioctlErr = pipeBuffered(int(fd))
	}); err != nil || ioctlErr != nil {
		return
	}
	for pending > 0 {
		var n int
		var rerr error
		if err := rc.Read(func(fd uintptr) bool {
			n, rerr = syscall.Read(int(fd), buf[:min(pending, len(buf))])
			return true
		}); err != nil || rerr != nil || n <= 0 || !emit(n) {
			return
		}
		pending -= n
	}
}

type agentProc struct {
	cmd    *exec.Cmd
	stdin  *os.File
	stdout *os.File
	stderr *os.File

	done chan struct{}
	code atomic.Int32

	once sync.Once
}

func (p *agentProc) exitCode() int32 { return p.code.Load() }

func (p *agentProc) pid() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (s *Service) spawn() (*agentProc, error) {
	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(s.cfg.command[0], s.cfg.command[1:]...) //nolint:gosec // the command is operator configuration
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, outW, errW
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	exited, err := s.cfg.start(cmd)
	_ = inR.Close()
	_ = outW.Close()
	_ = errW.Close()
	if err != nil {
		_, _ = inW.Close(), outR.Close()
		_ = errR.Close()
		return nil, err
	}
	proc := &agentProc{cmd: cmd, stdin: inW, stdout: outR, stderr: errR, done: make(chan struct{})}
	go func() {
		proc.code.Store(int32(<-exited))
		close(proc.done)
	}()
	return proc, nil
}

func (p *agentProc) signal(log *slog.Logger, sig syscall.Signal) {
	pid := p.pid()
	if pid <= 0 {
		return
	}
	if err := syscall.Kill(-pid, sig); err != nil {
		log.Debug("agent-runner: signal failed", "pid", pid, "signal", sig.String(), "err", err)
	}
}

func (p *agentProc) terminate(log *slog.Logger, delay time.Duration) {
	p.once.Do(func() {
		defer p.closeFDs()
		select {
		case <-p.done:
		default:
			p.signal(log, syscall.SIGTERM)
			select {
			case <-p.done:
			case <-time.After(delay):
				log.Warn("agent-runner: agent ignored TERM; killing", "pid", p.pid(), "after", delay.String())
			}
		}
		p.signal(log, syscall.SIGKILL)
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			log.Error("agent-runner: agent did not exit after KILL", "pid", p.pid())
		}
	})
}

func (p *agentProc) closeFDs() {
	_ = p.stdin.Close()
	_ = p.stdout.Close()
	_ = p.stderr.Close()
}

func startAndWait(cmd *exec.Cmd) (<-chan int, error) {
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start agent: %w", err)
	}
	exited := make(chan int, 1)
	go func() {
		err := cmd.Wait()
		exited <- exitCode(err)
	}()
	return exited, nil
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}
