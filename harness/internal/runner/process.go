package runner

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type agentProc struct {
	cmd    *exec.Cmd
	stdin  *os.File
	stdout *os.File

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

func (p *agentProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (s *Service) spawn() (*agentProc, error) {
	var opened []*os.File
	pipe := func() (*os.File, *os.File, error) {
		r, w, err := s.cfg.pipe()
		if err != nil {
			for _, f := range opened {
				_ = f.Close()
			}
			return nil, nil, err
		}
		opened = append(opened, r, w)
		return r, w, nil
	}
	inR, inW, err := pipe()
	if err != nil {
		return nil, err
	}
	outR, outW, err := pipe()
	if err != nil {
		return nil, err
	}
	errR, errW, err := pipe()
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
	proc := &agentProc{cmd: cmd, stdin: inW, stdout: outR, done: make(chan struct{})}
	go logStderr(s.log, errR, proc.pid())
	go func() {
		proc.code.Store(int32(<-exited))
		close(proc.done)
	}()
	return proc, nil
}

const maxStderrLine = 1 << 16

func logStderr(log *slog.Logger, f *os.File, pid int) {
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4096), maxStderrLine)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			log.Debug("agent stderr", "pid", pid, "line", line)
		}
	}
}

func (p *agentProc) signal(log *slog.Logger, sig syscall.Signal) {
	pid := p.pid()
	if pid <= 0 {
		return
	}
	if err := syscall.Kill(-pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		log.Debug("agent-runner: signal failed", "pid", pid, "signal", sig.String(), "err", err)
	}
}

func (p *agentProc) terminate(log *slog.Logger, delay time.Duration) {
	p.once.Do(func() {
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
