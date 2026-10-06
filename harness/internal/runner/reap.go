package runner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Reaper struct {
	log   *slog.Logger
	sigCh chan os.Signal

	mu      sync.Mutex
	tracked map[int]chan int
}

func NewReaper(log *slog.Logger) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	r := &Reaper{log: log, sigCh: make(chan os.Signal, 8), tracked: map[int]chan int{}}
	signal.Notify(r.sigCh, syscall.SIGCHLD)
	return r
}

const reapPoll = 5 * time.Second

func (r *Reaper) Run(ctx context.Context) {
	defer signal.Stop(r.sigCh)
	timer := time.NewTicker(reapPoll)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.sigCh:
		case <-timer.C:
		}
		r.reap()
	}
}

func (r *Reaper) Start(cmd *exec.Cmd) (<-chan int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start agent: %w", err)
	}
	ch := make(chan int, 1)
	r.tracked[cmd.Process.Pid] = ch
	return ch, nil
}

func (r *Reaper) reap() {
	for {
		r.mu.Lock()
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		if err != nil || pid <= 0 {
			r.mu.Unlock()
			return
		}
		ch := r.tracked[pid]
		delete(r.tracked, pid)
		r.mu.Unlock()

		code := statusCode(ws)
		if ch != nil {
			ch <- code
			continue
		}
		r.log.Debug("agent-runner: reaped an orphan", "pid", pid, "exit_code", code)
	}
}

func statusCode(ws syscall.WaitStatus) int {
	if ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return ws.ExitStatus()
}
