package runner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

type blockedReplayStream struct {
	runnerpb.AgentRunnerService_RunServer
	ctx     context.Context
	frames  chan *runnerpb.RunnerClientFrame
	blocked chan struct{}
	release chan struct{}
}

func (s *blockedReplayStream) Context() context.Context { return s.ctx }

func (s *blockedReplayStream) Recv() (*runnerpb.RunnerClientFrame, error) {
	select {
	case f := <-s.frames:
		return f, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *blockedReplayStream) Send(f *runnerpb.RunnerServerFrame) error {
	if f.GetReplayStart() != nil {
		close(s.blocked)
		<-s.release
	}
	return nil
}

func TestAReplacedStreamReturnsWhileItsSendIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := &blockedReplayStream{
		ctx: ctx, frames: make(chan *runnerpb.RunnerClientFrame, 2),
		blocked: make(chan struct{}), release: make(chan struct{}),
	}
	defer close(st.release)
	svc := New()
	svc.sess = newSession(slog.Default(), []byte("A"), &agentProc{cmd: &exec.Cmd{}, done: make(chan struct{})}, time.Second, outboxMax)
	st.frames <- &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Join{Join: &runnerpb.Join{}}}
	st.frames <- &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Replay{Replay: &runnerpb.Replay{}}}
	done := make(chan error, 1)
	go func() { done <- svc.Run(st) }()
	<-st.blocked

	_, newer := context.WithCancelCause(context.Background())
	defer newer(nil)
	svc.claimClient(newer)
	select {
	case err := <-done:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("the replaced stream returned %v, want Aborted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced stream waited for its blocked Send")
	}
}

func TestALongStderrLineKeepsThePipeDraining(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	done := make(chan struct{})
	go func() {
		logStderr(slog.Default(), r, 0)
		close(done)
	}()
	if _, err := w.Write(bytes.Repeat([]byte("x"), maxStderrLine+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("\nnext diagnostic\n")); err != nil {
		t.Fatalf("write after a long line: %v", err)
	}
	select {
	case <-done:
		t.Fatal("the stderr reader stopped at a long line")
	case <-time.After(100 * time.Millisecond):
	}
	_ = w.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stderr reader did not stop at EOF")
	}
}

func TestStderrClosesWhenTheAgentExits(t *testing.T) {
	var files []*os.File
	svc := New(WithCommand([]string{"/bin/sh", "-c", "exit 0"}), WithKillDelay(time.Millisecond))
	svc.cfg.pipe = func() (*os.File, *os.File, error) {
		r, w, err := os.Pipe()
		if err == nil {
			files = append(files, r, w)
		}
		return r, w, err
	}
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	var inherited *os.File
	svc.cfg.start = func(cmd *exec.Cmd) (<-chan int, error) {
		fd, err := syscall.Dup(int(cmd.Stderr.(*os.File).Fd()))
		if err != nil {
			return nil, err
		}
		inherited = os.NewFile(uintptr(fd), "inherited-stderr")
		return startAndWait(cmd)
	}
	p, err := svc.spawn()
	if err != nil {
		t.Fatal(err)
	}
	defer inherited.Close()
	s := newSession(slog.Default(), []byte("A"), p, time.Millisecond, outboxMax)
	go s.run(&agentlinkpb.SessionParams{Cwd: t.TempDir()})
	<-s.done
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := files[4].Stat(); errors.Is(err, os.ErrClosed) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the stderr reader stays open after the agent exited")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
