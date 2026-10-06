package runner

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"

	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

type gatedStream struct {
	grpc.ServerStream
	ctx     context.Context
	started chan struct{}
	gate    chan struct{}

	mu   sync.Mutex
	sent []*runnerpb.RunnerServerFrame
}

func (s *gatedStream) Context() context.Context { return s.ctx }

func (s *gatedStream) Recv() (*runnerpb.RunnerClientFrame, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *gatedStream) Send(f *runnerpb.RunnerServerFrame) error {
	if f.GetStarted() != nil {
		close(s.started)
		<-s.gate
	}
	s.mu.Lock()
	s.sent = append(s.sent, f)
	s.mu.Unlock()
	return nil
}

func (s *gatedStream) frames() []*runnerpb.RunnerServerFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*runnerpb.RunnerServerFrame(nil), s.sent...)
}

func TestBridgeSendsFinalOutputAndExitedBeforeReturning(t *testing.T) {
	svc := New(WithCommand([]string{"/bin/sh", "-c", "printf hello"}))
	p, err := svc.spawn()
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer p.terminate(svc.log, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &gatedStream{ctx: ctx, started: make(chan struct{}), gate: make(chan struct{})}
	release := sync.OnceFunc(func() { close(s.gate) })
	defer release()
	done := make(chan error, 1)
	go func() { done <- svc.bridge(s, p) }()

	<-s.started
	<-p.done
	select {
	case <-done:
		t.Fatal("bridge returned while Started was still being sent")
	case <-time.After(500 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bridge: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not return after the sends completed")
	}

	var stdout []byte
	frames := s.frames()
	for _, f := range frames {
		stdout = append(stdout, f.GetStdout()...)
	}
	if string(stdout) != "hello" {
		t.Errorf("stdout = %q, want %q", stdout, "hello")
	}
	if last := frames[len(frames)-1]; last.GetExited() == nil || last.GetExited().GetExitCode() != 0 {
		t.Errorf("last frame = %v, want Exited with code 0", last)
	}
}

func TestBridgeSendsAllOutputBeforeExitedToASlowClient(t *testing.T) {
	svc := New(WithCommand([]string{"/bin/sh", "-c", "for i in $(seq 1 40); do echo $i; sleep 0.01; done"}))
	p, err := svc.spawn()
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer p.terminate(svc.log, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &gatedStream{ctx: ctx, started: make(chan struct{}), gate: make(chan struct{})}
	release := sync.OnceFunc(func() { close(s.gate) })
	defer release()
	done := make(chan error, 1)
	go func() { done <- svc.bridge(s, p) }()

	<-s.started
	<-p.done
	time.Sleep(2500 * time.Millisecond)
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bridge: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not return after the sends completed")
	}

	var want, stdout []byte
	for i := 1; i <= 40; i++ {
		want = fmt.Appendf(want, "%d\n", i)
	}
	frames := s.frames()
	for _, f := range frames {
		stdout = append(stdout, f.GetStdout()...)
	}
	if string(stdout) != string(want) {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
	if last := frames[len(frames)-1]; last.GetExited() == nil {
		t.Errorf("last frame = %v, want Exited", last)
	}
}

type slowStream struct {
	grpc.ServerStream
	ctx     context.Context
	flowing chan struct{}
	once    sync.Once

	mu   sync.Mutex
	sent []*runnerpb.RunnerServerFrame
}

func (s *slowStream) Context() context.Context { return s.ctx }

func (s *slowStream) Recv() (*runnerpb.RunnerClientFrame, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *slowStream) Send(f *runnerpb.RunnerServerFrame) error {
	time.Sleep(5 * time.Millisecond)
	if f.GetStdout() != nil {
		s.once.Do(func() { close(s.flowing) })
	}
	s.mu.Lock()
	s.sent = append(s.sent, f)
	s.mu.Unlock()
	return nil
}

func TestBridgeSendsExitedWhileAnEscapedWriterFloodsStdout(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is not installed")
	}
	svc := New(WithCommand([]string{"/bin/sh", "-c", "setsid yes & echo $! >&2; read -r go; exit 4"}))
	p, err := svc.spawn()
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer p.terminate(svc.log, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &slowStream{ctx: ctx, flowing: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- svc.bridge(s, p) }()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		var stderr []byte
		for _, f := range s.sent {
			stderr = append(stderr, f.GetStderr()...)
		}
		if writer, err := strconv.Atoi(strings.TrimSpace(string(stderr))); err == nil {
			_ = syscall.Kill(writer, syscall.SIGKILL)
		}
	}()

	<-s.flowing
	if _, err := p.stdin.Write([]byte("\n")); err != nil {
		t.Fatalf("release the agent: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bridge: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("bridge kept draining the escaped writer's output and never sent Exited")
	}
	s.mu.Lock()
	last := s.sent[len(s.sent)-1]
	s.mu.Unlock()
	if last.GetExited().GetExitCode() != 4 {
		t.Errorf("last frame = %v, want Exited with code 4", last)
	}
}
