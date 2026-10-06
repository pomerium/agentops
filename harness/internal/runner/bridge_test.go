package runner

import (
	"context"
	"sync"
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
