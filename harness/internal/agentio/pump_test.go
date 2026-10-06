package agentio

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

type scriptedTransport struct {
	ctx    context.Context
	frames chan *agentlinkpb.AgentIOFrame
}

func newScriptedTransport(ctx context.Context, frames ...*agentlinkpb.AgentIOFrame) *scriptedTransport {
	t := &scriptedTransport{ctx: ctx, frames: make(chan *agentlinkpb.AgentIOFrame, len(frames))}
	for _, f := range frames {
		t.frames <- f
	}
	return t
}

func (t *scriptedTransport) Send(*agentlinkpb.AgentIOFrame) error { return nil }

func (t *scriptedTransport) Recv() (*agentlinkpb.AgentIOFrame, error) {
	select {
	case f := <-t.frames:
		return f, nil
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	}
}

func TestStopInterruptsInboundDelivery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		halt    func(stop chan struct{}, cancel context.CancelFunc)
		wantErr error
	}{
		{name: "WithStop", halt: func(stop chan struct{}, _ context.CancelFunc) { close(stop) }},
		{name: "canceled", halt: func(_ chan struct{}, cancel context.CancelFunc) { cancel() }, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			defer s.Close(nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			pumpCtx, cancelPump := context.WithCancel(ctx)
			stop := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- s.Pump(pumpCtx, newScriptedTransport(ctx, DataFrames([]byte("ab"), 2)...), 0, WithStop(stop))
			}()
			b := make([]byte, 1)
			if _, err := io.ReadFull(s.Inbound(), b); err != nil || string(b) != "a" {
				t.Fatalf("read %q, %v", b, err)
			}
			tc.halt(stop, cancelPump)
			select {
			case err := <-done:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("halted Pump returned %v, want %v", err, tc.wantErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Pump ignored the halt while delivering inbound bytes")
			}
			if got := s.Consumed(); got != 1 {
				t.Fatalf("consumed = %d after the reader got 1 byte, want 1", got)
			}

			go func() {
				done <- s.Pump(ctx, newScriptedTransport(ctx, DataFrames([]byte("b"), 2)...), 0)
			}()
			if _, err := io.ReadFull(s.Inbound(), b); err != nil || string(b) != "b" {
				t.Fatalf("resumed read %q, %v", b, err)
			}
			cancel()
			<-done
			if got := s.Consumed(); got != 2 {
				t.Fatalf("consumed = %d after the resume, want 2", got)
			}
		})
	}
}

func TestReturnedPumpLeavesNoGoroutines(t *testing.T) {
	s := New()
	defer s.Close(nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- s.Pump(ctx, newScriptedTransport(ctx), 0) }()
	cancel()
	<-done

	deadline := time.Now().Add(2 * time.Second)
	for n := pumpGoroutines(); n > 0; n = pumpGoroutines() {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines started by Pump outlived it", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func pumpGoroutines() int {
	buf := make([]byte, 1<<20)
	return strings.Count(string(buf[:runtime.Stack(buf, true)]), "created by github.com/pomerium/agentops/harness/internal/agentio.(*Stream).Pump")
}
