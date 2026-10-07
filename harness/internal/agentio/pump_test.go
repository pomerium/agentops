package agentio

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
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

func TestAcksAreHandledWhileTheAgentIsNotReadingStdin(t *testing.T) {
	s := New()
	defer s.Close(nil)
	s.Record([]byte("stdout"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stdin := []byte("stdin the agent never reads")
	frames := append(DataFrames(stdin, uint64(len(stdin))), AckFrame(6))
	go func() { _ = s.Pump(ctx, newScriptedTransport(ctx, frames...), 0) }()

	for {
		s.mu.Lock()
		acked := s.outAcked
		s.mu.Unlock()
		if acked == 6 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("an ack waited behind stdin the agent was not reading; the agent's stdout would block once the replay buffer filled")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestAPeerPastItsReplayCapIsAProtocolViolation(t *testing.T) {
	s := New()
	defer s.Close(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	flood := make([]byte, ReplayMax+2*FrameMax)
	err := s.Pump(ctx, newScriptedTransport(ctx, DataFrames(flood, uint64(len(flood)))...), 0)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Pump = %v, want ErrProtocol", err)
	}
}

func TestAnEmptyDataFrameIsAProtocolViolation(t *testing.T) {
	s := New()
	defer s.Close(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frames := append(DataFrames([]byte("x"), 1), &agentlinkpb.AgentIOFrame{
		Msg: &agentlinkpb.AgentIOFrame_Data{Data: &agentlinkpb.AgentIOData{Seq: 1}},
	})
	if err := s.Pump(ctx, newScriptedTransport(ctx, frames...), 0); !errors.Is(err, ErrProtocol) {
		t.Fatalf("an empty data frame returned %v, want ErrProtocol; empty frames would grow the queue past the replay cap", err)
	}
}

type heldSender struct {
	*scriptedTransport
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (t *heldSender) Send(*agentlinkpb.AgentIOFrame) error {
	t.once.Do(func() { close(t.entered) })
	<-t.gate
	return nil
}

func TestStopInterruptsAnAckWaitingForABlockedSender(t *testing.T) {
	s := New()
	defer s.Close(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := make(chan struct{})
	tr := &heldSender{scriptedTransport: newScriptedTransport(ctx), entered: make(chan struct{}), gate: make(chan struct{})}
	defer close(tr.gate)
	s.Record(make([]byte, 34*FrameMax))
	done := make(chan error, 1)
	go func() { done <- s.Pump(ctx, tr, 0, WithStop(stop)) }()
	<-tr.entered
	go func() { _, _ = io.ReadFull(s.Inbound(), make([]byte, 1)) }()
	tr.frames <- DataFrames([]byte("x"), 1)[0]
	deadline := time.Now().Add(time.Second)
	for s.Consumed() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the inbound byte was not consumed")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(2 * AckInterval)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WithStop did not interrupt an ack waiting for a blocked sender; a takeover would wait forever")
	}
}
