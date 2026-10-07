package agentio

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

var ErrProtocol = errors.New("agentio protocol violation")

type Transport interface {
	Send(*agentlinkpb.AgentIOFrame) error
	Recv() (*agentlinkpb.AgentIOFrame, error)
}

type PumpOption func(*pumpOptions)

type pumpOptions struct {
	stop <-chan struct{}
}

func WithStop(stop <-chan struct{}) PumpOption { return func(o *pumpOptions) { o.stop = stop } }

func (s *Stream) Pump(ctx context.Context, t Transport, cursor uint64, opts ...PumpOption) error {
	var o pumpOptions
	for _, opt := range opts {
		opt(&o)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	streamDone := make(chan struct{})
	defer close(streamDone)
	out := make(chan *agentlinkpb.AgentIOFrame, 32)
	fail := make(chan error, 2)

	push := func(f *agentlinkpb.AgentIOFrame) bool {
		select {
		case out <- f:
			return true
		case <-streamDone:
			return false
		case <-ctx.Done():
			return false
		}
	}
	report := func(err error) {
		select {
		case fail <- err:
		default:
		}
	}

	go func() {
		for {
			select {
			case f := <-out:
				if err := t.Send(f); err != nil {
					report(fmt.Errorf("agentio send: %w", err))
					return
				}
			case <-streamDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		c := cursor
		for {
			frames, next, err := s.framesAfter(ctx, c)
			if err != nil {
				report(err)
				return
			}
			c = next
			for _, f := range frames {
				if !push(f) {
					return
				}
			}
		}
	}()

	type recvResult struct {
		frame *agentlinkpb.AgentIOFrame
		err   error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			f, err := t.Recv()
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

	type chunk struct {
		seq     uint64
		payload []byte
	}
	var (
		qmu    sync.Mutex
		queue  []chunk
		queued int
	)
	queuedWake := make(chan struct{}, 1)
	delivered := make(chan struct{}, 1)
	deliverStop := make(chan struct{})
	deliverDone := make(chan struct{})
	signal := func(c chan struct{}) {
		select {
		case c <- struct{}{}:
		default:
		}
	}
	defer func() {
		close(deliverStop)
		<-deliverDone
	}()
	go func() {
		defer close(deliverDone)
		for {
			qmu.Lock()
			if len(queue) == 0 {
				qmu.Unlock()
				select {
				case <-queuedWake:
					continue
				case <-deliverStop:
					return
				}
			}
			c := queue[0]
			qmu.Unlock()
			if err := s.deliver(ctx, deliverStop, c.seq, c.payload); err != nil {
				if !errors.Is(err, errStopped) {
					report(err)
				}
				return
			}
			qmu.Lock()
			queue = queue[1:]
			queued -= len(c.payload)
			qmu.Unlock()
			signal(delivered)
		}
	}()

	ticker := time.NewTicker(AckInterval)
	defer ticker.Stop()
	received := s.Consumed()
	lastAck := received
	flushAck := func() bool {
		c := s.Consumed()
		if c == lastAck {
			return true
		}
		lastAck = c
		return push(AckFrame(c))
	}

	for {
		select {
		case r := <-recvCh:
			if r.err != nil {
				return fmt.Errorf("agentio stream ended: %w", r.err)
			}
			switch {
			case r.frame.GetData() != nil:
				d := r.frame.GetData()
				n := len(d.GetPayload())
				if n == 0 || n > FrameMax {
					return fmt.Errorf("%w: data frame of %d bytes is outside 1..%d",
						ErrProtocol, n, FrameMax)
				}
				if want := received + uint64(n); d.GetSeq() != want {
					return fmt.Errorf("%w: agentio data seq %d is not contiguous (expected %d)", ErrProtocol, d.GetSeq(), want)
				}
				qmu.Lock()
				if queued+n > ReplayMax+FrameMax {
					qmu.Unlock()
					return fmt.Errorf("%w: the peer sent more than %d unacknowledged bytes", ErrProtocol, ReplayMax+FrameMax)
				}
				queue = append(queue, chunk{seq: d.GetSeq(), payload: d.GetPayload()})
				queued += n
				qmu.Unlock()
				received = d.GetSeq()
				signal(queuedWake)
			case r.frame.GetAck() != nil:
				if err := s.Ack(r.frame.GetAck().GetConsumed()); err != nil {
					return fmt.Errorf("%w: %w", ErrProtocol, err)
				}
			case r.frame.GetOpen() != nil:
				return fmt.Errorf("%w: a second Open on one AgentIO stream", ErrProtocol)
			}
		case <-delivered:
			if s.Consumed()-lastAck >= AckBytes && !flushAck() {
				return nil
			}
		case <-ticker.C:
			if !flushAck() {
				return nil
			}
		case err := <-fail:
			return err
		case <-o.stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
