package agentio

import (
	"context"
	"errors"
	"fmt"
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
			frames, next, err := s.FramesAfter(c)
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

	ticker := time.NewTicker(AckInterval)
	defer ticker.Stop()
	var unacked uint64
	flushAck := func() bool {
		if unacked == 0 {
			return true
		}
		unacked = 0
		return push(AckFrame(s.Consumed()))
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
				if len(d.GetPayload()) > FrameMax {
					return fmt.Errorf("%w: data frame of %d bytes exceeds the %d limit",
						ErrProtocol, len(d.GetPayload()), FrameMax)
				}
				if err := s.DeliverInbound(d.GetSeq(), d.GetPayload()); err != nil {
					if errors.Is(err, ErrClosed) {
						return err
					}
					return fmt.Errorf("%w: %w", ErrProtocol, err)
				}
				unacked += uint64(len(d.GetPayload()))
				if unacked >= AckBytes && !flushAck() {
					return nil
				}
			case r.frame.GetAck() != nil:
				if err := s.Ack(r.frame.GetAck().GetConsumed()); err != nil {
					return fmt.Errorf("%w: %w", ErrProtocol, err)
				}
			case r.frame.GetOpen() != nil:
				return fmt.Errorf("%w: a second Open on one AgentIO stream", ErrProtocol)
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
