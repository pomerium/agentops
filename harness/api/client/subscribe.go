package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/cenkalti/backoff/v7"
	"google.golang.org/protobuf/proto"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

type SubscribeOption func(*subscribeOptions)

type subscribeOptions struct {
	keepaliveInterval time.Duration
	missedKeepalives  int
	logger            *slog.Logger
}

func WithKeepaliveInterval(d time.Duration) SubscribeOption {
	return func(o *subscribeOptions) { o.keepaliveInterval = d }
}

func WithMissedKeepalives(n int) SubscribeOption {
	return func(o *subscribeOptions) { o.missedKeepalives = n }
}

func WithLogger(l *slog.Logger) SubscribeOption {
	return func(o *subscribeOptions) { o.logger = l }
}

func (o subscribeOptions) silenceWindow() time.Duration {
	return time.Duration(o.missedKeepalives) * o.keepaliveInterval
}

func Subscribe(ctx context.Context, c harnessapipbconnect.HarnessAPIServiceClient, req *pb.SubscribeRequest, opts ...SubscribeOption) (*Subscription, error) {
	o := subscribeOptions{keepaliveInterval: api.KeepaliveInterval, missedKeepalives: 3, logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}

	s := &Subscription{c: c, req: req, opts: o, out: make(chan *pb.Event)}
	stream, first, err := s.open(ctx, req.GetAfterSeq())
	if err != nil && !errors.Is(err, errLogFinished) {
		return nil, err
	}

	ctx, s.cancel = context.WithCancel(ctx)
	if errors.Is(err, errLogFinished) {
		s.cancel()
		close(s.out)
		return s, nil
	}
	go s.run(ctx, stream, first, req.GetAfterSeq())
	return s, nil
}

type Subscription struct {
	c      harnessapipbconnect.HarnessAPIServiceClient
	req    *pb.SubscribeRequest
	opts   subscribeOptions
	out    chan *pb.Event
	cancel context.CancelFunc
}

func (s *Subscription) Events() <-chan *pb.Event { return s.out }

func (s *Subscription) Close() { s.cancel() }

func (s *Subscription) run(ctx context.Context, stream *connect.ServerStreamForClient[pb.SubscribeResponse], first *pb.SubscribeResponse, afterSeq int64) {
	defer close(s.out)
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = 2 * time.Second
	policy.MaxInterval = 30 * time.Second
	_, _ = backoff.Retry(ctx, func() (struct{}, error) {
		if stream == nil {
			next, opening, err := s.open(ctx, afterSeq)
			if err != nil {
				if final(err) {
					s.opts.logger.WarnContext(ctx, "event subscription cannot be resumed",
						"session", s.req.GetRef().GetSessionId(), "err", err)
					return struct{}{}, backoff.Permanent(err)
				}
				return struct{}{}, err
			}
			stream, first = next, opening
			policy.Reset()
		}
		last, ended, err := s.drain(ctx, stream, first, afterSeq)
		_ = stream.Close()
		stream, first = nil, nil
		afterSeq = max(afterSeq, last)
		switch {
		case ended:
			return struct{}{}, nil
		case ctx.Err() != nil:
			return struct{}{}, backoff.Permanent(ctx.Err())
		}
		if err == nil {
			err = errors.New("the subscription stream ended early")
		}
		s.opts.logger.WarnContext(ctx, "event subscription dropped; reconnecting",
			"session", s.req.GetRef().GetSessionId(), "after_seq", afterSeq, "err", err)
		return struct{}{}, err
	}, backoff.WithBackOff(policy), backoff.WithMaxElapsedTime(0))
}

func (s *Subscription) drain(ctx context.Context, stream *connect.ServerStreamForClient[pb.SubscribeResponse], first *pb.SubscribeResponse, afterSeq int64) (last int64, ended bool, err error) {
	if stream == nil {
		return afterSeq, false, nil
	}
	last = afterSeq

	deadline := s.opts.silenceWindow()
	silence := time.NewTimer(deadline)
	defer silence.Stop()

	if first != nil {
		if ended, err := s.deliver(ctx, first, &last); ended || err != nil {
			return last, ended, err
		}
	}

	recv := make(chan *pb.SubscribeResponse)
	recvErr := make(chan error, 1)
	go func() {
		defer close(recv)
		for stream.Receive() {
			select {
			case recv <- stream.Msg():
			case <-ctx.Done():
				return
			}
		}
		recvErr <- stream.Err()
	}()

	for {
		select {
		case <-ctx.Done():
			return last, false, ctx.Err()
		case <-silence.C:
			return last, false, errors.New("no events or keepalives within the liveness window")
		case err := <-recvErr:

			if err == nil || errors.Is(err, io.EOF) {
				return last, true, nil
			}
			return last, false, api.FromConnect(err)
		case msg, ok := <-recv:
			if !ok {

				recv = nil
				continue
			}
			silence.Reset(deadline)
			if ended, err := s.deliver(ctx, msg, &last); ended || err != nil {
				return last, ended, err
			}
		}
	}
}

func (s *Subscription) deliver(ctx context.Context, msg *pb.SubscribeResponse, last *int64) (ended bool, err error) {
	if msg.GetKeepalive() {
		return false, nil
	}
	ev := msg.GetEvent()
	if ev.GetSeq() <= *last {
		return false, nil
	}
	select {
	case s.out <- ev:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	*last = ev.GetSeq()
	return ev.GetSessionEnded() != nil, nil
}

func (s *Subscription) open(ctx context.Context, afterSeq int64) (*connect.ServerStreamForClient[pb.SubscribeResponse], *pb.SubscribeResponse, error) {
	req := proto.CloneOf(s.req)
	req.AfterSeq = afterSeq
	stream, err := s.c.Subscribe(ctx, req)
	if err != nil {
		return nil, nil, api.FromConnect(err)
	}

	received := make(chan bool, 1)
	go func() { received <- stream.Receive() }()

	deadline := s.opts.silenceWindow()
	silence := time.NewTimer(deadline)
	defer silence.Stop()

	var ok bool
	select {
	case ok = <-received:
	case <-silence.C:

		_ = stream.Close()
		return nil, nil, fmt.Errorf(
			"harnessapi client: no opening frame within %s; the subscription was never acknowledged", deadline)
	case <-ctx.Done():
		_ = stream.Close()
		return nil, nil, ctx.Err()
	}

	if !ok {
		err := stream.Err()
		_ = stream.Close()
		if err == nil {

			return nil, nil, errLogFinished
		}
		return nil, nil, api.FromConnect(err)
	}
	return stream, stream.Msg(), nil
}

var errLogFinished = errors.New("harnessapi client: the session log is finished")

func final(err error) bool {
	return errors.Is(err, errLogFinished) ||
		errors.Is(err, api.ErrNotFound) ||
		errors.Is(err, api.ErrForbidden) ||
		errors.Is(err, api.ErrInvalidArgument)
}
