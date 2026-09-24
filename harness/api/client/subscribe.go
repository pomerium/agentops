package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/wire"
)

// Subscribe opens a live event feed that survives the network.
//
// In-process, a subscription ends only when the session's log does. Over the
// wire it can also end because a proxy recycled a connection, a pod moved, or a
// path went quiet — none of which the caller should have to know about, and none
// of which mean the session is over. So the returned Subscription is not one
// stream: it is a loop that reopens the stream from the last sequence it
// delivered, and the channel it exposes closes only when the log truly ends or
// the caller stops listening.
//
// Two properties make that safe, and both come from the published delivery
// contract rather than from anything invented here. Delivery is at-least-once
// and ordered by Seq, so resuming from the last sequence seen can duplicate but
// cannot skip; and this loop drops anything it has already delivered, so the
// caller sees each event once even though the transport may deliver it twice.
func (c *Client) Subscribe(ctx context.Context, req api.SubscribeRequest) (api.Subscription, error) {
	// Opening is synchronous, so a subscription to a session that does not exist —
	// or that belongs to somebody else — fails here where the caller can see it,
	// rather than as a channel that silently never yields. That is what the
	// server's opening keepalive buys: a server-streaming call returns before the
	// server has looked at the request, so the acknowledgement has to be a message.
	stream, first, err := c.openStream(ctx, req, req.AfterSeq)
	if err != nil && !errors.Is(err, errLogFinished) {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	s := &subscription{client: c, req: req, out: make(chan api.Event), cancel: cancel}
	if errors.Is(err, errLogFinished) {
		// A subscription to a session whose log is already over. Accepted, and
		// immediately closed: there is nothing wrong and nothing to deliver, and a
		// caller ranging over Events() sees it end straight away.
		cancel()
		close(s.out)
		return s, nil
	}
	go s.run(ctx, stream, first, req.AfterSeq)
	return s, nil
}

// subscription is one logical feed across however many connections it takes.
type subscription struct {
	client *Client
	req    api.SubscribeRequest
	out    chan api.Event
	cancel context.CancelFunc
}

func (s *subscription) Events() <-chan api.Event { return s.out }

// Close is idempotent, as the interface promises — a CancelFunc already is, and
// the loop it cancels is what closes the channel, so a caller ranging over
// Events() always sees it end.
func (s *subscription) Close() { s.cancel() }

func (s *subscription) run(ctx context.Context, stream *connect.ServerStreamForClient[pb.SubscribeResponse], first *pb.SubscribeResponse, afterSeq int64) {
	defer close(s.out)
	for {
		last, ended, err := s.drain(ctx, stream, first, afterSeq)
		first = nil
		if last > afterSeq {
			afterSeq = last
		}
		if stream != nil {
			_ = stream.Close()
		}
		switch {
		case ended:
			// The session's log is finished. Nothing will ever follow, so a reconnect
			// would only re-read a closed log forever.
			return
		case ctx.Err() != nil:
			return
		}
		if err != nil {
			s.client.log.WarnContext(ctx, "event subscription dropped; reconnecting",
				"session", s.req.Ref.SessionID, "after_seq", afterSeq, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectBackoff):
		}
		next, opening, err := s.client.openStream(ctx, s.req, afterSeq)
		if err != nil {
			// A refusal is final — the session was deleted, is no longer ours, or its
			// log is over — and retrying it forever would hide that from the caller.
			// Anything else is worth another attempt after the same backoff.
			if final(err) {
				s.client.log.WarnContext(ctx, "event subscription cannot be resumed",
					"session", s.req.Ref.SessionID, "err", err)
				return
			}
			stream = nil
			continue
		}
		stream, first = next, opening
	}
}

// drain delivers one connection's events, reporting the last sequence it
// delivered and whether the log ended.
func (s *subscription) drain(ctx context.Context, stream *connect.ServerStreamForClient[pb.SubscribeResponse], first *pb.SubscribeResponse, afterSeq int64) (last int64, ended bool, err error) {
	if stream == nil {
		return afterSeq, false, nil
	}
	last = afterSeq
	// A stream that says nothing at all — not even a keepalive — for this long is
	// dead however healthy the socket looks. HTTP/2 PINGs terminate at each hop,
	// so the proxy in front can answer them on behalf of a harness that is gone.
	deadline := s.client.cfg.silenceWindow()
	silence := time.NewTimer(deadline)
	defer silence.Stop()

	// The message the open handshake already took off the stream, delivered here
	// so the handshake cannot swallow an event.
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
			// A clean end of stream is the server saying the log is finished; the
			// handler returns nil exactly there.
			if err == nil || errors.Is(err, io.EOF) {
				return last, true, nil
			}
			return last, false, err
		case msg, ok := <-recv:
			if !ok {
				// Drained; the receive goroutine reports why on recvErr. Nil'd so this
				// case stops being ready, which would otherwise spin the select.
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

// deliver hands one stream message to the caller, reporting whether the log
// ended. Draining stops on either an ended log or an error.
func (s *subscription) deliver(ctx context.Context, msg *pb.SubscribeResponse, last *int64) (ended bool, err error) {
	if msg.GetKeepalive() {
		return false, nil // liveness only; never surfaced to the caller
	}
	ev := wire.EventFrom(msg.GetEvent())
	if ev.Seq <= *last {
		return false, nil // at-least-once allows a duplicate after a resume
	}
	select {
	case s.out <- ev:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	*last = ev.Seq
	return ev.Type == api.EventSessionEnded, nil
}

// openStream opens one subscription connection at a sequence and waits for the
// server's opening message, which is what turns a refusal into an error the
// caller sees rather than a stream that never speaks.
func (c *Client) openStream(ctx context.Context, req api.SubscribeRequest, afterSeq int64) (*connect.ServerStreamForClient[pb.SubscribeResponse], *pb.SubscribeResponse, error) {
	creq := connect.NewRequest(&pb.SubscribeRequest{Ref: wire.Ref(req.Ref), AfterSeq: afterSeq})
	stream, err := c.stream.Subscribe(ctx, creq)
	if err != nil {
		return nil, nil, wire.FromConnect(err)
	}

	// The opening frame gets the same liveness budget an established stream does.
	//
	// It needs one for the same reason drain does, and more urgently: the
	// streaming transport deliberately carries no timeout, so a hop that accepts
	// the connection and then says nothing leaves this Receive blocked forever —
	// and because opening is synchronous, that is a caller stuck inside
	// Subscribe with no error and no stream, rather than a subscription that
	// reconnects. A proxy restarting mid-handshake is enough to produce it.
	received := make(chan bool, 1)
	go func() { received <- stream.Receive() }()

	deadline := c.cfg.silenceWindow()
	silence := time.NewTimer(deadline)
	defer silence.Stop()

	var ok bool
	select {
	case ok = <-received:
	case <-silence.C:
		// Closing unblocks the receive goroutine, which then sends to a buffered
		// channel nobody reads — so it finishes rather than leaking.
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
			// A clean end before anything arrived: the session's log is already
			// finished. Reported as its own condition rather than as a failure —
			// nothing went wrong, there is simply nothing further to deliver, and a
			// reconnect loop must stop rather than re-open a closed log forever.
			return nil, nil, errLogFinished
		}
		return nil, nil, wire.FromConnect(err)
	}
	return stream, stream.Msg(), nil
}

// errLogFinished reports a subscription opened onto a session whose log has
// already ended. It is a condition, not a failure.
var errLogFinished = errors.New("harnessapi client: the session log is finished")

// final reports an error a reconnect cannot fix: the platform has answered, and
// its answer is that this subscription may not exist or has nothing left.
func final(err error) bool {
	return errors.Is(err, errLogFinished) ||
		errors.Is(err, api.ErrNotFound) ||
		errors.Is(err, api.ErrForbidden) ||
		errors.Is(err, api.ErrInvalidArgument)
}
