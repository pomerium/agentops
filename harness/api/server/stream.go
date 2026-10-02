package server

import (
	"context"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

// Stream serves an accepted subscription: it sends the opening keepalive, then
// every event from events until that channel closes, with a keepalive whenever
// the feed has been quiet for api.KeepaliveInterval.
//
// An implementation's Subscribe resolves and authorizes the request first and
// returns its refusal as an error, before calling this — that ordering is what
// the opening keepalive is for. Until a message arrives a client cannot
// distinguish an accepted subscription from a refused one, and a refusal that
// reads as silence is the worst of both.
//
// events closing is the session's log being finished: a clean end of stream,
// not an error, because there is nothing further to deliver and a client should
// stop asking.
func Stream(ctx context.Context, stream *connect.ServerStream[pb.SubscribeResponse], events <-chan *pb.Event) error {
	if err := stream.Send(&pb.SubscribeResponse{Keepalive: true}); err != nil {
		return err
	}
	ticker := time.NewTicker(api.KeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.SubscribeResponse{Event: ev}); err != nil {
				return err
			}
		case <-ticker.C:
			if err := stream.Send(&pb.SubscribeResponse{Keepalive: true}); err != nil {
				return err
			}
		}
	}
}
