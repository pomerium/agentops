package server

import (
	"context"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

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
