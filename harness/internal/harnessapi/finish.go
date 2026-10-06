package harnessapi

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

func (l *durableLog) Finish(ctx context.Context, sessionID string, status api.SessionState, events []*pb.Event) error {
	at := time.Now().UTC().Truncate(time.Millisecond)
	rows := make([]sessionstore.NewSessionEvent, 0, len(events))
	for _, ev := range events {
		body, err := proto.Marshal(&pb.Event{Payload: ev.GetPayload()})
		if err != nil {
			return fmt.Errorf("marshal %s payload: %w", api.Kind(ev), err)
		}
		rows = append(rows, sessionstore.NewSessionEvent{Type: api.Kind(ev), TurnID: ev.GetTurnId(), At: at, Payload: body})
	}
	seqs, err := l.store.FinishSession(ctx, sessionID, status, rows)
	if err != nil {
		return fmt.Errorf("finish session %s: %w", sessionID, err)
	}
	for i, ev := range events {
		ev.SessionId, ev.Seq, ev.Timestamp = sessionID, seqs[i], timestamppb.New(at)
		l.publish(ev)
	}
	return nil
}
