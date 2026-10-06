package harnessapi

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

type EventLog interface {
	Append(ctx context.Context, ev *pb.Event) error

	Finish(ctx context.Context, sessionID string, status api.SessionState, events []*pb.Event) error

	History(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]*pb.Event, error)

	Subscribe(ctx context.Context, sessionID string, afterSeq int64) (Subscription, error)
}

type Subscription interface {
	Events() <-chan *pb.Event

	Err() error

	Close()
}

const subscriberBuffer = 32

type durableLog struct {
	store sessionstore.Events

	mu   sync.Mutex
	subs map[string]map[*subscription]struct{}
}

func NewEventLog(st sessionstore.Events) EventLog {
	return &durableLog{store: st, subs: map[string]map[*subscription]struct{}{}}
}

func (l *durableLog) Append(ctx context.Context, ev *pb.Event) error {
	kind := api.Kind(ev)

	body, err := proto.Marshal(&pb.Event{Payload: ev.GetPayload()})
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", kind, err)
	}

	at := time.Now().UTC().Truncate(time.Millisecond)
	seq, err := l.store.AppendSessionEvent(ctx, ev.GetSessionId(), kind, ev.GetTurnId(), at, body)
	if err != nil {
		return fmt.Errorf("append %s to session %s: %w", kind, ev.GetSessionId(), err)
	}
	ev.Seq, ev.Timestamp = seq, timestamppb.New(at)
	l.publish(ev)
	return nil
}

func (l *durableLog) History(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]*pb.Event, error) {
	if limit <= 0 || limit > sessionstore.DefaultEventPage {
		limit = sessionstore.DefaultEventPage
	}
	rows, err := l.store.ListSessionEvents(ctx, sessionID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("read session %s events: %w", sessionID, err)
	}
	out := make([]*pb.Event, 0, len(rows))
	for _, r := range rows {
		ev, err := eventFromStored(r)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

func eventFromStored(r sessionstore.SessionEvent) (*pb.Event, error) {
	ev := &pb.Event{}
	if err := proto.Unmarshal(r.Payload, ev); err != nil {
		return nil, fmt.Errorf("decode session %s event %d: %w", r.SessionID, r.Seq, err)
	}
	ev.SessionId, ev.Seq, ev.TurnId, ev.Timestamp = r.SessionID, r.Seq, r.TurnID, timestamppb.New(r.At)
	return ev, nil
}

func (l *durableLog) publish(ev *pb.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for s := range l.subs[ev.GetSessionId()] {
		select {
		case s.live <- ev:
		default:
			select {
			case s.stale <- struct{}{}:
			default:
			}
		}
	}
}

func (l *durableLog) Subscribe(ctx context.Context, sessionID string, afterSeq int64) (Subscription, error) {
	s := &subscription{
		log:       l,
		sessionID: sessionID,
		live:      make(chan *pb.Event, subscriberBuffer),
		stale:     make(chan struct{}, 1),
		out:       make(chan *pb.Event),
		done:      make(chan struct{}),
	}

	l.mu.Lock()
	if l.subs[sessionID] == nil {
		l.subs[sessionID] = map[*subscription]struct{}{}
	}
	l.subs[sessionID][s] = struct{}{}
	l.mu.Unlock()

	batch, err := l.History(ctx, sessionID, afterSeq, sessionstore.DefaultEventPage)
	ended := false
	if err == nil && len(batch) == 0 && afterSeq > 0 {
		ended, err = l.endsAt(ctx, sessionID, afterSeq)
	}
	if err != nil {
		l.unsubscribe(s)
		return nil, err
	}
	if ended {
		l.unsubscribe(s)
		close(s.out)
		return s, nil
	}
	go s.pump(ctx, afterSeq, batch)
	return s, nil
}

func (l *durableLog) endsAt(ctx context.Context, sessionID string, seq int64) (bool, error) {
	last, err := l.History(ctx, sessionID, seq-1, 1)
	if err != nil {
		return false, err
	}
	return len(last) == 1 && last[0].GetSessionEnded() != nil, nil
}

func (l *durableLog) unsubscribe(s *subscription) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m := l.subs[s.sessionID]; m != nil {
		delete(m, s)
		if len(m) == 0 {
			delete(l.subs, s.sessionID)
		}
	}
}

type subscription struct {
	log       *durableLog
	sessionID string
	live      chan *pb.Event
	stale     chan struct{}
	out       chan *pb.Event
	err       error

	closeOnce sync.Once
	done      chan struct{}
}

func (s *subscription) Events() <-chan *pb.Event { return s.out }

func (s *subscription) Err() error { return s.err }

func (s *subscription) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.log.unsubscribe(s)
	})
}

func (s *subscription) pump(ctx context.Context, sent int64, batch []*pb.Event) {
	defer close(s.out)
	defer s.log.unsubscribe(s)

	for {
		for _, ev := range batch {
			if ev.Seq <= sent {
				continue
			}
			select {
			case s.out <- ev:
			case <-s.done:
				return
			case <-ctx.Done():
				return
			}
			sent = ev.Seq
			if ev.GetSessionEnded() != nil {
				return
			}
		}
		contiguous := len(batch) < sessionstore.DefaultEventPage
		for contiguous {
			select {
			case ev := <-s.live:
				if ev.Seq <= sent {
					continue
				}
				if ev.Seq != sent+1 {
					contiguous = false
					continue
				}
				select {
				case s.out <- ev:
				case <-s.done:
					return
				case <-ctx.Done():
					return
				}
				sent = ev.Seq
				if ev.GetSessionEnded() != nil {
					return
				}
			case <-s.stale:
				contiguous = false
			case <-s.done:
				return
			case <-ctx.Done():
				return
			}
		}

		var err error
		if batch, err = s.log.History(ctx, s.sessionID, sent, sessionstore.DefaultEventPage); err != nil {
			s.err = err
			return
		}
	}
}
