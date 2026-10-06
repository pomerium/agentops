package harnessapi

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

type memEvents struct {
	mu        sync.Mutex
	rows      []sessionstore.SessionEvent
	firstRead chan struct{}
	readOnce  sync.Once
	readErr   error
}

func (m *memEvents) failReads(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readErr = err
}

func newMemEvents() *memEvents {
	return &memEvents{firstRead: make(chan struct{})}
}

func (m *memEvents) AppendSessionEvent(_ context.Context, sessionID, eventType, turnID string, at time.Time, payload []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := int64(len(m.rows) + 1)
	m.rows = append(m.rows, sessionstore.SessionEvent{
		SessionID: sessionID, Seq: seq, Type: eventType, TurnID: turnID, At: at, Payload: payload,
	})
	return seq, nil
}

func (m *memEvents) ListSessionEvents(_ context.Context, sessionID string, afterSeq int64, limit int) ([]sessionstore.SessionEvent, error) {
	m.mu.Lock()
	if m.readErr != nil {
		m.mu.Unlock()
		return nil, m.readErr
	}
	var out []sessionstore.SessionEvent
	for _, r := range m.rows {
		if r.SessionID == sessionID && r.Seq > afterSeq && len(out) < limit {
			out = append(out, r)
		}
	}
	m.mu.Unlock()
	m.readOnce.Do(func() { close(m.firstRead) })
	return out, nil
}

func TestSubscribeDeliversEventsDroppedFromAFullBuffer(t *testing.T) {
	ctx := context.Background()
	st := newMemEvents()
	log := NewEventLog(st)
	if err := log.Append(ctx, &pb.Event{SessionId: "s"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	sub, err := log.Subscribe(ctx, "s", 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	<-st.firstRead

	last := int64(subscriberBuffer + 2)
	for seq := int64(2); seq <= last; seq++ {
		ev := &pb.Event{SessionId: "s"}
		if seq == last {
			ev.Payload = &pb.Event_SessionEnded{SessionEnded: &pb.SessionEnded{}}
		}
		if err := log.Append(ctx, ev); err != nil {
			t.Fatalf("Append %d: %v", seq, err)
		}
	}

	deadline := time.After(10 * time.Second)
	for want := int64(1); want <= last; want++ {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("the subscription closed before seq %d", want)
			}
			if ev.GetSeq() != want {
				t.Fatalf("got seq %d, want %d", ev.GetSeq(), want)
			}
		case <-deadline:
			t.Fatalf("stored seq %d was never delivered", want)
		}
	}
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("the subscription sent an event after SessionEnded")
		}
	case <-deadline:
		t.Fatal("the subscription did not close after SessionEnded")
	}
}

func TestSubscribeReportsAFailedFirstRead(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("database read failed")
	st := newMemEvents()
	st.failReads(readErr)
	log := NewEventLog(st)

	sub, err := log.Subscribe(ctx, "s", 0)
	if sub != nil {
		sub.Close()
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("Subscribe: got %v, want %v", err, readErr)
	}
}

func TestSubscribeReportsAFailedLaterRead(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("database read failed")
	st := newMemEvents()
	log := NewEventLog(st)
	if err := log.Append(ctx, &pb.Event{SessionId: "s"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	sub, err := log.Subscribe(ctx, "s", 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	<-st.firstRead
	st.failReads(readErr)

	for seq := 2; seq <= subscriberBuffer+3; seq++ {
		if err := log.Append(ctx, &pb.Event{SessionId: "s"}); err != nil {
			t.Fatalf("Append %d: %v", seq, err)
		}
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-sub.Events():
			if ok {
				continue
			}
			if !errors.Is(sub.Err(), readErr) {
				t.Fatalf("Err: got %v, want %v", sub.Err(), readErr)
			}
			return
		case <-deadline:
			t.Fatal("the subscription did not end after the failed read")
		}
	}
}
