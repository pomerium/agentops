package apistub

import (
	"context"
	"testing"
	"time"

	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/apiserver"
)

// These drive the stub in-process rather than over the wire, because what they
// test is the stub's own feed — whether it stays open, whether it keeps up — and
// a client in between would hide exactly that: client.Subscribe stops on
// session_ended by design, and the transport's flow control would absorb a
// reader that stops reading.

// TestEndedEventLeavesALiveFeedOpen: the stub does not close a live feed after
// session_ended, so a client that waits for end-of-stream instead of stopping on
// the event hangs here rather than passing by accident.
func TestEndedEventLeavesALiveFeedOpen(t *testing.T) {
	s := New()
	ctx := apiserver.WithClientID(context.Background(), "a")
	created, err := s.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "runid", ConversationRef: "c", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := &pb.SessionRef{SessionId: created.GetSession().GetId()}
	sub, err := s.subscribe("a", &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	if _, err := s.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	for ev := range sub.Events() {
		if ev.GetSessionEnded() == nil {
			continue
		}
		select {
		case _, open := <-sub.Events():
			if !open {
				t.Fatal("the feed closed right after session_ended")
			}
		case <-time.After(50 * time.Millisecond):
		}
		return
	}
	t.Fatal("the feed closed before session_ended arrived")
}

// TestAnUnreadSubscriberDoesNotBreakPrompts: a test that stops reading its feed
// does not make the stub panic, however much the session goes on to say.
func TestAnUnreadSubscriberDoesNotBreakPrompts(t *testing.T) {
	s := New()
	ctx := apiserver.WithClientID(context.Background(), "a")
	created, err := s.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "runid", ConversationRef: "c", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := &pb.SessionRef{SessionId: created.GetSession().GetId()}
	sub, err := s.subscribe("a", &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("a prompt panicked: %v", p)
		}
	}()
	for range 100 {
		if _, err := s.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "work"}); err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	}
	// Nothing was dropped: the whole log arrives, in order.
	logged, err := s.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref, Limit: 10000})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	for i := range logged.GetEvents() {
		select {
		case ev := <-sub.Events():
			if ev.GetSeq() != int64(i+1) {
				t.Fatalf("event %d arrived as seq %d", i+1, ev.GetSeq())
			}
		case <-time.After(time.Second):
			t.Fatalf("the feed stopped after %d of %d events", i, len(logged.GetEvents()))
		}
	}
}
