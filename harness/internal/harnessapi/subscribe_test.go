package harnessapi_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
)

type brokenLog struct {
	harnessapi.EventLog
}

func (brokenLog) Subscribe(context.Context, string, int64) (harnessapi.Subscription, error) {
	return brokenSub{events: closedEvents()}, nil
}

func closedEvents() chan *pb.Event {
	ch := make(chan *pb.Event)
	close(ch)
	return ch
}

type brokenSub struct {
	events chan *pb.Event
}

func (s brokenSub) Events() <-chan *pb.Event { return s.events }
func (brokenSub) Err() error                 { return errors.New("database read failed") }
func (brokenSub) Close()                     {}

func TestSubscribeEndsWithTheLogsReadError(t *testing.T) {
	h := newHarness(t)
	created, err := h.svc.CreateSession(as(stubClient), &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "conv-broken", ApprovalPrompt: "go",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	h.svc = harnessapi.New(h.store, brokenLog{harnessapi.NewEventLog(h.store)}, h.launcher, h.tmpl, h.runs,
		harnessapi.WithLogger(testLogger(t)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := serveAPI(t, h, stubClient).Subscribe(ctx, &pb.SubscribeRequest{Ref: byID(created.GetSession().GetId())})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for stream.Receive() {
	}
	if stream.Err() == nil {
		t.Fatal("the stream ended OK after the event log failed a read")
	}
	if code := connect.CodeOf(stream.Err()); code == connect.CodeCanceled {
		t.Fatalf("stream ended %v, want the read error", code)
	}
}
