package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

type failingSubscriber struct {
	harnessapipbconnect.HarnessAPIServiceClient
	err   error
	calls atomic.Int32
}

func (s *failingSubscriber) Subscribe(context.Context, *pb.SubscribeRequest) (*connect.ServerStreamForClient[pb.SubscribeResponse], error) {
	s.calls.Add(1)
	return nil, api.ToConnect(s.err)
}

func heldThread(a *App) *thread {
	t := &thread{sessionID: "s1", channel: "C1", threadTS: "1.0", teamID: "T1", ownerUserID: "U1"}
	a.registerThread(t)
	return t
}

func TestAConsumerThatCannotSubscribeReleasesItsThread(t *testing.T) {
	a := New(&failingSubscriber{err: api.ErrNotFound}, nil, nil)
	th := heldThread(a)

	done := make(chan struct{})
	go func() {
		defer close(done)
		a.consume(context.Background(), th, "", 0)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a consumer whose session is gone kept running")
	}
	if a.lookup("C1", "1.0", "T1", "U1") != nil {
		t.Fatal("a consumer that never subscribed left its thread registered")
	}
}

func TestAConsumerRetriesAnUnavailableHarnessUntilStopped(t *testing.T) {
	sub := &failingSubscriber{err: api.ErrUnavailable}
	a := New(sub, nil, nil)
	th := heldThread(a)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.consume(ctx, th, "", 0)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for sub.calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("an unavailable harness was not retried")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if a.lookup("C1", "1.0", "T1", "U1") == nil {
		t.Fatal("a consumer that is still retrying gave up its thread")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a stopped consumer kept retrying")
	}
	if a.lookup("C1", "1.0", "T1", "U1") != nil {
		t.Fatal("a stopped consumer left its thread registered")
	}
}
