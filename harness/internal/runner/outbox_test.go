package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func thought(text string) *agentlinkpb.AgentEvent {
	return &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Thought{Thought: &agentlinkpb.AgentThought{Text: text}}}
}

func TestOutboxNumbersEventsAndReplaysAfterACursor(t *testing.T) {
	o := newOutbox(1 << 20)
	for _, s := range []string{"a", "b", "c"} {
		o.append(thought(s), false)
	}
	evs, err := o.after(context.Background(), 1)
	if err != nil || len(evs) != 2 || evs[0].GetSeq() != 2 || evs[1].GetSeq() != 3 {
		t.Fatalf("after(1) = %v, %v; want seqs 2 and 3", evs, err)
	}
	o.ack(2)
	if _, err := o.after(context.Background(), 1); !errors.Is(err, errReplayGone) {
		t.Fatalf("after(1) once 2 is acked: err = %v, want errReplayGone", err)
	}
	if _, err := o.after(context.Background(), 4); err == nil {
		t.Fatal("after(4) past the last event succeeded")
	}
	o.ack(1)
	o.ack(9)
	evs, err = o.after(context.Background(), 2)
	if err != nil || len(evs) != 1 || evs[0].GetSeq() != 3 {
		t.Fatalf("a regressed and an overreaching ack changed the outbox: %v, %v", evs, err)
	}
}

func TestOutboxAfterWaitsForTheNextEvent(t *testing.T) {
	o := newOutbox(1 << 20)
	got := make(chan []*agentlinkpb.AgentEvent, 1)
	go func() {
		evs, _ := o.after(context.Background(), 0)
		got <- evs
	}()
	select {
	case evs := <-got:
		t.Fatalf("after returned %v before any event", evs)
	case <-time.After(50 * time.Millisecond):
	}
	o.append(thought("x"), false)
	select {
	case evs := <-got:
		if len(evs) != 1 || evs[0].GetSeq() != 1 {
			t.Fatalf("after = %v, want seq 1", evs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("after did not wake for a new event")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := o.after(ctx, 1)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("after on a canceled context: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("after ignored its context")
	}
}

func TestAFullOutboxHoldsTheAgentUntilAnAck(t *testing.T) {
	o := newOutbox(1)
	o.append(thought("first"), false)
	appended := make(chan struct{})
	go func() {
		o.append(thought("second"), false)
		close(appended)
	}()
	select {
	case <-appended:
		t.Fatal("append went past a full outbox")
	case <-time.After(50 * time.Millisecond):
	}
	o.append(thought("terminal"), true)
	o.ack(2)
	select {
	case <-appended:
	case <-time.After(5 * time.Second):
		t.Fatal("an ack did not release the held append")
	}
}
