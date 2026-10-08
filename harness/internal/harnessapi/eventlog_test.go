package harnessapi_test

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

func TestHistoryReplaysIdenticallyAfterRestart(t *testing.T) {
	ctx := as(stubClient)
	dbPath := t.TempDir() + "/replay.db"

	h := newHarnessAt(t, dbPath)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())

	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "hello"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	rec := record(t, sub)
	rec.waitFor("turn_completed", 0)
	sub.Close()

	page, err := h.svc.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	before := page.GetEvents()
	if len(before) < 5 {
		t.Fatalf("expected a full launch's worth of events, got %d", len(before))
	}
	_ = h.store.Close()

	h2 := newHarnessAt(t, dbPath)
	page, err = h2.svc.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Events after restart: %v", err)
	}
	after := page.GetEvents()
	if len(after) != len(before) {
		t.Fatalf("replayed %d events, wrote %d", len(after), len(before))
	}
	for i := range before {
		if !proto.Equal(before[i], after[i]) {
			t.Errorf("event %d changed across the restart:\n before %v\n after  %v", i, before[i], after[i])
		}
	}
	for i, ev := range after {
		if ev.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d; the log must be gapless", i, ev.Seq)
		}
	}

	res, err := h2.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: ref})
	if err != nil {
		t.Fatalf("GetSession after restart: %v", err)
	}
	restarted := res.GetSession()
	if restarted.GetLastSeq() != after[len(after)-1].Seq {
		t.Errorf("LastSeq = %d, want %d", restarted.GetLastSeq(), after[len(after)-1].Seq)
	}
	h2.launcher.adoptErr = errors.New("the sandbox is gone")
	<-h2.svc.ReconcileOnStartup(ctx)
	page, err = h2.svc.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref, AfterSeq: restarted.GetLastSeq()})
	if err != nil {
		t.Fatalf("Events after reconcile: %v", err)
	}
	post := page.GetEvents()
	if len(post) == 0 {
		t.Fatal("a session that was live at restart must be told it was interrupted")
	}
	if post[0].Seq != restarted.GetLastSeq()+1 {
		t.Errorf("the first post-restart event has seq %d, want %d", post[0].Seq, restarted.GetLastSeq()+1)
	}
	last := post[len(post)-1]
	ended := last.GetSessionEnded()
	if ended == nil {
		t.Fatalf("last event after reconcile is %s, want session_ended", api.Kind(last))
	}
	if ended.GetReason() != api.EndInterrupted {
		t.Errorf("session_ended reason = %v, want %v", ended.GetReason(), api.EndInterrupted)
	}
}

func TestSubscribeResumesFromCursor(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())

	page, err := h.svc.ListEvents(ctx, &pb.ListEventsRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	history := page.GetEvents()
	cursor := history[len(history)-1].Seq

	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref, AfterSeq: cursor})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	rec := record(t, sub)

	if _, err := h.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "hello"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	ev, _ := rec.waitFor("turn_completed", 0)
	if ev.Seq <= cursor {
		t.Errorf("resumed feed delivered seq %d, at or before the cursor %d", ev.Seq, cursor)
	}
	for _, seen := range rec.kinds() {
		if seen == "approval_required" {
			t.Error("a resumed feed replayed history the client had already seen")
		}
	}
}

func TestSubscriptionClosesOnSessionEnd(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	view := launchRunning(t, h, "stub:conv-1")
	ref := byID(view.GetId())

	sub, err := h.subscribe(ctx, &pb.SubscribeRequest{Ref: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	closed := make(chan struct{})
	go func() {
		for range sub.Events() {
		}
		close(closed)
	}()

	if _, err := h.svc.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the feed did not close after session_ended")
	}
}
