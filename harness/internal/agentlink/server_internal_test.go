package agentlink

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pomerium/agentops/harness/internal/agentio"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func TestClaimRejectsAForgottenRun(t *testing.T) {
	run := &attachedRun{
		runID: "forgotten", hbInterval: time.Hour, io: agentio.New(),
		done: make(chan struct{}),
	}
	run.finish(errForgotten)
	s := &Server{now: time.Now}

	live, err := s.claim(run, &agentlinkpb.SidecarHello{ProtocolVersion: ProtocolVersion, Attempt: 1})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("claim on a forgotten run: err = %v (code %s), want NotFound", err, status.Code(err))
	}
	if live != nil || run.current() != nil {
		t.Fatal("a forgotten run holds a live attach")
	}
}

func TestReconnectAttachIsNotifiedAfterTheOldLoss(t *testing.T) {
	entered, resume, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	events := make(chan string, 2)
	run := &attachedRun{
		runID: "reconnect", hbInterval: time.Hour, io: agentio.New(),
		done: make(chan struct{}),
		opts: ExpectCallbacks{
			OnLost: func(error) {
				close(entered)
				<-resume
				events <- "lost"
			},
			OnAttached: func(uint32, bool) { events <- "attached" },
		},
	}
	s := &Server{now: time.Now, log: slog.Default()}
	old := newAttachStream(time.Now())
	run.live = old

	go func() {
		s.release(run, old, errors.New("disconnected"))
		close(released)
	}()
	<-entered
	if _, err := s.claim(run, &agentlinkpb.SidecarHello{ProtocolVersion: ProtocolVersion, Attempt: 2}); err != nil {
		close(resume)
		t.Fatalf("reconnect claim: %v", err)
	}
	go run.notifyAttached(2, true)
	select {
	case ev := <-events:
		close(resume)
		t.Fatalf("%q was notified while the old stream's OnLost was still running", ev)
	case <-time.After(200 * time.Millisecond):
	}
	close(resume)
	<-released
	for _, want := range []string{"lost", "attached"} {
		if got := <-events; got != want {
			t.Fatalf("callback order: got %q, want %q", got, want)
		}
	}
}
