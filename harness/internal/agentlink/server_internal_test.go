package agentlink

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func TestClaimRejectsAForgottenRun(t *testing.T) {
	run := &attachedRun{
		runID: "forgotten", hbInterval: time.Hour,
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
		runID: "reconnect", hbInterval: time.Hour,
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

type blockedAttachSend struct {
	agentlinkpb.AgentLinkService_AttachServer
	ctx     context.Context
	entered chan struct{}
}

func (b *blockedAttachSend) Context() context.Context { return b.ctx }

func (b *blockedAttachSend) Send(*agentlinkpb.ManagerFrame) error {
	close(b.entered)
	<-b.ctx.Done()
	return io.EOF
}

func (b *blockedAttachSend) Recv() (*agentlinkpb.SidecarFrame, error) {
	<-b.ctx.Done()
	return nil, b.ctx.Err()
}

func TestBlockedHelloAckDoesNotHoldAForgottenRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &blockedAttachSend{ctx: ctx, entered: make(chan struct{})}
	run := &attachedRun{
		runID: "blocked-send", hbInterval: time.Hour, hbMissLimit: 3,
		attached: make(chan struct{}), done: make(chan struct{}),
	}
	live := newAttachStream(time.Now())
	run.live = live
	s := &Server{now: time.Now, log: slog.Default()}

	returned := make(chan error, 1)
	go func() {
		returned <- s.serveAttach(ctx, stream, run, live, &agentlinkpb.SidecarHello{ProtocolVersion: ProtocolVersion, Attempt: 1})
	}()
	<-stream.entered
	run.finish(errForgotten)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("serveAttach stayed inside a blocked Send after the run was forgotten")
	}
	select {
	case <-run.attached:
		t.Fatal("the run was marked attached although its HelloAck was never sent")
	default:
	}
}

func TestClaimRejectsAnUnsupportedProtocolVersion(t *testing.T) {
	for _, version := range []uint32{0, ProtocolVersion + 1} {
		run := &attachedRun{
			runID: "wrong-version", hbInterval: time.Hour,
			done: make(chan struct{}),
		}
		s := &Server{now: time.Now}
		live, err := s.claim(run, &agentlinkpb.SidecarHello{ProtocolVersion: version, Attempt: 1})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("version %d: err = %v (code %s), want FailedPrecondition", version, err, status.Code(err))
		}
		if live != nil || run.current() != nil {
			t.Errorf("version %d installed a live attach", version)
		}
	}
}
