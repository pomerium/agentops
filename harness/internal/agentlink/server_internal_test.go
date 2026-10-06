package agentlink

import (
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
