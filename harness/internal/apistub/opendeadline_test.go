package apistub_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/apistub"
)

// TestSubscribeOpenDeadline: a hop that accepts the subscription and then says
// nothing must not hang the caller.
//
// Opening is synchronous, so without a deadline on the opening frame this is not
// a subscription that reconnects — it is a caller stuck inside Subscribe with no
// error and no stream, on a transport that deliberately has no timeout of its
// own. The failure mode is a process that looks healthy and is doing nothing.
func TestSubscribeOpenDeadline(t *testing.T) {
	ctx := context.Background()
	newClient := serve(t)
	driver := newClient(nil)

	view := create(ctx, t, driver, &pb.CreateSessionRequest{ConversationRef: "conv-1"})

	reader := newClient(map[string]string{
		apistub.HeaderScenario: apistub.ScenarioMute,
	})

	done := make(chan error, 1)
	go func() {
		_, err := subscribe(ctx, t, reader, view.GetId(), briefKeepalive)
		done <- err
	}()

	// The budget is 3 × 100ms; anything near the old behaviour never returns.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a subscription to a black-holed stream succeeded")
		}
		t.Logf("refused as expected: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe never returned: the opening frame has no deadline")
	}
}
