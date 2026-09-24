package apistub_test

import (
	"context"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
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

	view, err := driver.CreateSession(ctx, api.CreateSessionRequest{
		Template: "runid", ConversationRef: "conv-1", ApprovalPrompt: "ship it",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	reader := newClient(map[string]string{
		apistub.HeaderScenario: apistub.ScenarioMute,
	}, briefKeepalive)

	done := make(chan error, 1)
	go func() {
		_, err := reader.Subscribe(ctx, api.SubscribeRequest{Ref: api.SessionRef{SessionID: view.ID}})
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
