package wire_test

import (
	"reflect"
	"testing"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/api/wire"
)

// TestViewRoundTrips: every field of a session view survives the wire. A field
// the mapping forgets is a field no client can ever read back, and nothing else
// notices, because each side still agrees with itself.
func TestViewRoundTrips(t *testing.T) {
	in := api.SessionView{
		ID:              "sess-1",
		ConversationRef: "conv-1",
		State:           api.StateRunning,
		Template:        "runid",
		LastSeq:         42,
	}

	// The fixture has to exercise every field, or a new one could be dropped
	// on the wire and this test would still pass.
	rv := reflect.ValueOf(in)
	for i := range rv.NumField() {
		if rv.Field(i).IsZero() {
			t.Fatalf("fixture leaves SessionView.%s zero; set it so the round trip covers it", rv.Type().Field(i).Name)
		}
	}

	if out := wire.ViewFrom(wire.View(in)); !reflect.DeepEqual(out, in) {
		t.Errorf("round trip changed the view:\n got %+v\nwant %+v", out, in)
	}
}
