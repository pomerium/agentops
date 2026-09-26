package wire_test

import (
	"errors"
	"reflect"
	"testing"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
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

// TestEverySentinelIsPublished: each value of the proto's Sentinel enum has a
// Go error and a Connect code behind it, and survives the trip through both.
// Walked from the descriptor, so a sentinel added to the proto and not to the
// table fails here rather than arriving as an unclassified error.
func TestEverySentinelIsPublished(t *testing.T) {
	published := wire.Sentinels()
	values := pb.Sentinel_SENTINEL_UNSPECIFIED.Descriptor().Values()
	for i := range values.Len() {
		name := pb.Sentinel(values.Get(i).Number())
		if name == pb.Sentinel_SENTINEL_UNSPECIFIED {
			continue
		}
		t.Run(name.String(), func(t *testing.T) {
			s, ok := published[name]
			if !ok {
				t.Fatalf("%v has no Go error behind it", name)
			}
			cerr := new(connect.Error)
			if !errors.As(wire.ToConnect(api.Errorf(s.Err, "why")), &cerr) {
				t.Fatal("ToConnect did not produce a Connect error")
			}
			if cerr.Code() != s.Code {
				t.Errorf("travels as %v, want %v", cerr.Code(), s.Code)
			}
			back := wire.FromConnect(cerr)
			if !errors.Is(back, s.Err) {
				t.Errorf("came back as %v, want %v", back, s.Err)
			}
			if got, _, _ := wire.Classify(back); got != name {
				t.Errorf("classifies as %v, want %v", got, name)
			}
		})
	}
	if len(published) != values.Len()-1 {
		t.Errorf("%d sentinels published, the proto defines %d", len(published), values.Len()-1)
	}
}
