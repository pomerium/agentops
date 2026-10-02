package api_test

import (
	"errors"
	"testing"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

// TestEverySentinelIsPublished: each value of the proto's Sentinel enum has a
// Go error and a Connect code behind it, and survives the trip through both.
// Walked from the descriptor, so a sentinel added to the proto and not to the
// table fails here rather than arriving as an unclassified error.
func TestEverySentinelIsPublished(t *testing.T) {
	published := api.Sentinels()
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
			if !errors.As(api.ToConnect(api.Errorf(s.Err, "why")), &cerr) {
				t.Fatal("ToConnect did not produce a Connect error")
			}
			if cerr.Code() != s.Code {
				t.Errorf("travels as %v, want %v", cerr.Code(), s.Code)
			}
			// What a client sees: the error as it arrives off the wire, which
			// carries the code and the detail but not the Go value.
			wired := connect.NewError(cerr.Code(), errors.New(cerr.Message()))
			for _, d := range cerr.Details() {
				wired.AddDetail(d)
			}
			back := api.FromConnect(wired)
			if !errors.Is(back, s.Err) {
				t.Errorf("came back as %v, want %v", back, s.Err)
			}
			if got := connect.CodeOf(back); got != s.Code {
				t.Errorf("came back with code %v, want %v", got, s.Code)
			}
			if got, _, _ := api.Classify(back); got != name {
				t.Errorf("classifies as %v, want %v", got, name)
			}
			var typed *api.Error
			if !errors.As(back, &typed) || typed.Detail != "why" {
				t.Errorf("the detail did not survive: %v", back)
			}
		})
	}
	if len(published) != values.Len()-1 {
		t.Errorf("%d sentinels published, the proto defines %d", len(published), values.Len()-1)
	}
}
