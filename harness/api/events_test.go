package api_test

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

// TestKindNamesThePayload: an event's kind is the name of the payload field that
// is set, for every payload the contract defines — walked from the descriptor,
// so one added to the proto is covered without anybody listing it here.
func TestKindNamesThePayload(t *testing.T) {
	payload := (&pb.Event{}).ProtoReflect().Descriptor().Oneofs().ByName("payload")
	if payload == nil {
		t.Fatal("Event has no payload oneof")
	}
	for i := range payload.Fields().Len() {
		fd := payload.Fields().Get(i)
		ev := &pb.Event{}
		m := ev.ProtoReflect()
		m.Set(fd, protoreflect.ValueOfMessage(m.NewField(fd).Message()))
		if got := api.Kind(ev); got != string(fd.Name()) {
			t.Errorf("Kind with %s set = %q, want %q", fd.Name(), got, fd.Name())
		}
	}
	// An event whose payload this build does not know has none set.
	if got := api.Kind(&pb.Event{}); got != "" {
		t.Errorf("Kind with no payload = %q, want empty", got)
	}
}

// TestLiveClassifiesEveryState: whether a state still owns cluster resources
// decides whether its conversation ref is taken, so a state added to the proto
// has to be classified on purpose rather than falling into whichever answer the
// default gives.
func TestLiveClassifiesEveryState(t *testing.T) {
	want := map[api.SessionState]bool{
		api.StatePending:          true,
		api.StateLaunching:        true,
		api.StateAwaitingApproval: true,
		api.StateRunning:          true,
		api.StateSuspended:        true,
		api.StateEnded:            false,
		api.StateInterrupted:      false,
	}
	values := api.StatePending.Descriptor().Values()
	for i := range values.Len() {
		s := api.SessionState(values.Get(i).Number())
		if s == pb.SessionState_SESSION_STATE_UNSPECIFIED {
			if api.Live(s) {
				t.Error("an unspecified state reports live")
			}
			continue
		}
		live, ok := want[s]
		if !ok {
			t.Errorf("%v is classified neither live nor terminal here", s)
			continue
		}
		if got := api.Live(s); got != live {
			t.Errorf("Live(%v) = %v, want %v", s, got, live)
		}
	}
}

// TestNormalizeToolCallStatus: ACP's spellings, old and new, land on the closed
// set, and anything else is pending rather than passed through.
func TestNormalizeToolCallStatus(t *testing.T) {
	for in, want := range map[string]api.ToolCallStatus{
		"in_progress": api.ToolCallInProgress,
		"running":     api.ToolCallInProgress,
		"completed":   api.ToolCallCompleted,
		"success":     api.ToolCallCompleted,
		"failed":      api.ToolCallFailed,
		"error":       api.ToolCallFailed,
		"pending":     api.ToolCallPending,
		"":            api.ToolCallPending,
		"exploded":    api.ToolCallPending,
	} {
		if got := api.NormalizeToolCallStatus(in); got != want {
			t.Errorf("NormalizeToolCallStatus(%q) = %v, want %v", in, got, want)
		}
	}
}
