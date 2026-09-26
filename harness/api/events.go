// This file holds what the package adds to the log's event type, which is the
// contract's own pb.Event: its payload is a oneof, and which field is set is the
// event's kind.
//
// Delivery contract, published from day one and depended on by every consumer:
//
//   - Per session, events are strictly ordered by Seq.
//   - Seq is monotonic and never reset. It survives suspend/revive and a process
//     restart, because it is allocated from a high-water mark on the session row
//     rather than from anything in memory.
//   - Delivery is at-least-once. A consumer that reconnects may see events it has
//     already processed and is expected to dedup by Seq. Exactly-once is not
//     claimed anywhere and must not be assumed.
//   - The log is a record of what happened, not the thing that makes it happen.
//     An append that fails is logged and the operation it describes proceeds, so
//     a client can in principle miss an event that a later GetSession would
//     contradict. Reconciling against GetSession's State and LastSeq is therefore
//     legitimate — the stream is the fast path, not the only path.

package api

import pb "github.com/pomerium/agentops/harness/api/pb"

// payloadOneof is Event's payload oneof, resolved once.
var payloadOneof = (&pb.Event{}).ProtoReflect().Descriptor().Oneofs().ByName("payload")

// Kind names an event by the payload field that is set — "state_changed",
// "agent_message" — for a log line, a metric label or a store column. It is ""
// for an event whose payload this build does not know.
//
// Branch on the payload's type, not on this: a type switch is checked by the
// compiler, and a string is not.
func Kind(ev *pb.Event) string {
	fd := ev.ProtoReflect().WhichOneof(payloadOneof)
	if fd == nil {
		return ""
	}
	return string(fd.Name())
}

// NormalizeToolCallStatus maps an ACP tool-call status onto the published enum.
// ACP spells the running state "in_progress"; older harnesses say "running".
func NormalizeToolCallStatus(s string) ToolCallStatus {
	switch s {
	case "in_progress", "running":
		return ToolCallInProgress
	case "completed", "success":
		return ToolCallCompleted
	case "failed", "error":
		return ToolCallFailed
	default:
		return ToolCallPending
	}
}
