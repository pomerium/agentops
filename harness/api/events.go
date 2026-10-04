package api

import pb "github.com/pomerium/agentops/harness/api/pb"

var payloadOneof = (&pb.Event{}).ProtoReflect().Descriptor().Oneofs().ByName("payload")

func Kind(ev *pb.Event) string {
	fd := ev.ProtoReflect().WhichOneof(payloadOneof)
	if fd == nil {
		return ""
	}
	return string(fd.Name())
}

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
