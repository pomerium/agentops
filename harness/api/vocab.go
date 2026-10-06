package api

import (
	"errors"

	pb "github.com/pomerium/agentops/harness/api/pb"
)

type SessionState = pb.SessionState

const (
	StatePending          = pb.SessionState_SESSION_STATE_PENDING
	StateLaunching        = pb.SessionState_SESSION_STATE_LAUNCHING
	StateAwaitingApproval = pb.SessionState_SESSION_STATE_AWAITING_APPROVAL
	StateRunning          = pb.SessionState_SESSION_STATE_RUNNING
	StateSuspended        = pb.SessionState_SESSION_STATE_SUSPENDED
	StateEnded            = pb.SessionState_SESSION_STATE_ENDED
	StateInterrupted      = pb.SessionState_SESSION_STATE_INTERRUPTED
)

func Live(s SessionState) bool {
	switch s {
	case StatePending, StateLaunching, StateAwaitingApproval, StateRunning, StateSuspended:
		return true
	}
	return false
}

func Terminal(s SessionState) bool { return !Live(s) }

type Reason = pb.Reason

const (
	ReasonLaunch            = pb.Reason_REASON_LAUNCH
	ReasonRevive            = pb.Reason_REASON_REVIVE
	ReasonIdle              = pb.Reason_REASON_IDLE
	ReasonReviveFailed      = pb.Reason_REASON_REVIVE_FAILED
	ReasonResumeUnavailable = pb.Reason_REASON_RESUME_UNAVAILABLE
)

type EndReason = pb.EndReason

const (
	EndRevoked         = pb.EndReason_END_REASON_REVOKED
	EndExpired         = pb.EndReason_END_REASON_EXPIRED
	EndNeverApproved   = pb.EndReason_END_REASON_NEVER_APPROVED
	EndAgentExit       = pb.EndReason_END_REASON_AGENT_EXIT
	EndTunnelLost      = pb.EndReason_END_REASON_TUNNEL_LOST
	EndEnded           = pb.EndReason_END_REASON_ENDED
	EndInterrupted     = pb.EndReason_END_REASON_INTERRUPTED
	EndPrepareFailed   = pb.EndReason_END_REASON_PREPARE_FAILED
	EndRunCreateFailed = pb.EndReason_END_REASON_RUN_CREATE_FAILED
	EndAttachTimeout   = pb.EndReason_END_REASON_ATTACH_TIMEOUT
	EndLaunchFailed    = pb.EndReason_END_REASON_LAUNCH_FAILED
	EndIdle            = pb.EndReason_END_REASON_IDLE
)

type ToolCallStatus = pb.ToolCallStatus

const (
	ToolCallPending    = pb.ToolCallStatus_TOOL_CALL_STATUS_PENDING
	ToolCallInProgress = pb.ToolCallStatus_TOOL_CALL_STATUS_IN_PROGRESS
	ToolCallCompleted  = pb.ToolCallStatus_TOOL_CALL_STATUS_COMPLETED
	ToolCallFailed     = pb.ToolCallStatus_TOOL_CALL_STATUS_FAILED
)

type Resolution = pb.Resolution

const (
	ResolutionExpired    = pb.Resolution_RESOLUTION_EXPIRED
	ResolutionSuperseded = pb.Resolution_RESOLUTION_SUPERSEDED
)

var (
	ErrNotFound = errors.New("harnessapi: session not found")

	ErrUnknownRequest = errors.New("harnessapi: unknown or expired permission request")

	ErrForbidden = errors.New("harnessapi: forbidden")

	ErrConflict = errors.New("harnessapi: conversation already has a live session")

	ErrInvalidState = errors.New("harnessapi: not valid in this session state")

	ErrNotRevivable = errors.New("harnessapi: this conversation cannot be continued")

	ErrInvalidArgument = errors.New("harnessapi: invalid argument")

	ErrUnavailable = errors.New("harnessapi: temporarily unavailable")

	ErrQuotaExceeded = errors.New("harnessapi: client quota exceeded")
)
