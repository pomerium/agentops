// This file names the contract's enums in Go and declares the typed error set.
// The enums themselves are proto/harnessapi/v1's, documented there; these are
// aliases, so the compiler holds each name to the value it stands for.

package api

import (
	"errors"

	pb "github.com/pomerium/agentops/harness/api/pb"
)

// SessionState is where a session is in its life.
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

// Live reports whether a state still owns cluster resources, which is also what
// makes a ConversationRef unavailable to a second session.
func Live(s SessionState) bool {
	switch s {
	case StatePending, StateLaunching, StateAwaitingApproval, StateRunning, StateSuspended:
		return true
	}
	return false
}

// Terminal reports whether a session in this state can never run again.
func Terminal(s SessionState) bool { return !Live(s) }

// Reason is why a session changed state or was suspended.
type Reason = pb.Reason

const (
	ReasonLaunch       = pb.Reason_REASON_LAUNCH
	ReasonRevive       = pb.Reason_REASON_REVIVE
	ReasonIdle         = pb.Reason_REASON_IDLE
	ReasonReviveFailed = pb.Reason_REASON_REVIVE_FAILED
)

// EndReason is why a session ended.
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

// ToolCallStatus is the closed set of tool-call states.
type ToolCallStatus = pb.ToolCallStatus

const (
	ToolCallPending    = pb.ToolCallStatus_TOOL_CALL_STATUS_PENDING
	ToolCallInProgress = pb.ToolCallStatus_TOOL_CALL_STATUS_IN_PROGRESS
	ToolCallCompleted  = pb.ToolCallStatus_TOOL_CALL_STATUS_COMPLETED
	ToolCallFailed     = pb.ToolCallStatus_TOOL_CALL_STATUS_FAILED
)

// Resolution is how a permission request closed when nobody chose an option.
type Resolution = pb.Resolution

const (
	ResolutionExpired    = pb.Resolution_RESOLUTION_EXPIRED
	ResolutionSuperseded = pb.Resolution_RESOLUTION_SUPERSEDED
)

// The platform's published error set. Each travels as the Connect code and the
// Sentinel value package wire pairs it with, and comes back out of the client as
// itself, so errors.Is works the same on either side of the wire.
var (
	// ErrNotFound: no such session, or none this client may see.
	ErrNotFound = errors.New("harnessapi: session not found")
	// ErrUnknownRequest: an unknown or already-resolved permission request.
	ErrUnknownRequest = errors.New("harnessapi: unknown or expired permission request")
	// ErrForbidden: this client has no ClientBinding, or the template is not in
	// its binding. Another client's session is never Forbidden; it is NotFound.
	ErrForbidden = errors.New("harnessapi: forbidden")
	// ErrConflict: the conversation ref already has a live session.
	ErrConflict = errors.New("harnessapi: conversation already has a live session")
	// ErrInvalidState: the verb does not apply in this session's state. Notably
	// a prompt at a state that is neither running nor suspended: consent must
	// cover exactly what the approver read, so there is no queue.
	ErrInvalidState = errors.New("harnessapi: not valid in this session state")
	// ErrNotRevivable: the session is suspended but cannot be continued — no
	// recorded conversation, no workspace, or no approver to pin a fresh
	// approval to. A client is expected to start a new session instead.
	ErrNotRevivable = errors.New("harnessapi: this conversation cannot be continued")
	// ErrInvalidArgument: a malformed or missing field.
	ErrInvalidArgument = errors.New("harnessapi: invalid argument")
	// ErrUnavailable: a dependency (the orchestrator, the authorization server)
	// failed. Retryable.
	ErrUnavailable = errors.New("harnessapi: temporarily unavailable")
	// ErrQuotaExceeded: the client's ClientBinding caps what it may hold at
	// once, or how fast it may open sessions, and this call would exceed it.
	// Distinct from ErrForbidden because it is a "not now", not a "not ever":
	// the detail names which quota, and a client can retry or end something
	// first.
	ErrQuotaExceeded = errors.New("harnessapi: client quota exceeded")
)
