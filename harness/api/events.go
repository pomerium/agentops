// This file holds the event log's envelope. What travels inside it is in
// payloads.go and the vocabularies those payloads speak are in vocab.go — both
// generated, from proto/payloads.yaml and proto/vocabulary.yaml, so that the
// SDKs and the documentation cannot come to describe a different event than the
// one the platform writes.

package api

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event is one entry on a session's durable, ordered log.
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
type Event struct {
	SessionID string    `json:"session_id"`
	Seq       int64     `json:"seq"`
	Type      EventType `json:"type"`
	// TurnID is set on everything that happens inside a turn, empty otherwise.
	TurnID string `json:"turn_id,omitempty"`
	// At is when the event was recorded, to millisecond precision.
	At time.Time `json:"timestamp"`
	// Payload is the type-specific body, stored as JSON so an unknown type still
	// round-trips through the log byte-identically.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Decode unmarshals the event payload into v.
func (e Event) Decode(v any) error {
	if len(e.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("decode %s payload: %w", e.Type, err)
	}
	return nil
}

// NormalizeToolCallStatus maps an ACP tool-call status onto the published enum.
// ACP spells the running state "in_progress"; older harnesses say "running".
func NormalizeToolCallStatus(s string) ToolCallStatus {
	switch s {
	case string(ToolCallInProgress), "running":
		return ToolCallInProgress
	case string(ToolCallCompleted), "success":
		return ToolCallCompleted
	case string(ToolCallFailed), "error":
		return ToolCallFailed
	default:
		return ToolCallPending
	}
}
