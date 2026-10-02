// Package api is what the Harness API's generated code does not say on its own:
// the names a Go client branches on, the typed error set and how it crosses the
// wire, and the timing both ends of a subscription depend on.
//
// The contract itself is proto/harnessapi/v1. Its generated Connect code (package
// harnessapipbconnect) is the surface: a client calls the generated client, and
// the platform implements the generated handler. Nothing here restates a verb,
// a request or a view; the messages are the types.
//
// Depending on this package costs a client protobuf and Connect, and nothing
// else: no Kubernetes, no SQLite and no platform internals.
//
// The division of labour, which everything here leans on:
//
//   - Conversation semantics are the client's. Who may speak in a thread, how a
//     handoff reads, what a transcript carries — none of that is here.
//   - Session and authority semantics are the platform's. The idle clocks, the
//     suspend/revive machinery, the approval ceremony, the seal, and the
//     session→template binding live there and cannot be overridden by a client.
//     In particular the template is recorded on the session at creation and a
//     revive reuses the STORED spec, so a client cannot re-point a consented
//     conversation at different upstreams.
//   - Identity is the route's. No request message has a field for the caller:
//     the platform takes it from the assertion Pomerium verified, so a client
//     cannot state its own.
package api

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Error wraps a sentinel with a human-readable detail. Use Errorf to build one
// and errors.Is to test it.
type Error struct {
	Err    error
	Detail string
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.Err.Error()
	}
	return e.Err.Error() + ": " + e.Detail
}

func (e *Error) Unwrap() error { return e.Err }

// Errorf builds an *Error carrying a sentinel and a formatted detail.
func Errorf(sentinel error, format string, args ...any) error {
	return &Error{Err: sentinel, Detail: fmt.Sprintf(format, args...)}
}

// KeepaliveInterval is how often a subscription carries a keepalive while
// nothing else is happening on it.
//
// It is part of the contract rather than each side's private constant: the
// server promises it and the client times out on several of them elapsing, so
// two copies that drift make a client either reconnect for nothing or wait
// forever on a stream that is already dead.
//
// It exists because HTTP/2 PINGs terminate at each hop — a proxy in the path
// answers them on behalf of a harness that is gone — so an application-level
// tick is the only liveness signal that crosses the whole path.
const KeepaliveInterval = 20 * time.Second

// Timestamp renders an optional time for the wire, mapping the zero time to nil
// so "unset" survives the round trip rather than arriving as the Unix epoch.
func Timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// Time reads an optional wire timestamp back, mapping nil to the zero time.
// Calling AsTime on a nil timestamp instead yields 1970, which as a lower bound
// silently means "everything" only by accident and as anything else is wrong.
func Time(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
