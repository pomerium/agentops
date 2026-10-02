package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	pb "github.com/pomerium/agentops/harness/api/pb"
)

// published is the error set, in one table read from both directions: the
// Sentinel travels on the wire, the code is what a transport-only client sees.
//
// The Sentinel is what makes the mapping injective. Several errors share a code
// — a missing session and an unknown permission request are both not-found —
// so a client branching on errors.Is needs the one that was actually raised
// rather than the nearest code. A slice rather than two maps because the match
// order has to be deterministic: an error wrapping two sentinels must classify
// the same way every time.
var published = []struct {
	name pb.Sentinel
	err  error
	code connect.Code
}{
	{pb.Sentinel_SENTINEL_NOT_FOUND, ErrNotFound, connect.CodeNotFound},
	{pb.Sentinel_SENTINEL_UNKNOWN_REQUEST, ErrUnknownRequest, connect.CodeNotFound},
	{pb.Sentinel_SENTINEL_FORBIDDEN, ErrForbidden, connect.CodePermissionDenied},
	{pb.Sentinel_SENTINEL_CONFLICT, ErrConflict, connect.CodeAlreadyExists},
	{pb.Sentinel_SENTINEL_INVALID_STATE, ErrInvalidState, connect.CodeFailedPrecondition},
	{pb.Sentinel_SENTINEL_NOT_REVIVABLE, ErrNotRevivable, connect.CodeFailedPrecondition},
	{pb.Sentinel_SENTINEL_INVALID_ARGUMENT, ErrInvalidArgument, connect.CodeInvalidArgument},
	{pb.Sentinel_SENTINEL_UNAVAILABLE, ErrUnavailable, connect.CodeUnavailable},
	{pb.Sentinel_SENTINEL_QUOTA_EXCEEDED, ErrQuotaExceeded, connect.CodeResourceExhausted},
}

// Sentinel is one entry of the published error set: the error a caller matches
// with errors.Is, and the Connect code it arrives as.
type Sentinel struct {
	Err  error
	Code connect.Code
}

// Sentinels is the published error set keyed by the value that travels in
// ErrorInfo.sentinel.
//
// It is exported for conformance tooling — a stub asked to raise every
// published error, a test asserting an SDK maps all of them — so that set has
// one definition rather than a copy per consumer. A copy is exactly how a tenth
// sentinel comes to be untested everywhere at once.
func Sentinels() map[pb.Sentinel]Sentinel {
	out := make(map[pb.Sentinel]Sentinel, len(published))
	for _, p := range published {
		out[p.name] = Sentinel{Err: p.err, Code: p.code}
	}
	return out
}

// Classify reports which published sentinel an error carries, and the code it
// travels as. It reads a wire error's ErrorInfo too, so it gives the same answer
// whether or not the error passed through the client's interceptors.
//
// The match runs in the table's order, which is what makes it deterministic: an
// error wrapping two sentinels must classify the same way every time, and a map
// would decide by whichever key came up first. Anything unclassified reports
// false rather than being guessed at from its shape.
func Classify(err error) (pb.Sentinel, Sentinel, bool) {
	err = FromConnect(err)
	for _, p := range published {
		if errors.Is(err, p.err) {
			return p.name, Sentinel{Err: p.err, Code: p.code}, true
		}
	}
	return pb.Sentinel_SENTINEL_UNSPECIFIED, Sentinel{}, false
}

// ToConnect renders a platform error for the wire: a Connect code for anything
// that only reads codes, plus an ErrorInfo detail naming the sentinel so a client
// can get back an error that still satisfies errors.Is.
//
// An error that is already a Connect error, or a context ending, already says
// what it means and passes through. Any other error matching no sentinel becomes
// CodeInternal with no detail. That is deliberate: an unclassified error is a
// bug in the service, and dressing it up as one of the published ones would tell
// a client something untrue about whether to retry.
func ToConnect(err error) error {
	var cerr *connect.Error
	if err == nil || errors.As(err, &cerr) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	name, s, ok := Classify(err)
	if !ok {
		return connect.NewError(connect.CodeInternal, err)
	}
	cerr = connect.NewError(s.Code, err)
	info := &pb.ErrorInfo{Sentinel: name}
	var typed *Error
	if errors.As(err, &typed) {
		info.Detail = typed.Detail
	}
	if d, derr := connect.NewErrorDetail(info); derr == nil {
		cerr.AddDetail(d)
	}
	return cerr
}

// FromConnect gives a wire error back the sentinel the platform raised, so
// errors.Is(err, ErrNotFound) holds on the client exactly as it does on the
// server. The result is still a *connect.Error with the same code, so
// connect.CodeOf keeps working.
//
// An error carrying no recognizable sentinel is returned as-is rather than
// guessed at from its code: a client that cannot tell what happened is better
// served by the transport's own message than by a plausible mislabel.
func FromConnect(err error) error {
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		return err
	}
	for _, d := range cerr.Details() {
		msg, verr := d.Value()
		if verr != nil {
			continue
		}
		info, ok := msg.(*pb.ErrorInfo)
		if !ok {
			continue
		}
		for _, p := range published {
			if p.name == info.GetSentinel() {
				return connect.NewError(cerr.Code(), &Error{Err: p.err, Detail: info.GetDetail()})
			}
		}
	}
	return err
}
