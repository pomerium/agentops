// Package wire converts between the Harness API's Go types and their protobuf
// form, and maps the typed error set onto Connect codes and back.
//
// It sits between the two halves of the transport so both use one translation.
// A server that encoded errors one way and a client that decoded them another
// would produce exactly the failure this package exists to prevent: an
// errors.Is check that passes in-process and silently stops matching over the
// wire.
package wire

import (
	"errors"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

// --- errors ------------------------------------------------------------------

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
	{pb.Sentinel_SENTINEL_NOT_FOUND, api.ErrNotFound, connect.CodeNotFound},
	{pb.Sentinel_SENTINEL_UNKNOWN_REQUEST, api.ErrUnknownRequest, connect.CodeNotFound},
	{pb.Sentinel_SENTINEL_FORBIDDEN, api.ErrForbidden, connect.CodePermissionDenied},
	{pb.Sentinel_SENTINEL_CONFLICT, api.ErrConflict, connect.CodeAlreadyExists},
	{pb.Sentinel_SENTINEL_INVALID_STATE, api.ErrInvalidState, connect.CodeFailedPrecondition},
	{pb.Sentinel_SENTINEL_NOT_REVIVABLE, api.ErrNotRevivable, connect.CodeFailedPrecondition},
	{pb.Sentinel_SENTINEL_INVALID_ARGUMENT, api.ErrInvalidArgument, connect.CodeInvalidArgument},
	{pb.Sentinel_SENTINEL_UNAVAILABLE, api.ErrUnavailable, connect.CodeUnavailable},
	{pb.Sentinel_SENTINEL_QUOTA_EXCEEDED, api.ErrQuotaExceeded, connect.CodeResourceExhausted},
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
// would travel as.
//
// The match runs in the table's order, which is what makes it deterministic: an
// error wrapping two sentinels must classify the same way every time, and a map
// would decide by whichever key came up first. Anything unclassified reports
// false rather than being guessed at from its shape.
func Classify(err error) (pb.Sentinel, Sentinel, bool) {
	for _, p := range published {
		if errors.Is(err, p.err) {
			return p.name, Sentinel{Err: p.err, Code: p.code}, true
		}
	}
	return pb.Sentinel_SENTINEL_UNSPECIFIED, Sentinel{}, false
}

// ToConnect renders a service error for the wire: a Connect code for anything
// that only reads codes, plus an ErrorInfo detail naming the sentinel so the
// client wrapper can hand back an error that still satisfies errors.Is.
//
// An error matching no sentinel becomes CodeInternal with no detail. That is
// deliberate: an unclassified error is a bug in the service, and dressing it up
// as one of the published ones would tell a client something untrue about
// whether to retry.
func ToConnect(err error) error {
	if err == nil {
		return nil
	}
	for _, p := range published {
		if !errors.Is(err, p.err) {
			continue
		}
		cerr := connect.NewError(p.code, err)
		info := &pb.ErrorInfo{Sentinel: p.name}
		var typed *api.Error
		if errors.As(err, &typed) {
			info.Detail = typed.Detail
		}
		if d, derr := connect.NewErrorDetail(info); derr == nil {
			cerr.AddDetail(d)
		}
		return cerr
	}
	return connect.NewError(connect.CodeInternal, err)
}

// FromConnect turns a wire error back into the typed error the service raised,
// so callers of the Connect client can use the same errors.Is checks as callers
// of the in-process service.
//
// An error carrying no recognizable sentinel is returned as-is rather than
// guessed at from its code: a client that cannot tell what happened is better
// served by the transport's own message than by a plausible mislabel.
func FromConnect(err error) error {
	if err == nil {
		return nil
	}
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
				return &api.Error{Err: p.err, Detail: info.GetDetail()}
			}
		}
	}
	return err
}

// --- timestamps --------------------------------------------------------------

// Timestamp renders a time for the wire, mapping the zero time to nil so
// "unset" survives the round trip rather than arriving as the Unix epoch.
func Timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// Time reads a wire timestamp back, mapping nil to the zero time.
func Time(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// --- refs --------------------------------------------------------------------

// Ref renders a session ref for the wire. ClientID is dropped on purpose: the
// server takes the caller's identity from the verified assertion, so sending one
// would be sending a field that is ignored at best and believed at worst.
func Ref(ref api.SessionRef) *pb.SessionRef {
	return &pb.SessionRef{
		SessionId:       ref.SessionID,
		ConversationRef: ref.ConversationRef,
		IncludeTerminal: ref.IncludeTerminal,
	}
}

// RefFrom reads a session ref off the wire and stamps it with the client
// identity the handler verified. clientID is the only source of that field.
func RefFrom(ref *pb.SessionRef, clientID string) api.SessionRef {
	out := api.SessionRef{ClientID: clientID}
	if ref == nil {
		return out
	}
	out.SessionID = ref.GetSessionId()
	out.ConversationRef = ref.GetConversationRef()
	out.IncludeTerminal = ref.GetIncludeTerminal()
	return out
}

// --- views -------------------------------------------------------------------

// View renders a session view for the wire.
func View(v api.SessionView) *pb.SessionView {
	return &pb.SessionView{
		Id:              v.ID,
		ConversationRef: v.ConversationRef,
		State:           v.State,
		Template:        v.Template,
		LastSeq:         v.LastSeq,
	}
}

// ViewFrom reads a session view off the wire.
func ViewFrom(v *pb.SessionView) api.SessionView {
	if v == nil {
		return api.SessionView{}
	}
	return api.SessionView{
		ID:              v.GetId(),
		ConversationRef: v.GetConversationRef(),
		State:           v.GetState(),
		Template:        v.GetTemplate(),
		LastSeq:         v.GetLastSeq(),
	}
}
