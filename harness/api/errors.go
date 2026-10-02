package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	pb "github.com/pomerium/agentops/harness/api/pb"
)

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

type Sentinel struct {
	Err  error
	Code connect.Code
}

func Sentinels() map[pb.Sentinel]Sentinel {
	out := make(map[pb.Sentinel]Sentinel, len(published))
	for _, p := range published {
		out[p.name] = Sentinel{Err: p.err, Code: p.code}
	}
	return out
}

func Classify(err error) (pb.Sentinel, Sentinel, bool) {
	err = FromConnect(err)
	for _, p := range published {
		if errors.Is(err, p.err) {
			return p.name, Sentinel{Err: p.err, Code: p.code}, true
		}
	}
	return pb.Sentinel_SENTINEL_UNSPECIFIED, Sentinel{}, false
}

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
