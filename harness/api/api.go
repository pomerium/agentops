package api

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

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

func Errorf(sentinel error, format string, args ...any) error {
	return &Error{Err: sentinel, Detail: fmt.Sprintf(format, args...)}
}

const KeepaliveInterval = 20 * time.Second

func Timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func Time(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
