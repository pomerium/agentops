// Package server is what an implementation of the generated Harness API handler
// shares with every other one: how its errors go on the wire, and how a
// subscription is served.
//
// The platform installs these, the conformance stub does, and so can a test
// fake in a client's own module, which is the point of their being here rather
// than inside the platform: a fake that rendered errors or framed a stream its
// own way would be testing its client against a server nobody runs.
package server

import (
	"context"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
)

// ErrorInterceptor puts a handler's errors on the wire as the published error
// set (api.ToConnect): the sentinel's Connect code plus the ErrorInfo detail that
// lets a client restore it. An implementation returns api sentinels and never
// thinks about codes.
func ErrorInterceptor() connect.Interceptor { return errorInterceptor{} }

type errorInterceptor struct{}

func (errorInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		res, err := next(ctx, req)
		return res, api.ToConnect(err)
	}
}

func (errorInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		return api.ToConnect(next(ctx, conn))
	}
}

func (errorInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
