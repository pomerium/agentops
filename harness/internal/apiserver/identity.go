package apiserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
)

// ErrNoIdentity reports a request that reached the API with no usable assertion.
// It is always a deployment fault — the route is the only network path here and
// it stamps the header — so it gets its own error to be diagnosable in one line.
var ErrNoIdentity = errors.New("apiserver: request carries no verified client identity")

// Identify resolves a request's client id from its headers. It returns the
// verified subject and nothing else; a request that cannot be identified is
// refused rather than defaulted. The production one verifies Pomerium's
// assertion and lives with the platform wiring (cmd/harness), so this package
// stays free of it and the conformance stub can serve the API alone.
type Identify func(ctx context.Context, h http.Header) (string, error)

// clientIDKey carries the verified client id down the request context.
type clientIDKey struct{}

// clientIDFrom reads the verified client id a request was admitted under. The
// second return is false only for a request that never went through the
// middleware, which the handler treats as a programming error rather than as an
// anonymous caller.
func clientIDFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(clientIDKey{}).(string)
	return id, ok && id != ""
}

// identityInterceptor turns Pomerium's assertion into the one client identity
// every verb is scoped to.
//
// It is a Connect interceptor rather than HTTP middleware so a refusal is a
// Connect error like every other: it carries a code the client wrapper
// understands, it lands in the same per-verb metrics as everything else, and it
// goes through the transport's own error path instead of a plain-text 401 that
// no client of this API knows how to read. Streaming is wrapped too — a
// subscription admitted without an identity is the one call that reads another
// client's log.
//
// It also logs each client the first time it is seen. That line is how a
// deployment learns what Pomerium actually mints for its workloads, which is
// what a ClientBinding's subject has to match and is not something to guess at.
type identityInterceptor struct {
	identify Identify
	log      *slog.Logger
	seen     *seenClients
}

// admit resolves and records the caller, returning the context every handler
// reads its client id from.
func (i *identityInterceptor) admit(ctx context.Context, h http.Header, procedure string) (context.Context, error) {
	clientID, err := i.identify(ctx, h)
	if err != nil {
		i.log.WarnContext(ctx, "refusing an unidentified client API request",
			"procedure", procedure, "err", err)
		return nil, connect.NewError(connect.CodeUnauthenticated, err)
	}
	if i.seen.first(clientID) {
		i.log.InfoContext(ctx, "admitted a client on the harness API",
			"client_id", clientID, "procedure", procedure)
	}
	return context.WithValue(ctx, clientIDKey{}, clientID), nil
}

func (i *identityInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, err := i.admit(ctx, req.Header(), req.Spec().Procedure)
		if err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (i *identityInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		ctx, err := i.admit(ctx, conn.RequestHeader(), conn.Spec().Procedure)
		if err != nil {
			return err
		}
		return next(ctx, conn)
	}
}

func (i *identityInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
