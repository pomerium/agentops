package apiserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
)

var ErrNoIdentity = errors.New("apiserver: request carries no verified client identity")

type Identify func(ctx context.Context, h http.Header) (string, error)

type clientIDKey struct{}

func ClientID(ctx context.Context) (string, error) {
	id, ok := ctx.Value(clientIDKey{}).(string)
	if !ok || id == "" {
		return "", connect.NewError(connect.CodeUnauthenticated, ErrNoIdentity)
	}
	return id, nil
}

func WithClientID(ctx context.Context, clientID string) context.Context {
	return context.WithValue(ctx, clientIDKey{}, clientID)
}

type identityInterceptor struct {
	identify Identify
	log      *slog.Logger
	seen     *seenClients
}

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
	return WithClientID(ctx, clientID), nil
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
