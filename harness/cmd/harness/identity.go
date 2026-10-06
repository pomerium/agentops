package main

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/apiserver"
)

func assertionIdentity(v *agentlink.Verifier) apiserver.Identify {
	return func(ctx context.Context, h http.Header) (string, error) {
		raw := h.Get(agentlink.AssertionMetadataKey)
		if raw == "" {
			return "", apiserver.ErrNoIdentity
		}
		claims, err := v.VerifyClaims(ctx, raw)
		if errors.Is(err, agentlink.ErrKeysUnavailable) {
			return "", connect.NewError(connect.CodeUnavailable, err)
		}
		if err != nil {
			return "", err
		}
		if claims.Subject == "" {
			return "", apiserver.ErrNoIdentity
		}
		return claims.Subject, nil
	}
}
