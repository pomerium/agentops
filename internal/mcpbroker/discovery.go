package mcpbroker

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// oauthEndpoints is the subset of authorization-server metadata the broker needs
// to drive an authorization-code + PKCE flow with dynamic client registration.
type oauthEndpoints struct {
	Issuer                string
	AuthorizationEndpoint string
	TokenEndpoint         string
	RegistrationEndpoint  string
	Scopes                []string
}

// discover resolves the OAuth endpoints protecting an MCP server. It follows the
// RFC 9728 protected-resource-metadata pointer from a 401 challenge (falling
// back to the well-known location), then fetches RFC 8414 authorization-server
// metadata.
func (b *Broker) discover(ctx context.Context, serverURL string) (oauthEndpoints, error) {
	prmURL, err := b.protectedResourceMetadataURL(ctx, serverURL)
	if err != nil {
		return oauthEndpoints{}, err
	}

	prm, err := oauthex.GetProtectedResourceMetadata(ctx, prmURL, serverURL, b.httpClient)
	if err != nil {
		return oauthEndpoints{}, fmt.Errorf("fetch protected resource metadata: %w", err)
	}
	if len(prm.AuthorizationServers) == 0 {
		return oauthEndpoints{}, fmt.Errorf("protected resource %q advertises no authorization servers", serverURL)
	}

	issuer := prm.AuthorizationServers[0]
	asm, err := auth.GetAuthServerMetadata(ctx, issuer, b.httpClient)
	if err != nil {
		return oauthEndpoints{}, fmt.Errorf("fetch authorization server metadata: %w", err)
	}
	if asm.AuthorizationEndpoint == "" || asm.TokenEndpoint == "" {
		return oauthEndpoints{}, fmt.Errorf("authorization server %q missing required endpoints", issuer)
	}

	scopes := asm.ScopesSupported
	if len(prm.ScopesSupported) > 0 {
		scopes = prm.ScopesSupported
	}
	return oauthEndpoints{
		Issuer:                issuer,
		AuthorizationEndpoint: asm.AuthorizationEndpoint,
		TokenEndpoint:         asm.TokenEndpoint,
		RegistrationEndpoint:  asm.RegistrationEndpoint,
		Scopes:                scopes,
	}, nil
}

// protectedResourceMetadataURL determines where the protected-resource metadata
// document lives: it prefers the resource_metadata parameter from the server's
// WWW-Authenticate challenge, and otherwise falls back to the well-known path on
// the server's origin.
func (b *Broker) protectedResourceMetadataURL(ctx context.Context, serverURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL, nil)
	if err != nil {
		return "", fmt.Errorf("build discovery request: %w", err)
	}
	resp, err := b.httpClient.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if challenges, perr := oauthex.ParseWWWAuthenticate(resp.Header.Values("WWW-Authenticate")); perr == nil {
			for _, c := range challenges {
				if rm := c.Params["resource_metadata"]; rm != "" {
					return rm, nil
				}
			}
		}
	}
	return defaultProtectedResourceMetadataURL(serverURL)
}

func defaultProtectedResourceMetadataURL(serverURL string) (string, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("parse server URL %q: %w", serverURL, err)
	}
	u.Path = "/.well-known/oauth-protected-resource"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
