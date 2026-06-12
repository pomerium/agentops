// Package mcpbroker is the stateful MCP credential broker. On behalf of each
// Slack user it discovers an MCP server's OAuth configuration, performs dynamic
// client registration and an authorization-code + PKCE flow, persists the
// resulting per-(user, server) tokens, refreshes them proactively, and reports
// whether a user is currently connected to a given server.
package mcpbroker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"golang.org/x/oauth2"

	"github.com/pomerium/agentops/internal/chatops/store"
	"github.com/pomerium/agentops/internal/telemetry"
)

// ErrNotConnected indicates the user has no usable credential for a server.
var ErrNotConnected = errors.New("mcpbroker: user not connected to server")

// callbackPath is appended to the configured redirect base URL to form the
// OAuth redirect URI served by the gateway.
const callbackPath = "/oauth/callback"

// connectPath is appended to the configured redirect base URL to form the short
// per-flow connect URL the gateway 302-redirects to the real authorize URL. It
// keeps the long provider URL (with PKCE/state/client_id) out of the Slack
// button's hover tooltip.
const connectPath = "/connect"

// ErrFlowNotFound indicates the in-flight OAuth flow is unknown or has expired.
var ErrFlowNotFound = errors.New("mcpbroker: oauth flow not found or expired")

// ConnectFunc attempts a connection to an MCP server using the supplied HTTP
// client (which carries the user's bearer token). It returns nil if the server
// accepts the credential. It is injectable for testing.
type ConnectFunc func(ctx context.Context, serverURL string, httpClient *http.Client) error

// Options configures a Broker.
type Options struct {
	// HTTPClient is used for all OAuth/MCP HTTP requests. Defaults to
	// http.DefaultClient.
	HTTPClient *http.Client
	// RedirectBaseURL is the externally reachable base URL of this app, used to
	// construct the OAuth redirect URI (RedirectBaseURL + "/oauth/callback").
	RedirectBaseURL string
	// ClientName is the human-readable client name used in dynamic client
	// registration.
	ClientName string
	// FlowTTL bounds how long an in-flight OAuth flow remains valid. Defaults
	// to 15 minutes.
	FlowTTL time.Duration
	// Connect overrides the MCP connection probe (for tests). Defaults to a
	// real streamable-HTTP MCP connection.
	Connect ConnectFunc
	// Now overrides the clock (for tests).
	Now func() time.Time
	// Logger is used for diagnostics. Defaults to slog.Default().
	Logger *slog.Logger
}

// Broker is the credential broker.
type Broker struct {
	store           *store.Store
	httpClient      *http.Client
	redirectBaseURL string
	clientName      string
	flowTTL         time.Duration
	connect         ConnectFunc
	now             func() time.Time
	log             *slog.Logger
	tel             *telemetry.Component
}

// New constructs a Broker.
func New(s *store.Store, opts Options) *Broker {
	b := &Broker{
		store:           s,
		httpClient:      opts.HTTPClient,
		redirectBaseURL: strings.TrimRight(opts.RedirectBaseURL, "/"),
		clientName:      opts.ClientName,
		flowTTL:         opts.FlowTTL,
		connect:         opts.Connect,
		now:             opts.Now,
		log:             opts.Logger,
	}
	if b.httpClient == nil {
		b.httpClient = http.DefaultClient
	}
	if b.clientName == "" {
		b.clientName = "agentops"
	}
	if b.flowTTL == 0 {
		b.flowTTL = 15 * time.Minute
	}
	if b.connect == nil {
		b.connect = realConnect
	}
	if b.now == nil {
		b.now = time.Now
	}
	if b.log == nil {
		b.log = slog.Default()
	}
	b.tel = telemetry.New(b.log, "mcpbroker", slog.LevelDebug)
	return b
}

func (b *Broker) redirectURI() string { return b.redirectBaseURL + callbackPath }

// connectURL is the short, app-domain URL placed on the Slack connect button;
// the gateway resolves it back to the provider authorize URL via AuthorizeURL.
func (b *Broker) connectURL(flowID string) string {
	return b.redirectBaseURL + connectPath + "/" + flowID
}

// StartAuthRequest captures who is authorizing to which server, and the Slack
// context to return to once the flow completes.
type StartAuthRequest struct {
	SlackUserID    string
	SlackTeamID    string
	SlackChannelID string
	SlackThreadTS  string
	ServerName     string
	ServerURL      string
	WorkflowName   string
}

// StartAuthFlow discovers the server's OAuth configuration, registers a client
// via DCR, persists in-flight PKCE state, and returns the authorization URL the
// user should open. The gateway posts this URL into the Slack thread.
func (b *Broker) StartAuthFlow(ctx context.Context, req StartAuthRequest) (string, error) {
	ctx = telemetry.With(ctx, "user", req.SlackUserID, "server", req.ServerName)
	ctx, op := b.tel.Start(ctx, "StartAuthFlow")
	defer op.Complete()

	endpoints, err := b.discover(ctx, req.ServerURL)
	if err != nil {
		return "", op.Failure(err)
	}
	b.tel.Debug(ctx, "discovered oauth endpoints",
		"issuer", endpoints.Issuer, "authorization_endpoint", endpoints.AuthorizationEndpoint,
		"token_endpoint", endpoints.TokenEndpoint, "registration_endpoint", endpoints.RegistrationEndpoint)

	reg, err := b.registerClient(ctx, endpoints)
	if err != nil {
		return "", op.Failure(err)
	}
	b.tel.Debug(ctx, "dynamic client registered", "client_id", reg.ClientID)
	regJSON, _ := json.Marshal(reg)

	state, err := randomToken()
	if err != nil {
		return "", err
	}
	flowID, err := randomToken()
	if err != nil {
		return "", err
	}
	verifier := oauth2.GenerateVerifier()

	now := b.now()
	flow := store.OAuthFlow{
		FlowID:               flowID,
		State:                state,
		SlackUserID:          req.SlackUserID,
		SlackTeamID:          req.SlackTeamID,
		SlackChannelID:       req.SlackChannelID,
		SlackThreadTS:        req.SlackThreadTS,
		ServerName:           req.ServerName,
		ServerURL:            req.ServerURL,
		WorkflowName:         req.WorkflowName,
		AuthURL:              endpoints.AuthorizationEndpoint,
		TokenURL:             endpoints.TokenEndpoint,
		RegistrationEndpoint: endpoints.RegistrationEndpoint,
		ClientID:             reg.ClientID,
		ClientSecret:         reg.ClientSecret,
		Scopes:               strings.Join(endpoints.Scopes, " "),
		PKCEVerifier:         verifier,
		RedirectURI:          b.redirectURI(),
		ExpiresAt:            now.Add(b.flowTTL),
	}
	_ = regJSON // registration metadata is reconstructable from the stored fields
	if err := b.store.CreateOAuthFlow(ctx, flow); err != nil {
		return "", op.Failure(fmt.Errorf("persist oauth flow: %w", err))
	}
	b.tel.Debug(ctx, "auth flow created; returning connect URL", "flow_id", flowID)
	return b.connectURL(flowID), nil
}

// AuthorizeURL resolves a short connect URL's flow ID back to the provider
// authorization URL (rebuilt from the persisted flow). It does not consume the
// flow — HandleCallback still needs it by state. Returns ErrFlowNotFound if the
// flow is unknown or expired.
func (b *Broker) AuthorizeURL(ctx context.Context, flowID string) (string, error) {
	flow, err := b.store.GetOAuthFlow(ctx, flowID)
	if errors.Is(err, store.ErrNotFound) {
		return "", ErrFlowNotFound
	}
	if err != nil {
		return "", err
	}
	if !flow.ExpiresAt.IsZero() && b.now().After(flow.ExpiresAt) {
		_ = b.store.DeleteOAuthFlow(ctx, flow.FlowID)
		return "", ErrFlowNotFound
	}
	cfg := &oauth2.Config{
		ClientID:     flow.ClientID,
		ClientSecret: flow.ClientSecret,
		Endpoint:     oauth2.Endpoint{AuthURL: flow.AuthURL, TokenURL: flow.TokenURL},
		RedirectURL:  flow.RedirectURI,
		Scopes:       splitScopes(flow.Scopes),
	}
	return cfg.AuthCodeURL(flow.State, oauth2.S256ChallengeOption(flow.PKCEVerifier)), nil
}

func (b *Broker) registerClient(ctx context.Context, e oauthEndpoints) (*oauthex.ClientRegistrationResponse, error) {
	if e.RegistrationEndpoint == "" {
		return nil, fmt.Errorf("authorization server %q does not support dynamic client registration", e.Issuer)
	}
	meta := &oauthex.ClientRegistrationMetadata{
		ClientName:              b.clientName,
		RedirectURIs:            []string{b.redirectURI()},
		GrantTypes:              []string{"authorization_code", "refresh_token"},
		ResponseTypes:           []string{"code"},
		TokenEndpointAuthMethod: "client_secret_basic",
		Scope:                   strings.Join(e.Scopes, " "),
	}
	reg, err := oauthex.RegisterClient(ctx, e.RegistrationEndpoint, meta, b.httpClient)
	if err != nil {
		return nil, fmt.Errorf("dynamic client registration: %w", err)
	}
	return reg, nil
}

// CallbackResult carries the Slack context needed to resume the user's flow
// after a successful OAuth callback.
type CallbackResult struct {
	SlackUserID    string
	SlackTeamID    string
	SlackChannelID string
	SlackThreadTS  string
	ServerName     string
	WorkflowName   string
}

// HandleCallback exchanges an authorization code for tokens, persists the
// resulting credential (including the registration needed to refresh later),
// consumes the flow, and returns the Slack context to resume.
func (b *Broker) HandleCallback(ctx context.Context, code, state string) (CallbackResult, error) {
	ctx, op := b.tel.Start(ctx, "HandleCallback")
	defer op.Complete()

	flow, err := b.store.GetOAuthFlowByState(ctx, state)
	if errors.Is(err, store.ErrNotFound) {
		return CallbackResult{}, op.Failure(fmt.Errorf("unknown or expired oauth state"))
	}
	if err != nil {
		return CallbackResult{}, op.Failure(err)
	}
	ctx = telemetry.With(ctx, "user", flow.SlackUserID, "server", flow.ServerName)
	if !flow.ExpiresAt.IsZero() && b.now().After(flow.ExpiresAt) {
		_ = b.store.DeleteOAuthFlow(ctx, flow.FlowID)
		return CallbackResult{}, op.Failure(fmt.Errorf("oauth flow expired"))
	}

	cfg := &oauth2.Config{
		ClientID:     flow.ClientID,
		ClientSecret: flow.ClientSecret,
		Endpoint:     oauth2.Endpoint{AuthURL: flow.AuthURL, TokenURL: flow.TokenURL},
		RedirectURL:  flow.RedirectURI,
		Scopes:       splitScopes(flow.Scopes),
	}
	tok, err := cfg.Exchange(b.ctxWithClient(ctx), code, oauth2.VerifierOption(flow.PKCEVerifier))
	if err != nil {
		return CallbackResult{}, op.Failure(fmt.Errorf("exchange authorization code: %w", err))
	}
	b.tel.Debug(ctx, "exchanged code for token", "has_refresh", tok.RefreshToken != "")

	cred := store.MCPCredential{
		SlackUserID:  flow.SlackUserID,
		ServerName:   flow.ServerName,
		ServerURL:    flow.ServerURL,
		AccessToken:  tok.AccessToken,
		RefreshToken: tok.RefreshToken,
		TokenType:    tokenType(tok),
		Expiry:       tok.Expiry,
		AuthURL:      flow.AuthURL,
		TokenURL:     flow.TokenURL,
		ClientID:     flow.ClientID,
		ClientSecret: flow.ClientSecret,
		Scopes:       flow.Scopes,
	}
	if err := b.store.SaveMCPCredential(ctx, cred); err != nil {
		return CallbackResult{}, op.Failure(fmt.Errorf("persist credential: %w", err))
	}
	if err := b.store.DeleteOAuthFlow(ctx, flow.FlowID); err != nil {
		b.log.WarnContext(ctx, "failed to delete consumed oauth flow", "flow_id", flow.FlowID, "err", err)
	}

	return CallbackResult{
		SlackUserID:    flow.SlackUserID,
		SlackTeamID:    flow.SlackTeamID,
		SlackChannelID: flow.SlackChannelID,
		SlackThreadTS:  flow.SlackThreadTS,
		ServerName:     flow.ServerName,
		WorkflowName:   flow.WorkflowName,
	}, nil
}

// AccessToken returns a currently-valid access token for (user, server),
// refreshing and persisting it if necessary. The serverURL must match the URL
// the credential was issued for; a mismatch is treated as not-connected so a
// token minted for one server is never sent to a different one. Returns
// ErrNotConnected if the user has no usable credential.
func (b *Broker) AccessToken(ctx context.Context, slackUserID, serverName, serverURL string) (string, error) {
	ctx = telemetry.With(ctx, "user", slackUserID, "server", serverName)
	ctx, op := b.tel.Start(ctx, "AccessToken")
	defer op.Complete()

	cred, err := b.store.GetMCPCredential(ctx, slackUserID, serverName)
	if errors.Is(err, store.ErrNotFound) {
		// Not connected is an expected outcome, not a failure — return the
		// sentinel unwrapped so callers can errors.Is/== it.
		return "", ErrNotConnected
	}
	if err != nil {
		return "", op.Failure(err)
	}
	if !sameServer(cred.ServerURL, serverURL) {
		return "", ErrNotConnected
	}
	ts := b.tokenSource(ctx, cred)
	tok, err := ts.Token()
	if err != nil {
		return "", op.Failure(fmt.Errorf("obtain access token: %w", err))
	}
	return tok.AccessToken, nil
}

// ConnectionStatus reports whether the user currently has a working connection
// to the server: a stored credential whose (possibly refreshed) token is
// accepted by the server. A missing credential or a rejected token reports
// not-connected without an error (the caller should prompt for auth).
func (b *Broker) ConnectionStatus(ctx context.Context, slackUserID, serverName, serverURL string) (bool, error) {
	ctx = telemetry.With(ctx, "user", slackUserID, "server", serverName)
	ctx, op := b.tel.Start(ctx, "ConnectionStatus")
	defer op.Complete()

	cred, err := b.store.GetMCPCredential(ctx, slackUserID, serverName)
	if errors.Is(err, store.ErrNotFound) {
		b.tel.Debug(ctx, "no stored credential; needs auth")
		return false, nil
	}
	if err != nil {
		return false, op.Failure(err)
	}
	if !sameServer(cred.ServerURL, serverURL) {
		// Stored credential was issued for a different server URL under the
		// same name; force a fresh auth rather than reuse it.
		b.tel.Debug(ctx, "stored credential is for a different server URL; needs auth",
			"stored_url", cred.ServerURL, "requested_url", serverURL)
		return false, nil
	}
	ts := b.tokenSource(ctx, cred)
	if _, err := ts.Token(); err != nil {
		b.tel.Info(ctx, "stored token unusable; needs reauth", "err", err)
		return false, nil
	}
	httpClient := oauth2.NewClient(b.ctxWithClient(ctx), ts)
	if err := b.connect(ctx, serverURL, httpClient); err != nil {
		b.tel.Info(ctx, "mcp connection probe failed; needs reauth", "err", err)
		return false, nil
	}
	b.tel.Debug(ctx, "mcp connection probe succeeded")
	return true, nil
}

// tokenSource builds a refreshing oauth2.TokenSource for a credential that
// persists any refreshed token back to the store.
func (b *Broker) tokenSource(ctx context.Context, cred store.MCPCredential) oauth2.TokenSource {
	cfg := &oauth2.Config{
		ClientID:     cred.ClientID,
		ClientSecret: cred.ClientSecret,
		Endpoint:     oauth2.Endpoint{AuthURL: cred.AuthURL, TokenURL: cred.TokenURL},
		Scopes:       splitScopes(cred.Scopes),
	}
	tok := &oauth2.Token{
		AccessToken:  cred.AccessToken,
		RefreshToken: cred.RefreshToken,
		TokenType:    cred.TokenType,
		Expiry:       cred.Expiry,
	}
	base := cfg.TokenSource(b.ctxWithClient(ctx), tok)
	return &persistingTokenSource{
		base:  base,
		store: b.store,
		cred:  cred,
		log:   b.log,
	}
}

// ctxWithClient returns a context carrying the broker's HTTP client so the
// oauth2 package uses it for token endpoint calls.
func (b *Broker) ctxWithClient(ctx context.Context) context.Context {
	return context.WithValue(ctx, oauth2.HTTPClient, b.httpClient)
}

// persistingTokenSource wraps a refreshing token source and writes any newly
// minted token back to the store.
type persistingTokenSource struct {
	base  oauth2.TokenSource
	store *store.Store
	cred  store.MCPCredential
	log   *slog.Logger
}

func (p *persistingTokenSource) Token() (*oauth2.Token, error) {
	tok, err := p.base.Token()
	if err != nil {
		return nil, err
	}
	if tok.AccessToken == p.cred.AccessToken && tok.RefreshToken == p.cred.RefreshToken {
		return tok, nil // unchanged; nothing to persist
	}
	rotated := tok.RefreshToken != "" && tok.RefreshToken != p.cred.RefreshToken
	updated := p.cred
	updated.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		updated.RefreshToken = tok.RefreshToken
	}
	updated.TokenType = tokenType(tok)
	updated.Expiry = tok.Expiry
	if err := p.store.SaveMCPCredential(context.Background(), updated); err != nil {
		// If the refresh token rotated, failing to persist it means the next
		// call would re-present the now-consumed old refresh token and lose
		// access. Surface that as an error rather than silently proceeding.
		if rotated {
			return nil, fmt.Errorf("persist rotated refresh token: %w", err)
		}
		p.log.Warn("failed to persist refreshed token", "server", p.cred.ServerName, "err", err)
	} else {
		p.cred = updated
	}
	return tok, nil
}

func realConnect(ctx context.Context, serverURL string, httpClient *http.Client) error {
	transport := &mcp.StreamableClientTransport{Endpoint: serverURL, HTTPClient: httpClient}
	client := mcp.NewClient(&mcp.Implementation{Name: "agentops", Version: "0.1.0"}, nil)
	sess, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return err
	}
	return sess.Close()
}

func tokenType(t *oauth2.Token) string {
	if tt := t.Type(); tt != "" {
		return tt
	}
	return "Bearer"
}

// sameServer reports whether two MCP server URLs refer to the same server,
// comparing scheme+host+path case-insensitively for the host and ignoring a
// trailing slash and any query/fragment.
func sameServer(a, b string) bool {
	return normalizeServerURL(a) == normalizeServerURL(b)
}

func normalizeServerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return strings.ToLower(strings.TrimRight(raw, "/"))
	}
	host := strings.ToLower(u.Host)
	path := strings.TrimRight(u.Path, "/")
	return strings.ToLower(u.Scheme) + "://" + host + path
}

func splitScopes(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.Fields(s)
}

func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
