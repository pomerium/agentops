// Package client is the Harness API over the wire, satisfying the same Go
// interface the in-process implementation does.
//
// That equality is the point of the package. A client written against api.API —
// the Slack bot, its tests, anything later — runs unchanged whether the platform
// is a function call away or a Pomerium route away, so the boundary can move
// without the code above it noticing.
//
// What this adds over the generated stubs is everything the network makes
// necessary and the interface must not mention: the bearer token, re-read per
// request; retries for the errors that are worth retrying; and a subscription
// that survives a dropped connection by resuming from the last sequence it saw.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/api/wire"
)

// Config configures a Client.
type Config struct {
	// BaseURL is the API's route, e.g. https://harness-api.example.com.
	BaseURL string
	// TokenFile is the projected service-account token presented as the bearer.
	// It is re-read per request rather than cached: a projected token is rotated
	// under a running process, and a copy taken at startup stops working partway
	// through the day. Empty sends no Authorization header, which is for tests.
	TokenFile string
	// DialAddress, when set, is the address every request dials while the URL's
	// Host and SNI stay as written — for a public hostname that does not resolve
	// in-cluster.
	DialAddress string
	// CAFile optionally trusts a private PEM CA bundle.
	CAFile string
	// RequestTimeout bounds a unary call. It is deliberately NOT applied to
	// subscriptions; see the two transports below.
	RequestTimeout time.Duration
	// Retries is how many times a retryable unary call is re-sent. Zero means the
	// default; a negative value disables retrying.
	Retries int
	// KeepaliveInterval is how often the server promises a keepalive on an idle
	// subscription. MissedKeepalives of them elapsing with nothing at all arriving
	// is what makes the client declare a stream dead.
	KeepaliveInterval time.Duration
	// MissedKeepalives is how many intervals of silence are tolerated.
	MissedKeepalives int
	// HTTPClient overrides both transports (tests). When set, the streaming call
	// uses it too, so its timeout must be zero.
	HTTPClient connect.HTTPClient
	Logger     *slog.Logger
}

const (
	defaultRequestTimeout   = 30 * time.Second
	defaultRetries          = 2
	defaultMissedKeepalives = 3
	// reconnectBackoff paces subscription retries. Short, because the thing being
	// waited on is a proxy or a pod coming back, not a human.
	reconnectBackoff = 2 * time.Second
)

// silenceWindow is how long a subscription may say nothing at all — not even a
// keepalive — before it is presumed dead.
//
// It has one definition because both ends of a subscription's life depend on it:
// the opening frame is bounded by it, and so is every frame after. Two copies of
// the arithmetic could drift, and a client that waited longer to notice a dead
// handshake than a dead stream would be strictly harder to reason about than one
// that waits the same time for both.
func (c Config) silenceWindow() time.Duration {
	return time.Duration(c.MissedKeepalives) * c.KeepaliveInterval
}

// Client is the Harness API over Connect.
type Client struct {
	cfg Config
	log *slog.Logger
	// unary and stream are separate deliberately. A 30s http.Client.Timeout is
	// right for a verb and fatal for a subscription: Timeout covers the whole
	// exchange including the response body, so a single client would guillotine
	// every stream at thirty seconds, and the reconnect logic would paper over it
	// as an infinite reconnect loop rather than report it.
	unary  harnessapipbconnect.HarnessAPIServiceClient
	stream harnessapipbconnect.HarnessAPIServiceClient
}

var _ api.API = (*Client)(nil)

// New builds a Client.
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("harnessapi client: a base URL is required")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.Retries == 0 {
		cfg.Retries = defaultRetries
	}
	if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	if cfg.KeepaliveInterval == 0 {
		cfg.KeepaliveInterval = api.KeepaliveInterval
	}
	if cfg.MissedKeepalives == 0 {
		cfg.MissedKeepalives = defaultMissedKeepalives
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	unaryHC, streamHC := cfg.HTTPClient, cfg.HTTPClient
	if unaryHC == nil {
		transport, err := newTransport(cfg.DialAddress, cfg.CAFile)
		if err != nil {
			return nil, err
		}
		unaryHC = &http.Client{Timeout: cfg.RequestTimeout, Transport: transport}
		streamHC = &http.Client{Transport: transport}
	}

	base := strings.TrimRight(cfg.BaseURL, "/")
	opts := []connect.ClientOption{connect.WithInterceptors(bearerInterceptor{tokenFile: cfg.TokenFile})}
	return &Client{
		cfg:    cfg,
		log:    cfg.Logger,
		unary:  harnessapipbconnect.NewHarnessAPIServiceClient(unaryHC, base, opts...),
		stream: harnessapipbconnect.NewHarnessAPIServiceClient(streamHC, base, opts...),
	}, nil
}

// newTransport builds the shared transport, mirroring the AS client's
// dial-address override and private-CA handling.
func newTransport(dialAddr, caFile string) (*http.Transport, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pemBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file %q: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("no certificates found in CA file %q", caFile)
		}
		tlsCfg.RootCAs = pool
	}
	transport := &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
	if dialAddr != "" {
		transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, dialAddr)
		}
	}
	return transport, nil
}

// bearerInterceptor attaches the projected token to every call.
//
// It covers streaming as well as unary, which is the only way authentication
// stays structural: a unary-only interceptor would leave each streaming verb to
// remember the header for itself, and the one that forgets is the one nobody
// notices until it is refused in production.
type bearerInterceptor struct{ tokenFile string }

func (b bearerInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := setBearer(req.Header(), b.tokenFile); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (b bearerInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		// The interface has no error return here. A token that cannot be read
		// leaves the header unset and the server refuses with Unauthenticated,
		// which is both honest and visible to the caller on the first Receive.
		_ = setBearer(conn.RequestHeader(), b.tokenFile)
		return conn
	}
}

func (b bearerInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func setBearer(h http.Header, tokenFile string) error {
	if tokenFile == "" {
		return nil
	}
	b, err := os.ReadFile(tokenFile)
	if err != nil {
		return connect.NewError(connect.CodeUnauthenticated,
			fmt.Errorf("read workload token %s: %w", tokenFile, err))
	}
	h.Set("Authorization", "Bearer "+strings.TrimSpace(string(b)))
	return nil
}

// call runs a unary request, retrying the failures that a retry can fix.
//
// Retryable means the platform said it was unavailable, or the transport never
// got an answer. Everything else — a refusal, a conflict, a bad argument — is
// the platform's considered answer and re-sending it would only ask again.
// CreateSession is safe to retry because a client that supplies an idempotency
// key gets the session it already made rather than a second pod; one that does
// not has said it does not care.
func call[Req, Res any](ctx context.Context, c *Client,
	do func(context.Context, *connect.Request[Req]) (*connect.Response[Res], error), msg *Req,
) (*Res, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		res, err := do(ctx, connect.NewRequest(msg))
		if err == nil {
			return res.Msg, nil
		}
		lastErr = err
		if attempt >= c.cfg.Retries || !retryable(err) || ctx.Err() != nil {
			return nil, wire.FromConnect(lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, wire.FromConnect(lastErr)
		case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
		}
	}
}

func retryable(err error) bool {
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		// No Connect error at all means the request did not complete: a dial
		// failure, a reset connection, a proxy restarting.
		return true
	}
	switch cerr.Code() {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeUnknown:
		return true
	}
	return false
}

// --- verbs -------------------------------------------------------------------

// The client identity is absent from every request built here, and that is the
// contract rather than an omission: the server takes it from the assertion
// Pomerium verified on the route. req.ClientID is what an in-process caller
// fills in; over the wire it is simply not sent.

func (c *Client) CreateSession(ctx context.Context, req api.CreateSessionRequest) (api.SessionView, error) {
	res, err := call(ctx, c, c.unary.CreateSession, &pb.CreateSessionRequest{
		Template:             req.Template,
		ConversationRef:      req.ConversationRef,
		ParentSessionId:      req.ParentSessionID,
		ApprovalPrompt:       req.ApprovalPrompt,
		InitialPrompt:        req.InitialPrompt,
		SystemPromptAppendix: req.SystemPromptAppendix,
	})
	if err != nil {
		return api.SessionView{}, err
	}
	return wire.ViewFrom(res.GetSession()), nil
}

func (c *Client) Prompt(ctx context.Context, req api.PromptRequest) (api.PromptResult, error) {
	res, err := call(ctx, c, c.unary.Prompt, &pb.PromptRequest{
		Ref: wire.Ref(req.Ref), Content: req.Content,
	})
	if err != nil {
		return api.PromptResult{}, err
	}
	return api.PromptResult{TurnID: res.GetTurnId()}, nil
}

func (c *Client) RespondPermission(ctx context.Context, req api.RespondPermissionRequest) error {
	_, err := call(ctx, c, c.unary.RespondPermission, &pb.RespondPermissionRequest{
		Ref: wire.Ref(req.Ref), RequestId: req.RequestID, OptionId: req.OptionID,
	})
	return err
}

func (c *Client) EndSession(ctx context.Context, req api.EndSessionRequest) error {
	_, err := call(ctx, c, c.unary.EndSession, &pb.EndSessionRequest{
		Ref: wire.Ref(req.Ref), Reason: req.Reason,
	})
	return err
}

func (c *Client) GetSession(ctx context.Context, ref api.SessionRef) (api.SessionView, error) {
	res, err := call(ctx, c, c.unary.GetSession, &pb.GetSessionRequest{Ref: wire.Ref(ref)})
	if err != nil {
		return api.SessionView{}, err
	}
	return wire.ViewFrom(res.GetSession()), nil
}

func (c *Client) ListSessions(ctx context.Context, req api.ListSessionsRequest) ([]api.SessionView, error) {
	res, err := call(ctx, c, c.unary.ListSessions, &pb.ListSessionsRequest{
		LiveOnly: req.LiveOnly, UpdatedSince: wire.Timestamp(req.UpdatedSince),
	})
	if err != nil {
		return nil, err
	}
	out := make([]api.SessionView, 0, len(res.GetSessions()))
	for _, v := range res.GetSessions() {
		out = append(out, wire.ViewFrom(v))
	}
	return out, nil
}

func (c *Client) ListTemplates(ctx context.Context, _ string) ([]api.TemplateSummary, error) {
	res, err := call(ctx, c, c.unary.ListTemplates, &pb.ListTemplatesRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]api.TemplateSummary, 0, len(res.GetTemplates()))
	for _, t := range res.GetTemplates() {
		out = append(out, api.TemplateSummary{Name: t.GetName(), Description: t.GetDescription()})
	}
	return out, nil
}

func (c *Client) ListEvents(ctx context.Context, req api.EventsRequest) ([]api.Event, error) {
	res, err := call(ctx, c, c.unary.ListEvents, &pb.ListEventsRequest{
		Ref: wire.Ref(req.Ref), AfterSeq: req.AfterSeq, Limit: int32(req.Limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]api.Event, 0, len(res.GetEvents()))
	for _, ev := range res.GetEvents() {
		out = append(out, wire.EventFrom(ev))
	}
	return out, nil
}
