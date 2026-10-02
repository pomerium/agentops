// Package client dials the Harness API: the generated Connect client, with what
// the network makes necessary added as interceptors, and a Subscribe that
// survives a dropped connection.
//
// New returns the generated harnessapipbconnect.HarnessAPIServiceClient itself,
// so a caller codes against the contract's own types and nothing in between.
// What New adds is invisible at the call site: the bearer token, re-read per
// request; a per-attempt timeout and retries for the errors worth retrying; and
// errors that still satisfy errors.Is against the api sentinels after crossing
// the wire.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

// Config configures a client.
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
	// RequestTimeout bounds each attempt of a unary call. It is applied through
	// the attempt's context rather than as http.Client.Timeout, so it holds
	// whatever HTTPClient is configured and never touches a subscription:
	// Timeout covers the whole exchange including the response body, and would
	// cut every stream at the same age.
	RequestTimeout time.Duration
	// Retries is how many times a retryable unary call is re-sent. Zero means the
	// default; a negative value disables retrying.
	Retries int
	// HTTPClient overrides the transport (tests). It carries subscriptions too,
	// so its Timeout must be zero.
	HTTPClient connect.HTTPClient
}

const (
	defaultRequestTimeout = 30 * time.Second
	defaultRetries        = 2
)

// New builds a Harness API client.
func New(cfg Config) (harnessapipbconnect.HarnessAPIServiceClient, error) {
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
	hc := cfg.HTTPClient
	if hc == nil {
		transport, err := newTransport(cfg.DialAddress, cfg.CAFile)
		if err != nil {
			return nil, err
		}
		hc = &http.Client{Transport: transport}
	}
	// Outermost first: errors are restored once, on the final outcome; retries
	// wrap the bearer so every attempt re-reads the token.
	return harnessapipbconnect.NewHarnessAPIServiceClient(hc, strings.TrimRight(cfg.BaseURL, "/"),
		connect.WithInterceptors(
			errorInterceptor(),
			retryInterceptor(cfg.Retries, cfg.RequestTimeout),
			bearerInterceptor{tokenFile: cfg.TokenFile},
		)), nil
}

// newTransport builds the transport, mirroring the AS client's dial-address
// override and private-CA handling.
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

// errorInterceptor restores the api sentinel on a unary call's error. A
// subscription's errors are restored by Subscribe, which reads them off the
// stream.
func errorInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			res, err := next(ctx, req)
			return res, api.FromConnect(err)
		}
	}
}

// retryInterceptor bounds each unary attempt and re-sends the failures a retry
// can fix.
//
// Retryable means the platform said it was unavailable, or the transport never
// got an answer. Everything else — a refusal, a conflict, a bad argument — is
// the platform's considered answer and re-sending it would only ask again.
// CreateSession is safe to retry because a conversation holds one live session:
// a retry of a create that succeeded is ErrConflict, not a second pod.
//
// A Prompt without an idempotency key is never re-sent. One the server accepted
// but whose response was lost has already started a turn, and nothing on the
// wire could tell a resent one from a new one: retrying it would run the same
// instruction twice. The key is what makes a resend recognizable.
func retryInterceptor(retries int, timeout time.Duration) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			retries := retries
			if p, ok := req.Any().(*pb.PromptRequest); ok && p.GetIdempotencyKey() == "" {
				retries = 0
			}
			for attempt := 0; ; attempt++ {
				actx, cancel := context.WithTimeout(ctx, timeout)
				res, err := next(actx, req)
				cancel()
				if err == nil || attempt >= retries || !retryable(err) || ctx.Err() != nil {
					return res, err
				}
				select {
				case <-ctx.Done():
					return nil, err
				case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
				}
			}
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
