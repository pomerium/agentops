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

type Config struct {
	BaseURL string

	TokenFile string

	DialAddress string

	CAFile string

	RequestTimeout time.Duration

	Retries int

	HTTPClient connect.HTTPClient
}

const (
	defaultRequestTimeout = 30 * time.Second
	defaultRetries        = 2
)

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

	return harnessapipbconnect.NewHarnessAPIServiceClient(hc, strings.TrimRight(cfg.BaseURL, "/"),
		connect.WithInterceptors(
			errorInterceptor(),
			retryInterceptor(cfg.Retries, cfg.RequestTimeout),
			bearerInterceptor{tokenFile: cfg.TokenFile},
		)), nil
}

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

func errorInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			res, err := next(ctx, req)
			return res, api.FromConnect(err)
		}
	}
}

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

		return true
	}
	switch cerr.Code() {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeUnknown:
		return true
	}
	return false
}

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
