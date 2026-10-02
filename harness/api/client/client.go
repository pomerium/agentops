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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

type Option func(*options)

type options struct {
	tokenFile      string
	dialAddress    string
	caFile         string
	requestTimeout time.Duration
	retries        int
	httpClient     connect.HTTPClient
}

func WithTokenFile(path string) Option { return func(o *options) { o.tokenFile = path } }

func WithDialAddress(addr string) Option { return func(o *options) { o.dialAddress = addr } }

func WithCAFile(path string) Option { return func(o *options) { o.caFile = path } }

func WithRequestTimeout(d time.Duration) Option { return func(o *options) { o.requestTimeout = d } }

func WithRetries(n int) Option { return func(o *options) { o.retries = max(0, n) } }

func WithHTTPClient(c connect.HTTPClient) Option { return func(o *options) { o.httpClient = c } }

func New(baseURL string, opts ...Option) (harnessapipbconnect.HarnessAPIServiceClient, error) {
	if baseURL == "" {
		return nil, errors.New("harnessapi client: a base URL is required")
	}
	o := options{requestTimeout: 30 * time.Second, retries: 2}
	for _, opt := range opts {
		opt(&o)
	}
	hc := o.httpClient
	if hc == nil {
		transport, err := newTransport(o.dialAddress, o.caFile)
		if err != nil {
			return nil, err
		}
		hc = &http.Client{Transport: transport}
	}

	return harnessapipbconnect.NewHarnessAPIServiceClient(hc, strings.TrimRight(baseURL, "/"),
		connect.WithInterceptors(
			errorInterceptor(),
			retryInterceptor(o.retries, o.requestTimeout),
			bearerInterceptor{tokenFile: o.tokenFile},
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
			if req.Spec().IdempotencyLevel == connect.IdempotencyUnknown && !hasIdempotencyKey(req.Any()) {
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

func hasIdempotencyKey(msg any) bool {
	m, ok := msg.(proto.Message)
	if !ok {
		return false
	}
	r := m.ProtoReflect()
	fd := r.Descriptor().Fields().ByName("idempotency_key")
	return fd != nil && fd.Kind() == protoreflect.StringKind && r.Get(fd).String() != ""
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
