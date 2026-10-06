package agentlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/pomeriumtls"
)

const AssertionMetadataKey = "x-pomerium-jwt-assertion"

const (
	claimRunID          = "run_id"
	claimNamespace      = "act.kubernetes.io.namespace"
	claimServiceAccount = "act.kubernetes.io.serviceaccount.name"
	claimPodName        = "act.kubernetes.io.pod.name"
	claimPodUID         = "act.kubernetes.io.pod.uid"
)

const defaultJWKSPath = "/.well-known/pomerium/jwks.json"

var signatureAlgs = []jose.SignatureAlgorithm{jose.ES256, jose.ES384, jose.ES512, jose.RS256, jose.EdDSA}

const clockSkew = time.Minute

type VerifiedClaims struct {
	Subject  string
	Audience []string
	Claims   map[string]any
}

type Assertion struct {
	RunID    string
	Executor agenticrun.Executor
	Subject  string
	Audience []string
}

var ErrMissingRunClaims = errors.New("assertion carries no run_id — check pomerium jwt_claims_headers")

type VerifierOption func(*verifierOptions)

type verifierOptions struct {
	audience    string
	jwksURL     string
	dialAddress string
	caFile      string
	httpClient  *http.Client
	logger      *slog.Logger
}

func WithAudience(aud string) VerifierOption { return func(o *verifierOptions) { o.audience = aud } }

func WithJWKSURL(u string) VerifierOption { return func(o *verifierOptions) { o.jwksURL = u } }

func WithDialAddress(addr string) VerifierOption {
	return func(o *verifierOptions) { o.dialAddress = addr }
}

func WithCAFile(path string) VerifierOption { return func(o *verifierOptions) { o.caFile = path } }

func WithHTTPClient(c *http.Client) VerifierOption {
	return func(o *verifierOptions) { o.httpClient = c }
}

func WithVerifierLogger(l *slog.Logger) VerifierOption {
	return func(o *verifierOptions) { o.logger = l }
}

type Verifier struct {
	issuer  string
	cfg     verifierOptions
	jwksURL string
	hc      *http.Client
	log     *slog.Logger

	mu        sync.Mutex
	keys      map[string]jose.JSONWebKey
	lastFetch time.Time
}

const jwksRefetchInterval = 30 * time.Second

func NewVerifier(issuer string, opts ...VerifierOption) (*Verifier, error) {
	if issuer == "" {
		return nil, errors.New("harness assertion verifier: issuer is required")
	}
	var o verifierOptions
	for _, opt := range opts {
		opt(&o)
	}
	jwksURL := o.jwksURL
	if jwksURL == "" {
		var err error
		if jwksURL, err = deriveJWKSURL(issuer); err != nil {
			return nil, err
		}
	}
	hc := o.httpClient
	if hc == nil {
		var err error
		if hc, err = newJWKSClient(o.dialAddress, o.caFile); err != nil {
			return nil, err
		}
	}
	log := o.logger
	if log == nil {
		log = slog.Default()
	}
	return &Verifier{
		issuer: issuer, cfg: o, jwksURL: jwksURL, hc: hc, log: log,
		keys: map[string]jose.JSONWebKey{},
	}, nil
}

func deriveJWKSURL(issuer string) (string, error) {
	raw := issuer
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("harness assertion verifier: invalid issuer %q", issuer)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = defaultJWKSPath
	}
	return u.String(), nil
}

func newJWKSClient(dialAddr, caFile string) (*http.Client, error) {
	transport, err := pomeriumtls.Transport(dialAddr, caFile)
	if err != nil {
		return nil, err
	}
	return &http.Client{Timeout: 15 * time.Second, Transport: transport}, nil
}

func (v *Verifier) Verify(ctx context.Context, raw string) (*Assertion, error) {
	vc, err := v.VerifyClaims(ctx, raw)
	if err != nil {
		return nil, err
	}
	a := &Assertion{
		Subject:  vc.Subject,
		Audience: vc.Audience,
		RunID:    claimString(vc.Claims, claimRunID),
		Executor: agenticrun.Executor{
			Namespace:      claimString(vc.Claims, claimNamespace),
			ServiceAccount: claimString(vc.Claims, claimServiceAccount),
			PodName:        claimString(vc.Claims, claimPodName),
			PodUID:         claimString(vc.Claims, claimPodUID),
		},
	}
	if a.RunID == "" {
		return nil, ErrMissingRunClaims
	}
	if err := a.Executor.Validate(); err != nil {
		return nil, fmt.Errorf("assertion pod identity incomplete (%w) — check pomerium jwt_claims_headers lists every act.kubernetes.io.* claim", err)
	}
	return a, nil
}

func (v *Verifier) VerifyClaims(ctx context.Context, raw string) (*VerifiedClaims, error) {
	sig, err := jose.ParseSigned(raw, signatureAlgs)
	if err != nil {
		return nil, fmt.Errorf("parse assertion: %w", err)
	}
	if len(sig.Signatures) != 1 {
		return nil, fmt.Errorf("assertion must carry exactly one signature, got %d", len(sig.Signatures))
	}
	key, err := v.key(ctx, sig.Signatures[0].Header.KeyID)
	if err != nil {
		return nil, err
	}
	payload, err := sig.Verify(key)
	if err != nil {
		return nil, fmt.Errorf("invalid assertion signature: %w", err)
	}

	var std jwt.Claims
	if err := json.Unmarshal(payload, &std); err != nil {
		return nil, fmt.Errorf("decode assertion claims: %w", err)
	}
	expected := jwt.Expected{Issuer: v.issuer, Time: time.Now()}
	if err := std.ValidateWithLeeway(expected, clockSkew); err != nil {
		return nil, fmt.Errorf("assertion claims rejected: %w", err)
	}
	if err := v.checkAudience(std.Audience); err != nil {
		return nil, err
	}

	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("decode assertion claims: %w", err)
	}
	return &VerifiedClaims{Subject: std.Subject, Audience: std.Audience, Claims: claims}, nil
}

func (v *Verifier) checkAudience(aud jwt.Audience) error {
	if v.cfg.audience == "" || aud.Contains(v.cfg.audience) {
		return nil
	}
	return fmt.Errorf("assertion audience %v does not include %q", []string(aud), v.cfg.audience)
}

func (v *Verifier) key(ctx context.Context, kid string) (*jose.JSONWebKey, error) {
	v.mu.Lock()
	if k, ok := v.keys[kid]; ok {
		v.mu.Unlock()
		return &k, nil
	}
	if !v.lastFetch.IsZero() && time.Now().Sub(v.lastFetch) < jwksRefetchInterval {
		v.mu.Unlock()
		return nil, fmt.Errorf("no assertion signing key with kid %q (refetch throttled)", kid)
	}
	v.lastFetch = time.Now()
	v.mu.Unlock()

	keys, err := v.fetchJWKS(ctx)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	for _, k := range keys {
		v.keys[k.KeyID] = k
	}
	k, ok := v.keys[kid]
	v.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no assertion signing key with kid %q at %s", kid, v.jwksURL)
	}
	return &k, nil
}

func (v *Verifier) fetchJWKS(ctx context.Context) ([]jose.JSONWebKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch assertion JWKS %s: %w", v.jwksURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch assertion JWKS %s: unexpected status %d", v.jwksURL, resp.StatusCode)
	}
	var set jose.JSONWebKeySet
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 1<<20)).Decode(&set); err != nil {
		return nil, fmt.Errorf("decode assertion JWKS %s: %w", v.jwksURL, err)
	}
	keys := make([]jose.JSONWebKey, 0, len(set.Keys))
	for _, k := range set.Keys {
		if k.Valid() && k.IsPublic() {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("assertion JWKS %s carries no usable public keys", v.jwksURL)
	}
	return keys, nil
}

func claimString(claims map[string]any, key string) string {
	if s, ok := claims[key].(string); ok {
		return s
	}
	parts := strings.Split(key, ".")
	var cur any = claims
	for i, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		if v, ok := m[strings.Join(parts[i:], ".")]; ok {
			s, _ := v.(string)
			return s
		}
		cur, ok = m[p]
		if !ok {
			return ""
		}
	}
	s, _ := cur.(string)
	return s
}
