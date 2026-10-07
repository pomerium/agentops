package agentlinktest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
)

const Audience = "harness.test"

type IDP struct {
	key *ecdsa.PrivateKey
	kid string
	srv *httptest.Server
}

func NewIDP(t *testing.T) *IDP {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate assertion key: %v", err)
	}
	idp := &IDP{key: key, kid: "kid-1"}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/pomerium/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		set := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key: key.Public(), KeyID: idp.kid, Algorithm: string(jose.ES256), Use: "sig",
		}}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	})
	idp.srv = httptest.NewServer(mux)
	t.Cleanup(idp.srv.Close)
	return idp
}

func (i *IDP) Issuer() string { return i.srv.URL }

func (i *IDP) HTTPClient() *http.Client { return i.srv.Client() }

func (i *IDP) Verifier(t *testing.T) *agentlink.Verifier {
	t.Helper()
	v, err := agentlink.NewVerifier(i.Issuer(),
		agentlink.WithAudience(Audience),
		agentlink.WithHTTPClient(i.HTTPClient()),
	)
	if err != nil {
		t.Fatalf("agentlink.NewVerifier: %v", err)
	}
	return v
}

func Claims(runID string, seal agenticrun.Executor) map[string]any {
	return map[string]any{
		"run_id":                                runID,
		"act.kubernetes.io.namespace":           seal.Namespace,
		"act.kubernetes.io.serviceaccount.name": seal.ServiceAccount,
		"act.kubernetes.io.pod.name":            seal.PodName,
		"act.kubernetes.io.pod.uid":             seal.PodUID,
	}
}

func (i *IDP) Sign(t *testing.T, extra map[string]any) string {
	t.Helper()
	now := time.Now()
	claims := map[string]any{
		"iss": i.Issuer(),
		"aud": Audience,
		"sub": "approver@example.com",
		"iat": now.Unix(),
		"exp": now.Add(5 * time.Minute).Unix(),
	}
	for k, v := range extra {
		if v == nil {
			delete(claims, k)
			continue
		}
		claims[k] = v
	}
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.ES256, Key: i.key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", i.kid),
	)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatalf("sign assertion: %v", err)
	}
	return raw
}

func (i *IDP) SignFor(t *testing.T, runID string, seal agenticrun.Executor) string {
	t.Helper()
	return i.Sign(t, Claims(runID, seal))
}

func Serve(t *testing.T, srv *agentlink.Server, assertion func() string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.StreamInterceptor(stampAssertion(assertion)))
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func stampAssertion(assertion func() string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		md = md.Copy()
		md.Set(agentlink.AssertionMetadataKey, assertion())
		return handler(srv, stampedStream{ServerStream: ss, ctx: metadata.NewIncomingContext(ss.Context(), md)})
	}
}

type stampedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s stampedStream) Context() context.Context { return s.ctx }
