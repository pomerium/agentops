package agentlink

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

type jwksServer struct {
	srv *httptest.Server

	mu   sync.Mutex
	keys []jose.JSONWebKey
	down bool
	hits int
}

func newJWKSServer(t *testing.T) *jwksServer {
	t.Helper()
	j := &jwksServer{}
	j.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		j.mu.Lock()
		j.hits++
		set, down := jose.JSONWebKeySet{Keys: j.keys}, j.down
		j.mu.Unlock()
		if down {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(j.srv.Close)
	return j
}

func (j *jwksServer) publish(t *testing.T, kids ...string) {
	t.Helper()
	keys := make([]jose.JSONWebKey, 0, len(kids))
	for _, kid := range kids {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		keys = append(keys, jose.JSONWebKey{Key: key.Public(), KeyID: kid, Algorithm: string(jose.ES256), Use: "sig"})
	}
	j.mu.Lock()
	j.keys = keys
	j.mu.Unlock()
}

func (j *jwksServer) setDown(down bool) {
	j.mu.Lock()
	j.down = down
	j.mu.Unlock()
}

func (j *jwksServer) fetches() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.hits
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestVerifier(t *testing.T, j *jwksServer) (*Verifier, *fakeClock) {
	t.Helper()
	v, err := NewVerifier(j.srv.URL, WithHTTPClient(j.srv.Client()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	clk := &fakeClock{t: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)}
	v.now = clk.now
	return v, clk
}

func TestVerifierDropsWithdrawnKeys(t *testing.T) {
	j := newJWKSServer(t)
	v, clk := newTestVerifier(t, j)
	ctx := context.Background()

	j.publish(t, "k1")
	if _, err := v.key(ctx, "k1"); err != nil {
		t.Fatalf("k1: %v", err)
	}
	if _, err := v.key(ctx, "unknown"); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("unknown kid inside the refetch interval: err = %v, want the retryable ErrKeysUnavailable", err)
	}
	if n := j.fetches(); n != 1 {
		t.Fatalf("JWKS fetched %d times, want 1: an unknown kid must not refetch inside the interval", n)
	}

	j.publish(t, "k2")
	clk.advance(jwksRefetchInterval)
	if _, err := v.key(ctx, "k2"); err != nil {
		t.Fatalf("k2 after rotation: %v", err)
	}
	if _, err := v.key(ctx, "k1"); err == nil {
		t.Fatal("k1 is still trusted after a refresh that no longer publishes it")
	}
	clk.advance(jwksRefetchInterval)
	if _, err := v.key(ctx, "k1"); err == nil || errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("k1 after a fresh fetch without it: err = %v, want a definitive rejection", err)
	}

	j.publish(t, "k3")
	clk.advance(jwksMaxAge)
	if _, err := v.key(ctx, "k2"); err == nil {
		t.Fatal("a cached key is still trusted after the JWKS withdrew it and the cache aged out")
	}
}

func TestVerifierReportsAndThrottlesJWKSOutage(t *testing.T) {
	j := newJWKSServer(t)
	v, clk := newTestVerifier(t, j)
	ctx := context.Background()

	j.publish(t, "k1")
	j.setDown(true)
	for range 3 {
		if _, err := v.key(ctx, "k1"); !errors.Is(err, ErrKeysUnavailable) {
			t.Fatalf("err = %v, want ErrKeysUnavailable", err)
		}
	}
	if n := j.fetches(); n != 1 {
		t.Fatalf("JWKS fetched %d times during an outage, want 1 per refetch interval", n)
	}

	j.setDown(false)
	clk.advance(jwksRefetchInterval)
	if _, err := v.key(ctx, "k1"); err != nil {
		t.Fatalf("k1 after the outage: %v", err)
	}
}
