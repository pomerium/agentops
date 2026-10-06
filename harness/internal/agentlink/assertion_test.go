package agentlink_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/agentlink/agentlinktest"
)

const (
	testNamespace = "agentops"
	testSA        = "sandbox-agent"
	testPod       = "smc-pod-1"
	testPodUID    = "uid-0001"
)

func sign(t *testing.T, idp *agentlinktest.IDP, extra map[string]any) string {
	t.Helper()
	claims := agentlinktest.Claims("run-1", testSeal)
	claims["sub"] = "approver@example.com"
	for k, v := range extra {
		claims[k] = v
	}
	return idp.Sign(t, claims)
}

func TestVerifyExtractsFlattenedRunClaims(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	v := idp.Verifier(t)

	a, err := v.Verify(context.Background(), sign(t, idp, nil))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if a.RunID != "run-1" {
		t.Errorf("RunID = %q", a.RunID)
	}
	if a.Executor.Namespace != testNamespace || a.Executor.ServiceAccount != testSA ||
		a.Executor.PodName != testPod || a.Executor.PodUID != testPodUID {
		t.Errorf("executor = %+v", a.Executor)
	}
	if a.Subject != "approver@example.com" {
		t.Errorf("Subject = %q", a.Subject)
	}
}

func TestVerifyAcceptsNestedActClaims(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	v := idp.Verifier(t)

	raw := sign(t, idp, map[string]any{
		"act.kubernetes.io.namespace":           nil,
		"act.kubernetes.io.serviceaccount.name": nil,
		"act.kubernetes.io.pod.name":            nil,
		"act.kubernetes.io.pod.uid":             nil,
		"act": map[string]any{
			"kubernetes.io.namespace":           testNamespace,
			"kubernetes.io.serviceaccount.name": testSA,
			"kubernetes.io.pod.name":            testPod,
			"kubernetes.io.pod.uid":             testPodUID,
		},
	})
	a, err := v.Verify(context.Background(), raw)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if a.Executor.PodUID != testPodUID {
		t.Errorf("executor = %+v", a.Executor)
	}
}

func TestVerifyMissingRunIDFailsClosed(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	v := idp.Verifier(t)

	_, err := v.Verify(context.Background(), sign(t, idp, map[string]any{"run_id": nil}))
	if !errors.Is(err, agentlink.ErrMissingRunClaims) {
		t.Fatalf("err = %v, want ErrMissingRunClaims", err)
	}
}

func TestVerifyIncompletePodIdentityFailsClosed(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	v := idp.Verifier(t)

	_, err := v.Verify(context.Background(), sign(t, idp, map[string]any{"act.kubernetes.io.pod.uid": nil}))
	if err == nil {
		t.Fatal("expected an error when the pod uid claim is absent")
	}
}

func TestVerifyRejectsWrongIssuerAudienceAndExpiry(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	v := idp.Verifier(t)

	cases := map[string]map[string]any{
		"issuer":   {"iss": "https://elsewhere.example"},
		"audience": {"aud": "some-other-route"},
		"expired":  {"exp": time.Now().Add(-10 * time.Minute).Unix()},
	}
	for name, extra := range cases {
		if _, err := v.Verify(context.Background(), sign(t, idp, extra)); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
	}
}

func TestVerifyRejectsForeignSignature(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	other := agentlinktest.NewIDP(t)
	v := idp.Verifier(t)

	raw := sign(t, other, map[string]any{"iss": idp.Issuer()})
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected a signature failure for a foreign signer")
	}
}

func TestBindAdvisory(t *testing.T) {
	if got := agentlink.BindAdvisory(":8090"); got == "" {
		t.Error("a wildcard bind should produce an advisory")
	}
	if got := agentlink.BindAdvisory("127.0.0.1:8090"); got != "" {
		t.Errorf("loopback bind should be quiet, got %q", got)
	}
	if got := agentlink.BindAdvisory("10.0.0.5:8090"); got != "" {
		t.Errorf("private bind should be quiet, got %q", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConcurrentFirstVerificationsShareOneFetch(t *testing.T) {
	idp := agentlinktest.NewIDP(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var fetches atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if fetches.Add(1) == 1 {
			close(entered)
			<-release
		}
		return idp.HTTPClient().Transport.RoundTrip(r)
	})}
	v, err := agentlink.NewVerifier(idp.Issuer(),
		agentlink.WithAudience(agentlinktest.Audience), agentlink.WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	raw := sign(t, idp, nil)

	results := make(chan error, 2)
	verify := func() {
		_, err := v.Verify(context.Background(), raw)
		results <- err
	}
	go verify()
	<-entered
	go verify()
	select {
	case err := <-results:
		close(release)
		t.Fatalf("a verification finished while the first JWKS fetch was still in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Errorf("valid assertion rejected: %v", err)
		}
	}
	if n := fetches.Load(); n != 1 {
		t.Errorf("JWKS fetched %d times, want 1", n)
	}
}
