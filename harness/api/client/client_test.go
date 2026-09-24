package client_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api/client"
)

// blockingHTTPClient never answers: a transport that hangs until the request's
// own context gives up.
type blockingHTTPClient struct{}

func (blockingHTTPClient) Do(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, r.Context().Err()
}

// TestRequestTimeoutAppliesToACustomHTTPClient: RequestTimeout bounds a unary
// call whatever transport the caller supplies, not only the one New builds.
func TestRequestTimeoutAppliesToACustomHTTPClient(t *testing.T) {
	c, err := client.New(client.Config{
		BaseURL:        "http://example.invalid",
		RequestTimeout: 50 * time.Millisecond,
		Retries:        -1,
		HTTPClient:     blockingHTTPClient{},
	})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := time.Now()
	if _, err := c.ListTemplates(ctx, ""); err == nil {
		t.Fatal("a call to a transport that never answers succeeded")
	}
	if waited := time.Since(started); waited > 2*time.Second {
		t.Fatalf("the call waited %v; RequestTimeout (50ms) was ignored", waited)
	}
}
