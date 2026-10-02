package client_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api/client"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

type blockingHTTPClient struct{}

func (blockingHTTPClient) Do(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, r.Context().Err()
}

func TestRequestTimeoutAppliesToACustomHTTPClient(t *testing.T) {
	c, err := client.New("http://example.invalid",
		client.WithRequestTimeout(50*time.Millisecond),
		client.WithRetries(0),
		client.WithHTTPClient(blockingHTTPClient{}),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := time.Now()
	if _, err := c.ListTemplates(ctx, &pb.ListTemplatesRequest{}); err == nil {
		t.Fatal("a call to a transport that never answers succeeded")
	}
	if waited := time.Since(started); waited > 2*time.Second {
		t.Fatalf("the call waited %v; RequestTimeout (50ms) was ignored", waited)
	}
}

type unavailableHTTPClient struct{ calls *atomic.Int32 }

func (u unavailableHTTPClient) Do(r *http.Request) (*http.Response, error) {
	u.calls.Add(1)
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"code":"unavailable","message":"down"}`)),
		Request:    r,
	}, nil
}

func TestOnlyIdempotentCallsAreRetried(t *testing.T) {
	for _, tc := range []struct {
		name     string
		call     func(context.Context, harnessapipbconnect.HarnessAPIServiceClient) error
		attempts int32
	}{
		{"a read", func(ctx context.Context, c harnessapipbconnect.HarnessAPIServiceClient) error {
			_, err := c.GetSession(ctx, &pb.GetSessionRequest{})
			return err
		}, 3},
		{"CreateSession", func(ctx context.Context, c harnessapipbconnect.HarnessAPIServiceClient) error {
			_, err := c.CreateSession(ctx, &pb.CreateSessionRequest{})
			return err
		}, 1},
		{"EndSession", func(ctx context.Context, c harnessapipbconnect.HarnessAPIServiceClient) error {
			_, err := c.EndSession(ctx, &pb.EndSessionRequest{})
			return err
		}, 1},
		{"a Prompt without a key", func(ctx context.Context, c harnessapipbconnect.HarnessAPIServiceClient) error {
			_, err := c.Prompt(ctx, &pb.PromptRequest{})
			return err
		}, 1},
		{"a Prompt with a key", func(ctx context.Context, c harnessapipbconnect.HarnessAPIServiceClient) error {
			_, err := c.Prompt(ctx, &pb.PromptRequest{IdempotencyKey: "msg-1"})
			return err
		}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			c, err := client.New("http://example.invalid", client.WithHTTPClient(unavailableHTTPClient{&calls}))
			if err != nil {
				t.Fatalf("client.New: %v", err)
			}
			if err := tc.call(context.Background(), c); err == nil {
				t.Fatal("a call to an unavailable server succeeded")
			}
			if got := calls.Load(); got != tc.attempts {
				t.Errorf("sent %d times, want %d", got, tc.attempts)
			}
		})
	}
}
