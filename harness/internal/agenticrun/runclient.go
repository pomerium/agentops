package agenticrun

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pomerium/agentops/harness/internal/pomeriumtls"
)

const (
	agenticRunsPath  = "/agentic/runs"
	promptMaxBytes   = 4096
	truncationMarker = "… [truncated]"
)

func ClampPrompt(s string) string {
	if len(s) <= promptMaxBytes {
		return s
	}
	cut := promptMaxBytes - len(truncationMarker)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncationMarker
}

type RunClient interface {
	CreateRun(ctx context.Context, req CreateRunRequest) (*CreateRunResult, error)
	GetRun(ctx context.Context, runID string) (*RunStatus, error)
}

type CreateRunRequest struct {
	Prompt          string
	MCPServers      []string
	TTL             time.Duration
	Executor        map[string]string
	ExpectedSubject string
	Labels          map[string]string
}

type CreateRunResult struct {
	RunID       string
	ApprovalURL string
	ExpiresAt   time.Time
}

type RunStatus struct {
	State           string
	Bound           bool
	Revoked         bool
	ExpiresAt       time.Time
	ApproverSubject string
}

type RunError struct {
	Op         string
	StatusCode int
	Body       string
	Retryable  bool
}

func (e *RunError) Error() string {
	if e.StatusCode == 0 {
		return fmt.Sprintf("%s: %s", e.Op, e.Body)
	}
	return fmt.Sprintf("%s: unexpected status %d: %s", e.Op, e.StatusCode, e.Body)
}

func IsRetryable(err error) bool {
	var re *RunError
	if errors.As(err, &re) {
		return re.Retryable
	}
	return false
}

type HTTPRunClient struct {
	baseURL   string
	tokenFile string
	hc        *http.Client
}

type clientOptions struct {
	caFile string
}

type Option func(*clientOptions)

func WithCAFile(path string) Option {
	return func(o *clientOptions) { o.caFile = path }
}

func NewHTTPRunClient(baseURL, dialAddr, tokenFile string, opts ...Option) (*HTTPRunClient, error) {
	var o clientOptions
	for _, opt := range opts {
		opt(&o)
	}

	if u, err := url.Parse(baseURL); err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("agentic AS URL %q: must be an https URL", baseURL)
	}

	transport, err := pomeriumtls.Transport(dialAddr, o.caFile)
	if err != nil {
		return nil, err
	}

	return &HTTPRunClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		tokenFile: tokenFile,
		hc: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

var _ RunClient = (*HTTPRunClient)(nil)

func (c *HTTPRunClient) token() (string, error) {
	b, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return "", fmt.Errorf("read workload token %s: %w", c.tokenFile, err)
	}
	return strings.TrimSpace(string(b)), nil
}

func (c *HTTPRunClient) CreateRun(ctx context.Context, req CreateRunRequest) (*CreateRunResult, error) {
	fields := map[string]any{
		"mcp_servers": req.MCPServers,
		"ttl_seconds": int64(req.TTL.Seconds()),
		"prompt":      ClampPrompt(req.Prompt),
		"executor":    req.Executor,
	}
	if req.ExpectedSubject != "" {
		fields["expected_subject"] = req.ExpectedSubject
	}
	if len(req.Labels) > 0 {
		fields["labels"] = req.Labels
	}
	reqBody, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}

	var out struct {
		RunID       string `json:"run_id"`
		ApprovalURL string `json:"approval_url"`
		ExpiresAt   string `json:"expires_at"`
	}
	const op = "create run"
	if err := c.do(ctx, op, http.MethodPost, c.baseURL+agenticRunsPath, reqBody, http.StatusCreated, &out); err != nil {
		return nil, err
	}
	res := &CreateRunResult{RunID: out.RunID, ApprovalURL: out.ApprovalURL}
	if res.ExpiresAt, err = parseExpiry(out.ExpiresAt); err != nil {
		return nil, &RunError{Op: op, Body: err.Error()}
	}
	return res, nil
}

func (c *HTTPRunClient) GetRun(ctx context.Context, runID string) (*RunStatus, error) {
	var out struct {
		State           string `json:"state"`
		Bound           bool   `json:"bound"`
		Revoked         bool   `json:"revoked"`
		ExpiresAt       string `json:"expires_at"`
		ApproverSubject string `json:"approver_subject"`
	}
	const op = "get run"
	if err := c.do(ctx, op, http.MethodGet, c.baseURL+agenticRunsPath+"/"+url.PathEscape(runID), nil, http.StatusOK, &out); err != nil {
		return nil, err
	}
	st := &RunStatus{
		State: out.State, Bound: out.Bound, Revoked: out.Revoked,
		ApproverSubject: out.ApproverSubject,
	}
	var err error
	if st.ExpiresAt, err = parseExpiry(out.ExpiresAt); err != nil {
		return nil, &RunError{Op: op, Body: err.Error()}
	}
	return st, nil
}

func (c *HTTPRunClient) do(ctx context.Context, op, method, target string, reqBody []byte, want int, out any) error {
	tok, err := c.token()
	if err != nil {
		return err
	}
	var body io.Reader
	if reqBody != nil {
		body = bytes.NewReader(reqBody)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Authorization", "Bearer "+tok)
	if reqBody != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return &RunError{Op: op, Body: err.Error(), Retryable: true}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != want {
		return &RunError{
			Op:         op,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(respBody)),
			Retryable:  resp.StatusCode == http.StatusServiceUnavailable,
		}
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return &RunError{Op: op, Body: fmt.Sprintf("decode response: %v", err)}
	}
	return nil
}

func parseExpiry(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse expires_at %q: %w", s, err)
	}
	return t, nil
}
