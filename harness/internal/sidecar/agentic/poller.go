package agentic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pomerium/agentops/harness/internal/pomeriumtls"
)

const agenticTokenPath = "/agentic/token"

type HTTPPollerConfig struct {
	BaseURL   string
	DialAddr  string
	TokenFile string
	CAFile    string
	Logger    *slog.Logger
}

type HTTPPoller struct {
	url       string
	tokenFile string
	hc        *http.Client
	log       *slog.Logger
}

func NewHTTPPoller(cfg HTTPPollerConfig) (*HTTPPoller, error) {
	if u, err := url.Parse(cfg.BaseURL); err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("agentic AS URL %q: must be an https URL", cfg.BaseURL)
	}
	transport, err := pomeriumtls.Transport(cfg.DialAddr, cfg.CAFile)
	if err != nil {
		return nil, err
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	p := &HTTPPoller{
		url:       strings.TrimRight(cfg.BaseURL, "/") + agenticTokenPath,
		tokenFile: cfg.TokenFile,
		hc: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		log: log,
	}
	dial := cfg.DialAddr
	if dial == "" {
		dial = "resolved from the URL"
	}
	p.log.Info("run token: token exchange endpoint",
		"url", p.url, "dial", dial, "token_file", cfg.TokenFile, "private_ca", cfg.CAFile != "")
	return p, nil
}

func (p *HTTPPoller) Poll(ctx context.Context) PollResult {
	tokBytes, err := os.ReadFile(p.tokenFile)
	if err != nil {
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: fmt.Errorf("read projected token: %w", err)}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, nil)
	if err != nil {
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tokBytes)))

	started := time.Now()
	resp, err := p.hc.Do(req)
	if err != nil {
		return PollResult{Kind: PollRetryable, Err: err}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	p.log.Debug("run token: exchange answered",
		"status", resp.StatusCode, "duration", time.Since(started).Round(time.Millisecond))

	switch resp.StatusCode {
	case http.StatusOK:
		return p.parseOK(body)
	case http.StatusBadRequest:
		if strings.Contains(string(body), "authorization_pending") {
			return PollResult{Kind: PollPending}
		}
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: fmt.Errorf("unexpected 400: %s", strings.TrimSpace(string(body)))}
	case http.StatusUnauthorized:
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: fmt.Errorf("token exchange rejected the projected token (401)")}
	case http.StatusForbidden:
		return PollResult{Kind: PollTerminal, Reason: ReasonRevokedOrExpired, Err: fmt.Errorf("token exchange denied (403)")}
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return PollResult{Kind: PollRetryable, Err: fmt.Errorf("token exchange unavailable (%d)", resp.StatusCode)}
	default:
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: fmt.Errorf("unexpected status %d", resp.StatusCode)}
	}
}

func (p *HTTPPoller) parseOK(body []byte) PollResult {
	var out struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
		RunID       string `json:"run_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: fmt.Errorf("decode token response: %w", err)}
	}
	if out.AccessToken == "" {
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: fmt.Errorf("token response has no access_token")}
	}
	if out.ExpiresIn <= 0 {
		return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: fmt.Errorf("token response has no positive expires_in (%d)", out.ExpiresIn)}
	}
	scheme := out.TokenType
	if scheme == "" {
		scheme = "Bearer"
	}
	p.log.Debug("run token minted", "run_id", out.RunID, "expires_in_seconds", out.ExpiresIn)
	return PollResult{Kind: PollOk, Token: &Token{
		Bearer:    scheme + " " + out.AccessToken,
		RunID:     out.RunID,
		ExpiresIn: time.Duration(out.ExpiresIn) * time.Second,
	}}
}
