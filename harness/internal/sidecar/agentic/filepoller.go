package agentic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const minRemaining = time.Second

type FilePollerConfig struct {
	TokenFile string
	Audience  string
	Now       func() time.Time
}

type FilePoller struct {
	cfg FilePollerConfig
}

func NewFilePoller(cfg FilePollerConfig) (*FilePoller, error) {
	if cfg.TokenFile == "" {
		return nil, errors.New("token file is required")
	}
	if cfg.Audience == "" {
		return nil, errors.New("audience is required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &FilePoller{cfg: cfg}, nil
}

func (p *FilePoller) Poll(context.Context) PollResult {
	raw, err := os.ReadFile(p.cfg.TokenFile)
	if err != nil {
		return configError(fmt.Errorf("read projected token: %w", err))
	}
	token := strings.TrimSpace(string(raw))
	claims, err := jwtClaims(token)
	if err != nil {
		return configError(fmt.Errorf("projected token %s: %w", p.cfg.TokenFile, err))
	}
	if !claims.Audience.Contains(p.cfg.Audience) {
		return configError(fmt.Errorf("projected token %s is for audience %q, want %q", p.cfg.TokenFile, []string(claims.Audience), p.cfg.Audience))
	}
	remaining := claims.Expiry.Time().Sub(p.cfg.Now())
	if remaining < minRemaining {
		return PollResult{Kind: PollTerminal, Reason: ReasonRevokedOrExpired,
			Err: fmt.Errorf("projected token %s expired %s ago; the kubelet is not rotating it", p.cfg.TokenFile, (-remaining).Round(time.Second))}
	}
	return PollResult{Kind: PollOk, Token: &Token{Bearer: "Bearer " + token, ExpiresIn: remaining}}
}

func configError(err error) PollResult {
	return PollResult{Kind: PollTerminal, Reason: ReasonConfigError, Err: err}
}

var signatureAlgs = []jose.SignatureAlgorithm{jose.RS256, jose.RS384, jose.RS512, jose.ES256, jose.ES384, jose.ES512, jose.EdDSA}

func jwtClaims(raw string) (jwt.Claims, error) {
	tok, err := jwt.ParseSigned(raw, signatureAlgs)
	if err != nil {
		return jwt.Claims{}, errors.New("not a JWT")
	}
	var c jwt.Claims
	if err := tok.UnsafeClaimsWithoutVerification(&c); err != nil {
		return jwt.Claims{}, errors.New("JWT claims are not a JSON object")
	}
	if c.Expiry == nil {
		return jwt.Claims{}, errors.New("JWT has no exp claim")
	}
	return c, nil
}
