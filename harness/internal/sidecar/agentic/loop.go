package agentic

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v7"
)

const (
	ReasonRevokedOrExpired = "revoked_or_expired"
	ReasonASUnreachable    = "as_unreachable"
	ReasonConfigError      = "config_error"
)

const baseBackoff = time.Second

const pendingLogInterval = 30 * time.Second

type Token struct {
	Bearer    string
	RunID     string
	ExpiresIn time.Duration
}

type PollKind int

const (
	PollOk PollKind = iota
	PollPending
	PollRetryable
	PollTerminal
)

type PollResult struct {
	Kind   PollKind
	Token  *Token
	Reason string
	Err    error
}

type Poller interface {
	Poll(ctx context.Context) PollResult
}

type TerminalError struct {
	Reason string
	Err    error
}

func (e *TerminalError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("token terminal (%s): %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("token terminal (%s)", e.Reason)
}

func (e *TerminalError) Unwrap() error { return e.Err }

type LoopConfig struct {
	Poll             Poller
	Sink             func(*Token) error
	PendingInterval  time.Duration
	RotationInterval time.Duration
	MaxBackoff       time.Duration
	Sleep            func(ctx context.Context, d time.Duration) error
	Now              func() time.Time
	Label            string
	Logger           *slog.Logger
}

type Loop struct {
	cfg       LoopConfig
	ready     chan struct{}
	readyOnce sync.Once
	log       *slog.Logger
	refresh   chan struct{}

	mu   sync.Mutex
	held *Token
}

func NewLoop(cfg LoopConfig) *Loop {
	if cfg.PendingInterval <= 0 {
		cfg.PendingInterval = 3 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cfg.Label = cmp.Or(cfg.Label, "run token")
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Loop{cfg: cfg, ready: make(chan struct{}), log: log, refresh: make(chan struct{}, 1)}
}

func (l *Loop) Ready() <-chan struct{} { return l.ready }

func (l *Loop) Current() *Token {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

func (l *Loop) Bearer() string {
	if t := l.Current(); t != nil {
		return t.Bearer
	}
	return ""
}

func (l *Loop) Refresh() {
	select {
	case l.refresh <- struct{}{}:
	default:
	}
}

func (l *Loop) sleep(ctx context.Context, d time.Duration) error {
	if l.cfg.Sleep != nil {
		return l.cfg.Sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.refresh:
		return nil
	case <-timer.C:
		return nil
	}
}

func (l *Loop) Run(ctx context.Context) error {
	retry := &backoff.ExponentialBackOff{
		InitialInterval: baseBackoff,
		Multiplier:      2,
		MaxInterval:     l.cfg.MaxBackoff,
	}
	retry.Reset()
	var heldAt time.Time
	var pendingSince, pendingLoggedAt time.Time

	label := l.cfg.Label
	l.log.Info(label+": loop started",
		"pending_interval", l.cfg.PendingInterval, "max_backoff", l.cfg.MaxBackoff)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res := l.cfg.Poll.Poll(ctx)
		switch res.Kind {
		case PollOk:
			first := heldAt.IsZero()
			unchanged := !first && l.Current().Bearer == res.Token.Bearer
			if !unchanged {
				if err := l.cfg.Sink(res.Token); err != nil {
					return &TerminalError{Reason: ReasonConfigError, Err: fmt.Errorf("deliver %s: %w", label, err)}
				}
			}
			l.mu.Lock()
			l.held = res.Token
			l.mu.Unlock()
			heldAt = l.cfg.Now()
			l.readyOnce.Do(func() { close(l.ready) })
			retry.Reset()
			rotateIn := l.rotationInterval(res.Token)
			attrs := []any{"expires_in", res.Token.ExpiresIn, "rotate_in", rotateIn}
			if res.Token.RunID != "" {
				attrs = append(attrs, "run_id", res.Token.RunID)
			}
			switch {
			case first:
				if !pendingSince.IsZero() {
					attrs = append(attrs, "waited_for_approval", heldAt.Sub(pendingSince).Round(time.Second))
				}
				l.log.Info(label+": first token delivered", attrs...)
			case unchanged:
				l.log.Debug(label+": unchanged", attrs...)
			default:
				l.log.Info(label+": rotated", attrs...)
			}
			pendingSince, pendingLoggedAt = time.Time{}, time.Time{}
			if err := l.sleep(ctx, rotateIn); err != nil {
				return err
			}
		case PollPending:
			now := l.cfg.Now()
			switch {
			case pendingSince.IsZero():
				pendingSince, pendingLoggedAt = now, now
				l.log.Info(label+": authorization_pending; waiting for the run to be approved",
					"retry_in", l.cfg.PendingInterval)
			case now.Sub(pendingLoggedAt) >= pendingLogInterval:
				pendingLoggedAt = now
				l.log.Info(label+": still authorization_pending",
					"waiting_for", now.Sub(pendingSince).Round(time.Second))
			default:
				l.log.Debug(label + ": authorization_pending; still waiting for approval")
			}
			if err := l.sleep(ctx, l.cfg.PendingInterval); err != nil {
				return err
			}
		case PollRetryable:
			held := l.Current()
			var remaining time.Duration
			if held != nil {
				remaining = held.ExpiresIn - l.cfg.Now().Sub(heldAt)
				if remaining <= 0 {
					l.log.Error(label+": AS still unreachable and the last token has expired; the run is over",
						"err", res.Err)
					return &TerminalError{Reason: ReasonASUnreachable, Err: res.Err}
				}
			}
			pendingSince, pendingLoggedAt = time.Time{}, time.Time{}
			wait := retry.NextBackOff()
			if held != nil {
				wait = min(wait, remaining)
			}
			l.log.Warn(label+": AS unreachable; serving last token",
				"backoff", wait, "have_token", held != nil, "err", res.Err)
			if err := l.sleep(ctx, wait); err != nil {
				return err
			}
		case PollTerminal:
			l.log.Error(label+": terminal; no more tokens", "reason", res.Reason, "err", res.Err)
			return &TerminalError{Reason: res.Reason, Err: res.Err}
		}
	}
}

func (l *Loop) rotationInterval(t *Token) time.Duration {
	if l.cfg.RotationInterval > 0 {
		return l.cfg.RotationInterval
	}
	d := t.ExpiresIn / 6
	if d <= 0 || d > 10*time.Minute {
		d = 10 * time.Minute
	}
	return d
}
