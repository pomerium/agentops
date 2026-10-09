package agentic

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptPoller struct {
	seq  []PollResult
	next int
}

func (p *scriptPoller) Poll(context.Context) PollResult {
	r := p.seq[p.next]
	if p.next < len(p.seq)-1 {
		p.next++
	}
	return r
}

type pollFunc func(context.Context) PollResult

func (f pollFunc) Poll(ctx context.Context) PollResult { return f(ctx) }

func tok(bearer string, expiresIn time.Duration) *Token {
	return &Token{Bearer: bearer, RunID: "run-1", ExpiresIn: expiresIn}
}

func newTestLoop(t *testing.T, cfg LoopConfig, poll Poller) (*Loop, *[]string, *[]time.Duration) {
	t.Helper()
	var delivered []string
	var slept []time.Duration
	cfg.Poll = poll
	if cfg.Sink == nil {
		cfg.Sink = func(tk *Token) error { delivered = append(delivered, tk.Bearer); return nil }
	}
	cfg.Sleep = func(ctx context.Context, d time.Duration) error {
		slept = append(slept, d)
		return ctx.Err()
	}
	return NewLoop(cfg), &delivered, &slept
}

func TestLoop_ReadinessWithheldUntilFirstToken(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollPending},
		{Kind: PollPending},
		{Kind: PollOk, Token: tok("Bearer pom_art_1", time.Hour)},
		{Kind: PollTerminal, Reason: ReasonRevokedOrExpired},
	}}
	loop, delivered, _ := newTestLoop(t, LoopConfig{PendingInterval: 3 * time.Second}, poll)

	select {
	case <-loop.Ready():
		t.Fatal("ready before first token")
	default:
	}

	err := loop.Run(context.Background())
	require.Error(t, err)

	select {
	case <-loop.Ready():
	default:
		t.Fatal("ready not closed after first token")
	}
	assert.Equal(t, []string{"Bearer pom_art_1"}, *delivered)
}

func TestLoop_RotatesAtInterval(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollOk, Token: tok("Bearer pom_art_1", time.Hour)},
		{Kind: PollOk, Token: tok("Bearer pom_art_2", time.Hour)},
		{Kind: PollOk, Token: tok("Bearer pom_art_3", time.Hour)},
		{Kind: PollTerminal, Reason: ReasonRevokedOrExpired},
	}}
	loop, delivered, slept := newTestLoop(t, LoopConfig{RotationInterval: 5 * time.Second}, poll)

	err := loop.Run(context.Background())
	require.Error(t, err)

	assert.Equal(t, []string{"Bearer pom_art_1", "Bearer pom_art_2", "Bearer pom_art_3"}, *delivered)
	for _, d := range *slept {
		assert.Equal(t, 5*time.Second, d)
	}
}

func TestLoop_RotationIntervalDefault(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 10*time.Minute, NewLoop(LoopConfig{}).rotationInterval(tok("b", 2*time.Hour)))
	assert.Equal(t, 10*time.Minute, NewLoop(LoopConfig{}).rotationInterval(tok("b", time.Hour)))
	assert.Equal(t, 5*time.Minute, NewLoop(LoopConfig{}).rotationInterval(tok("b", 30*time.Minute)))
	l := NewLoop(LoopConfig{RotationInterval: 5 * time.Second})
	assert.Equal(t, 5*time.Second, l.rotationInterval(tok("b", time.Hour)))
}

func TestLoop_ConfiguredRotationStaysBeforeExpiry(t *testing.T) {
	t.Parallel()
	l := NewLoop(LoopConfig{RotationInterval: 5 * time.Second})
	assert.Equal(t, 500*time.Millisecond, l.rotationInterval(tok("b", 3*time.Second)),
		"a configured interval must not outlast the default share of the token lifetime")
	l = NewLoop(LoopConfig{RotationInterval: time.Hour})
	assert.Equal(t, 20*time.Minute, l.rotationInterval(tok("b", 2*time.Hour)))
}

func TestLoop_BackoffCapped(t *testing.T) {
	t.Parallel()
	seq := make([]PollResult, 0, 12)
	for i := 0; i < 10; i++ {
		seq = append(seq, PollResult{Kind: PollRetryable, Err: errors.New("dial tcp: connection refused")})
	}
	seq = append(seq, PollResult{Kind: PollTerminal, Reason: ReasonConfigError})
	poll := &scriptPoller{seq: seq}
	loop, _, slept := newTestLoop(t, LoopConfig{MaxBackoff: 30 * time.Second}, poll)

	err := loop.Run(context.Background())
	require.Error(t, err)

	require.NotEmpty(t, *slept)
	for i, d := range *slept {
		assert.LessOrEqualf(t, d, 30*time.Second, "sleep[%d]=%v exceeds cap", i, d)
		if i > 0 {
			assert.GreaterOrEqual(t, d, (*slept)[i-1], "backoff must not shrink")
		}
	}
	assert.Equal(t, 30*time.Second, (*slept)[len(*slept)-1])
}

func TestLoop_TerminalReasonPropagates(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{{Kind: PollTerminal, Reason: ReasonRevokedOrExpired}}}
	loop, _, _ := newTestLoop(t, LoopConfig{}, poll)

	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, ReasonRevokedOrExpired, te.Reason)
}

func TestLoop_SinkErrorIsTerminalConfigError(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{{Kind: PollOk, Token: tok("Bearer x", time.Hour)}}}
	loop, _, _ := newTestLoop(t, LoopConfig{
		Sink: func(*Token) error { return errors.New("write sds: read-only fs") },
	}, poll)

	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, ReasonConfigError, te.Reason)
}

func TestLoop_ASUnreachablePastTokenExpiry(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollOk, Token: tok("Bearer pom_art_1", 10*time.Second)},
		{Kind: PollRetryable, Err: errors.New("dial: timeout")},
		{Kind: PollRetryable, Err: errors.New("dial: timeout")},
	}}

	now := time.Unix(1000, 0)
	cfg := LoopConfig{
		MaxBackoff: 30 * time.Second,
		Now:        func() time.Time { return now },
	}
	var delivered []string
	cfg.Sink = func(tk *Token) error { delivered = append(delivered, tk.Bearer); return nil }
	cfg.Poll = poll
	cfg.Sleep = func(ctx context.Context, _ time.Duration) error {
		now = now.Add(6 * time.Second)
		return ctx.Err()
	}
	loop := NewLoop(cfg)

	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, ReasonASUnreachable, te.Reason)
	assert.Equal(t, []string{"Bearer pom_art_1"}, delivered)
}

func TestLoop_RetrySleepStopsAtTokenExpiry(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollOk, Token: tok("Bearer pom_art_1", 6*time.Second)},
		{Kind: PollRetryable, Err: errors.New("token exchange unavailable (503)")},
	}}

	now := time.Unix(1000, 0)
	expiresAt := now.Add(6 * time.Second)
	var slept []time.Duration
	loop := NewLoop(LoopConfig{
		Poll:             poll,
		Sink:             func(*Token) error { return nil },
		RotationInterval: time.Second,
		Now:              func() time.Time { return now },
		Sleep: func(ctx context.Context, d time.Duration) error {
			slept = append(slept, d)
			now = now.Add(d)
			return ctx.Err()
		},
	})

	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, ReasonASUnreachable, te.Reason)
	assert.Equal(t, expiresAt, now, "the loop must stop when the token expires, not after it")
	assert.Equal(t, []time.Duration{time.Second, time.Second, 2 * time.Second, 2 * time.Second}, slept)
}

func TestLoop_TokenLifetimeCountsFromPollStart(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	expiresAt := now.Add(6 * time.Second)
	polls := 0
	poll := pollFunc(func(context.Context) PollResult {
		polls++
		if polls == 1 {
			now = now.Add(2 * time.Second)
			return PollResult{Kind: PollOk, Token: tok("Bearer pom_art_1", 6*time.Second)}
		}
		return PollResult{Kind: PollRetryable, Err: errors.New("token exchange unavailable (503)")}
	})
	loop := NewLoop(LoopConfig{
		Poll: poll,
		Sink: func(*Token) error { return nil },
		Now:  func() time.Time { return now },
		Sleep: func(ctx context.Context, d time.Duration) error {
			now = now.Add(d)
			return ctx.Err()
		},
	})

	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, ReasonASUnreachable, te.Reason)
	assert.Equal(t, expiresAt, now, "the lifetime starts when the exchange was sent, not when it answered")
}

func TestLoop_RotationSleepStopsAtTokenExpiry(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		exchange  time.Duration
		wantSlept []time.Duration
		wantEnd   time.Duration
	}{
		{"answered with time left", 5500 * time.Millisecond, []time.Duration{500 * time.Millisecond}, 6 * time.Second},
		{"answered after the lifetime", 7 * time.Second, []time.Duration{0}, 7 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start := time.Unix(1000, 0)
			now := start
			polls := 0
			poll := pollFunc(func(context.Context) PollResult {
				polls++
				if polls == 1 {
					now = now.Add(tc.exchange)
					return PollResult{Kind: PollOk, Token: tok("Bearer pom_art_1", 6*time.Second)}
				}
				return PollResult{Kind: PollRetryable, Err: errors.New("token exchange unavailable (503)")}
			})
			var slept []time.Duration
			loop := NewLoop(LoopConfig{
				Poll: poll,
				Sink: func(*Token) error { return nil },
				Now:  func() time.Time { return now },
				Sleep: func(ctx context.Context, d time.Duration) error {
					slept = append(slept, d)
					now = now.Add(d)
					return ctx.Err()
				},
			})

			err := loop.Run(context.Background())
			var te *TerminalError
			require.ErrorAs(t, err, &te)
			assert.Equal(t, ReasonASUnreachable, te.Reason)
			assert.Equal(t, tc.wantSlept, slept, "a slow exchange must not push the next one past the new token's expiry")
			assert.Equal(t, start.Add(tc.wantEnd), now)
		})
	}
}

func TestLoop_PollIsBoundedByHeldTokenLifetime(t *testing.T) {
	t.Parallel()
	var deadline time.Time
	var hasDeadline bool
	polls := 0
	poll := pollFunc(func(ctx context.Context) PollResult {
		polls++
		if polls == 1 {
			return PollResult{Kind: PollOk, Token: tok("Bearer pom_art_1", time.Hour)}
		}
		deadline, hasDeadline = ctx.Deadline()
		return PollResult{Kind: PollTerminal, Reason: ReasonRevokedOrExpired}
	})
	loop, _, _ := newTestLoop(t, LoopConfig{}, poll)

	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	require.True(t, hasDeadline, "an exchange must not outlive the token it renews")
	assert.LessOrEqual(t, time.Until(deadline), time.Hour)
	assert.Greater(t, time.Until(deadline), 59*time.Minute)
}

func TestLoop_HangingExchangeEndsAtTokenExpiry(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = io.WriteString(w, `{"access_token":"pom_art_1","token_type":"Bearer","expires_in":1,"run_id":"r"}`)
			return
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	loop := NewLoop(LoopConfig{
		Poll: newTestPoller(t, srv, writeTokenFile(t, "sa")),
		Sink: func(*Token) error { return nil },
	})

	started := time.Now()
	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, ReasonASUnreachable, te.Reason)
	assert.Less(t, time.Since(started), 10*time.Second, "a hung exchange must not keep an expired token in service")
}

func TestLoop_PendingAfterTokenEndsAtExpiry(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollOk, Token: tok("Bearer pom_art_1", 6*time.Second)},
		{Kind: PollPending},
	}}
	now := time.Unix(1000, 0)
	expiresAt := now.Add(6 * time.Second)
	errSleptPastExpiry := errors.New("slept past the held token's expiry")
	loop := NewLoop(LoopConfig{
		Poll: poll,
		Sink: func(*Token) error { return nil },
		Now:  func() time.Time { return now },
		Sleep: func(ctx context.Context, d time.Duration) error {
			now = now.Add(d)
			if now.After(expiresAt) {
				return errSleptPastExpiry
			}
			return ctx.Err()
		},
	})

	err := loop.Run(context.Background())
	var te *TerminalError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, ReasonRevokedOrExpired, te.Reason)
	assert.Equal(t, expiresAt, now)
}

func TestLoop_ContextCancelStops(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{{Kind: PollPending}}}
	loop, _, _ := newTestLoop(t, LoopConfig{PendingInterval: time.Second}, poll)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := loop.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

type logRecorder struct {
	records []slog.Record
}

func (h *logRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h *logRecorder) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logRecorder) WithGroup(string) slog.Handler      { return h }

func (h *logRecorder) find(want string) (slog.Record, bool) {
	for _, r := range h.records {
		if strings.Contains(r.Message, want) {
			return r, true
		}
	}
	return slog.Record{}, false
}

func attr(r slog.Record, key string) (slog.Value, bool) {
	var v slog.Value
	var found bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	return v, found
}

func TestLoop_PendingIsLoggedAtInfoOnAHeartbeat(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollPending},
		{Kind: PollPending},
		{Kind: PollPending},
		{Kind: PollOk, Token: tok("Bearer pom_art_1", time.Hour)},
	}}

	now := time.Unix(1000, 0)
	rec := &logRecorder{}
	cfg := LoopConfig{
		PendingInterval: 15 * time.Second,
		Now:             func() time.Time { return now },
		Logger:          slog.New(rec),
	}
	cfg.Poll = poll
	cfg.Sink = func(*Token) error { return nil }
	polls := 0
	cfg.Sleep = func(ctx context.Context, _ time.Duration) error {
		now = now.Add(15 * time.Second)
		if polls++; polls == 4 {
			return context.Canceled
		}
		return ctx.Err()
	}

	err := NewLoop(cfg).Run(context.Background())
	require.ErrorIs(t, err, context.Canceled)

	var levels []slog.Level
	var messages []string
	for _, r := range rec.records {
		levels = append(levels, r.Level)
		messages = append(messages, r.Message)
	}
	require.Equal(t, []string{
		"run token: loop started",
		"run token: authorization_pending; waiting for the run to be approved",
		"run token: authorization_pending; still waiting for approval",
		"run token: still authorization_pending",
		"run token: first token delivered",
	}, messages)
	assert.Equal(t,
		[]slog.Level{slog.LevelInfo, slog.LevelInfo, slog.LevelDebug, slog.LevelInfo, slog.LevelInfo},
		levels)

	beat, ok := rec.find("still authorization_pending")
	require.True(t, ok)
	waiting, ok := attr(beat, "waiting_for")
	require.True(t, ok)
	assert.Equal(t, 30*time.Second, waiting.Any())

	minted, ok := rec.find("first token delivered")
	require.True(t, ok)
	runID, ok := attr(minted, "run_id")
	require.True(t, ok)
	assert.Equal(t, "run-1", runID.String())
	waited, ok := attr(minted, "waited_for_approval")
	require.True(t, ok)
	assert.Equal(t, 45*time.Second, waited.Any())
}

func TestLoop_TerminalIsLoggedAtError(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollTerminal, Reason: ReasonRevokedOrExpired, Err: errors.New("denied (403)")},
	}}
	rec := &logRecorder{}
	loop, _, _ := newTestLoop(t, LoopConfig{Logger: slog.New(rec)}, poll)

	require.Error(t, loop.Run(context.Background()))

	r, ok := rec.find("terminal; no more tokens")
	require.True(t, ok)
	assert.Equal(t, slog.LevelError, r.Level)
	reason, ok := attr(r, "reason")
	require.True(t, ok)
	assert.Equal(t, ReasonRevokedOrExpired, reason.String())
}

func TestLoop_UnchangedTokenIsNotRedelivered(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollOk, Token: tok("Bearer same", time.Hour)},
		{Kind: PollOk, Token: tok("Bearer same", 50*time.Minute)},
		{Kind: PollOk, Token: tok("Bearer next", time.Hour)},
		{Kind: PollTerminal, Reason: ReasonConfigError},
	}}
	rec := &logRecorder{}
	loop, delivered, _ := newTestLoop(t, LoopConfig{Logger: slog.New(rec)}, poll)
	require.Error(t, loop.Run(context.Background()))

	assert.Equal(t, []string{"Bearer same", "Bearer next"}, *delivered)
	unchanged, ok := rec.find("run token: unchanged")
	require.True(t, ok)
	assert.Equal(t, slog.LevelDebug, unchanged.Level)
	_, ok = rec.find("run token: rotated")
	assert.True(t, ok)
}

func TestLoop_LabelReplacesRunToken(t *testing.T) {
	t.Parallel()
	poll := &scriptPoller{seq: []PollResult{
		{Kind: PollOk, Token: &Token{Bearer: "Bearer jwt", ExpiresIn: 10 * time.Minute}},
		{Kind: PollTerminal, Reason: ReasonConfigError, Err: errors.New("read projected token")},
	}}
	rec := &logRecorder{}
	loop, _, _ := newTestLoop(t, LoopConfig{Label: "workload token", Logger: slog.New(rec)}, poll)
	err := loop.Run(context.Background())
	require.Error(t, err)

	var messages []string
	for _, r := range rec.records {
		messages = append(messages, r.Message)
	}
	assert.Equal(t, []string{
		"workload token: loop started",
		"workload token: first token delivered",
		"workload token: terminal; no more tokens",
	}, messages)
	loaded, ok := rec.find("first token delivered")
	require.True(t, ok)
	_, hasRunID := attr(loaded, "run_id")
	assert.False(t, hasRunID, "a token without a run id logs none")
}
