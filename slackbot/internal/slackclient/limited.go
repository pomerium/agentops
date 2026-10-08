package client

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/slack-go/slack"
	"golang.org/x/time/rate"
)

// Sender is the underlying Slack sender Limited wraps (implemented by
// *Poster).
type Sender interface {
	PostMessage(ctx context.Context, channelID string, opts ...slack.MsgOption) (string, error)
	PostEphemeral(ctx context.Context, channelID, userID string, opts ...slack.MsgOption) (string, error)
	UpdateMessage(ctx context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error)
	AddReaction(ctx context.Context, channelID, timestamp, emoji string) error
	RemoveReaction(ctx context.Context, channelID, timestamp, emoji string) error
	Respond(ctx context.Context, responseURL string, replaceOriginal bool, text string, blocks []slack.Block) error
	ThreadReplies(ctx context.Context, channelID, threadTS string, max int) ([]slack.Message, error)
	Permalink(ctx context.Context, channelID, ts string) (string, error)
}

// Limits are the token-bucket parameters for each Slack Web API budget the
// wrapper models.
type Limits struct {
	// PostPerChannel paces chat.postMessage / chat.postEphemeral per channel
	// (Slack's special tier: ~1 message/sec/channel, short bursts tolerated).
	PostPerChannel rate.Limit
	PostBurst      int
	// UpdateFinal paces guaranteed chat.update calls (a turn's final flush and
	// pre-rollover updates) per channel.
	UpdateFinal      rate.Limit
	UpdateFinalBurst int
	// UpdateStream paces coalesced (droppable) chat.update calls per channel.
	// It is a separate bucket from UpdateFinal so a chatty stream can never
	// consume the budget a final answer needs.
	UpdateStream      rate.Limit
	UpdateStreamBurst int
	// Reactions paces reactions.add/remove (Tier 3), shared across channels.
	Reactions      rate.Limit
	ReactionsBurst int
	// Global is a workspace-wide backstop across all guaranteed sends.
	Global      rate.Limit
	GlobalBurst int
}

// DefaultLimits approximates Slack's documented budgets, deliberately
// conservative: chat.postMessage is ~1/sec/channel; chat.update and
// reactions.* are Tier 3 (~50/min), with the update budget split between
// guaranteed finals and droppable streams.
func DefaultLimits() Limits {
	return Limits{
		PostPerChannel: rate.Every(1100 * time.Millisecond), PostBurst: 3,
		UpdateFinal: rate.Every(3 * time.Second), UpdateFinalBurst: 5,
		UpdateStream: rate.Every(2 * time.Second), UpdateStreamBurst: 2,
		Reactions: rate.Every(1500 * time.Millisecond), ReactionsBurst: 4,
		Global: rate.Limit(15), GlobalBurst: 15,
	}
}

// rateLimitedRetries bounds how many Slack 429s a guaranteed send absorbs
// (sleeping each Retry-After) before giving up and returning the error.
const rateLimitedRetries = 3

// workerPollInterval is the fallback cadence at which the coalescing worker
// re-checks parked updates when no new ones arrive to wake it (e.g. after the
// stream bucket ran dry mid-drain).
const workerPollInterval = 100 * time.Millisecond

// pendingUpdate is the latest parked coalesced update for one message.
type pendingUpdate struct {
	channel string
	ts      string
	opts    []slack.MsgOption
}

// Limited enforces Slack's posting rate limits over a Sender with two delivery
// classes:
//
//   - Guaranteed — posts, reactions, ephemeral/webhook responses, and final
//     message updates. These wait for budget (and honor 429 Retry-After) but
//     are never dropped.
//   - Coalesced — mid-turn streaming updates via UpdateMessageDebounced. Each
//     message has a single pending slot: a newer update replaces the parked one
//     (last-one-wins; intermediate content is eaten, never queued). A worker
//     drains slots round-robin so one chatty message can't starve others, and
//     only when its bucket has spare capacity right now, so coalesced traffic
//     never delays guaranteed sends.
type Limited struct {
	next Sender
	log  *slog.Logger

	global    *rate.Limiter
	reactions *rate.Limiter

	mu        sync.Mutex
	posts     map[string]*rate.Limiter // channel -> chat.postMessage bucket
	updFinal  map[string]*rate.Limiter // channel -> guaranteed chat.update bucket
	updStream map[string]*rate.Limiter // channel -> coalesced chat.update bucket
	pending   map[string]*pendingUpdate
	order     []string               // round-robin queue of pending keys
	sendMu    map[string]*sync.Mutex // per-message lock: a final update waits out an in-flight coalesced send

	lim  Limits
	wake chan struct{}
}

// NewLimited wraps next with DefaultLimits.
func NewLimited(next Sender, log *slog.Logger) *Limited {
	return NewLimitedWith(next, DefaultLimits(), log)
}

// NewLimitedWith wraps next with explicit limits. log may be nil.
func NewLimitedWith(next Sender, lim Limits, log *slog.Logger) *Limited {
	if log == nil {
		log = slog.Default()
	}
	l := &Limited{
		next:      next,
		log:       log,
		lim:       lim,
		global:    rate.NewLimiter(lim.Global, lim.GlobalBurst),
		reactions: rate.NewLimiter(lim.Reactions, lim.ReactionsBurst),
		posts:     map[string]*rate.Limiter{},
		updFinal:  map[string]*rate.Limiter{},
		updStream: map[string]*rate.Limiter{},
		pending:   map[string]*pendingUpdate{},
		sendMu:    map[string]*sync.Mutex{},
		wake:      make(chan struct{}, 1),
	}
	go l.runWorker()
	return l
}

func updateKey(channel, ts string) string { return channel + "|" + ts }

// limiterFor returns the lazily created bucket for a channel from the given
// class map.
func (l *Limited) limiterFor(m map[string]*rate.Limiter, channel string, limit rate.Limit, burst int) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := m[channel]
	if !ok {
		lim = rate.NewLimiter(limit, burst)
		m[channel] = lim
	}
	return lim
}

// keyMu returns the per-message send lock for key.
func (l *Limited) keyMu(key string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()
	mu, ok := l.sendMu[key]
	if !ok {
		mu = &sync.Mutex{}
		l.sendMu[key] = mu
	}
	return mu
}

// guaranteed waits for the class and global buckets, then sends, absorbing a
// bounded number of 429s (sleeping each Retry-After). It never drops the send.
func (l *Limited) guaranteed(ctx context.Context, bucket *rate.Limiter, send func(context.Context) error) error {
	if err := l.global.Wait(ctx); err != nil {
		return err
	}
	if err := bucket.Wait(ctx); err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		err := send(ctx)
		var rl *slack.RateLimitedError
		if errors.As(err, &rl) && attempt < rateLimitedRetries {
			l.log.WarnContext(ctx, "slack rate limited; retrying", "retry_after", rl.RetryAfter, "attempt", attempt+1)
			select {
			case <-time.After(rl.RetryAfter):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return err
	}
}

func (l *Limited) PostMessage(ctx context.Context, channelID string, opts ...slack.MsgOption) (string, error) {
	bucket := l.limiterFor(l.posts, channelID, l.lim.PostPerChannel, l.lim.PostBurst)
	var ts string
	err := l.guaranteed(ctx, bucket, func(ctx context.Context) error {
		var err error
		ts, err = l.next.PostMessage(ctx, channelID, opts...)
		return err
	})
	return ts, err
}

func (l *Limited) PostEphemeral(ctx context.Context, channelID, userID string, opts ...slack.MsgOption) (string, error) {
	bucket := l.limiterFor(l.posts, channelID, l.lim.PostPerChannel, l.lim.PostBurst)
	var ts string
	err := l.guaranteed(ctx, bucket, func(ctx context.Context) error {
		var err error
		ts, err = l.next.PostEphemeral(ctx, channelID, userID, opts...)
		return err
	})
	return ts, err
}

// UpdateMessage is the guaranteed update path (a turn's final flush). It
// cancels any pending coalesced update for the same message — without this, a
// parked stale update could land after the final one and resurrect mid-turn
// narration — and waits out an in-flight coalesced send for the message so the
// final content always lands last.
func (l *Limited) UpdateMessage(ctx context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error) {
	key := updateKey(channelID, ts)
	l.cancelPending(key)
	mu := l.keyMu(key)
	mu.Lock()
	defer mu.Unlock()

	bucket := l.limiterFor(l.updFinal, channelID, l.lim.UpdateFinal, l.lim.UpdateFinalBurst)
	var newTS string
	err := l.guaranteed(ctx, bucket, func(ctx context.Context) error {
		var err error
		newTS, err = l.next.UpdateMessage(ctx, channelID, ts, opts...)
		return err
	})
	return newTS, err
}

// UpdateMessageDebounced parks a coalesced update for the message; see the
// type comment for its drop/last-one-wins semantics. The send happens
// asynchronously (the passed ctx is not used for it), so a turn-scoped ctx
// cancelling cannot lose a still-relevant update.
func (l *Limited) UpdateMessageDebounced(_ context.Context, channelID, ts string, opts ...slack.MsgOption) {
	key := updateKey(channelID, ts)
	l.mu.Lock()
	if _, ok := l.pending[key]; !ok {
		l.order = append(l.order, key)
	}
	l.pending[key] = &pendingUpdate{channel: channelID, ts: ts, opts: opts}
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *Limited) AddReaction(ctx context.Context, channelID, timestamp, emoji string) error {
	return l.guaranteed(ctx, l.reactions, func(ctx context.Context) error {
		return l.next.AddReaction(ctx, channelID, timestamp, emoji)
	})
}

func (l *Limited) RemoveReaction(ctx context.Context, channelID, timestamp, emoji string) error {
	return l.guaranteed(ctx, l.reactions, func(ctx context.Context) error {
		return l.next.RemoveReaction(ctx, channelID, timestamp, emoji)
	})
}

// Respond goes through the global backstop only: response_url webhooks have
// their own generous per-URL budget on Slack's side.
func (l *Limited) Respond(ctx context.Context, responseURL string, replaceOriginal bool, text string, blocks []slack.Block) error {
	if err := l.global.Wait(ctx); err != nil {
		return err
	}
	return l.next.Respond(ctx, responseURL, replaceOriginal, text, blocks)
}

// ThreadReplies and Permalink are low-volume reads (one fetch and a few
// permalinks per loop-in session), so like Respond they go through the global
// backstop only.
func (l *Limited) ThreadReplies(ctx context.Context, channelID, threadTS string, max int) ([]slack.Message, error) {
	if err := l.global.Wait(ctx); err != nil {
		return nil, err
	}
	return l.next.ThreadReplies(ctx, channelID, threadTS, max)
}

func (l *Limited) Permalink(ctx context.Context, channelID, ts string) (string, error) {
	if err := l.global.Wait(ctx); err != nil {
		return "", err
	}
	return l.next.Permalink(ctx, channelID, ts)
}

// cancelPending drops the parked coalesced update for key, if any.
func (l *Limited) cancelPending(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.pending[key]; !ok {
		return
	}
	delete(l.pending, key)
	for i, k := range l.order {
		if k == key {
			l.order = append(l.order[:i], l.order[i+1:]...)
			break
		}
	}
}

// runWorker drains parked coalesced updates for the lifetime of the process
// (the wrapper is built once at startup).
func (l *Limited) runWorker() {
	for {
		select {
		case <-l.wake:
		case <-time.After(workerPollInterval):
		}
		l.drain()
	}
}

// drain sends parked updates while any key has budget, visiting keys in FIFO
// order (round-robin: a sent or skipped key goes to the back via re-parking,
// so a hot key can't shadow the others).
func (l *Limited) drain() {
	for {
		up, key, ok := l.nextSendable()
		if !ok {
			return
		}
		mu := l.keyMu(key)
		mu.Lock()
		// Sends use a background ctx: the parking caller's request ctx is long
		// gone, and a turn-scoped cancellation must not lose the update.
		_, err := l.next.UpdateMessage(context.Background(), up.channel, up.ts, up.opts...)
		mu.Unlock()
		if err != nil {
			l.log.Debug("coalesced update failed; re-parking", "channel", up.channel, "ts", up.ts, "err", err)
			// Re-park unless a newer update (or a cancel) replaced the slot, then
			// back off until the next wake/poll.
			l.mu.Lock()
			if _, exists := l.pending[key]; !exists {
				l.pending[key] = up
				l.order = append(l.order, key)
			}
			l.mu.Unlock()
			return
		}
	}
}

// nextSendable pops the first parked key whose stream bucket (and the global
// backstop) has a token available right now. Keys without budget are rotated
// to the back; it reports !ok once no key is currently sendable.
func (l *Limited) nextSendable() (*pendingUpdate, string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for range len(l.order) {
		key := l.order[0]
		l.order = l.order[1:]
		up, ok := l.pending[key]
		if !ok {
			continue // cancelled
		}
		bucket, exists := l.updStream[up.channel]
		if !exists {
			bucket = rate.NewLimiter(l.lim.UpdateStream, l.lim.UpdateStreamBurst)
			l.updStream[up.channel] = bucket
		}
		if !bucket.Allow() {
			l.order = append(l.order, key) // no budget now; rotate to the back
			continue
		}
		delete(l.pending, key)
		return up, key, true
	}
	return nil, "", false
}
