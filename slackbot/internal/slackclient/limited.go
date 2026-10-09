package client

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/slack-go/slack"
	"golang.org/x/time/rate"
)

type Sender interface {
	PostMessage(ctx context.Context, channelID string, opts ...slack.MsgOption) (string, error)
	PostDM(ctx context.Context, userID string, opts ...slack.MsgOption) (string, string, error)
	PostEphemeral(ctx context.Context, channelID, userID string, opts ...slack.MsgOption) (string, error)
	UpdateMessage(ctx context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error)
	DeleteMessage(ctx context.Context, channelID, ts string) error
	AddReaction(ctx context.Context, channelID, timestamp, emoji string) error
	RemoveReaction(ctx context.Context, channelID, timestamp, emoji string) error
	Respond(ctx context.Context, responseURL string, replaceOriginal bool, text string, blocks []slack.Block) error
	ThreadReplies(ctx context.Context, channelID, threadTS, since string, max int) ([]slack.Message, error)
	Permalink(ctx context.Context, channelID, ts string) (string, error)
}

type Limits struct {
	PostPerChannel    rate.Limit
	PostBurst         int
	UpdateFinal       rate.Limit
	UpdateFinalBurst  int
	UpdateStream      rate.Limit
	UpdateStreamBurst int
	Reactions         rate.Limit
	ReactionsBurst    int
	Global            rate.Limit
	GlobalBurst       int
}

func DefaultLimits() Limits {
	return Limits{
		PostPerChannel: rate.Every(1100 * time.Millisecond), PostBurst: 3,
		UpdateFinal: rate.Every(3 * time.Second), UpdateFinalBurst: 5,
		UpdateStream: rate.Every(2 * time.Second), UpdateStreamBurst: 2,
		Reactions: rate.Every(1500 * time.Millisecond), ReactionsBurst: 4,
		Global: rate.Limit(15), GlobalBurst: 15,
	}
}

const rateLimitedRetries = 3

const workerPollInterval = 100 * time.Millisecond

type pendingUpdate struct {
	channel string
	ts      string
	opts    []slack.MsgOption
}

type Limited struct {
	next Sender
	log  *slog.Logger

	global    *rate.Limiter
	reactions *rate.Limiter

	mu        sync.Mutex
	posts     map[string]*rate.Limiter
	updFinal  map[string]*rate.Limiter
	updStream map[string]*rate.Limiter
	pending   map[string]*pendingUpdate
	order     []string
	sendMu    map[string]*sync.Mutex

	lim  Limits
	wake chan struct{}
}

func NewLimited(next Sender, log *slog.Logger) *Limited {
	return NewLimitedWith(next, DefaultLimits(), log)
}

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

func (l *Limited) guaranteed(ctx context.Context, bucket *rate.Limiter, send func(context.Context) error) error {
	if err := l.global.Wait(ctx); err != nil {
		return err
	}
	if err := bucket.Wait(ctx); err != nil {
		return err
	}
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		err := send(ctx)
		var rl *slack.RateLimitedError
		if errors.As(err, &rl) {
			return struct{}{}, backoff.RetryAfter(rl.RetryAfter, err)
		}
		return struct{}{}, backoff.Permanent(err)
	},
		backoff.WithBackOff(backoff.NewConstantBackOff(0)),
		backoff.WithMaxTries(rateLimitedRetries+1),
		backoff.WithMaxElapsedTime(0),
		backoff.WithNotify(func(err error, next time.Duration) {
			l.log.WarnContext(ctx, "slack rate limited; retrying", "retry_after", next, "err", err)
		}))
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if re := backoff.AsRetryError(err); re != nil {
		return re.LastErr
	}
	return err
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

func (l *Limited) PostDM(ctx context.Context, userID string, opts ...slack.MsgOption) (string, string, error) {
	bucket := l.limiterFor(l.posts, userID, l.lim.PostPerChannel, l.lim.PostBurst)
	var channel, ts string
	err := l.guaranteed(ctx, bucket, func(ctx context.Context) error {
		var err error
		channel, ts, err = l.next.PostDM(ctx, userID, opts...)
		return err
	})
	return channel, ts, err
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

func (l *Limited) DeleteMessage(ctx context.Context, channelID, ts string) error {
	key := updateKey(channelID, ts)
	l.cancelPending(key)
	mu := l.keyMu(key)
	mu.Lock()
	defer mu.Unlock()

	bucket := l.limiterFor(l.updFinal, channelID, l.lim.UpdateFinal, l.lim.UpdateFinalBurst)
	err := l.guaranteed(ctx, bucket, func(ctx context.Context) error {
		return l.next.DeleteMessage(ctx, channelID, ts)
	})
	if err == nil {
		l.mu.Lock()
		if l.sendMu[key] == mu {
			delete(l.sendMu, key)
		}
		l.mu.Unlock()
	}
	return err
}

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

func (l *Limited) Respond(ctx context.Context, responseURL string, replaceOriginal bool, text string, blocks []slack.Block) error {
	if err := l.global.Wait(ctx); err != nil {
		return err
	}
	return l.next.Respond(ctx, responseURL, replaceOriginal, text, blocks)
}

func (l *Limited) ThreadReplies(ctx context.Context, channelID, threadTS, since string, max int) ([]slack.Message, error) {
	if err := l.global.Wait(ctx); err != nil {
		return nil, err
	}
	return l.next.ThreadReplies(ctx, channelID, threadTS, since, max)
}

func (l *Limited) Permalink(ctx context.Context, channelID, ts string) (string, error) {
	if err := l.global.Wait(ctx); err != nil {
		return "", err
	}
	return l.next.Permalink(ctx, channelID, ts)
}

func (l *Limited) cancelPending(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.pending[key]; !ok {
		return
	}
	delete(l.pending, key)
	if i := slices.Index(l.order, key); i >= 0 {
		l.order = slices.Delete(l.order, i, i+1)
	}
}

func (l *Limited) runWorker() {
	for {
		l.mu.Lock()
		idle := len(l.pending) == 0
		l.mu.Unlock()
		var poll <-chan time.Time
		if !idle {
			poll = time.After(workerPollInterval)
		}
		select {
		case <-l.wake:
		case <-poll:
		}
		l.drain()
	}
}

func (l *Limited) drain() {
	for {
		up, key, ok := l.nextSendable()
		if !ok {
			return
		}
		mu := l.keyMu(key)
		mu.Lock()
		_, err := l.next.UpdateMessage(context.Background(), up.channel, up.ts, up.opts...)
		mu.Unlock()
		if err != nil {
			l.log.Debug("coalesced update failed; re-parking", "channel", up.channel, "ts", up.ts, "err", err)
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

func (l *Limited) nextSendable() (*pendingUpdate, string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for range len(l.order) {
		key := l.order[0]
		l.order = l.order[1:]
		up, ok := l.pending[key]
		if !ok {
			continue
		}
		bucket, exists := l.updStream[up.channel]
		if !exists {
			bucket = rate.NewLimiter(l.lim.UpdateStream, l.lim.UpdateStreamBurst)
			l.updStream[up.channel] = bucket
		}
		if !bucket.Allow() {
			l.order = append(l.order, key)
			continue
		}
		delete(l.pending, key)
		return up, key, true
	}
	return nil, "", false
}
