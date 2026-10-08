package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"golang.org/x/time/rate"
)

// sentMsg records one delivered post/update with its rendered content.
type sentMsg struct {
	channel string
	ts      string
	text    string
}

// fakeSender is a scriptable Sender: optional per-call latency (to simulate
// the Slack API round-trip) and scripted PostMessage errors.
type fakeSender struct {
	mu             sync.Mutex
	latency        time.Duration
	postErrs       []error // popped per PostMessage call; nil entry = success
	posts          []sentMsg
	updates        []sentMsg
	repliesCalls   []string
	permalinkCalls []string
}

func optsText(opts []slack.MsgOption) string {
	_, vals, _ := slack.UnsafeApplyMsgOptions("t", "c", "https://slack.com/api/", opts...)
	var b strings.Builder
	for _, vs := range vals {
		for _, v := range vs {
			b.WriteString(v)
			b.WriteByte(' ')
		}
	}
	return b.String()
}

func (f *fakeSender) PostMessage(_ context.Context, channel string, opts ...slack.MsgOption) (string, error) {
	if f.latency > 0 {
		time.Sleep(f.latency)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.posts = append(f.posts, sentMsg{channel: channel, text: optsText(opts)})
	if len(f.postErrs) > 0 {
		err := f.postErrs[0]
		f.postErrs = f.postErrs[1:]
		if err != nil {
			return "", err
		}
	}
	return "ts-new", nil
}

func (f *fakeSender) PostEphemeral(_ context.Context, channel, _ string, opts ...slack.MsgOption) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.posts = append(f.posts, sentMsg{channel: channel, text: optsText(opts)})
	return "ts-new", nil
}

func (f *fakeSender) UpdateMessage(_ context.Context, channel, ts string, opts ...slack.MsgOption) (string, error) {
	if f.latency > 0 {
		time.Sleep(f.latency)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, sentMsg{channel: channel, ts: ts, text: optsText(opts)})
	return ts, nil
}

func (f *fakeSender) AddReaction(context.Context, string, string, string) error    { return nil }
func (f *fakeSender) RemoveReaction(context.Context, string, string, string) error { return nil }
func (f *fakeSender) Respond(context.Context, string, bool, string, []slack.Block) error {
	return nil
}

func (f *fakeSender) ThreadReplies(_ context.Context, channel, threadTS string, _ int) ([]slack.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repliesCalls = append(f.repliesCalls, channel+"|"+threadTS)
	return []slack.Message{{Msg: slack.Msg{Timestamp: threadTS, Text: "root"}}}, nil
}

func (f *fakeSender) Permalink(_ context.Context, channel, ts string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.permalinkCalls = append(f.permalinkCalls, channel+"|"+ts)
	return "https://slack.test/" + channel + "/" + ts, nil
}

func (f *fakeSender) postCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.posts)
}

func (f *fakeSender) updateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.updates)
}

func (f *fakeSender) lastUpdateText() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.updates) == 0 {
		return ""
	}
	return f.updates[len(f.updates)-1].text
}

func (f *fakeSender) updatesFor(ts string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, u := range f.updates {
		if u.ts == ts {
			n++
		}
	}
	return n
}

// openLimits never throttles anything — for tests exercising semantics, not
// pacing.
func openLimits() Limits {
	return Limits{
		PostPerChannel: rate.Inf, PostBurst: 1,
		UpdateFinal: rate.Inf, UpdateFinalBurst: 1,
		UpdateStream: rate.Inf, UpdateStreamBurst: 1,
		Reactions: rate.Inf, ReactionsBurst: 1,
		Global: rate.Inf, GlobalBurst: 1,
	}
}

// A storm of debounced updates for one message collapses: far fewer calls
// reach Slack and the LAST content is what lands (last-one-wins, never queued).
func TestLimitedCoalescesUpdateStorm(t *testing.T) {
	f := &fakeSender{latency: 5 * time.Millisecond}
	l := NewLimitedWith(f, openLimits(), nil)

	const n = 50
	for i := range n {
		l.UpdateMessageDebounced(context.Background(), "C1", "ts1", slack.MsgOptionText(fmt.Sprintf("v%d", i), false))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(f.lastUpdateText(), fmt.Sprintf("v%d", n-1)) {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(f.lastUpdateText(), fmt.Sprintf("v%d", n-1)) {
		t.Fatalf("final content never landed; last=%q after %d updates", f.lastUpdateText(), f.updateCount())
	}
	if got := f.updateCount(); got >= n/2 {
		t.Errorf("storm should collapse (last-one-wins), got %d/%d calls through", got, n)
	}
}

// Parked coalesced updates must never block or consume the guaranteed path:
// with the stream bucket fully closed, a PostMessage still goes out.
func TestLimitedStormDoesNotBlockGuaranteed(t *testing.T) {
	f := &fakeSender{}
	lim := openLimits()
	lim.UpdateStream, lim.UpdateStreamBurst = 0, 0 // stream bucket closed: parked updates can never send
	l := NewLimitedWith(f, lim, nil)

	for i := range 10 {
		l.UpdateMessageDebounced(context.Background(), "C1", "ts1", slack.MsgOptionText(fmt.Sprintf("v%d", i), false))
	}
	if _, err := l.PostMessage(context.Background(), "C1", slack.MsgOptionText("important", false)); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if got := f.postCount(); got != 1 {
		t.Errorf("expected the guaranteed post delivered, got %d", got)
	}
	if got := f.updateCount(); got != 0 {
		t.Errorf("closed stream bucket must hold coalesced updates, got %d through", got)
	}
}

// A guaranteed UpdateMessage (a turn's final flush) cancels any pending
// coalesced update for the same message, so stale narration can't land after
// the final answer.
func TestLimitedFinalUpdateCancelsPending(t *testing.T) {
	f := &fakeSender{}
	lim := openLimits()
	lim.UpdateStream, lim.UpdateStreamBurst = 0, 0 // park the stale update forever
	l := NewLimitedWith(f, lim, nil)

	l.UpdateMessageDebounced(context.Background(), "C1", "ts1", slack.MsgOptionText("stale narration", false))
	if _, err := l.UpdateMessage(context.Background(), "C1", "ts1", slack.MsgOptionText("final answer", false)); err != nil {
		t.Fatalf("UpdateMessage: %v", err)
	}

	if got := f.updateCount(); got != 1 {
		t.Fatalf("expected exactly the final update, got %d", got)
	}
	if !strings.Contains(f.lastUpdateText(), "final answer") {
		t.Errorf("expected the final content, got %q", f.lastUpdateText())
	}
	l.mu.Lock()
	pending := len(l.pending)
	l.mu.Unlock()
	if pending != 0 {
		t.Errorf("pending coalesced update should be cancelled by the final update, %d left", pending)
	}
	time.Sleep(150 * time.Millisecond) // give the worker a chance to misbehave
	if got := f.updateCount(); got != 1 {
		t.Errorf("stale update resurrected after the final one: %d updates", got)
	}
}

// One chatty message can't starve another: while key A is stormed
// continuously, a single parked update for key B still gets through.
func TestLimitedRoundRobinAcrossKeys(t *testing.T) {
	f := &fakeSender{latency: 5 * time.Millisecond}
	l := NewLimitedWith(f, openLimits(), nil)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			l.UpdateMessageDebounced(context.Background(), "C1", "ts-A", slack.MsgOptionText(fmt.Sprintf("a%d", i), false))
			time.Sleep(time.Millisecond)
		}
	})
	l.UpdateMessageDebounced(context.Background(), "C1", "ts-B", slack.MsgOptionText("b-content", false))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && f.updatesFor("ts-B") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if f.updatesFor("ts-B") == 0 {
		t.Errorf("key B starved by the key-A storm: %d A updates, 0 B updates", f.updatesFor("ts-A"))
	}
}

// Guaranteed sends honor Slack 429s: wait RetryAfter, retry, succeed.
func TestLimitedRetriesRateLimited(t *testing.T) {
	f := &fakeSender{postErrs: []error{&slack.RateLimitedError{RetryAfter: 5 * time.Millisecond}, nil}}
	l := NewLimitedWith(f, openLimits(), nil)

	if _, err := l.PostMessage(context.Background(), "C1", slack.MsgOptionText("hello", false)); err != nil {
		t.Fatalf("PostMessage should retry past a 429, got: %v", err)
	}
	if got := f.postCount(); got != 2 {
		t.Errorf("expected 2 attempts (429 then success), got %d", got)
	}
}

// ThreadReplies and Permalink pass through to the wrapped sender (gated only
// by the global backstop).
func TestLimitedThreadRepliesAndPermalinkPassThrough(t *testing.T) {
	f := &fakeSender{}
	l := NewLimitedWith(f, openLimits(), nil)

	msgs, err := l.ThreadReplies(context.Background(), "C1", "1.0", 10)
	if err != nil || len(msgs) != 1 || msgs[0].Text != "root" {
		t.Errorf("ThreadReplies = (%v, %v), want the wrapped sender's reply", msgs, err)
	}
	if len(f.repliesCalls) != 1 || f.repliesCalls[0] != "C1|1.0" {
		t.Errorf("ThreadReplies not forwarded, got %v", f.repliesCalls)
	}

	link, err := l.Permalink(context.Background(), "C1", "2.0")
	if err != nil || link != "https://slack.test/C1/2.0" {
		t.Errorf("Permalink = (%q, %v), want the wrapped sender's link", link, err)
	}
	if len(f.permalinkCalls) != 1 || f.permalinkCalls[0] != "C1|2.0" {
		t.Errorf("Permalink not forwarded, got %v", f.permalinkCalls)
	}
}
