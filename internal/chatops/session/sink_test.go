package session

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/internal/sandbox"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// syncBuf is a goroutine-safe writer capturing slog output for assertions.
type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncBuf) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuf) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// debugLogger returns a debug-level logger writing to the returned buffer.
func debugLogger() (*slog.Logger, *syncBuf) {
	buf := &syncBuf{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// sinkReaction records one Add/RemoveReaction call observed by the fake.
type sinkReaction struct {
	op    string // "add" or "remove"
	ts    string
	emoji string
}

type sinkFakePoster struct {
	mu          sync.Mutex
	posts       int
	updates     int
	debounced   int
	lines       []string // captured content of posted (not updated) messages
	updateTexts []string // captured content of guaranteed updates
	debTexts    []string // captured content of debounced updates
	reactions   []sinkReaction
}

func msgText(opts []slack.MsgOption) string {
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

func (p *sinkFakePoster) PostMessage(_ context.Context, _ string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.posts++
	p.lines = append(p.lines, msgText(opts))
	return "ts-1", nil
}

func (p *sinkFakePoster) PostEphemeral(_ context.Context, _, _ string, _ ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.posts++
	return "ts-1", nil
}

func (p *sinkFakePoster) UpdateMessage(_ context.Context, _, ts string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updates++
	p.updateTexts = append(p.updateTexts, msgText(opts))
	return ts, nil
}

func (p *sinkFakePoster) UpdateMessageDebounced(_ context.Context, _, _ string, opts ...slack.MsgOption) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.debounced++
	p.debTexts = append(p.debTexts, msgText(opts))
}

func (p *sinkFakePoster) Respond(_ context.Context, _ string, _ bool, _ string, _ []slack.Block) error {
	return nil
}

func (p *sinkFakePoster) ThreadReplies(_ context.Context, _, _ string, _ int) ([]slack.Message, error) {
	return nil, nil
}

func (p *sinkFakePoster) Permalink(_ context.Context, channel, ts string) (string, error) {
	return "https://slack.test/archives/" + channel + "/p" + ts, nil
}

func (p *sinkFakePoster) AddReaction(_ context.Context, _, ts, emoji string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reactions = append(p.reactions, sinkReaction{op: "add", ts: ts, emoji: emoji})
	return nil
}

func (p *sinkFakePoster) RemoveReaction(_ context.Context, _, ts, emoji string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reactions = append(p.reactions, sinkReaction{op: "remove", ts: ts, emoji: emoji})
	return nil
}

func (p *sinkFakePoster) counts() (posts, updates, debounced int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.posts, p.updates, p.debounced
}

func (p *sinkFakePoster) allLines() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.lines, "\n")
}

func (p *sinkFakePoster) lastDebounced() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.debTexts) == 0 {
		return ""
	}
	return p.debTexts[len(p.debTexts)-1]
}

func (p *sinkFakePoster) lastUpdate() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.updateTexts) == 0 {
		return ""
	}
	return p.updateTexts[len(p.updateTexts)-1]
}

func (p *sinkFakePoster) reactionOps() []sinkReaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sinkReaction(nil), p.reactions...)
}

// A turn without tool calls renders exactly once: chunks are buffered and the
// complete text is posted at end of turn — no mid-stream updates, no busy
// reaction, no ellipsis.
func TestThreadSinkPostsOnceWhenComplete(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, discardLogger())

	s.BeginTurn()
	s.AgentMessage(context.Background(), "Hello")
	s.AgentMessage(context.Background(), " world")
	if posts, updates, debounced := p.counts(); posts != 0 || updates != 0 || debounced != 0 {
		t.Fatalf("nothing should render before the turn completes; got posts=%d updates=%d debounced=%d", posts, updates, debounced)
	}

	s.EndTurn(context.Background())
	posts, updates, debounced := p.counts()
	if posts != 1 || updates != 0 || debounced != 0 {
		t.Errorf("expected a single post at end of turn; got posts=%d updates=%d debounced=%d", posts, updates, debounced)
	}
	if !strings.Contains(p.allLines(), "Hello world") {
		t.Errorf("expected the complete text, got: %q", p.allLines())
	}
	if strings.Contains(p.allLines(), "…") {
		t.Errorf("final output must not carry the in-progress ellipsis, got: %q", p.allLines())
	}
	if len(p.reactionOps()) != 0 {
		t.Errorf("no busy reaction without an intermediary message, got %v", p.reactionOps())
	}
}

func TestThreadSinkBeginTurnCounts(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, discardLogger())
	if got := s.BeginTurn(); got != 1 {
		t.Errorf("first BeginTurn = %d, want 1", got)
	}
	if got := s.BeginTurn(); got != 2 {
		t.Errorf("second BeginTurn = %d, want 2", got)
	}
}

// Intermediary segments (complete narration between tool calls) live-replace a
// single message marked in-progress (busy reaction + trailing ellipsis); the
// final answer replaces it via the guaranteed path and clears the marker.
func TestThreadSinkIntermediarySegmentsLiveReplace(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, discardLogger())

	s.BeginTurn()
	s.AgentMessage(context.Background(), "narration one")
	s.ToolCall(context.Background(), sandbox.ToolCallEvent{ID: "c1", Title: "Reading", Status: "pending"})

	posts, _, _ := p.counts()
	if posts != 1 {
		t.Fatalf("first complete segment should post once, got %d posts", posts)
	}
	if !strings.Contains(p.allLines(), "narration one …") {
		t.Errorf("intermediary message should end with an ellipsis, got: %q", p.allLines())
	}
	ops := p.reactionOps()
	if len(ops) != 1 || ops[0].op != "add" || ops[0].emoji != "waiting" || ops[0].ts != "ts-1" {
		t.Errorf("expected the busy reaction added to the intermediary message, got %v", ops)
	}

	s.AgentMessage(context.Background(), "narration two")
	s.ToolCall(context.Background(), sandbox.ToolCallEvent{ID: "c2", Title: "Searching", Status: "pending"})
	if _, _, debounced := p.counts(); debounced != 1 {
		t.Errorf("subsequent segments should replace via the droppable path, got %d", debounced)
	}
	if got := p.lastDebounced(); !strings.Contains(got, "narration two …") || strings.Contains(got, "narration one") {
		t.Errorf("replacement should show only the new segment with ellipsis, got: %q", got)
	}

	s.AgentMessage(context.Background(), "the final answer")
	s.EndTurn(context.Background())
	if _, updates, _ := p.counts(); updates != 1 {
		t.Errorf("final answer should land via a guaranteed update, got %d", updates)
	}
	last := p.lastUpdate()
	if !strings.Contains(last, "the final answer") || strings.Contains(last, "…") || strings.Contains(last, "narration") {
		t.Errorf("final content should be the bare answer, got: %q", last)
	}
	ops = p.reactionOps()
	final := ops[len(ops)-1]
	if final.op != "remove" || final.emoji != "waiting" || final.ts != "ts-1" {
		t.Errorf("busy reaction should be removed at end of turn, got %v", ops)
	}
	if strings.Contains(p.allLines(), "Reading") || strings.Contains(p.allLines(), ":wrench:") {
		t.Errorf("tool calls must not be posted to Slack, got: %q", p.allLines())
	}
}

// EndTurn reports the ts of the turn's final answer message so the caller can
// link to it (e.g. cross-posting a permalink into an origin thread), and ""
// when the turn produced no visible output.
func TestEndTurnReturnsFinalMessageTS(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, discardLogger())

	// Fresh-post path: the final answer is a new message.
	s.BeginTurn()
	s.AgentMessage(context.Background(), "answer")
	if got := s.EndTurn(context.Background()); got != "ts-1" {
		t.Errorf("EndTurn after fresh post = %q, want %q", got, "ts-1")
	}

	// Update path: an intermediary message exists and the final answer updates it.
	s.BeginTurn()
	s.AgentMessage(context.Background(), "narration")
	s.ToolCall(context.Background(), sandbox.ToolCallEvent{ID: "c1", Title: "Reading", Status: "pending"})
	s.AgentMessage(context.Background(), "final")
	if got := s.EndTurn(context.Background()); got != "ts-1" {
		t.Errorf("EndTurn after update = %q, want %q", got, "ts-1")
	}

	// Empty turn: nothing was rendered.
	s.BeginTurn()
	if got := s.EndTurn(context.Background()); got != "" {
		t.Errorf("EndTurn with no output = %q, want empty", got)
	}
}

// A turn that ends on a tool call (no trailing text) still cleans up: the
// shown narration loses its ellipsis and the busy reaction is removed.
func TestThreadSinkTurnEndingOnToolCallCleansUp(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, discardLogger())

	s.BeginTurn()
	s.AgentMessage(context.Background(), "checking things")
	s.ToolCall(context.Background(), sandbox.ToolCallEvent{ID: "c1", Title: "Reading", Status: "pending"})
	s.EndTurn(context.Background())

	last := p.lastUpdate()
	if !strings.Contains(last, "checking things") || strings.Contains(last, "…") {
		t.Errorf("end of turn should re-render the narration without the ellipsis, got: %q", last)
	}
	ops := p.reactionOps()
	if len(ops) == 0 || ops[len(ops)-1].op != "remove" || ops[len(ops)-1].emoji != "waiting" {
		t.Errorf("busy reaction should be removed at end of turn, got %v", ops)
	}
}

// Tool calls and thoughts post nothing; they are logged at debug with the
// sandbox and turn tags instead.
func TestThreadSinkToolCallAndThoughtLogOnly(t *testing.T) {
	p := &sinkFakePoster{}
	log, buf := debugLogger()
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, log)
	s.setSandbox("sbx-9")

	s.BeginTurn()
	s.AgentThought(context.Background(), "pondering the schema")
	s.ToolCall(context.Background(), sandbox.ToolCallEvent{ID: "c1", Title: "Reading project files", Status: "pending"})
	s.ToolCall(context.Background(), sandbox.ToolCallEvent{ID: "c1", Status: "completed", Update: true})
	s.EndTurn(context.Background())

	posts, updates, debounced := p.counts()
	if posts != 0 || updates != 0 || debounced != 0 {
		t.Errorf("tool calls/thoughts must not touch Slack; got posts=%d updates=%d debounced=%d", posts, updates, debounced)
	}

	logs := buf.String()
	for _, want := range []string{"tool call", "Reading project files", "completed", "sandbox=sbx-9", "turn=1", "agent thought", "pondering the schema"} {
		if !strings.Contains(logs, want) {
			t.Errorf("debug log missing %q, got:\n%s", want, logs)
		}
	}
	if !strings.Contains(logs, "c1") {
		t.Errorf("debug log should carry the tool-call id, got:\n%s", logs)
	}
}

// Narration superseded by a tool call lands in the debug log (with the
// humanized kind as the title fallback).
func TestThreadSinkNarrationLogged(t *testing.T) {
	p := &sinkFakePoster{}
	log, buf := debugLogger()
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, log)

	s.BeginTurn()
	s.AgentMessage(context.Background(), "let me check the database")
	s.ToolCall(context.Background(), sandbox.ToolCallEvent{ID: "c1", Kind: "execute", Status: "pending"})

	if !strings.Contains(buf.String(), "let me check the database") {
		t.Errorf("expected superseded narration in the debug log, got:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "Running a command") {
		t.Errorf("expected humanized kind fallback in the log, got:\n%s", buf.String())
	}
}

// fallbackText extracts the plain-text fallback field (slack.MsgOptionText)
// that Slack caps at maxTextChars; exceeding it is the msg_too_long error.
func fallbackText(opts []slack.MsgOption) string {
	_, vals, _ := slack.UnsafeApplyMsgOptions("t", "c", "https://slack.com/api/", opts...)
	return vals.Get("text")
}

// The text fallback must never exceed Slack's text-field limit, regardless of
// how large the rendered blocks are — overshooting it is what returned
// msg_too_long and froze the turn's final message.
func TestMessageContentFallbackWithinSlackLimit(t *testing.T) {
	opts := messageContent(strings.Repeat("a", maxMessageChars))
	if n := utf8.RuneCountInString(fallbackText(opts)); n > maxTextChars {
		t.Errorf("fallback text is %d chars, exceeds Slack's %d-char limit", n, maxTextChars)
	}
}

// A final answer too large for one Slack message is split across several, all
// delivered guaranteed.
func TestThreadSinkFinalSplitsLongAnswer(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, discardLogger())

	s.BeginTurn()
	s.AgentMessage(context.Background(), strings.Repeat("a", maxMessageChars+100))
	s.EndTurn(context.Background())

	posts, updates, debounced := p.counts()
	if posts != 2 || updates != 0 || debounced != 0 {
		t.Errorf("expected the long answer split into 2 posts, got posts=%d updates=%d debounced=%d", posts, updates, debounced)
	}
}

func TestThreadSinkPermissionResolves(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", time.Minute, discardLogger())

	type result struct {
		d   sandbox.PermissionDecision
		err error
	}
	done := make(chan result, 1)
	go func() {
		d, err := s.Permission(context.Background(), sandbox.PermissionRequest{
			ToolCallID: "call-9",
			Title:      "Edit file",
			Options:    []sandbox.PermissionOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}},
		})
		done <- result{d, err}
	}()

	// Give the goroutine a moment to register the waiter and post the prompt.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		_, ok := s.waiters["call-9"]
		s.mu.Unlock()
		if ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	s.resolvePermission("call-9", sandbox.PermissionDecision{OptionID: "allow"})

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Permission returned error: %v", r.err)
		}
		if r.d.OptionID != "allow" {
			t.Errorf("expected decision allow, got %+v", r.d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Permission did not return after resolve")
	}
}

func TestThreadSinkPermissionTimeout(t *testing.T) {
	p := &sinkFakePoster{}
	s := newThreadSink(p, "sess", "C1", "thread-1", 20*time.Millisecond, discardLogger())
	d, err := s.Permission(context.Background(), sandbox.PermissionRequest{
		ToolCallID: "call-x",
		Title:      "risky",
		Options:    []sandbox.PermissionOption{{ID: "allow", Name: "Allow"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !d.Cancelled {
		t.Errorf("expected a cancelled decision on timeout, got %+v", d)
	}
}
