package session

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/internal/channels/slack/gateway"
	"github.com/pomerium/agentops/internal/mdsplit"
	"github.com/pomerium/agentops/internal/sandbox"
)

// maxMessageChars splits output across messages past this size. It is kept
// comfortably below Slack's undocumented ~13,200-char total-blocks ceiling (a
// chat.update past it fails with msg_blocks_too_long and freezes the message).
const maxMessageChars = 11000

// maxTextChars caps the plain-text fallback (slack.MsgOptionText) at Slack's
// hard 4,000-char limit for the text field; exceeding it fails with
// msg_too_long. The real content rides in the section blocks — the fallback is
// only the notification/accessibility preview, so truncating it is harmless.
const maxTextChars = 3900

// threadSink implements sandbox.EventSink by rendering an ACP session's output
// into a Slack thread. A turn renders into a single message, and content is
// only shown once it is complete — chunks are buffered, never streamed
// partially. A narration segment becomes complete when a tool call interrupts
// it: it is then shown marked in-progress (a busy reaction plus a trailing
// ellipsis, so a half-finished look reads as intentional) and replaced by the
// next segment, converging to the turn's final answer, which clears the
// marker. Intermediary output — tool calls, thoughts, superseded narration —
// never reaches Slack; it is logged at debug level tagged with the sandbox and
// turn so the full trace lives in the application log instead.
//
// Note: these debug lines deliberately include payload content (tool titles,
// thought text, narration) — an explicit exception to the "kinds only, never
// payloads" rule the ACP-level traces follow. Keep payloads out of info-level
// logs.
type threadSink struct {
	poster      Poster
	sessionID   string
	channel     string
	threadTS    string
	permTimeout time.Duration
	log         *slog.Logger

	mu         sync.Mutex
	sandbox    string          // sandbox name, set once the launch returns it
	turn       int64           // current turn number, incremented by BeginTurn
	curTS      string          // ts of the turn's message, "" if none yet
	buf        strings.Builder // current segment: text since the last display
	lastShown  string          // last displayed intermediary segment (sans ellipsis)
	reacted    bool            // busy reaction currently on the turn's message
	thoughtBuf strings.Builder // current thinking span, logged once on span end

	toolTitle map[string]string // tool-call id -> best-known title

	waiters map[string]chan sandbox.PermissionDecision
}

var _ sandbox.EventSink = (*threadSink)(nil)

func newThreadSink(poster Poster, sessionID, channel, threadTS string, permTimeout time.Duration, log *slog.Logger) *threadSink {
	return &threadSink{
		poster:      poster,
		sessionID:   sessionID,
		channel:     channel,
		threadTS:    threadTS,
		permTimeout: permTimeout,
		log:         log,
		toolTitle:   map[string]string{},
		waiters:     map[string]chan sandbox.PermissionDecision{},
	}
}

// setSandbox records the sandbox name used to tag this session's debug trace.
// Called once after the launch returns it (the sink is built before launch).
func (s *threadSink) setSandbox(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sandbox = name
}

// tagsLocked returns the correlation attrs every debug line carries. Caller
// must hold s.mu.
func (s *threadSink) tagsLocked() []any {
	return []any{"session_id", s.sessionID, "sandbox", s.sandbox, "turn", s.turn}
}

// BeginTurn starts a fresh message for the next turn and returns the turn
// number (for the caller's own log correlation).
func (s *threadSink) BeginTurn() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turn++
	s.curTS = ""
	s.buf.Reset()
	s.lastShown = ""
	s.reacted = false
	s.thoughtBuf.Reset()
	return s.turn
}

// EndTurn delivers the turn's final output on the guaranteed path and clears
// the in-progress marker. If the turn ended on a tool call (no trailing text),
// the shown narration is re-rendered without the ellipsis so the message
// doesn't read as still running. It returns the ts of the turn's final answer
// message ("" when the turn produced no visible output) so the caller can link
// to it.
func (s *threadSink) EndTurn(ctx context.Context) string {
	s.mu.Lock()
	seg := s.buf.String()
	s.buf.Reset()
	if seg == "" {
		seg = s.lastShown
	}
	reacted := s.reacted
	s.reacted = false
	thought, thoughtTags := s.takeThoughtLocked()
	s.mu.Unlock()

	if thought != "" {
		s.log.DebugContext(ctx, "agent thought", append(thoughtTags, "text", thought)...)
	}
	var finalTS string
	if seg != "" {
		finalTS = s.showFinal(ctx, seg)
	}
	if reacted {
		s.removeBusyReaction(ctx)
	}
	return finalTS
}

// AgentMessage buffers text chunks; nothing renders until the segment is
// complete (interrupted by a tool call) or the turn ends.
func (s *threadSink) AgentMessage(ctx context.Context, text string) {
	if text == "" {
		return
	}
	s.mu.Lock()
	thought, tags := s.takeThoughtLocked() // visible output ends the thinking span
	s.buf.WriteString(text)
	s.mu.Unlock()
	if thought != "" {
		s.log.DebugContext(ctx, "agent thought", append(tags, "text", thought)...)
	}
}

// AgentThought buffers reasoning chunks; the whole span (a run of thought
// chunks not interrupted by visible output or a tool call) is logged as one
// debug line when it ends. Nothing is posted to Slack.
func (s *threadSink) AgentThought(_ context.Context, text string) {
	if text == "" {
		return
	}
	s.mu.Lock()
	s.thoughtBuf.WriteString(text)
	s.mu.Unlock()
}

// takeThoughtLocked drains the buffered thinking span, returning it along with
// the log tags captured under the same lock. Caller must hold s.mu.
func (s *threadSink) takeThoughtLocked() (string, []any) {
	if s.thoughtBuf.Len() == 0 {
		return "", nil
	}
	t := s.thoughtBuf.String()
	s.thoughtBuf.Reset()
	return t, s.tagsLocked()
}

// ToolCall completes the current narration segment (showing it as the turn's
// in-progress message and logging it) and logs the event at debug. Nothing
// about the tool call itself is posted to Slack.
func (s *threadSink) ToolCall(ctx context.Context, ev sandbox.ToolCallEvent) {
	s.mu.Lock()
	// Resolve the best title: prefer this event's, else one we saw earlier for
	// this tool-call id (updates often omit it), else a humanized kind — never
	// the opaque tool-call id.
	title := ev.Title
	if title == "" {
		title = s.toolTitle[ev.ID]
	}
	if title != "" {
		s.toolTitle[ev.ID] = title
	} else {
		title = friendlyKind(ev.Kind)
	}
	status := ev.Status
	if status == "" {
		status = "running"
	}

	thought, thoughtTags := s.takeThoughtLocked() // a tool call ends the thinking span
	seg := s.buf.String()
	s.buf.Reset()
	tags := s.tagsLocked()
	s.mu.Unlock()

	if thought != "" {
		s.log.DebugContext(ctx, "agent thought", append(thoughtTags, "text", thought)...)
	}
	if seg != "" {
		s.log.DebugContext(ctx, "narration superseded by tool call", append(tags, "text", seg)...)
		s.showIntermediary(ctx, seg)
	}
	s.log.DebugContext(ctx, "tool call",
		append(tags, "id", ev.ID, "title", title, "kind", ev.Kind, "status", status, "update", ev.Update)...)
}

// showIntermediary renders a complete narration segment as the turn's
// in-progress message: trailing ellipsis, busy reaction, droppable
// (last-one-wins) delivery for replacements. Oversized segments are truncated
// — the full text is in the debug log and the segment is transient anyway.
func (s *threadSink) showIntermediary(ctx context.Context, seg string) {
	seg = mdsplit.Split(seg, maxMessageChars)[0]
	s.mu.Lock()
	cur := s.curTS
	s.lastShown = seg
	s.mu.Unlock()

	content := messageContent(withEllipsis(seg))
	if cur != "" {
		s.poster.UpdateMessageDebounced(ctx, s.channel, cur, content...)
		return
	}
	ts, err := s.poster.PostMessage(ctx, s.channel, append(content, slack.MsgOptionTS(s.threadTS))...)
	if err != nil {
		s.log.ErrorContext(ctx, "post agent message failed",
			"channel", s.channel, "thread_ts", s.threadTS, "err", err)
		return
	}
	s.mu.Lock()
	s.curTS = ts
	s.reacted = true
	s.mu.Unlock()
	// Best-effort, like every reaction: if adding fails we still try the
	// removal at end of turn, which is equally harmless.
	if err := s.poster.AddReaction(ctx, s.channel, ts, reactionBusy); err != nil {
		s.log.DebugContext(ctx, "add busy reaction failed", "ts", ts, "err", err)
	}
}

// showFinal delivers the turn's final output on the guaranteed path, replacing
// the in-progress message when one exists and splitting output too large for a
// single Slack message across several. It returns the ts of the answer's first
// message ("" when every delivery failed).
func (s *threadSink) showFinal(ctx context.Context, seg string) string {
	s.mu.Lock()
	cur := s.curTS
	s.mu.Unlock()

	var firstTS string
	for i, piece := range mdsplit.Split(seg, maxMessageChars) {
		content := messageContent(piece)
		if i == 0 && cur != "" {
			if _, err := s.poster.UpdateMessage(ctx, s.channel, cur, content...); err != nil {
				s.log.ErrorContext(ctx, "update agent message failed",
					"channel", s.channel, "ts", cur, "err", err)
			}
			firstTS = cur
			continue
		}
		ts, err := s.poster.PostMessage(ctx, s.channel, append(content, slack.MsgOptionTS(s.threadTS))...)
		if err != nil {
			s.log.ErrorContext(ctx, "post agent message failed",
				"channel", s.channel, "thread_ts", s.threadTS, "err", err)
			continue
		}
		if firstTS == "" {
			firstTS = ts
		}
	}
	return firstTS
}

// removeBusyReaction clears the busy marker from the turn's message,
// best-effort.
func (s *threadSink) removeBusyReaction(ctx context.Context) {
	s.mu.Lock()
	cur := s.curTS
	s.mu.Unlock()
	if cur == "" {
		return
	}
	if err := s.poster.RemoveReaction(ctx, s.channel, cur, reactionBusy); err != nil {
		s.log.DebugContext(ctx, "remove busy reaction failed", "ts", cur, "err", err)
	}
}

// messageContent renders text as Block Kit sections with a plain-text fallback
// (so notifications and block-less surfaces show the content). The fallback is
// truncated to Slack's text-field limit; the full content lives in the blocks.
func messageContent(text string) []slack.MsgOption {
	return []slack.MsgOption{
		slack.MsgOptionBlocks(gateway.AgentMessageBlocks(text)...),
		slack.MsgOptionText(truncateRunes(text, maxTextChars), false),
	}
}

// truncateRunes shortens text to at most limit runes, cutting on a rune
// boundary so the fallback never carries a partial rune.
func truncateRunes(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	n := 0
	for i := range text {
		if n == limit {
			return text[:i]
		}
		n++
	}
	return text
}

// withEllipsis marks text as in-progress with a trailing ellipsis (unless it
// already ends with one).
func withEllipsis(text string) string {
	t := strings.TrimRight(text, " \t\n")
	if strings.HasSuffix(t, "…") || strings.HasSuffix(t, "...") {
		return t
	}
	return t + " …"
}

// friendlyKind turns an ACP tool kind into a human label.
func friendlyKind(kind string) string {
	switch strings.ToLower(kind) {
	case "read":
		return "Reading files"
	case "edit":
		return "Editing files"
	case "delete":
		return "Deleting"
	case "move":
		return "Moving"
	case "search":
		return "Searching"
	case "execute":
		return "Running a command"
	case "fetch":
		return "Fetching"
	case "think":
		return "Thinking"
	case "":
		return "Tool call"
	default:
		return strings.ToUpper(kind[:1]) + kind[1:]
	}
}

func (s *threadSink) Permission(ctx context.Context, req sandbox.PermissionRequest) (sandbox.PermissionDecision, error) {
	ch := make(chan sandbox.PermissionDecision, 1)
	s.mu.Lock()
	s.waiters[req.ToolCallID] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiters, req.ToolCallID)
		s.mu.Unlock()
	}()

	choices := make([]gateway.PermissionChoice, 0, len(req.Options))
	for _, o := range req.Options {
		choices = append(choices, gateway.PermissionChoice{OptionID: o.ID, Name: o.Name, Kind: o.Kind})
	}
	blocks := gateway.PermissionBlocks(s.sessionID, req.ToolCallID, req.Title, choices)
	if _, err := s.poster.PostMessage(ctx, s.channel, slack.MsgOptionBlocks(blocks...), slack.MsgOptionTS(s.threadTS)); err != nil {
		s.log.ErrorContext(ctx, "post permission prompt failed",
			"channel", s.channel, "thread_ts", s.threadTS, "err", err)
	}

	select {
	case d := <-ch:
		return d, nil
	case <-time.After(s.permTimeout):
		return sandbox.PermissionDecision{Cancelled: true}, nil
	case <-ctx.Done():
		return sandbox.PermissionDecision{Cancelled: true}, ctx.Err()
	}
}

// resolvePermission delivers the user's decision to a waiting Permission call.
func (s *threadSink) resolvePermission(toolCallID string, d sandbox.PermissionDecision) {
	s.mu.Lock()
	ch := s.waiters[toolCallID]
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- d:
		default:
		}
	}
}
