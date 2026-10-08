package app

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cenkalti/backoff/v7"
	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/api/client"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/slackbot/internal/gateway"
	"github.com/pomerium/agentops/slackbot/internal/mdsplit"
	"github.com/pomerium/agentops/slackbot/internal/telemetry"
)

const maxTextChars = 3900

const maxMessageChars = maxTextChars

type renderer struct {
	mu       sync.Mutex
	curTS    string
	reacted  bool
	turnID   string
	pieceTS  map[int]string
	text     string
	broken   bool
	finished bool
	permTS   map[string]string
}

func newRenderer() *renderer { return &renderer{permTS: map[string]string{}} }

func (r *renderer) beginTurn(turnID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.turnID == turnID {
		return
	}
	r.turnID = turnID
	r.curTS = ""
	r.reacted = false
	r.pieceTS = nil
	r.text = ""
	r.broken = false
	r.finished = false
}

func (r *renderer) unfinish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finished = false
}

func (r *renderer) toolCall() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.broken = r.text != ""
}

func (r *renderer) finish(turnID string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.turnID != turnID || r.finished || r.text == "" {
		return "", false
	}
	r.finished = true
	return r.text, true
}

func (r *renderer) appendPart(part string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.broken && part != "" && !endsInSpace(r.text) && !startsInSpace(part) {
		r.text += "\n\n"
	}
	r.broken = false
	r.text += part
	return r.text
}

func endsInSpace(s string) bool {
	r, _ := utf8.DecodeLastRuneInString(s)
	return unicode.IsSpace(r)
}

func startsInSpace(s string) bool {
	r, _ := utf8.DecodeRuneInString(s)
	return unicode.IsSpace(r)
}

func (a *App) startConsumer(t *thread, ackTS string, afterSeq int64) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(context.Background()))
	t.stop = cancel
	if t.render == nil {
		t.render = newRenderer()
	}
	if !t.meta().Watching {
		a.saveMeta(context.Background(), t, func(m *sessionMeta) { m.Watching = true })
	}
	go func() {
		defer cancel()
		a.consume(ctx, t, ackTS, afterSeq)
	}()
}

func (a *App) consume(ctx context.Context, t *thread, ackTS string, afterSeq int64) {
	ctx = telemetry.With(ctx, "session_id", t.sessionID, "channel", t.channel, "thread_ts", t.threadTS)
	defer a.unregisterThread(t)
	defer a.drainBusy(context.WithoutCancel(ctx), t)
	sub, err := a.subscribe(ctx, t, afterSeq)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		a.log.ErrorContext(ctx, "could not follow this session's events; the thread will not update",
			"session", t.sessionID, "err", err)
		return
	}
	defer sub.Close()

	if ackTS == "" {
		ackTS = t.threadTS
	}
	inTurn := false
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			if ctx.Err() != nil {
				return
			}
			var atBoundary bool
			atBoundary, inTurn = turnBoundary(ev, inTurn)
			if atBoundary {
				t.applyMeta(func(m *sessionMeta) { m.LastSeq = max(m.LastSeq, ev.GetSeq()) })
			}
			a.renderEvent(ctx, t, ackTS, ev)
		}
	}
}

func turnBoundary(ev *pb.Event, inTurn bool) (atBoundary, stillInTurn bool) {
	switch ev.GetPayload().(type) {
	case *pb.Event_TurnCompleted, *pb.Event_TurnFailed, *pb.Event_SessionEnded:
		return true, false
	}
	if ev.GetTurnId() != "" {
		return false, true
	}
	return !inTurn, inTurn
}

func (a *App) subscribe(ctx context.Context, t *thread, afterSeq int64) (*client.Subscription, error) {
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = 500 * time.Millisecond
	policy.MaxInterval = 30 * time.Second
	return backoff.Retry(ctx, func() (*client.Subscription, error) {
		sub, err := client.Subscribe(ctx, a.api, &pb.SubscribeRequest{
			Ref: a.ref(t.sessionID), AfterSeq: afterSeq,
		}, client.WithLogger(a.log))
		switch {
		case err == nil:
			return sub, nil
		case errors.Is(err, api.ErrNotFound), errors.Is(err, api.ErrForbidden), errors.Is(err, api.ErrInvalidArgument):
			return nil, backoff.Permanent(err)
		default:
			return nil, err
		}
	},
		backoff.WithBackOff(policy),
		backoff.WithMaxElapsedTime(0),
		backoff.WithNotify(func(err error, next time.Duration) {
			a.log.WarnContext(ctx, "could not open this session's event stream; retrying",
				"session", t.sessionID, "retry_in", next, "err", err)
		}))
}

func (a *App) renderEvent(ctx context.Context, t *thread, ackTS string, ev *pb.Event) {
	switch p := ev.GetPayload().(type) {
	case *pb.Event_StateChanged:
		a.renderState(ctx, t, ackTS, p.StateChanged, ev.GetSeq())

	case *pb.Event_ApprovalRequired:
		if exp := p.ApprovalRequired.GetExpiresAt(); exp != nil {
			t.approvalWindow.Store(int64(time.Until(exp.AsTime())))
		}
		if t.pastApproval(ev.GetSeq()) {
			return
		}
		a.deliverApproval(ctx, t, p.ApprovalRequired)

	case *pb.Event_Approved:
		a.updateApprovalDM(ctx, t)

	case *pb.Event_AgentMessage:
		t.render.beginTurn(ev.GetTurnId())
		text := t.render.appendPart(p.AgentMessage.GetText())
		if p.AgentMessage.GetFinal() {
			t.render.finish(ev.GetTurnId())
			if !a.showFinal(ctx, t, text) {
				t.render.unfinish()
			}
			return
		}
		a.showIntermediary(ctx, t, text)

	case *pb.Event_AgentThought:
		a.log.DebugContext(ctx, "agent thought",
			"session", t.sessionID, "turn_id", ev.GetTurnId(), "text", p.AgentThought.GetText())

	case *pb.Event_ToolCall:
		t.render.toolCall()
		tc := p.ToolCall
		a.log.DebugContext(ctx, "tool call",
			"session", t.sessionID, "turn_id", ev.GetTurnId(),
			"id", tc.GetId(), "title", tc.GetTitle(), "kind", tc.GetKind(), "status", tc.GetStatus(), "update", tc.GetUpdate())

	case *pb.Event_Usage:
		u := p.Usage
		a.log.DebugContext(ctx, "agent usage",
			"session", t.sessionID, "turn_id", ev.GetTurnId(),
			"input_tokens", u.GetInputTokens(), "output_tokens", u.GetOutputTokens(),
			"total_tokens", u.GetTotalTokens(), "cost_usd", u.GetCostUsd())

	case *pb.Event_PermissionRequest:
		a.askPermission(ctx, t, p.PermissionRequest)

	case *pb.Event_PermissionResolved:
		a.closePermission(ctx, t, p.PermissionResolved)

	case *pb.Event_TurnCompleted:
		a.endTurn(ctx, t, ev.GetTurnId())

	case *pb.Event_TurnFailed:
		a.endTurn(ctx, t, ev.GetTurnId())
		a.log.WarnContext(ctx, "turn failed", "session", t.sessionID, "turn_id", ev.GetTurnId(), "reason", p.TurnFailed.GetReason())
		a.tellOwner(ctx, t, msgTurnFailed)

	case *pb.Event_LaunchStalled:
		a.editStatus(ctx, t, msgStatusWorkspaceUnreachable)

	case *pb.Event_IdleWarning:
		a.warnIdle(ctx, t, p.IdleWarning.GetLead().AsDuration())

	case *pb.Event_Suspended:
		a.clearIdleWarning(ctx, t, msgIdleClosed)
		if t.pastPause(ev.GetSeq()) {
			return
		}
		a.restateStatus(ctx, t, msgStatusIdleSuspended)
		a.watchSuspended(ctx, t, ev.GetSeq())

	case *pb.Event_Revived:
		a.swapReaction(ctx, t.channel, ackTS, reactionStarting, reactionReady)

	case *pb.Event_Released:
		a.editStatus(ctx, t, statusWorkspaceReleased(p.Released.GetRetainedFor().AsDuration()))
		t.released.Store(true)

	case *pb.Event_SessionEnded:
		a.renderEnded(ctx, t, ackTS, p.SessionEnded)
	}
}

func (a *App) renderState(ctx context.Context, t *thread, ackTS string, p *pb.StateChanged, seq int64) {
	if p.GetNew() == api.StateEnded {
		from := p.GetOld()
		t.endedFrom.Store(&from)
	}
	t.setState(p.GetNew())
	switch p.GetNew() {
	case api.StateAwaitingApproval:
		a.editStatus(ctx, t, msgStatusAwaitingApproval)
	case api.StateRunning:
		if t.openingTurn.CompareAndSwap(true, false) {
			a.markBusy(ctx, t)
		}
		a.swapReaction(ctx, t.channel, ackTS, reactionStarting, reactionReady)
		a.editStatus(ctx, t, readyStatusFor(t.busy.Load() > 0, t.multiplayer()))
		a.updateApprovalDM(ctx, t)
	case api.StateSuspended:
		switch p.GetReason() {
		case api.ReasonReviveFailed:
			a.swapReaction(ctx, t.channel, ackTS, reactionStarting, reactionFailed)
			t.reviveMention.Store(nil)
			a.drainBusy(ctx, t)
			if t.pastPause(seq) {
				return
			}
			a.restateStatus(ctx, t, msgStatusRetryContinue)
			a.watchSuspended(ctx, t, seq)
		case api.ReasonResumeUnavailable:
			a.swapReaction(ctx, t.channel, ackTS, reactionStarting, reactionFailed)
			in := t.reviveMention.Swap(nil)
			if in == nil {
				a.log.InfoContext(ctx, "a continuation failed with no request to start over from",
					"session", t.sessionID)
				return
			}
			a.startOver(ctx, t, *in)
		}
	}
}

func (a *App) renderEnded(ctx context.Context, t *thread, ackTS string, p *pb.SessionEnded) {
	a.clearIdleWarning(ctx, t, msgIdleClosed)
	from := t.currentState()
	if f := t.endedFrom.Load(); f != nil {
		from = *f
	}
	switch from {
	case api.StateRunning, api.StatePending, api.StateLaunching, api.StateAwaitingApproval:
		a.swapReaction(ctx, t.channel, ackTS, reactionStarting, endReaction(p.GetReason()))
	}
	t.setState(api.StateEnded)
	if t.released.Load() {
		a.saveMeta(ctx, t, func(m *sessionMeta) { m.Watching = false })
		return
	}
	t.applyMeta(func(m *sessionMeta) { m.Watching = false })
	a.restateStatus(ctx, t, endedStatus(p, time.Duration(t.approvalWindow.Load()), t.workflow))
}

func endReaction(reason api.EndReason) string {
	if reason == api.EndEnded {
		return reactionReady
	}
	return reactionFailed
}

func (a *App) endTurn(ctx context.Context, t *thread, turnID string) {
	delivered := true
	if text, ok := t.render.finish(turnID); ok {
		delivered = a.showFinal(ctx, t, text)
	}
	a.clearBusy(ctx, t)
	if delivered {
		a.saveMeta(ctx, t, func(m *sessionMeta) { m.AnswerTurn, m.AnswerTS = "", "" })
	} else {
		a.log.ErrorContext(ctx, "the turn's answer did not reach Slack after a retry",
			"session", t.sessionID, "turn_id", turnID)
	}
	a.settleReadyStatus(ctx, t)
	t.render.mu.Lock()
	finalTS, reacted := t.render.curTS, t.render.reacted
	t.render.reacted = false
	t.render.mu.Unlock()
	if reacted && finalTS != "" {
		a.removeReaction(ctx, t.channel, finalTS, reactionBusy)
	}
}

func (a *App) settleReadyStatus(ctx context.Context, t *thread) {
	if t.busy.Load() > 0 || t.currentState() != api.StateRunning {
		return
	}
	if t.meta().statusText != lifecycleText(t, msgStatusReadyWorking) {
		return
	}
	a.editStatus(ctx, t, readyStatusFor(false, t.multiplayer()))
}

func (a *App) startOver(ctx context.Context, t *thread, in gateway.MentionInvocation) {
	ctx = context.WithoutCancel(ctx)
	t.reviveMention.Store(nil)
	if t.stop != nil {
		t.stop()
	}
	a.drainBusy(ctx, t)
	if _, err := a.api.EndSession(ctx, &pb.EndSessionRequest{Ref: a.ref(t.sessionID)}); err != nil {
		a.log.ErrorContext(ctx, "could not end a session that cannot be continued; not starting over",
			"session", t.sessionID, "err", err)
		a.tellOwner(ctx, t, msgTurnRejected)
		return
	}
	t.setState(api.StateEnded)
	t.applyMeta(func(m *sessionMeta) { m.Watching = false })
	a.restateStatus(ctx, t, msgStatusCannotContinue)
	a.unregisterThread(t)
	a.startJoin(ctx, in, !t.multiplayer())
}

func (a *App) showIntermediary(ctx context.Context, t *thread, seg string) {
	seg = mdsplit.Split(seg, maxMessageChars)[0]
	t.render.mu.Lock()
	cur := t.render.curTS
	t.render.mu.Unlock()

	content := messageContent(t, withEllipsis(seg), true)
	if cur != "" {
		a.poster.UpdateMessageDebounced(ctx, t.channel, cur, content...)
		return
	}
	ts, err := a.poster.PostMessage(ctx, t.channel, append(content, slack.MsgOptionTS(t.threadTS))...)
	if err != nil {
		a.log.ErrorContext(ctx, "post agent message failed",
			"channel", t.channel, "thread_ts", t.threadTS, "err", err)
		return
	}
	t.render.mu.Lock()
	t.render.curTS = ts
	t.render.reacted = true
	t.render.mu.Unlock()
	a.rememberAnswer(ctx, t, ts)
	if err := a.poster.AddReaction(ctx, t.channel, ts, reactionBusy); err != nil {
		a.log.DebugContext(ctx, "add busy reaction failed", "ts", ts, "err", err)
	}
}

func (a *App) showFinal(ctx context.Context, t *thread, seg string) bool {
	t.render.mu.Lock()
	cur := t.render.curTS
	sent := maps.Clone(t.render.pieceTS)
	t.render.mu.Unlock()
	if sent == nil {
		sent = map[int]string{}
	}

	delivered := true
	var firstTS string
	for i, piece := range mdsplit.Split(seg, maxMessageChars) {
		content := messageContent(t, piece, i == 0)
		known := sent[i]
		if i == 0 {
			known = cur
		}
		if known != "" {
			if _, err := a.poster.UpdateMessage(ctx, t.channel, known, content...); err != nil {
				a.log.ErrorContext(ctx, "update agent message failed",
					"channel", t.channel, "ts", known, "err", err)
				delivered = false
			}
			if i == 0 {
				firstTS = known
			}
			continue
		}
		ts, err := a.poster.PostMessage(ctx, t.channel, append(content, slack.MsgOptionTS(t.threadTS))...)
		if err != nil {
			a.log.ErrorContext(ctx, "post agent message failed",
				"channel", t.channel, "thread_ts", t.threadTS, "err", err)
			delivered = false
			continue
		}
		sent[i] = ts
		if i == 0 {
			firstTS = ts
			a.rememberAnswer(ctx, t, ts)
		}
	}
	t.render.mu.Lock()
	if firstTS != "" {
		t.render.curTS = firstTS
	}
	delete(sent, 0)
	t.render.pieceTS = sent
	t.render.mu.Unlock()
	return delivered
}

func (a *App) rememberAnswer(ctx context.Context, t *thread, ts string) {
	t.render.mu.Lock()
	turnID := t.render.turnID
	t.render.mu.Unlock()
	a.saveMeta(ctx, t, func(m *sessionMeta) { m.AnswerTurn, m.AnswerTS = turnID, ts })
}

func (a *App) askPermission(ctx context.Context, t *thread, p *pb.PermissionRequest) {
	for _, o := range p.GetOptions() {
		if v := gateway.PermissionValue(t.sessionID, p.GetRequestId(), o.GetId()); gateway.IsPermissionToken(v) {
			a.permissionTokens.Store(v, permissionChoice{sessionID: t.sessionID, requestID: p.GetRequestId(), optionID: o.GetId()})
		}
	}
	t.render.mu.Lock()
	_, shown := t.render.permTS[p.GetRequestId()]
	t.render.mu.Unlock()
	if shown {
		return
	}
	choices := make([]gateway.PermissionChoice, 0, len(p.GetOptions()))
	for _, o := range p.GetOptions() {
		choices = append(choices, gateway.PermissionChoice{OptionID: o.GetId(), Name: o.GetName(), Kind: o.GetKind()})
	}
	title := p.GetSummary()
	if title == "" {
		title = "a tool call"
	}
	blocks := gateway.PermissionBlocks(t.sessionID, p.GetRequestId(), t.ownerUserID, title, choices)
	ts, err := a.poster.PostMessage(ctx, t.channel,
		chromeMeta(t.sessionID), slack.MsgOptionBlocks(blocks...), slack.MsgOptionTS(t.threadTS))
	if err != nil {
		a.log.ErrorContext(ctx, "post permission prompt failed",
			"channel", t.channel, "thread_ts", t.threadTS, "err", err)
		return
	}
	t.render.mu.Lock()
	t.render.permTS[p.GetRequestId()] = ts
	t.render.mu.Unlock()
	a.saveMeta(ctx, t, func(m *sessionMeta) {
		m.PermissionPrompts = withPermissionPrompt(m.PermissionPrompts, p.GetRequestId(), ts)
	})
}

func (a *App) closePermission(ctx context.Context, t *thread, p *pb.PermissionResolved) {
	t.render.mu.Lock()
	ts := t.render.permTS[p.GetRequestId()]
	t.render.mu.Unlock()
	if ts == "" {
		return
	}
	text := permissionResolvedText(p)
	if _, err := a.poster.UpdateMessage(ctx, t.channel, ts,
		chromeMeta(t.sessionID), slack.MsgOptionText(text, false),
		slack.MsgOptionBlocks(slack.NewSectionBlock(
			slack.NewTextBlockObject(slack.MarkdownType, text, false, false), nil, nil))); err != nil {
		a.log.WarnContext(ctx, "could not close an answered permission prompt; keeping it so a replay closes it",
			"session", t.sessionID, "request", p.GetRequestId(), "err", err)
		return
	}
	t.render.mu.Lock()
	delete(t.render.permTS, p.GetRequestId())
	t.render.mu.Unlock()
	a.saveMeta(ctx, t, func(m *sessionMeta) {
		m.PermissionPrompts = withPermissionPrompt(m.PermissionPrompts, p.GetRequestId(), "")
	})
}

func (a *App) warnIdle(ctx context.Context, t *thread, lead time.Duration) {
	notice := idleNotice{channel: t.channel, dm: t.multiplayer()}
	var err error
	if notice.dm {
		notice.channel, notice.ts, err = a.poster.PostDM(ctx, t.ownerUserID, chromeMeta(t.sessionID),
			slack.MsgOptionText(idleWarningDM(lead, a.threadLink(ctx, t)), false))
	} else {
		notice.ts, err = a.poster.PostMessage(ctx, t.channel, chromeMeta(t.sessionID),
			slack.MsgOptionText(lifecycleText(t, idleWarning(lead)), false),
			slack.MsgOptionTS(t.threadTS))
	}
	if err != nil {
		a.log.WarnContext(ctx, "deliver idle warning failed", "session", t.sessionID, "err", err)
		return
	}
	t.idleWarn.Store(&notice)
}

func messageContent(t *thread, text string, turnStart bool) []slack.MsgOption {
	return []slack.MsgOption{
		contentMeta(t.sessionID),
		slack.MsgOptionBlocks(answerBlocks(t, text, turnStart)...),
		slack.MsgOptionText(truncateRunes(text, maxTextChars), false),
	}
}

func answerBlocks(t *thread, text string, turnStart bool) []slack.Block {
	blocks := gateway.AgentMessageBlocks(text)
	if !turnStart || !t.multiplayer() || t.ownerUserID == "" {
		return blocks
	}
	return append([]slack.Block{slack.NewContextBlock("agentops_attribution",
		slack.NewTextBlockObject(slack.MarkdownType, attribution(t.ownerUserID), false, false))},
		blocks...)
}

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

func withEllipsis(text string) string {
	t := strings.TrimRight(text, " \t\n")
	if strings.HasSuffix(t, "…") || strings.HasSuffix(t, "...") {
		return t
	}
	return t + " …"
}
