package app

import (
	"context"
	"time"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

func lifecycleText(t *thread, text string) string {
	if !t.multiplayer() {
		return text
	}
	return ownerPrefixed(t.ownerUserID, text)
}

func (a *App) threadLink(ctx context.Context, t *thread) string {
	if t.threadLink == "" {
		link, err := a.poster.Permalink(ctx, t.channel, t.threadTS)
		if err != nil {
			a.tel.Debug(ctx, "thread permalink failed", "err", err)
		}
		t.threadLink = link
	}
	return linkOr(t.threadLink, "the thread")
}

func (a *App) postStatus(ctx context.Context, t *thread, text string) {
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	a.postStatusLocked(ctx, t, lifecycleText(t, text))
}

func (a *App) postStatusLocked(ctx context.Context, t *thread, rendered string) (string, error) {
	ts, err := a.poster.PostMessage(ctx, t.channel,
		statusMeta(t.sessionID, t.meta()), slack.MsgOptionText(rendered, false), slack.MsgOptionTS(t.threadTS))
	if err != nil {
		a.log.ErrorContext(ctx, "post status message failed", "channel", t.channel, "err", err)
	}
	t.applyMeta(func(m *sessionMeta) {
		m.StatusMessageTS = ts
		m.statusText = rendered
	})
	return ts, err
}

func (a *App) editStatus(ctx context.Context, t *thread, text string) {
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	a.editStatusLocked(ctx, t, lifecycleText(t, text))
}

func (a *App) editStatusLocked(ctx context.Context, t *thread, rendered string) {
	statusTS := t.status()
	if statusTS == "" {
		_, _ = a.postStatusLocked(ctx, t, rendered)
		return
	}
	if _, err := a.poster.UpdateMessage(ctx, t.channel, statusTS,
		statusMeta(t.sessionID, t.meta()), slack.MsgOptionText(rendered, false)); err != nil {
		a.log.WarnContext(ctx, "edit status message failed",
			"session", t.sessionID, "channel", t.channel, "ts", statusTS, "err", err)
		return
	}
	t.applyMeta(func(m *sessionMeta) { m.statusText = rendered })
}

func (a *App) restateStatus(ctx context.Context, t *thread, text string) {
	if t.multiplayer() {
		a.editStatus(ctx, t, text)
		a.tellOwner(ctx, t, text)
		return
	}
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	rendered := lifecycleText(t, text)
	statusTS := t.status()
	ts, err := a.poster.PostMessage(ctx, t.channel,
		statusMeta(t.sessionID, t.meta()), slack.MsgOptionText(rendered, false), slack.MsgOptionTS(t.threadTS))
	if err != nil {
		a.log.WarnContext(ctx, "could not restate the status message at the end of the thread; editing it in place",
			"session", t.sessionID, "channel", t.channel, "err", err)
		a.editStatusLocked(ctx, t, rendered)
		return
	}
	if statusTS != "" {
		if err := a.poster.DeleteMessage(ctx, t.channel, statusTS); err != nil {
			a.log.WarnContext(ctx, "could not take down the superseded status message",
				"session", t.sessionID, "channel", t.channel, "ts", statusTS, "err", err)
		}
	}
	t.applyMeta(func(m *sessionMeta) {
		m.StatusMessageTS = ts
		m.statusText = rendered
	})
}

func endedStatus(p *pb.SessionEnded, approvalWindow time.Duration, workflow string) string {
	const resume = " @mention me here and I'll continue in a fresh session."
	switch p.GetReason() {
	case api.EndEnded:
		return msgStatusFinished
	case api.EndInterrupted:
		return msgStatusInterrupted
	case api.EndIdle:
		return msgStatusIdleEnded
	case api.EndRevoked:
		return ":x: The approval for this session was withdrawn, so I've stopped and cleaned up." + resume
	case api.EndNeverApproved:
		return statusLapsed(approvalWindow, workflow)
	case api.EndExpired:
		return ":x: This session reached its time limit, so I've stopped and cleaned up." + resume
	case api.EndAgentExit:
		return ":x: The agent stopped, so this session is over." + resume
	case api.EndTunnelLost:
		return ":x: I lost my connection to this session's workspace and it didn't come back." + resume
	case api.EndPrepareFailed:
		return statusStopped(stopPrepareFailed)
	case api.EndRunCreateFailed:
		return statusStopped(stopRunFailed)
	case api.EndAttachTimeout:
		return statusStopped(stopNeverConnected)
	case api.EndLaunchFailed:
		return statusStopped(stopActivateFailed)
	default:
		return ":x: This session stopped unexpectedly." + resume
	}
}

func (a *App) deliverApproval(ctx context.Context, t *thread, p *pb.ApprovalRequired) {
	channel, ts, err := a.poster.PostDM(ctx, t.ownerUserID,
		append([]slack.MsgOption{chromeMeta(t.sessionID)},
			approvalMsgOptions(t.ownerUserID, t.workflow, a.threadLink(ctx, t), p.GetApprovalUrl())...)...)
	if err != nil {
		a.log.ErrorContext(ctx, "failed to DM the approval prompt; ending the session", "err", err)
		a.editStatus(ctx, t, statusStopped(stopDMFailed))
		if _, err := a.api.EndSession(ctx, &pb.EndSessionRequest{Ref: a.ref(t.sessionID)}); err != nil {
			a.log.WarnContext(ctx, "could not end a session whose approval could not be delivered",
				"session", t.sessionID, "err", err)
		}
		return
	}
	a.saveMeta(ctx, t, func(m *sessionMeta) {
		m.ApprovalChannelID = channel
		m.ApprovalMessageTS = ts
	})
}

func (a *App) updateApprovalDM(ctx context.Context, t *thread) {
	m := t.meta()
	if m.ApprovalChannelID == "" || m.ApprovalMessageTS == "" {
		return
	}
	if prev, _ := t.approvalDMEdited.Swap(m.ApprovalMessageTS).(string); prev == m.ApprovalMessageTS {
		return
	}
	if _, err := a.poster.UpdateMessage(ctx, m.ApprovalChannelID, m.ApprovalMessageTS,
		dmFinalOptions(t.sessionID, approvalDMDoneText(linkOr(t.threadLink, "your thread")))...); err != nil {
		t.approvalDMEdited.CompareAndSwap(m.ApprovalMessageTS, "")
		a.tel.Debug(ctx, "edit approval DM failed", "session", t.sessionID, "err", err)
	}
}

func approvalMsgOptions(userID, workflow, threadLink, approvalURL string) []slack.MsgOption {
	text := approvalDMText(userID, workflow, threadLink)
	btn := slack.NewButtonBlockElement("approve_run", "",
		slack.NewTextBlockObject(slack.PlainTextType, "Review & approve", false, false))
	btn.URL = approvalURL
	blocks := []slack.Block{
		slack.NewSectionBlock(slack.NewTextBlockObject(slack.MarkdownType, text, false, false), nil, nil),
		slack.NewActionBlock("approve_run_actions", btn),
	}
	return []slack.MsgOption{
		slack.MsgOptionText(approvalDMFallback(userID, workflow, threadLink, approvalURL), false),
		slack.MsgOptionBlocks(blocks...),
	}
}

func dmFinalOptions(sessionID, text string) []slack.MsgOption {
	return []slack.MsgOption{
		chromeMeta(sessionID),
		slack.MsgOptionText(text, false),
		slack.MsgOptionBlocks(slack.NewSectionBlock(
			slack.NewTextBlockObject(slack.MarkdownType, text, false, false), nil, nil)),
	}
}

func (a *App) ReconcileOnStartup(ctx context.Context) {
	res, err := a.api.ListSessions(ctx, &pb.ListSessionsRequest{LiveOnly: true})
	if err != nil {
		a.log.ErrorContext(ctx, "startup reconcile: list sessions failed; the sweeper picks the sessions up", "err", err)
		return
	}
	sessions := res.GetSessions()
	deferred := 0
	for _, view := range sessions {
		if !a.reconcile(ctx, view) {
			deferred++
		}
	}
	a.log.InfoContext(ctx, "slack startup reconcile complete",
		"live_sessions", len(sessions), "left_to_the_sweeper", deferred)
}

func (a *App) followAgain(ctx context.Context, view *pb.SessionView, m sessionMeta) {
	t := threadFromMeta(view, m)
	t.replayThrough = view.GetLastSeq()
	if ok, _ := a.registerThread(t); !ok {
		return
	}
	after := a.turnStartBefore(ctx, view.GetId(), m.LastSeq)
	a.startConsumer(t, "", after)
	a.log.InfoContext(ctx, "following a live session again",
		"session", view.GetId(), "state", view.GetState(), "after_seq", after)
}

func (a *App) turnStartBefore(ctx context.Context, sessionID string, cursor int64) int64 {
	boundary, inTurn, after := int64(0), false, int64(0)
	for after < cursor {
		res, err := a.api.ListEvents(ctx, &pb.ListEventsRequest{Ref: a.ref(sessionID), AfterSeq: after})
		if err != nil {
			a.log.WarnContext(ctx, "startup reconcile: could not read a session's history; resuming from its saved cursor",
				"session", sessionID, "err", err)
			return cursor
		}
		events := res.GetEvents()
		if len(events) == 0 {
			break
		}
		for _, ev := range events {
			if ev.GetSeq() > cursor {
				return boundary
			}
			var atBoundary bool
			if atBoundary, inTurn = turnBoundary(ev, inTurn); atBoundary {
				boundary = ev.GetSeq()
			}
			after = ev.GetSeq()
		}
	}
	return boundary
}
