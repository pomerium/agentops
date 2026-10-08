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
		return ":x: This session's approval ran out, so I've stopped and cleaned up." + resume
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
	ts, err := a.poster.PostMessage(ctx, t.ownerUserID,
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
		m.ApprovalChannelID = t.ownerUserID
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
		a.log.ErrorContext(ctx, "startup reconcile: list sessions failed", "err", err)
		return
	}
	sessions := res.GetSessions()
	settled := 0
	for _, view := range sessions {
		if view.GetState() == api.StateSuspended {
			continue
		}
		m, err := a.loadMeta(ctx, view)
		if err != nil {
			a.tel.Debug(ctx, "startup reconcile: no Slack state in this session's thread", "session", view.GetId())
			continue
		}
		a.settleInterrupted(ctx, view, m)
		settled++
	}
	a.log.InfoContext(ctx, "slack startup reconcile complete",
		"live_sessions", len(sessions), "threads_told_they_were_interrupted", settled)
}

func (a *App) settleInterrupted(ctx context.Context, view *pb.SessionView, m sessionMeta) {
	if view.GetState() == api.StateAwaitingApproval && m.ApprovalChannelID != "" && m.ApprovalMessageTS != "" {
		link, err := a.poster.Permalink(ctx, m.ChannelID, m.ThreadTS)
		if err != nil {
			a.tel.Debug(ctx, "thread permalink failed", "session", view.GetId(), "err", err)
		}
		if _, err := a.poster.UpdateMessage(ctx, m.ApprovalChannelID, m.ApprovalMessageTS,
			dmFinalOptions(view.GetId(), approvalDMStaleText(linkOr(link, "the thread")))...); err != nil {
			a.log.WarnContext(ctx, "startup reconcile: edit approval DM failed", "session", view.GetId(), "err", err)
		}
	}
	a.restateStatus(ctx, threadFromMeta(view, m), msgStatusInterrupted)
}
