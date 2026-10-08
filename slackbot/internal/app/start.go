package app

import (
	"context"
	"errors"
	"strings"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/slackbot/internal/gateway"
	"github.com/pomerium/agentops/slackbot/internal/telemetry"
)

type startSpec struct {
	in              gateway.MentionInvocation
	template        string
	threadTS        string
	originKind      string
	parentSessionID string
	approvalPrompt  string
	agentPrompt     string
	threadLink      string
	ackTS           string
	joinedAt        string
	flipFrom        string
}

func (a *App) startSession(ctx context.Context, spec startSpec) *thread {
	in := spec.in
	ctx = telemetry.With(ctx, "template", spec.template, "origin_kind", spec.originKind)

	ackTS := spec.ackTS
	if ackTS == "" {
		ackTS = spec.threadTS
	}
	if strings.TrimSpace(spec.approvalPrompt) == "" {
		spec.approvalPrompt = approvalAsk(spec.originKind)
	}
	a.addReaction(ctx, in.ChannelID, ackTS, reactionStarting)

	t := &thread{
		ownerUserID: in.UserID,
		teamID:      in.TeamID,
		channel:     in.ChannelID,
		threadTS:    spec.threadTS,
		workflow:    spec.template,
		threadLink:  spec.threadLink,
	}
	t.setState(api.StatePending)
	ok, flipped := a.registerThread(t)
	if !ok {
		a.tel.Debug(ctx, "this person already has a session in this thread; not starting a second one")
		a.removeReaction(ctx, in.ChannelID, ackTS, reactionStarting)
		return nil
	}

	res, err := a.api.CreateSession(ctx, &pb.CreateSessionRequest{
		Template:             spec.template,
		ConversationRef:      conversationRef(in.ChannelID, spec.threadTS, in.TeamID, in.UserID),
		ParentSessionId:      spec.parentSessionID,
		ApprovalPrompt:       spec.approvalPrompt,
		InitialPrompt:        spec.agentPrompt,
		SystemPromptAppendix: composeAppendix(spec.joinedAt != ""),
	})
	if err != nil {
		a.unregisterThread(t)
		a.swapReaction(ctx, in.ChannelID, ackTS, reactionStarting, reactionFailed)
		a.log.ErrorContext(ctx, "create session failed", "err", err)
		text := startFailureText(err, spec.template)
		a.post(ctx, in.ChannelID, spec.threadTS, slack.MsgOptionText(text, false))
		return nil
	}
	view := res.GetSession()
	t.sessionID = view.GetId()
	ctx = telemetry.With(ctx, "session_id", view.GetId())

	t.applyMeta(func(m *sessionMeta) {
		m.ChannelID = in.ChannelID
		m.ThreadTS = spec.threadTS
		m.UserID = in.UserID
		m.TeamID = in.TeamID
		m.Multiplayer = m.Multiplayer || spec.joinedAt != ""
		if m.LastSeenTS == "" {
			m.LastSeenTS = spec.joinedAt
		}
	})
	a.flipRoom(ctx, t, flipped, spec.flipFrom)
	a.postStatus(ctx, t, msgStatusPreparing)
	a.startConsumer(t, ackTS, 0)

	a.log.InfoContext(ctx, "session started",
		"session_id", view.GetId(), "channel", in.ChannelID, "thread_ts", spec.threadTS,
		"owner", in.UserID, "template", spec.template, "origin_kind", spec.originKind,
		"parent_session_id", spec.parentSessionID)
	return t
}

func startFailureText(err error, template string) string {
	switch {
	case errors.Is(err, api.ErrNotFound), errors.Is(err, api.ErrForbidden):
		return missingAgent(template)
	case errors.Is(err, api.ErrConflict):
		return statusStopped("there's already a session running for this thread")
	case errors.Is(err, api.ErrQuotaExceeded):
		return statusStopped("I'm at my limit of running sessions right now — try again in a minute")
	default:
		return statusStopped("I couldn't start a session")
	}
}

var errBindingClaimed = errors.New("another session claimed this binding")

func (a *App) adopt(ctx context.Context, view *pb.SessionView, atTS string) (*thread, error) {
	m, err := a.loadMeta(ctx, view)
	if err != nil {
		return nil, err
	}
	t := threadFromMeta(view, m)
	ok, flipped := a.registerThread(t)
	if !ok {
		if existing := a.lookup(m.ChannelID, m.ThreadTS, m.TeamID, m.UserID); existing != nil {
			return existing, nil
		}
		return nil, errBindingClaimed
	}
	a.flipRoom(ctx, t, flipped, atTS)
	after := view.GetLastSeq()
	if m.Watching && m.LastSeq > 0 {
		after = m.LastSeq
	}
	a.startConsumer(t, "", after)
	return t, nil
}

func threadFromMeta(view *pb.SessionView, m sessionMeta) *thread {
	t := &thread{
		sessionID:   view.GetId(),
		ownerUserID: m.UserID,
		teamID:      m.TeamID,
		channel:     m.ChannelID,
		threadTS:    m.ThreadTS,
		workflow:    view.GetTemplate(),
		metaState:   m,
	}
	t.render = newRenderer()
	t.setState(view.GetState())
	return t
}

func (a *App) startJoin(ctx context.Context, in gateway.MentionInvocation) {
	ctx = telemetry.With(ctx, "origin_thread_ts", in.OriginThreadTS, "origin_kind", originJoin)
	ctx, op := a.tel.Start(ctx, "startJoin")
	defer op.Complete()

	template, ok := a.templateForChannel(ctx, in.ChannelID, in.OriginThreadTS)
	if !ok {
		return
	}

	roommates := a.roommates(ctx, in.ChannelID, in.OriginThreadTS, in.UserID, in.TeamID)
	if other := retemplated(roommates, template); other != nil {
		a.log.InfoContext(ctx, "the channel's agent changed since this thread started; not joining",
			"channel", in.ChannelID, "thread_ts", in.OriginThreadTS,
			"thread_template", other.workflow(), "channel_template", template)
		a.post(ctx, in.ChannelID, in.OriginThreadTS,
			slack.MsgOptionText(retemplatedRefusal(template, other.workflow()), false))
		return
	}

	flags := carryoverFlags{inflight: anyBusy(roommates)}
	replies, entries := a.readCarryover(ctx, in.ChannelID, in.OriginThreadTS, in.MessageTS, &flags)
	carried := carriedMessages(entries)

	threadLink, err := a.poster.Permalink(ctx, in.ChannelID, in.OriginThreadTS)
	if err != nil {
		a.tel.Debug(ctx, "thread permalink failed", "err", err)
	}
	approvalPrompt := consentPrompt(in.Prompt, originJoin, carried, threadLink)

	a.post(ctx, in.ChannelID, in.OriginThreadTS,
		slack.MsgOptionText(joinNotice(in.UserID, template, carried, flags), false))

	a.log.InfoContext(ctx, "session joined a thread",
		"channel", in.ChannelID, "thread_ts", in.OriginThreadTS, "owner", in.UserID,
		"template", template, "roommates", len(roommates),
		"carried_messages", carried, "truncated", flags.truncated, "inflight", flags.inflight)

	a.startSession(ctx, startSpec{
		in:              in,
		template:        template,
		threadTS:        in.OriginThreadTS,
		originKind:      originJoin,
		parentSessionID: parentOf(roommates),
		approvalPrompt:  approvalPrompt,
		agentPrompt:     composeJoinPrompt(entries, flags, in.Prompt),
		threadLink:      threadLink,
		ackTS:           in.MessageTS,
		joinedAt:        in.MessageTS,
		flipFrom:        priorTS(replies, in.MessageTS),
	})
}

func (a *App) roommates(ctx context.Context, channel, threadTS, userID, teamID string) []roommate {
	if held := a.participantsOf(channel, threadTS); len(held) > 0 {
		out := make([]roommate, 0, len(held))
		for _, t := range held {
			if t.owns(userID, teamID) {
				continue
			}
			out = append(out, roommate{live: t})
		}
		return out
	}
	res, err := a.api.ListSessions(ctx, &pb.ListSessionsRequest{LiveOnly: true})
	if err != nil {
		a.log.WarnContext(ctx, "could not list this thread's other sessions; joining without a roster",
			"channel", channel, "thread_ts", threadTS, "err", err)
		return nil
	}
	var out []roommate
	for _, view := range res.GetSessions() {
		m, ok := identityOf(view)
		if !ok || m.ChannelID != channel || m.ThreadTS != threadTS {
			continue
		}
		if m.UserID == userID && m.TeamID == teamID {
			continue
		}
		out = append(out, roommate{view: view})
	}
	return out
}

func (a *App) readCarryover(ctx context.Context, channel, threadTS, beforeTS string, flags *carryoverFlags) ([]slack.Message, []transcriptEntry) {
	replies, err := a.poster.ThreadReplies(ctx, channel, threadTS, "", transcriptFetchMax)
	if err != nil {
		a.log.WarnContext(ctx, "read the thread for a carryover failed; continuing with the request alone",
			"channel", channel, "thread_ts", threadTS, "err", err)
		flags.truncated = true
		return nil, nil
	}
	if n := untaggedChromeLike(replies, a.botUserID); n > 0 {
		a.tel.Debug(ctx, "untagged bot messages look like chrome; check message metadata plumbing",
			"count", n)
	}
	entries, capped := capEntries(sessionTranscript(replies, seedCarry(beforeTS, a.botUserID)))
	flags.truncated = flags.truncated || capped
	return replies, entries
}

func consentPrompt(request, originKind string, carried int, link string) string {
	if strings.TrimSpace(request) == "" {
		request = approvalAsk(originKind)
	}
	if carried > 0 {
		request += provenanceClause(carried, link)
	}
	return request
}

func retemplated(roommates []roommate, template string) *roommate {
	for _, r := range roommates {
		if w := r.workflow(); w != "" && w != template {
			return &r
		}
	}
	return nil
}

func anyBusy(roommates []roommate) bool {
	for _, r := range roommates {
		if r.inflight() {
			return true
		}
	}
	return false
}

func parentOf(roommates []roommate) string {
	parent := ""
	for _, r := range roommates {
		if id := r.sessionID(); parent == "" || id < parent {
			parent = id
		}
	}
	return parent
}

type roommate struct {
	live *thread
	view *pb.SessionView
}

func (r roommate) sessionID() string {
	if r.live != nil {
		return r.live.sessionID
	}
	return r.view.GetId()
}

func (r roommate) workflow() string {
	if r.live != nil {
		return r.live.workflow
	}
	return r.view.GetTemplate()
}

func (r roommate) inflight() bool {
	return r.live != nil && r.live.busy.Load() > 0
}

const transcriptFetchMax = 200
