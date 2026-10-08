package app

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/slackbot/internal/channelmap"
	"github.com/pomerium/agentops/slackbot/internal/gateway"
	"github.com/pomerium/agentops/slackbot/internal/telemetry"
)

const (
	reactionStarting = "hourglass_flowing_sand"
	reactionReady    = "rocket"
	reactionFailed   = "x"
	reactionBusy     = "waiting"
)

const (
	originMention = "mention"
	originJoin    = "join"
)

type ChannelTemplates interface {
	TemplateFor(ctx context.Context, channelID string) (string, error)
}

type Poster interface {
	PostMessage(ctx context.Context, channelID string, opts ...slack.MsgOption) (ts string, err error)
	PostEphemeral(ctx context.Context, channelID, userID string, opts ...slack.MsgOption) (ts string, err error)
	UpdateMessage(ctx context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error)
	DeleteMessage(ctx context.Context, channelID, ts string) error
	Respond(ctx context.Context, responseURL string, replaceOriginal bool, text string, blocks []slack.Block) error
	AddReaction(ctx context.Context, channelID, timestamp, emoji string) error
	RemoveReaction(ctx context.Context, channelID, timestamp, emoji string) error
	UpdateMessageDebounced(ctx context.Context, channelID, ts string, opts ...slack.MsgOption)
	ThreadReplies(ctx context.Context, channelID, threadTS, since string, max int) ([]slack.Message, error)
	Permalink(ctx context.Context, channelID, ts string) (string, error)
}

type Option func(*App)

func WithBotUserID(id string) Option { return func(a *App) { a.botUserID = id } }

func WithHomeTeamID(id string) Option { return func(a *App) { a.homeTeamID = id } }

func WithLogger(l *slog.Logger) Option { return func(a *App) { a.log = l } }

type App struct {
	botUserID  string
	homeTeamID string

	api      harnessapipbconnect.HarnessAPIServiceClient
	poster   Poster
	resolver ChannelTemplates
	log      *slog.Logger
	tel      *telemetry.Component

	mu      sync.Mutex
	threads map[string]*thread
	rooms   map[string]*room
}

var _ gateway.App = (*App)(nil)

func New(api harnessapipbconnect.HarnessAPIServiceClient, p Poster, r ChannelTemplates, opts ...Option) *App {
	a := &App{
		api: api, poster: p, resolver: r, log: slog.Default(),
		threads: map[string]*thread{},
		rooms:   map[string]*room{},
	}
	for _, opt := range opts {
		opt(a)
	}
	a.tel = telemetry.New(a.log, "slackapp", slog.LevelDebug)
	return a
}

type room struct {
	busy    atomic.Int32
	flipped atomic.Bool
	hinted  sync.Map
}

type thread struct {
	sessionID        string
	ownerUserID      string
	teamID           string
	channel          string
	threadTS         string
	room             *room
	workflow         string
	threadLink       string
	approvalWindow   atomic.Int64
	approvalDMEdited atomic.Value

	state         atomic.Pointer[api.SessionState]
	endedFrom     atomic.Pointer[api.SessionState]
	metaMu        sync.Mutex
	metaState     sessionMeta
	statusMu      sync.Mutex
	busy          atomic.Int32
	openingTurn   atomic.Bool
	idleWarn      atomic.Pointer[idleNotice]
	reviveMention atomic.Pointer[gateway.MentionInvocation]
	released      atomic.Bool

	render *renderer
	stop   context.CancelFunc
}

type idleNotice struct {
	channel string
	ts      string
	dm      bool
}

func (t *thread) currentState() api.SessionState {
	if s := t.state.Load(); s != nil {
		return *s
	}
	return api.StatePending
}

func (t *thread) setState(s api.SessionState) { t.state.Store(&s) }

func (t *thread) owns(userID, teamID string) bool {
	return userID == t.ownerUserID && teamID == t.teamID
}

func (t *thread) status() string {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	return t.metaState.StatusMessageTS
}

func (t *thread) multiplayer() bool {
	if t.room != nil && t.room.flipped.Load() {
		return true
	}
	return t.meta().Multiplayer
}

func threadKey(channel, threadTS, teamID, userID string) string {
	return channel + "|" + threadTS + "|" + teamID + "|" + userID
}

func roomKey(channel, threadTS string) string { return channel + "|" + threadTS }

func (a *App) HandleMention(ctx context.Context, in gateway.MentionInvocation) {
	ctx = telemetry.With(ctx, "channel", in.ChannelID, "user", in.UserID)
	ctx, op := a.tel.Start(ctx, "HandleMention")
	defer op.Complete()

	if a.foreignWorkspace(ctx, in.TeamID) {
		return
	}
	if in.OriginThreadTS != "" {
		a.handleThreadedMention(ctx, in)
		return
	}
	if a.lookup(in.ChannelID, in.ThreadTS, in.TeamID, in.UserID) != nil {
		a.tel.Debug(ctx, "mention on the actor's own live thread root; ignoring")
		return
	}
	if a.roomFor(in.ChannelID, in.ThreadTS) != nil {
		in.OriginThreadTS = in.ThreadTS
		a.handleThreadedMention(ctx, in)
		return
	}
	template, ok := a.templateForChannel(ctx, in.ChannelID, in.ThreadTS)
	if !ok {
		return
	}
	a.startSession(ctx, startSpec{
		in:             in,
		template:       template,
		threadTS:       in.ThreadTS,
		originKind:     originMention,
		approvalPrompt: in.Prompt,
		agentPrompt:    in.Prompt,
	})
}

func (a *App) handleThreadedMention(ctx context.Context, in gateway.MentionInvocation) {
	if t := a.lookup(in.ChannelID, in.OriginThreadTS, in.TeamID, in.UserID); t != nil {
		switch t.currentState() {
		case api.StateRunning:
			a.promptThread(ctx, t, in.Text, in.MessageTS, in.MessageTS)
		case api.StateSuspended:
			a.reviveThread(ctx, t, in, in.MessageTS)
		default:
			a.hintInThread(ctx, in.ChannelID, in.OriginThreadTS, in.UserID,
				slack.MsgOptionText(msgHintLaunching, false))
		}
		return
	}

	sess, ok, err := a.sessionForParticipant(ctx, in.ChannelID, in.OriginThreadTS, in.TeamID, in.UserID)
	switch {
	case err != nil:
		a.postLookupFailure(ctx, in.ChannelID, in.OriginThreadTS, err)
	case ok && sess.GetState() == api.StateSuspended:
		a.reviveDetached(ctx, sess, in, in.MessageTS, false)
	case ok && api.Live(sess.GetState()):
		a.hintInThread(ctx, in.ChannelID, in.OriginThreadTS, in.UserID,
			slack.MsgOptionText(msgHintLaunching, false))
	default:
		a.startJoin(ctx, in, false)
	}
}

func (a *App) HandleMessage(ctx context.Context, in gateway.ThreadMessage) {
	ctx = telemetry.With(ctx, "channel", in.ChannelID, "thread_ts", in.ThreadTS, "user", in.UserID)
	ctx, op := a.tel.Start(ctx, "HandleMessage")
	defer op.Complete()

	if a.foreignWorkspace(ctx, in.TeamID) {
		return
	}
	t := a.lookup(in.ChannelID, in.ThreadTS, in.TeamID, in.UserID)
	if t == nil {
		a.hintNoLiveSession(ctx, in)
		return
	}
	if t.multiplayer() {
		a.tel.Debug(ctx, "plain message in a multiplayer thread; carried as discussion",
			"session", t.sessionID)
		return
	}
	switch t.currentState() {
	case api.StateRunning:
		a.promptThread(ctx, t, in.Text, "", in.MessageTS)
	case api.StateSuspended:
		a.reviveThread(ctx, t, gateway.MentionInvocation{
			TeamID: in.TeamID, UserID: in.UserID, ChannelID: in.ChannelID,
			MessageTS: in.MessageTS, ThreadTS: in.ThreadTS, OriginThreadTS: in.ThreadTS,
			Text: in.Text, Prompt: in.Text,
		}, in.MessageTS)
	default:
		a.hintInThread(ctx, in.ChannelID, in.ThreadTS, in.UserID,
			slack.MsgOptionText(msgHintLaunching, false))
	}
}

func (a *App) hintNoLiveSession(ctx context.Context, in gateway.ThreadMessage) {
	if r := a.roomFor(in.ChannelID, in.ThreadTS); r != nil && r.flipped.Load() {
		a.hintRoomEtiquette(ctx, in.ChannelID, in.ThreadTS, in.UserID)
		return
	}
	sess, ok, err := a.sessionForParticipant(ctx, in.ChannelID, in.ThreadTS, in.TeamID, in.UserID)
	switch {
	case err != nil:
		a.log.WarnContext(ctx, "look up thread session failed", "channel", in.ChannelID, "err", err)
		return
	case !ok:
		a.hintRoomEtiquette(ctx, in.ChannelID, in.ThreadTS, in.UserID)
		return
	}
	var m sessionMeta
	if sess.GetState() == api.StateSuspended {
		m, _ = a.loadMeta(ctx, sess)
	}
	switch {
	case sess.GetState() == api.StateSuspended && !m.Multiplayer:
		a.reviveDetached(ctx, sess, gateway.MentionInvocation{
			TeamID: in.TeamID, UserID: in.UserID, ChannelID: in.ChannelID,
			MessageTS: in.MessageTS, ThreadTS: in.ThreadTS, OriginThreadTS: in.ThreadTS,
			Text: in.Text, Prompt: in.Text,
		}, in.MessageTS, true)
	case sess.GetState() == api.StateSuspended:
		a.tel.Debug(ctx, "plain message in a multiplayer thread; not reviving", "session", sess.GetId())
	case api.Live(sess.GetState()):
		a.hintInThread(ctx, in.ChannelID, in.ThreadTS, in.UserID,
			slack.MsgOptionText(msgHintLaunching, false))
	default:
		a.hintInThread(ctx, in.ChannelID, in.ThreadTS, in.UserID,
			slack.MsgOptionText(msgHintEnded, false))
	}
}

func (a *App) hintRoomEtiquette(ctx context.Context, channel, threadTS, userID string) {
	r := a.roomFor(channel, threadTS)
	if r == nil {
		a.tel.Debug(ctx, "reply in a thread with no session; ignoring", "live_threads", a.threadCount())
		return
	}
	if _, already := r.hinted.LoadOrStore(userID, struct{}{}); already {
		return
	}
	a.hintInThread(ctx, channel, threadTS, userID, slack.MsgOptionText(msgHintDiscussion, false))
}

func (a *App) HandleInteraction(ctx context.Context, in gateway.Interaction) {
	if a.foreignWorkspace(ctx, in.TeamID) {
		return
	}
	switch in.ActionID {
	case gateway.ActionPermission:
		a.handlePermissionClick(ctx, in)
	}
}

func (a *App) handlePermissionClick(ctx context.Context, in gateway.Interaction) {
	sessionID, requestID, optionID, ok := gateway.DecodePermissionValue(in.Value)
	if !ok {
		a.log.WarnContext(ctx, "unreadable permission button value; ignoring the click",
			"action", in.ActionID, "user", in.UserID)
		return
	}
	t := a.threadForSession(in.ChannelID, in.ThreadTS, sessionID)
	if t == nil {
		a.respondEphemeral(ctx, in, msgHintPermissionStale)
		return
	}
	if !t.owns(in.UserID, in.TeamID) {
		a.log.InfoContext(ctx, "ignoring permission interaction from a non-owner",
			"session", t.sessionID, "owner", t.ownerUserID, "actor", in.UserID)
		a.respondEphemeral(ctx, in, hintPermissionNotYours(t.ownerUserID))
		return
	}
	_, err := a.api.RespondPermission(ctx, &pb.RespondPermissionRequest{
		Ref:       a.ref(t.sessionID),
		RequestId: requestID,
		OptionId:  optionID,
	})
	if errors.Is(err, api.ErrUnknownRequest) {
		a.respondEphemeral(ctx, in, msgHintPermissionStale)
		return
	}
	if err != nil {
		a.log.WarnContext(ctx, "relaying a permission decision failed",
			"session", t.sessionID, "request", requestID, "err", err)
	}
}

func (a *App) promptThread(ctx context.Context, t *thread, text, triggerTS, messageTS string) {
	if err := a.sendTurn(ctx, t, text, triggerTS, messageTS); err != nil {
		a.log.ErrorContext(ctx, "prompt failed", "session", t.sessionID, "err", err)
		a.tellOwner(ctx, t, msgTurnRejected)
	}
}

func (a *App) sendTurn(ctx context.Context, t *thread, text, triggerTS, messageTS string) error {
	a.clearIdleWarning(ctx, t, msgIdleKeptAlive)
	block, read := a.catchup(ctx, t, triggerTS)
	content := withCatchup(block, text)
	a.markBusy(ctx, t)
	if _, err := a.api.Prompt(ctx, &pb.PromptRequest{
		Ref: a.ref(t.sessionID), Content: content, IdempotencyKey: promptKey(t.channel, messageTS),
	}); err != nil {
		a.clearBusy(ctx, t)
		return err
	}
	if read {
		a.advanceCursor(ctx, t, triggerTS)
	}
	return nil
}

func (a *App) catchup(ctx context.Context, t *thread, beforeTS string) (string, bool) {
	if !t.multiplayer() {
		return "", true
	}
	cursor := t.meta().LastSeenTS
	if cursor == "" {
		a.tel.Debug(ctx, "no catch-up cursor yet; this turn carries no delta", "session", t.sessionID)
		return "", true
	}
	replies, err := a.poster.ThreadReplies(ctx, t.channel, t.threadTS, "", transcriptFetchMax)
	if err != nil {
		a.log.WarnContext(ctx, "read the whole thread for a catch-up failed; reading only what came after the cursor",
			"session", t.sessionID, "err", err)
		replies, err = a.poster.ThreadReplies(ctx, t.channel, t.threadTS, cursor, transcriptFetchMax)
	}
	if err != nil {
		a.log.WarnContext(ctx, "read the thread for a catch-up failed; the turn goes without one and the next turn carries it",
			"session", t.sessionID, "err", err)
		return "", false
	}
	entries, truncated := capEntries(transcriptMessages(replies,
		catchupCarry(cursor, beforeTS, a.botUserID, t.sessionID, t.ownerUserID)))
	block := composeCatchupBlock(entries, truncated)
	if block != "" {
		a.tel.Debug(ctx, "catching a session up with the thread",
			"session", t.sessionID, "entries", len(entries), "truncated", truncated)
	}
	return block, true
}

func (a *App) advanceCursor(ctx context.Context, t *thread, triggerTS string) {
	if triggerTS == "" || !t.multiplayer() {
		return
	}
	if t.meta().LastSeenTS >= triggerTS {
		return
	}
	a.saveMeta(ctx, t, func(m *sessionMeta) { m.LastSeenTS = triggerTS })
}

func (a *App) markBusy(ctx context.Context, t *thread) {
	t.busy.Add(1)
	if t.room != nil && t.room.busy.Add(1) == 1 {
		a.addReaction(ctx, t.channel, t.threadTS, reactionBusy)
	}
}

func (a *App) clearBusy(ctx context.Context, t *thread) {
	if t.busy.Add(-1) < 0 {
		t.busy.Store(0)
		return
	}
	if t.room == nil {
		return
	}
	if t.room.busy.Add(-1) <= 0 {
		t.room.busy.Store(0)
		a.removeReaction(ctx, t.channel, t.threadTS, reactionBusy)
	}
}

func (a *App) drainBusy(ctx context.Context, t *thread) {
	for t.busy.Load() > 0 {
		a.clearBusy(ctx, t)
	}
}

func (a *App) reviveThread(ctx context.Context, t *thread, in gateway.MentionInvocation, messageTS string) {
	t.reviveMention.Store(&in)
	prompt := in.Prompt
	if prompt == "" {
		prompt = in.Text
	}
	err := a.sendTurn(ctx, t, prompt, in.MessageTS, messageTS)
	switch {
	case errors.Is(err, api.ErrNotRevivable):
		a.log.InfoContext(ctx, "this conversation cannot be continued; starting over in the same thread",
			"session", t.sessionID, "err", err)
		a.startOver(ctx, t, in)
		return
	case err != nil:
		a.log.ErrorContext(ctx, "continuing a paused thread failed", "session", t.sessionID, "err", err)
		a.tellOwner(ctx, t, msgTurnRejected)
		return
	}
	a.restateStatus(ctx, t, msgStatusContinuing)
	a.addReaction(ctx, in.ChannelID, reactTS(in), reactionStarting)
}

func (a *App) reviveDetached(ctx context.Context, sess *pb.SessionView, in gateway.MentionInvocation, messageTS string, solo bool) {
	t, err := a.adopt(ctx, sess, in.MessageTS)
	switch {
	case errors.Is(err, errBindingClaimed):
		a.hintInThread(ctx, in.ChannelID, in.OriginThreadTS, in.UserID,
			slack.MsgOptionText(msgHintTryAgain, false))
		return
	case errors.Is(err, errNoSlackState):
		a.log.WarnContext(ctx, "a paused session has no Slack state; starting over",
			"session", sess.GetId())
		if _, err := a.api.EndSession(ctx, &pb.EndSessionRequest{Ref: a.ref(sess.GetId())}); err != nil {
			a.log.ErrorContext(ctx, "could not end a session with no Slack state; not starting over",
				"session", sess.GetId(), "err", err)
			a.post(ctx, in.ChannelID, in.OriginThreadTS, slack.MsgOptionText(msgTurnRejected, false))
			return
		}
		a.post(ctx, in.ChannelID, in.OriginThreadTS, slack.MsgOptionText(msgStatusCannotContinue, false))
		a.startJoin(ctx, in, solo)
		return
	case err != nil:
		a.log.WarnContext(ctx, "could not adopt a paused thread; not continuing it",
			"session", sess.GetId(), "err", err)
		a.post(ctx, in.ChannelID, in.OriginThreadTS, slack.MsgOptionText(msgTurnRejected, false))
		return
	}
	a.reviveThread(ctx, t, in, messageTS)
}

func promptKey(channel, messageTS string) string {
	if messageTS == "" {
		return ""
	}
	return channel + "/" + messageTS
}

func (a *App) ref(sessionID string) *pb.SessionRef {
	return &pb.SessionRef{SessionId: sessionID}
}

func conversationRef(channelID, threadTS, teamID, userID string) string {
	return "slack:" + channelID + ":" + threadTS + ":" + teamID + ":" + userID
}

func (a *App) sessionForParticipant(ctx context.Context, channelID, threadTS, teamID, userID string) (*pb.SessionView, bool, error) {
	res, err := a.api.GetSession(ctx, &pb.GetSessionRequest{Ref: &pb.SessionRef{
		ConversationRef: conversationRef(channelID, threadTS, teamID, userID),
		IncludeTerminal: true,
	}})
	switch {
	case err == nil:
		return res.GetSession(), true, nil
	case errors.Is(err, api.ErrNotFound):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

func (a *App) foreignWorkspace(ctx context.Context, teamID string) bool {
	if a.homeTeamID == "" || teamID == "" || teamID == a.homeTeamID {
		return false
	}
	a.log.InfoContext(ctx, "ignoring event from another workspace",
		"actor_team", teamID, "home_team", a.homeTeamID)
	return true
}

func (a *App) templateForChannel(ctx context.Context, channelID, replyThreadTS string) (string, bool) {
	name, err := a.resolver.TemplateFor(ctx, channelID)
	switch {
	case errors.Is(err, channelmap.ErrChannelNotBound):
		a.log.InfoContext(ctx, "mention in a channel with no agent binding", "channel", channelID)
		a.post(ctx, channelID, replyThreadTS, slack.MsgOptionText(msgNoAgentForChannel, false))
		return "", false
	case err != nil:
		a.postLookupFailure(ctx, channelID, replyThreadTS, err)
		return "", false
	}
	return name, true
}

func (a *App) postLookupFailure(ctx context.Context, channelID, replyThreadTS string, err error) {
	a.log.ErrorContext(ctx, "resolve channel agent template failed", "channel", channelID, "err", err)
	a.post(ctx, channelID, replyThreadTS, slack.MsgOptionText(msgLookupFailed, false))
}

func (a *App) hintInThread(ctx context.Context, channel, threadTS, userID string, opts ...slack.MsgOption) {
	tagged := append([]slack.MsgOption{chromeMeta("")}, opts...)
	if _, err := a.poster.PostEphemeral(ctx, channel, userID,
		append(tagged, slack.MsgOptionTS(threadTS))...); err == nil {
		return
	}
	if _, err := a.poster.PostEphemeral(ctx, channel, userID, tagged...); err != nil {
		a.log.WarnContext(ctx, "post hint failed", "channel", channel, "user", userID, "err", err)
	}
}

func (a *App) tellOwner(ctx context.Context, t *thread, text string) {
	if !t.multiplayer() {
		a.post(ctx, t.channel, t.threadTS, slack.MsgOptionText(text, false))
		return
	}
	a.hintInThread(ctx, t.channel, t.threadTS, t.ownerUserID, slack.MsgOptionText(text, false))
}

func (a *App) respondEphemeral(ctx context.Context, in gateway.Interaction, text string) {
	if in.ResponseURL == "" {
		a.tel.Debug(ctx, "interaction carried no response_url; not answering", "action", in.ActionID)
		return
	}
	if err := a.poster.Respond(ctx, in.ResponseURL, false, text, nil); err != nil {
		a.log.WarnContext(ctx, "respond to interaction failed", "action", in.ActionID, "err", err)
	}
}

func (a *App) post(ctx context.Context, channel, threadTS string, opts ...slack.MsgOption) {
	opts = append([]slack.MsgOption{chromeMeta("")}, opts...)
	if threadTS != "" {
		opts = append(opts, slack.MsgOptionTS(threadTS))
	}
	if _, err := a.poster.PostMessage(ctx, channel, opts...); err != nil {
		a.log.ErrorContext(ctx, "post message failed", "channel", channel, "thread_ts", threadTS, "err", err)
	}
}

func (a *App) addReaction(ctx context.Context, channel, ts, emoji string) {
	if ts == "" {
		return
	}
	err := a.poster.AddReaction(ctx, channel, ts, emoji)
	if err == nil || err.Error() == "already_reacted" {
		return
	}
	a.tel.Warn(ctx, "add reaction failed; grant the bot the reactions:write scope to show launch progress",
		"emoji", emoji, "err", err)
}

func (a *App) removeReaction(ctx context.Context, channel, ts, emoji string) {
	if ts == "" {
		return
	}
	if err := a.poster.RemoveReaction(ctx, channel, ts, emoji); err != nil {
		a.tel.Debug(ctx, "remove reaction failed", "emoji", emoji, "err", err)
	}
}

func (a *App) swapReaction(ctx context.Context, channel, ts, from, to string) {
	if ts == "" {
		return
	}
	a.removeReaction(ctx, channel, ts, from)
	a.addReaction(ctx, channel, ts, to)
}

func (a *App) clearIdleWarning(ctx context.Context, t *thread, outcome string) {
	n := t.idleWarn.Swap(nil)
	if n == nil {
		return
	}
	if n.dm {
		if _, err := a.poster.UpdateMessage(ctx, n.channel, n.ts,
			chromeMeta(t.sessionID), slack.MsgOptionText(outcome, false)); err != nil {
			a.log.WarnContext(ctx, "could not withdraw the idle warning DM",
				"session", t.sessionID, "ts", n.ts, "err", err)
		}
		return
	}
	if err := a.poster.DeleteMessage(ctx, n.channel, n.ts); err != nil {
		a.log.WarnContext(ctx, "could not withdraw the idle warning",
			"session", t.sessionID, "ts", n.ts, "err", err)
	}
}

func linkOr(url, label string) string {
	if url == "" {
		return label
	}
	return "<" + url + "|" + label + ">"
}

func reactTS(in gateway.MentionInvocation) string {
	return cmp.Or(in.MessageTS, in.ThreadTS)
}

func (a *App) lookup(channel, threadTS, teamID, userID string) *thread {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.threads[threadKey(channel, threadTS, teamID, userID)]
}

func (a *App) threadCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.threads)
}

func (a *App) participantsOf(channel, threadTS string) []*thread {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.participantsLocked(channel, threadTS, nil)
}

func (a *App) participantsLocked(channel, threadTS string, except *thread) []*thread {
	var out []*thread
	for _, t := range a.threads {
		if t.channel == channel && t.threadTS == threadTS && t != except {
			out = append(out, t)
		}
	}
	return out
}

func (a *App) anyParticipantLocked(channel, threadTS string) bool {
	for _, t := range a.threads {
		if t.channel == channel && t.threadTS == threadTS {
			return true
		}
	}
	return false
}

func (a *App) roomFor(channel, threadTS string) *room {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.rooms[roomKey(channel, threadTS)]
}

func (a *App) registerThread(t *thread) (ok bool, others []*thread) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := threadKey(t.channel, t.threadTS, t.teamID, t.ownerUserID)
	if _, exists := a.threads[key]; exists {
		return false, nil
	}
	r := a.rooms[roomKey(t.channel, t.threadTS)]
	if r == nil {
		r = &room{}
		a.rooms[roomKey(t.channel, t.threadTS)] = r
	}
	if t.meta().Multiplayer {
		r.flipped.Store(true)
	}
	a.threads[key] = t
	t.room = r
	return true, a.participantsLocked(t.channel, t.threadTS, t)
}

func (a *App) unregisterThread(t *thread) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := threadKey(t.channel, t.threadTS, t.teamID, t.ownerUserID)
	if a.threads[key] == t {
		delete(a.threads, key)
	}
	if !a.anyParticipantLocked(t.channel, t.threadTS) {
		delete(a.rooms, roomKey(t.channel, t.threadTS))
	}
}

func (a *App) threadForSession(channel, threadTS, sessionID string) *thread {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, t := range a.threads {
		if t.channel == channel && t.threadTS == threadTS && t.sessionID == sessionID {
			return t
		}
	}
	return nil
}

func (a *App) flipRoom(ctx context.Context, t *thread, others []*thread, atTS string) {
	if len(others) == 0 || t.room == nil {
		return
	}
	if t.room.flipped.CompareAndSwap(false, true) {
		a.log.InfoContext(ctx, "thread is now multiplayer",
			"channel", t.channel, "thread_ts", t.threadTS, "participants", len(others)+1)
		a.post(ctx, t.channel, t.threadTS, slack.MsgOptionText(msgRoomMultiplayer, false))
	}
	for _, o := range append(others, t) {
		if o.meta().Multiplayer {
			continue
		}
		a.saveMeta(ctx, o, func(m *sessionMeta) {
			m.Multiplayer = true
			if m.LastSeenTS == "" {
				m.LastSeenTS = atTS
			}
		})
	}
}

func (a *App) Shutdown() {
	a.mu.Lock()
	threads := make([]*thread, 0, len(a.threads))
	for _, t := range a.threads {
		threads = append(threads, t)
	}
	a.threads = map[string]*thread{}
	a.mu.Unlock()
	for _, t := range threads {
		if t.stop != nil {
			t.stop()
		}
	}
}
