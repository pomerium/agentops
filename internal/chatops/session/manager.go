// Package session is the application brain. It implements gateway.App: it
// resolves an @mentioned agent template name to its AgentTemplate, gates execution
// on the invoking user being connected to every required MCP server (driving
// OAuth flows when not), launches the sandbox once connected, and bridges each
// Slack thread to a live multi-turn ACP session.
package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/slack-go/slack"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/agenttemplate"
	"github.com/pomerium/agentops/internal/channels/slack/gateway"
	"github.com/pomerium/agentops/internal/chatops/store"
	"github.com/pomerium/agentops/internal/mcpbroker"
	"github.com/pomerium/agentops/internal/sandbox"
	"github.com/pomerium/agentops/internal/telemetry"
)

// Broker is the subset of the MCP credential broker the manager depends on.
type Broker interface {
	ConnectionStatus(ctx context.Context, slackUserID, serverName, serverURL string) (bool, error)
	StartAuthFlow(ctx context.Context, req mcpbroker.StartAuthRequest) (string, error)
	AccessToken(ctx context.Context, slackUserID, serverName, serverURL string) (string, error)
	HandleCallback(ctx context.Context, code, state string) (mcpbroker.CallbackResult, error)
	AuthorizeURL(ctx context.Context, flowID string) (string, error)
}

// TemplateResolver resolves an agent template name to its AgentTemplate and
// lists the available templates (for the help shown on an unknown name).
type TemplateResolver interface {
	Resolve(ctx context.Context, name string) (*v1alpha1.AgentTemplate, error)
	List(ctx context.Context) ([]v1alpha1.AgentTemplate, error)
}

// LiveSession is a running multi-turn ACP session.
type LiveSession interface {
	ID() string
	Prompt(ctx context.Context, text string) (acp.StopReason, error)
	Cancel(ctx context.Context) error
	Close() error
}

// Launcher creates and tears down sandboxes, returning a live ACP session.
// The proxied endpoints carry the per-user MCP credentials destined for the
// sandbox sidecar; they never reach the agent container.
type Launcher interface {
	Launch(ctx context.Context, sink sandbox.EventSink, spec sandbox.LaunchSpec, endpoints []sandbox.ProxiedEndpoint) (LiveSession, sandbox.LaunchResult, error)
	Teardown(ctx context.Context, claimName string) error
}

// Poster posts and updates Slack messages.
type Poster interface {
	PostMessage(ctx context.Context, channelID string, opts ...slack.MsgOption) (ts string, err error)
	PostEphemeral(ctx context.Context, channelID, userID string, opts ...slack.MsgOption) (ts string, err error)
	UpdateMessage(ctx context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error)
	// Respond posts or (with replaceOriginal) edits an ephemeral message via a
	// Slack response_url — the only way to update an ephemeral message.
	Respond(ctx context.Context, responseURL string, replaceOriginal bool, text string, blocks []slack.Block) error
	// AddReaction/RemoveReaction manage an emoji reaction (named without colons)
	// on a message, used to signal sandbox-launch progress on the root message.
	AddReaction(ctx context.Context, channelID, timestamp, emoji string) error
	RemoveReaction(ctx context.Context, channelID, timestamp, emoji string) error
	// UpdateMessageDebounced schedules a low-priority, coalescable update to an
	// existing message: pending updates for the same message are replaced
	// (last-one-wins) and may be dropped entirely if a guaranteed UpdateMessage
	// for the message follows. Used for mid-turn streaming updates only — never
	// for content that must be delivered.
	UpdateMessageDebounced(ctx context.Context, channelID, ts string, opts ...slack.MsgOption)
	// ThreadReplies returns up to max messages of a thread (root included),
	// oldest first.
	ThreadReplies(ctx context.Context, channelID, threadTS string, max int) ([]slack.Message, error)
	// Permalink returns the canonical permalink for a message.
	Permalink(ctx context.Context, channelID, ts string) (string, error)
}

// Lifecycle reaction emojis placed on the @mention message while the sandbox
// starts, then swapped to a terminal marker. reactionBusy additionally marks
// the root message while a turn is running (a custom :waiting: emoji in the
// workspace; adding it is best-effort like the rest).
const (
	reactionStarting = "hourglass_flowing_sand"
	reactionReady    = "rocket"
	reactionFailed   = "x"
	reactionBusy     = "waiting"
)

// Config configures the Manager.
type Config struct {
	Namespace         string
	SessionTTL        time.Duration
	PermissionTimeout time.Duration
}

// Manager implements gateway.App.
type Manager struct {
	cfg      Config
	store    *store.Store
	broker   Broker
	resolver TemplateResolver
	launcher Launcher
	poster   Poster
	log      *slog.Logger
	tel      *telemetry.Component

	mu        sync.Mutex
	live      map[string]*binding // key: threadKey(channel, threadTS)
	launching map[string]struct{} // threads with an in-flight launch
}

type binding struct {
	sessionID   string
	ownerUserID string
	claimName   string
	session     LiveSession
	sink        *threadSink
	// originThreadTS, for a loop-in session, is the foreign thread the bot was
	// summoned from; the first answer's permalink is cross-posted there. Empty
	// for sessions started by a top-level mention.
	originThreadTS string
	// firstAnswerPosted guards the one-time answer cross-post.
	firstAnswerPosted atomic.Bool
	// busy counts in-flight turns so the busy reaction is added on the first
	// and removed only when the last one finishes (turns can overlap when the
	// user sends several replies quickly).
	busy atomic.Int32
}

var _ gateway.App = (*Manager)(nil)

// NewManager constructs a Manager.
func NewManager(cfg Config, s *store.Store, b Broker, r TemplateResolver, l Launcher, p Poster, log *slog.Logger) *Manager {
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = time.Hour
	}
	if cfg.PermissionTimeout == 0 {
		cfg.PermissionTimeout = 5 * time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		cfg: cfg, store: s, broker: b, resolver: r, launcher: l, poster: p, log: log,
		tel:       telemetry.New(log, "session", slog.LevelDebug),
		live:      map[string]*binding{},
		launching: map[string]struct{}{},
	}
}

func threadKey(channel, threadTS string) string { return channel + "|" + threadTS }

// HandleMention starts a session from an @mention of the bot. The first word
// after the mention is the agent template name; the rest is the initial
// prompt. A top-level mention is the thread root the bot replies under; a
// mention inside a foreign thread loops the bot into that discussion (a new
// session thread seeded with it); a mention inside a thread the bot already
// drives is just another turn.
func (m *Manager) HandleMention(ctx context.Context, in gateway.MentionInvocation) {
	ctx = telemetry.With(ctx, "channel", in.ChannelID, "user", in.UserID, "template", in.Template)
	ctx, op := m.tel.Start(ctx, "HandleMention")
	defer op.Complete()

	if in.OriginThreadTS != "" {
		// The gateway routes every threaded mention here (never to the
		// message handler), so a mention in a live bot thread must run the
		// turn itself — returning would swallow it.
		if b := m.lookup(in.ChannelID, in.OriginThreadTS); b != nil {
			if in.UserID != b.ownerUserID {
				m.log.InfoContext(ctx, "ignoring thread mention from non-owner",
					"session", b.sessionID, "owner", b.ownerUserID, "actor", in.UserID)
				return
			}
			m.runTurn(ctx, b, in.Text)
			return
		}
		m.startLoopIn(ctx, in)
		return
	}

	// A repeat mention on a live thread root must not start a second session.
	if m.lookup(in.ChannelID, in.ThreadTS) != nil {
		m.tel.Debug(ctx, "mention on a live thread root; ignoring")
		return
	}

	tmpl, ok := m.resolveOrHelp(ctx, in.Template, in.ChannelID, in.ThreadTS)
	if !ok {
		return
	}

	sessionID := newID()
	if err := m.store.CreateSession(ctx, store.Session{
		ID:             sessionID,
		SlackChannelID: in.ChannelID,
		SlackThreadTS:  in.ThreadTS,
		SlackUserID:    in.UserID,
		WorkflowName:   tmpl.Name,
		InitialPrompt:  in.Args,
		Status:         "pending",
	}); err != nil {
		m.log.ErrorContext(ctx, "create session failed", "err", err)
		return
	}

	m.connectOrLaunch(ctx, launchContext{
		sessionID: sessionID,
		userID:    in.UserID,
		teamID:    in.TeamID,
		channelID: in.ChannelID,
		threadTS:  in.ThreadTS,
		template:  tmpl,
		args:      in.Args,
	})
}

// resolveOrHelp resolves a template name, posting the help text (unknown
// name) or an infrastructure warning (anything else) into the given thread
// when it fails.
func (m *Manager) resolveOrHelp(ctx context.Context, template, channelID, threadTS string) (*v1alpha1.AgentTemplate, bool) {
	tmpl, err := m.resolver.Resolve(ctx, template)
	if err == nil {
		return tmpl, true
	}
	// Only a genuine "no such template" gets the help text. Anything else
	// is an infrastructure failure (missing CRD, RBAC, API-server trouble)
	// that would otherwise masquerade as a typo — say so and log it.
	if !errors.Is(err, agenttemplate.ErrNotFound) {
		m.log.ErrorContext(ctx, "resolve agent template failed", "template", template, "err", err)
		m.post(ctx, channelID, threadTS, slack.MsgOptionText(
			fmt.Sprintf(":warning: I couldn't look up agent `%s` — something is broken on my side, not a typo. Ask an admin to check the app logs.", template), false))
		return nil, false
	}
	m.post(ctx, channelID, threadTS, slack.MsgOptionText(m.unknownTemplateText(ctx, template), false))
	return nil, false
}

// startLoopIn starts a session from a mention inside a foreign thread: the
// origin discussion (prior to the mention) seeds the first prompt, the session
// runs in a new thread rooted at a fresh bot message, and the two threads are
// cross-linked. Transcript and permalink failures degrade gracefully — only a
// failure to post the new root aborts, because without it there is no thread
// for the session to run in.
func (m *Manager) startLoopIn(ctx context.Context, in gateway.MentionInvocation) {
	ctx = telemetry.With(ctx, "origin_thread_ts", in.OriginThreadTS)
	ctx, op := m.tel.Start(ctx, "startLoopIn")
	defer op.Complete()

	tmpl, ok := m.resolveOrHelp(ctx, in.Template, in.ChannelID, in.OriginThreadTS)
	if !ok {
		return
	}

	var texts []string
	replies, err := m.poster.ThreadReplies(ctx, in.ChannelID, in.OriginThreadTS, transcriptFetchMax)
	if err != nil {
		m.log.WarnContext(ctx, "fetch origin thread failed; continuing without transcript",
			"channel", in.ChannelID, "err", err)
	} else {
		texts = transcriptTexts(replies, in.MessageTS)
	}
	prompt := composeLoopInPrompt(texts, in.Args)

	originLink, err := m.poster.Permalink(ctx, in.ChannelID, in.MessageTS)
	if err != nil {
		m.tel.Debug(ctx, "origin permalink failed", "err", err)
	}
	rootText := fmt.Sprintf(":thread: <@%s> looped me in from %s — running *%s*. I'll work in this thread.",
		in.UserID, linkOr(originLink, "another discussion"), tmpl.Name)
	rootTS, err := m.poster.PostMessage(ctx, in.ChannelID, slack.MsgOptionText(rootText, false))
	if err != nil {
		m.log.ErrorContext(ctx, "post loop-in root failed", "channel", in.ChannelID, "err", err)
		m.post(ctx, in.ChannelID, in.OriginThreadTS,
			slack.MsgOptionText(":x: I couldn't start a session thread for this discussion.", false))
		return
	}

	rootLink, err := m.poster.Permalink(ctx, in.ChannelID, rootTS)
	if err != nil {
		m.tel.Debug(ctx, "root permalink failed", "err", err)
	}
	m.post(ctx, in.ChannelID, in.OriginThreadTS,
		slack.MsgOptionText(":robot_face: On it — follow along in "+linkOr(rootLink, "the thread I just started")+".", false))

	sessionID := newID()
	// The composed prompt (not the bare question) is persisted so a launch
	// resumed from an OAuth detour keeps the transcript.
	if err := m.store.CreateSession(ctx, store.Session{
		ID:             sessionID,
		SlackChannelID: in.ChannelID,
		SlackThreadTS:  rootTS,
		SlackUserID:    in.UserID,
		WorkflowName:   tmpl.Name,
		InitialPrompt:  prompt,
		Status:         "pending",
	}); err != nil {
		m.log.ErrorContext(ctx, "create session failed", "err", err)
		return
	}

	m.connectOrLaunch(ctx, launchContext{
		sessionID:      sessionID,
		userID:         in.UserID,
		teamID:         in.TeamID,
		channelID:      in.ChannelID,
		threadTS:       rootTS,
		template:       tmpl,
		args:           prompt,
		ackTS:          in.MessageTS,
		originThreadTS: in.OriginThreadTS,
	})
}

// transcriptFetchMax bounds how many origin-thread messages are fetched; the
// transcript itself is capped tighter (see capTranscript).
const transcriptFetchMax = 200

// linkOr renders a Slack mrkdwn link with the given label, or just the label
// when the permalink could not be resolved.
func linkOr(url, label string) string {
	if url == "" {
		return label
	}
	return "<" + url + "|" + label + ">"
}

// unknownTemplateText is the help shown when a mention names a template that
// doesn't resolve (or names none), listing the available templates.
func (m *Manager) unknownTemplateText(ctx context.Context, template string) string {
	available := m.availableTemplates(ctx)
	switch {
	case template == "" && available == "":
		return ":wave: Mention me with an agent name to start, e.g. `@me <agent> <prompt>`."
	case template == "":
		return ":wave: Mention me with an agent name to start. Available: " + available
	case available == "":
		return fmt.Sprintf(":warning: No agent named `%s` exists.", template)
	default:
		return fmt.Sprintf(":warning: No agent named `%s`. Available: %s", template, available)
	}
}

// availableTemplates renders the registered template names as a comma-separated
// list of code spans, or "" when none are registered or the list fails.
func (m *Manager) availableTemplates(ctx context.Context) string {
	list, err := m.resolver.List(ctx)
	if err != nil {
		m.log.ErrorContext(ctx, "list agent templates failed", "err", err)
		return ""
	}
	if len(list) == 0 {
		return ""
	}
	names := make([]string, 0, len(list))
	for i := range list {
		names = append(names, "`"+list[i].Name+"`")
	}
	return strings.Join(names, ", ")
}

type launchContext struct {
	sessionID string
	userID    string
	teamID    string
	channelID string
	threadTS  string
	template  *v1alpha1.AgentTemplate
	args      string
	// responseURL is a Slack response_url used to post and edit the ephemeral
	// auth prompt in place. Unused by the @mention entry point (mentions carry no
	// response_url), so it is empty and the prompt falls back to a plain ephemeral
	// message; retained for any future entry point that does provide one.
	responseURL string
	// replacePrompt is true when this run should edit an existing auth prompt
	// (i.e. resumed from an OAuth callback) rather than post a fresh one.
	replacePrompt bool
	// ackTS is the message lifecycle reactions (starting/ready/failed) land
	// on. Empty means the thread root (a top-level mention is its own root);
	// a loop-in sets it to the origin mention message, where the user is
	// looking.
	ackTS string
	// originThreadTS, for a loop-in, is the foreign thread to cross-post the
	// first answer's permalink into. Empty otherwise (including launches
	// resumed from an OAuth detour, which lose the origin — acceptable).
	originThreadTS string
}

// reactTS returns the message lifecycle reactions land on.
func (lc launchContext) reactTS() string {
	if lc.ackTS != "" {
		return lc.ackTS
	}
	return lc.threadTS
}

// connectOrLaunch checks every required server; if any is unconnected it posts
// auth links, otherwise it launches the sandbox and opens the ACP session.
func (m *Manager) connectOrLaunch(ctx context.Context, lc launchContext) {
	ctx = telemetry.With(ctx, "session_id", lc.sessionID, "thread_ts", lc.threadTS)
	ctx, op := m.tel.Start(ctx, "connectOrLaunch")
	defer op.Complete()

	// Already launched for this thread? Don't double-launch.
	if m.lookup(lc.channelID, lc.threadTS) != nil {
		return
	}

	// Acknowledge on the @mention message right away (before the connection
	// probes and the sandbox cold-start) so the user sees the bot is working.
	// launch() swaps this to a ready/failed marker; it lingers through the
	// connect step while the user authorizes.
	m.addReaction(ctx, lc.channelID, lc.reactTS(), reactionStarting)

	var servers []gateway.AuthStatus
	anyMissing := false
	for _, srv := range lc.template.Spec.RequiredMCPServers {
		connected, err := m.broker.ConnectionStatus(ctx, lc.userID, srv.Name, srv.URL)
		if err != nil {
			m.log.ErrorContext(ctx, "connection status check failed", "server", srv.Name, "err", err)
		}
		m.tel.Debug(ctx, "mcp server connection check", "server", srv.Name, "connected", connected)
		if connected {
			servers = append(servers, gateway.AuthStatus{ServerName: srv.Name, Connected: true})
			continue
		}
		connectURL, err := m.broker.StartAuthFlow(ctx, mcpbroker.StartAuthRequest{
			SlackUserID:    lc.userID,
			SlackTeamID:    lc.teamID,
			SlackChannelID: lc.channelID,
			SlackThreadTS:  lc.threadTS,
			ServerName:     srv.Name,
			ServerURL:      srv.URL,
			WorkflowName:   lc.template.Name,
		})
		if err != nil {
			m.log.ErrorContext(ctx, "start auth flow failed", "server", srv.Name, "err", err)
			m.swapReaction(ctx, lc.channelID, lc.reactTS(), reactionStarting, reactionFailed)
			m.postEphemeral(ctx, lc.channelID, lc.userID,
				slack.MsgOptionText(fmt.Sprintf(":x: Could not start authorization for *%s*.", srv.Name), false))
			return
		}
		anyMissing = true
		servers = append(servers, gateway.AuthStatus{ServerName: srv.Name, URL: connectURL})
	}

	if anyMissing {
		m.setStatus(ctx, lc.sessionID, "awaiting_auth")
		m.postAuthPrompt(ctx, lc, servers)
		return
	}

	// Everything is connected. If we were editing an existing prompt (resumed
	// from an OAuth callback), replace it with a closing note so its now-stale
	// buttons disappear; the public "Connected and ready" lands in the thread.
	if lc.replacePrompt && lc.responseURL != "" {
		_ = m.poster.Respond(ctx, lc.responseURL, true,
			fmt.Sprintf(":white_check_mark: All tools connected — launching *%s*…", lc.template.Name), nil)
	}
	m.launch(ctx, lc)
}

// postAuthPrompt renders the per-server connect prompt and delivers it as an
// ephemeral message. When a slash-command response_url is available it posts (or
// edits, when replacePrompt is set) via that URL so the prompt can be updated in
// place as servers connect; otherwise it falls back to a fresh ephemeral message.
func (m *Manager) postAuthPrompt(ctx context.Context, lc launchContext, servers []gateway.AuthStatus) {
	blocks := gateway.AuthPromptBlocks(lc.template.Name, servers)
	text := authFallbackText(lc.template.Name, servers)
	if lc.responseURL != "" {
		err := m.poster.Respond(ctx, lc.responseURL, lc.replacePrompt, text, blocks)
		if err == nil {
			return
		}
		m.log.ErrorContext(ctx, "respond via response_url failed; falling back to ephemeral",
			"channel", lc.channelID, "user", lc.userID, "err", err)
	}
	m.postEphemeral(ctx, lc.channelID, lc.userID,
		slack.MsgOptionText(text, false), slack.MsgOptionBlocks(blocks...))
}

func (m *Manager) launch(ctx context.Context, lc launchContext) {
	ctx, op := m.tel.Start(ctx, "launch")
	defer op.Complete()

	// Atomically reserve this thread so two concurrent triggers (e.g. a slash
	// command racing an OAuth callback) can't both launch a sandbox.
	if !m.reserve(lc.channelID, lc.threadTS) {
		m.tel.Debug(ctx, "launch already in progress for thread; skipping")
		return
	}
	launched := false
	defer func() {
		if !launched {
			m.releaseReservation(lc.channelID, lc.threadTS)
		}
	}()

	endpoints, err := m.buildProxiedEndpoints(ctx, lc)
	if err != nil {
		m.log.ErrorContext(ctx, "build mcp servers failed", "err", err)
		m.swapReaction(ctx, lc.channelID, lc.reactTS(), reactionStarting, reactionFailed)
		m.post(ctx, lc.channelID, lc.threadTS, slack.MsgOptionText(":x: Failed to assemble MCP credentials.", false))
		m.setStatus(ctx, lc.sessionID, "failed")
		return
	}

	sink := newThreadSink(m.poster, lc.sessionID, lc.channelID, lc.threadTS, m.cfg.PermissionTimeout, m.log)

	spec := sandbox.LaunchSpec{
		SessionID:    lc.sessionID,
		Template:     lc.template,
		SystemPrompt: composeSystemPrompt(lc.template.Spec.SystemPrompt),
		Deadline:     time.Now().Add(m.cfg.SessionTTL),
	}

	liveSess, result, err := m.launcher.Launch(ctx, sink, spec, endpoints)
	if err != nil {
		m.log.ErrorContext(ctx, "sandbox launch failed", "err", err)
		m.swapReaction(ctx, lc.channelID, lc.reactTS(), reactionStarting, reactionFailed)
		m.post(ctx, lc.channelID, lc.threadTS, slack.MsgOptionText(":x: Failed to launch the sandbox.", false))
		m.setStatus(ctx, lc.sessionID, "failed")
		return
	}

	_ = m.store.UpdateSessionSandbox(ctx, lc.sessionID, result.ClaimName, result.SandboxName, "launching")
	_ = m.store.UpdateSessionACP(ctx, lc.sessionID, liveSess.ID(), "running")

	m.register(lc.channelID, lc.threadTS, &binding{
		sessionID:      lc.sessionID,
		ownerUserID:    lc.userID,
		claimName:      result.ClaimName,
		session:        liveSess,
		sink:           sink,
		originThreadTS: lc.originThreadTS,
	})
	launched = true

	sink.setSandbox(result.SandboxName)

	m.swapReaction(ctx, lc.channelID, lc.reactTS(), reactionStarting, reactionReady)
	readyText := fmt.Sprintf(":rocket: Connected and ready — sandbox `%s`. Reply in this thread to talk to the agent.", result.SandboxName)
	if lc.args != "" {
		readyText = fmt.Sprintf(":rocket: Connected and ready — sandbox `%s`. Working on your prompt…", result.SandboxName)
	}
	m.post(ctx, lc.channelID, lc.threadTS, slack.MsgOptionText(readyText, false))

	if lc.args != "" {
		b := m.lookup(lc.channelID, lc.threadTS)
		go m.runTurn(context.WithoutCancel(ctx), b, lc.args)
	}
}

// buildProxiedEndpoints fetches a fresh access token per required MCP server
// and builds the sidecar endpoint configs. The Authorization header lives
// only in the endpoint config consumed by the sandbox sidecar; the agent is
// pointed at the sidecar's credential-free 127.0.0.1 listeners instead.
func (m *Manager) buildProxiedEndpoints(ctx context.Context, lc launchContext) ([]sandbox.ProxiedEndpoint, error) {
	endpoints := make([]sandbox.ProxiedEndpoint, 0, len(lc.template.Spec.RequiredMCPServers))
	for i, srv := range lc.template.Spec.RequiredMCPServers {
		token, err := m.broker.AccessToken(ctx, lc.userID, srv.Name, srv.URL)
		if err != nil {
			return nil, fmt.Errorf("access token for %q: %w", srv.Name, err)
		}
		endpoints = append(endpoints, sandbox.ProxiedEndpoint{
			Name:        srv.Name,
			ListenPort:  sandbox.MCPListenPort(i),
			UpstreamURL: srv.URL,
			Headers:     map[string]string{"authorization": "Bearer " + token},
		})
	}
	return endpoints, nil
}

// HandleMessage forwards a user thread reply as the next ACP turn. Only the
// user who started the session may drive it — the agent runs with that user's
// MCP tokens, so replies from others must not steer it.
func (m *Manager) HandleMessage(ctx context.Context, in gateway.ThreadMessage) {
	ctx = telemetry.With(ctx, "channel", in.ChannelID, "thread_ts", in.ThreadTS, "user", in.UserID)
	ctx, op := m.tel.Start(ctx, "HandleMessage")
	defer op.Complete()

	b := m.lookup(in.ChannelID, in.ThreadTS)
	if b == nil {
		// A common silent failure: reply to a thread we hold no live session
		// for (never launched, launch in progress, or lost on restart).
		m.tel.Debug(ctx, "no live session for thread; ignoring reply", "live_sessions", m.liveCount())
		return
	}
	if in.UserID != b.ownerUserID {
		m.log.InfoContext(ctx, "ignoring thread reply from non-owner",
			"session", b.sessionID, "owner", b.ownerUserID, "actor", in.UserID)
		return
	}
	m.runTurn(ctx, b, in.Text)
}

func (m *Manager) runTurn(ctx context.Context, b *binding, text string) {
	if b == nil {
		return
	}
	turn := b.sink.BeginTurn()
	ctx = telemetry.With(ctx, "session_id", b.sessionID, "turn", turn)
	ctx, op := m.tel.Start(ctx, "runTurn", "chars", len(text))
	defer op.Complete()

	// Mark the root message busy for the duration of the turn. Turns can
	// overlap, so only the first adds the reaction and only the last removes it.
	if b.busy.Add(1) == 1 {
		m.addReaction(ctx, b.sink.channel, b.sink.threadTS, reactionBusy)
	}
	defer func() {
		if b.busy.Add(-1) == 0 {
			m.removeReaction(ctx, b.sink.channel, b.sink.threadTS, reactionBusy)
		}
	}()

	if _, err := b.session.Prompt(ctx, text); err != nil {
		m.log.ErrorContext(ctx, "acp prompt failed", "session", b.sessionID, "err", err)
		m.post(ctx, b.sink.channel, b.sink.threadTS, slack.MsgOptionText(":x: The agent encountered an error.", false))
	}
	finalTS := b.sink.EndTurn(ctx)

	// A loop-in session cross-posts its first answer back into the origin
	// thread, once. Only a turn that produced visible output counts, so an
	// empty first turn doesn't burn the one-shot.
	if finalTS != "" && b.originThreadTS != "" && b.firstAnswerPosted.CompareAndSwap(false, true) {
		link, err := m.poster.Permalink(ctx, b.sink.channel, finalTS)
		if err != nil {
			m.tel.Debug(ctx, "answer permalink failed", "err", err)
		}
		m.post(ctx, b.sink.channel, b.originThreadTS,
			slack.MsgOptionText(":white_check_mark: Answered in "+linkOr(link, "the session thread")+".", false))
	}
}

// HandleInteraction routes a Block Kit action: a "Connect" button click records
// its response_url so the connect prompt can be edited in place once OAuth
// completes; a permission button click resolves an in-flight tool-call prompt.
func (m *Manager) HandleInteraction(ctx context.Context, in gateway.Interaction) {
	if strings.HasPrefix(in.ActionID, gateway.ActionConnectPrefix) {
		m.rememberConnectResponseURL(ctx, in.Value, in.ResponseURL)
		return
	}
	if in.ActionID != gateway.ActionPermission {
		return
	}
	sessionID, toolCallID, optionID, ok := gateway.DecodePermissionValue(in.Value)
	if !ok {
		return
	}
	b := m.lookup(in.ChannelID, in.ThreadTS)
	if b == nil {
		return
	}
	// Only the session owner may approve/deny tool calls that run with their
	// tokens, and the click must target this session.
	if in.UserID != b.ownerUserID || sessionID != b.sessionID {
		m.log.InfoContext(ctx, "ignoring permission interaction from non-owner or stale session",
			"session", b.sessionID, "owner", b.ownerUserID, "actor", in.UserID, "claimed_session", sessionID)
		return
	}
	b.sink.resolvePermission(toolCallID, sandbox.PermissionDecision{OptionID: optionID})
}

// rememberConnectResponseURL persists the response_url from a "Connect" button
// click so OAuthCallback can edit the ephemeral connect prompt in place
// (button → ✅) once the user finishes authorizing — an ephemeral message can
// only be updated via a response_url, which a plain @mention can't carry. The
// session is located via the flow id embedded in the connect URL (the button's
// value).
func (m *Manager) rememberConnectResponseURL(ctx context.Context, connectURL, responseURL string) {
	if responseURL == "" {
		return
	}
	flowID := flowIDFromConnectURL(connectURL)
	if flowID == "" {
		return
	}
	flow, err := m.store.GetOAuthFlow(ctx, flowID)
	if err != nil {
		return
	}
	sess, err := m.store.GetSessionByThread(ctx, flow.SlackChannelID, flow.SlackThreadTS)
	if err != nil {
		return
	}
	if err := m.store.UpdateSessionResponseURL(ctx, sess.ID, responseURL); err != nil {
		m.log.WarnContext(ctx, "persist connect response_url failed", "session", sess.ID, "err", err)
	}
}

// flowIDFromConnectURL extracts the flow id from a connect URL of the form
// ".../connect/<flowID>".
func flowIDFromConnectURL(connectURL string) string {
	const marker = "/connect/"
	i := strings.LastIndex(connectURL, marker)
	if i < 0 {
		return ""
	}
	id := connectURL[i+len(marker):]
	if j := strings.IndexAny(id, "?#"); j >= 0 {
		id = id[:j]
	}
	return id
}

// ConnectRedirect resolves a short connect link's flow id to the provider
// authorization URL (used by the gateway's /connect/{id} redirect).
func (m *Manager) ConnectRedirect(ctx context.Context, flowID string) (string, error) {
	return m.broker.AuthorizeURL(ctx, flowID)
}

// OAuthCallback completes a server's OAuth flow and resumes the launch if all
// required servers are now connected.
func (m *Manager) OAuthCallback(ctx context.Context, code, state string) error {
	ctx, op := m.tel.Start(ctx, "OAuthCallback")
	defer op.Complete()

	res, err := m.broker.HandleCallback(ctx, code, state)
	if err != nil {
		return op.Failure(err)
	}
	ctx = telemetry.With(ctx, "user", res.SlackUserID, "server", res.ServerName, "thread_ts", res.SlackThreadTS)
	m.tel.Debug(ctx, "oauth callback connected server", "template", res.WorkflowName)

	tmpl, err := m.resolver.Resolve(ctx, res.WorkflowName)
	if err != nil {
		return nil // can't resume automatically; user can re-run
	}
	sess, err := m.store.GetSessionByThread(ctx, res.SlackChannelID, res.SlackThreadTS)
	if err != nil {
		return nil
	}
	if sess.Status == "running" || sess.Status == "launching" {
		return nil // already launched
	}
	// The token exchange (HandleCallback above) is done synchronously so the
	// browser gets an accurate result. The connect-or-launch that follows can
	// take minutes, so run it detached from the browser request's context.
	// replacePrompt edits the original connect prompt in place (button → ✅
	// Connected) rather than posting a fresh one.
	go m.connectOrLaunch(context.WithoutCancel(ctx), launchContext{
		sessionID:     sess.ID,
		userID:        res.SlackUserID,
		teamID:        res.SlackTeamID,
		channelID:     res.SlackChannelID,
		threadTS:      res.SlackThreadTS,
		template:      tmpl,
		args:          sess.InitialPrompt,
		responseURL:   sess.SlackResponseURL,
		replacePrompt: true,
	})
	return nil
}

// EndSession tears down the sandbox and forgets the live binding for a thread.
func (m *Manager) EndSession(ctx context.Context, channelID, threadTS string) {
	b := m.lookup(channelID, threadTS)
	if b == nil {
		return
	}
	_ = b.session.Close()
	if err := m.launcher.Teardown(ctx, b.claimName); err != nil {
		m.log.WarnContext(ctx, "sandbox teardown failed", "claim", b.claimName, "err", err)
	}
	m.setStatus(ctx, b.sessionID, "ended")
	m.unregister(channelID, threadTS)
}

// ReconcileOnStartup brings persisted state in line with reality after a
// restart. Live ACP sessions do not survive a process restart (they are held in
// memory), so any session left "running"/"launching" is orphaned: its sandbox
// is torn down and it is marked interrupted, with a notice posted to the thread.
func (m *Manager) ReconcileOnStartup(ctx context.Context) {
	sessions, err := m.store.ListActiveSessions(ctx)
	if err != nil {
		m.log.ErrorContext(ctx, "startup reconcile: list active sessions failed", "err", err)
		return
	}
	for _, s := range sessions {
		if s.SandboxClaimName != "" {
			if err := m.launcher.Teardown(ctx, s.SandboxClaimName); err != nil {
				m.log.WarnContext(ctx, "startup reconcile: teardown failed", "claim", s.SandboxClaimName, "err", err)
			}
		}
		m.setStatus(ctx, s.ID, "interrupted")
		if s.Status == "running" || s.Status == "launching" {
			m.post(ctx, s.SlackChannelID, s.SlackThreadTS,
				slack.MsgOptionText(":warning: This session was interrupted by a restart. Please start a new one.", false))
		}
	}
}

// SweepExpired ends sessions whose TTL has elapsed: it tears down the sandbox,
// closes the live ACP session if present, and marks the row ended. It also
// purges expired in-flight OAuth flows.
func (m *Manager) SweepExpired(ctx context.Context) {
	if err := m.store.DeleteExpiredOAuthFlows(ctx, time.Now()); err != nil {
		m.log.WarnContext(ctx, "sweep: delete expired oauth flows failed", "err", err)
	}
	sessions, err := m.store.ListActiveSessions(ctx)
	if err != nil {
		m.log.WarnContext(ctx, "sweep: list active sessions failed", "err", err)
		return
	}
	for _, s := range sessions {
		if time.Since(s.CreatedAt) < m.cfg.SessionTTL {
			continue
		}
		if b := m.lookup(s.SlackChannelID, s.SlackThreadTS); b != nil {
			m.post(ctx, s.SlackChannelID, s.SlackThreadTS,
				slack.MsgOptionText(":hourglass: Session timed out and was ended.", false))
			m.EndSession(ctx, s.SlackChannelID, s.SlackThreadTS)
			continue
		}
		// No live binding (e.g. orphaned): tear down the claim if any, mark ended.
		if s.SandboxClaimName != "" {
			if err := m.launcher.Teardown(ctx, s.SandboxClaimName); err != nil {
				m.log.WarnContext(ctx, "sweep: teardown failed", "claim", s.SandboxClaimName, "err", err)
			}
		}
		m.setStatus(ctx, s.ID, "ended")
	}
}

// --- helpers ----------------------------------------------------------------

func (m *Manager) post(ctx context.Context, channel, threadTS string, opts ...slack.MsgOption) {
	if threadTS != "" {
		opts = append(opts, slack.MsgOptionTS(threadTS))
	}
	if _, err := m.poster.PostMessage(ctx, channel, opts...); err != nil {
		m.log.ErrorContext(ctx, "post message failed", "channel", channel, "thread_ts", threadTS, "err", err)
	}
}

// postEphemeral posts a message visible only to userID, used for per-user
// auth-flow notices (OAuth links and connection status) that shouldn't be
// exposed to the rest of the channel.
//
// These are deliberately NOT threaded. Slack only renders an ephemeral message
// carrying a thread_ts while the user is actively viewing that thread, so a
// prompt posted into the mention's thread may be invisible if the user is still
// looking at the channel. Posting at channel level makes it appear immediately
// where the user just @mentioned the bot.
func (m *Manager) postEphemeral(ctx context.Context, channel, userID string, opts ...slack.MsgOption) {
	if _, err := m.poster.PostEphemeral(ctx, channel, userID, opts...); err != nil {
		m.log.ErrorContext(ctx, "post ephemeral message failed", "channel", channel, "user", userID, "err", err)
	}
}

// addReaction places an emoji on a message, best-effort: a failure here must not
// abort a launch. A genuine failure is logged at Warn (the common cause is the
// bot missing the reactions:write scope); a benign "already on the message" is
// ignored.
func (m *Manager) addReaction(ctx context.Context, channel, ts, emoji string) {
	if ts == "" {
		return
	}
	err := m.poster.AddReaction(ctx, channel, ts, emoji)
	if err == nil || err.Error() == "already_reacted" {
		return
	}
	m.tel.Warn(ctx, "add reaction failed; grant the bot the reactions:write scope to show launch progress",
		"emoji", emoji, "err", err)
}

// removeReaction removes an emoji from a message, best-effort (a failure —
// commonly "no_reaction" when it was never added — is only a debug note).
func (m *Manager) removeReaction(ctx context.Context, channel, ts, emoji string) {
	if ts == "" {
		return
	}
	if err := m.poster.RemoveReaction(ctx, channel, ts, emoji); err != nil {
		m.tel.Debug(ctx, "remove reaction failed", "emoji", emoji, "err", err)
	}
}

// swapReaction removes the "from" reaction and adds the "to" reaction on a
// message (e.g. hourglass → checkmark), best-effort.
func (m *Manager) swapReaction(ctx context.Context, channel, ts, from, to string) {
	if ts == "" {
		return
	}
	m.removeReaction(ctx, channel, ts, from)
	m.addReaction(ctx, channel, ts, to)
}

func (m *Manager) setStatus(ctx context.Context, sessionID, status string) {
	if err := m.store.UpdateSessionStatus(ctx, sessionID, status); err != nil {
		m.log.WarnContext(ctx, "update session status failed", "session", sessionID, "status", status, "err", err)
	}
}

func (m *Manager) lookup(channel, threadTS string) *binding {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.live[threadKey(channel, threadTS)]
}

func (m *Manager) liveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.live)
}

// reserve marks a thread as launching iff it is neither already live nor
// already launching, returning true if the caller won the reservation.
func (m *Manager) reserve(channel, threadTS string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := threadKey(channel, threadTS)
	if _, live := m.live[key]; live {
		return false
	}
	if _, launching := m.launching[key]; launching {
		return false
	}
	m.launching[key] = struct{}{}
	return true
}

func (m *Manager) releaseReservation(channel, threadTS string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.launching, threadKey(channel, threadTS))
}

func (m *Manager) register(channel, threadTS string, b *binding) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := threadKey(channel, threadTS)
	m.live[key] = b
	delete(m.launching, key)
}

func (m *Manager) unregister(channel, threadTS string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := threadKey(channel, threadTS)
	delete(m.live, key)
	delete(m.launching, key)
}

// authFallbackText is the plain-text notification fallback that accompanies the
// auth-prompt blocks (Slack shows it in notifications and clients without Block
// Kit support). It lists only the servers still needing a connection.
func authFallbackText(template string, servers []gateway.AuthStatus) string {
	var parts []string
	for _, s := range servers {
		if s.Connected {
			continue
		}
		parts = append(parts, s.ServerName+": "+s.URL)
	}
	return fmt.Sprintf("To run %s, connect these tools — %s", template, strings.Join(parts, " | "))
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
