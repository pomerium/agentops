package session_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/slack-go/slack"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/agenttemplate"
	"github.com/pomerium/agentops/internal/channels/slack/gateway"
	"github.com/pomerium/agentops/internal/chatops/session"
	"github.com/pomerium/agentops/internal/chatops/store"
	"github.com/pomerium/agentops/internal/mcpbroker"
	"github.com/pomerium/agentops/internal/sandbox"
)

// --- fakes ------------------------------------------------------------------

type fakeBroker struct {
	connected   map[string]bool // server name -> connected
	startCalls  []string
	tokenCalls  []string
	callback    mcpbroker.CallbackResult
	callbackErr error
}

func (f *fakeBroker) ConnectionStatus(_ context.Context, _, server, _ string) (bool, error) {
	return f.connected[server], nil
}
func (f *fakeBroker) StartAuthFlow(_ context.Context, req mcpbroker.StartAuthRequest) (string, error) {
	f.startCalls = append(f.startCalls, req.ServerName)
	return "https://auth.example/" + req.ServerName, nil
}
func (f *fakeBroker) AccessToken(_ context.Context, _, server, _ string) (string, error) {
	f.tokenCalls = append(f.tokenCalls, server)
	return "tok-" + server, nil
}
func (f *fakeBroker) HandleCallback(_ context.Context, _, _ string) (mcpbroker.CallbackResult, error) {
	return f.callback, f.callbackErr
}
func (f *fakeBroker) AuthorizeURL(_ context.Context, flowID string) (string, error) {
	return "https://provider.example/authorize?flow=" + flowID, nil
}

// fakeResolver binds channels per its bindings map and resolves only tmpl's
// name.
type fakeResolver struct {
	tmpl     *v1alpha1.AgentTemplate
	bindings map[string]string
}

// boundResolver resolves tmpl and binds it to channel C1, the channel the
// tests mention from.
func boundResolver(tmpl *v1alpha1.AgentTemplate) *fakeResolver {
	return &fakeResolver{tmpl: tmpl, bindings: map[string]string{"C1": tmpl.Name}}
}

func (f *fakeResolver) SlackChannelTemplate(_ context.Context, channelID string) (string, error) {
	if name, ok := f.bindings[channelID]; ok {
		return name, nil
	}
	return "", fmt.Errorf("%w: %q", agenttemplate.ErrChannelNotBound, channelID)
}

func (f *fakeResolver) Resolve(_ context.Context, name string) (*v1alpha1.AgentTemplate, error) {
	if f.tmpl != nil && f.tmpl.Name == name {
		return f.tmpl, nil
	}
	return nil, fmt.Errorf("%w: %q", agenttemplate.ErrNotFound, name)
}

// failingResolver simulates an infrastructure failure (missing CRD, RBAC,
// API-server trouble): every call errors with something that is NOT
// ErrChannelNotBound/ErrNotFound.
type failingResolver struct{}

func (failingResolver) SlackChannelTemplate(_ context.Context, _ string) (string, error) {
	return "", errors.New("no matches for kind ChannelConfig in version agents.pomerium.com/v1alpha1")
}

func (failingResolver) Resolve(_ context.Context, _ string) (*v1alpha1.AgentTemplate, error) {
	return nil, errors.New("no matches for kind AgentTemplate in version agents.pomerium.com/v1alpha1")
}

type fakeLiveSession struct {
	prompts []string
	mu      sync.Mutex
	gate    chan struct{} // if non-nil, Prompt blocks until the channel is closed
	// reply, when set, is emitted to the launch's event sink on every Prompt so
	// the turn produces visible output (and EndTurn reports a final message).
	reply string
	sink  sandbox.EventSink
}

func (s *fakeLiveSession) ID() string { return "acp-sess" }
func (s *fakeLiveSession) Prompt(ctx context.Context, text string) (acp.StopReason, error) {
	s.mu.Lock()
	s.prompts = append(s.prompts, text)
	gate := s.gate
	reply, sink := s.reply, s.sink
	s.mu.Unlock()
	if reply != "" && sink != nil {
		sink.AgentMessage(ctx, reply)
	}
	if gate != nil {
		<-gate
	}
	return acp.StopReasonEndTurn, nil
}
func (s *fakeLiveSession) Cancel(context.Context) error { return nil }
func (s *fakeLiveSession) Close() error                 { return nil }
func (s *fakeLiveSession) promptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.prompts)
}

type fakeLauncher struct {
	mu            sync.Mutex
	launches      int
	lastEndpoints []sandbox.ProxiedEndpoint
	session       *fakeLiveSession
	teardowns     []string
}

func (l *fakeLauncher) Launch(_ context.Context, sink sandbox.EventSink, _ sandbox.LaunchSpec, endpoints []sandbox.ProxiedEndpoint) (session.LiveSession, sandbox.LaunchResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.launches++
	l.lastEndpoints = endpoints
	if l.session == nil {
		l.session = &fakeLiveSession{}
	}
	l.session.mu.Lock()
	l.session.sink = sink
	l.session.mu.Unlock()
	return l.session, sandbox.LaunchResult{ClaimName: "claim-1", SandboxName: "sbx-1"}, nil
}
func (l *fakeLauncher) Teardown(_ context.Context, claimName string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.teardowns = append(l.teardowns, claimName)
	return nil
}
func (l *fakeLauncher) launchCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.launches
}
func (l *fakeLauncher) mcpCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lastEndpoints)
}

func (l *fakeLauncher) endpoints() []sandbox.ProxiedEndpoint {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastEndpoints
}
func (l *fakeLauncher) liveSession() *fakeLiveSession {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.session
}

type fakePoster struct {
	mu        sync.Mutex
	seq       int
	posts     []postRecord
	messages  []string
	ephemeral []ephemeralPost
	reactions []reactionOp
	// failTopLevel makes PostMessage fail for posts that carry no thread_ts
	// (i.e. would start a new top-level message).
	failTopLevel bool
	// replies/repliesErr are the canned ThreadReplies result.
	replies      []slack.Message
	repliesErr   error
	permalinkErr error
}

type ephemeralPost struct {
	userID string
	text   string
}

// postRecord is one public message as observed by the fake, with the routing
// fields tests assert on.
type postRecord struct {
	channel  string
	threadTS string // "" for a top-level message
	ts       string
	text     string
}

// reactionOp records one Add/RemoveReaction call on a message.
type reactionOp struct {
	op    string // "add" or "remove"
	ts    string
	emoji string
}

func (p *fakePoster) PostMessage(_ context.Context, channel string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	threadTS, text := renderOptions(opts)
	if p.failTopLevel && threadTS == "" {
		return "", errors.New("post failed")
	}
	p.seq++
	ts := fmt.Sprintf("1700000000.%04d", p.seq)
	p.posts = append(p.posts, postRecord{channel: channel, threadTS: threadTS, ts: ts, text: text})
	p.messages = append(p.messages, text)
	return ts, nil
}

func (p *fakePoster) ThreadReplies(_ context.Context, _, _ string, _ int) ([]slack.Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]slack.Message(nil), p.replies...), p.repliesErr
}

func (p *fakePoster) Permalink(_ context.Context, channel, ts string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.permalinkErr != nil {
		return "", p.permalinkErr
	}
	return "https://slack.test/archives/" + channel + "/p" + strings.ReplaceAll(ts, ".", ""), nil
}

// postsIn returns the concatenated public messages posted into the given
// thread ("" selects top-level messages).
func (p *fakePoster) postsIn(threadTS string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var parts []string
	for _, r := range p.posts {
		if r.threadTS == threadTS {
			parts = append(parts, r.text)
		}
	}
	return strings.Join(parts, "\n")
}

// firstTopLevelTS returns the ts of the first top-level message posted, or "".
func (p *fakePoster) firstTopLevelTS() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.posts {
		if r.threadTS == "" {
			return r.ts
		}
	}
	return ""
}
func (p *fakePoster) PostEphemeral(_ context.Context, _, userID string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, text := renderOptions(opts)
	p.ephemeral = append(p.ephemeral, ephemeralPost{userID: userID, text: text})
	return "1700000000.0001", nil
}
func (p *fakePoster) UpdateMessage(_ context.Context, _, _ string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, text := renderOptions(opts)
	p.messages = append(p.messages, text)
	return "1700000000.0001", nil
}
func (p *fakePoster) UpdateMessageDebounced(_ context.Context, _, _ string, opts ...slack.MsgOption) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, text := renderOptions(opts)
	p.messages = append(p.messages, text)
}
func (p *fakePoster) Respond(_ context.Context, _ string, _ bool, _ string, _ []slack.Block) error {
	return nil
}

func (p *fakePoster) AddReaction(_ context.Context, _, ts, emoji string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reactions = append(p.reactions, reactionOp{op: "add", ts: ts, emoji: emoji})
	return nil
}

func (p *fakePoster) RemoveReaction(_ context.Context, _, ts, emoji string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reactions = append(p.reactions, reactionOp{op: "remove", ts: ts, emoji: emoji})
	return nil
}

// addedReactions returns the emojis added (in order) across all messages.
func (p *fakePoster) addedReactions() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, r := range p.reactions {
		if r.op == "add" {
			out = append(out, r.emoji)
		}
	}
	return out
}

// reactionOps returns a copy of every reaction add/remove in call order.
func (p *fakePoster) reactionOps() []reactionOp {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]reactionOp(nil), p.reactions...)
}

// countReaction counts ops of the given kind ("add"/"remove") for an emoji.
func (p *fakePoster) countReaction(op, emoji string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.reactions {
		if r.op == op && r.emoji == emoji {
			n++
		}
	}
	return n
}

// all returns every message posted, whether public or ephemeral.
func (p *fakePoster) all() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	parts := append([]string(nil), p.messages...)
	for _, e := range p.ephemeral {
		parts = append(parts, e.text)
	}
	return strings.Join(parts, "\n")
}

// public returns only non-ephemeral messages.
func (p *fakePoster) public() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.messages, "\n")
}

// ephemeralFor returns the concatenated ephemeral messages addressed to userID.
func (p *fakePoster) ephemeralFor(userID string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var parts []string
	for _, e := range p.ephemeral {
		if e.userID == userID {
			parts = append(parts, e.text)
		}
	}
	return strings.Join(parts, "\n")
}

// renderOptions extracts the thread_ts and all serialized form values from
// slack MsgOptions so tests can assert on routing, text, and Block Kit content
// alike.
func renderOptions(opts []slack.MsgOption) (threadTS, text string) {
	_, vals, _ := slack.UnsafeApplyMsgOptions("tok", "chan", "https://slack.com/api/", opts...)
	var b strings.Builder
	for _, vs := range vals {
		for _, v := range vs {
			b.WriteString(v)
			b.WriteByte(' ')
		}
	}
	return vals.Get("thread_ts"), b.String()
}

func newManager(t *testing.T, b session.Broker, r session.TemplateResolver, l session.Launcher, p session.Poster) *session.Manager {
	t.Helper()
	s, err := store.Open(context.Background(), t.TempDir()+"/m.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return session.NewManager(session.Config{Namespace: "ns"}, s, b, r, l, p, nil)
}

func deployTemplate() *v1alpha1.AgentTemplate {
	return &v1alpha1.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "deploy"},
		Spec: v1alpha1.AgentTemplateSpec{
			SystemPrompt: "be helpful",
			WarmPoolRef:  v1alpha1.SandboxWarmPoolReference{Name: "claude-code"},
			RequiredMCPServers: []v1alpha1.MCPServerRef{
				{Name: "github", URL: "https://gh/mcp"},
				{Name: "k8s", URL: "https://k8s/mcp"},
			},
		},
	}
}

func TestHandleMentionPromptsAuthWhenMissing(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": false}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001",
	})

	if launcher.launchCount() != 0 {
		t.Errorf("should not launch while a server is unconnected; launches=%d", launcher.launchCount())
	}
	if len(broker.startCalls) != 1 || broker.startCalls[0] != "k8s" {
		t.Errorf("expected auth flow started for k8s only, got %v", broker.startCalls)
	}
	// The auth prompt is personal to the requesting user: it must be posted as
	// an ephemeral message addressed to U1, not into the shared thread.
	if !strings.Contains(poster.ephemeralFor("U1"), "auth.example/k8s") {
		t.Errorf("expected auth link posted ephemerally to U1, got: %s", poster.ephemeralFor("U1"))
	}
	if strings.Contains(poster.public(), "auth.example/k8s") {
		t.Errorf("auth link must not be posted to the public thread, got: %s", poster.public())
	}
	// The mention is acknowledged immediately with the starting reaction, but it
	// must not yet show a ready marker while still awaiting auth.
	added := poster.addedReactions()
	if !slices.Contains(added, "hourglass_flowing_sand") {
		t.Errorf("expected the starting reaction as acknowledgment, got %v", added)
	}
	if slices.Contains(added, "white_check_mark") {
		t.Errorf("must not show ready while awaiting auth, got %v", added)
	}
}

func TestHandleMentionLaunchesWhenAllConnected(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001", Prompt: "ship it",
	})

	if launcher.launchCount() != 1 {
		t.Fatalf("expected 1 launch, got %d", launcher.launchCount())
	}
	if len(broker.startCalls) != 0 {
		t.Errorf("should not start auth when all connected, got %v", broker.startCalls)
	}
	// Per-server proxied endpoints (with bearer headers destined for the
	// sidecar) must be passed to the launcher, on deterministic ports.
	if launcher.mcpCount() != 2 {
		t.Errorf("expected 2 mcp endpoints passed to launch, got %d", launcher.mcpCount())
	}
	for i, ep := range launcher.endpoints() {
		if got := ep.Headers["authorization"]; !strings.HasPrefix(got, "Bearer ") {
			t.Errorf("endpoint %s authorization = %q, want a bearer token", ep.Name, got)
		}
		if want := sandbox.MCPListenPort(i); ep.ListenPort != want {
			t.Errorf("endpoint %s port = %d, want %d", ep.Name, ep.ListenPort, want)
		}
	}
	if len(broker.tokenCalls) != 2 {
		t.Errorf("expected access tokens fetched for both servers, got %v", broker.tokenCalls)
	}
	// The mention message gets an hourglass while starting, then the ready
	// rocket.
	added := poster.addedReactions()
	if !slices.Contains(added, "hourglass_flowing_sand") || !slices.Contains(added, "rocket") {
		t.Errorf("expected starting+ready reactions on the root message, got %v", added)
	}
}

func TestHandleMentionUnboundChannel(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	// C9 has no binding (boundResolver binds C1 only).
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C9", MessageTS: "1.0", ThreadTS: "1.0",
	})
	if launcher.launchCount() != 0 {
		t.Error("should not launch in an unbound channel")
	}
	if !strings.Contains(strings.ToLower(poster.all()), "no agent is configured for this channel") {
		t.Errorf("expected the unbound-channel notice, got: %s", poster.all())
	}
}

// A channel bound to a template that doesn't exist is an operator error, not a
// user typo — the message must say so, distinctly from the unbound case.
func TestHandleMentionBoundTemplateMissing(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, &fakeResolver{
		tmpl:     deployTemplate(),
		bindings: map[string]string{"C1": "ghost"},
	}, launcher, poster)

	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", MessageTS: "1.0", ThreadTS: "1.0",
	})
	if launcher.launchCount() != 0 {
		t.Error("should not launch when the bound template is missing")
	}
	all := poster.all()
	if !strings.Contains(all, "`ghost`") || !strings.Contains(all, "configuration problem") {
		t.Errorf("expected the missing-template config-error notice naming the binding, got: %s", all)
	}
	if strings.Contains(strings.ToLower(all), "no agent is configured for this channel") {
		t.Errorf("a bad binding must not be reported as an unbound channel, got: %s", all)
	}
}

// A resolver infrastructure failure must not masquerade as "no such agent":
// the user gets a "broken on my side" message and the error reaches the log
// (a missing CRD/RBAC once rendered exactly like a typo, invisibly — see the
// AgentTemplate rename rollout).
func TestHandleMentionResolverFailureIsDistinguishedFromUnknown(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}

	var logBuf strings.Builder
	log := slog.New(slog.NewTextHandler(&logBuf, nil))
	st, err := store.Open(context.Background(), t.TempDir()+"/m.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	m := session.NewManager(session.Config{Namespace: "ns"}, st, broker, failingResolver{}, launcher, poster, log)

	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", MessageTS: "1.0", ThreadTS: "1.0",
	})

	if launcher.launchCount() != 0 {
		t.Error("should not launch when the resolver fails")
	}
	all := strings.ToLower(poster.all())
	if strings.Contains(all, "no agent is configured for this channel") {
		t.Errorf("an infra failure must not be reported as an unbound channel, got: %s", poster.all())
	}
	if !strings.Contains(all, "look up") {
		t.Errorf("expected a lookup-failure message, got: %s", poster.all())
	}
	if !strings.Contains(logBuf.String(), "no matches for kind ChannelConfig") {
		t.Errorf("expected the resolve error in the log, got: %s", logBuf.String())
	}
}

func TestHandleMentionInLiveThreadIsIgnored(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	mention := gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001",
	}
	m.HandleMention(context.Background(), mention)
	if launcher.launchCount() != 1 {
		t.Fatalf("precondition: expected a launched session, got %d", launcher.launchCount())
	}
	// A second mention in the same (now live) thread must not launch again.
	m.HandleMention(context.Background(), mention)
	if launcher.launchCount() != 1 {
		t.Errorf("a mention in a live thread must not start a second sandbox; launches=%d", launcher.launchCount())
	}
}

func TestHandleMessageRejectsNonOwner(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001",
	})
	if launcher.launchCount() != 1 || launcher.liveSession() == nil {
		t.Fatalf("precondition: expected a launched session")
	}

	// A reply from a different user must not drive the agent.
	m.HandleMessage(context.Background(), gateway.ThreadMessage{
		UserID: "U2", ChannelID: "C1", ThreadTS: "1700000000.0001", Text: "rm -rf /",
	})
	if got := launcher.liveSession().promptCount(); got != 0 {
		t.Errorf("non-owner reply should be ignored; prompts=%d", got)
	}

	// The owner's reply is forwarded.
	m.HandleMessage(context.Background(), gateway.ThreadMessage{
		UserID: "U1", ChannelID: "C1", ThreadTS: "1700000000.0001", Text: "status",
	})
	if got := launcher.liveSession().promptCount(); got != 1 {
		t.Errorf("owner reply should be forwarded; prompts=%d", got)
	}
}

func TestOAuthCallbackResumesLaunch(t *testing.T) {
	// github already connected; k8s connects via the callback, after which the
	// template should launch.
	broker := &fakeBroker{
		connected: map[string]bool{"github": true, "k8s": false},
		callback: mcpbroker.CallbackResult{
			SlackUserID: "U1", SlackChannelID: "C1", SlackThreadTS: "1700000000.0001",
			ServerName: "k8s", WorkflowName: "deploy",
		},
	}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	// First, the mention leaves the session awaiting auth for k8s.
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001",
	})
	if launcher.launchCount() != 0 {
		t.Fatalf("precondition: should be awaiting auth, launches=%d", launcher.launchCount())
	}

	// k8s connects.
	broker.connected["k8s"] = true
	if err := m.OAuthCallback(context.Background(), "code", "state"); err != nil {
		t.Fatalf("OAuthCallback: %v", err)
	}
	// The resume launch is detached from the browser request, so poll for it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && launcher.launchCount() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if launcher.launchCount() != 1 {
		t.Errorf("expected launch after final server connected, got %d", launcher.launchCount())
	}
	// The standalone per-server "Connected *k8s*." confirmation was dropped; the
	// in-list checkmark is the feedback now, so it must appear nowhere.
	if strings.Contains(poster.all(), "Connected *k8s*") {
		t.Errorf("standalone connected confirmation should be gone, got: %s", poster.all())
	}
}

// An OAuth resume whose persisted template no longer resolves (deleted or
// renamed mid-flow) must tell the thread instead of vanishing: the browser
// already said "connected", so silence would look like a launch that never
// comes.
func TestOAuthCallbackTemplateGoneNotifiesThread(t *testing.T) {
	broker := &fakeBroker{
		connected: map[string]bool{"github": true, "k8s": false},
		callback: mcpbroker.CallbackResult{
			SlackUserID: "U1", SlackChannelID: "C1", SlackThreadTS: "1700000000.0001",
			ServerName: "k8s", WorkflowName: "deploy",
		},
	}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	resolver := boundResolver(deployTemplate())
	m := newManager(t, broker, resolver, launcher, poster)

	// The mention leaves the session awaiting auth for k8s.
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001",
	})
	if launcher.launchCount() != 0 {
		t.Fatalf("precondition: should be awaiting auth, launches=%d", launcher.launchCount())
	}

	// The admin deletes the template before the user finishes connecting.
	resolver.tmpl = nil
	broker.connected["k8s"] = true
	if err := m.OAuthCallback(context.Background(), "code", "state"); err != nil {
		t.Fatalf("OAuthCallback: %v", err)
	}

	if launcher.launchCount() != 0 {
		t.Errorf("must not launch without a template, launches=%d", launcher.launchCount())
	}
	if !strings.Contains(poster.all(), "no longer exists") {
		t.Errorf("expected a missing-template notice in the thread, got: %s", poster.all())
	}
}

func TestLaunchMessageMentionsPromptWork(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	// A mention WITH an initial prompt: the ready message says we're working on
	// it rather than inviting a reply.
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001", Prompt: "ship it",
	})
	if !strings.Contains(poster.public(), "Working on your prompt") {
		t.Errorf("ready message should say it's working on the prompt, got: %s", poster.public())
	}
	if strings.Contains(poster.public(), "Reply in this thread") {
		t.Errorf("ready message should not invite a reply when a prompt was given, got: %s", poster.public())
	}
}

func TestLaunchMessageInvitesReplyWithoutPrompt(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.0001", ThreadTS: "1700000000.0001",
	})
	if !strings.Contains(poster.public(), "Reply in this thread") {
		t.Errorf("ready message should invite a reply when no prompt was given, got: %s", poster.public())
	}
}

func TestTurnTogglesBusyReaction(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	root := "1700000000.0001"
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: root, ThreadTS: root,
	})

	// A turn marks the root message busy for its duration: add before the
	// prompt, remove after.
	m.HandleMessage(context.Background(), gateway.ThreadMessage{
		UserID: "U1", ChannelID: "C1", ThreadTS: root, Text: "status",
	})

	addIdx, removeIdx := -1, -1
	for i, op := range poster.reactionOps() {
		if op.emoji != "waiting" {
			continue
		}
		if op.ts != root {
			t.Errorf("busy reaction must target the root message, got ts=%s", op.ts)
		}
		if op.op == "add" && addIdx == -1 {
			addIdx = i
		}
		if op.op == "remove" {
			removeIdx = i
		}
	}
	if addIdx == -1 || removeIdx == -1 || removeIdx < addIdx {
		t.Errorf("expected busy reaction added then removed around the turn, got %v", poster.reactionOps())
	}
}

// --- loop-in: a mention inside an existing (non-bot) thread ------------------

const (
	originTS  = "1690000000.0001"
	mentionTS = "1690000000.0042"
)

// loopInMention is a bot mention posted inside an existing discussion thread.
func loopInMention(args string) gateway.MentionInvocation {
	text := "<@U0BOT>"
	if args != "" {
		text += " " + args
	}
	return gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: mentionTS, ThreadTS: originTS, OriginThreadTS: originTS,
		Text: text, Prompt: args,
	}
}

func userReply(ts, text string) slack.Message {
	return slack.Message{Msg: slack.Msg{Timestamp: ts, Text: text}}
}

func botReply(ts, text string) slack.Message {
	m := userReply(ts, text)
	m.BotID = "B1"
	return m
}

// waitFor polls until cond holds or fails the test after a deadline.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (s *fakeLiveSession) allPrompts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.prompts...)
}

// A mention inside a thread the bot already drives is just another way to talk
// to the agent: it runs a turn with the raw text and must not launch again.
func TestMentionInLiveThreadRunsTurn(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	root := "1700000000.5000"
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: root, ThreadTS: root,
	})
	if launcher.launchCount() != 1 {
		t.Fatalf("precondition: expected a launched session, got %d", launcher.launchCount())
	}

	turnText := "<@U0BOT> deploy status please"
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.5001", ThreadTS: root, OriginThreadTS: root,
		Text: turnText, Prompt: "status please",
	})

	if launcher.launchCount() != 1 {
		t.Errorf("a mention in a live thread must not start a second sandbox; launches=%d", launcher.launchCount())
	}
	if got := launcher.liveSession().allPrompts(); len(got) != 1 || got[0] != turnText {
		t.Errorf("expected the raw mention text forwarded as the turn, got %v", got)
	}
}

func TestMentionInLiveThreadNonOwnerIgnored(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	root := "1700000000.5000"
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: root, ThreadTS: root,
	})

	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U2", ChannelID: "C1", TeamID: "T1",
		MessageTS: "1700000000.5001", ThreadTS: root, OriginThreadTS: root,
		Text: "<@U0BOT> deploy do bad things", Prompt: "do bad things",
	})

	if launcher.launchCount() != 1 {
		t.Errorf("non-owner mention must not launch; launches=%d", launcher.launchCount())
	}
	if got := launcher.liveSession().promptCount(); got != 0 {
		t.Errorf("non-owner mention must not drive the agent; prompts=%d", got)
	}
}

// A mention in a foreign thread loops the bot in: a new session thread is
// rooted at a fresh top-level message, both threads are cross-linked, and the
// agent's first prompt carries the prior discussion (texts only, no speakers).
func TestLoopInStartsNewSession(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{
		replies: []slack.Message{
			userReply(originTS, "we have a problem"),
			userReply("1690000000.0002", "I think it's the cache"),
			botReply("1690000000.0003", "bot noise"),
			userReply(mentionTS, "<@U0BOT> deploy what do you think?"),
			userReply("1690000000.0050", "later message"),
		},
	}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), loopInMention("what do you think?"))

	if launcher.launchCount() != 1 {
		t.Fatalf("expected 1 launch, got %d", launcher.launchCount())
	}
	rootTS := poster.firstTopLevelTS()
	if rootTS == "" {
		t.Fatal("expected a new top-level root message for the session thread")
	}
	// The root links back to the origin mention.
	if !strings.Contains(poster.postsIn(""), "p16900000000042") {
		t.Errorf("root message should link to the origin mention, got: %s", poster.postsIn(""))
	}
	// The origin thread gets a cross-link to the new session thread.
	if !strings.Contains(poster.postsIn(originTS), "p"+strings.ReplaceAll(rootTS, ".", "")) {
		t.Errorf("origin thread should link to the session thread, got: %s", poster.postsIn(originTS))
	}
	// The ready message lands in the session thread.
	if !strings.Contains(poster.postsIn(rootTS), "Connected and ready") {
		t.Errorf("ready message should land in the session thread, got: %s", poster.postsIn(rootTS))
	}

	waitFor(t, "first prompt", func() bool { return launcher.liveSession().promptCount() == 1 })
	prompt := launcher.liveSession().allPrompts()[0]
	for _, want := range []string{"we have a problem", "I think it's the cache", "what do you think?"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt missing %q: %s", want, prompt)
		}
	}
	for _, reject := range []string{"bot noise", "later message", "<@U0BOT>", "U1"} {
		if strings.Contains(prompt, reject) {
			t.Errorf("prompt must not contain %q: %s", reject, prompt)
		}
	}
}

func TestLoopInTranscriptCapped(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	for i := range 60 {
		poster.replies = append(poster.replies,
			userReply(fmt.Sprintf("1690000000.%04d", i+1), fmt.Sprintf("msg-%d", i)))
	}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	in := loopInMention("summarize")
	in.MessageTS = "1690000000.9999"
	m.HandleMention(context.Background(), in)

	waitFor(t, "first prompt", func() bool { return launcher.liveSession() != nil && launcher.liveSession().promptCount() == 1 })
	prompt := launcher.liveSession().allPrompts()[0]
	if !strings.Contains(prompt, "msg-59 ") && !strings.Contains(prompt, "msg-59\n") {
		t.Errorf("most recent messages must be kept, got: %s", prompt)
	}
	if strings.Contains(prompt, "msg-0\n") || strings.Contains(prompt, "msg-9 ") {
		t.Errorf("oldest messages should be dropped past the cap: %s", prompt)
	}
	if !strings.Contains(prompt, "earlier messages omitted") {
		t.Errorf("expected an omission marker when truncated, got: %s", prompt)
	}
}

func TestLoopInRepliesFetchFailsDegrades(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{repliesErr: errors.New("missing_scope")}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), loopInMention("what do you think?"))

	if launcher.launchCount() != 1 {
		t.Fatalf("a failed transcript fetch must not block the session; launches=%d", launcher.launchCount())
	}
	waitFor(t, "first prompt", func() bool { return launcher.liveSession().promptCount() == 1 })
	if got := launcher.liveSession().allPrompts()[0]; got != "what do you think?" {
		t.Errorf("prompt should degrade to the bare question, got: %q", got)
	}
}

func TestLoopInPermalinkFailsDegrades(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{
		replies:      []slack.Message{userReply(originTS, "we have a problem")},
		permalinkErr: errors.New("message_not_found"),
	}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), loopInMention("help"))

	if launcher.launchCount() != 1 {
		t.Fatalf("permalink failures must not block the session; launches=%d", launcher.launchCount())
	}
	if poster.firstTopLevelTS() == "" {
		t.Error("root message should still be posted without a permalink")
	}
	if strings.Contains(poster.public(), "https://slack.test") {
		t.Errorf("no links should render when permalinks fail, got: %s", poster.public())
	}
	if poster.postsIn(originTS) == "" {
		t.Error("origin thread should still get a cross-link note without the link")
	}
}

func TestLoopInRootPostFailsAborts(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{failTopLevel: true}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), loopInMention("help"))

	if launcher.launchCount() != 0 {
		t.Errorf("no session thread means no launch; launches=%d", launcher.launchCount())
	}
	if !strings.Contains(poster.postsIn(originTS), ":x:") {
		t.Errorf("expected a failure notice in the origin thread, got: %s", poster.postsIn(originTS))
	}
}

func TestLoopInFirstAnswerCrossPostedOnce(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{session: &fakeLiveSession{reply: "the answer"}}
	poster := &fakePoster{replies: []slack.Message{userReply(originTS, "we have a problem")}}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), loopInMention("what do you think?"))

	waitFor(t, "first answer cross-post", func() bool {
		return strings.Contains(poster.postsIn(originTS), "Answered")
	})
	rootTS := poster.firstTopLevelTS()

	// A follow-up turn in the session thread must not cross-post again.
	m.HandleMessage(context.Background(), gateway.ThreadMessage{
		UserID: "U1", ChannelID: "C1", ThreadTS: rootTS, Text: "more please",
	})
	waitFor(t, "second prompt", func() bool { return launcher.liveSession().promptCount() == 2 })
	if got := strings.Count(poster.postsIn(originTS), "Answered"); got != 1 {
		t.Errorf("the answer link must be cross-posted exactly once, got %d in: %s", got, poster.postsIn(originTS))
	}
}

func TestLoopInReactionsOnOriginMention(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{replies: []slack.Message{userReply(originTS, "we have a problem")}}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	m.HandleMention(context.Background(), loopInMention("help"))

	var starting, ready bool
	for _, op := range poster.reactionOps() {
		if op.op != "add" {
			continue
		}
		if op.emoji == "hourglass_flowing_sand" && op.ts == mentionTS {
			starting = true
		}
		if op.emoji == "rocket" && op.ts == mentionTS {
			ready = true
		}
	}
	if !starting || !ready {
		t.Errorf("lifecycle reactions must land on the origin mention message; got %v", poster.reactionOps())
	}
}

func TestLoopInUnboundChannelNoticeInOriginThread(t *testing.T) {
	broker := &fakeBroker{connected: map[string]bool{}}
	launcher := &fakeLauncher{}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	in := loopInMention("help")
	in.ChannelID = "C9" // no binding (boundResolver binds C1 only)
	m.HandleMention(context.Background(), in)

	if launcher.launchCount() != 0 {
		t.Error("should not launch in an unbound channel")
	}
	if !strings.Contains(strings.ToLower(poster.postsIn(originTS)), "no agent is configured for this channel") {
		t.Errorf("the unbound-channel notice must land in the origin thread, got: %s", poster.all())
	}
	if poster.firstTopLevelTS() != "" {
		t.Errorf("no session root should be posted for an unbound channel, got: %s", poster.postsIn(""))
	}
}

func TestOverlappingTurnsKeepBusyUntilLast(t *testing.T) {
	gate := make(chan struct{})
	broker := &fakeBroker{connected: map[string]bool{"github": true, "k8s": true}}
	launcher := &fakeLauncher{session: &fakeLiveSession{gate: gate}}
	poster := &fakePoster{}
	m := newManager(t, broker, boundResolver(deployTemplate()), launcher, poster)

	root := "1700000000.0001"
	m.HandleMention(context.Background(), gateway.MentionInvocation{
		UserID: "U1", ChannelID: "C1", TeamID: "T1",
		MessageTS: root, ThreadTS: root,
	})

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			m.HandleMessage(context.Background(), gateway.ThreadMessage{
				UserID: "U1", ChannelID: "C1", ThreadTS: root, Text: "go",
			})
		})
	}
	// Wait until both turns are blocked inside Prompt.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && launcher.liveSession().promptCount() < 2 {
		time.Sleep(2 * time.Millisecond)
	}
	if got := launcher.liveSession().promptCount(); got != 2 {
		t.Fatalf("expected 2 in-flight prompts, got %d", got)
	}
	if got := poster.countReaction("add", "waiting"); got != 1 {
		t.Errorf("busy reaction should be added once across overlapping turns, got %d", got)
	}
	if got := poster.countReaction("remove", "waiting"); got != 0 {
		t.Errorf("busy reaction must not be removed while a turn is still running, got %d removes", got)
	}

	close(gate)
	wg.Wait()
	if got := poster.countReaction("remove", "waiting"); got != 1 {
		t.Errorf("busy reaction should be removed exactly once when the last turn ends, got %d", got)
	}
}
