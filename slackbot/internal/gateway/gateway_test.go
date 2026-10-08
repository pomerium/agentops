package gateway_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/internal/channels/slack/gateway"
)

const testSecret = "8f742231b10e8888abcd99yyyzzz85a5"

func sign(ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(testSecret))
	fmt.Fprintf(mac, "v0:%s:%s", ts, body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func signedRequest(t *testing.T, path, contentType string, body []byte) *http.Request {
	t.Helper()
	ts := fmt.Sprintf("%d", time.Now().Unix())
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", sign(ts, body))
	return req
}

type fakeApp struct {
	mu             sync.Mutex
	mentions       []gateway.MentionInvocation
	messages       []gateway.ThreadMessage
	interactions   []gateway.Interaction
	callbackErr    error
	gotCallback    [2]string
	connectURL     string
	connectErr     error
	gotConnectFlow string
}

func (f *fakeApp) HandleMention(_ context.Context, in gateway.MentionInvocation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mentions = append(f.mentions, in)
}
func (f *fakeApp) HandleMessage(_ context.Context, in gateway.ThreadMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, in)
}
func (f *fakeApp) HandleInteraction(_ context.Context, in gateway.Interaction) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.interactions = append(f.interactions, in)
}
func (f *fakeApp) OAuthCallback(_ context.Context, code, state string) error {
	f.gotCallback = [2]string{code, state}
	return f.callbackErr
}
func (f *fakeApp) ConnectRedirect(_ context.Context, flowID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotConnectFlow = flowID
	return f.connectURL, f.connectErr
}

func (f *fakeApp) waitMention(t *testing.T) gateway.MentionInvocation {
	t.Helper()
	for i := 0; i < 100; i++ {
		f.mu.Lock()
		n := len(f.mentions)
		if n > 0 {
			s := f.mentions[0]
			f.mu.Unlock()
			return s
		}
		f.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for mention invocation")
	return gateway.MentionInvocation{}
}

const testBotUserID = "U0BOT"

func newServer(app gateway.App) *gateway.Server {
	return gateway.New(gateway.Config{SigningSecret: testSecret, BotUserID: testBotUserID}, app, nil)
}

func TestParseMention(t *testing.T) {
	cases := []struct{ in, bot, prompt string }{
		{"<@U0BOT> deploy prod now", "U0BOT", "deploy prod now"},
		{"<@U0BOT|the-bot> review", "U0BOT", "review"},
		{"  <@U0BOT>   hello   ", "U0BOT", "hello"},
		{"hey <@U0BOT> do it", "U0BOT", "do it"},
		{"<@U0BOT>", "U0BOT", ""},
		{"<@U0BOT> stuff", "", "stuff"}, // unknown bot id: strip leading mention
	}
	for _, c := range cases {
		if prompt := gateway.ParseMention(c.in, c.bot); prompt != c.prompt {
			t.Errorf("ParseMention(%q, %q) = %q, want %q", c.in, c.bot, prompt, c.prompt)
		}
	}
}

func TestMentionInMessageStartsSession(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)

	// Slack delivers a channel @mention as a plain message event whose text
	// contains the bot's <@id> (there is no separate app_mention subscription).
	inner := `{"type":"event_callback","team_id":"T1","event":{"type":"message","user":"U1","channel":"C1","text":"<@U0BOT> deploy-service ship it","ts":"1700000000.0001"}}`
	req := signedRequest(t, "/slack/events", "application/json", []byte(inner))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	got := app.waitMention(t)
	if got.Prompt != "deploy-service ship it" {
		t.Errorf("mention parse wrong: %+v", got)
	}
	if got.UserID != "U1" || got.ChannelID != "C1" || got.MessageTS != "1700000000.0001" || got.ThreadTS != "1700000000.0001" {
		t.Errorf("mention fields wrong: %+v", got)
	}
}

func TestTopLevelMessageWithoutMentionIgnored(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)

	inner := `{"type":"event_callback","team_id":"T1","event":{"type":"message","user":"U1","channel":"C1","text":"just chatting, no bot here","ts":"1700000000.0002"}}`
	req := signedRequest(t, "/slack/events", "application/json", []byte(inner))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	time.Sleep(20 * time.Millisecond)
	app.mu.Lock()
	n := len(app.mentions)
	app.mu.Unlock()
	if n != 0 {
		t.Errorf("a top-level message without a bot mention must not start a session; mentions=%d", n)
	}
}

func TestThreadedMentionDispatchesMention(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)

	// A mention inside an existing thread loops the bot in: it dispatches as a
	// mention (carrying the origin thread), not as a plain thread reply.
	inner := `{"type":"event_callback","team_id":"T1","event":{"type":"message","user":"U1","channel":"C1","text":"<@U0BOT> sre what do you think?","ts":"1700000000.0042","thread_ts":"1700000000.0001"}}`
	req := signedRequest(t, "/slack/events", "application/json", []byte(inner))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	got := app.waitMention(t)
	if got.Prompt != "sre what do you think?" {
		t.Errorf("mention parse wrong: %+v", got)
	}
	if got.OriginThreadTS != "1700000000.0001" || got.MessageTS != "1700000000.0042" {
		t.Errorf("origin thread fields wrong: %+v", got)
	}
	if got.Text != "<@U0BOT> sre what do you think?" {
		t.Errorf("raw text not carried: %+v", got)
	}
	app.mu.Lock()
	n := len(app.messages)
	app.mu.Unlock()
	if n != 0 {
		t.Errorf("a threaded mention must not also dispatch as a thread message; messages=%d", n)
	}
}

func TestThreadBroadcastMentionDispatchesMention(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)

	// An "also send to channel" reply carries subtype thread_broadcast but is a
	// normal user message; a bot mention in one must still loop the bot in.
	inner := `{"type":"event_callback","team_id":"T1","event":{"type":"message","subtype":"thread_broadcast","user":"U1","channel":"C1","text":"<@U0BOT> sre help here","ts":"1700000000.0043","thread_ts":"1700000000.0001"}}`
	req := signedRequest(t, "/slack/events", "application/json", []byte(inner))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	got := app.waitMention(t)
	if got.Prompt != "sre help here" || got.OriginThreadTS != "1700000000.0001" {
		t.Errorf("broadcast mention fields wrong: %+v", got)
	}
}

func TestThreadReplyDispatchesMessage(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)

	// A reply inside a thread (no mention needed) drives the next turn.
	inner := `{"type":"event_callback","team_id":"T1","event":{"type":"message","user":"U1","channel":"C1","text":"status?","ts":"2.0","thread_ts":"1700000000.0001"}}`
	req := signedRequest(t, "/slack/events", "application/json", []byte(inner))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		app.mu.Lock()
		n := len(app.messages)
		app.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if len(app.messages) != 1 || app.messages[0].ThreadTS != "1700000000.0001" || app.messages[0].Text != "status?" {
		t.Errorf("expected thread reply dispatched as a message turn, got %+v", app.messages)
	}
	if len(app.mentions) != 0 {
		t.Errorf("a mention-less thread reply must not dispatch as a mention; got %+v", app.mentions)
	}
}

func TestHTTPRequestLogged(t *testing.T) {
	app := &fakeApp{}
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv := gateway.New(gateway.Config{SigningSecret: testSecret}, app, logger)

	body := []byte(`{"type":"event_callback","event":{"type":"app_mention","user":"U1","channel":"C1","text":"<@U0BOT> hello","ts":"1.0"}}`)
	req := signedRequest(t, "/slack/events", "application/json", body)
	srv.Handler().ServeHTTP(httptest.NewRecorder(), req)

	if !strings.Contains(buf.String(), `"msg":"http request"`) ||
		!strings.Contains(buf.String(), `"path":"/slack/events"`) ||
		!strings.Contains(buf.String(), `"status":200`) {
		t.Errorf("expected an http request log with path+status; got: %s", buf.String())
	}
}

func TestSignatureRejected(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)
	body := []byte(`{"type":"event_callback","event":{"type":"app_mention","text":"x"}}`)
	req := signedRequest(t, "/slack/events", "application/json", body)
	req.Header.Set("X-Slack-Signature", "v0=deadbeef") // tamper
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("tampered signature: status = %d, want 401", rec.Code)
	}
}

func TestEventURLVerification(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)
	payload := map[string]string{"type": "url_verification", "challenge": "chal-123"}
	body, _ := json.Marshal(payload)
	req := signedRequest(t, "/slack/events", "application/json", body)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "chal-123") {
		t.Errorf("expected challenge echoed, got %q", rec.Body.String())
	}
}

func TestEventRetryIsNotRedispatched(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)
	inner := `{"type":"event_callback","team_id":"T1","event":{"type":"message","user":"U1","channel":"C1","ts":"2.0","thread_ts":"1.0","text":"hi"}}`
	body := []byte(inner)
	req := signedRequest(t, "/slack/events", "application/json", body)
	req.Header.Set("X-Slack-Retry-Num", "1")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	time.Sleep(20 * time.Millisecond)
	app.mu.Lock()
	n := len(app.messages)
	app.mu.Unlock()
	if n != 0 {
		t.Errorf("a retried event must not be re-dispatched; got %d messages", n)
	}
}

func TestOversizedBodyRejected(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)
	big := make([]byte, (1<<20)+1024) // > 1 MiB
	for i := range big {
		big[i] = 'a'
	}
	req := signedRequest(t, "/slack/events", "application/json", big)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: status = %d, want 413", rec.Code)
	}
}

func TestOAuthCallbackRoutes(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)
	req := httptest.NewRequest(http.MethodGet, "/oauth/callback?code=abc&state=xyz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if app.gotCallback[0] != "abc" || app.gotCallback[1] != "xyz" {
		t.Errorf("callback args = %v", app.gotCallback)
	}
}

func TestConnectRedirectRoutes(t *testing.T) {
	app := &fakeApp{connectURL: "https://provider.example/authorize?client_id=x&state=y"}
	srv := newServer(app)
	req := httptest.NewRequest(http.MethodGet, "/connect/flow-123", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != app.connectURL {
		t.Errorf("Location = %q, want %q", got, app.connectURL)
	}
	if app.gotConnectFlow != "flow-123" {
		t.Errorf("flow id = %q, want flow-123", app.gotConnectFlow)
	}
}

func TestConnectRedirectExpired(t *testing.T) {
	app := &fakeApp{connectErr: fmt.Errorf("expired")}
	srv := newServer(app)
	req := httptest.NewRequest(http.MethodGet, "/connect/nope", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for unresolvable connect link", rec.Code)
	}
}

func TestPermissionValueRoundTrip(t *testing.T) {
	v := gateway.EncodePermissionValue("sess-1", "call-2", "allow")
	sessionID, toolCallID, optionID, ok := gateway.DecodePermissionValue(v)
	if !ok {
		t.Fatalf("decode failed for %q", v)
	}
	if sessionID != "sess-1" || toolCallID != "call-2" || optionID != "allow" {
		t.Errorf("round trip = (%q,%q,%q)", sessionID, toolCallID, optionID)
	}
}

func TestAuthPromptBlocksContainsLinks(t *testing.T) {
	blocks := gateway.AuthPromptBlocks("deploy-service", []gateway.AuthStatus{
		{ServerName: "github", URL: "https://app.example/connect/gh"},
		{ServerName: "k8s", Connected: true},
	})
	js, _ := json.Marshal(blocks)
	s := string(js)
	// The unconnected server keeps its connect link/button.
	for _, want := range []string{"github", "https://app.example/connect/gh", "deploy-service"} {
		if !strings.Contains(s, want) {
			t.Errorf("auth blocks missing %q in %s", want, s)
		}
	}
	// The connected server shows a checkmark and NO connect button.
	if !strings.Contains(s, "Connected *k8s*") {
		t.Errorf("expected connected checkmark row for k8s in %s", s)
	}
	if strings.Contains(s, "Connect k8s") {
		t.Errorf("connected server must not render a Connect button: %s", s)
	}
}

func TestAgentMessageBlocksSplitsLongText(t *testing.T) {
	// ~11k chars must split into multiple sections, each within Slack's 3000 cap.
	long := strings.Repeat("abcdefghij\n", 1000)
	blocks := gateway.AgentMessageBlocks(long)
	if len(blocks) < 2 {
		t.Fatalf("expected long text to split into multiple blocks, got %d", len(blocks))
	}
	for i, b := range blocks {
		sb, ok := b.(*slack.SectionBlock)
		if !ok || sb.Text == nil {
			t.Fatalf("block %d is not a section block: %T", i, b)
		}
		if n := len(sb.Text.Text); n > 3000 {
			t.Errorf("block %d section text is %d chars, exceeds Slack's 3000 limit", i, n)
		}
	}
}

func TestPermissionBlocksEncodeAction(t *testing.T) {
	blocks := gateway.PermissionBlocks("sess-1", "call-2", "Edit config", []gateway.PermissionChoice{
		{OptionID: "allow", Name: "Allow", Kind: "allow_once"},
		{OptionID: "deny", Name: "Deny", Kind: "reject_once"},
	})
	js, _ := json.Marshal(blocks)
	s := string(js)
	if !strings.Contains(s, gateway.ActionPermission) {
		t.Errorf("permission blocks missing action id %q", gateway.ActionPermission)
	}
	if !strings.Contains(s, "Edit config") || !strings.Contains(s, "Allow") {
		t.Errorf("permission blocks missing content: %s", s)
	}
}
