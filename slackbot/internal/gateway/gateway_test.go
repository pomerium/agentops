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
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/slackbot/internal/gateway"
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
	mu           sync.Mutex
	mentions     []gateway.MentionInvocation
	messages     []gateway.ThreadMessage
	interactions []gateway.Interaction
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
	return gateway.New(testSecret, app, gateway.WithBotUserID(testBotUserID))
}

func TestParseMention(t *testing.T) {
	cases := []struct{ in, bot, prompt string }{
		{"<@U0BOT> deploy prod now", "U0BOT", "deploy prod now"},
		{"<@U0BOT|the-bot> review", "U0BOT", "review"},
		{"  <@U0BOT>   hello   ", "U0BOT", "hello"},
		{"hey <@U0BOT> do it", "U0BOT", "do it"},
		{"<@U0BOT>", "U0BOT", ""},
		{"<@U0BOT> stuff", "", "stuff"},
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
	srv := gateway.New(testSecret, app, gateway.WithLogger(logger))

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
	req.Header.Set("X-Slack-Signature", "v0=deadbeef")
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
	big := make([]byte, (1<<20)+1024)
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

func TestActionValuesSurviveSlack(t *testing.T) {
	values := map[string]string{
		"permission": gateway.EncodePermissionValue("sess-1", "call-2", "allow"),
	}
	for name, v := range values {
		if v == "" {
			t.Errorf("%s: encoded to the empty string", name)
		}
		for i, r := range v {
			if r < 0x20 || r == 0x7f {
				t.Errorf("%s: value %q has a control character %#U at %d; Slack strips these and the decode then fails",
					name, v, r, i)
			}
		}
	}

	mangled := "sess-1call-2allow"
	if _, _, _, ok := gateway.DecodePermissionValue(mangled); ok {
		t.Errorf("DecodePermissionValue(%q) accepted a separator-stripped value", mangled)
	}
}

func TestAgentMessageBlocksSplitsLongText(t *testing.T) {
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
	blocks := gateway.PermissionBlocks("sess-1", "call-2", "U1", "Edit config", []gateway.PermissionChoice{
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
	section, ok := blocks[0].(*slack.SectionBlock)
	if !ok || section.Text == nil {
		t.Fatalf("first block is not a section block: %T", blocks[0])
	}
	if !strings.Contains(section.Text.Text, "<@U1>, the agent needs your permission") {
		t.Errorf("permission prompt should name the owner: %s", section.Text.Text)
	}
}

func (f *fakeApp) waitInteraction(t *testing.T) gateway.Interaction {
	t.Helper()
	for range 100 {
		f.mu.Lock()
		if len(f.interactions) > 0 {
			in := f.interactions[0]
			f.mu.Unlock()
			return in
		}
		f.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the interaction to be dispatched")
	return gateway.Interaction{}
}

func TestInteractivityDispatchesAPermissionClick(t *testing.T) {
	app := &fakeApp{}
	srv := newServer(app)

	value := gateway.EncodePermissionValue("sess-1", "call-2", "allow")
	payload := fmt.Sprintf(`{
      "type": "block_actions",
      "user": {"id": "U0456GHIJKL", "username": "alice", "team_id": "T0789MNOPQR"},
      "team": {"id": "T0789MNOPQR"},
      "channel": {"id": "C0123ABCDEF", "name": "agentops-test"},
      "container": {"type": "message", "message_ts": "1785246742.001300",
                    "channel_id": "C0123ABCDEF", "is_ephemeral": false,
                    "thread_ts": "1785246201.347149"},
      "message": {"type": "message", "ts": "1785246742.001300",
                  "thread_ts": "1785246201.347149"},
      "response_url": "https://hooks.slack.com/actions/T1/1/abc",
      "actions": [{"action_id": %q, "block_id": "acp_permission_actions",
                   "text": {"type": "plain_text", "text": "Allow once"},
                   "value": %q, "style": "primary", "type": "button",
                   "action_ts": "1785246784.030897"}]
    }`, gateway.ActionPermission, value)

	form := "payload=" + url.QueryEscape(payload)
	req := signedRequest(t, "/slack/interactivity", "application/x-www-form-urlencoded", []byte(form))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	in := app.waitInteraction(t)
	if in.ActionID != gateway.ActionPermission {
		t.Errorf("ActionID = %q, want %q", in.ActionID, gateway.ActionPermission)
	}
	if in.UserID != "U0456GHIJKL" || in.TeamID != "T0789MNOPQR" {
		t.Errorf("actor = %q/%q, want U0456GHIJKL/T0789MNOPQR", in.UserID, in.TeamID)
	}
	if in.ChannelID != "C0123ABCDEF" || in.ThreadTS != "1785246201.347149" {
		t.Errorf("thread = %q/%q, want C0123ABCDEF/1785246201.347149", in.ChannelID, in.ThreadTS)
	}
	if in.ResponseURL == "" {
		t.Error("ResponseURL is the only way to answer a click; it must survive")
	}
	sessionID, requestID, optionID, ok := gateway.DecodePermissionValue(in.Value)
	if !ok {
		t.Fatalf("the value Slack echoed back did not decode: %q", in.Value)
	}
	if sessionID != "sess-1" || requestID != "call-2" || optionID != "allow" {
		t.Errorf("decoded = %q/%q/%q", sessionID, requestID, optionID)
	}
}

func TestAnInteractionPayloadOutsideTheSignedBodyIsRejected(t *testing.T) {
	payload := `{"type":"block_actions","user":{"id":"UOWNER","team_id":"T1"},"team":{"id":"T1"},` +
		`"channel":{"id":"C1"},"message":{"thread_ts":"1.0"},` +
		`"actions":[{"action_id":"acp_permission","value":"forged"}]}`
	for _, tc := range []struct {
		contentType string
		body        string
	}{
		{"application/json", `{}`},
		{"application/x-www-form-urlencoded", `other=1`},
	} {
		t.Run(tc.contentType, func(t *testing.T) {
			app := &fakeApp{}
			req := signedRequest(t, "/slack/interactivity?payload="+url.QueryEscape(payload), tc.contentType, []byte(tc.body))
			rec := httptest.NewRecorder()
			newServer(app).Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 for a payload Slack did not sign", rec.Code)
			}
			time.Sleep(20 * time.Millisecond)
			app.mu.Lock()
			defer app.mu.Unlock()
			if len(app.interactions) != 0 {
				t.Fatalf("an unsigned payload was dispatched: %+v", app.interactions)
			}
		})
	}
}

func TestALongPermissionTitleFitsTheSection(t *testing.T) {
	for _, owner := range []string{"", "U0123456789ABCDEF"} {
		blocks := gateway.PermissionBlocks("sess-1", "call-2", owner, strings.Repeat("x", 3000),
			[]gateway.PermissionChoice{{OptionID: "allow", Name: "Allow", Kind: "allow_once"}})
		section, ok := blocks[0].(*slack.SectionBlock)
		if !ok || section.Text == nil {
			t.Fatalf("first block is not a section block: %T", blocks[0])
		}
		if n := utf8.RuneCountInString(section.Text.Text); n > 3000 {
			t.Errorf("owner %q: the permission section has %d characters; Slack rejects more than 3000", owner, n)
		}
		if !strings.Contains(section.Text.Text, "xxx…*") {
			t.Errorf("owner %q: a cut title must say it was cut: %.80s", owner, section.Text.Text)
		}
	}
}

func TestALongPermissionOptionNameFitsItsButton(t *testing.T) {
	blocks := gateway.PermissionBlocks("sess-1", "call-2", "U1", "Edit config",
		[]gateway.PermissionChoice{{OptionID: "allow", Name: strings.Repeat("x", 76), Kind: "allow_once"}})
	actions, ok := blocks[1].(*slack.ActionBlock)
	if !ok {
		t.Fatalf("second block is not an action block: %T", blocks[1])
	}
	button, ok := actions.Elements.ElementSet[0].(*slack.ButtonBlockElement)
	if !ok {
		t.Fatalf("first element is not a button: %T", actions.Elements.ElementSet[0])
	}
	if n := utf8.RuneCountInString(button.Text.Text); n > 75 {
		t.Fatalf("the button label has %d characters; Slack rejects more than 75", n)
	}
}
