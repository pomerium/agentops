// Package gateway is the HTTP gateway for agentops. It serves the
// plain-HTTP Slack endpoints (Events API — app mentions and thread replies — and
// interactivity) and the OAuth redirect callback, verifies every Slack request
// against the signing secret, parses each payload, and delegates the work to an
// App implementation.
// It also holds the Block Kit rendering helpers used to talk back to Slack.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"

	"github.com/pomerium/agentops/internal/telemetry"
)

// MentionInvocation is a parsed @mention of the bot in a channel. The channel
// determines which agent template runs; the whole text after the mention is
// the initial prompt (which may be empty).
type MentionInvocation struct {
	TeamID    string
	UserID    string
	ChannelID string
	// MessageTS is the timestamp of the mention message itself.
	MessageTS string
	// ThreadTS is the thread the mention was posted in: equal to MessageTS for
	// a top-level mention (the bot threads its work under the user's message),
	// or the surrounding thread's root for a mention posted inside one.
	ThreadTS string
	// OriginThreadTS is non-empty when the mention was posted inside an
	// existing thread (it equals that thread's root ts). The app decides
	// whether that thread is one the bot already drives (run a turn) or a
	// foreign discussion to loop the bot into.
	OriginThreadTS string
	// Text is the raw message text including the bot mention. It is used
	// verbatim as the turn prompt when the mention lands in a thread the bot
	// already drives, matching how plain thread replies are forwarded.
	Text string
	// Prompt is Text with the bot mention stripped.
	Prompt string
}

// ThreadMessage is a user message posted in a thread.
type ThreadMessage struct {
	TeamID    string
	UserID    string
	ChannelID string
	ThreadTS  string
	Text      string
}

// Interaction is a Block Kit interactive action (e.g. a permission button).
type Interaction struct {
	TeamID    string
	UserID    string
	ChannelID string
	ThreadTS  string
	ActionID  string
	Value     string
	// ResponseURL edits the message the action came from (e.g. the ephemeral
	// connect prompt). Empty for actions Slack doesn't provide one for.
	ResponseURL string
}

// App is the application logic the gateway delegates to. The Handle* methods are
// invoked asynchronously (the gateway has already acked Slack) and must not
// block on long-running work the caller awaits; OAuthCallback is synchronous so
// its error can be reflected in the browser response.
type App interface {
	HandleMention(ctx context.Context, in MentionInvocation)
	HandleMessage(ctx context.Context, in ThreadMessage)
	HandleInteraction(ctx context.Context, in Interaction)
	OAuthCallback(ctx context.Context, code, state string) error
	// ConnectRedirect resolves a short connect link's flow id to the provider
	// authorization URL the user's browser should be redirected to.
	ConnectRedirect(ctx context.Context, flowID string) (string, error)
}

// Config configures the gateway.
type Config struct {
	SigningSecret string
	// BotUserID is the bot's own Slack user id, used to strip its mention from
	// app_mention text. Empty is tolerated (a leading mention is stripped instead).
	BotUserID string
}

// Server is the HTTP gateway.
type Server struct {
	cfg Config
	app App
	log *slog.Logger
	tel *telemetry.Component
}

// New constructs a Server.
func New(cfg Config, app App, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, app: app, log: log, tel: telemetry.New(log, "gateway", slog.LevelDebug)}
}

// Handler returns the HTTP handler exposing all gateway routes, wrapped with
// request logging.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /slack/events", s.handleEvents)
	mux.HandleFunc("POST /slack/interactivity", s.handleInteractivity)
	mux.HandleFunc("GET /oauth/callback", s.handleOAuthCallback)
	mux.HandleFunc("GET /connect/{id}", s.handleConnect)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	return s.logging(mux)
}

// statusRecorder captures the response status and byte count for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// logging logs one structured line per HTTP request (method, path, status,
// duration). Health checks log at debug to avoid noise.
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		level := slog.LevelInfo
		if r.URL.Path == "/healthz" {
			level = slog.LevelDebug
		}
		s.tel.Logger(r.Context()).Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"bytes", rec.bytes, "duration_ms", time.Since(start).Milliseconds())
	})
}

// maxBodyBytes bounds the size of an inbound Slack request body to prevent a
// memory-exhaustion DoS.
const maxBodyBytes = 1 << 20 // 1 MiB

// verifiedBody reads and signature-verifies the request body. On failure it
// writes the appropriate status and returns ok=false.
func (s *Server) verifiedBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		s.tel.Warn(r.Context(), "request body too large or unreadable", "path", r.URL.Path, "err", err)
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	verifier, err := slack.NewSecretsVerifier(r.Header, s.cfg.SigningSecret)
	if err != nil {
		s.tel.Warn(r.Context(), "signature verifier init failed", "path", r.URL.Path, "err", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	if _, err := verifier.Write(body); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	if err := verifier.Ensure(); err != nil {
		s.tel.Warn(r.Context(), "slack signature verification failed", "path", r.URL.Path)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	return body, true
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	body, ok := s.verifiedBody(w, r)
	if !ok {
		return
	}
	event, err := slackevents.ParseEvent(body, slackevents.OptionNoVerifyToken())
	if err != nil {
		s.tel.Warn(r.Context(), "event parse failed", "err", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if event.Type == slackevents.URLVerification {
		var ch slackevents.ChallengeResponse
		if err := json.Unmarshal(body, &ch); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, ch.Challenge)
		return
	}

	// Slack redelivers events it thinks failed. Since we ack quickly and do the
	// real work asynchronously, treat any retry as a duplicate and skip
	// re-dispatch to avoid running a turn (or launching a sandbox) twice.
	if retry := r.Header.Get("X-Slack-Retry-Num"); event.Type == slackevents.CallbackEvent && retry == "" {
		s.dispatchCallback(r.Context(), event)
	} else if retry != "" {
		s.tel.Debug(r.Context(), "ignoring retried event", "retry_num", retry, "reason", r.Header.Get("X-Slack-Retry-Reason"))
	}
	w.WriteHeader(http.StatusOK)
}

// dispatchCallback routes a (non-retried) event_callback to the right App
// method. Both app mentions (which start a session) and thread replies (which
// drive the next turn) arrive as message events — Slack delivers a channel
// @mention as a plain message containing the bot's <@id>, and an app needs the
// message events for thread replies regardless. (A separate app_mention
// subscription, if configured, would duplicate the same message, so it is
// intentionally ignored here.) Everything runs asynchronously; Slack is already
// acked.
func (s *Server) dispatchCallback(ctx context.Context, event slackevents.EventsAPIEvent) {
	switch data := event.InnerEvent.Data.(type) {
	case *slackevents.MessageEvent:
		s.handleMessageEvent(ctx, event, data)
	default:
		s.tel.Debug(ctx, "event ignored: unhandled inner type", "inner_type", event.InnerEvent.Type)
	}
}

// handleMessageEvent routes a message event: a message that @mentions the bot
// dispatches as a mention (a top-level one starts a session under itself; a
// threaded one loops the bot into that discussion — the app decides); a
// mention-less threaded reply drives the next turn of that thread's session.
// Bot/self and subtyped messages are dropped — except thread_broadcast
// ("also send to channel" replies), which are normal user messages. Each drop
// is logged at debug so a "no response" can be traced to the exact filter.
func (s *Server) handleMessageEvent(ctx context.Context, event slackevents.EventsAPIEvent, msg *slackevents.MessageEvent) {
	switch {
	case msg.BotID != "":
		s.tel.Debug(ctx, "message ignored: from a bot", "bot_id", msg.BotID)
		return
	case msg.SubType != "" && msg.SubType != "thread_broadcast":
		s.tel.Debug(ctx, "message ignored: has subtype", "subtype", msg.SubType)
		return
	}

	if s.mentionsBot(msg.Text) {
		in := MentionInvocation{
			TeamID:         event.TeamID,
			UserID:         msg.User,
			ChannelID:      msg.Channel,
			MessageTS:      msg.TimeStamp,
			ThreadTS:       msg.TimeStamp,
			OriginThreadTS: msg.ThreadTimeStamp,
			Text:           msg.Text,
			Prompt:         ParseMention(msg.Text, s.cfg.BotUserID),
		}
		if msg.ThreadTimeStamp != "" {
			in.ThreadTS = msg.ThreadTimeStamp
		}
		s.tel.Debug(ctx, "dispatching app mention", "user", in.UserID, "channel", in.ChannelID,
			"thread_ts", in.ThreadTS, "origin_thread_ts", in.OriginThreadTS)
		go s.app.HandleMention(context.WithoutCancel(ctx), in)
		return
	}

	// A mention-less reply inside a thread drives the next turn of that
	// thread's session.
	if msg.ThreadTimeStamp != "" {
		tm := ThreadMessage{
			TeamID:    event.TeamID,
			UserID:    msg.User,
			ChannelID: msg.Channel,
			ThreadTS:  msg.ThreadTimeStamp,
			Text:      msg.Text,
		}
		s.tel.Debug(ctx, "dispatching thread message",
			"user", tm.UserID, "channel", tm.ChannelID, "thread_ts", tm.ThreadTS)
		go s.app.HandleMessage(context.WithoutCancel(ctx), tm)
		return
	}

	s.tel.Debug(ctx, "message ignored: top-level and no bot mention", "channel", msg.Channel, "ts", msg.TimeStamp)
}

// mentionsBot reports whether text mentions the bot's user id (as <@ID> or
// <@ID|label>). Requires a known bot user id; without one (auth.test failed at
// startup) top-level messages can't be reliably attributed and are not treated
// as mentions.
func (s *Server) mentionsBot(text string) bool {
	id := s.cfg.BotUserID
	if id == "" {
		return false
	}
	return strings.Contains(text, "<@"+id+">") || strings.Contains(text, "<@"+id+"|")
}

func (s *Server) handleInteractivity(w http.ResponseWriter, r *http.Request) {
	body, ok := s.verifiedBody(w, r)
	if !ok {
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var cb slack.InteractionCallback
	if err := json.Unmarshal([]byte(r.FormValue("payload")), &cb); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	for _, action := range cb.ActionCallback.BlockActions {
		in := Interaction{
			TeamID:      cb.Team.ID,
			UserID:      cb.User.ID,
			ChannelID:   cb.Channel.ID,
			ThreadTS:    cb.Message.ThreadTimestamp,
			ActionID:    action.ActionID,
			Value:       action.Value,
			ResponseURL: cb.ResponseURL,
		}
		go s.app.HandleInteraction(context.WithoutCancel(r.Context()), in)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if errMsg := q.Get("error"); errMsg != "" {
		s.renderCallbackPage(w, http.StatusBadRequest, "Authorization failed: "+errMsg)
		return
	}
	code, state := q.Get("code"), q.Get("state")
	if code == "" || state == "" {
		s.renderCallbackPage(w, http.StatusBadRequest, "Missing code or state.")
		return
	}
	if err := s.app.OAuthCallback(r.Context(), code, state); err != nil {
		s.log.ErrorContext(r.Context(), "oauth callback failed", "err", err)
		s.renderCallbackPage(w, http.StatusBadRequest, "Could not complete authorization. You can close this window and try again from Slack.")
		return
	}
	s.renderCallbackPage(w, http.StatusOK, "Connected! You can close this window and return to Slack.")
}

// handleConnect resolves a short /connect/{id} link to the provider authorization
// URL and 302-redirects the user's browser to it. This keeps the long OAuth URL
// (with PKCE/state/client_id) out of the Slack button's hover tooltip.
func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	url, err := s.app.ConnectRedirect(r.Context(), id)
	if err != nil {
		s.tel.Warn(r.Context(), "connect link resolution failed", "err", err)
		s.renderCallbackPage(w, http.StatusBadRequest,
			"This connection link has expired or is invalid. Please re-run the command in Slack.")
		return
	}
	http.Redirect(w, r, url, http.StatusFound)
}

func (s *Server) renderCallbackPage(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, "<!doctype html><html><body style=\"font-family:sans-serif;padding:2rem\"><p>"+message+"</p></body></html>")
}
