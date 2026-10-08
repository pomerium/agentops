package gateway

import (
	"cmp"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"

	"github.com/pomerium/agentops/slackbot/internal/telemetry"
)

type MentionInvocation struct {
	TeamID         string
	UserID         string
	ChannelID      string
	MessageTS      string
	ThreadTS       string
	OriginThreadTS string
	Text           string
	Prompt         string
}

type ThreadMessage struct {
	TeamID    string
	UserID    string
	ChannelID string
	ThreadTS  string
	MessageTS string
	Text      string
}

type Interaction struct {
	TeamID      string
	UserID      string
	ChannelID   string
	ThreadTS    string
	ActionID    string
	Value       string
	ResponseURL string
}

type App interface {
	HandleMention(ctx context.Context, in MentionInvocation)
	HandleMessage(ctx context.Context, in ThreadMessage)
	HandleInteraction(ctx context.Context, in Interaction)
}

type Option func(*Server)

func WithBotUserID(id string) Option { return func(s *Server) { s.botUserID = id } }

func WithLogger(l *slog.Logger) Option { return func(s *Server) { s.log = l } }

type Server struct {
	signingSecret string
	botUserID     string
	app           App
	log           *slog.Logger
	tel           *telemetry.Component
	inflight      sync.WaitGroup
	seen          seenEvents
}

func New(signingSecret string, app App, opts ...Option) *Server {
	s := &Server{signingSecret: signingSecret, app: app, log: slog.Default()}
	for _, opt := range opts {
		opt(s)
	}
	s.tel = telemetry.New(s.log, "gateway", slog.LevelDebug)
	return s
}

func (s *Server) dispatch(action func()) {
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		action()
	}()
}

func (s *Server) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /slack/events", s.handleEvents)
	mux.HandleFunc("POST /slack/interactivity", s.handleInteractivity)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	return s.logging(mux)
}

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

const maxBodyBytes = 1 << 20

func (s *Server) verifiedBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		s.tel.Warn(r.Context(), "request body too large or unreadable", "path", r.URL.Path, "err", err)
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	verifier, err := slack.NewSecretsVerifier(r.Header, s.signingSecret)
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

	if event.Type == slackevents.CallbackEvent {
		if id := eventID(event); id != "" && !s.seen.first(id, time.Now()) {
			s.tel.Debug(r.Context(), "ignoring an event already received", "event_id", id,
				"retry_num", r.Header.Get("X-Slack-Retry-Num"), "reason", r.Header.Get("X-Slack-Retry-Reason"))
		} else {
			s.dispatchCallback(r.Context(), event)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func eventID(event slackevents.EventsAPIEvent) string {
	if cb, ok := event.Data.(*slackevents.EventsAPICallbackEvent); ok {
		return cb.EventID
	}
	return ""
}

const seenEventsFor = time.Hour

type seenEvents struct {
	mu sync.Mutex
	at map[string]time.Time
}

func (s *seenEvents) first(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.at == nil {
		s.at = map[string]time.Time{}
	}
	for k, at := range s.at {
		if now.Sub(at) > seenEventsFor {
			delete(s.at, k)
		}
	}
	if _, seen := s.at[id]; seen {
		return false
	}
	s.at[id] = now
	return true
}

func (s *Server) dispatchCallback(ctx context.Context, event slackevents.EventsAPIEvent) {
	switch data := event.InnerEvent.Data.(type) {
	case *slackevents.MessageEvent:
		s.handleMessageEvent(ctx, event, data)
	default:
		s.tel.Debug(ctx, "event ignored: unhandled inner type", "inner_type", event.InnerEvent.Type)
	}
}

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
			TeamID:         actorTeam(event, msg),
			UserID:         msg.User,
			ChannelID:      msg.Channel,
			MessageTS:      msg.TimeStamp,
			ThreadTS:       msg.TimeStamp,
			OriginThreadTS: msg.ThreadTimeStamp,
			Text:           msg.Text,
			Prompt:         ParseMention(msg.Text, s.botUserID),
		}
		if msg.ThreadTimeStamp != "" {
			in.ThreadTS = msg.ThreadTimeStamp
		}
		s.tel.Debug(ctx, "dispatching app mention", "user", in.UserID, "channel", in.ChannelID,
			"thread_ts", in.ThreadTS, "origin_thread_ts", in.OriginThreadTS)
		s.dispatch(func() { s.app.HandleMention(context.WithoutCancel(ctx), in) })
		return
	}

	if msg.ThreadTimeStamp != "" {
		tm := ThreadMessage{
			TeamID:    actorTeam(event, msg),
			UserID:    msg.User,
			ChannelID: msg.Channel,
			ThreadTS:  msg.ThreadTimeStamp,
			MessageTS: msg.TimeStamp,
			Text:      msg.Text,
		}
		s.tel.Debug(ctx, "dispatching thread message",
			"user", tm.UserID, "channel", tm.ChannelID, "thread_ts", tm.ThreadTS)
		s.dispatch(func() { s.app.HandleMessage(context.WithoutCancel(ctx), tm) })
		return
	}

	s.tel.Debug(ctx, "message ignored: top-level and no bot mention", "channel", msg.Channel, "ts", msg.TimeStamp)
}

func actorTeam(event slackevents.EventsAPIEvent, msg *slackevents.MessageEvent) string {
	return cmp.Or(msg.UserTeam, event.TeamID)
}

func interactionTeam(cb slack.InteractionCallback) string {
	return cmp.Or(cb.User.TeamID, cb.Team.ID)
}

func (s *Server) mentionsBot(text string) bool {
	id := s.botUserID
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
	form, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var cb slack.InteractionCallback
	if err := json.Unmarshal([]byte(form.Get("payload")), &cb); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	for _, action := range cb.ActionCallback.BlockActions {
		in := Interaction{
			TeamID:      interactionTeam(cb),
			UserID:      cb.User.ID,
			ChannelID:   cb.Channel.ID,
			ThreadTS:    cb.Message.ThreadTimestamp,
			ActionID:    action.ActionID,
			Value:       action.Value,
			ResponseURL: cb.ResponseURL,
		}
		s.dispatch(func() { s.app.HandleInteraction(context.WithoutCancel(r.Context()), in) })
	}
	w.WriteHeader(http.StatusOK)
}
