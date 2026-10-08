package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/slack-go/slack"

	pb "github.com/pomerium/agentops/harness/api/pb"
)

type sessionMeta struct {
	Version int `json:"v"`

	ChannelID         string `json:"channel_id"`
	ThreadTS          string `json:"thread_ts"`
	UserID            string `json:"user_id"`
	TeamID            string `json:"team_id,omitempty"`
	StatusMessageTS   string `json:"status_ts,omitempty"`
	ApprovalChannelID string `json:"approval_channel_id,omitempty"`
	ApprovalMessageTS string `json:"approval_ts,omitempty"`

	Multiplayer bool   `json:"multiplayer,omitempty"`
	LastSeenTS  string `json:"last_seen_ts,omitempty"`

	Watching bool  `json:"watching,omitempty"`
	LastSeq  int64 `json:"last_seq,omitempty"`

	AnswerTurn string `json:"answer_turn,omitempty"`
	AnswerTS   string `json:"answer_ts,omitempty"`

	PermissionPrompts map[string]string `json:"permission_prompts,omitempty"`

	statusText string
}

const metaVersion = 1

const stateKey = "state"

func statusMeta(sessionID string, m sessionMeta) slack.MsgOption {
	m.Version = metaVersion
	var state map[string]any
	if raw, err := json.Marshal(m); err == nil {
		_ = json.Unmarshal(raw, &state)
	}
	return slack.MsgOptionMetadata(slack.SlackMetadata{
		EventType:    chromeEventType,
		EventPayload: map[string]any{"session_id": sessionID, stateKey: state},
	})
}

func stateFrom(msg slack.Message, sessionID string) (sessionMeta, bool) {
	md := msg.Metadata
	if md.EventType != chromeEventType || md.EventPayload["session_id"] != sessionID {
		return sessionMeta{}, false
	}
	state, ok := md.EventPayload[stateKey]
	if !ok {
		return sessionMeta{}, false
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return sessionMeta{}, false
	}
	var m sessionMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return sessionMeta{}, false
	}
	if m.Version != metaVersion || m.ChannelID == "" || m.ThreadTS == "" {
		return sessionMeta{}, false
	}
	m.StatusMessageTS = msg.Timestamp
	m.statusText = msg.Text
	return m, true
}

func refThread(ref string) (channelID, threadTS, teamID, userID string, ok bool) {
	parts := strings.Split(ref, ":")
	if len(parts) != 5 || parts[0] != "slack" || parts[1] == "" || parts[2] == "" || parts[4] == "" {
		return "", "", "", "", false
	}
	return parts[1], parts[2], parts[3], parts[4], true
}

var errNoSlackState = errors.New("this session's thread carries no readable Slack state")

func (a *App) loadMeta(ctx context.Context, view *pb.SessionView) (sessionMeta, error) {
	channelID, threadTS, _, _, ok := refThread(view.GetConversationRef())
	if !ok {
		return sessionMeta{}, errNoSlackState
	}
	msgs, err := a.poster.ThreadReplies(ctx, channelID, threadTS, a.sessionStart(ctx, view.GetId()), maxThreadRead)
	if err != nil {
		return sessionMeta{}, fmt.Errorf("read the thread of session %s: %w", view.GetId(), err)
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		if m, ok := stateFrom(msgs[i], view.GetId()); ok {
			return m, nil
		}
	}
	return sessionMeta{}, errNoSlackState
}

func (a *App) sessionStart(ctx context.Context, sessionID string) string {
	res, err := a.api.ListEvents(ctx, &pb.ListEventsRequest{Ref: a.ref(sessionID), Limit: 1})
	if err != nil || len(res.GetEvents()) == 0 || res.GetEvents()[0].GetTimestamp() == nil {
		return ""
	}
	return slackTS(res.GetEvents()[0].GetTimestamp().AsTime().Add(-time.Minute))
}

func identityOf(view *pb.SessionView) (sessionMeta, bool) {
	channelID, threadTS, teamID, userID, ok := refThread(view.GetConversationRef())
	if !ok {
		return sessionMeta{}, false
	}
	return sessionMeta{ChannelID: channelID, ThreadTS: threadTS, TeamID: teamID, UserID: userID}, true
}

func (t *thread) meta() sessionMeta {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	return t.metaState
}

func (t *thread) applyMeta(edit func(m *sessionMeta)) sessionMeta {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	edit(&t.metaState)
	t.metaState.Version = metaVersion
	return t.metaState
}

func withPermissionPrompt(prompts map[string]string, requestID, ts string) map[string]string {
	out := maps.Clone(prompts)
	if ts == "" {
		delete(out, requestID)
		return out
	}
	if out == nil {
		out = map[string]string{}
	}
	out[requestID] = ts
	return out
}

const maxThreadRead = 3000

func (a *App) saveMeta(ctx context.Context, t *thread, edit func(m *sessionMeta)) {
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	m := t.applyMeta(edit)
	if t.sessionID == "" || m.StatusMessageTS == "" {
		return
	}
	if _, err := a.poster.UpdateMessage(ctx, t.channel, m.StatusMessageTS,
		statusMeta(t.sessionID, m), slack.MsgOptionText(m.statusText, false)); err != nil {
		a.log.ErrorContext(ctx, "could not record where this session lives in Slack; a restart will not find it",
			"session", t.sessionID, "err", err)
	}
}
