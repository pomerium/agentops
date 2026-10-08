package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	acp "github.com/coder/acp-go-sdk"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

var errResumeUnavailable = errors.New("this conversation cannot be resumed")

type openedSession struct {
	id        acp.SessionId
	resumable bool
}

func openSession(ctx context.Context, log *slog.Logger, conn *acp.ClientSideConnection, params *agentlinkpb.SessionParams) (openedSession, error) {
	log.Debug("acp -> initialize", "protocol_version", acp.ProtocolVersionNumber)
	initResp, err := conn.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion:    acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{},
	})
	if err != nil {
		return openedSession{}, fmt.Errorf("acp initialize: %w", err)
	}
	resumable := initResp.AgentCapabilities.SessionCapabilities.Resume != nil
	log.Debug("acp <- initialized", "protocol_version", initResp.ProtocolVersion, "resume_supported", resumable)

	servers := mcpServers(params.GetMcpServers())
	sessionID, configOptions, err := openOrResume(ctx, log, conn, params, servers, initResp)
	if err != nil {
		return openedSession{}, err
	}

	if _, err := conn.SetSessionMode(ctx, acp.SetSessionModeRequest{
		SessionId: sessionID,
		ModeId:    acp.SessionModeId("bypassPermissions"),
	}); err != nil {
		log.Warn("set session mode failed (continuing)", "mode", "bypassPermissions", "err", err)
	}

	cfgReqs, err := sessionConfigRequests(sessionID, configOptions, params.GetConfig())
	if err != nil {
		return openedSession{}, fmt.Errorf("acp session config: %w", err)
	}
	for _, req := range cfgReqs {
		id := configRequestID(req)
		if _, err := conn.SetSessionConfigOption(ctx, req); err != nil {
			return openedSession{}, fmt.Errorf("acp set session config option %q: %w", id, err)
		}
		log.Debug("acp -> set session config option", "session_id", string(sessionID), "config_id", id)
	}
	return openedSession{id: sessionID, resumable: resumable}, nil
}

func mcpServers(in []*agentlinkpb.McpServer) []acp.McpServer {
	out := make([]acp.McpServer, 0, len(in))
	for _, s := range in {
		out = append(out, acp.McpServer{Http: &acp.McpServerHttpInline{
			Name: s.GetName(), Type: "http", Url: s.GetUrl(), Headers: []acp.HttpHeader{},
		}})
	}
	return out
}

func systemPromptMeta(prompt string) map[string]any {
	if prompt == "" {
		return nil
	}
	return map[string]any{"systemPrompt": map[string]any{"append": prompt}}
}

func openOrResume(
	ctx context.Context,
	log *slog.Logger,
	conn *acp.ClientSideConnection,
	params *agentlinkpb.SessionParams,
	servers []acp.McpServer,
	initResp acp.InitializeResponse,
) (acp.SessionId, []acp.SessionConfigOption, error) {
	if params.GetResumeSessionId() == "" {
		log.Debug("acp -> new session", "cwd", params.GetCwd(), "mcp_servers", len(servers),
			"system_prompt_chars", len(params.GetSystemPrompt()))
		resp, err := conn.NewSession(ctx, acp.NewSessionRequest{
			Cwd:        params.GetCwd(),
			McpServers: servers,
			Meta:       systemPromptMeta(params.GetSystemPrompt()),
		})
		if err != nil {
			return "", nil, fmt.Errorf("acp new session: %w", err)
		}
		return resp.SessionId, resp.ConfigOptions, nil
	}

	sessionID := acp.SessionId(params.GetResumeSessionId())
	caps := initResp.AgentCapabilities.SessionCapabilities
	if caps.Resume == nil {
		return "", nil, fmt.Errorf("%w: the agent does not support session/resume", errResumeUnavailable)
	}
	if caps.List != nil {
		if err := confirmSessionKnown(ctx, log, conn, params.GetCwd(), sessionID); err != nil {
			return "", nil, err
		}
	}
	log.Debug("acp -> resume session", "session_id", string(sessionID), "cwd", params.GetCwd(), "mcp_servers", len(servers))
	resp, err := conn.ResumeSession(ctx, acp.ResumeSessionRequest{
		SessionId:  sessionID,
		Cwd:        params.GetCwd(),
		McpServers: servers,
	})
	if err != nil {
		return "", nil, fmt.Errorf("%w: %v", errResumeUnavailable, err)
	}
	return sessionID, resp.ConfigOptions, nil
}

const maxSessionListPages = 20

func confirmSessionKnown(ctx context.Context, log *slog.Logger, conn *acp.ClientSideConnection, cwd string, want acp.SessionId) error {
	var cursor *string
	for page := 0; page < maxSessionListPages; page++ {
		resp, err := conn.ListSessions(ctx, acp.ListSessionsRequest{Cwd: &cwd, Cursor: cursor})
		if err != nil {
			log.Warn("could not list the agent's sessions; attempting the resume anyway", "session_id", string(want), "err", err)
			return nil
		}
		for _, s := range resp.Sessions {
			if s.SessionId == want {
				return nil
			}
		}
		if resp.NextCursor == nil || *resp.NextCursor == "" {
			return fmt.Errorf("%w: the agent has no record of session %s", errResumeUnavailable, want)
		}
		cursor = resp.NextCursor
	}
	log.Warn("gave up paging the agent's sessions; attempting the resume anyway", "session_id", string(want))
	return nil
}

func sessionConfigRequests(sessionID acp.SessionId, advertised []acp.SessionConfigOption, cfg map[string]string) ([]acp.SetSessionConfigOptionRequest, error) {
	if len(cfg) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(cfg))
	for id := range cfg {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if i := slices.Index(ids, "model"); i > 0 {
		ids = append([]string{"model"}, slices.Delete(ids, i, i+1)...)
	}

	reqs := make([]acp.SetSessionConfigOptionRequest, 0, len(ids))
	for _, id := range ids {
		value := cfg[id]
		opt := findConfigOption(advertised, id)
		switch {
		case opt == nil:
			return nil, fmt.Errorf("session config option %q is not supported by the agent (supported: %s)",
				id, strings.Join(configOptionIDs(advertised), ", "))
		case opt.Boolean != nil:
			b, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("session config option %q is boolean; value %q is not true/false", id, value)
			}
			reqs = append(reqs, acp.SetSessionConfigOptionRequest{
				Boolean: &acp.SetSessionConfigOptionBoolean{
					SessionId: sessionID, ConfigId: acp.SessionConfigId(id), Type: "boolean", Value: b,
				},
			})
		default:
			reqs = append(reqs, acp.SetSessionConfigOptionRequest{
				ValueId: &acp.SetSessionConfigOptionValueId{
					SessionId: sessionID, ConfigId: acp.SessionConfigId(id), Value: acp.SessionConfigValueId(value),
				},
			})
		}
	}
	return reqs, nil
}

func findConfigOption(advertised []acp.SessionConfigOption, id string) *acp.SessionConfigOption {
	for i := range advertised {
		o := &advertised[i]
		if (o.Select != nil && string(o.Select.Id) == id) || (o.Boolean != nil && string(o.Boolean.Id) == id) {
			return o
		}
	}
	return nil
}

func configOptionIDs(advertised []acp.SessionConfigOption) []string {
	ids := make([]string, 0, len(advertised))
	for _, o := range advertised {
		switch {
		case o.Select != nil:
			ids = append(ids, string(o.Select.Id))
		case o.Boolean != nil:
			ids = append(ids, string(o.Boolean.Id))
		}
	}
	if len(ids) == 0 {
		return []string{"none"}
	}
	return ids
}

func configRequestID(req acp.SetSessionConfigOptionRequest) string {
	switch {
	case req.Boolean != nil:
		return string(req.Boolean.ConfigId)
	case req.ValueId != nil:
		return string(req.ValueId.ConfigId)
	}
	return ""
}

const partMax = 256 << 10

func cutPart(s string) (string, string) {
	if len(s) <= partMax {
		return s, ""
	}
	i := partMax
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i], s[i:]
}

type Aggregator struct {
	emit func(turnID string, ev *agentlinkpb.AgentEvent)

	mu      sync.Mutex
	turnID  string
	part    int
	buf     strings.Builder
	thought strings.Builder
	titles  map[string]string
}

func NewAggregator(emit func(turnID string, ev *agentlinkpb.AgentEvent)) *Aggregator {
	return &Aggregator{emit: emit}
}

func (a *Aggregator) Begin(turnID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.turnID, a.part = turnID, 0
	a.buf.Reset()
	a.thought.Reset()
}

func (a *Aggregator) End() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushThoughtLocked()
	a.flushMessageLocked(true)
	a.turnID = ""
}

func (a *Aggregator) Current() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.turnID
}

func (a *Aggregator) Title(toolCallID string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.titles[toolCallID]
}

func (a *Aggregator) flushThoughtLocked() {
	text := a.thought.String()
	a.thought.Reset()
	for text != "" {
		var part string
		part, text = cutPart(text)
		a.emit(a.turnID, &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Thought{Thought: &agentlinkpb.AgentThought{Text: part}}})
	}
}

func (a *Aggregator) flushMessageLocked(final bool) {
	text := a.buf.String()
	a.buf.Reset()
	for text != "" {
		var part string
		part, text = cutPart(text)
		a.emitPartLocked(part, final && text == "")
	}
}

func (a *Aggregator) splitFullLocked(b *strings.Builder, emit func(string)) {
	if b.Len() <= partMax {
		return
	}
	text := b.String()
	b.Reset()
	for len(text) > partMax {
		var part string
		part, text = cutPart(text)
		emit(part)
	}
	b.WriteString(text)
}

func (a *Aggregator) emitPartLocked(text string, final bool) {
	a.part++
	turn := a.turnID
	if turn == "" {
		turn = "t0"
	}
	a.emit(a.turnID, &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Message{Message: &agentlinkpb.AgentMessage{
		PartId: turn + "." + strconv.Itoa(a.part), Text: text, Final: final,
	}}})
}

func (a *Aggregator) Update(u acp.SessionUpdate) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case u.AgentMessageChunk != nil:
		if text := contentText(u.AgentMessageChunk.Content); text != "" {
			a.flushThoughtLocked()
			a.buf.WriteString(text)
			a.splitFullLocked(&a.buf, func(part string) { a.emitPartLocked(part, false) })
		}
	case u.AgentThoughtChunk != nil:
		a.thought.WriteString(contentText(u.AgentThoughtChunk.Content))
		a.splitFullLocked(&a.thought, func(part string) {
			a.emit(a.turnID, &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Thought{Thought: &agentlinkpb.AgentThought{Text: part}}})
		})
	case u.ToolCall != nil:
		a.toolCallLocked(&agentlinkpb.ToolCall{
			Id:       string(u.ToolCall.ToolCallId),
			Title:    u.ToolCall.Title,
			Kind:     string(u.ToolCall.Kind),
			Status:   string(u.ToolCall.Status),
			RawInput: rawJSON(u.ToolCall.RawInput),
		})
	case u.ToolCallUpdate != nil:
		c := &agentlinkpb.ToolCall{
			Id:       string(u.ToolCallUpdate.ToolCallId),
			RawInput: rawJSON(u.ToolCallUpdate.RawInput),
			Update:   true,
		}
		if u.ToolCallUpdate.Title != nil {
			c.Title = *u.ToolCallUpdate.Title
		}
		if u.ToolCallUpdate.Status != nil {
			c.Status = string(*u.ToolCallUpdate.Status)
		}
		if u.ToolCallUpdate.Kind != nil {
			c.Kind = string(*u.ToolCallUpdate.Kind)
		}
		a.toolCallLocked(c)
	case u.UsageUpdate != nil:
		usage := &agentlinkpb.Usage{
			ContextWindow: int64(u.UsageUpdate.Size),
			ContextUsed:   int64(u.UsageUpdate.Used),
		}
		if c := u.UsageUpdate.Cost; c != nil && strings.EqualFold(c.Currency, "USD") {
			usage.CostUsd = c.Amount
		}
		a.emit(a.turnID, &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_Usage{Usage: usage}})
	}
}

func (a *Aggregator) toolCallLocked(c *agentlinkpb.ToolCall) {
	a.flushThoughtLocked()
	if a.titles == nil {
		a.titles = map[string]string{}
	}
	if c.Title == "" {
		c.Title = a.titles[c.Id]
	}
	if c.Title != "" {
		a.titles[c.Id] = c.Title
	}
	a.flushMessageLocked(false)
	a.emit(a.turnID, &agentlinkpb.AgentEvent{Payload: &agentlinkpb.AgentEvent_ToolCall{ToolCall: c}})
}

func contentText(b acp.ContentBlock) string {
	if b.Text != nil {
		return b.Text.Text
	}
	return ""
}

func rawJSON(v any) []byte {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) > partMax {
		return nil
	}
	return b
}

func usageFrom(u *acp.Usage) *agentlinkpb.Usage {
	out := &agentlinkpb.Usage{
		InputTokens:  int64(u.InputTokens),
		OutputTokens: int64(u.OutputTokens),
		TotalTokens:  int64(u.TotalTokens),
	}
	if u.CachedReadTokens != nil {
		out.CachedReadTokens = int64(*u.CachedReadTokens)
	}
	if u.CachedWriteTokens != nil {
		out.CachedWriteTokens = int64(*u.CachedWriteTokens)
	}
	if u.ThoughtTokens != nil {
		out.ThoughtTokens = int64(*u.ThoughtTokens)
	}
	return out
}
