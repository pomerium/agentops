package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"

	acp "github.com/coder/acp-go-sdk"

	"github.com/pomerium/agentops/harness/internal/telemetry"
)

type ToolCallEvent struct {
	ID       string
	Title    string
	Status   string
	Kind     string
	RawInput any
	Update   bool
}

type UsageEvent struct {
	InputTokens       int64
	OutputTokens      int64
	CachedReadTokens  int64
	CachedWriteTokens int64
	ThoughtTokens     int64
	TotalTokens       int64
	ContextWindow     int64
	ContextUsed       int64
	CostUSD           float64
}

type PermissionOption struct {
	ID   string
	Name string
	Kind string
}

type PermissionRequest struct {
	ToolCallID string
	Title      string
	Options    []PermissionOption
}

type PermissionDecision struct {
	OptionID  string
	Cancelled bool
}

type EventSink interface {
	AgentMessage(ctx context.Context, text string)
	AgentThought(ctx context.Context, text string)
	ToolCall(ctx context.Context, ev ToolCallEvent)
	Usage(ctx context.Context, ev UsageEvent)
	Permission(ctx context.Context, req PermissionRequest) (PermissionDecision, error)
}

type acpClient struct {
	sink EventSink
	tel  *telemetry.Component
}

var _ acp.Client = (*acpClient)(nil)

func NewACPClient(sink EventSink, tel *telemetry.Component) acp.Client {
	return &acpClient{sink: sink, tel: defaultTel(tel)}
}

func defaultTel(tel *telemetry.Component) *telemetry.Component {
	if tel != nil {
		return tel
	}
	return telemetry.New(slog.Default(), "acp", slog.LevelDebug)
}

func (c *acpClient) SessionUpdate(ctx context.Context, params acp.SessionNotification) error {
	c.tel.Debug(ctx, "acp <- session update",
		"session_id", string(params.SessionId), "update_kind", updateKind(params.Update))
	u := params.Update
	switch {
	case u.AgentMessageChunk != nil:
		c.sink.AgentMessage(ctx, contentText(u.AgentMessageChunk.Content))
	case u.AgentThoughtChunk != nil:
		c.sink.AgentThought(ctx, contentText(u.AgentThoughtChunk.Content))
	case u.ToolCall != nil:
		c.sink.ToolCall(ctx, ToolCallEvent{
			ID:       string(u.ToolCall.ToolCallId),
			Title:    u.ToolCall.Title,
			Status:   string(u.ToolCall.Status),
			Kind:     string(u.ToolCall.Kind),
			RawInput: u.ToolCall.RawInput,
		})
	case u.ToolCallUpdate != nil:
		ev := ToolCallEvent{
			ID:       string(u.ToolCallUpdate.ToolCallId),
			RawInput: u.ToolCallUpdate.RawInput,
			Update:   true,
		}
		if u.ToolCallUpdate.Title != nil {
			ev.Title = *u.ToolCallUpdate.Title
		}
		if u.ToolCallUpdate.Status != nil {
			ev.Status = string(*u.ToolCallUpdate.Status)
		}
		if u.ToolCallUpdate.Kind != nil {
			ev.Kind = string(*u.ToolCallUpdate.Kind)
		}
		c.sink.ToolCall(ctx, ev)
	case u.UsageUpdate != nil:
		ev := UsageEvent{
			ContextWindow: int64(u.UsageUpdate.Size),
			ContextUsed:   int64(u.UsageUpdate.Used),
		}
		if c := u.UsageUpdate.Cost; c != nil && strings.EqualFold(c.Currency, "USD") {
			ev.CostUSD = c.Amount
		}
		c.sink.Usage(ctx, ev)
	}
	return nil
}

func usageFrom(u *acp.Usage) UsageEvent {
	ev := UsageEvent{
		InputTokens:  int64(u.InputTokens),
		OutputTokens: int64(u.OutputTokens),
		TotalTokens:  int64(u.TotalTokens),
	}
	if u.CachedReadTokens != nil {
		ev.CachedReadTokens = int64(*u.CachedReadTokens)
	}
	if u.CachedWriteTokens != nil {
		ev.CachedWriteTokens = int64(*u.CachedWriteTokens)
	}
	if u.ThoughtTokens != nil {
		ev.ThoughtTokens = int64(*u.ThoughtTokens)
	}
	return ev
}

func (c *acpClient) RequestPermission(ctx context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	req := PermissionRequest{ToolCallID: string(params.ToolCall.ToolCallId)}
	if params.ToolCall.Title != nil {
		req.Title = *params.ToolCall.Title
	}
	for _, o := range params.Options {
		req.Options = append(req.Options, PermissionOption{
			ID:   string(o.OptionId),
			Name: o.Name,
			Kind: string(o.Kind),
		})
	}
	c.tel.Debug(ctx, "acp <- request permission",
		"session_id", string(params.SessionId), "tool_call_id", req.ToolCallID, "options", len(req.Options))
	decision, err := c.sink.Permission(ctx, req)
	if err != nil {
		return acp.RequestPermissionResponse{}, err
	}
	c.tel.Debug(ctx, "acp -> permission decision",
		"tool_call_id", req.ToolCallID, "cancelled", decision.Cancelled, "option_id", decision.OptionID)
	if decision.Cancelled {
		return acp.RequestPermissionResponse{
			Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}},
		}, nil
	}
	return acp.RequestPermissionResponse{
		Outcome: acp.RequestPermissionOutcome{
			Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(decision.OptionID)},
		},
	}, nil
}

func (c *acpClient) ReadTextFile(ctx context.Context, _ acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	c.tel.Debug(ctx, "acp <- readTextFile (unsupported)")
	return acp.ReadTextFileResponse{}, fmt.Errorf("fs.readTextFile not supported by this client")
}

func (c *acpClient) WriteTextFile(ctx context.Context, _ acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	c.tel.Debug(ctx, "acp <- writeTextFile (unsupported)")
	return acp.WriteTextFileResponse{}, fmt.Errorf("fs.writeTextFile not supported by this client")
}

func (c *acpClient) CreateTerminal(ctx context.Context, _ acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	c.tel.Debug(ctx, "acp <- createTerminal (unsupported)")
	return acp.CreateTerminalResponse{}, fmt.Errorf("terminal not supported by this client")
}

func (c *acpClient) KillTerminal(ctx context.Context, _ acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	c.tel.Debug(ctx, "acp <- killTerminal (unsupported)")
	return acp.KillTerminalResponse{}, fmt.Errorf("terminal not supported by this client")
}

func (c *acpClient) TerminalOutput(ctx context.Context, _ acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	c.tel.Debug(ctx, "acp <- terminalOutput (unsupported)")
	return acp.TerminalOutputResponse{}, fmt.Errorf("terminal not supported by this client")
}

func (c *acpClient) ReleaseTerminal(ctx context.Context, _ acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	c.tel.Debug(ctx, "acp <- releaseTerminal (unsupported)")
	return acp.ReleaseTerminalResponse{}, fmt.Errorf("terminal not supported by this client")
}

func (c *acpClient) WaitForTerminalExit(ctx context.Context, _ acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	c.tel.Debug(ctx, "acp <- waitForTerminalExit (unsupported)")
	return acp.WaitForTerminalExitResponse{}, fmt.Errorf("terminal not supported by this client")
}

func contentText(b acp.ContentBlock) string {
	if b.Text != nil {
		return b.Text.Text
	}
	return ""
}

func normalizeMCPHeaders(servers []acp.McpServer) {
	for i := range servers {
		if servers[i].Http != nil && servers[i].Http.Headers == nil {
			servers[i].Http.Headers = []acp.HttpHeader{}
		}
		if servers[i].Sse != nil && servers[i].Sse.Headers == nil {
			servers[i].Sse.Headers = []acp.HttpHeader{}
		}
	}
}

func mcpServerNames(servers []acp.McpServer) []string {
	names := make([]string, 0, len(servers))
	for _, s := range servers {
		switch {
		case s.Http != nil:
			names = append(names, s.Http.Name)
		case s.Sse != nil:
			names = append(names, s.Sse.Name)
		case s.Stdio != nil:
			names = append(names, s.Stdio.Name)
		}
	}
	return names
}

func updateKind(u acp.SessionUpdate) string {
	switch {
	case u.AgentMessageChunk != nil:
		return "agent_message_chunk"
	case u.AgentThoughtChunk != nil:
		return "agent_thought_chunk"
	case u.UserMessageChunk != nil:
		return "user_message_chunk"
	case u.ToolCall != nil:
		return "tool_call"
	case u.ToolCallUpdate != nil:
		return "tool_call_update"
	case u.Plan != nil:
		return "plan"
	case u.AvailableCommandsUpdate != nil:
		return "available_commands_update"
	case u.CurrentModeUpdate != nil:
		return "current_mode_update"
	default:
		return "other"
	}
}

type Session struct {
	conn      *acp.ClientSideConnection
	sessionID acp.SessionId
	close     func() error
	closeOnce sync.Once
	closeErr  error
	tel       *telemetry.Component
	sink      EventSink
	resumable bool
}

func (s *Session) Resumable() bool { return s.resumable }

type SessionParams struct {
	Cwd             string
	MCPServers      []acp.McpServer
	SystemPrompt    string
	SessionConfig   map[string]string
	ResumeSessionID string
}

var ErrResumeUnavailable = errors.New("this conversation cannot be resumed")

func systemPromptMeta(prompt string) map[string]any {
	if prompt == "" {
		return nil
	}
	return map[string]any{
		"systemPrompt": map[string]any{"append": prompt},
	}
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

func OpenSession(ctx context.Context, tel *telemetry.Component, sink EventSink, agentStdin io.Writer, agentStdout io.Reader, closeFn func() error, params SessionParams) (*Session, error) {
	tel = defaultTel(tel)
	conn := acp.NewClientSideConnection(NewACPClient(sink, tel), agentStdin, agentStdout)

	tel.Debug(ctx, "acp -> initialize", "protocol_version", acp.ProtocolVersionNumber)
	initResp, err := conn.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion:    acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{},
	})
	if err != nil {
		return nil, fmt.Errorf("acp initialize: %w", err)
	}
	resumable := initResp.AgentCapabilities.SessionCapabilities.Resume != nil
	tel.Debug(ctx, "acp <- initialized",
		"protocol_version", initResp.ProtocolVersion,
		"resume_supported", resumable,
		"load_session_supported", initResp.AgentCapabilities.LoadSession,
		"agent_capabilities", initResp.AgentCapabilities)

	normalizeMCPHeaders(params.MCPServers)

	sessionID, configOptions, err := openOrResume(ctx, tel, conn, params, initResp)
	if err != nil {
		return nil, err
	}

	if _, err := conn.SetSessionMode(ctx, acp.SetSessionModeRequest{
		SessionId: sessionID,
		ModeId:    acp.SessionModeId("bypassPermissions"),
	}); err != nil {
		tel.Warn(ctx, "set session mode failed (continuing)", "mode", "bypassPermissions", "err", err)
	} else {
		tel.Debug(ctx, "acp -> set session mode", "session_id", string(sessionID), "mode", "bypassPermissions")
	}

	cfgReqs, err := sessionConfigRequests(sessionID, configOptions, params.SessionConfig)
	if err != nil {
		return nil, fmt.Errorf("acp session config: %w", err)
	}
	for _, req := range cfgReqs {
		id := configRequestID(req)
		if _, err := conn.SetSessionConfigOption(ctx, req); err != nil {
			return nil, fmt.Errorf("acp set session config option %q: %w", id, err)
		}
		tel.Debug(ctx, "acp -> set session config option",
			"session_id", string(sessionID), "config_id", id, "value", params.SessionConfig[id])
	}

	return &Session{
		conn: conn, sessionID: sessionID, close: closeFn, tel: tel, sink: sink, resumable: resumable,
	}, nil
}

func openOrResume(
	ctx context.Context,
	tel *telemetry.Component,
	conn *acp.ClientSideConnection,
	params SessionParams,
	initResp acp.InitializeResponse,
) (acp.SessionId, []acp.SessionConfigOption, error) {
	if params.ResumeSessionID == "" {
		tel.Debug(ctx, "acp -> new session",
			"cwd", params.Cwd, "mcp_servers", len(params.MCPServers), "mcp_server_names", mcpServerNames(params.MCPServers),
			"system_prompt_chars", len(params.SystemPrompt))
		resp, err := conn.NewSession(ctx, acp.NewSessionRequest{
			Cwd:        params.Cwd,
			McpServers: params.MCPServers,
			Meta:       systemPromptMeta(params.SystemPrompt),
		})
		if err != nil {
			return "", nil, fmt.Errorf("acp new session: %w", err)
		}
		tel.Debug(ctx, "acp <- session created", "session_id", string(resp.SessionId))
		return resp.SessionId, resp.ConfigOptions, nil
	}

	sessionID := acp.SessionId(params.ResumeSessionID)
	caps := initResp.AgentCapabilities.SessionCapabilities
	if caps.Resume == nil {
		tel.Debug(ctx, "agent does not support resuming a session", "session_id", params.ResumeSessionID)
		return "", nil, fmt.Errorf("%w: the agent does not support session/resume", ErrResumeUnavailable)
	}

	if caps.List != nil {
		if err := confirmSessionKnown(ctx, tel, conn, params.Cwd, sessionID); err != nil {
			return "", nil, err
		}
	}

	tel.Debug(ctx, "acp -> resume session",
		"session_id", params.ResumeSessionID, "cwd", params.Cwd,
		"mcp_servers", len(params.MCPServers), "mcp_server_names", mcpServerNames(params.MCPServers))
	resp, err := conn.ResumeSession(ctx, acp.ResumeSessionRequest{
		SessionId:  sessionID,
		Cwd:        params.Cwd,
		McpServers: params.MCPServers,
	})
	if err != nil {
		tel.Debug(ctx, "acp <- resume session refused", "session_id", params.ResumeSessionID, "err", err)
		return "", nil, fmt.Errorf("%w: %v", ErrResumeUnavailable, err)
	}
	tel.Debug(ctx, "acp <- session resumed", "session_id", params.ResumeSessionID)
	return sessionID, resp.ConfigOptions, nil
}

func confirmSessionKnown(
	ctx context.Context,
	tel *telemetry.Component,
	conn *acp.ClientSideConnection,
	cwd string,
	want acp.SessionId,
) error {
	var cursor *string
	for page := 0; page < maxSessionListPages; page++ {
		resp, err := conn.ListSessions(ctx, acp.ListSessionsRequest{Cwd: &cwd, Cursor: cursor})
		if err != nil {
			tel.Warn(ctx, "could not list the agent's sessions; attempting the resume anyway",
				"session_id", string(want), "err", err)
			return nil
		}
		for _, s := range resp.Sessions {
			if s.SessionId == want {
				return nil
			}
		}
		if resp.NextCursor == nil || *resp.NextCursor == "" {
			tel.Debug(ctx, "the agent does not know this session; its transcript is gone",
				"session_id", string(want), "cwd", cwd)
			return fmt.Errorf("%w: the agent has no record of session %s", ErrResumeUnavailable, want)
		}
		cursor = resp.NextCursor
	}
	tel.Warn(ctx, "gave up paging the agent's sessions; attempting the resume anyway",
		"session_id", string(want), "pages", maxSessionListPages)
	return nil
}

const maxSessionListPages = 20

func (s *Session) ID() string { return string(s.sessionID) }

func (s *Session) Prompt(ctx context.Context, text string) (acp.StopReason, error) {
	s.tel.Debug(ctx, "acp -> prompt", "session_id", string(s.sessionID), "chars", len(text))
	resp, err := s.conn.Prompt(ctx, acp.PromptRequest{
		SessionId: s.sessionID,
		Prompt:    []acp.ContentBlock{acp.TextBlock(text)},
	})
	if err != nil {
		s.tel.Error(ctx, "acp <- prompt error", "session_id", string(s.sessionID), "err", err)
		return "", fmt.Errorf("acp prompt: %w", err)
	}
	s.tel.Debug(ctx, "acp <- prompt complete", "session_id", string(s.sessionID), "stop_reason", string(resp.StopReason))
	if resp.Usage != nil && s.sink != nil {
		s.sink.Usage(ctx, usageFrom(resp.Usage))
	}
	return resp.StopReason, nil
}

func (s *Session) Cancel(ctx context.Context) error {
	s.tel.Debug(ctx, "acp -> cancel", "session_id", string(s.sessionID))
	return s.conn.Cancel(ctx, acp.CancelNotification{SessionId: s.sessionID})
}

func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		if s.close != nil {
			s.closeErr = s.close()
		}
	})
	return s.closeErr
}
