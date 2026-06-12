package sandbox

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"

	acp "github.com/coder/acp-go-sdk"

	"github.com/pomerium/agentops/internal/telemetry"
)

// ToolCallEvent is a transport-agnostic view of an ACP tool-call notification,
// surfaced to the sink for rendering in Slack.
type ToolCallEvent struct {
	ID     string
	Title  string
	Status string
	Kind   string
	// Update is true when this is a status/result update to an existing call,
	// false when it announces a new call.
	Update bool
}

// PermissionOption is one choice the user can pick when approving a tool call.
type PermissionOption struct {
	ID   string
	Name string
	Kind string
}

// PermissionRequest asks the user to authorize a sensitive tool call.
type PermissionRequest struct {
	ToolCallID string
	Title      string
	Options    []PermissionOption
}

// PermissionDecision is the user's response to a PermissionRequest.
type PermissionDecision struct {
	OptionID  string
	Cancelled bool
}

// EventSink receives the agent's streamed output and permission prompts for a
// session. The Slack side implements it to render Block Kit messages and route
// interactive approvals back into the ACP session.
type EventSink interface {
	AgentMessage(ctx context.Context, text string)
	AgentThought(ctx context.Context, text string)
	ToolCall(ctx context.Context, ev ToolCallEvent)
	Permission(ctx context.Context, req PermissionRequest) (PermissionDecision, error)
}

// acpClient adapts the ACP Client interface to an EventSink. The app is the ACP
// *client*; the agent in the sandbox is the ACP *agent*.
type acpClient struct {
	sink EventSink
	tel  *telemetry.Component
}

var _ acp.Client = (*acpClient)(nil)

// NewACPClient returns an acp.Client that forwards session updates and
// permission requests to sink. tel may be nil.
func NewACPClient(sink EventSink, tel *telemetry.Component) acp.Client {
	return &acpClient{sink: sink, tel: defaultTel(tel)}
}

// defaultTel returns tel or a default "acp" component, so callers/tests may pass
// nil.
func defaultTel(tel *telemetry.Component) *telemetry.Component {
	if tel != nil {
		return tel
	}
	return telemetry.New(slog.Default(), "acp", slog.LevelDebug)
}

func (c *acpClient) SessionUpdate(ctx context.Context, params acp.SessionNotification) error {
	// Trace the harness reaction (kind only — never the payload).
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
			ID:     string(u.ToolCall.ToolCallId),
			Title:  u.ToolCall.Title,
			Status: string(u.ToolCall.Status),
			Kind:   string(u.ToolCall.Kind),
		})
	case u.ToolCallUpdate != nil:
		ev := ToolCallEvent{ID: string(u.ToolCallUpdate.ToolCallId), Update: true}
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
	}
	// Other update kinds (plan, mode, usage, etc.) are ignored for now.
	return nil
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

// The agent runs inside the sandbox with its own filesystem and terminal; the
// app does not proxy these. We advertise no fs/terminal capabilities, so a
// well-behaved agent never calls them. They return errors defensively (and log
// at debug, since an unexpected call here is a useful diagnostic).

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

// normalizeMCPHeaders ensures every HTTP/SSE MCP server carries a non-nil
// Headers slice, so it marshals to a JSON array (`[]`) rather than `null`. See
// the call site in OpenSession for why `null` is fatal to the whole session.
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

// mcpServerNames lists the configured MCP server names (no URLs/tokens) for the
// session trace.
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

// updateKind names an ACP session-update variant for the comms trace (no
// payload — just the kind).
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

// --- live session -----------------------------------------------------------

// Session is a live, multi-turn ACP session bound to a Slack thread.
type Session struct {
	conn      *acp.ClientSideConnection
	sessionID acp.SessionId
	close     func() error
	closeOnce sync.Once
	closeErr  error
	tel       *telemetry.Component
}

// SessionParams configures a new ACP session.
type SessionParams struct {
	// Cwd is the working directory inside the sandbox (absolute path).
	Cwd string
	// MCPServers are the per-server HTTP MCP configurations (with Authorization
	// headers) the agent should connect to.
	MCPServers []acp.McpServer
	// SystemPrompt is appended to the harness's own system prompt for this
	// session. Empty means no customization. It is delivered over the ACP
	// session/new request (see systemPromptMeta), NOT via a container env var —
	// the claude-agent-acp adapter ignores env-based prompts.
	SystemPrompt string
	// SessionConfig sets agent-advertised ACP session config options
	// (session/set_config_option) after the session is created, keyed by
	// option id — e.g. model/effort on the claude-agent-acp harness. Strict:
	// an id the agent does not advertise, or a value it rejects, fails
	// OpenSession (see sessionConfigRequests).
	SessionConfig map[string]string
}

// systemPromptMeta builds the ACP session/new `_meta` that injects a custom
// system prompt into the Claude Code harness. The claude-agent-acp adapter
// reads `_meta.systemPrompt`: when it is an OBJECT it is merged into the
// built-in "claude_code" preset (the adapter forces type/preset), so passing
// `{append: prompt}` APPENDS our instructions to Claude Code's own system
// prompt rather than replacing it. (A bare string would replace the preset and
// strip Claude Code's tool scaffolding — never do that.) Returns nil for an
// empty prompt so no `_meta` is sent.
func systemPromptMeta(prompt string) map[string]any {
	if prompt == "" {
		return nil
	}
	return map[string]any{
		"systemPrompt": map[string]any{"append": prompt},
	}
}

// sessionConfigRequests resolves the template's sessionConfig map against the
// options the agent advertised in session/new. Select options take the value
// as a value id (the agent validates it — claude-agent-acp resolves model
// aliases like "opus", so the client must not allow-list against Options);
// boolean options parse "true"/"false". An id the agent does not advertise, or
// an unparseable boolean, is an error: config that silently fails to apply
// would mislead the template author, so it must fail the launch instead. The
// error lists the advertised ids so the AgentTemplate YAML can be fixed from
// the surfaced message alone.
//
// "model" is always applied first (then the rest, sorted): the adapter
// rebuilds dependent options such as "effort" when the model changes, so an
// effort set before a model switch would be lost.
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

// findConfigOption returns the advertised option with the given id, or nil.
func findConfigOption(advertised []acp.SessionConfigOption, id string) *acp.SessionConfigOption {
	for i := range advertised {
		o := &advertised[i]
		if (o.Select != nil && string(o.Select.Id) == id) || (o.Boolean != nil && string(o.Boolean.Id) == id) {
			return o
		}
	}
	return nil
}

// configOptionIDs lists the advertised option ids, or "none" when the agent
// advertises no config options at all.
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

// configRequestID extracts the option id from either request variant, for
// tracing and error messages.
func configRequestID(req acp.SetSessionConfigOptionRequest) string {
	switch {
	case req.Boolean != nil:
		return string(req.Boolean.ConfigId)
	case req.ValueId != nil:
		return string(req.ValueId.ConfigId)
	}
	return ""
}

// OpenSession wires an ACP client over the given stdio streams (the agent's
// stdin/stdout), performs Initialize and NewSession, and returns a live
// Session. closeFn releases the underlying transport when the session ends.
// tel may be nil.
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
	// Surface what the agent advertises — notably whether it supports HTTP/SSE
	// MCP servers, which is required for the per-user MCP tokens we inject.
	tel.Debug(ctx, "acp <- initialized",
		"protocol_version", initResp.ProtocolVersion, "agent_capabilities", initResp.AgentCapabilities)

	// acp-go-sdk marshals a nil Headers slice as JSON `null`, but the ACP schema
	// requires `headers` to be an array. A `null` makes the agent reject the
	// ENTIRE session/new with -32602 ("Invalid params"), which silently
	// disconnects *every* MCP server — the agent then sees zero connected
	// servers. This bites any server configured without custom headers (e.g. a
	// public, tokenless MCP endpoint), so normalize nil → [] before sending.
	normalizeMCPHeaders(params.MCPServers)

	tel.Debug(ctx, "acp -> new session",
		"cwd", params.Cwd, "mcp_servers", len(params.MCPServers), "mcp_server_names", mcpServerNames(params.MCPServers),
		"system_prompt_chars", len(params.SystemPrompt))
	resp, err := conn.NewSession(ctx, acp.NewSessionRequest{
		Cwd:        params.Cwd,
		McpServers: params.MCPServers,
		Meta:       systemPromptMeta(params.SystemPrompt),
	})
	if err != nil {
		return nil, fmt.Errorf("acp new session: %w", err)
	}
	tel.Debug(ctx, "acp <- session created", "session_id", string(resp.SessionId))

	// Run the agent with permissions bypassed: Claude Code otherwise asks the
	// client to approve nearly every tool (including read-only MCP calls), which
	// we'd surface as blocking Slack prompts. bypassPermissions auto-allows all
	// tools so MCP-backed workflows run unattended. Best-effort: harnesses that
	// don't support the mode (e.g. the demo agent) treat this as a no-op.
	if _, err := conn.SetSessionMode(ctx, acp.SetSessionModeRequest{
		SessionId: resp.SessionId,
		ModeId:    acp.SessionModeId("bypassPermissions"),
	}); err != nil {
		tel.Warn(ctx, "set session mode failed (continuing)", "mode", "bypassPermissions", "err", err)
	} else {
		tel.Debug(ctx, "acp -> set session mode", "session_id", string(resp.SessionId), "mode", "bypassPermissions")
	}

	// Apply the template's session config (model, effort, ...). Unlike the
	// best-effort mode above (our own default), this is explicit template
	// intent: any option the agent doesn't advertise or rejects fails the
	// launch, so the author learns the config didn't apply instead of getting
	// a session that silently ignores it. Ids/values are config, not payload,
	// so they're safe to trace.
	cfgReqs, err := sessionConfigRequests(resp.SessionId, resp.ConfigOptions, params.SessionConfig)
	if err != nil {
		return nil, fmt.Errorf("acp session config: %w", err)
	}
	for _, req := range cfgReqs {
		id := configRequestID(req)
		if _, err := conn.SetSessionConfigOption(ctx, req); err != nil {
			return nil, fmt.Errorf("acp set session config option %q: %w", id, err)
		}
		tel.Debug(ctx, "acp -> set session config option",
			"session_id", string(resp.SessionId), "config_id", id, "value", params.SessionConfig[id])
	}

	return &Session{conn: conn, sessionID: resp.SessionId, close: closeFn, tel: tel}, nil
}

// ID returns the ACP session id.
func (s *Session) ID() string { return string(s.sessionID) }

// Prompt sends one user turn and blocks until the turn completes, returning the
// stop reason. Streaming output is delivered to the EventSink in the meantime.
func (s *Session) Prompt(ctx context.Context, text string) (acp.StopReason, error) {
	// Trace the turn (length only — never the message text).
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
	return resp.StopReason, nil
}

// Cancel interrupts the current turn.
func (s *Session) Cancel(ctx context.Context) error {
	s.tel.Debug(ctx, "acp -> cancel", "session_id", string(s.sessionID))
	return s.conn.Cancel(ctx, acp.CancelNotification{SessionId: s.sessionID})
}

// Close tears down the transport. It does not delete the sandbox; the
// orchestrator handles that separately. Close is idempotent: the sidecar
// supervisor and the session manager may both call it.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		if s.close != nil {
			s.closeErr = s.close()
		}
	})
	return s.closeErr
}
