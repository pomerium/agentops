package runnertest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	acp "github.com/coder/acp-go-sdk"
)

const (
	EnvFakeAgent = "AGENTOPS_FAKE_ACP_AGENT"
	EnvSessionID = "AGENTOPS_FAKE_ACP_SESSION_ID"
	EnvNoResume  = "AGENTOPS_FAKE_ACP_NO_RESUME"
	EnvKnown     = "AGENTOPS_FAKE_ACP_KNOWN"
	EnvRecord    = "AGENTOPS_FAKE_ACP_RECORD"
	EnvOptions   = "AGENTOPS_FAKE_ACP_CONFIG_OPTIONS"
)

func Command(env ...string) []string {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	argv := []string{"/usr/bin/env", EnvFakeAgent + "=1"}
	argv = append(argv, env...)
	return append(argv, exe)
}

func RunIfRequested() {
	if os.Getenv(EnvFakeAgent) == "" {
		return
	}
	os.Exit(Serve(os.Stdin, os.Stdout))
}

func Serve(in io.Reader, out io.Writer) int {
	a := &fakeAgent{
		sessionID: cmpOr(os.Getenv(EnvSessionID), "fake-session"),
		resume:    os.Getenv(EnvNoResume) == "",
		options:   splitList(os.Getenv(EnvOptions)),
		record:    os.Getenv(EnvRecord),
	}
	a.known = append(splitList(os.Getenv(EnvKnown)), a.sessionID)
	a.conn = acp.NewAgentSideConnection(a, out, in)
	<-a.conn.Done()
	return 0
}

type Request struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func ReadRecord(path string) ([]Request, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Request
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var r Request
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

type fakeAgent struct {
	conn      *acp.AgentSideConnection
	sessionID string
	resume    bool
	known     []string
	options   []string
	record    string

	mu sync.Mutex
}

func (a *fakeAgent) note(method string, params any) {
	if a.record == "" {
		return
	}
	body, _ := json.Marshal(params)
	line, _ := json.Marshal(Request{Method: method, Params: body})
	a.mu.Lock()
	defer a.mu.Unlock()
	f, err := os.OpenFile(a.record, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = f.Write(append(line, '\n'))
}

func (a *fakeAgent) Initialize(_ context.Context, p acp.InitializeRequest) (acp.InitializeResponse, error) {
	a.note("initialize", p)
	caps := acp.SessionCapabilities{List: &acp.SessionListCapabilities{}}
	if a.resume {
		caps.Resume = &acp.SessionResumeCapabilities{}
	}
	return acp.InitializeResponse{
		ProtocolVersion:   acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{SessionCapabilities: caps},
		AuthMethods:       []acp.AuthMethod{},
	}, nil
}

func (a *fakeAgent) configOptions() []acp.SessionConfigOption {
	out := make([]acp.SessionConfigOption, 0, len(a.options))
	for _, id := range a.options {
		out = append(out, acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
			Id: acp.SessionConfigId(id), Name: id, Type: "select", CurrentValue: "default",
			Options: acp.SessionConfigSelectOptions{Ungrouped: &acp.SessionConfigSelectOptionsUngrouped{}},
		}})
	}
	return out
}

func (a *fakeAgent) NewSession(_ context.Context, p acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	a.note("session/new", p)
	return acp.NewSessionResponse{SessionId: acp.SessionId(a.sessionID), ConfigOptions: a.configOptions()}, nil
}

func (a *fakeAgent) ResumeSession(_ context.Context, p acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	a.note("session/resume", p)
	if !slices.Contains(a.known, string(p.SessionId)) {
		return acp.ResumeSessionResponse{}, acp.NewInvalidParams(map[string]string{"error": "unknown session"})
	}
	return acp.ResumeSessionResponse{ConfigOptions: a.configOptions()}, nil
}

func (a *fakeAgent) ListSessions(_ context.Context, p acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	a.note("session/list", p)
	cwd := ""
	if p.Cwd != nil {
		cwd = *p.Cwd
	}
	out := make([]acp.SessionInfo, 0, len(a.known))
	for _, id := range a.known {
		out = append(out, acp.SessionInfo{SessionId: acp.SessionId(id), Cwd: cwd})
	}
	return acp.ListSessionsResponse{Sessions: out}, nil
}

func (a *fakeAgent) SetSessionMode(_ context.Context, p acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	a.note("session/set_mode", p)
	return acp.SetSessionModeResponse{}, nil
}

func (a *fakeAgent) SetSessionConfigOption(_ context.Context, p acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	a.note("session/set_config_option", p)
	return acp.SetSessionConfigOptionResponse{ConfigOptions: a.configOptions()}, nil
}

func (a *fakeAgent) Cancel(_ context.Context, p acp.CancelNotification) error {
	a.note("session/cancel", p)
	return nil
}

func (a *fakeAgent) Prompt(ctx context.Context, p acp.PromptRequest) (acp.PromptResponse, error) {
	a.note("session/prompt", p)
	var text strings.Builder
	for _, b := range p.Prompt {
		if b.Text != nil {
			text.WriteString(b.Text.Text)
		}
	}
	stop := acp.StopReasonEndTurn
	for _, line := range strings.Split(text.String(), "\n") {
		verb, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
		switch verb {
		case "":
		case "say":
			a.update(ctx, p.SessionId, acp.UpdateAgentMessageText(arg))
		case "think":
			a.update(ctx, p.SessionId, acp.UpdateAgentThoughtText(arg))
		case "tool":
			id, title, _ := strings.Cut(arg, " ")
			a.update(ctx, p.SessionId, acp.StartToolCall(acp.ToolCallId(id), title,
				acp.WithStartKind(acp.ToolKindExecute), acp.WithStartStatus(acp.ToolCallStatusPending),
				acp.WithStartRawInput(map[string]any{"command": title})))
		case "tooldone":
			a.update(ctx, p.SessionId, acp.UpdateToolCall(acp.ToolCallId(arg),
				acp.WithUpdateStatus(acp.ToolCallStatusCompleted)))
		case "ask":
			resp, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{
				SessionId: p.SessionId,
				ToolCall:  acp.ToolCallUpdate{ToolCallId: acp.ToolCallId(arg)},
				Options: []acp.PermissionOption{
					{OptionId: "allow", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
					{OptionId: "reject", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
				},
			})
			switch {
			case err != nil:
				return acp.PromptResponse{}, err
			case resp.Outcome.Selected != nil:
				a.update(ctx, p.SessionId, acp.UpdateAgentMessageText(
					fmt.Sprintf("permission %s: %s", arg, resp.Outcome.Selected.OptionId)))
			default:
				a.update(ctx, p.SessionId, acp.UpdateAgentMessageText(fmt.Sprintf("permission %s: cancelled", arg)))
			}
		case "wait":
			deadline := time.Now().Add(30 * time.Second)
			for {
				if _, err := os.Stat(arg); err == nil {
					break
				}
				if time.Now().After(deadline) {
					return acp.PromptResponse{}, fmt.Errorf("waited too long for %s", arg)
				}
				time.Sleep(10 * time.Millisecond)
			}
		case "sleep":
			d, _ := time.ParseDuration(arg)
			time.Sleep(d)
		case "exit":
			code, _ := strconv.Atoi(arg)
			os.Exit(code)
		case "fail":
			return acp.PromptResponse{}, errors.New(arg)
		case "stop":
			stop = acp.StopReason(arg)
		default:
			a.update(ctx, p.SessionId, acp.UpdateAgentMessageText("echo: "+strings.TrimSpace(line)))
		}
	}
	return acp.PromptResponse{StopReason: stop, Usage: &acp.Usage{InputTokens: 10, OutputTokens: 20, TotalTokens: 30}}, nil
}

func (a *fakeAgent) update(ctx context.Context, id acp.SessionId, u acp.SessionUpdate) {
	_ = a.conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: id, Update: u})
}

var errUnsupported = errors.New("not supported by the fake agent")

func (a *fakeAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, errUnsupported
}

func (a *fakeAgent) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, errUnsupported
}

func (a *fakeAgent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, errUnsupported
}

func (a *fakeAgent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, errUnsupported
}

func (a *fakeAgent) UnstableDidChangeDocument(context.Context, acp.UnstableDidChangeDocumentNotification) error {
	return errUnsupported
}

func (a *fakeAgent) UnstableDidCloseDocument(context.Context, acp.UnstableDidCloseDocumentNotification) error {
	return errUnsupported
}

func (a *fakeAgent) UnstableDidFocusDocument(context.Context, acp.UnstableDidFocusDocumentNotification) error {
	return errUnsupported
}

func (a *fakeAgent) UnstableDidOpenDocument(context.Context, acp.UnstableDidOpenDocumentNotification) error {
	return errUnsupported
}

func (a *fakeAgent) UnstableDidSaveDocument(context.Context, acp.UnstableDidSaveDocumentNotification) error {
	return errUnsupported
}

func (a *fakeAgent) UnstableAcceptNes(context.Context, acp.UnstableAcceptNesNotification) error {
	return errUnsupported
}

func (a *fakeAgent) UnstableCloseNes(context.Context, acp.UnstableCloseNesRequest) (acp.UnstableCloseNesResponse, error) {
	return acp.UnstableCloseNesResponse{}, errUnsupported
}

func (a *fakeAgent) UnstableRejectNes(context.Context, acp.UnstableRejectNesNotification) error {
	return errUnsupported
}

func (a *fakeAgent) UnstableStartNes(context.Context, acp.UnstableStartNesRequest) (acp.UnstableStartNesResponse, error) {
	return acp.UnstableStartNesResponse{}, errUnsupported
}

func (a *fakeAgent) UnstableSuggestNes(context.Context, acp.UnstableSuggestNesRequest) (acp.UnstableSuggestNesResponse, error) {
	return acp.UnstableSuggestNesResponse{}, errUnsupported
}

func (a *fakeAgent) UnstableDisableProvider(context.Context, acp.UnstableDisableProviderRequest) (acp.UnstableDisableProviderResponse, error) {
	return acp.UnstableDisableProviderResponse{}, errUnsupported
}

func (a *fakeAgent) UnstableListProviders(context.Context, acp.UnstableListProvidersRequest) (acp.UnstableListProvidersResponse, error) {
	return acp.UnstableListProvidersResponse{}, errUnsupported
}

func (a *fakeAgent) UnstableSetProvider(context.Context, acp.UnstableSetProviderRequest) (acp.UnstableSetProviderResponse, error) {
	return acp.UnstableSetProviderResponse{}, errUnsupported
}

func (a *fakeAgent) UnstableDeleteSession(context.Context, acp.UnstableDeleteSessionRequest) (acp.UnstableDeleteSessionResponse, error) {
	return acp.UnstableDeleteSessionResponse{}, errUnsupported
}

func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
