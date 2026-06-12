package sandbox_test

import (
	"context"
	"strings"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/sandbox"
)

func TestBuildSandboxClaimInjectsContext(t *testing.T) {
	wf := &v1alpha1.AgentTemplate{
		Spec: v1alpha1.AgentTemplateSpec{
			SandboxTemplateRef: v1alpha1.SandboxTemplateReference{Name: "pomerium-zero-claude-code"},
		},
	}
	deadline := time.Now().Add(30 * time.Minute)
	claim := sandbox.BuildSandboxClaim("agentops", sandbox.LaunchSpec{
		SessionID:    "abc123",
		Template:     wf,
		SystemPrompt: "You are a deployment assistant.",
		Deadline:     deadline,
	})

	if claim.Namespace != "agentops" {
		t.Errorf("namespace = %q", claim.Namespace)
	}
	if claim.Spec.TemplateRef.Name != "pomerium-zero-claude-code" {
		t.Errorf("template ref = %q, want pomerium-zero-claude-code", claim.Spec.TemplateRef.Name)
	}
	if claim.Spec.WarmPool == nil || *claim.Spec.WarmPool != "none" {
		t.Errorf("warmpool = %v, want none", claim.Spec.WarmPool)
	}
	if claim.Spec.Lifecycle == nil || claim.Spec.Lifecycle.ShutdownTime == nil {
		t.Fatal("expected lifecycle shutdown time to be set from deadline")
	}

	env := map[string]string{}
	target := map[string]string{}
	for _, e := range claim.Spec.Env {
		env[e.Name] = e.Value
		target[e.Name] = e.ContainerName
	}
	if env[sandbox.EnvSessionID] != "abc123" {
		t.Errorf("session id env = %q", env[sandbox.EnvSessionID])
	}
	// The system prompt must NOT ride as a container env var: the harness
	// ignores env-based prompts, so it is delivered over the ACP session/new
	// request instead (see systemPromptMeta).
	if _, ok := env["AGENT_SYSTEM_PROMPT"]; ok {
		t.Error("system prompt leaked into claim env; it must travel over ACP, not env")
	}

	// The claim carries ONLY the per-run agent context. The git working
	// context (repo URL/ref and credentials) is baked into the agent
	// template's SandboxTemplate — nothing git-related may appear in
	// the claim, whose spec is plainly readable (kubectl get sandboxclaim).
	for name := range env {
		if strings.HasPrefix(name, "GIT_") {
			t.Errorf("claim carries git env %s; the git context belongs in the SandboxTemplate", name)
		}
	}

	// The agent context is targeted explicitly at the agent container.
	for _, name := range []string{sandbox.EnvSessionID} {
		if got := target[name]; got != sandbox.AgentContainerName {
			t.Errorf("env %s targets container %q, want %q", name, got, sandbox.AgentContainerName)
		}
	}
}

// Finding #7: when the operator runs the agent in a non-default container
// (AGENT_CONTAINER=main), the claim env must target that same container.
// Hardcoding "agent" would land the system prompt / session id on a container
// the agent process never runs in, silently losing them.
func TestBuildSandboxClaimTargetsConfiguredContainer(t *testing.T) {
	wf := &v1alpha1.AgentTemplate{
		Spec: v1alpha1.AgentTemplateSpec{
			SandboxTemplateRef: v1alpha1.SandboxTemplateReference{Name: "tmpl"},
		},
	}
	claim := sandbox.BuildSandboxClaim("ns", sandbox.LaunchSpec{
		SessionID:      "s1",
		Template:       wf,
		SystemPrompt:   "hi",
		AgentContainer: "main",
	})
	for _, e := range claim.Spec.Env {
		if e.ContainerName != "main" {
			t.Errorf("env %s targets container %q, want main", e.Name, e.ContainerName)
		}
	}
}

// An empty AgentContainer keeps the default ("agent") so existing templates and
// the unconfigured path are unaffected.
func TestBuildSandboxClaimDefaultsContainer(t *testing.T) {
	wf := &v1alpha1.AgentTemplate{
		Spec: v1alpha1.AgentTemplateSpec{
			SandboxTemplateRef: v1alpha1.SandboxTemplateReference{Name: "tmpl"},
		},
	}
	claim := sandbox.BuildSandboxClaim("ns", sandbox.LaunchSpec{
		SessionID: "s1", Template: wf, SystemPrompt: "hi",
	})
	for _, e := range claim.Spec.Env {
		if e.ContainerName != sandbox.AgentContainerName {
			t.Errorf("env %s targets container %q, want default %q", e.Name, e.ContainerName, sandbox.AgentContainerName)
		}
	}
}

// --- ACP client dispatch ----------------------------------------------------

type recordingSink struct {
	messages   []string
	thoughts   []string
	toolCalls  []sandbox.ToolCallEvent
	permReq    sandbox.PermissionRequest
	permanswer sandbox.PermissionDecision
}

func (r *recordingSink) AgentMessage(_ context.Context, text string) {
	r.messages = append(r.messages, text)
}
func (r *recordingSink) AgentThought(_ context.Context, text string) {
	r.thoughts = append(r.thoughts, text)
}
func (r *recordingSink) ToolCall(_ context.Context, ev sandbox.ToolCallEvent) {
	r.toolCalls = append(r.toolCalls, ev)
}
func (r *recordingSink) Permission(_ context.Context, req sandbox.PermissionRequest) (sandbox.PermissionDecision, error) {
	r.permReq = req
	return r.permanswer, nil
}

func TestACPClientDispatchesAgentMessage(t *testing.T) {
	sink := &recordingSink{}
	c := sandbox.NewACPClient(sink, nil)
	err := c.SessionUpdate(context.Background(), acp.SessionNotification{
		SessionId: "s1",
		Update:    acp.UpdateAgentMessageText("hello world"),
	})
	if err != nil {
		t.Fatalf("SessionUpdate: %v", err)
	}
	if len(sink.messages) != 1 || sink.messages[0] != "hello world" {
		t.Errorf("messages = %v", sink.messages)
	}
}

func TestACPClientDispatchesToolCall(t *testing.T) {
	sink := &recordingSink{}
	c := sandbox.NewACPClient(sink, nil)
	err := c.SessionUpdate(context.Background(), acp.SessionNotification{
		SessionId: "s1",
		Update: acp.StartToolCall(
			acp.ToolCallId("call_1"),
			"Reading files",
			acp.WithStartStatus(acp.ToolCallStatusPending),
			acp.WithStartKind(acp.ToolKindRead),
		),
	})
	if err != nil {
		t.Fatalf("SessionUpdate: %v", err)
	}
	if len(sink.toolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(sink.toolCalls))
	}
	tc := sink.toolCalls[0]
	if tc.ID != "call_1" || tc.Title != "Reading files" {
		t.Errorf("tool call = %+v", tc)
	}
}

func TestACPClientPermissionSelectsOption(t *testing.T) {
	sink := &recordingSink{permanswer: sandbox.PermissionDecision{OptionID: "allow"}}
	c := sandbox.NewACPClient(sink, nil)
	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: "s1",
		ToolCall:  acp.ToolCallUpdate{ToolCallId: acp.ToolCallId("call_2"), Title: acp.Ptr("Edit config")},
		Options: []acp.PermissionOption{
			{Kind: acp.PermissionOptionKindAllowOnce, Name: "Allow", OptionId: acp.PermissionOptionId("allow")},
			{Kind: acp.PermissionOptionKindRejectOnce, Name: "Deny", OptionId: acp.PermissionOptionId("deny")},
		},
	})
	if err != nil {
		t.Fatalf("RequestPermission: %v", err)
	}
	if resp.Outcome.Selected == nil || string(resp.Outcome.Selected.OptionId) != "allow" {
		t.Errorf("expected selected option allow, got %+v", resp.Outcome)
	}
	if sink.permReq.ToolCallID != "call_2" || sink.permReq.Title != "Edit config" {
		t.Errorf("permission request not forwarded correctly: %+v", sink.permReq)
	}
	if len(sink.permReq.Options) != 2 {
		t.Errorf("expected 2 options forwarded, got %d", len(sink.permReq.Options))
	}
}

func TestACPClientPermissionCancelled(t *testing.T) {
	sink := &recordingSink{permanswer: sandbox.PermissionDecision{Cancelled: true}}
	c := sandbox.NewACPClient(sink, nil)
	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: "s1",
		ToolCall:  acp.ToolCallUpdate{ToolCallId: acp.ToolCallId("c")},
		Options:   []acp.PermissionOption{{Name: "Allow", OptionId: acp.PermissionOptionId("allow")}},
	})
	if err != nil {
		t.Fatalf("RequestPermission: %v", err)
	}
	if resp.Outcome.Cancelled == nil {
		t.Errorf("expected cancelled outcome, got %+v", resp.Outcome)
	}
}
