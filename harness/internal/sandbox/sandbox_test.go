package sandbox_test

import (
	"context"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func TestBuildSandboxClaimCarriesNothingThatBlocksAdoption(t *testing.T) {
	wf := &v1alpha1.AgentTemplate{
		Spec: v1alpha1.AgentTemplateSpec{
			WarmPoolRef: v1alpha1.SandboxWarmPoolReference{Name: "pomerium-zero-claude-code"},
		},
	}
	deadline := time.Now().Add(30 * time.Minute)
	claim := sandbox.BuildSandboxClaim("agentops", sandbox.LaunchSpec{
		SessionID:    "abc123",
		Template:     wf,
		SystemPrompt: "You are a deployment assistant.",
		Endpoints: []sandbox.ProxiedEndpoint{{
			Name: "agno", ListenPort: 9100, UpstreamURL: "https://x.example.com",
		}},
		Deadline: deadline,
	})

	if claim.Namespace != "agentops" {
		t.Errorf("namespace = %q", claim.Namespace)
	}
	if claim.Spec.WarmPoolRef.Name != "pomerium-zero-claude-code" {
		t.Errorf("warm pool ref = %q, want pomerium-zero-claude-code", claim.Spec.WarmPoolRef.Name)
	}
	if len(claim.Spec.Env) != 0 {
		t.Errorf("claim carries %d env vars; any at all forfeits warm-pool adoption: %+v",
			len(claim.Spec.Env), claim.Spec.Env)
	}
	if len(claim.Spec.VolumeClaimTemplates) != 0 {
		t.Errorf("claim carries %d volume claim templates; they forfeit adoption too",
			len(claim.Spec.VolumeClaimTemplates))
	}
	if claim.Spec.Lifecycle == nil || claim.Spec.Lifecycle.ShutdownTime == nil {
		t.Fatal("expected lifecycle shutdown time to be set from deadline")
	}
}

type recordingSink struct {
	messages   []string
	thoughts   []string
	toolCalls  []sandbox.ToolCallEvent
	permReq    sandbox.PermissionRequest
	permanswer sandbox.PermissionDecision
	usage      []sandbox.UsageEvent
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
func (r *recordingSink) Usage(_ context.Context, ev sandbox.UsageEvent) {
	r.usage = append(r.usage, ev)
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

func TestACPClientDispatchesUsage(t *testing.T) {
	sink := &recordingSink{}
	c := sandbox.NewACPClient(sink, nil)
	err := c.SessionUpdate(context.Background(), acp.SessionNotification{
		SessionId: "s1",
		Update: acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
			SessionUpdate: "usage_update",
			Size:          200000,
			Used:          1234,
			Cost:          &acp.Cost{Amount: 0.42, Currency: "USD"},
		}},
	})
	if err != nil {
		t.Fatalf("SessionUpdate: %v", err)
	}
	if len(sink.usage) != 1 {
		t.Fatalf("usage events = %d, want 1", len(sink.usage))
	}
	got := sink.usage[0]
	if got.ContextWindow != 200000 || got.ContextUsed != 1234 || got.CostUSD != 0.42 {
		t.Errorf("usage = %+v", got)
	}
}

func TestACPClientCarriesToolInput(t *testing.T) {
	sink := &recordingSink{}
	c := sandbox.NewACPClient(sink, nil)
	err := c.SessionUpdate(context.Background(), acp.SessionNotification{
		SessionId: "s1",
		Update: acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
			ToolCallId: "tc-1",
			Title:      "Read file",
			Status:     acp.ToolCallStatusInProgress,
			RawInput:   map[string]any{"path": "main.go"},
		}},
	})
	if err != nil {
		t.Fatalf("SessionUpdate: %v", err)
	}
	if len(sink.toolCalls) != 1 {
		t.Fatalf("tool calls = %d, want 1", len(sink.toolCalls))
	}
	input, ok := sink.toolCalls[0].RawInput.(map[string]any)
	if !ok || input["path"] != "main.go" {
		t.Errorf("raw input = %#v", sink.toolCalls[0].RawInput)
	}
}
