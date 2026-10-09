package sandbox_test

import (
	"testing"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func TestBuildSandboxClaimCarriesNothingThatBlocksAdoption(t *testing.T) {
	wf := &v1alpha1.AgentTemplate{
		Spec: v1alpha1.AgentTemplateSpec{
			WarmPoolRef: v1alpha1.SandboxWarmPoolReference{Name: "pomerium-zero-claude-code"},
		},
	}
	claim := sandbox.BuildSandboxClaim("agentops", sandbox.LaunchSpec{
		SessionID:    "abc123",
		Template:     wf,
		SystemPrompt: "You are a deployment assistant.",
		Endpoints: []sandbox.ProxiedEndpoint{{
			Name: "agno", ListenPort: 9100, UpstreamURL: "https://x.example.com",
		}},
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
	if claim.Spec.Lifecycle != nil {
		t.Errorf("claim carries a lifecycle %+v; the deadline is the Sandbox's lease, not the claim's", claim.Spec.Lifecycle)
	}
}
