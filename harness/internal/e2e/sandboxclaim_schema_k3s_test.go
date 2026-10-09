//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func TestSandboxClaimSchemaK3sV1beta1(t *testing.T) {
	if os.Getenv(optInEnv) == "" {
		t.Skipf("opt-in e2e test; set %s=1 to run", optInEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ctr := startK3sContainer(ctx, t)
	restCfg, _ := k8sClients(ctx, t, ctr)
	installAgentSandboxCRD(ctx, t, restCfg, "sandboxclaims.extensions.agents.x-k8s.io")

	const ns = "default"
	claims, err := sandbox.NewClaimClient(restCfg, ns)
	if err != nil {
		t.Fatalf("NewClaimClient: %v", err)
	}

	claim := sandbox.BuildSandboxClaim(ns, sandbox.LaunchSpec{
		SessionID: "schema-e2e",
		Template: &v1alpha1.AgentTemplate{
			Spec: v1alpha1.AgentTemplateSpec{
				WarmPoolRef: v1alpha1.SandboxWarmPoolReference{Name: "claude-code"},
			},
		},
		Endpoints: []sandbox.ProxiedEndpoint{{
			Name: "mcp-echo", ListenPort: sandbox.MCPListenPort(0),
			UpstreamURL: "https://echo.example.com/mcp",
		}},
	})

	created, err := claims.Create(ctx, claim)
	if err != nil {
		t.Fatalf("create v1beta1 SandboxClaim: %v", err)
	}
	t.Logf("apiserver accepted SandboxClaim %q", created.Name)

	got, err := claims.Get(ctx, created.Name)
	if err != nil {
		t.Fatalf("get SandboxClaim: %v", err)
	}

	if got.Spec.WarmPoolRef.Name != "claude-code" {
		t.Errorf("warmPoolRef.name = %q, want claude-code", got.Spec.WarmPoolRef.Name)
	}

	if len(got.Spec.Env) != 0 {
		t.Errorf("claim came back with env, which forfeits warm-pool adoption: %+v", got.Spec.Env)
	}
	if len(got.Spec.VolumeClaimTemplates) != 0 {
		t.Errorf("claim came back with volume claim templates, which forfeit adoption too: %+v",
			got.Spec.VolumeClaimTemplates)
	}
	rendered, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal round-tripped claim: %v", err)
	}
	if bytes.Contains(rendered, []byte("echo.example.com")) {
		t.Error("an endpoint reached the SandboxClaim; endpoints belong on the attach stream")
	}

	if got.Spec.Lifecycle != nil {
		t.Errorf("claim came back with a lifecycle %+v; the deadline is the Sandbox's lease, not the claim's", got.Spec.Lifecycle)
	}

	if err := claims.Delete(ctx, created.Name); err != nil {
		t.Errorf("delete SandboxClaim: %v", err)
	}
}
