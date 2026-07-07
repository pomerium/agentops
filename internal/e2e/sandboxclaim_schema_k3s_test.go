//go:build e2e

// End-to-end test that the SandboxClaim object agentops builds is accepted by a
// real Kubernetes API server running the agent-sandbox v0.5.0 SandboxClaim CRD.
//
// This is the integration half of the v1alpha1 → v1beta1 cutover: it proves our
// generated v1beta1 client (sandbox.NewClaimClient) and the object shape
// produced by sandbox.BuildSandboxClaim round-trip against the published v1beta1
// OpenAPI schema. Before the migration the client targeted
// extensions.agents.x-k8s.io/v1alpha1 and emitted spec.sandboxTemplateRef, which
// the v1beta1 schema (required spec.warmPoolRef, no templateRef) rejects.
//
// It installs only the CRD — no controller, no conversion webhook. v1beta1 is
// the storage version, so creating/reading at v1beta1 needs no conversion; to be
// fully independent of the upstream webhook we install only the CRD's v1beta1
// version with conversion strategy None.
//
// Run it:
//
//	AGENTOPS_E2E=1 go test -tags e2e -timeout 15m ./internal/e2e/... -run SandboxClaimSchema -v
package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/sandbox"
)

func TestSandboxClaimSchemaK3sV1beta1(t *testing.T) {
	if os.Getenv(optInEnv) == "" {
		t.Skipf("opt-in e2e test; set %s=1 to run", optInEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// API-server-only test: no sandbox pods, so a bare k3s (no image loading).
	ctr := startK3sContainer(ctx, t)
	restCfg, _ := k8sClients(ctx, t, ctr)
	installSandboxClaimCRD(ctx, t, restCfg)

	const ns = "default"
	claims, err := sandbox.NewClaimClient(restCfg, ns)
	if err != nil {
		t.Fatalf("NewClaimClient: %v", err)
	}

	deadline := time.Now().Add(30 * time.Minute)
	claim := sandbox.BuildSandboxClaim(ns, sandbox.LaunchSpec{
		SessionID: "schema-e2e",
		Template: &v1alpha1.AgentTemplate{
			Spec: v1alpha1.AgentTemplateSpec{
				WarmPoolRef: v1alpha1.SandboxWarmPoolReference{Name: "claude-code"},
			},
		},
		Deadline: deadline,
	})

	// The real apiserver validates this against the v1beta1 OpenAPI schema; a
	// v1alpha1-shaped object (sandboxTemplateRef, no required warmPoolRef) would
	// be rejected here.
	created, err := claims.Create(ctx, claim)
	if err != nil {
		t.Fatalf("create v1beta1 SandboxClaim: %v", err)
	}
	t.Logf("apiserver accepted SandboxClaim %q", created.Name)

	got, err := claims.Get(ctx, created.Name)
	if err != nil {
		t.Fatalf("get SandboxClaim: %v", err)
	}

	// The required warmPoolRef survived the round-trip exactly as built.
	if got.Spec.WarmPoolRef.Name != "claude-code" {
		t.Errorf("warmPoolRef.name = %q, want claude-code", got.Spec.WarmPoolRef.Name)
	}
	// The per-run agent context env is preserved and targets the agent container.
	var sawSession bool
	for _, e := range got.Spec.Env {
		if e.Name != sandbox.EnvSessionID {
			continue
		}
		sawSession = true
		if e.Value != "schema-e2e" {
			t.Errorf("%s env = %q, want schema-e2e", e.Name, e.Value)
		}
		if e.ContainerName != sandbox.AgentContainerName {
			t.Errorf("%s targets container %q, want %q", e.Name, e.ContainerName, sandbox.AgentContainerName)
		}
	}
	if !sawSession {
		t.Errorf("claim env missing %s after round-trip: %+v", sandbox.EnvSessionID, got.Spec.Env)
	}
	// The session-TTL lifecycle (Deadline → ShutdownTime) is preserved.
	if got.Spec.Lifecycle == nil || got.Spec.Lifecycle.ShutdownTime == nil {
		t.Errorf("lifecycle shutdown time lost on round-trip: %+v", got.Spec.Lifecycle)
	}

	// Delete exercises the v1beta1 client's Delete path too.
	if err := claims.Delete(ctx, created.Name); err != nil {
		t.Errorf("delete SandboxClaim: %v", err)
	}
}

// installSandboxClaimCRD applies the agent-sandbox v0.5.0 sandboxclaims CRD from
// testdata, reduced to its v1beta1 (storage) version with conversion disabled,
// and waits for it to become Established. Reducing to a single version lets the
// test run without the upstream conversion webhook (which v1beta1-only traffic
// never needs); the v1beta1 schema we validate against is unchanged.
func installSandboxClaimCRD(ctx context.Context, t *testing.T, cfg *rest.Config) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t),
		"internal", "e2e", "testdata", "agent-sandbox-v0.5.0-sandboxclaim-crd.yaml"))
	if err != nil {
		t.Fatalf("read CRD testdata: %v", err)
	}
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("unmarshal CRD: %v", err)
	}

	var v1beta1Only []apiextv1.CustomResourceDefinitionVersion
	for _, v := range crd.Spec.Versions {
		if v.Name == "v1beta1" {
			v.Served, v.Storage = true, true
			v1beta1Only = append(v1beta1Only, v)
		}
	}
	if len(v1beta1Only) != 1 {
		t.Fatalf("expected exactly one v1beta1 version in the CRD, found %d", len(v1beta1Only))
	}
	crd.Spec.Versions = v1beta1Only
	crd.Spec.Conversion = &apiextv1.CustomResourceConversion{Strategy: apiextv1.NoneConverter}
	crd.ResourceVersion = ""

	cs, err := apiextclient.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("apiextensions client: %v", err)
	}
	if _, err := cs.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, &crd, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create CRD %q: %v", crd.Name, err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		got, gerr := cs.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if gerr == nil {
			for _, c := range got.Status.Conditions {
				if c.Type == apiextv1.Established && c.Status == apiextv1.ConditionTrue {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("CRD %q not Established within 60s (last err: %v)", crd.Name, gerr)
		}
		time.Sleep(time.Second)
	}
}
