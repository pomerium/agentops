//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentsv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sbxclientv1 "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/typed/api/v1beta1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func TestSandboxLifecyclePatchesK3s(t *testing.T) {
	if os.Getenv(optInEnv) == "" {
		t.Skipf("opt-in e2e test; set %s=1 to run", optInEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ctr := startK3sContainer(ctx, t)
	restCfg, _ := k8sClients(ctx, t, ctr)
	installAgentSandboxCRD(ctx, t, restCfg, "sandboxes.agents.x-k8s.io")

	const ns = "default"
	const name = "lifecycle-e2e"
	const claimName = "smc-lifecycle-e2e"
	const lease = time.Hour

	typed, err := sbxclientv1.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("sandbox clientset: %v", err)
	}
	if _, err := typed.Sandboxes(ns).Create(ctx, &agentsv1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: agentsv1.SandboxSpec{
			SandboxBlueprint: agentsv1.SandboxBlueprint{
				PodTemplate: agentsv1.PodTemplate{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "agent", Image: "registry.k8s.io/pause:3.10"}},
					},
				},
			},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create Sandbox: %v", err)
	}

	sandboxes, err := sandbox.NewSandboxClient(restCfg, ns)
	if err != nil {
		t.Fatalf("NewSandboxClient: %v", err)
	}
	orch := sandbox.New(boundClaim{name: claimName, sandbox: name}, nil, sandboxes, nil,
		sandbox.WithNamespace(ns), sandbox.WithLease(lease))

	before := time.Now()
	until, err := orch.ExtendLease(ctx, claimName)
	after := time.Now()
	if err != nil {
		t.Fatalf("ExtendLease: %v", err)
	}
	if until.Before(before.Add(lease)) || until.After(after.Add(lease)) {
		t.Errorf("ExtendLease returned %v, want now+%s", until, lease)
	}
	got := getSandbox(ctx, t, sandboxes, name)
	if got.Spec.ShutdownTime == nil {
		t.Fatal("ExtendLease did not set spec.shutdownTime: the apiserver pruned the field, so the patch names the wrong path (a Sandbox's shutdownTime sits directly under spec, not under spec.lifecycle like a claim's)")
	}
	requireLease(t, "ExtendLease", got, before, after, lease)
	first := got.Spec.ShutdownTime.Time

	time.Sleep(1100 * time.Millisecond)
	before = time.Now()
	if _, err := orch.ExtendLease(ctx, claimName); err != nil {
		t.Fatalf("second ExtendLease: %v", err)
	}
	after = time.Now()
	got = getSandbox(ctx, t, sandboxes, name)
	requireLease(t, "second ExtendLease", got, before, after, lease)
	if !got.Spec.ShutdownTime.Time.After(first) {
		t.Errorf("second ExtendLease left spec.shutdownTime at %v; it must slide past %v", got.Spec.ShutdownTime.Time, first)
	}
	extended := got.Spec.ShutdownTime.Time

	if err := orch.Suspend(ctx, claimName); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	got = getSandbox(ctx, t, sandboxes, name)
	if got.Spec.OperatingMode != agentsv1.SandboxOperatingModeSuspended {
		t.Errorf("after Suspend spec.operatingMode = %q, want %q", got.Spec.OperatingMode, agentsv1.SandboxOperatingModeSuspended)
	}
	if got.Spec.ShutdownTime == nil || !got.Spec.ShutdownTime.Time.Equal(extended) {
		t.Errorf("Suspend changed the lease: spec.shutdownTime = %v, want %v", got.Spec.ShutdownTime, extended)
	}

	before = time.Now()
	resumed, err := orch.Resume(ctx, claimName)
	after = time.Now()
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed != name {
		t.Errorf("Resume returned sandbox %q, want %q", resumed, name)
	}
	got = getSandbox(ctx, t, sandboxes, name)
	if got.Spec.OperatingMode != agentsv1.SandboxOperatingModeRunning {
		t.Errorf("after Resume spec.operatingMode = %q, want %q", got.Spec.OperatingMode, agentsv1.SandboxOperatingModeRunning)
	}
	requireLease(t, "Resume", got, before, after, lease)

	if containers := got.Spec.PodTemplate.Spec.Containers; len(containers) != 1 || containers[0].Name != "agent" {
		t.Errorf("the lifecycle patches changed the pod template; they must merge into spec, not replace it: %+v", containers)
	}

	if err := sandboxes.Patch(ctx, name, []byte(`{"spec":{"operatingMode":"Paused"}}`)); err == nil {
		t.Error("the apiserver accepted an operatingMode outside the enum; a typo would then silently do nothing")
	}
}

func getSandbox(ctx context.Context, t *testing.T, sandboxes sandbox.SandboxClient, name string) *agentsv1.Sandbox {
	t.Helper()
	got, err := sandboxes.Get(ctx, name)
	if err != nil {
		t.Fatalf("get Sandbox %s: %v", name, err)
	}
	return got
}

func requireLease(t *testing.T, what string, got *agentsv1.Sandbox, before, after time.Time, lease time.Duration) {
	t.Helper()
	if got.Spec.ShutdownTime == nil {
		t.Fatalf("after %s spec.shutdownTime is unset", what)
	}
	at := got.Spec.ShutdownTime.Time
	low, high := before.Add(lease).Truncate(time.Second), after.Add(lease)
	if at.Before(low) || at.After(high) {
		t.Errorf("after %s spec.shutdownTime = %v, want between %v and %v (now+%s)", what, at, low, high, lease)
	}
}

type boundClaim struct {
	name    string
	sandbox string
}

func (c boundClaim) Create(context.Context, *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error) {
	return nil, errors.New("the lifecycle test binds one existing claim; it creates none")
}

func (c boundClaim) Get(_ context.Context, name string) (*sbxv1.SandboxClaim, error) {
	if name != c.name {
		return nil, fmt.Errorf("the lifecycle test knows claim %q, not %q", c.name, name)
	}
	return &sbxv1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: sbxv1.SandboxClaimStatus{
			Conditions:    []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			SandboxStatus: sbxv1.SandboxStatus{Name: c.sandbox},
		},
	}, nil
}

func (c boundClaim) Delete(context.Context, string) error {
	return errors.New("the lifecycle test binds one existing claim; it deletes none")
}
