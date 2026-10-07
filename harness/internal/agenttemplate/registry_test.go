package agenttemplate_test

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenttemplate"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

func agt(ns, name string) *v1alpha1.AgentTemplate {
	return &v1alpha1.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.AgentTemplateSpec{
			WarmPoolRef: v1alpha1.SandboxWarmPoolReference{Name: "claude-code"},
		},
	}
}

func TestResolveByName(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(agt("ns", "deploy-service"), agt("ns", "review-pr")).
		Build()

	reg := agenttemplate.New(c, "ns")

	got, err := reg.Resolve(ctx, "deploy-service")
	if err != nil {
		t.Fatalf("Resolve(deploy-service): %v", err)
	}
	if got.Name != "deploy-service" {
		t.Errorf("resolved %q, want deploy-service", got.Name)
	}

	if _, err := reg.Resolve(ctx, "does-not-exist"); !errors.Is(err, agenttemplate.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestResolveIgnoresOtherNamespaces(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(agt("other-ns", "deploy-service")).
		Build()
	reg := agenttemplate.New(c, "ns")
	if _, err := reg.Resolve(ctx, "deploy-service"); !errors.Is(err, agenttemplate.ErrNotFound) {
		t.Errorf("expected templates in other namespaces to be ignored, got %v", err)
	}
}
