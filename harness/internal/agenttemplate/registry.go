package agenttemplate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

var ErrNotFound = errors.New("agenttemplate: no agent template with that name")

var ErrNoBinding = errors.New("agenttemplate: no client binding for this subject")

type Registry struct {
	reader    client.Reader
	namespace string
	tel       *telemetry.Component
}

func New(reader client.Reader, namespace string) *Registry {
	return &Registry{
		reader:    reader,
		namespace: namespace,
		tel:       telemetry.New(slog.Default(), "agenttemplate", slog.LevelDebug),
	}
}

func (r *Registry) Resolve(ctx context.Context, name string) (*v1alpha1.AgentTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	var tmpl v1alpha1.AgentTemplate
	key := client.ObjectKey{Namespace: r.namespace, Name: name}
	if err := r.reader.Get(ctx, key, &tmpl); err != nil {
		if apierrors.IsNotFound(err) {
			r.tel.Debug(ctx, "no agent template with name", "name", name)
			return nil, fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		return nil, fmt.Errorf("get agent template %q: %w", name, err)
	}
	r.tel.Debug(ctx, "resolved agent template", "name", name)
	return &tmpl, nil
}

func (r *Registry) List(ctx context.Context) ([]v1alpha1.AgentTemplate, error) {
	var list v1alpha1.AgentTemplateList
	if err := r.reader.List(ctx, &list, client.InNamespace(r.namespace)); err != nil {
		return nil, fmt.Errorf("list agent templates: %w", err)
	}
	slices.SortFunc(list.Items, func(a, b v1alpha1.AgentTemplate) int {
		return strings.Compare(a.Name, b.Name)
	})
	return list.Items, nil
}

func (r *Registry) ClientBinding(ctx context.Context, subject string) (*v1alpha1.ClientBinding, error) {
	if subject == "" {
		return nil, fmt.Errorf("%w: %q", ErrNoBinding, subject)
	}
	var list v1alpha1.ClientBindingList
	if err := r.reader.List(ctx, &list, client.InNamespace(r.namespace)); err != nil {
		return nil, fmt.Errorf("list client bindings: %w", err)
	}
	for i := range list.Items {
		if list.Items[i].Spec.Subject == subject {
			return &list.Items[i], nil
		}
	}
	r.tel.Debug(ctx, "no client binding for subject", "subject", subject)
	return nil, fmt.Errorf("%w: %q", ErrNoBinding, subject)
}
