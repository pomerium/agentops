// Package agenttemplate resolves an agent template name to its AgentTemplate
// definition. It reads templates from a controller-runtime cache-backed reader,
// so lookups are served from a local informer cache rather than hitting the API
// server on every Slack request.
package agenttemplate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/telemetry"
)

// ErrNotFound is returned when no AgentTemplate matches a name.
var ErrNotFound = errors.New("agenttemplate: no agent template with that name")

// Registry resolves AgentTemplate names to their definitions within a
// namespace.
type Registry struct {
	reader    client.Reader
	namespace string
	tel       *telemetry.Component
}

// New returns a Registry backed by the given reader (typically a
// controller-runtime cache) scoped to namespace. It logs through the default
// slog logger (set by main at the configured level).
func New(reader client.Reader, namespace string) *Registry {
	return &Registry{
		reader:    reader,
		namespace: namespace,
		tel:       telemetry.New(slog.Default(), "agenttemplate", slog.LevelDebug),
	}
}

// Resolve returns the AgentTemplate named name (its metadata.name) within the
// registry's namespace, or ErrNotFound.
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

// List returns all AgentTemplates in the registry's namespace.
func (r *Registry) List(ctx context.Context) ([]v1alpha1.AgentTemplate, error) {
	list, err := r.list(ctx)
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *Registry) list(ctx context.Context) (*v1alpha1.AgentTemplateList, error) {
	var list v1alpha1.AgentTemplateList
	if err := r.reader.List(ctx, &list, client.InNamespace(r.namespace)); err != nil {
		return nil, fmt.Errorf("list agent templates: %w", err)
	}
	return &list, nil
}
