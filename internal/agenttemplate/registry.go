// Package agenttemplate resolves which AgentTemplate a chat channel runs: the
// singleton ChannelConfig maps a channel id to a template name, and the name
// resolves to its AgentTemplate definition. Both reads go through a
// controller-runtime cache-backed reader, so lookups are served from a local
// informer cache rather than hitting the API server on every Slack request —
// which also means binding changes take effect without a restart.
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

// ErrChannelNotBound is returned when a channel has no agent template bound to
// it (the singleton ChannelConfig is missing or doesn't map the channel).
var ErrChannelNotBound = errors.New("agenttemplate: no agent template bound to this channel")

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

// SlackChannelTemplate returns the agent template name bound to the Slack
// channelID in the singleton ChannelConfig (its slackChannelTemplates map —
// hence the Slack in the name; other channel types would get their own map
// and method). A missing ChannelConfig or an unmapped channel returns
// ErrChannelNotBound.
func (r *Registry) SlackChannelTemplate(ctx context.Context, channelID string) (string, error) {
	var cfg v1alpha1.ChannelConfig
	key := client.ObjectKey{Namespace: r.namespace, Name: v1alpha1.ChannelConfigName}
	if err := r.reader.Get(ctx, key, &cfg); err != nil {
		if apierrors.IsNotFound(err) {
			r.tel.Debug(ctx, "no channel config", "channel", channelID)
			return "", fmt.Errorf("%w: %q", ErrChannelNotBound, channelID)
		}
		return "", fmt.Errorf("get channel config: %w", err)
	}
	name, ok := cfg.Spec.SlackChannelTemplates[channelID]
	if !ok || name == "" {
		r.tel.Debug(ctx, "channel not bound to an agent template", "channel", channelID)
		return "", fmt.Errorf("%w: %q", ErrChannelNotBound, channelID)
	}
	r.tel.Debug(ctx, "resolved channel binding", "channel", channelID, "name", name)
	return name, nil
}
