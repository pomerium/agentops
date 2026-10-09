package channelmap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"sync"
	"time"

	"sigs.k8s.io/yaml"
)

var ErrChannelNotBound = errors.New("channelmap: no agent template bound to this channel")

type file struct {
	Channels map[string]string `json:"channels"`
	Default  string            `json:"default,omitempty"`
}

type Map struct {
	path string
	log  *slog.Logger

	mu     sync.RWMutex
	byID   map[string]string
	byDflt string
	source string
}

func New(path string, log *slog.Logger) (*Map, error) {
	if log == nil {
		log = slog.Default()
	}
	m := &Map{path: path, log: log, byID: map[string]string{}}
	if path == "" {
		log.Warn("no channel map configured; no Slack channel will start a session")
		return m, nil
	}
	if err := m.reload(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Map) Watch(ctx context.Context, interval time.Duration) {
	if m.path == "" {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.reload(); err != nil {
				m.log.WarnContext(ctx, "re-reading the channel map failed; keeping the previous one",
					"path", m.path, "err", err)
			}
		}
	}
}

func (m *Map) reload() error {
	var doc file
	raw, err := os.ReadFile(m.path)
	missing := errors.Is(err, os.ErrNotExist)
	switch {
	case missing:
	case err != nil:
		return fmt.Errorf("read channel map %s: %w", m.path, err)
	default:
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("parse channel map %s: %w", m.path, err)
		}
	}
	next := make(map[string]string, len(doc.Channels))
	for id, name := range doc.Channels {
		if id == "" || name == "" {
			continue
		}
		next[id] = name
	}
	source := "file"
	if missing {
		source = "none"
	}
	m.mu.Lock()
	changed := doc.Default != m.byDflt || !maps.Equal(next, m.byID) || source != m.source
	m.byID, m.byDflt, m.source = next, doc.Default, source
	m.mu.Unlock()
	switch {
	case !changed:
	case missing:
		m.log.Warn("no channel map at the configured path; no Slack channel will start a session", "path", m.path)
	default:
		m.log.Info("channel map loaded", "path", m.path, "channels", len(next), "default", doc.Default)
	}
	return nil
}

func (m *Map) TemplateFor(_ context.Context, channelID string) (string, error) {
	m.mu.RLock()
	name, ok := m.byID[channelID]
	if !ok {
		name = m.byDflt
	}
	m.mu.RUnlock()
	if name == "" {
		return "", fmt.Errorf("%w: %q", ErrChannelNotBound, channelID)
	}
	return name, nil
}
