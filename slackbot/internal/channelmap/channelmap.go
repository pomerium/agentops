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

	mu      sync.RWMutex
	byID    map[string]string
	byDflt  string
	loadErr error
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
	raw, err := os.ReadFile(m.path)
	if errors.Is(err, os.ErrNotExist) {
		m.log.Warn("no channel map at the configured path; no Slack channel will start a session", "path", m.path)
		return nil
	}
	if err != nil {
		return fmt.Errorf("read channel map %s: %w", m.path, err)
	}
	var doc file
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("parse channel map %s: %w", m.path, err)
	}
	next := make(map[string]string, len(doc.Channels))
	for id, name := range doc.Channels {
		if id == "" || name == "" {
			continue
		}
		next[id] = name
	}
	m.mu.Lock()
	changed := doc.Default != m.byDflt || !maps.Equal(next, m.byID)
	m.byID, m.byDflt = next, doc.Default
	m.mu.Unlock()
	if changed {
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
