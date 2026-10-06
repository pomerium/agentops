package harnessapi

import (
	"sync"
	"time"
)

type ttlMemo[V any] struct {
	ttl   time.Duration
	now   func() time.Time
	stamp func(V) time.Time

	mu        sync.Mutex
	entries   map[string]V
	nextPrune time.Time
}

func newTTLMemo[V any](ttl time.Duration, stamp func(V) time.Time) ttlMemo[V] {
	return ttlMemo[V]{ttl: ttl, now: time.Now, stamp: stamp, entries: map[string]V{}}
}

func (m *ttlMemo[V]) expired(v V, now time.Time) bool {
	at := m.stamp(v)
	return !at.IsZero() && now.Sub(at) > m.ttl
}

func (m *ttlMemo[V]) prune(now time.Time) {
	if now.Before(m.nextPrune) {
		return
	}
	for k, v := range m.entries {
		if m.expired(v, now) {
			delete(m.entries, k)
		}
	}
	m.nextPrune = now.Add(m.ttl)
}
