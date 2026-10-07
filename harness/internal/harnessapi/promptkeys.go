package harnessapi

import (
	"context"
	"time"
)

type promptKeys struct {
	ttlMemo[*promptClaim]
}

type promptClaim struct {
	done     chan struct{}
	turnID   string
	err      error
	accepted time.Time
}

const promptKeyTTL = 10 * time.Minute

func newPromptKeys() *promptKeys {
	return &promptKeys{newTTLMemo(promptKeyTTL, func(c *promptClaim) time.Time { return c.accepted })}
}

func (p *promptKeys) do(ctx context.Context, sessionID, key string, start func() (string, error)) (string, error) {
	k := sessionID + "/" + key
	p.mu.Lock()
	now := p.now()
	p.prune(now)
	c, repeat := p.entries[k]
	if repeat && p.expired(c, now) {
		repeat = false
	}
	if !repeat {
		c = &promptClaim{done: make(chan struct{})}
		p.entries[k] = c
	}
	p.mu.Unlock()

	if repeat {
		select {
		case <-c.done:
			return c.turnID, c.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	turnID, err := start()
	p.mu.Lock()
	c.turnID, c.err = turnID, err
	if err != nil {
		delete(p.entries, k)
	} else {
		c.accepted = p.now()
	}
	p.mu.Unlock()
	close(c.done)
	return turnID, err
}
