package harnessapi

import (
	"sync"
	"sync/atomic"
	"time"
)

type binding struct {
	sessionID string
	owner     *owner
	claimName string
	session   LiveSession
	sink      *logSink

	ready chan struct{}

	done chan struct{}

	mu      sync.Mutex
	closed  bool
	settled *sync.Cond
	issued  uint64
	serving uint64

	busy atomic.Int32

	lastActivity atomic.Int64

	idleWarned atomic.Bool

	leaseUntil atomic.Int64
}

func newBinding(sessionID, claimName string, session LiveSession, sink *logSink) *binding {
	b := &binding{
		sessionID: sessionID,
		claimName: claimName,
		session:   session,
		sink:      sink,
		ready:     make(chan struct{}),
		done:      make(chan struct{}),
	}
	b.settled = sync.NewCond(&b.mu)
	b.touch()
	return b
}

func (b *binding) touch() {
	b.lastActivity.Store(time.Now().UnixNano())
	b.idleWarned.Store(false)
}

func (b *binding) idleFor(now time.Time) time.Duration {
	if b.busy.Load() > 0 {
		return 0
	}
	return now.Sub(time.Unix(0, b.lastActivity.Load()))
}

func (b *binding) enter() (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, false
	}
	b.busy.Add(1)
	b.touch()
	ticket := b.issued
	b.issued++
	return ticket, true
}

func (b *binding) awaitTurn(ticket uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.serving != ticket {
		b.settled.Wait()
	}
}

func (b *binding) leave() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.touch()
	b.serving++
	b.busy.Add(-1)
	b.settled.Broadcast()
}

func (b *binding) forfeit(ticket uint64) {
	b.awaitTurn(ticket)
	b.leave()
}

func (b *binding) close(ifIdleFor time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || ifIdleFor > 0 && b.idleFor(time.Now()) < ifIdleFor {
		return false
	}
	b.closed = true
	close(b.done)
	return true
}

func (b *binding) drain() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.busy.Load() > 0 {
		b.settled.Wait()
	}
}
