package harnessapi

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

type binding struct {
	sessionID string
	owner     *owner
	claimName string
	runID     string
	session   LiveSession
	sink      *logSink

	ready chan struct{}

	done chan struct{}

	consumed chan struct{}

	mu     sync.Mutex
	closed bool
	turns  []string

	sendMu sync.Mutex
	gated  bool
	held   []*agentlinkpb.Prompt

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
		consumed:  make(chan struct{}),
	}
	b.touch()
	return b
}

func (b *binding) touch() {
	b.lastActivity.Store(time.Now().UnixNano())
	b.idleWarned.Store(false)
}

func (b *binding) idleFor(now time.Time) time.Duration {
	b.mu.Lock()
	busy := len(b.turns) > 0
	b.mu.Unlock()
	if busy {
		return 0
	}
	return now.Sub(time.Unix(0, b.lastActivity.Load()))
}

func (b *binding) enter(turnID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	if !slices.Contains(b.turns, turnID) {
		b.turns = append(b.turns, turnID)
	}
	b.touch()
	return true
}

func (b *binding) leave(turnID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if i := slices.Index(b.turns, turnID); i >= 0 {
		b.turns = slices.Delete(b.turns, i, i+1)
	}
	b.touch()
}

func (b *binding) setOutstanding(turnIDs []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.turns = slices.Clone(turnIDs)
}

func (b *binding) outstanding() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.turns)
}

func (b *binding) close(ifIdleFor time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	if ifIdleFor > 0 && (len(b.turns) > 0 || time.Since(time.Unix(0, b.lastActivity.Load())) < ifIdleFor) {
		return false
	}
	b.closed = true
	close(b.done)
	return true
}
