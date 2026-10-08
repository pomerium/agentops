package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

const outboxMax = 8 << 20

var errReplayGone = errors.New("the replay point is no longer held")

type outbox struct {
	mu     sync.Mutex
	cond   *sync.Cond
	events []*agentlinkpb.AgentEvent
	size   int
	max    int
	last   uint64
	acked  uint64
	open   bool
}

func newOutbox(max int) *outbox {
	o := &outbox{max: max}
	o.cond = sync.NewCond(&o.mu)
	return o
}

func (o *outbox) append(ev *agentlinkpb.AgentEvent, urgent bool) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	for !urgent && !o.open && o.size >= o.max {
		o.cond.Wait()
	}
	o.last++
	ev.Seq = o.last
	o.events = append(o.events, ev)
	o.size += proto.Size(ev)
	o.cond.Broadcast()
	return ev.Seq
}

func (o *outbox) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.open = true
	o.cond.Broadcast()
}

func (o *outbox) ack(n uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if n <= o.acked || n > o.last {
		return
	}
	drop := int(n - o.acked)
	for _, ev := range o.events[:drop] {
		o.size -= proto.Size(ev)
	}
	o.events = append([]*agentlinkpb.AgentEvent(nil), o.events[drop:]...)
	o.acked = n
	o.cond.Broadcast()
}

func (o *outbox) check(cursor uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.checkLocked(cursor)
}

func (o *outbox) checkLocked(cursor uint64) error {
	switch {
	case cursor < o.acked:
		return fmt.Errorf("%w: %d is before the acked event %d", errReplayGone, cursor, o.acked)
	case cursor > o.last:
		return fmt.Errorf("replay point %d is past the last event %d", cursor, o.last)
	}
	return nil
}

func (o *outbox) after(ctx context.Context, cursor uint64) ([]*agentlinkpb.AgentEvent, error) {
	stop := context.AfterFunc(ctx, func() {
		o.mu.Lock()
		o.cond.Broadcast()
		o.mu.Unlock()
	})
	defer stop()
	o.mu.Lock()
	defer o.mu.Unlock()
	for {
		if err := o.checkLocked(cursor); err != nil {
			return nil, err
		}
		if o.last > cursor {
			return append([]*agentlinkpb.AgentEvent(nil), o.events[cursor-o.acked:]...), nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		o.cond.Wait()
	}
}
