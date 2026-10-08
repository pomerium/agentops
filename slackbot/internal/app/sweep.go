package app

import (
	"context"
	"errors"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

const DefaultSweepInterval = 2 * time.Minute

func (a *App) RunSweeper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var since time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-ticker.C:
			if a.sweepWatched(ctx, since) {
				since = tick.Add(-interval)
			}
		}
	}
}

func (a *App) sweepWatched(ctx context.Context, since time.Time) bool {
	res, err := a.api.ListSessions(ctx, &pb.ListSessionsRequest{
		UpdatedSince: api.Timestamp(since),
	})
	if err != nil {
		a.log.WarnContext(ctx, "sweep: list sessions failed; the next sweep covers this window again", "err", err)
		return false
	}
	complete := true
	for _, view := range res.GetSessions() {
		if !a.reconcile(ctx, view) {
			complete = false
		}
	}
	return complete
}

func (a *App) reconcile(ctx context.Context, view *pb.SessionView) bool {
	if a.holds(view.GetId()) {
		return true
	}
	live, paused := api.Live(view.GetState()), view.GetState() == api.StateSuspended
	m, err := a.loadMeta(ctx, view)
	switch {
	case errors.Is(err, errNoSlackState):
		identity, ok := identityOf(view)
		if !ok || !live || paused {
			return true
		}
		m = identity
	case err != nil:
		a.log.WarnContext(ctx, "could not read a session's thread; the next sweep tries again",
			"session", view.GetId(), "err", err)
		return false
	}
	switch {
	case paused && !m.Watching:
		return a.renderMissed(ctx, view, m, true)
	case paused:
		return true
	case live:
		a.followAgain(ctx, view, m)
		return true
	case m.Watching:
		return a.renderMissed(ctx, view, m, false)
	}
	return true
}

func (a *App) renderMissed(ctx context.Context, view *pb.SessionView, m sessionMeta, keepWatching bool) bool {
	t := threadFromMeta(view, m)
	if ok, _ := a.registerThread(t); !ok {
		return true
	}
	defer a.unregisterThread(t)

	t.catchingUp = true
	t.setState(api.StateSuspended)
	last := a.turnStartBefore(ctx, view.GetId(), m.LastSeq)
	boundary, inTurn, rendered := last, false, 0
	for {
		res, err := a.api.ListEvents(ctx, &pb.ListEventsRequest{
			Ref: a.ref(view.GetId()), AfterSeq: last,
		})
		if err != nil {
			a.log.WarnContext(ctx, "read a session's events failed; the next sweep continues from here",
				"session", view.GetId(), "after_seq", last, "err", err)
			a.saveMeta(ctx, t, func(meta *sessionMeta) { meta.LastSeq = boundary })
			return false
		}
		events := res.GetEvents()
		for _, ev := range events {
			var atBoundary bool
			if atBoundary, inTurn = turnBoundary(ev, inTurn); atBoundary {
				boundary = ev.GetSeq()
			}
			a.renderEvent(ctx, t, "", ev)
			last = ev.GetSeq()
		}
		rendered += len(events)
		if len(events) == 0 || last >= view.GetLastSeq() {
			break
		}
	}

	a.saveMeta(ctx, t, func(meta *sessionMeta) {
		meta.Watching = keepWatching
		meta.LastSeq = last
	})
	a.log.InfoContext(ctx, "rendered the events of a session the bot was not following",
		"session", view.GetId(), "state", view.GetState(), "events", rendered, "watching", keepWatching)
	return true
}

func (a *App) watchSuspended(ctx context.Context, t *thread, seq int64) {
	a.saveMeta(ctx, t, func(m *sessionMeta) {
		m.Watching = true
		m.LastSeq = seq
	})
	if t.stop != nil {
		t.stop()
	}
}
