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
		if view.GetState() == api.StateSuspended {
			continue
		}
		m, err := a.loadMeta(ctx, view)
		switch {
		case errors.Is(err, errNoSlackState):
			continue
		case err != nil:
			a.log.WarnContext(ctx, "sweep: could not read a session's thread; the next sweep covers it again",
				"session", view.GetId(), "err", err)
			complete = false
			continue
		case !m.Watching:
			continue
		}
		if !a.renderWatchedEnding(ctx, view, m) {
			complete = false
		}
	}
	return complete
}

func (a *App) renderWatchedEnding(ctx context.Context, view *pb.SessionView, m sessionMeta) bool {
	t := threadFromMeta(view, m)
	if ok, _ := a.registerThread(t); !ok {
		return true
	}
	defer a.unregisterThread(t)

	res, err := a.api.ListEvents(ctx, &pb.ListEventsRequest{
		Ref: a.ref(view.GetId()), AfterSeq: m.LastSeq,
	})
	if err != nil {
		a.log.WarnContext(ctx, "sweep: read a paused session's events failed",
			"session", view.GetId(), "err", err)
		return false
	}

	t.setState(api.StateSuspended)
	events := res.GetEvents()
	last := m.LastSeq
	for _, ev := range events {
		a.renderEvent(ctx, t, "", ev)
		last = ev.Seq
	}

	a.saveMeta(ctx, t, func(meta *sessionMeta) {
		meta.Watching = false
		meta.LastSeq = last
	})
	a.log.InfoContext(ctx, "sweep: rendered a paused session's ending",
		"session", view.GetId(), "state", view.GetState(), "events", len(events))
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
