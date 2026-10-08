package app

import (
	"context"
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
			a.sweepWatched(ctx, since)
			since = tick.Add(-interval)
		}
	}
}

func (a *App) sweepWatched(ctx context.Context, since time.Time) {
	res, err := a.api.ListSessions(ctx, &pb.ListSessionsRequest{
		UpdatedSince: api.Timestamp(since),
	})
	if err != nil {
		a.log.WarnContext(ctx, "sweep: list sessions failed", "err", err)
		return
	}
	for _, view := range res.GetSessions() {
		if view.GetState() == api.StateSuspended {
			continue
		}
		m, err := a.loadMeta(ctx, view)
		if err != nil || !m.Watching {
			continue
		}
		a.renderWatchedEnding(ctx, view, m)
	}
}

func (a *App) renderWatchedEnding(ctx context.Context, view *pb.SessionView, m sessionMeta) {
	t := threadFromMeta(view, m)
	if ok, _ := a.registerThread(t); !ok {
		return
	}
	defer a.unregisterThread(t)

	res, err := a.api.ListEvents(ctx, &pb.ListEventsRequest{
		Ref: a.ref(view.GetId()), AfterSeq: m.LastSeq,
	})
	if err != nil {
		a.log.WarnContext(ctx, "sweep: read a paused session's events failed",
			"session", view.GetId(), "err", err)
		return
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
