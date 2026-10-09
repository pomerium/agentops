package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

type flakyLister struct {
	harnessapipbconnect.HarnessAPIServiceClient
	mu    sync.Mutex
	calls int
	since []time.Time
	stop  context.CancelFunc
}

func (f *flakyLister) ListSessions(_ context.Context, req *pb.ListSessionsRequest) (*pb.ListSessionsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.since = append(f.since, api.Time(req.GetUpdatedSince()))
	if f.calls < 3 {
		return nil, errors.New("harness unavailable")
	}
	f.stop()
	return &pb.ListSessionsResponse{}, nil
}

func TestAFailedSweepDoesNotMoveTheSweepWindow(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	lister := &flakyLister{stop: stop}
	a := New(lister, nil, nil)
	started := time.Now()

	a.RunSweeper(ctx, 10*time.Millisecond)

	lister.mu.Lock()
	defer lister.mu.Unlock()
	if len(lister.since) < 3 {
		t.Fatalf("the sweeper stopped after %d sweeps", len(lister.since))
	}
	if third := lister.since[2]; third.After(started) {
		t.Errorf("after two failed sweeps the window starts at %v, after the sweeper started (%v); endings in between are skipped",
			third, started)
	}
}
