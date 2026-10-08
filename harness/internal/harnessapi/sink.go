package harnessapi

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

type logSink struct {
	svc         *Service
	sessionID   string
	permTimeout time.Duration
	expireRetry time.Duration

	mu      sync.Mutex
	waiters map[string]*permissionWaiter
	stopped bool
	firing  sync.WaitGroup
}

type permissionWaiter struct {
	turnID   string
	options  []string
	deadline time.Time
	expire   func(requestID string)
	timer    *time.Timer
}

func newLogSink(svc *Service, sessionID string, permTimeout time.Duration) *logSink {
	return &logSink{
		svc:         svc,
		sessionID:   sessionID,
		permTimeout: permTimeout,
		expireRetry: time.Second,
		waiters:     map[string]*permissionWaiter{},
	}
}

func (s *logSink) deadline() time.Time { return time.Now().Add(s.permTimeout) }

func (s *logSink) await(req *agentlinkpb.PermissionRequest, deadline time.Time, expire func(requestID string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.waiters[req.GetRequestId()]; ok || s.stopped {
		return
	}
	offered := make([]string, 0, len(req.GetOptions()))
	for _, o := range req.GetOptions() {
		offered = append(offered, o.GetId())
	}
	id := req.GetRequestId()
	w := &permissionWaiter{turnID: req.GetTurnId(), options: offered, deadline: deadline, expire: expire}
	s.waiters[id] = w
	s.armLocked(id, w, time.Until(deadline))
}

func (s *logSink) armLocked(id string, w *permissionWaiter, after time.Duration) {
	w.timer = time.AfterFunc(after, func() {
		if s.fire() {
			defer s.firing.Done()
			w.expire(id)
		}
	})
}

func (s *logSink) restore(id string, w *permissionWaiter, after time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.waiters[id]; ok || s.stopped {
		return
	}
	s.waiters[id] = w
	s.armLocked(id, w, max(after, 0))
}

func (s *logSink) fire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	s.firing.Add(1)
	return true
}

func (s *logSink) stop() {
	s.mu.Lock()
	s.stopped = true
	for _, w := range s.waiters {
		w.timer.Stop()
	}
	s.mu.Unlock()
	s.firing.Wait()
}

func (s *logSink) validate(requestID, optionID string) error {
	s.mu.Lock()
	w, ok := s.waiters[requestID]
	s.mu.Unlock()
	if !ok {
		return api.Errorf(api.ErrUnknownRequest, "permission request %q is unknown or already resolved", requestID)
	}
	if !slices.Contains(w.options, optionID) {
		return api.Errorf(api.ErrInvalidArgument, "option %q was not offered by permission request %q", optionID, requestID)
	}
	return nil
}

func (s *logSink) take(requestID string) (*permissionWaiter, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.waiters[requestID]
	if !ok {
		return nil, false
	}
	delete(s.waiters, requestID)
	w.timer.Stop()
	return w, true
}

func (s *logSink) pending() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.waiters))
	for id := range s.waiters {
		out = append(out, id)
	}
	return out
}

func (s *logSink) supersedeAll(ctx context.Context) {
	for _, id := range s.pending() {
		w, ok := s.take(id)
		if !ok {
			continue
		}
		s.svc.emit(ctx, s.sessionID, &pb.Event{TurnId: w.turnID, Payload: &pb.Event_PermissionResolved{PermissionResolved: &pb.PermissionResolved{
			RequestId: id, Resolution: &pb.PermissionResolved_Unanswered{Unanswered: api.ResolutionSuperseded},
		}}})
	}
}

func unanswered(why api.Resolution) *pb.PermissionResolved {
	return &pb.PermissionResolved{Resolution: &pb.PermissionResolved_Unanswered{Unanswered: why}}
}
