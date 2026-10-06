package harnessapi

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/pomerium/agentops/harness/api"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenttemplate"
)

func (s *Service) authorize(ctx context.Context, clientID string) (*v1alpha1.ClientBinding, error) {
	binding, err := s.templates.ClientBinding(ctx, clientID)
	switch {
	case errors.Is(err, agenttemplate.ErrNoBinding):
		return nil, api.Errorf(api.ErrForbidden,
			"no ClientBinding registers client %q", clientID)
	case err != nil:
		return nil, api.Errorf(api.ErrUnavailable, "read client binding: %v", err)
	}
	return binding, nil
}

type spend struct {
	newSession bool
}

func (s *Service) checkQuotas(ctx context.Context, binding *v1alpha1.ClientBinding, clientID string, sp spend) error {
	if binding.Spec.Quotas == nil {
		return nil
	}
	q := binding.Spec.Quotas

	if q.CreateRatePerMinute > 0 && !s.rates.allow(clientID, q.CreateRatePerMinute) {
		return api.Errorf(api.ErrQuotaExceeded,
			"client %q may start %d sessions per minute", clientID, q.CreateRatePerMinute)
	}
	if (q.MaxLiveSessions <= 0 || !sp.newSession) && q.MaxPendingApprovals <= 0 {
		return nil
	}
	live, err := s.store.ListSessionsByClient(ctx, clientID, true, time.Time{})
	if err != nil {
		return api.Errorf(api.ErrUnavailable, "count live sessions: %v", err)
	}
	if sp.newSession && q.MaxLiveSessions > 0 && int32(len(live)) >= q.MaxLiveSessions {
		return api.Errorf(api.ErrQuotaExceeded,
			"client %q already holds %d live sessions; its cap is %d", clientID, len(live), q.MaxLiveSessions)
	}
	if q.MaxPendingApprovals > 0 {
		var pending int32
		for _, sess := range live {
			if sess.Status == api.StateAwaitingApproval {
				pending++
			}
		}
		if pending >= q.MaxPendingApprovals {
			return api.Errorf(api.ErrQuotaExceeded,
				"client %q already has %d approvals outstanding; its cap is %d",
				clientID, pending, q.MaxPendingApprovals)
		}
	}
	return nil
}

type rateLimiters struct {
	mu sync.Mutex
	by map[string]*clientRate
}

type clientRate struct {
	perMinute int32
	limiter   *rate.Limiter
}

func newRateLimiters() *rateLimiters { return &rateLimiters{by: map[string]*clientRate{}} }

func (r *rateLimiters) allow(clientID string, perMinute int32) bool {
	r.mu.Lock()
	cr, ok := r.by[clientID]
	if !ok || cr.perMinute != perMinute {
		cr = &clientRate{
			perMinute: perMinute,
			limiter:   rate.NewLimiter(rate.Limit(float64(perMinute)/60.0), int(perMinute)),
		}
		r.by[clientID] = cr
	}
	limiter := cr.limiter
	r.mu.Unlock()
	return limiter.Allow()
}

type resolvedRequests struct {
	ttlMemo[time.Time]
}

const resolvedTTL = 10 * time.Minute

func newResolvedRequests() *resolvedRequests {
	return &resolvedRequests{newTTLMemo(resolvedTTL, func(at time.Time) time.Time { return at })}
}

func resolvedKey(sessionID, requestID string) string { return sessionID + "/" + requestID }

func (r *resolvedRequests) seen(sessionID, requestID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune(r.now())
	_, ok := r.entries[resolvedKey(sessionID, requestID)]
	return ok
}

func (r *resolvedRequests) record(sessionID, requestID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[resolvedKey(sessionID, requestID)] = r.now()
}
