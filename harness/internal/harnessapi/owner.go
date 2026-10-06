package harnessapi

import (
	"context"

	"github.com/pomerium/agentops/harness/api"
)

type owner struct {
	clientID string
	cancel   context.CancelFunc
	outcome  *runOutcome
	live     *binding
	stop     *stopSpec
}

func (s *Service) newLaunch(ctx context.Context, clientID string) (context.Context, *owner) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return ctx, &owner{clientID: clientID, cancel: cancel, outcome: &runOutcome{}}
}

func (s *Service) claim(sessionID string, o *owner) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, held := s.owners[sessionID]; held {
		return false
	}
	s.owners[sessionID] = o
	return true
}

func (s *Service) release(sessionID string, o *owner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners[sessionID] == o {
		delete(s.owners, sessionID)
	}
}

func (s *Service) lookup(sessionID string) *binding {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.owners[sessionID]; o != nil {
		return o.live
	}
	return nil
}

func (s *Service) counts() (live, launching int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.owners {
		if o.live != nil {
			live++
		} else {
			launching++
		}
	}
	return live, launching
}

func (s *Service) launchesOf(clientID string) map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]struct{}{}
	for id, o := range s.owners {
		if o.cancel != nil && o.live == nil && o.clientID == clientID {
			out[id] = struct{}{}
		}
	}
	return out
}

func (s *Service) register(o *owner, b *binding) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners[b.sessionID] != o || o.stop != nil {
		return false
	}
	b.owner = o
	o.live = b
	return true
}

func (s *Service) stopped(o *owner) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return o.stop != nil
}

func (s *Service) takeStop(o *owner) (stopSpec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o.stop == nil {
		return stopSpec{}, false
	}
	spec := *o.stop
	o.stop = nil
	return spec, true
}

func (s *Service) detach(sessionID string, want *owner, spec stopSpec) (*binding, *owner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.owners[sessionID]
	if want != nil && o != want {
		return nil, nil
	}
	if o != nil && o.live == nil {
		if spec.suspend == api.Reason(0) && o.stop == nil {
			o.stop = &spec
			if o.cancel != nil {
				o.cancel()
			}
		}
		return nil, nil
	}
	if o != nil && !o.live.close(spec.ifIdleFor) {
		return nil, nil
	}
	held := &owner{}
	s.owners[sessionID] = held
	if o != nil {
		return o.live, held
	}
	return nil, held
}

func (s *Service) finish(sessionID string, o *owner) (stopSpec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o.stop != nil {
		spec := *o.stop
		o.stop = nil
		return spec, true
	}
	if s.owners[sessionID] == o {
		delete(s.owners, sessionID)
	}
	return stopSpec{}, false
}

func (s *Service) settle(ctx context.Context, sessionID string, o *owner) {
	for {
		spec, stopped := s.finish(sessionID, o)
		if !stopped {
			return
		}
		s.stopDetached(ctx, sessionID, spec)
	}
}
