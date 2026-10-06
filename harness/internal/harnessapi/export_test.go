package harnessapi

import (
	"context"

	"github.com/pomerium/agentops/harness/api"
)

func (s *Service) SuspendForTest(ctx context.Context, sessionID string) {
	s.stopSession(ctx, sessionID, stopSpec{suspend: api.ReasonIdle, end: api.EndIdle})
}
