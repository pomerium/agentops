package session

import (
	"context"

	"github.com/pomerium/agentops/internal/sandbox"
)

// orchestratorLauncher adapts a *sandbox.Orchestrator to the Launcher interface,
// returning the concrete *sandbox.Session as a LiveSession.
type orchestratorLauncher struct {
	o *sandbox.Orchestrator
}

// NewOrchestratorLauncher wraps an Orchestrator as a Launcher.
func NewOrchestratorLauncher(o *sandbox.Orchestrator) Launcher {
	return orchestratorLauncher{o: o}
}

func (l orchestratorLauncher) Launch(ctx context.Context, sink sandbox.EventSink, spec sandbox.LaunchSpec, endpoints []sandbox.ProxiedEndpoint) (LiveSession, sandbox.LaunchResult, error) {
	s, result, err := l.o.Launch(ctx, sink, spec, endpoints)
	if err != nil {
		return nil, result, err
	}
	return s, result, nil
}

func (l orchestratorLauncher) Teardown(ctx context.Context, claimName string) error {
	return l.o.Teardown(ctx, claimName)
}
