package harnessapi

import (
	"context"
	"time"

	"github.com/pomerium/agentops/harness/internal/sandbox"
)

type orchestratorLauncher struct {
	o *sandbox.Orchestrator
}

func NewOrchestratorLauncher(o *sandbox.Orchestrator) Launcher {
	return orchestratorLauncher{o: o}
}

func (l orchestratorLauncher) Prepare(ctx context.Context, spec sandbox.LaunchSpec) (*sandbox.Prepared, error) {
	return l.o.Prepare(ctx, spec)
}

func (l orchestratorLauncher) Expect(runID string, prepared *sandbox.Prepared, opts ...sandbox.SupervisionOption) (*sandbox.Attachment, error) {
	return l.o.Expect(runID, prepared, opts...)
}

func (l orchestratorLauncher) Activate(ctx context.Context, prepared *sandbox.Prepared, att *sandbox.Attachment) (LiveSession, error) {
	s, err := l.o.Activate(ctx, prepared, att)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (l orchestratorLauncher) Adopt(ctx context.Context, spec sandbox.AdoptSpec, opts ...sandbox.SupervisionOption) (LiveSession, error) {
	s, err := l.o.Adopt(ctx, spec, opts...)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (l orchestratorLauncher) Teardown(ctx context.Context, claimName string) error {
	return l.o.Teardown(ctx, claimName)
}

func (l orchestratorLauncher) Suspend(ctx context.Context, claimName string) error {
	return l.o.Suspend(ctx, claimName)
}

func (l orchestratorLauncher) Revive(ctx context.Context, claimName string, spec sandbox.LaunchSpec) (*sandbox.Prepared, error) {
	return l.o.Revive(ctx, claimName, spec)
}

func (l orchestratorLauncher) ExtendLease(ctx context.Context, claimName string) (time.Time, error) {
	return l.o.ExtendLease(ctx, claimName)
}

func (l orchestratorLauncher) LeaseLength() time.Duration { return l.o.Lease() }
