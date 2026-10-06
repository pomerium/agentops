package harnessapi

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
)

type stopSpec struct {
	suspend api.Reason

	end    api.EndReason
	detail string

	ifIdleFor time.Duration
}

func (s *Service) stopSession(ctx context.Context, sessionID string, spec stopSpec) bool {
	return s.stopOwned(ctx, sessionID, nil, spec)
}

func (s *Service) stopOwned(ctx context.Context, sessionID string, want *owner, spec stopSpec) bool {
	ctx = context.WithoutCancel(ctx)
	b, held := s.detach(sessionID, want, spec)
	if held == nil {
		return true
	}
	defer s.settle(ctx, sessionID, held)
	if b != nil {
		return s.stopLive(ctx, b, spec)
	}
	return s.stopDetached(ctx, sessionID, spec)
}

func (s *Service) stopLive(ctx context.Context, b *binding, spec stopSpec) bool {
	ctx = context.WithoutCancel(ctx)
	sessionID := b.sessionID
	_ = b.session.Close()
	<-b.ready

	b.sink.supersedeAll()
	b.drain()

	suspended := false
	if spec.suspend != pb.Reason_REASON_UNSPECIFIED {
		if err := s.launcher.Suspend(ctx, b.claimName); err != nil {
			s.log.WarnContext(ctx, "sandbox suspend failed; tearing the workspace down instead",
				"session", sessionID, "claim", b.claimName, "err", err)
		} else {
			suspended = true
		}
	}
	if !suspended {
		if err := s.launcher.Teardown(ctx, b.claimName); err != nil {
			s.log.ErrorContext(ctx, "sandbox teardown failed; the session stays live so a stop can retry",
				"session", sessionID, "claim", b.claimName, "err", err)
			return false
		}
	}

	if suspended {
		now := time.Now()
		if !s.write(ctx, sessionID, func(ctx context.Context) error {
			return s.store.UpdateSessionSuspended(ctx, sessionID, api.StateSuspended, now)
		}) {
			return false
		}
		s.emit(ctx, sessionID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{
			Old: api.StateRunning, New: api.StateSuspended, Reason: spec.suspend,
		}}})
		s.emit(ctx, sessionID, &pb.Event{Payload: &pb.Event_Suspended{Suspended: &pb.Suspended{
			Reason: spec.suspend, RetainedFor: durationpb.New(s.cfg.suspendedTTL),
		}}})
		return true
	}
	return s.endSession(ctx, sessionID, api.StateRunning, spec.end, spec.detail)
}

func (s *Service) stopDetached(ctx context.Context, sessionID string, spec stopSpec) bool {
	ctx = context.WithoutCancel(ctx)
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		s.log.WarnContext(ctx, "stop: could not read the session", "session", sessionID, "err", err)
		return false
	}
	if api.Terminal(sess.Status) {
		return true
	}
	if sess.SandboxClaimName != "" {
		if err := s.launcher.Teardown(ctx, sess.SandboxClaimName); err != nil {
			s.log.WarnContext(ctx, "stop: releasing a workspace failed",
				"session", sessionID, "claim", sess.SandboxClaimName, "err", err)
			return false
		}
	}
	return s.endSession(ctx, sessionID, sess.Status, spec.end, spec.detail)
}

func (s *Service) endSession(ctx context.Context, sessionID string, from api.SessionState, reason api.EndReason, detail string) bool {
	if reason == pb.EndReason_END_REASON_UNSPECIFIED {
		reason = api.EndEnded
	}
	to := api.StateEnded
	if reason == api.EndInterrupted {
		to = api.StateInterrupted
	}
	if !s.setState(ctx, sessionID, from, to, pb.Reason_REASON_UNSPECIFIED) {
		s.log.ErrorContext(ctx, "the session's end was not recorded; it stays live until an end is retried",
			"session", sessionID, "reason", reason)
		return false
	}
	s.emit(ctx, sessionID, &pb.Event{Payload: &pb.Event_SessionEnded{SessionEnded: &pb.SessionEnded{Reason: reason, Detail: detail}}})
	return true
}

func (s *Service) superviseLaunch(ctx context.Context, sessionID string, o *owner) func(string) {
	ctx = context.WithoutCancel(ctx)
	return func(cause string) {
		s.log.WarnContext(ctx, "session closed by supervision", "session", sessionID, "cause", cause)
		s.stopOwned(ctx, sessionID, o, stopSpec{end: supervisionReason(cause), detail: cause})
	}
}

func supervisionReason(cause string) api.EndReason {
	if cause == "agent_exited" {
		return api.EndAgentExit
	}
	return api.EndTunnelLost
}

func (s *Service) ReconcileOnStartup(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	sessions, err := s.store.ListActiveSessions(ctx)
	if err != nil {
		s.log.ErrorContext(ctx, "startup reconcile: list active sessions failed", "err", err)
		close(done)
		return done
	}
	go func() {
		defer close(done)
		s.reconcile(ctx, sessions)
	}()
	return done
}

func (s *Service) reconcile(ctx context.Context, sessions []sessionstore.Session) {
	ctx = context.WithoutCancel(ctx)
	interrupted := 0
	for _, sess := range sessions {
		if sess.Status == api.StateSuspended {
			continue
		}
		o := &owner{}
		if !s.claim(sess.ID, o) {
			continue
		}
		if s.interrupt(ctx, sess) {
			interrupted++
		}
		s.settle(ctx, sess.ID, o)
	}
	s.log.InfoContext(ctx, "startup reconcile complete",
		"active_sessions", len(sessions), "interrupted", interrupted)
}

func (s *Service) interrupt(ctx context.Context, sess sessionstore.Session) bool {
	if sess.SandboxClaimName != "" {
		if err := s.launcher.Teardown(ctx, sess.SandboxClaimName); err != nil {
			s.log.ErrorContext(ctx, "startup reconcile: teardown failed; the session stays live so a stop can retry",
				"session", sess.ID, "claim", sess.SandboxClaimName, "err", err)
			return false
		}
	}
	return s.endSession(ctx, sess.ID, sess.Status, api.EndInterrupted,
		"the harness restarted while this session was live")
}

func (s *Service) SweepExpired(ctx context.Context) {
	s.sweepIdle(ctx)

	sessions, err := s.store.ListActiveSessions(ctx)
	if err != nil {
		s.log.WarnContext(ctx, "sweep: list active sessions failed", "err", err)
		return
	}
	s.sweepSuspended(ctx, sessions)
	s.sweepAbsolute(ctx, sessions)
}

func (s *Service) sweepIdle(ctx context.Context) {
	if s.cfg.sessionIdleTTL <= 0 {
		return
	}
	now := time.Now()
	warnAfter := s.cfg.sessionIdleTTL - s.cfg.idleWarnLead

	s.mu.Lock()
	var expired, warn []*binding
	for _, o := range s.owners {
		b := o.live
		if b == nil {
			continue
		}
		switch idle := b.idleFor(now); {
		case idle >= s.cfg.sessionIdleTTL:
			expired = append(expired, b)
		case s.cfg.idleWarnLead > 0 && warnAfter > 0 && idle >= warnAfter &&
			b.idleWarned.CompareAndSwap(false, true):
			warn = append(warn, b)
		}
	}
	s.mu.Unlock()

	for _, b := range warn {
		s.log.InfoContext(ctx, "session is going quiet; warning before the idle suspend",
			"session", b.sessionID, "idle_ttl", s.cfg.sessionIdleTTL.String())
		s.emit(ctx, b.sessionID, &pb.Event{Payload: &pb.Event_IdleWarning{IdleWarning: &pb.IdleWarning{
			Lead: durationpb.New(s.cfg.idleWarnLead),
		}}})
	}
	for _, b := range expired {
		s.log.InfoContext(ctx, "suspending an idle session",
			"session", b.sessionID, "idle", b.idleFor(now).String(),
			"idle_ttl", s.cfg.sessionIdleTTL.String())

		s.stopOwned(ctx, b.sessionID, b.owner, stopSpec{
			suspend: api.ReasonIdle, end: api.EndIdle, ifIdleFor: s.cfg.sessionIdleTTL,
		})
	}
}

func (s *Service) sweepSuspended(ctx context.Context, sessions []sessionstore.Session) {
	if s.cfg.suspendedTTL <= 0 {
		return
	}
	for _, sess := range sessions {
		if sess.Status == api.StateSuspended && s.suspendedFor(sess) >= s.cfg.suspendedTTL {
			s.releaseSuspended(ctx, sess.ID)
		}
	}
}

func (s *Service) suspendedFor(sess sessionstore.Session) time.Duration {
	since := sess.SuspendedAt
	if since.IsZero() {
		since = sess.UpdatedAt
	}
	return time.Since(since)
}

func (s *Service) releaseSuspended(ctx context.Context, sessionID string) {
	ctx = context.WithoutCancel(ctx)
	o := &owner{}
	if !s.claim(sessionID, o) {
		return
	}
	defer s.settle(ctx, sessionID, o)
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		s.log.WarnContext(ctx, "sweep: could not read a suspended session", "session", sessionID, "err", err)
		return
	}
	held := s.suspendedFor(sess)
	if sess.Status != api.StateSuspended || held < s.cfg.suspendedTTL {
		return
	}
	s.log.InfoContext(ctx, "releasing a suspended session's workspace",
		"session", sess.ID, "claim", sess.SandboxClaimName,
		"suspended_for", held.Round(time.Second).String(),
		"suspended_ttl", s.cfg.suspendedTTL.String())
	if sess.SandboxClaimName != "" {
		if err := s.launcher.Teardown(ctx, sess.SandboxClaimName); err != nil {
			s.log.WarnContext(ctx, "sweep: releasing a suspended workspace failed",
				"session", sess.ID, "claim", sess.SandboxClaimName, "err", err)
			return
		}
	}

	s.emit(ctx, sess.ID, &pb.Event{Payload: &pb.Event_Released{Released: &pb.Released{
		RetainedFor: durationpb.New(s.cfg.suspendedTTL),
	}}})
	s.endSession(ctx, sess.ID, api.StateSuspended, api.EndExpired,
		"the workspace retention window lapsed")
}

func (s *Service) sweepAbsolute(ctx context.Context, sessions []sessionstore.Session) {
	if s.cfg.sessionTTL <= 0 {
		return
	}
	for _, sess := range sessions {
		if sess.Status == api.StateSuspended {
			continue
		}
		if time.Since(sess.CreatedAt) < s.cfg.sessionTTL {
			continue
		}
		s.log.InfoContext(ctx, "ending a session past its absolute lifetime",
			"session", sess.ID, "session_ttl", s.cfg.sessionTTL.String())
		s.stopSession(ctx, sess.ID, stopSpec{end: api.EndExpired, detail: "the session reached its maximum lifetime"})
	}
}

func (s *Service) Shutdown() {
	s.mu.Lock()
	var live []*binding
	for id, o := range s.owners {
		if o.live != nil && o.live.close(0) {
			live = append(live, o.live)
			delete(s.owners, id)
		}
	}
	s.mu.Unlock()
	for _, b := range live {
		_ = b.session.Close()
	}
}
