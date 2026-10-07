package harnessapi

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

type launchOpts struct {
	revive bool

	approvalPrompt string

	agentPrompt string

	turnID string
}

func (s *Service) launch(ctx context.Context, o *owner, sessionID string, opts launchOpts) {
	ctx = telemetry.With(ctx, "session_id", sessionID, "reviving", opts.revive)
	ctx, op := s.tel.Start(ctx, "launch")
	defer op.Complete()

	cancel := o.cancel
	defer cancel()
	registered := false
	defer func() {
		if !registered {
			s.settle(ctx, sessionID, o)
		}
	}()
	outcome := o.outcome

	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		s.log.ErrorContext(ctx, "launch: could not read the session", "err", err)
		if opts.turnID != "" {
			s.emit(ctx, sessionID, &pb.Event{TurnId: opts.turnID, Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{
				Reason: "the session could not be read to continue it",
			}}})
		}
		return
	}
	if s.stopped(o) {
		s.failLaunch(ctx, sess, opts, o, "", api.EndLaunchFailed, "")
		return
	}
	if opts.agentPrompt != "" && opts.turnID == "" {
		turnID, err := s.nextTurnID(ctx, sess.ID)
		if err != nil {
			s.log.ErrorContext(ctx, "launch: could not allocate the opening turn", "err", err)
			s.failLaunch(ctx, sess, opts, o, "", api.EndLaunchFailed, "the opening turn could not be allocated")
			return
		}
		opts.turnID = turnID
	}
	from := sess.Status
	tmpl, promptAppendix, err := storedTemplate(sess)
	if err != nil {
		s.log.ErrorContext(ctx, "launch: no usable template snapshot", "err", err)
		s.endSession(ctx, sess.ID, from, api.EndLaunchFailed, "the session's agent template snapshot could not be read")
		return
	}
	if !s.setState(ctx, sess.ID, from, api.StateLaunching, launchReason(opts.revive)) {
		s.failLaunch(ctx, sess, opts, o, "", api.EndLaunchFailed, unrecorded)
		return
	}

	spec := sandbox.LaunchSpec{
		SessionID:    sess.ID,
		Template:     tmpl,
		SystemPrompt: composeSystemPrompt(tmpl.Spec.SystemPrompt, promptAppendix),
		Endpoints:    runIdentityEndpoints(tmpl),
	}
	var prepared *sandbox.Prepared
	if opts.revive {
		spec.ResumeACPSessionID = sess.ACPSessionID
		prepared, err = s.launcher.Revive(ctx, sess.SandboxClaimName, spec)
	} else {
		prepared, err = s.launcher.Prepare(ctx, spec)
	}
	if err != nil {
		s.log.ErrorContext(ctx, "could not get a workspace ready", "err", err)
		s.failLaunch(ctx, sess, opts, o, "", api.EndPrepareFailed, "the workspace could not be prepared")
		return
	}

	if !s.write(ctx, sess.ID, func(ctx context.Context) error {
		return s.store.UpdateSessionSandbox(ctx, sess.ID, prepared.ClaimName, prepared.SandboxName, api.StateLaunching)
	}) {
		s.failLaunch(ctx, sess, opts, o, prepared.ClaimName, api.EndLaunchFailed, unrecorded)
		return
	}

	res, err := s.runs.CreateRun(ctx, agenticrun.CreateRunRequest{
		Prompt:          opts.approvalPrompt,
		MCPServers:      mcpServerURLs(tmpl),
		TTL:             s.cfg.runTTL,
		Executor:        prepared.Executor.Seal(),
		ExpectedSubject: expectedSubject(sess, opts.revive),

		Labels: map[string]string{"template": tmpl.Name},
	})
	if err != nil {
		s.log.ErrorContext(ctx, "create run failed", "err", err)
		s.failLaunch(ctx, sess, opts, o, prepared.ClaimName, api.EndRunCreateFailed, "a run could not be created for this session")
		return
	}
	if !s.write(ctx, sess.ID, func(ctx context.Context) error {
		return s.store.UpdateSessionRun(ctx, sess.ID, res.RunID, res.ApprovalURL, res.ExpiresAt, api.StateAwaitingApproval)
	}) {
		s.failLaunch(ctx, sess, opts, o, prepared.ClaimName, api.EndLaunchFailed, unrecorded)
		return
	}

	att, err := s.launcher.Expect(res.RunID, prepared,
		sandbox.WithOnDown(s.superviseLaunch(ctx, sess.ID, o)),
		sandbox.WithOnDelayed(func(waited time.Duration) {
			s.reportAttachDelayed(context.WithoutCancel(ctx), sess.ID, res.RunID, outcome, waited)
		}),
	)
	if err != nil {
		s.log.ErrorContext(ctx, "register harness expectation failed", "run_id", res.RunID, "err", err)
		s.failLaunch(ctx, sess, opts, o, prepared.ClaimName, api.EndLaunchFailed,
			"this session could not be registered with the agent link")
		return
	}

	defer func() {
		if !registered {
			att.Forget()
		}
	}()

	s.emit(ctx, sess.ID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{
		Old: api.StateLaunching, New: api.StateAwaitingApproval, Reason: launchReason(opts.revive),
	}}})
	s.emit(ctx, sess.ID, &pb.Event{Payload: &pb.Event_ApprovalRequired{ApprovalRequired: &pb.ApprovalRequired{
		ApprovalUrl: res.ApprovalURL, ExpiresAt: api.Timestamp(res.ExpiresAt),
	}}})

	go s.pollRun(ctx, cancel, sess.ID, res.RunID, outcome)

	b := s.activateAndRun(ctx, sess, opts, prepared, att, res.RunID, o)
	registered = b != nil
	if registered {
		go s.watchRun(context.WithoutCancel(ctx), b, res.RunID)
	}
}

const unrecorded = "the session's state could not be recorded"

func launchReason(revive bool) api.Reason {
	if revive {
		return api.ReasonRevive
	}
	return api.ReasonLaunch
}

func expectedSubject(sess sessionstore.Session, revive bool) string {
	if revive {
		return sess.ApproverSubject
	}
	return ""
}

func (s *Service) activateAndRun(
	ctx context.Context,
	sess sessionstore.Session,
	opts launchOpts,
	prepared *sandbox.Prepared,
	att *sandbox.Attachment,
	runID string,
	o *owner,
) *binding {
	outcome := o.outcome
	sink := newLogSink(s, sess.ID, s.cfg.permissionTimeout)

	liveSess, err := s.launcher.Activate(ctx, sink, prepared, att)
	if err != nil {
		if errors.Is(err, sandbox.ErrResumeUnavailable) && opts.revive && !s.stopped(o) {
			s.log.InfoContext(ctx, "this conversation could not be continued", "err", err)
			s.failRevive(ctx, sess, opts, api.ReasonResumeUnavailable, "the conversation could not be continued")
			return nil
		}
		reason, detail := api.EndLaunchFailed, "the workspace could not be activated"

		if errors.Is(err, sandbox.ErrAttachTimeout) {
			reason, detail = api.EndAttachTimeout, "the workspace started but never connected back"
		}

		switch outcome.get() {
		case api.EndNeverApproved:
			reason, detail = api.EndNeverApproved, "nobody approved this session before its run lapsed"
		case api.EndRevoked:
			reason, detail = api.EndRevoked, "this session's run was revoked"
		}
		s.log.ErrorContext(ctx, "activate failed", "err", err)
		s.failLaunch(ctx, sess, opts, o, prepared.ClaimName, reason, detail)
		return nil
	}

	b := newBinding(sess.ID, prepared.ClaimName, liveSess, sink)
	defer close(b.ready)
	if !prepared.LeaseUntil.IsZero() {
		b.leaseUntil.Store(prepared.LeaseUntil.UnixNano())
	}
	var ticket uint64
	opening := false
	if opts.agentPrompt != "" {
		ticket, opening = b.enter()
	}

	if !s.register(o, b) {
		if opening {
			b.forfeit(ticket)
		}
		_ = liveSess.Close()
		s.failLaunch(ctx, sess, opts, o, prepared.ClaimName, api.EndLaunchFailed, "")
		return nil
	}
	ctx = context.WithoutCancel(ctx)
	if !s.write(ctx, sess.ID, func(ctx context.Context) error {
		if err := s.store.UpdateSessionSandbox(ctx, sess.ID, prepared.ClaimName, prepared.SandboxName, api.StateLaunching); err != nil {
			return err
		}
		return s.store.UpdateSessionACP(ctx, sess.ID, liveSess.ID(), api.StateRunning)
	}) {
		if opening {
			s.emit(ctx, sess.ID, &pb.Event{TurnId: opts.turnID, Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{Reason: unrecorded}}})
			b.forfeit(ticket)
		}
		spec := stopSpec{end: api.EndLaunchFailed, detail: unrecorded}
		if opts.revive {
			spec.suspend = api.ReasonReviveFailed
		}
		go s.stopOwned(ctx, sess.ID, o, spec)
		return b
	}

	s.recordApproverFromRun(ctx, sess.ID, runID)

	s.emit(ctx, sess.ID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{
		Old: api.StateAwaitingApproval, New: api.StateRunning, Reason: launchReason(opts.revive),
	}}})
	if opts.revive {
		s.emit(ctx, sess.ID, &pb.Event{Payload: &pb.Event_Revived{Revived: &pb.Revived{}}})
	}

	if opening {
		go s.runTurn(context.WithoutCancel(ctx), b, ticket, opts.turnID, opts.agentPrompt)
	}
	return b
}

func (s *Service) failLaunch(ctx context.Context, sess sessionstore.Session, opts launchOpts, o *owner, claimName string, reason api.EndReason, detail string) {
	ctx = context.WithoutCancel(ctx)
	if spec, stopped := s.takeStop(o); stopped {
		reason, detail = spec.end, spec.detail
		if claimName == "" {
			claimName = sess.SandboxClaimName
		}
		if opts.turnID != "" {
			s.emit(ctx, sess.ID, &pb.Event{TurnId: opts.turnID, Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{Reason: "the session ended"}}})
		}
	} else if opts.revive {
		s.failRevive(ctx, sess, opts, api.ReasonReviveFailed, detail)
		return
	}
	if claimName != "" {
		if err := s.launcher.Teardown(ctx, claimName); err != nil {
			if s.recordsClaim(ctx, sess.ID, claimName) {
				s.log.ErrorContext(ctx, "failed launch: teardown failed; the session stays live so a stop can retry",
					"session", sess.ID, "claim", claimName, "err", err)
				return
			}
			s.log.ErrorContext(ctx, "failed launch: teardown failed and the session records no workspace to retry; its lease bounds the sandbox",
				"session", sess.ID, "claim", claimName, "err", err)
		}
	}
	s.endSession(ctx, sess.ID, api.StateLaunching, reason, detail)
}

func (s *Service) recordsClaim(ctx context.Context, sessionID, claimName string) bool {
	current, err := s.store.GetSession(ctx, sessionID)
	return err == nil && current.SandboxClaimName == claimName
}

func (s *Service) failRevive(ctx context.Context, sess sessionstore.Session, opts launchOpts, reason api.Reason, detail string) {
	ctx = context.WithoutCancel(ctx)
	if sess.SandboxClaimName != "" {
		if err := s.launcher.Suspend(ctx, sess.SandboxClaimName); err != nil {
			s.log.WarnContext(ctx, "could not put a revived sandbox back down; it may run until its lease lapses",
				"session", sess.ID, "claim", sess.SandboxClaimName, "err", err)
		}
	}
	if opts.turnID != "" {
		s.emit(ctx, sess.ID, &pb.Event{TurnId: opts.turnID, Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{Reason: detail}}})
	}
	if !s.write(ctx, sess.ID, func(ctx context.Context) error {
		return s.store.UpdateSessionSuspended(ctx, sess.ID, api.StateSuspended, suspendedAtOf(sess))
	}) {
		return
	}
	s.emit(ctx, sess.ID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{
		Old: api.StateLaunching, New: api.StateSuspended, Reason: reason,
	}}})
	s.log.InfoContext(ctx, "continuation failed; the workspace is still held", "session", sess.ID, "why", detail)
}

func suspendedAtOf(sess sessionstore.Session) time.Time {
	if sess.SuspendedAt.IsZero() {
		return time.Now()
	}
	return sess.SuspendedAt
}

type runOutcome struct {
	mu     sync.Mutex
	reason api.EndReason

	approved bool
}

func (o *runOutcome) set(r api.EndReason) { o.mu.Lock(); o.reason = r; o.mu.Unlock() }
func (o *runOutcome) get() api.EndReason  { o.mu.Lock(); defer o.mu.Unlock(); return o.reason }

func (o *runOutcome) markApproved() { o.mu.Lock(); o.approved = true; o.mu.Unlock() }
func (o *runOutcome) isApproved() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.approved
}

func (s *Service) pollRun(ctx context.Context, cancel context.CancelFunc, sessionID, runID string, outcome *runOutcome) {
	ticker := time.NewTicker(runPollInterval)
	defer ticker.Stop()
	var storedExpiry time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		st, err := s.runs.GetRun(ctx, runID)
		if err != nil {
			if agenticrun.IsRetryable(err) || ctx.Err() != nil {
				continue
			}
			s.log.ErrorContext(ctx, "run poll terminal error", "session", sessionID, "err", err)
			outcome.set(api.EndRevoked)
			cancel()
			return
		}
		if !st.ExpiresAt.IsZero() && !st.ExpiresAt.Equal(storedExpiry) {
			if s.store.UpdateSessionRunExpiry(context.WithoutCancel(ctx), sessionID, st.ExpiresAt) == nil {
				storedExpiry = st.ExpiresAt
			}
		}
		switch {
		case st.Revoked:
			outcome.set(api.EndRevoked)
			cancel()
			return
		case !st.ExpiresAt.IsZero() && time.Now().After(st.ExpiresAt):

			outcome.set(api.EndNeverApproved)
			cancel()
			return
		case st.State == "approved" && !outcome.isApproved():
			outcome.markApproved()
		}
	}
}

func (s *Service) watchRun(ctx context.Context, b *binding, runID string) {
	sessionID := b.sessionID
	ticker := time.NewTicker(s.cfg.runWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.done:
			return
		case <-ticker.C:
		}
		st, err := s.runs.GetRun(ctx, runID)
		if err != nil {
			if agenticrun.IsRetryable(err) || ctx.Err() != nil {
				continue
			}
			s.log.WarnContext(ctx, "run watch terminal error; ending the session",
				"session", sessionID, "run_id", runID, "err", err)
			s.stopOwned(ctx, sessionID, b.owner, stopSpec{end: api.EndRevoked, detail: "this session's run could not be read"})
			return
		}
		switch {
		case st.Revoked:
			s.stopOwned(ctx, sessionID, b.owner, stopSpec{end: api.EndRevoked})
			return
		case !st.ExpiresAt.IsZero() && time.Now().After(st.ExpiresAt):
			s.stopOwned(ctx, sessionID, b.owner, stopSpec{end: api.EndExpired})
			return
		}
	}
}

func (s *Service) recordApproverFromRun(ctx context.Context, sessionID, runID string) {
	st, err := s.runs.GetRun(ctx, runID)
	if err != nil {
		s.log.WarnContext(ctx, "could not read the run to learn who approved it; this session will not be continuable",
			"session", sessionID, "run_id", runID, "err", err)
		return
	}
	if st.ApproverSubject == "" {
		s.tel.Debug(ctx, "the authorization server reported no approver subject; this session will not be continuable",
			"session", sessionID)
		return
	}
	if err := s.store.UpdateSessionApprover(context.WithoutCancel(ctx), sessionID, st.ApproverSubject); err != nil {
		s.log.WarnContext(ctx, "could not record the approver; this session will not be continuable",
			"session", sessionID, "err", err)
	}
	s.emit(ctx, sessionID, &pb.Event{Payload: &pb.Event_Approved{Approved: &pb.Approved{ApproverSubject: st.ApproverSubject}}})
}

func (s *Service) reportAttachDelayed(ctx context.Context, sessionID, runID string, outcome *runOutcome, waited time.Duration) {
	if !outcome.isApproved() {
		s.tel.Debug(ctx, "sandbox has not attached yet, but the run is not approved either",
			"session", sessionID, "run_id", runID, "waited", waited.String())
		return
	}
	s.log.WarnContext(ctx, "run is approved but the sandbox has not connected to the agent link",
		"session", sessionID, "run_id", runID, "waited", waited.String(),
		"hint", "check the sandbox sidecar container logs and that the harness route reaches this process")

	s.emit(ctx, sessionID, &pb.Event{Payload: &pb.Event_LaunchStalled{LaunchStalled: &pb.LaunchStalled{Waited: durationpb.New(waited)}}})
}

func (s *Service) runTurn(ctx context.Context, b *binding, ticket uint64, turnID, text string) {
	defer b.leave()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func(finished <-chan struct{}) {
		select {
		case <-b.done:
			cancel()
		case <-finished:
		}
	}(ctx.Done())

	s.extendLease(ctx, b)

	ctx = telemetry.With(ctx, "session_id", b.sessionID, "turn_id", turnID)
	ctx, op := s.tel.Start(ctx, "runTurn", "chars", len(text))
	defer op.Complete()

	b.awaitTurn(ticket)
	if ctx.Err() != nil {
		s.emit(ctx, b.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{Reason: "the session stopped before this turn ran"}}})
		return
	}
	b.sink.beginTurn(turnID)
	defer b.sink.endTurn(ctx)

	stop, err := b.session.Prompt(ctx, text)
	if err != nil {
		s.log.ErrorContext(ctx, "acp prompt failed", "session", b.sessionID, "err", err)

		b.sink.endTurn(ctx)
		s.emit(ctx, b.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_TurnFailed{TurnFailed: &pb.TurnFailed{Reason: agentErrorReason(err)}}})
		return
	}
	b.sink.endTurn(ctx)
	s.emit(ctx, b.sessionID, &pb.Event{TurnId: turnID, Payload: &pb.Event_TurnCompleted{TurnCompleted: &pb.TurnCompleted{StopReason: string(stop)}}})
}

func agentErrorReason(err error) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return msg
}

func (s *Service) extendLease(ctx context.Context, b *binding) {
	lease := s.launcher.LeaseLength()
	if lease <= 0 {
		return
	}
	if remaining := time.Until(time.Unix(0, b.leaseUntil.Load())); remaining > lease/2 {
		return
	}
	until, err := s.launcher.ExtendLease(ctx, b.claimName)
	if err != nil {
		s.log.WarnContext(ctx, "could not extend the sandbox lease; the session continues but its deadline is not moving",
			"session", b.sessionID, "claim", b.claimName, "err", err)
		return
	}
	b.leaseUntil.Store(until.UnixNano())
	s.tel.Debug(ctx, "sandbox lease extended", "session", b.sessionID, "until", until)
}
