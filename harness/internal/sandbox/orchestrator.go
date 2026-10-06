package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	agentsv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

const readyConditionType = "Ready"

const (
	sandboxCwd        = "/workspace"
	readyTimeout      = 3 * time.Minute
	readyPollInterval = 2 * time.Second
)

type ClaimClient interface {
	Create(ctx context.Context, claim *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error)
	Get(ctx context.Context, name string) (*sbxv1.SandboxClaim, error)
	Delete(ctx context.Context, name string) error
}

type SandboxClient interface {
	Get(ctx context.Context, name string) (*agentsv1.Sandbox, error)
	Patch(ctx context.Context, name string, data []byte) error
}

type PodGetter interface {
	Get(ctx context.Context, name string) (*corev1.Pod, error)
}

type AgentLink interface {
	Expect(runID string, seal agenticrun.Executor, cfg *agentlinkpb.SandboxConfig, opts ...agentlink.ExpectOption) (AttachHandle, error)
	Forget(runID string)
}

type AttachHandle interface {
	AwaitAttach(ctx context.Context) error
	AwaitReady(ctx context.Context) error
	SpawnAgent(ctx context.Context) error
	AwaitAgentIO(ctx context.Context) (io.Writer, io.Reader, error)
	Shutdown(reason string)
}

type serverAgentLink struct{ srv *agentlink.Server }

func NewAgentLink(srv *agentlink.Server) AgentLink { return serverAgentLink{srv: srv} }

func (g serverAgentLink) Expect(runID string, seal agenticrun.Executor, cfg *agentlinkpb.SandboxConfig, opts ...agentlink.ExpectOption) (AttachHandle, error) {
	return g.srv.Expect(runID, seal, cfg, opts...)
}

func (g serverAgentLink) Forget(runID string) { g.srv.Forget(runID) }

type Prepared struct {
	ClaimName   string
	SandboxName string
	Executor    agenticrun.Executor
	LeaseUntil  time.Time
	spec        LaunchSpec
	Resumed     bool
}

type Option func(*options)

type options struct {
	namespace       string
	harnessRoute    string
	attachGrace     time.Duration
	attachTimeout   time.Duration
	attachWarnAfter time.Duration
	lease           time.Duration
	logger          *slog.Logger
}

func WithNamespace(ns string) Option { return func(o *options) { o.namespace = ns } }

func WithHarnessRoute(route string) Option { return func(o *options) { o.harnessRoute = route } }

func WithAttachGrace(d time.Duration) Option { return func(o *options) { o.attachGrace = d } }

func WithAttachTimeout(d time.Duration) Option { return func(o *options) { o.attachTimeout = d } }

func WithAttachWarnAfter(d time.Duration) Option { return func(o *options) { o.attachWarnAfter = d } }

func WithLease(d time.Duration) Option { return func(o *options) { o.lease = d } }

func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

type Orchestrator struct {
	claims    ClaimClient
	sandboxes SandboxClient
	pods      PodGetter
	link      AgentLink
	cfg       options
	log       *slog.Logger
	tel       *telemetry.Component

	openSession func(ctx context.Context, tel *telemetry.Component, sink EventSink, agentStdin io.Writer, agentStdout io.Reader, closeFn func() error, params SessionParams) (*Session, error)
}

func New(claims ClaimClient, pods PodGetter, sandboxes SandboxClient, link AgentLink, opts ...Option) *Orchestrator {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.attachGrace == 0 {
		cfg.attachGrace = 2 * time.Minute
	}
	if cfg.attachTimeout == 0 {
		cfg.attachTimeout = 20 * time.Minute
	}
	if cfg.attachWarnAfter == 0 {
		cfg.attachWarnAfter = 90 * time.Second
	}
	if cfg.lease == 0 {
		cfg.lease = time.Hour
	}
	log := cfg.logger
	if log == nil {
		log = slog.Default()
	}
	return &Orchestrator{
		claims: claims, pods: pods, sandboxes: sandboxes, link: link, cfg: cfg, log: log,
		tel:         telemetry.New(log, "sandbox", slog.LevelDebug),
		openSession: OpenSession,
	}
}

type LaunchResult struct {
	ClaimName   string
	SandboxName string
}

func (o *Orchestrator) Prepare(ctx context.Context, spec LaunchSpec) (*Prepared, error) {
	ctx, op := o.tel.Start(ctx, "Prepare", "session_id", spec.SessionID)
	defer op.Complete()

	claim := BuildSandboxClaim(o.cfg.namespace, spec)
	created, err := o.claims.Create(ctx, claim)
	if apierrors.IsAlreadyExists(err) {
		o.tel.Debug(ctx, "sandbox claim already exists; adopting", "claim", claim.Name)
		created, err = o.claims.Get(ctx, claim.Name)
	}
	if err != nil {
		return nil, op.Failure(fmt.Errorf("create sandbox claim: %w", err))
	}
	prepared := &Prepared{ClaimName: created.Name, spec: spec}
	o.tel.Debug(ctx, "sandbox claim created; waiting for ready", "claim", created.Name)

	sandboxName, err := o.waitReady(ctx, created.Name)
	if err != nil {
		o.teardownQuietly(ctx, created.Name)
		return nil, op.Failure(err)
	}
	prepared.SandboxName = sandboxName

	if o.sandboxes != nil && o.cfg.lease > 0 {
		if until, err := o.ExtendLease(ctx, created.Name); err != nil {
			o.log.ErrorContext(ctx, "could not set the sandbox's shutdown lease; this pod has no controller-enforced deadline and will outlive this process if it dies",
				"claim", created.Name, "sandbox", sandboxName, "err", err,
				"hint", "the manager needs patch on sandboxes.agents.x-k8s.io")
		} else {
			prepared.LeaseUntil = until
			o.tel.Debug(ctx, "sandbox lease armed", "claim", created.Name, "until", until)
		}
	}

	exec, err := o.executorFor(ctx, sandboxName)
	if err != nil {
		o.teardownQuietly(ctx, created.Name)
		return nil, op.Failure(err)
	}
	prepared.Executor = exec
	return prepared, nil
}

func (o *Orchestrator) executorFor(ctx context.Context, sandboxName string) (agenticrun.Executor, error) {
	pod, err := o.pods.Get(ctx, sandboxName)
	if err != nil {
		return agenticrun.Executor{}, fmt.Errorf("get pod %q: %w", sandboxName, err)
	}
	exec := agenticrun.Executor{
		Namespace:      pod.Namespace,
		ServiceAccount: pod.Spec.ServiceAccountName,
		PodName:        pod.Name,
		PodUID:         string(pod.UID),
	}
	if err := exec.Validate(); err != nil {
		return agenticrun.Executor{}, fmt.Errorf("resolve executor identity for pod %q: %w", sandboxName, err)
	}
	return exec, nil
}

func (o *Orchestrator) Revive(ctx context.Context, claimName string, spec LaunchSpec) (*Prepared, error) {
	ctx, op := o.tel.Start(ctx, "Revive", "session_id", spec.SessionID, "claim", claimName)
	defer op.Complete()

	fail := func(err error) (*Prepared, error) {
		if serr := o.Suspend(ctx, claimName); serr != nil {
			o.log.WarnContext(ctx, "could not re-suspend a sandbox after a failed revive; it may be left running",
				"claim", claimName, "err", serr)
		}
		return nil, op.Failure(err)
	}

	sandboxName, err := o.Resume(ctx, claimName)
	if err != nil {
		return fail(err)
	}
	exec, err := o.executorFor(ctx, sandboxName)
	if err != nil {
		return fail(err)
	}
	prepared := &Prepared{
		ClaimName:   claimName,
		SandboxName: sandboxName,
		Executor:    exec,
		spec:        spec,
		Resumed:     true,
	}
	if o.cfg.lease > 0 {
		prepared.LeaseUntil = time.Now().Add(o.cfg.lease)
	}
	o.log.InfoContext(ctx, "sandbox revived",
		"claim", claimName, "sandbox", sandboxName, "pod_uid", exec.PodUID)
	return prepared, nil
}

type Attachment struct {
	runID  string
	handle AttachHandle
	link   AgentLink

	forgotten atomic.Bool
	down      func(cause string)
	downOnce  sync.Once
	onDelayed func(waited time.Duration)
	mu        sync.Mutex
	grace     *time.Timer
}

func (a *Attachment) RunID() string { return a.runID }

func (a *Attachment) Forget() {
	if a == nil || a.forgotten.Swap(true) {
		return
	}
	a.cancelGrace()
	if a.link != nil {
		a.link.Forget(a.runID)
	}
}

type Supervision struct {
	OnDown    func(cause string)
	OnDelayed func(waited time.Duration)
}

type SupervisionOption func(*Supervision)

func WithOnDown(f func(cause string)) SupervisionOption { return func(s *Supervision) { s.OnDown = f } }

func WithOnDelayed(f func(waited time.Duration)) SupervisionOption {
	return func(s *Supervision) { s.OnDelayed = f }
}

func (o *Orchestrator) Expect(runID string, prepared *Prepared, opts ...SupervisionOption) (*Attachment, error) {
	if o.link == nil {
		return nil, errors.New("no agent link configured")
	}
	var sup Supervision
	for _, opt := range opts {
		opt(&sup)
	}
	att := &Attachment{runID: runID, link: o.link, down: sup.OnDown, onDelayed: sup.OnDelayed}
	handle, err := o.link.Expect(runID, prepared.Executor, sandboxConfig(prepared.spec),
		agentlink.WithOnAttached(
			func(attempt uint32, agentRunning bool) {
				att.cancelGrace()
				o.log.Info("sandbox attached", "run_id", runID, "pod", prepared.SandboxName,
					"attempt", attempt, "agent_running", agentRunning)
				if attempt > 1 && !agentRunning {
					att.fire("sidecar_restarted")
				}
			}),
		agentlink.WithOnLost(func(cause error) {
			o.log.Warn("sandbox tunnel lost; holding the session open",
				"run_id", runID, "pod", prepared.SandboxName,
				"grace", o.cfg.attachGrace.String(), "err", cause)
			att.startGrace(o.cfg.attachGrace)
		}),
		agentlink.WithOnError(func(reason string, cause error) {
			o.log.Error("sandbox reported a terminal error",
				"run_id", runID, "pod", prepared.SandboxName, "reason", reason, "err", cause)
			att.fire(reason)
		}),
		agentlink.WithOnAgentExit(func(code int32) {
			o.log.Info("sandbox agent exited; ending the session",
				"run_id", runID, "pod", prepared.SandboxName, "exit_code", code)
			att.fire("agent_exited")
		}),
	)
	if err != nil {
		return nil, err
	}
	att.handle = handle
	return att, nil
}

func (a *Attachment) fire(cause string) {
	if a.forgotten.Load() {
		return
	}
	a.downOnce.Do(func() {
		if a.down != nil {
			a.down(cause)
		}
	})
}

func (a *Attachment) startGrace(d time.Duration) {
	a.mu.Lock()
	if a.grace != nil {
		a.grace.Stop()
	}
	a.grace = time.AfterFunc(d, func() { a.fire("tunnel_lost") })
	a.mu.Unlock()
}

func (a *Attachment) cancelGrace() {
	a.mu.Lock()
	if a.grace != nil {
		a.grace.Stop()
		a.grace = nil
	}
	a.mu.Unlock()
}

func (o *Orchestrator) Activate(ctx context.Context, sink EventSink, prepared *Prepared, att *Attachment) (*Session, error) {
	ctx, op := o.tel.Start(ctx, "Activate", "session_id", prepared.spec.SessionID)
	defer op.Complete()

	claimName, sandboxName := prepared.ClaimName, prepared.SandboxName
	release := func() {
		if !prepared.Resumed {
			o.teardownQuietly(ctx, claimName)
		}
	}
	fail := func(err error) (*Session, error) {
		att.Forget()
		release()
		return nil, op.Failure(err)
	}

	pod, err := o.pods.Get(ctx, sandboxName)
	if err != nil {
		return fail(fmt.Errorf("re-read pod %q: %w", sandboxName, err))
	}
	if string(pod.UID) != prepared.Executor.PodUID {
		return fail(fmt.Errorf("pod %q was replaced since prepare (uid %s != sealed %s)",
			sandboxName, pod.UID, prepared.Executor.PodUID))
	}

	attachCtx, cancel := context.WithTimeout(ctx, o.cfg.attachTimeout)
	defer cancel()
	if err := o.awaitAttach(attachCtx, att, sandboxName); err != nil {
		return fail(err)
	}

	if err := att.handle.AwaitReady(attachCtx); err != nil {
		return fail(fmt.Errorf("await sandbox ready: %w", err))
	}
	if err := att.handle.SpawnAgent(ctx); err != nil {
		return fail(fmt.Errorf("spawn agent: %w", err))
	}
	stdin, stdout, err := att.handle.AwaitAgentIO(attachCtx)
	if err != nil {
		return fail(fmt.Errorf("await agent io: %w", err))
	}

	closeAll := func() error {
		att.handle.Shutdown("session_end")
		att.Forget()
		return nil
	}
	session, err := o.openSession(ctx, o.tel, sink, stdin, stdout, closeAll, SessionParams{
		Cwd:             sandboxCwd,
		MCPServers:      RewriteMCPServers(prepared.spec.Endpoints),
		SystemPrompt:    prepared.spec.SystemPrompt,
		SessionConfig:   prepared.spec.Template.Spec.SessionConfig,
		ResumeSessionID: prepared.spec.ResumeACPSessionID,
	})
	if err != nil {
		_ = closeAll()
		if !errors.Is(err, ErrResumeUnavailable) {
			release()
		}
		return nil, op.Failure(err)
	}

	o.tel.Debug(ctx, "acp session live", "claim", claimName, "pod", sandboxName, "acp_session_id", session.ID())
	return session, nil
}

var ErrAttachTimeout = errors.New("the sandbox never connected back to the agent link")

func (o *Orchestrator) awaitAttach(ctx context.Context, att *Attachment, sandboxName string) error {
	o.log.InfoContext(ctx, "waiting for the sandbox to attach",
		"pod", sandboxName, "run_id", att.runID, "harness_route", o.cfg.harnessRoute,
		"warn_after", o.cfg.attachWarnAfter.String(), "timeout", o.cfg.attachTimeout.String())

	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- att.handle.AwaitAttach(ctx) }()

	var delayed <-chan time.Time
	if o.cfg.attachWarnAfter > 0 {
		timer := time.NewTimer(o.cfg.attachWarnAfter)
		defer timer.Stop()
		delayed = timer.C
	}
	ticker := time.NewTicker(attachProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			if err == nil {
				o.log.InfoContext(ctx, "sandbox attached",
					"pod", sandboxName, "run_id", att.runID, "waited", time.Since(started).Round(time.Second).String())
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				o.log.ErrorContext(ctx, "the sandbox never attached; it could not reach the agent link",
					"pod", sandboxName, "run_id", att.runID, "harness_route", o.cfg.harnessRoute,
					"waited", time.Since(started).Round(time.Second).String(),
					"hint", "check the sandbox's sidecar container logs and the Pomerium route")
				return fmt.Errorf("%w after %s (it dials %s)", ErrAttachTimeout,
					time.Since(started).Round(time.Second), o.cfg.harnessRoute)
			}
			return fmt.Errorf("await sandbox attach: %w", err)
		case <-delayed:
			delayed = nil
			waited := time.Since(started).Round(time.Second)
			o.log.WarnContext(ctx, "the sandbox has not attached yet",
				"pod", sandboxName, "run_id", att.runID, "harness_route", o.cfg.harnessRoute, "waited", waited.String(),
				"hint", "normal while the run is unapproved; otherwise check the sidecar logs and the Pomerium route")
			if att.onDelayed != nil {
				att.onDelayed(waited)
			}
		case <-ticker.C:
			o.tel.Debug(ctx, "still waiting for the sandbox to attach",
				"pod", sandboxName, "run_id", att.runID, "waited", time.Since(started).Round(time.Second).String())
		}
	}
}

const attachProgressInterval = 30 * time.Second

var ErrNoSandboxClient = errors.New("no sandbox client configured; cannot suspend or resume")

func (o *Orchestrator) Suspend(ctx context.Context, claimName string) error {
	ctx, op := o.tel.Start(ctx, "Suspend", "claim", claimName)
	defer op.Complete()

	name, err := o.sandboxNameFor(ctx, claimName)
	if err != nil {
		return op.Failure(err)
	}
	if err := o.patchSandbox(ctx, name, map[string]any{
		"operatingMode": string(agentsv1.SandboxOperatingModeSuspended),
	}); err != nil {
		return op.Failure(fmt.Errorf("suspend sandbox %q: %w", name, err))
	}
	o.log.InfoContext(ctx, "sandbox suspended; the workspace is retained",
		"claim", claimName, "sandbox", name)
	return nil
}

func (o *Orchestrator) Resume(ctx context.Context, claimName string) (string, error) {
	ctx, op := o.tel.Start(ctx, "Resume", "claim", claimName)
	defer op.Complete()

	name, err := o.sandboxNameFor(ctx, claimName)
	if err != nil {
		return "", op.Failure(err)
	}
	patch := map[string]any{"operatingMode": string(agentsv1.SandboxOperatingModeRunning)}
	if o.cfg.lease > 0 {
		patch["shutdownTime"] = leaseTime(time.Now(), o.cfg.lease)
	}
	if err := o.patchSandbox(ctx, name, patch); err != nil {
		return "", op.Failure(fmt.Errorf("resume sandbox %q: %w", name, err))
	}
	o.log.InfoContext(ctx, "sandbox resuming", "claim", claimName, "sandbox", name)

	if _, err := o.waitReady(ctx, claimName); err != nil {
		return "", op.Failure(err)
	}
	return name, nil
}

func (o *Orchestrator) ExtendLease(ctx context.Context, claimName string) (time.Time, error) {
	if o.cfg.lease <= 0 {
		return time.Time{}, nil
	}
	name, err := o.sandboxNameFor(ctx, claimName)
	if err != nil {
		return time.Time{}, err
	}
	until := time.Now().Add(o.cfg.lease)
	if err := o.patchSandbox(ctx, name, map[string]any{
		"shutdownTime": leaseTime(time.Now(), o.cfg.lease),
	}); err != nil {
		return time.Time{}, fmt.Errorf("extend lease on sandbox %q: %w", name, err)
	}
	o.tel.Debug(ctx, "sandbox lease extended", "claim", claimName, "sandbox", name, "until", until)
	return until, nil
}

func (o *Orchestrator) Lease() time.Duration { return o.cfg.lease }

func leaseTime(now time.Time, lease time.Duration) string {
	return metav1.NewTime(now.Add(lease).Truncate(time.Second)).UTC().Format(time.RFC3339)
}

func (o *Orchestrator) patchSandbox(ctx context.Context, name string, spec map[string]any) error {
	if o.sandboxes == nil {
		return ErrNoSandboxClient
	}
	data, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		return err
	}
	return o.sandboxes.Patch(ctx, name, data)
}

func (o *Orchestrator) sandboxNameFor(ctx context.Context, claimName string) (string, error) {
	if o.sandboxes == nil {
		return "", ErrNoSandboxClient
	}
	claim, err := o.claims.Get(ctx, claimName)
	if err != nil {
		return "", fmt.Errorf("get sandbox claim %q: %w", claimName, err)
	}
	name := claim.Status.SandboxStatus.Name
	if name == "" {
		return "", fmt.Errorf("sandbox claim %q has no bound sandbox yet", claimName)
	}
	return name, nil
}

func (o *Orchestrator) Teardown(ctx context.Context, claimName string) error {
	if err := o.claims.Delete(ctx, claimName); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete sandbox claim %q: %w", claimName, err)
	}
	return nil
}

func (o *Orchestrator) teardownQuietly(ctx context.Context, claimName string) {
	if err := o.Teardown(ctx, claimName); err != nil {
		o.log.WarnContext(ctx, "failed to tear down sandbox claim after launch failure",
			"claim", claimName, "err", err)
	}
}

func (o *Orchestrator) waitReady(ctx context.Context, claimName string) (string, error) {
	var name string
	var getErr error
	err := wait.PollUntilContextTimeout(ctx, readyPollInterval, readyTimeout, true, func(ctx context.Context) (bool, error) {
		claim, err := o.claims.Get(ctx, claimName)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			getErr = fmt.Errorf("get sandbox claim %q: %w", claimName, err)
			return false, getErr
		default:
			if n, ready := sandboxReady(claim); ready {
				o.tel.Debug(ctx, "sandbox ready", "claim", claimName, "pod", n)
				name = n
				return true, nil
			}
		}
		o.tel.Debug(ctx, "waiting for sandbox to be ready", "claim", claimName)
		return false, nil
	})
	if getErr != nil {
		return "", getErr
	}
	if err != nil {
		return "", fmt.Errorf("sandbox %q not ready within %s: %w", claimName, readyTimeout, err)
	}
	return name, nil
}

func sandboxReady(claim *sbxv1.SandboxClaim) (string, bool) {
	name := claim.Status.SandboxStatus.Name
	if name == "" {
		return "", false
	}
	if c := meta.FindStatusCondition(claim.Status.Conditions, readyConditionType); c != nil {
		return name, c.Status == metav1.ConditionTrue
	}
	if len(claim.Status.SandboxStatus.PodIPs) > 0 {
		return name, true
	}
	return name, false
}
