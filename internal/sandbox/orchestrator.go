package sandbox

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/pomerium/agentops/internal/telemetry"
)

// readyConditionType is the SandboxClaim/Sandbox condition that signals the
// sandbox pod is running and reachable.
const readyConditionType = "Ready"

// ClaimClient is the subset of the agent-sandbox SandboxClaim API the
// orchestrator needs. It is satisfied by the generated extensions clientset and
// is injectable for testing.
type ClaimClient interface {
	Create(ctx context.Context, claim *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error)
	Get(ctx context.Context, name string) (*sbxv1.SandboxClaim, error)
	Delete(ctx context.Context, name string) error
}

// AgentConn is a bidirectional stdio pipe to the ACP agent process running in a
// sandbox pod.
type AgentConn struct {
	Stdin   io.WriteCloser
	Stdout  io.Reader
	CloseFn func() error
}

// Close releases the connection.
func (a *AgentConn) Close() error {
	if a.CloseFn != nil {
		return a.CloseFn()
	}
	return nil
}

// PodExecutor opens a stdio stream to a command executed inside a pod
// container. An empty container targets the pod's default (first) container.
type PodExecutor interface {
	OpenAgent(ctx context.Context, namespace, pod, container string, command []string) (*AgentConn, error)
}

// Config configures an Orchestrator.
type Config struct {
	Namespace string
	// AgentCommand is the command exec'd inside the sandbox pod that speaks ACP
	// over its stdio.
	AgentCommand []string
	// AgentContainer is the pod container the agent command is exec'd in.
	// Defaults to "agent".
	AgentContainer string
	// SidecarContainer is the pod container running the secret-isolating proxy.
	SidecarContainer string
	// SidecarCommand is the command exec'd in the sidecar container; it speaks
	// SidecarControlService gRPC over its stdio.
	SidecarCommand []string
	// SidecarReadyTimeout bounds how long Launch waits for the sidecar's
	// listeners to come up after sending the configuration.
	SidecarReadyTimeout time.Duration
	// Cwd is the working directory passed to ACP NewSession.
	Cwd string
	// ReadyTimeout bounds how long Launch waits for the sandbox to become ready.
	ReadyTimeout time.Duration
	// PollInterval is the readiness poll cadence.
	PollInterval time.Duration
}

// Orchestrator creates SandboxClaims, waits for readiness, opens the ACP
// session, and tears sandboxes down.
type Orchestrator struct {
	claims ClaimClient
	exec   PodExecutor
	cfg    Config
	log    *slog.Logger
	tel    *telemetry.Component

	// openSession is OpenSession, injectable for tests.
	openSession func(ctx context.Context, tel *telemetry.Component, sink EventSink, agentStdin io.Writer, agentStdout io.Reader, closeFn func() error, params SessionParams) (*Session, error)
}

// New constructs an Orchestrator.
func New(claims ClaimClient, exec PodExecutor, cfg Config, log *slog.Logger) *Orchestrator {
	if cfg.ReadyTimeout == 0 {
		cfg.ReadyTimeout = 3 * time.Minute
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if len(cfg.AgentCommand) == 0 {
		cfg.AgentCommand = []string{"/bin/sh", "-lc", "exec ${ACP_AGENT_CMD:-acp-agent}"}
	}
	// Sandbox pods always have at least two containers now, and the exec API
	// refuses requests without an explicit container name on multi-container
	// pods — so both names need non-empty defaults.
	if cfg.AgentContainer == "" {
		cfg.AgentContainer = AgentContainerName
	}
	if cfg.SidecarContainer == "" {
		cfg.SidecarContainer = SidecarContainerName
	}
	if len(cfg.SidecarCommand) == 0 {
		cfg.SidecarCommand = []string{"/usr/local/bin/sidecar", "serve"}
	}
	if cfg.SidecarReadyTimeout == 0 {
		cfg.SidecarReadyTimeout = 30 * time.Second
	}
	if cfg.Cwd == "" {
		cfg.Cwd = "/workspace"
	}
	if log == nil {
		log = slog.Default()
	}
	return &Orchestrator{
		claims: claims, exec: exec, cfg: cfg, log: log,
		tel:         telemetry.New(log, "sandbox", slog.LevelDebug),
		openSession: OpenSession,
	}
}

// LaunchResult identifies the created sandbox resources.
type LaunchResult struct {
	ClaimName   string
	SandboxName string
}

// Launch creates (or adopts) the SandboxClaim for spec, waits for it to become
// ready, configures the secret-isolating sidecar with the proxied endpoints
// (MCP tokens and other secret headers stay in the sidecar; the agent only
// sees 127.0.0.1 listeners), opens an ACP session over an exec stream to the
// agent, and returns the live Session. On any failure after the claim is
// created, the claim is torn down to avoid leaks.
func (o *Orchestrator) Launch(ctx context.Context, sink EventSink, spec LaunchSpec, endpoints []ProxiedEndpoint) (*Session, LaunchResult, error) {
	ctx, op := o.tel.Start(ctx, "Launch", "session_id", spec.SessionID)
	defer op.Complete()

	// The claim env must land on the same container the agent is exec'd in.
	spec.AgentContainer = o.cfg.AgentContainer
	claim := BuildSandboxClaim(o.cfg.Namespace, spec)

	created, err := o.claims.Create(ctx, claim)
	if apierrors.IsAlreadyExists(err) {
		o.tel.Debug(ctx, "sandbox claim already exists; adopting", "claim", claim.Name)
		created, err = o.claims.Get(ctx, claim.Name)
	}
	if err != nil {
		return nil, LaunchResult{}, op.Failure(fmt.Errorf("create sandbox claim: %w", err))
	}
	result := LaunchResult{ClaimName: created.Name}
	o.tel.Debug(ctx, "sandbox claim created; waiting for ready", "claim", created.Name)

	sandboxName, err := o.waitReady(ctx, created.Name)
	if err != nil {
		o.teardownQuietly(ctx, created.Name)
		return nil, result, op.Failure(err)
	}
	result.SandboxName = sandboxName

	// Phase 1: start and configure the sidecar proxy. Secret headers travel
	// only over this stream, never through the agent container.
	o.tel.Debug(ctx, "exec sidecar command in sandbox", "namespace", o.cfg.Namespace,
		"pod", sandboxName, "container", o.cfg.SidecarContainer, "command", o.cfg.SidecarCommand)
	sidecarConn, err := o.exec.OpenAgent(ctx, o.cfg.Namespace, sandboxName, o.cfg.SidecarContainer, o.cfg.SidecarCommand)
	if err != nil {
		o.teardownQuietly(ctx, created.Name)
		return nil, result, op.Failure(fmt.Errorf("open sidecar stream: %w", err))
	}
	sidecar, err := configureSidecar(ctx, o.tel, sidecarConn, endpoints, o.cfg.SidecarReadyTimeout)
	if err != nil {
		o.teardownQuietly(ctx, created.Name)
		return nil, result, op.Failure(fmt.Errorf("configure sidecar: %w", err))
	}

	// Phase 2: start the agent and open the ACP session against the sidecar's
	// loopback listeners.
	o.tel.Debug(ctx, "exec agent command in sandbox", "namespace", o.cfg.Namespace,
		"pod", sandboxName, "container", o.cfg.AgentContainer, "command", o.cfg.AgentCommand)
	agentConn, err := o.exec.OpenAgent(ctx, o.cfg.Namespace, sandboxName, o.cfg.AgentContainer, o.cfg.AgentCommand)
	if err != nil {
		_ = sidecar.Close()
		o.teardownQuietly(ctx, created.Name)
		return nil, result, op.Failure(fmt.Errorf("open agent stream: %w", err))
	}

	closeAll := func() error {
		err := agentConn.Close()
		if serr := sidecar.Close(); err == nil {
			err = serr
		}
		return err
	}
	session, err := o.openSession(ctx, o.tel, sink, agentConn.Stdin, agentConn.Stdout, closeAll, SessionParams{
		Cwd:           o.cfg.Cwd,
		MCPServers:    RewriteMCPServers(endpoints),
		SystemPrompt:  spec.SystemPrompt,
		SessionConfig: spec.Template.Spec.SessionConfig,
	})
	if err != nil {
		_ = closeAll()
		o.teardownQuietly(ctx, created.Name)
		return nil, result, op.Failure(err)
	}

	// Supervise the sidecar for the session's lifetime: if envoy dies the
	// sidecar reports STATE_ERROR, and we close the session (which tears down
	// both exec streams) so the next turn fails fast instead of hanging on
	// dead loopback listeners until the session TTL.
	supervisorCtx := context.WithoutCancel(ctx)
	sidecar.Supervise(supervisorCtx, o.tel, func(cause error) {
		o.log.WarnContext(supervisorCtx, "sidecar failed mid-session; closing session",
			"claim", created.Name, "pod", sandboxName, "err", cause)
		_ = session.Close()
	})

	o.tel.Debug(ctx, "acp session live", "claim", created.Name, "pod", sandboxName, "acp_session_id", session.ID())
	return session, result, nil
}

// Teardown deletes the SandboxClaim, which cascades to the sandbox pod.
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

// waitReady polls the SandboxClaim until it reports a Ready condition and an
// assigned sandbox name, or the timeout elapses.
func (o *Orchestrator) waitReady(ctx context.Context, claimName string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, o.cfg.ReadyTimeout)
	defer cancel()

	ticker := time.NewTicker(o.cfg.PollInterval)
	defer ticker.Stop()

	for {
		claim, err := o.claims.Get(ctx, claimName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return "", fmt.Errorf("get sandbox claim %q: %w", claimName, err)
			}
		} else if name, ready := sandboxReady(claim); ready {
			o.tel.Debug(ctx, "sandbox ready", "claim", claimName, "pod", name)
			return name, nil
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("sandbox %q not ready within %s: %w", claimName, o.cfg.ReadyTimeout, ctx.Err())
		case <-ticker.C:
			o.tel.Debug(ctx, "waiting for sandbox to be ready", "claim", claimName)
		}
	}
}

// sandboxReady reports whether a claim has an assigned sandbox that is reachable.
func sandboxReady(claim *sbxv1.SandboxClaim) (string, bool) {
	name := claim.Status.SandboxStatus.Name
	if name == "" {
		return "", false
	}
	for _, c := range claim.Status.Conditions {
		if c.Type == readyConditionType {
			return name, c.Status == "True"
		}
	}
	// Some controllers populate pod IPs before surfacing a Ready condition;
	// treat an assigned, IP-bearing sandbox as reachable.
	if len(claim.Status.SandboxStatus.PodIPs) > 0 {
		return name, true
	}
	return name, false
}
