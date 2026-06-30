// Package sandbox launches agent-sandbox SandboxClaims for an agent template,
// injects
// per-run context (system prompt, git checkout credentials), and owns the live
// ACP client connection to the running agent for the lifetime of a Slack
// thread.
package sandbox

import (
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
)

// Environment variable names injected into the sandbox at launch; the agent
// harness consumes them. The git working context (GIT_REPO_URL, GIT_REPO_REF,
// GIT_USERNAME, GIT_TOKEN) is deliberately NOT injected by the app — it is
// baked into the agent template's SandboxTemplate git-init init container.
//
// The system prompt is NOT here: the claude-agent-acp harness ignores env-based
// prompts and only honors a custom prompt delivered over the ACP session/new
// request. It is threaded through LaunchSpec.SystemPrompt → SessionParams and
// injected by OpenSession (see systemPromptMeta in acp.go).
const (
	EnvSessionID = "AGENT_SESSION_ID"
)

// Container names the SandboxTemplate must use; claim env vars are targeted
// at them explicitly.
const (
	// AgentContainerName runs the ACP harness; it receives the agent context
	// env and never any credentials.
	AgentContainerName = "agent"
	// GitInitContainerName is the init container that checks out the
	// template's git repo into the shared workspace. The git credentials are
	// targeted at it so they are gone (with the container) before the agent
	// starts.
	GitInitContainerName = "git-init"
	// SidecarContainerName runs the secret-isolating proxy the orchestrator
	// execs `sidecar serve` in.
	SidecarContainerName = "sidecar"
)

// LaunchSpec is the fully-resolved input to a sandbox launch: the agent
// template and the rendered system prompt. Git credentials are deliberately
// absent — the SandboxTemplate's git-init container references its
// credentials Secret directly via secretKeyRef, so the token never passes
// through this app or the SandboxClaim.
type LaunchSpec struct {
	SessionID    string
	Template     *v1alpha1.AgentTemplate
	SystemPrompt string
	// AgentContainer is the pod container the agent context env (system prompt,
	// session id) is targeted at. It must match the container the orchestrator
	// execs the agent in. Empty means the default (AgentContainerName).
	AgentContainer string
	// Deadline, if set, becomes the SandboxClaim shutdown time (the claim is
	// deleted when reached), enforcing the session TTL.
	Deadline time.Time
}

// ClaimName returns the deterministic SandboxClaim name for a session, so a
// retried launch is idempotent.
func ClaimName(sessionID string) string {
	return "smc-" + sanitizeName(sessionID)
}

// BuildSandboxClaim constructs the SandboxClaim for a launch. It is a pure
// function (no clock, no cluster access) so it can be unit-tested.
func BuildSandboxClaim(namespace string, spec LaunchSpec) *sbxv1.SandboxClaim {
	claim := &sbxv1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClaimName(spec.SessionID),
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "agentops",
				"agents.pomerium.com/session":  sanitizeName(spec.SessionID),
			},
		},
		// v1beta1: a claim binds to a SandboxWarmPool (which references the
		// harness SandboxTemplate), not to a template directly. There is no
		// warm-pool policy field anymore; instead, because the claim sets Env,
		// the sandbox is always cold-started from the pool's template — i.e. a
		// fresh pod per run, the behavior the old WarmPoolPolicyNone gave us.
		// Operators run the referenced pool with replicas: 0 so nothing is
		// pre-warmed.
		Spec: sbxv1.SandboxClaimSpec{
			WarmPoolRef: sbxv1.SandboxWarmPoolRef{Name: spec.Template.Spec.WarmPoolRef.Name},
			Env:         buildEnv(spec),
		},
	}

	if !spec.Deadline.IsZero() {
		shutdown := metav1.NewTime(spec.Deadline)
		claim.Spec.Lifecycle = &sbxv1.Lifecycle{
			ShutdownTime:   &shutdown,
			ShutdownPolicy: sbxv1.ShutdownPolicyDelete,
		}
	}
	return claim
}

func buildEnv(spec LaunchSpec) []sbxv1.EnvVar {
	container := spec.AgentContainer
	if container == "" {
		container = AgentContainerName
	}
	// Only the session id rides as a container env var. The system prompt is
	// delivered over ACP (see systemPromptMeta), not via env — the harness
	// ignores env-based prompts.
	return []sbxv1.EnvVar{
		{Name: EnvSessionID, Value: spec.SessionID, ContainerName: container},
	}
}

// sanitizeName lowercases and replaces characters that are invalid in a
// Kubernetes resource name with '-', keeping the result DNS-1123 friendly.
func sanitizeName(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "session"
	}
	if len(out) > 200 {
		out = out[:200]
	}
	return out
}
