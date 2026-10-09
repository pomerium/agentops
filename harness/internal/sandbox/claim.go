package sandbox

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

const (
	AgentContainerName   = "agent"
	GitInitContainerName = "git-init"
	SidecarContainerName = "sidecar"
)

type LaunchSpec struct {
	SessionID          string
	Template           *v1alpha1.AgentTemplate
	SystemPrompt       string
	Endpoints          []ProxiedEndpoint
	ResumeACPSessionID string
}

func ClaimName(sessionID string) string {
	return "smc-" + sanitizeName(sessionID)
}

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
		Spec: sbxv1.SandboxClaimSpec{
			WarmPoolRef: sbxv1.SandboxWarmPoolRef{Name: spec.Template.Spec.WarmPoolRef.Name},
		},
	}
	return claim
}

func sandboxConfig(spec LaunchSpec) *agentlinkpb.SandboxConfig {
	eps := make([]*agentlinkpb.ProxiedEndpoint, 0, len(spec.Endpoints))
	for _, ep := range spec.Endpoints {
		eps = append(eps, &agentlinkpb.ProxiedEndpoint{
			Name:        SidecarEndpointName(ep.Name),
			ListenPort:  ep.ListenPort,
			UpstreamUrl: ep.UpstreamURL,
			DialAddress: ep.DialAddress,
		})
	}
	return &agentlinkpb.SandboxConfig{Endpoints: eps}
}

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
