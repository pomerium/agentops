package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MCPServerRef is an MCP server the agent uses. The platform lists its URL in
// the run that the person approves. The sandbox sidecar serves it to the agent
// on a loopback port and adds the run token to each request.
type MCPServerRef struct {
	// Name identifies the server and must be unique within the AgentTemplate.
	// The agent sees the server under this name.
	// +required
	Name string `json:"name"`

	// URL is the streamable-HTTP endpoint of the MCP server: a Pomerium route
	// that accepts the run token. It must be https, because the sandbox sidecar
	// sends the run token only over TLS.
	// +required
	// +kubebuilder:validation:Pattern=`^https://`
	URL string `json:"url"`

	// DialAddress optionally overrides the host[:port] the sandbox sidecar
	// connects to for this server, while TLS SNI and the Host header still come
	// from URL. Use it to reach an in-cluster Service
	// (e.g. "pomerium-proxy.pomerium.svc.cluster.local:443") instead of
	// hairpinning through an external LoadBalancer VIP. Empty dials the
	// host:port parsed from URL.
	// +optional
	DialAddress string `json:"dialAddress,omitempty"`
}

// SandboxWarmPoolReference selects the agent-sandbox SandboxWarmPool that runs
// the agent. The pool references the SandboxTemplate.
type SandboxWarmPoolReference struct {
	// Name of the SandboxWarmPool (in the same namespace).
	// +required
	Name string `json:"name"`
}

// AgentTemplateSpec defines the desired configuration of a workflow.
//
// A workflow runs when a client asks for it by metadata.name. Which Slack
// channel asks for which template is the Slack bot's own configuration and is
// deliberately not a cluster resource.
type AgentTemplateSpec struct {
	// SystemPrompt is the system prompt provided to the agent for this
	// workflow.
	// +optional
	SystemPrompt string `json:"systemPrompt,omitempty"`

	// SessionConfig sets agent-advertised ACP session configuration options
	// (session/set_config_option) after the session is created, keyed by
	// option id. The value is the option's value id for select options (e.g.
	// model: opus, effort: high on the Claude Code harness) or "true"/"false"
	// for boolean options. Strict: if the harness does not advertise a
	// configured option id, or rejects the value, the session fails to
	// launch — a session running with silently unapplied config would be
	// misleading.
	// +optional
	SessionConfig map[string]string `json:"sessionConfig,omitempty"`

	// RequiredMCPServers lists the MCP servers that must be connected for the
	// invoking user before the workflow can run. They are disclosed to the
	// approver on Pomerium's consent page, with a Connect link for any the
	// approver has not yet authorized.
	//
	// This is not what the run token is allowed to reach. Nothing here grants
	// access: each Pomerium route decides for itself whether it accepts a run
	// token and which runs its policy admits.
	// +optional
	// +listType=map
	// +listMapKey=name
	RequiredMCPServers []MCPServerRef `json:"requiredMCPServers,omitempty"`

	// WarmPoolRef selects the agent-sandbox SandboxWarmPool that runs this
	// agent. The pool references a SandboxTemplate, which defines the harness
	// image, the sidecar, and, when the agent needs a repository checked out,
	// the git working context on the template's git-init init container. A pool
	// whose template checks out a repository belongs to one agent. The platform
	// creates one SandboxClaim against this pool per session. The claim carries
	// no session data, so it can adopt a pre-warmed pod from the pool.
	// +required
	WarmPoolRef SandboxWarmPoolReference `json:"warmPoolRef"`
}

// AgentTemplateStatus is the observed state of an AgentTemplate.
type AgentTemplateStatus struct {
	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=agt
// +kubebuilder:printcolumn:name="WarmPool",type=string,JSONPath=`.spec.warmPoolRef.name`

// AgentTemplate is the declarative definition of an agent that clients of the
// Harness API can run.
type AgentTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentTemplateSpec   `json:"spec,omitempty"`
	Status AgentTemplateStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AgentTemplateList contains a list of AgentTemplate.
type AgentTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentTemplate `json:"items"`
}
