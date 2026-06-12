package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MCPServerRef identifies an MCP server that a workflow requires the invoking
// user to be connected to before the workflow can run.
type MCPServerRef struct {
	// Name is a short, stable identifier for the server, unique within a
	// AgentTemplate. It is used as the credential key and surfaced to the
	// user in auth prompts.
	// +required
	Name string `json:"name"`

	// URL is the streamable-HTTP endpoint of the MCP server.
	// +required
	URL string `json:"url"`
}

// SandboxTemplateReference selects the SandboxTemplate that provides the agent
// harness and base image for a workflow.
type SandboxTemplateReference struct {
	// Name of the SandboxTemplate (in the same namespace).
	// +required
	Name string `json:"name"`
}

// AgentTemplateSpec defines the desired configuration of a workflow.
//
// A workflow is selected from Slack by @mentioning the bot followed by this
// template's metadata.name, e.g. "@bot deploy-service ...".
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
	// invoking user before the workflow can run.
	// +optional
	// +listType=map
	// +listMapKey=name
	RequiredMCPServers []MCPServerRef `json:"requiredMCPServers,omitempty"`

	// SandboxTemplateRef selects how the sandbox is baked for this workflow:
	// the harness + base image, the secret-isolating sidecar, and — when the
	// workflow needs a repository checked out — the git working context (repo
	// URL/ref and credentials secretKeyRef on the template's git-init init
	// container). Templates that bake a repo are workflow-specific by design
	// (e.g. "pomerium-zero-claude-code").
	// +required
	SandboxTemplateRef SandboxTemplateReference `json:"sandboxTemplateRef"`
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
// +kubebuilder:printcolumn:name="Sandbox",type=string,JSONPath=`.spec.sandboxTemplateRef.name`

// AgentTemplate is the declarative definition of an agentic workflow that
// users can invoke from Slack.
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
