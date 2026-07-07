package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ChannelConfigName is the required metadata.name of the singleton
// ChannelConfig (enforced by CEL validation on the CRD).
const ChannelConfigName = "default"

// ChannelConfigSpec binds chat channels to agent templates.
type ChannelConfigSpec struct {
	// SlackChannelTemplates maps a Slack channel ID (e.g. "C0123ABC") to the
	// AgentTemplate (its metadata.name, same namespace) that channel runs.
	// Mentioning the bot in a mapped channel starts that agent with the whole
	// post-mention text as the prompt; map keys make the channel:template
	// binding 1:1 by construction.
	// +optional
	SlackChannelTemplates map[string]string `json:"slackChannelTemplates,omitempty"`
}

// ChannelConfigStatus is the observed state of a ChannelConfig.
type ChannelConfigStatus struct {
	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=chcfg
// +kubebuilder:validation:XValidation:rule="self.metadata.name == 'default'",message="ChannelConfig is a singleton; it must be named 'default'"

// ChannelConfig is the global (singleton, named "default") binding of chat
// channels to the agent templates they run. Keeping all bindings in one map
// guarantees each channel maps to exactly one template.
type ChannelConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ChannelConfigSpec   `json:"spec,omitempty"`
	Status ChannelConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ChannelConfigList contains a list of ChannelConfig.
type ChannelConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ChannelConfig `json:"items"`
}
