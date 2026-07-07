// Package v1alpha1 contains the API types for the
// agents.pomerium.com/v1alpha1 group, including the AgentTemplate CRD that
// declaratively defines the agentic workflows agentops can run.
//
// +kubebuilder:object:generate=true
// +groupName=agents.pomerium.com
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the group/version used to register these objects.
var GroupVersion = schema.GroupVersion{Group: "agents.pomerium.com", Version: "v1alpha1"}

// SchemeBuilder collects the functions that add this group-version's types to a
// runtime.Scheme.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme adds the types in this group-version to the given scheme.
var AddToScheme = SchemeBuilder.AddToScheme

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&AgentTemplate{}, &AgentTemplateList{},
		&ChannelConfig{}, &ChannelConfigList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
