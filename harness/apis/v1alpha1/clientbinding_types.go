package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClientBindingSpec registers one client of the Harness API and says what it may
// do.
//
// It is the platform team's artifact, not a client's: a client cannot create,
// widen or read its own binding. Who a client IS is a Pomerium decision (the
// route's identity_providers plus its policy); what a client MAY DO is this.
type ClientBindingSpec struct {
	// Subject is the verified caller identity this binding applies to: the
	// subject of the assertion Pomerium stamps on the client API route. A client
	// cannot state its own.
	//
	// Pomerium mints it as "<identity provider>/<sub>", so for a projected
	// ServiceAccount token verified by a provider named "cluster" it reads
	// "cluster/system:serviceaccount:agentops-slackbot:agentops-slackbot".
	//
	// Note this is NOT the string the route's own PPL policy matches. That one
	// is evaluated against the raw sub of the presented token and carries no
	// prefix; this is the sub of the assertion Pomerium MINTS, which does. The
	// two are easy to swap and fail in opposite directions — a wrong policy is a
	// 403 the client reports, a wrong binding is a binding that never applies.
	//
	// Take the exact value from the harness's log rather than from this comment:
	// the prefix is a property of the deployment's provider name, and the harness
	// prints each client once, at Info, the first time it sees one ("admitted a
	// client on the harness API"). A binding whose subject does not match is a
	// binding that never applies.
	Subject string `json:"subject"`

	// Templates is the allow-list of AgentTemplate names this client may run.
	// Empty means the binding grants no templates. A client with no
	// ClientBinding is refused every call.
	// +optional
	Templates []string `json:"templates,omitempty"`

	// Quotas bound what a client can spend. The platform checks them each time
	// the client creates or continues a session. Unset means no limit.
	// +optional
	Quotas *ClientQuotas `json:"quotas,omitempty"`
}

// ClientQuotas caps how many sessions a client holds at once and how fast it
// starts them. None of them caps a client's total use.
type ClientQuotas struct {
	// MaxLiveSessions caps sessions in any non-terminal state.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxLiveSessions int32 `json:"maxLiveSessions,omitempty"`

	// MaxPendingApprovals caps sessions waiting for a human to approve. Pending is
	// the expensive state and is capped separately: a never-approved session holds
	// a warm pod for the whole approval window, so the two limits answer different
	// questions.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxPendingApprovals int32 `json:"maxPendingApprovals,omitempty"`

	// CreateRatePerMinute caps how many sessions a client may start per minute.
	// A new session and the continuation of a suspended one both count, and a
	// client may spend a whole minute's allowance at once. Each start asks a
	// human for approval, so an uncapped client is a spam primitive as much as a
	// cost one.
	// +optional
	// +kubebuilder:validation:Minimum=0
	CreateRatePerMinute int32 `json:"createRatePerMinute,omitempty"`
}

// ClientBindingStatus is the observed state of a ClientBinding.
type ClientBindingStatus struct {
	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=clibind
// +kubebuilder:printcolumn:name="Subject",type=string,JSONPath=`.spec.subject`
// +kubebuilder:printcolumn:name="Templates",type=string,JSONPath=`.spec.templates`

// ClientBinding registers a client of the Harness API: which verified identity
// it is, and which agent templates it may run.
type ClientBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClientBindingSpec   `json:"spec,omitempty"`
	Status ClientBindingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClientBindingList contains a list of ClientBinding.
type ClientBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClientBinding `json:"items"`
}
