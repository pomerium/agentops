package sandbox

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	agentsv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sbxclientv1 "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/typed/api/v1beta1"
	extv1 "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned/typed/api/v1beta1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

type clientsetClaimClient struct {
	iface extv1.SandboxClaimInterface
}

func NewClaimClient(cfg *rest.Config, namespace string) (ClaimClient, error) {
	c, err := extv1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &clientsetClaimClient{iface: c.SandboxClaims(namespace)}, nil
}

func (c *clientsetClaimClient) Create(ctx context.Context, claim *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error) {
	return c.iface.Create(ctx, claim, metav1.CreateOptions{})
}

func (c *clientsetClaimClient) Get(ctx context.Context, name string) (*sbxv1.SandboxClaim, error) {
	return c.iface.Get(ctx, name, metav1.GetOptions{})
}

func (c *clientsetClaimClient) Delete(ctx context.Context, name string) error {
	return c.iface.Delete(ctx, name, metav1.DeleteOptions{})
}

type clientsetSandboxClient struct {
	iface sbxclientv1.SandboxInterface
}

func NewSandboxClient(cfg *rest.Config, namespace string) (SandboxClient, error) {
	c, err := sbxclientv1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &clientsetSandboxClient{iface: c.Sandboxes(namespace)}, nil
}

func (c *clientsetSandboxClient) Get(ctx context.Context, name string) (*agentsv1.Sandbox, error) {
	return c.iface.Get(ctx, name, metav1.GetOptions{})
}

func (c *clientsetSandboxClient) Patch(ctx context.Context, name string, data []byte) error {
	_, err := c.iface.Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

type clientsetPodGetter struct {
	clientset kubernetes.Interface
	namespace string
}

func NewPodGetter(cfg *rest.Config, namespace string) (PodGetter, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &clientsetPodGetter{clientset: cs, namespace: namespace}, nil
}

func (g *clientsetPodGetter) Get(ctx context.Context, name string) (*corev1.Pod, error) {
	return g.clientset.CoreV1().Pods(g.namespace).Get(ctx, name, metav1.GetOptions{})
}
