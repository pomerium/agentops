//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type podExec struct {
	config    *rest.Config
	clientset kubernetes.Interface
}

func newPodExec(t *testing.T, cfg *rest.Config) *podExec {
	t.Helper()
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("kubernetes client: %v", err)
	}
	return &podExec{config: cfg, clientset: cs}
}

func (e *podExec) run(ctx context.Context, namespace, pod, container string, command []string) (string, error) {
	req := e.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.String() + stderr.String(), err
}

func (e *podExec) script(ctx context.Context, namespace, pod, container, script string) (string, error) {
	return e.run(ctx, namespace, pod, container, []string{"/bin/sh", "-c", script})
}
