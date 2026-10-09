//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

const (
	optInEnv = "AGENTOPS_E2E"

	k3sImage  = "rancher/k3s:v1.33.4-k3s1"
	echoImage = "mendhak/http-https-echo:36"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

func startK3sContainer(ctx context.Context, t *testing.T) *k3s.K3sContainer {
	t.Helper()

	ctr, err := k3s.Run(ctx, k3sImage,
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Privileged = true
			hc.CgroupnsMode = "private"
			hc.Tmpfs = map[string]string{"/run": "", "/var/run": ""}
			hc.Mounts = []mount.Mount{}
		}),
	)
	if err != nil {
		t.Fatalf("start k3s: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_ = ctr.Terminate(cctx)
	})
	return ctr
}

func k8sClients(ctx context.Context, t *testing.T, ctr *k3s.K3sContainer) (*rest.Config, *kubernetes.Clientset) {
	t.Helper()
	kubeconfig, err := ctr.GetKubeConfig(ctx)
	if err != nil {
		t.Fatalf("get kubeconfig: %v", err)
	}
	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		t.Fatalf("parse kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("build clientset: %v", err)
	}
	return restCfg, clientset
}

func deployEcho(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "echo", Namespace: ns, Labels: map[string]string{"app": "echo"}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:            "echo",
				Image:           echoImage,
				ImagePullPolicy: corev1.PullNever,
				Ports:           []corev1.ContainerPort{{ContainerPort: 8080}},
			}},
		},
	}
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create echo pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "echo", Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "echo"},
			Ports:    []corev1.ServicePort{{Port: 8080, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	if _, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create echo service: %v", err)
	}
	waitPodReady(ctx, t, cs, ns, "echo")
}

func waitPodReady(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, name string) {
	t.Helper()
	const stableForPolls = 5
	deadline := time.Now().Add(3 * time.Minute)
	stable := 0
	for {
		pod, err := cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		ready := false
		if err == nil {
			for _, c := range pod.Status.Conditions {
				if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
					ready = true
				}
			}
		}
		if ready {
			stable++
			if stable >= stableForPolls {
				for _, cs := range pod.Status.ContainerStatuses {
					t.Logf("pod %s container %s ready=%v restarts=%d state=%+v",
						name, cs.Name, cs.Ready, cs.RestartCount, cs.State)
				}
				return
			}
		} else {
			stable = 0
		}
		if time.Now().After(deadline) {
			t.Fatalf("pod %s/%s not stably ready in time (last err: %v)", ns, name, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func execInContainer(ctx context.Context, t *testing.T, pe *podExec, ns, pod, container, script string) string {
	t.Helper()
	out, err := pe.script(ctx, ns, pod, container, script)
	if err != nil {
		return fmt.Sprintf("%s\n[exec error: %v]", out, err)
	}
	return out
}

func dumpPodDiagnostics(t *testing.T, cs *kubernetes.Clientset, ns, pod string) {
	dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dcancel()
	if p, err := cs.CoreV1().Pods(ns).Get(dctx, pod, metav1.GetOptions{}); err == nil {
		for _, cs := range p.Status.ContainerStatuses {
			t.Logf("[diag] container %s ready=%v restarts=%d state=%+v last=%+v",
				cs.Name, cs.Ready, cs.RestartCount, cs.State, cs.LastTerminationState)
		}
	}
	if evs, err := cs.CoreV1().Events(ns).List(dctx, metav1.ListOptions{}); err == nil {
		for _, ev := range evs.Items {
			t.Logf("[diag] event %s %s/%s: %s %s", ev.Type, ev.InvolvedObject.Kind, ev.InvolvedObject.Name, ev.Reason, ev.Message)
		}
	}
}

func dumpK3sLogs(t *testing.T, ctr *k3s.K3sContainer) {
	out, err := exec.Command("docker", "logs", "--tail", "2000", ctr.GetContainerID()).CombinedOutput()
	if err != nil {
		t.Logf("[diag] docker logs: %v", err)
		return
	}
	for _, line := range strings.Split(string(out), "\n") {
		l := strings.ToLower(line)
		if strings.Contains(l, "sandbox") || strings.Contains(l, "kill") ||
			strings.Contains(l, "oom") || strings.Contains(l, "evict") {
			t.Logf("[k3s] %s", line)
		}
	}
}

func installAgentSandboxCRD(ctx context.Context, t *testing.T, cfg *rest.Config, name string) {
	t.Helper()
	crd := agentSandboxCRD(t, name)
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Name != "v1beta1" {
		t.Fatalf("CRD %q: want exactly one version v1beta1, got %d versions", crd.Name, len(crd.Spec.Versions))
	}
	if crd.Spec.Conversion != nil && crd.Spec.Conversion.Strategy != apiextv1.NoneConverter {
		t.Fatalf("CRD %q: unexpected conversion strategy %q, no webhook runs in this test",
			crd.Name, crd.Spec.Conversion.Strategy)
	}

	cs, err := apiextclient.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("apiextensions client: %v", err)
	}
	if _, err := cs.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, &crd, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create CRD %q: %v", crd.Name, err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		got, gerr := cs.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if gerr == nil {
			for _, c := range got.Status.Conditions {
				if c.Type == apiextv1.Established && c.Status == apiextv1.ConditionTrue {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("CRD %q never became Established (last err: %v)", crd.Name, gerr)
		}
		time.Sleep(time.Second)
	}
}

func agentSandboxCRD(t *testing.T, name string) apiextv1.CustomResourceDefinition {
	t.Helper()
	path := filepath.Join(repoRoot(t), "..", "deploy", "agent-sandbox.yaml")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	r := utilyaml.NewYAMLReader(bufio.NewReader(f))
	for {
		doc, err := r.Read()
		if errors.Is(err, io.EOF) {
			t.Fatalf("CRD %q not found in %s", name, path)
		}
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var meta metav1.PartialObjectMetadata
		if err := yaml.Unmarshal(doc, &meta); err != nil {
			t.Fatalf("unmarshal document in %s: %v", path, err)
		}
		if meta.Kind != "CustomResourceDefinition" || meta.Name != name {
			continue
		}
		var crd apiextv1.CustomResourceDefinition
		if err := yaml.Unmarshal(doc, &crd); err != nil {
			t.Fatalf("unmarshal CRD %q: %v", name, err)
		}
		return crd
	}
}

type lineLogger struct {
	t      *testing.T
	prefix string
	mu     sync.Mutex
	buf    []byte
	done   bool
}

func newLineLogger(t *testing.T, prefix string) *lineLogger {
	w := &lineLogger{t: t, prefix: prefix}
	t.Cleanup(func() {
		w.mu.Lock()
		w.done = true
		w.mu.Unlock()
	})
	return w
}

func (w *lineLogger) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return len(p), nil
	}
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(w.buf[:i]), "\r")
		w.buf = w.buf[i+1:]
		if line != "" {
			w.t.Logf("%s %s", w.prefix, line)
		}
	}
	return len(p), nil
}
