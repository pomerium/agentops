//go:build e2e

// End-to-end test for the secret-isolating sidecar on a real Kubernetes
// cluster (k3s via testcontainers).
//
// It drives the production launch path end-to-end: the real
// Orchestrator.Launch with its default configuration (container names,
// commands), the real SPDY pod-exec transport, the gRPC-over-stdio sidecar
// configuration, envoy, and a real ACP agent (the no-LLM demo harness). Only
// the SandboxClaim controller is faked — claim→pod materialization is the
// agent-sandbox project's responsibility.
//
// What it proves:
//
//  1. Launch execs `sidecar serve` in the sidecar container, configures it
//     over gRPC-on-stdio (merging env-defined SIDECAR_HTTP_* endpoints with
//     stream-delivered MCP endpoints), waits for envoy READY, then execs the
//     agent and opens an ACP session — all with default Config.
//  2. Requests from the agent container to the sidecar's 127.0.0.1 listeners
//     reach the upstream with the secret headers injected — overwriting any
//     value the agent tried to smuggle in.
//  3. The agent container's environment contains no secrets.
//  4. Closing the session shuts the sidecar (and its listeners) down.
//
// Run it:
//
//	AGENTOPS_E2E=1 go test -tags e2e -timeout 15m ./internal/e2e/... -run SidecarK3s -v
package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"

	v1alpha1 "github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/sandbox"
)

const (
	k3sImage       = "rancher/k3s:v1.33.4-k3s1"
	echoImage      = "mendhak/http-https-echo:36"
	sidecarImage   = "agentops-sidecar:dev"
	demoAgentImage = "agentops-agent-demo:e2e"

	sandboxPodName = "sandbox-e2e"

	// dummyAnthropicKey stands in for the real Anthropic API key mounted into
	// the sidecar container; the test asserts it never shows up in the agent
	// container while reaching the upstream as an x-api-key header.
	dummyAnthropicKey = "sk-ant-e2e-dummy-key"
	// mcpBearerToken is the per-user MCP credential delivered over the gRPC
	// control stream, as production does for each required MCP server.
	mcpBearerToken = "Bearer e2e-mcp-secret-token"
)

func TestSidecarK3sIsolatesSecrets(t *testing.T) {
	if os.Getenv(optInEnv) == "" {
		t.Skipf("opt-in e2e test; set %s=1 to run", optInEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()

	buildImage(t, sidecarImage, "Dockerfile.sidecar", ".")
	buildImage(t, demoAgentImage, "deploy/harness/demo/Dockerfile", "deploy/harness")

	k3sCtr := startK3s(ctx, t)
	restCfg, clientset := k8sClients(ctx, t, k3sCtr)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		dumpK3sLogs(t, k3sCtr)
	})

	const ns = "default"
	deployEcho(ctx, t, clientset, ns)
	pod := deploySandboxPod(ctx, t, clientset, ns)
	t.Cleanup(func() {
		if t.Failed() {
			dumpPodDiagnostics(t, clientset, ns, pod)
		}
	})

	// Debug-level logger so the exec'd processes' stderr (where the sidecar
	// reports envoy/config errors) lands in the test log.
	log := slog.New(slog.NewTextHandler(&lineLogger{t: t, prefix: "[exec]"}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	podExec, err := sandbox.NewPodExecutor(restCfg, log)
	if err != nil {
		t.Fatalf("NewPodExecutor: %v", err)
	}

	// The real orchestrator with DEFAULT config (container names, exec
	// commands) — the exact production path; only the claim controller is
	// faked. A custom AgentContainer/SidecarContainer here would have masked
	// the "container name must be specified for multi-container pods" bug.
	orch := sandbox.New(&fakeClaimClient{podName: pod}, podExec, sandbox.Config{
		Namespace: ns,
	}, log)

	endpoints := []sandbox.ProxiedEndpoint{{
		Name:        "echo-mcp",
		ListenPort:  sandbox.MCPListenPort(0),
		UpstreamURL: fmt.Sprintf("http://echo.%s.svc.cluster.local:8080/mcp", ns),
		Headers:     map[string]string{"authorization": mcpBearerToken},
	}}
	spec := sandbox.LaunchSpec{
		SessionID: "sidecar-e2e",
		Workflow: &v1alpha1.AgentTemplate{
			Spec: v1alpha1.AgentTemplateSpec{
				SandboxTemplateRef: v1alpha1.SandboxTemplateReference{Name: "claude-code"},
			},
		},
	}

	// Launch failures right after pod readiness can be transient (the kubelet
	// may briefly answer exec with "container not found" on fresh k3s nodes),
	// so retry the whole launch.
	var sess *sandbox.Session
	sink := newRecordingSink(t)
	for attempt := 0; ; attempt++ {
		sess, _, err = orch.Launch(ctx, sink, spec, endpoints)
		if err == nil {
			break
		}
		if attempt >= 5 {
			t.Fatalf("orchestrator launch: %v", err)
		}
		t.Logf("launch attempt %d failed (retrying): %v", attempt, err)
		time.Sleep(3 * time.Second)
	}
	t.Logf("ACP session live: %s", sess.ID())

	// --- the admin interface must NOT be reachable from the agent ----------
	// Envoy binds admin on a unix socket on the sidecar's own filesystem, not a
	// loopback TCP port. The agent shares only the pod network namespace, so a
	// TCP admin would let it read injected secret headers via /config_dump.
	// curl returns a non-zero exit (connection refused) and never the config.
	adminProbe := agentRun(ctx, t, podExec, ns, pod,
		`curl -s -m 5 http://127.0.0.1:9901/config_dump; echo "exit=$?"`)
	t.Logf("agent admin probe: %s", adminProbe)
	if strings.Contains(adminProbe, "exit=0") {
		t.Errorf("envoy admin is reachable from the agent container on 127.0.0.1:9901:\n%s", adminProbe)
	}

	// --- requests via the loopback listeners carry injected secrets --------
	// The agent tries to smuggle its own authorization header; envoy must
	// overwrite it with the real credential.
	echoResp := agentRun(ctx, t, podExec, ns, pod,
		fmt.Sprintf(`curl -s -m 10 -H "authorization: agent-forged-value" -w "\nHTTP_CODE=%%{http_code}\n" http://127.0.0.1:%d/probe; echo "exit=$?"`, sandbox.MCPListenPort(0)))
	t.Logf("echo response: %s", echoResp)
	if !strings.Contains(echoResp, mcpBearerToken) {
		t.Errorf("echo upstream did not see the injected MCP bearer token:\n%s", echoResp)
	}
	if strings.Contains(echoResp, "agent-forged-value") {
		t.Errorf("agent-supplied authorization header reached the upstream (must be overwritten):\n%s", echoResp)
	}
	if !strings.Contains(echoResp, "/probe") {
		t.Errorf("request path not forwarded:\n%s", echoResp)
	}

	// The env-defined endpoint (SIDECAR_HTTP_ANTHROPIC_*) injects the API key.
	anthroResp := agentRun(ctx, t, podExec, ns, pod, `curl -s -m 10 http://127.0.0.1:9999/v1/messages`)
	if !strings.Contains(anthroResp, dummyAnthropicKey) {
		t.Errorf("echo upstream did not see the injected x-api-key:\n%s", anthroResp)
	}

	// --- no secrets anywhere in the agent container -------------------------
	agentEnv := agentRun(ctx, t, podExec, ns, pod, "env")
	for _, secret := range []string{dummyAnthropicKey, "e2e-mcp-secret-token"} {
		if strings.Contains(agentEnv, secret) {
			t.Errorf("agent container env leaks secret %q:\n%s", secret, agentEnv)
		}
	}
	if !strings.Contains(agentEnv, "ANTHROPIC_BASE_URL=http://127.0.0.1:9999") {
		t.Errorf("agent env missing ANTHROPIC_BASE_URL:\n%s", agentEnv)
	}

	// Closing the session must shut the sidecar down with it (the control
	// stream closes → sidecar kills envoy → listeners go dark).
	if err := sess.Close(); err != nil {
		t.Logf("session close: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		out := agentRun(ctx, t, podExec, ns, pod, fmt.Sprintf(
			`curl -s -o /dev/null -m 2 -w "%%{http_code}" http://127.0.0.1:%d/probe || true`, sandbox.MCPListenPort(0)))
		if !strings.Contains(out, "200") {
			break
		}
		if time.Now().After(deadline) {
			t.Error("sidecar listener still serving 30s after the session closed")
			break
		}
		time.Sleep(time.Second)
	}
}

// fakeClaimClient satisfies sandbox.ClaimClient: the pod is created directly
// by the test, so every claim is immediately "ready" and points at it.
type fakeClaimClient struct {
	podName string
}

func (f *fakeClaimClient) ready(claim *sbxv1.SandboxClaim) *sbxv1.SandboxClaim {
	c := claim.DeepCopy()
	c.Status.SandboxStatus.Name = f.podName
	c.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: "True"}}
	return c
}

func (f *fakeClaimClient) Create(_ context.Context, claim *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error) {
	return f.ready(claim), nil
}

func (f *fakeClaimClient) Get(_ context.Context, name string) (*sbxv1.SandboxClaim, error) {
	return f.ready(&sbxv1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}), nil
}

func (f *fakeClaimClient) Delete(context.Context, string) error { return nil }

// buildImage builds a local image from the repo, mirroring the Makefile
// targets.
func buildImage(t *testing.T, tag, dockerfile, buildContext string) {
	t.Helper()
	root := repoRoot(t)
	cmd := exec.Command("docker", "build",
		"-f", filepath.Join(root, dockerfile),
		"-t", tag,
		filepath.Join(root, buildContext))
	cmd.Stdout = &lineLogger{t: t, prefix: "[build " + tag + "]"}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("build %s: %v", tag, err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

// startK3s runs the k3s container and imports the images the sandbox pod
// needs (k3s has no access to the host Docker daemon's image store).
func startK3s(ctx context.Context, t *testing.T) *k3s.K3sContainer {
	t.Helper()
	// Replace the module's host config: it sets CgroupnsMode "host", under
	// which (at least on OrbStack/cgroup-v2 hosts) the kubelet treats every
	// pod cgroup as orphaned and SIGKILLs its processes every sync — pods
	// crash-loop with exit 137. The k3s image handles private cgroup
	// namespaces itself (cgroup evacuation), so run it the documented
	// k3s-in-docker way: privileged + private cgroupns.
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

	cmd := exec.Command("docker", "pull", echoImage)
	cmd.Stdout = &lineLogger{t: t, prefix: "[pull]"}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("pull %s: %v", echoImage, err)
	}
	if err := ctr.LoadImages(ctx, sidecarImage, demoAgentImage, echoImage); err != nil {
		t.Fatalf("load images into k3s: %v", err)
	}
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

// deployEcho runs a header-echoing HTTP upstream plus a Service in front of
// it; the sidecar proxies to it by service DNS name.
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

// deploySandboxPod creates the two-container sandbox pod exactly as the
// claude-code SandboxTemplate renders it: the demo ACP agent first
// (env-injection invariant), sidecar second with the template-level secret
// env.
func deploySandboxPod(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns string) string {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxPodName, Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            "agent",
					Image:           demoAgentImage,
					ImagePullPolicy: corev1.PullNever,
					Env: []corev1.EnvVar{
						{Name: "ANTHROPIC_BASE_URL", Value: "http://127.0.0.1:9999"},
					},
				},
				{
					Name:            "sidecar",
					Image:           sidecarImage,
					ImagePullPolicy: corev1.PullNever,
					Env: []corev1.EnvVar{
						{Name: "SIDECAR_HTTP_ANTHROPIC_PORT", Value: "9999"},
						// Points at the echo upstream so the test can observe
						// the injected header; in production this is
						// https://api.anthropic.com.
						{Name: "SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL", Value: fmt.Sprintf("http://echo.%s.svc.cluster.local:8080", ns)},
						{Name: "SIDECAR_HTTP_ANTHROPIC_HEADER_X_API_KEY", Value: dummyAnthropicKey},
					},
				},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create sandbox pod: %v", err)
	}
	waitPodReady(ctx, t, cs, ns, sandboxPodName)
	return sandboxPodName
}

// waitPodReady waits until the pod reports Ready and stays Ready across
// several consecutive polls. Freshly-started k3s nodes are known to recreate
// pod sandboxes shortly after startup ("SandboxChanged" events kill and
// restart all containers), so a single Ready observation is not enough.
func waitPodReady(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, name string) {
	t.Helper()
	const stableForPolls = 5 // × 2s = 10s of sustained readiness
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

// agentRun execs a shell command in the agent container over the production
// SPDY executor and returns its stdout. Command failures are reported in the
// returned text (never fatal) so assertions and diagnostics stay in control.
func agentRun(ctx context.Context, t *testing.T, podExec sandbox.PodExecutor, ns, pod, script string) string {
	t.Helper()
	// Two SPDY exec quirks: closing stdin early can kill the stream, and a
	// process exiting immediately after writing can lose trailing stdout
	// (kubelet drain race) — hence stdin stays open and the trailing sleep.
	conn, err := podExec.OpenAgent(ctx, ns, pod, "agent", []string{"sh", "-c", script + "; sleep 1"})
	if err != nil {
		t.Fatalf("exec in agent container: %v", err)
	}
	defer func() { _ = conn.Close() }()
	out, err := io.ReadAll(conn.Stdout)
	if err != nil {
		return fmt.Sprintf("%s\n[exec error: %v]", out, err)
	}
	return string(out)
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
