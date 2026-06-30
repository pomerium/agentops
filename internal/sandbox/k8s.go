package sandbox

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	extv1 "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned/typed/api/v1beta1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/pomerium/agentops/internal/telemetry"
)

// clientsetClaimClient implements ClaimClient over the generated agent-sandbox
// extensions clientset.
type clientsetClaimClient struct {
	iface extv1.SandboxClaimInterface
}

// NewClaimClient builds a ClaimClient for SandboxClaims in namespace using the
// supplied in-cluster (or kubeconfig) REST config.
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

// spdyPodExecutor implements PodExecutor using the Kubernetes pod exec
// subresource over a SPDY-upgraded connection, exposing the agent's stdio as
// io pipes.
type spdyPodExecutor struct {
	config    *rest.Config
	clientset kubernetes.Interface
	tel       *telemetry.Component
}

// NewPodExecutor builds a PodExecutor from a REST config. log may be nil.
func NewPodExecutor(cfg *rest.Config, log *slog.Logger) (PodExecutor, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &spdyPodExecutor{
		config: cfg, clientset: cs,
		tel: telemetry.New(log, "sandbox.exec", slog.LevelDebug),
	}, nil
}

// lineLogWriter forwards the agent process's stderr into structured debug logs,
// one line per record. This is where the harness (Claude Code / claude-agent-acp)
// reports MCP server connection errors, tool failures, etc. — the closest thing
// to a "harness-side MCP health" view.
type lineLogWriter struct {
	tel *telemetry.Component
	pod string
	buf bytes.Buffer
}

func (w *lineLogWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil { // no full line yet; keep the remainder
			w.buf.WriteString(line)
			break
		}
		if t := strings.TrimRight(line, "\r\n"); t != "" {
			w.tel.Debug(context.Background(), "agent stderr", "pod", w.pod, "line", t)
		}
	}
	return len(p), nil
}

func (e *spdyPodExecutor) OpenAgent(ctx context.Context, namespace, pod, container string, command []string) (*AgentConn, error) {
	req := e.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return nil, err
	}

	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()

	// The session outlives the Launch call's context, so the stream runs under
	// its own cancelable context, released by CloseFn.
	streamCtx, cancel := context.WithCancel(context.Background())
	e.tel.Debug(ctx, "opening exec stream", "namespace", namespace, "pod", pod, "command", command)
	go func() {
		err := executor.StreamWithContext(streamCtx, remotecommand.StreamOptions{
			Stdin:  stdinR,
			Stdout: stdoutW,
			Stderr: &lineLogWriter{tel: e.tel, pod: pod},
		})
		// Previously swallowed: a stream error here closes the agent's stdout
		// (so ACP reads hit EOF) — surface it instead of failing silently.
		if err != nil && streamCtx.Err() == nil {
			e.tel.Warn(streamCtx, "exec stream ended with error", "pod", pod, "err", err)
		} else {
			e.tel.Debug(streamCtx, "exec stream closed", "pod", pod)
		}
		_ = stdoutW.CloseWithError(err)
		_ = stdinR.CloseWithError(err)
	}()

	closeFn := func() error {
		cancel()
		_ = stdinW.Close()
		_ = stdoutR.Close()
		return nil
	}
	return &AgentConn{Stdin: stdinW, Stdout: stdoutR, CloseFn: closeFn}, nil
}
