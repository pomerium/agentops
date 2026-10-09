//go:build e2e

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	pomeriumImageEnv = "POMERIUM_IMAGE"

	defaultPomeriumImage = "pomerium/pomerium:experimental-agentic@sha256:a96244889ef0a008b03bb10a3aa85fe9f17416601f6102d97d70dda93abe0ce2"

	runIDNamespace = "agentops-e2e"
	curlImage      = "curlimages/curl:8.10.1"
	busyboxImage   = "busybox:1.36"

	agenticAudience = "pomerium-agentic-as"

	k8sIssuer = "https://kubernetes.default.svc.cluster.local"
)

var statusRe = regexp.MustCompile(`STATUS:(\d+)`)

func TestWorkloadIdentityK3s(t *testing.T) {
	if os.Getenv(optInEnv) == "" {
		t.Skipf("opt-in e2e test; set %s=1 to run", optInEnv)
	}
	pomeriumImage := os.Getenv(pomeriumImageEnv)
	if pomeriumImage == "" {
		pomeriumImage = defaultPomeriumImage
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	ctr := startK3sRunIdentity(ctx, t)
	restCfg, cs := k8sClients(ctx, t, ctr)

	ns := runIDNamespace
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		dumpK3sLogs(t, ctr)
		dumpPodDiagnostics(t, cs, ns, "pomerium")
		dumpPodDiagnostics(t, cs, ns, "executor-a")
		dumpPomeriumLogs(t, cs, ns)
	})

	pulls := []string{echoImage, curlImage, busyboxImage}
	if pomeriumImage == defaultPomeriumImage {
		pulls = append(pulls, pomeriumImage)
	}
	for _, img := range pulls {
		dockerPull(t, img)
	}

	if pomeriumImage == defaultPomeriumImage {
		const local = "agentops-e2e/pomerium:pinned"
		if out, err := exec.Command("docker", "tag", pomeriumImage, local).CombinedOutput(); err != nil {
			t.Fatalf("docker tag %s %s: %v\n%s", pomeriumImage, local, err, out)
		}
		pomeriumImage = local
	}
	if err := ctr.LoadImages(ctx, pomeriumImage, echoImage, curlImage, busyboxImage); err != nil {
		t.Fatalf("load images into k3s: %v", err)
	}

	createNamespace(ctx, t, cs, ns)

	for _, sa := range []string{"harness", "executor", "pomerium"} {
		createServiceAccount(ctx, t, cs, ns, sa)
	}

	bindIssuerDiscovery(ctx, t, cs, ns)

	deployEcho(ctx, t, cs, ns)

	certPEM, keyPEM := generateWildcardCert(t)
	createSecret(ctx, t, cs, ns, "pomerium-tls", map[string][]byte{
		"tls.crt": certPEM,
		"tls.key": keyPEM,
	})

	createConfigMap(ctx, t, cs, ns, "pomerium-config", map[string]string{
		"config.yaml": pomeriumConfigYAML(ns, randomSecret(t), randomSecret(t)),
	})

	deployPomerium(ctx, t, cs, ns, pomeriumImage)

	deployExecutorPod(ctx, t, cs, ns, "executor-a")

	pe := newPodExec(t, restCfg)

	svcHost := fmt.Sprintf("pomerium.%s.svc.cluster.local", ns)
	curlSetup := fmt.Sprintf("CURL='curl -sk "+
		"--connect-to echo.localhost.pomerium.io:443:%s:443 "+
		"--connect-to other.localhost.pomerium.io:443:%s:443'\n", svcHost, svcHost)

	waitPomeriumServing(ctx, t, pe, ns, "executor-a", curlSetup)

	out := execInContainer(ctx, t, pe, ns, "executor-a", "sidecar", curlSetup+`
code=$($CURL -o /tmp/body.txt -w '%{http_code}' \
  -H "Authorization: Bearer $(cat /var/run/agentic/token)" \
  https://echo.localhost.pomerium.io/echo)
echo "STATUS:$code"
cat /tmp/body.txt`)
	requireStatus(t, out, "200", "workload access with its own projected token")
	if !strings.Contains(out, "/echo") {
		t.Errorf("workload access: upstream echo body not observed:\n%s", out)
	}

	out = execInContainer(ctx, t, pe, ns, "executor-a", "sidecar", curlSetup+`
code=$($CURL -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $(cat /var/run/agentic/token)" \
  https://other.localhost.pomerium.io/echo)
echo "STATUS:$code"`)
	expectStatus(t, out, "403", "a route that admits only another service account, with this pod's projected token")

	harnessToken := mintSAToken(ctx, t, cs, ns, "harness", []string{agenticAudience})
	out = execInContainer(ctx, t, pe, ns, "executor-a", "sidecar", curlSetup+
		"BEARER='"+harnessToken+"'\n"+`
code=$($CURL -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $BEARER" \
  https://echo.localhost.pomerium.io/echo)
echo "STATUS:$code"`)
	expectStatus(t, out, "403", "the echo route, with the harness service account's token")

	wrongAudToken := mintSAToken(ctx, t, cs, ns, "executor", []string{k8sIssuer})
	out = execInContainer(ctx, t, pe, ns, "executor-a", "sidecar", curlSetup+
		"BEARER='"+wrongAudToken+"'\n"+`
code=$($CURL -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $BEARER" \
  https://echo.localhost.pomerium.io/echo)
echo "STATUS:$code"`)
	expectStatus(t, out, "403", "the echo route, with an executor token minted for the apiserver audience")

	out = execInContainer(ctx, t, pe, ns, "executor-a", "sidecar", curlSetup+`
code=$($CURL -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer not-a-jwt" \
  https://echo.localhost.pomerium.io/echo)
echo "STATUS:$code"`)
	expectStatus(t, out, "403", "the echo route, with a bearer that is not a JWT")

	out = execInContainer(ctx, t, pe, ns, "executor-a", "agent", `
if [ ! -e /var/run/secrets/kubernetes.io/serviceaccount/token ] && [ ! -e /var/run/agentic/token ]; then
  echo NO_TOKEN
else
  echo HAS_TOKEN
  ls -la /var/run/secrets/kubernetes.io/serviceaccount/ /var/run/agentic/ 2>&1 || true
fi`)
	if !strings.Contains(out, "NO_TOKEN") {
		t.Errorf("the agent container must mount no service account token, neither the default one nor the projected agentic one:\n%s", out)
	}
}

func requireStatus(t *testing.T, out, want, what string) {
	t.Helper()
	if got := parseStatus(t, out); got != want {
		t.Fatalf("%s: expected HTTP %s, got %s:\n%s", what, want, got, out)
	}
}

func expectStatus(t *testing.T, out, want, what string) {
	t.Helper()
	if got := parseStatus(t, out); got != want {
		t.Errorf("%s: expected HTTP %s, got %s:\n%s", what, want, got, out)
	}
}

func parseStatus(t *testing.T, out string) string {
	t.Helper()
	m := statusRe.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no STATUS marker in exec output:\n%s", out)
	}
	return m[1]
}

func dockerPull(t *testing.T, image string) {
	t.Helper()
	cmd := exec.Command("docker", "pull", image)
	cmd.Stdout = newLineLogger(t, "[pull]")
	cmd.Stderr = cmd.Stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("pull %s: %v", image, err)
	}
}

func createNamespace(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns string) {
	t.Helper()
	obj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, obj, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", ns, err)
	}
}

func startK3sRunIdentity(ctx context.Context, t *testing.T) *k3s.K3sContainer {
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

func bindIssuerDiscovery(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns string) {
	t.Helper()
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "agentops-e2e-issuer-discovery"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "system:service-account-issuer-discovery",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      "pomerium",
			Namespace: ns,
		}},
	}
	if _, err := cs.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("bind issuer-discovery role: %v", err)
	}
}

func createServiceAccount(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, name string) {
	t.Helper()
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	if _, err := cs.CoreV1().ServiceAccounts(ns).Create(ctx, sa, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create serviceaccount %s: %v", name, err)
	}
}

func createSecret(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, name string, data map[string][]byte) {
	t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Data: data}
	if _, err := cs.CoreV1().Secrets(ns).Create(ctx, s, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create secret %s: %v", name, err)
	}
}

func createConfigMap(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, name string, data map[string]string) {
	t.Helper()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Data: data}
	if _, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create configmap %s: %v", name, err)
	}
}

func mintSAToken(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, sa string, audiences []string) string {
	t.Helper()
	tr, err := cs.CoreV1().ServiceAccounts(ns).CreateToken(ctx, sa, &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{Audiences: audiences},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("mint SA token for %s (aud=%v): %v", sa, audiences, err)
	}
	return tr.Status.Token
}

func deployPomerium(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, image string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pomerium", Namespace: ns, Labels: map[string]string{"app": "pomerium"}},
		Spec: corev1.PodSpec{
			ServiceAccountName: "pomerium",
			Containers: []corev1.Container{{
				Name:            "pomerium",
				Image:           image,
				ImagePullPolicy: corev1.PullNever,
				Args:            []string{"--config", "/pomerium/config.yaml"},
				Ports:           []corev1.ContainerPort{{ContainerPort: 8443}},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "config", MountPath: "/pomerium/config.yaml", SubPath: "config.yaml", ReadOnly: true},
					{Name: "tls", MountPath: "/pomerium/tls", ReadOnly: true},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "pomerium-config"}},
				}},
				{Name: "tls", VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: "pomerium-tls"},
				}},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pomerium pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "pomerium", Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "pomerium"},
			Ports:    []corev1.ServicePort{{Port: 443, TargetPort: intstr.FromInt32(8443)}},
		},
	}
	if _, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pomerium service: %v", err)
	}
	waitPodReady(ctx, t, cs, ns, "pomerium")
}

func deployExecutorPod(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, ns, name string) string {
	t.Helper()
	off := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app": name}},
		Spec: corev1.PodSpec{
			ServiceAccountName:           "executor",
			AutomountServiceAccountToken: &off,
			Containers: []corev1.Container{
				{
					Name:            "sidecar",
					Image:           curlImage,
					ImagePullPolicy: corev1.PullNever,
					Command:         []string{"sh", "-c", "sleep 36000"},
					VolumeMounts:    []corev1.VolumeMount{agenticTokenMount()},
				},
				{
					Name:            "agent",
					Image:           busyboxImage,
					ImagePullPolicy: corev1.PullNever,
					Command:         []string{"sh", "-c", "sleep 36000"},
				},
			},
			Volumes: []corev1.Volume{agenticTokenVolume()},
		},
	}
	created, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		t.Fatalf("create executor pod %s: %v", name, err)
	}
	waitPodReady(ctx, t, cs, ns, name)
	return string(created.UID)
}

func agenticTokenVolume() corev1.Volume {
	expiry := int64(600)
	return corev1.Volume{
		Name: "agentic-token",
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
						Audience:          agenticAudience,
						ExpirationSeconds: &expiry,
						Path:              "token",
					},
				}},
			},
		},
	}
}

func agenticTokenMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: "agentic-token", MountPath: "/var/run/agentic", ReadOnly: true}
}

func waitPomeriumServing(ctx context.Context, t *testing.T, pe *podExec, ns, pod, curlSetup string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var last string
	for {
		last = execInContainer(ctx, t, pe, ns, pod, "sidecar", curlSetup+`
code=$($CURL -o /dev/null -w '%{http_code}' https://echo.localhost.pomerium.io/healthz || true)
[ -z "$code" ] && code=000
echo "STATUS:$code"`)
		if code := statusRe.FindStringSubmatch(last); code != nil && code[1] != "000" {
			t.Logf("pomerium serving (healthz → %s)", code[1])
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pomerium not serving within 2m; last probe:\n%s", last)
		}
		time.Sleep(2 * time.Second)
	}
}

func pomeriumConfigYAML(ns, sharedSecret, cookieSecret string) string {
	return fmt.Sprintf(`address: ":8443"
grpc_insecure: true
shared_secret: "%s"
cookie_secret: "%s"
certificate_file: /pomerium/tls/tls.crt
certificate_key_file: /pomerium/tls/tls.key
authenticate_service_url: https://authenticate.pomerium.app
certificate_authority_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
databroker_storage_type: memory
identity_providers:
  cluster:
    issuer: kubernetes:///
    audiences: ["%s"]
    supported_algs: ["RS256"]
routes:
  - from: https://echo.localhost.pomerium.io
    to: http://echo.%s.svc.cluster.local:8080
    bearer_token_format: jwt
    identity_providers: [cluster]
    policy:
      allow:
        and:
          - claim/sub: system:serviceaccount:%s:executor
          - claim/kubernetes.io.namespace: %s
          - claim/kubernetes.io.serviceaccount.name: executor
  - from: https://other.localhost.pomerium.io
    to: http://echo.%s.svc.cluster.local:8080
    bearer_token_format: jwt
    identity_providers: [cluster]
    policy:
      allow:
        and:
          - claim/kubernetes.io.namespace: %s
          - claim/kubernetes.io.serviceaccount.name: some-other-workload
`, sharedSecret, cookieSecret, agenticAudience, ns, ns, ns, ns, ns)
}

func generateWildcardCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "*.localhost.pomerium.io"},
		DNSNames:              []string{"*.localhost.pomerium.io", "localhost.pomerium.io"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func randomSecret(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("read random: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func dumpPomeriumLogs(t *testing.T, cs *kubernetes.Clientset, ns string) {
	dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tail := int64(200)
	raw, err := cs.CoreV1().Pods(ns).GetLogs("pomerium", &corev1.PodLogOptions{
		Container: "pomerium",
		TailLines: &tail,
	}).DoRaw(dctx)
	if err != nil {
		t.Logf("[diag] pomerium logs: %v", err)
		return
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if line != "" {
			t.Logf("[pomerium] %s", line)
		}
	}
}
