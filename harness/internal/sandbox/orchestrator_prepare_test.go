package sandbox

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakePodGetter struct {
	mu   sync.Mutex
	pods []*corev1.Pod
	n    int
}

func (f *fakePodGetter) Get(_ context.Context, name string) (*corev1.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.pods[f.n].DeepCopy()
	if f.n < len(f.pods)-1 {
		f.n++
	}
	p.Name = name
	return p, nil
}

func pod(ns, sa, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, UID: types.UID(uid)},
		Spec:       corev1.PodSpec{ServiceAccountName: sa},
	}
}

func TestPrepare_ResolvesExecutorIdentityFromPod(t *testing.T) {
	claims := newFakeClaims()
	pods := &fakePodGetter{pods: []*corev1.Pod{pod("agentops", "sandbox-agent", "uid-A")}}
	o := New(claims, pods, nil, newFakeAgentLink(), WithNamespace("agentops"), WithHarnessRoute("https://harness.example.com"))

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	seal := prepared.Executor.Seal()
	if seal["kubernetes.io.namespace"] != "agentops" ||
		seal["kubernetes.io.serviceaccount.name"] != "sandbox-agent" ||
		seal["kubernetes.io.pod.uid"] != "uid-A" {
		t.Errorf("sealed identity = %v", seal)
	}
	if prepared.Executor.PodName != prepared.SandboxName {
		t.Errorf("sealed pod name %q != sandbox name %q", prepared.Executor.PodName, prepared.SandboxName)
	}
}

func TestPrepare_TearsDownOnIncompleteIdentity(t *testing.T) {
	claims := newFakeClaims()
	pods := &fakePodGetter{pods: []*corev1.Pod{pod("agentops", "", "uid-A")}}
	o := New(claims, pods, nil, newFakeAgentLink(), WithNamespace("agentops"), WithHarnessRoute("https://harness.example.com"))

	_, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err == nil {
		t.Fatal("expected Prepare to fail on incomplete identity")
	}
	claims.mu.Lock()
	deleted := len(claims.deleted)
	claims.mu.Unlock()
	if deleted != 1 {
		t.Errorf("claim deleted %d times, want 1 (teardown on validate failure)", deleted)
	}
}

func TestActivate_FailsFastOnPodReplacement(t *testing.T) {
	claims := newFakeClaims()
	link := newFakeAgentLink()
	pods := &fakePodGetter{pods: []*corev1.Pod{
		pod("agentops", "sandbox-agent", "uid-A"),
		pod("agentops", "sandbox-agent", "uid-B"),
	}}
	o := New(claims, pods, nil, link, WithNamespace("agentops"), WithHarnessRoute("https://harness.example.com"))

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	att, err := o.Expect("run-1", prepared)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	if _, err := o.Activate(context.Background(), nil, prepared, att); err == nil {
		t.Fatal("expected Activate to fail fast on pod replacement")
	}
	if link.live() != 0 {
		t.Errorf("the expectation leaked on the mismatch path: %d registered", link.live())
	}
	if deleted := claims.deletedNames(); len(deleted) != 1 {
		t.Errorf("claim deleted %d times, want 1 (teardown on mismatch)", len(deleted))
	}
}

func TestPrepareActivate_OpensSessionWhenUIDStable(t *testing.T) {
	claims := newFakeClaims()
	link := newFakeAgentLink()
	pods := &fakePodGetter{pods: []*corev1.Pod{pod("agentops", "sandbox-agent", "uid-A")}}

	o := New(claims, pods, nil, link, WithNamespace("agentops"), WithHarnessRoute("https://harness.example.com"))
	o.openSession = stubOpenSession

	prepared, err := o.Prepare(context.Background(), LaunchSpec{SessionID: "s1", Template: testTemplate()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	att, err := o.Expect("run-1", prepared)
	if err != nil {
		t.Fatalf("Expect: %v", err)
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		link.handle("run-1").attach(1, false)
	}()

	sess, err := o.Activate(context.Background(), nil, prepared, att)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sess.Close() }()
}
