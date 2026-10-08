package harnessapi_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	agentsv1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sbxv1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agentlink"
	"github.com/pomerium/agentops/harness/internal/agentlink/agentlinktest"
	"github.com/pomerium/agentops/harness/internal/harnessapi"
	"github.com/pomerium/agentops/harness/internal/runner"
	"github.com/pomerium/agentops/harness/internal/runner/runnertest"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore/sqlite"
	"github.com/pomerium/agentops/harness/internal/sidecar/harnessclient"

	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	runnertest.RunIfRequested()
	os.Exit(m.Run())
}

type readyClaims struct {
	mu     sync.Mutex
	claims map[string]*sbxv1.SandboxClaim
}

func (c *readyClaims) Create(_ context.Context, claim *sbxv1.SandboxClaim) (*sbxv1.SandboxClaim, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	claim = claim.DeepCopy()
	claim.Status.SandboxStatus.Name = "sandbox-pod"
	claim.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
	c.claims[claim.Name] = claim
	return claim, nil
}

func (c *readyClaims) Get(_ context.Context, name string) (*sbxv1.SandboxClaim, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if claim, ok := c.claims[name]; ok {
		return claim, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "sandboxclaims"}, name)
}

func (c *readyClaims) Delete(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.claims, name)
	return nil
}

type onePod struct{}

func (onePod) Get(_ context.Context, name string) (*corev1.Pod, error) {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "agentops", UID: types.UID("pod-uid-1")},
		Spec:       corev1.PodSpec{ServiceAccountName: "sandbox-agent"},
	}, nil
}

type noopSandboxes struct{}

func (noopSandboxes) Get(context.Context, string) (*agentsv1.Sandbox, error) { return nil, nil }
func (noopSandboxes) Patch(context.Context, string, []byte) error            { return nil }

var podSeal = agenticrun.Executor{Namespace: "agentops", ServiceAccount: "sandbox-agent", PodName: "sandbox-pod", PodUID: "pod-uid-1"}

type harnessSide struct {
	svc  *harnessapi.Service
	link *agentlink.Server
	stop func()
}

type slowLeaseSandboxes struct {
	noopSandboxes
	delay time.Duration
}

func (s slowLeaseSandboxes) Patch(context.Context, string, []byte) error {
	time.Sleep(s.delay)
	return nil
}

func startHarnessSide(t *testing.T, store *sqlite.Store, claims *readyClaims, runs *fakeRunClient, tmpl *fakeTemplates,
	idp *agentlinktest.IDP, addr string, orchOpts ...sandbox.Option,
) *harnessSide {
	t.Helper()
	return startHarnessSideWith(t, store, claims, runs, tmpl, idp, addr, noopSandboxes{}, orchOpts...)
}

func startHarnessSideWith(t *testing.T, store *sqlite.Store, claims *readyClaims, runs *fakeRunClient, tmpl *fakeTemplates,
	idp *agentlinktest.IDP, addr string, sandboxes sandbox.SandboxClient, orchOpts ...sandbox.Option,
) *harnessSide {
	t.Helper()
	link, err := agentlink.New(idp.Verifier(t),
		agentlink.WithHeartbeatInterval(time.Second), agentlink.WithLogger(testLogger(t)), agentlink.WithStartupHold())
	if err != nil {
		t.Fatalf("agentlink.New: %v", err)
	}
	orch := sandbox.New(claims, onePod{}, sandboxes, sandbox.NewAgentLink(link), append([]sandbox.Option{
		sandbox.WithNamespace("agentops"), sandbox.WithHarnessRoute("http://" + addr),
		sandbox.WithAttachGrace(30 * time.Second), sandbox.WithLogger(testLogger(t)),
	}, orchOpts...)...)
	svc := harnessapi.New(store, harnessapi.NewEventLog(store), harnessapi.NewOrchestratorLauncher(orch), tmpl, runs,
		harnessapi.WithLogger(testLogger(t)))
	var lis net.Listener
	deadline := time.Now().Add(10 * time.Second)
	for {
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("listen on %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop := agentlinktest.ServeOn(lis, link, func() string { return idp.SignFor(t, "run-1", podSeal) })
	<-svc.ReconcileOnStartup(context.Background())
	link.EndStartupHold()
	return &harnessSide{svc: svc, link: link, stop: stop}
}

func startPod(t *testing.T, addr string) *harnessclient.Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "rnr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "r.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	rsvc := runner.New(runner.WithCommand(runnertest.Command()), runner.WithKillDelay(time.Second), runner.WithLogger(testLogger(t)))
	gs := grpc.NewServer()
	rsvc.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		rsvc.Close()
	})
	uds, err := harnessclient.NewUDSRunner(socket, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(uds.Close)
	client, err := harnessclient.New(harnessclient.Config{
		URL: "http://" + addr, Insecure: true,
		Token:       staticBearer("Bearer pom_art_test"),
		Runner:      uds,
		BaseBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
		Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

type staticBearer string

func (b staticBearer) Bearer() string { return string(b) }
func (staticBearer) Refresh()         {}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type startSide func(t *testing.T, store *sqlite.Store, claims *readyClaims, runs *fakeRunClient, tmpl *fakeTemplates,
	idp *agentlinktest.IDP, addr string) *harnessSide

func TestAHarnessRestartKeepsTheTurnThatIsRunning(t *testing.T) {
	restartMidTurn(t, func(t *testing.T, store *sqlite.Store, claims *readyClaims, runs *fakeRunClient, tmpl *fakeTemplates,
		idp *agentlinktest.IDP, addr string,
	) *harnessSide {
		return startHarnessSide(t, store, claims, runs, tmpl, idp, addr)
	})
}

func TestARestartWhoseLeaseUpdateOutlastsTheAttachGraceKeepsTheSession(t *testing.T) {
	restartMidTurn(t, func(t *testing.T, store *sqlite.Store, claims *readyClaims, runs *fakeRunClient, tmpl *fakeTemplates,
		idp *agentlinktest.IDP, addr string,
	) *harnessSide {
		return startHarnessSideWith(t, store, claims, runs, tmpl, idp, addr, slowLeaseSandboxes{delay: 1500 * time.Millisecond},
			sandbox.WithAttachGrace(500*time.Millisecond), sandbox.WithLease(time.Hour))
	})
}

func restartMidTurn(t *testing.T, startSecond startSide) {
	ctx := as(stubClient)
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	idp := agentlinktest.NewIDP(t)
	claims := &readyClaims{claims: map[string]*sbxv1.SandboxClaim{}}
	runs := &fakeRunClient{}
	h := &harness{t: t, store: store, runs: runs, tmpl: &fakeTemplates{tmpl: testTemplate()}}
	for _, client := range []string{stubClient} {
		bind(h, client, []string{"deploy"}, nil)
	}
	first := startHarnessSide(t, store, claims, runs, h.tmpl, idp, addr)

	created, err := first.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "stub:restart", ApprovalPrompt: "ship it", InitialPrompt: "say hello",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ref := byID(created.GetSession().GetId())
	waitFor(t, "the run expectation", func() bool { return first.link.Expecting() == 1 })

	pod := startPod(t, addr)
	podCtx, stopPod := context.WithCancel(context.Background())
	t.Cleanup(stopPod)
	go func() { _ = pod.Run(podCtx) }()

	h.svc = first.svc
	waitForEvent(t, h, ref, kindOf("turn_completed", "t1"))
	if got := turnText(history(t, h, ref), "t1"); got != "hello" {
		t.Fatalf("opening reply = %q, want hello", got)
	}

	gate := filepath.Join(t.TempDir(), "gate")
	res, err := first.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "say before\ntool c1 build\nwait " + gate + "\nsay after"})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	turn := res.GetTurnId()
	waitForEvent(t, h, ref, kindOf("tool_call", turn))

	first.svc.Shutdown()
	first.stop()

	second := startSecond(t, store, claims, runs, h.tmpl, idp, addr)
	t.Cleanup(second.stop)
	h.svc = second.svc
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, h, ref, kindOf("turn_completed", turn))

	evs := history(t, h, ref)
	if got := turnText(evs, turn); got != "beforeafter" {
		t.Errorf("the turn's reply across the restart = %q, want beforeafter with no part repeated or lost", got)
	}
	for _, kind := range []string{"tool_call", "turn_completed"} {
		if n := countKind(evs, kind, turn); n != 1 {
			t.Errorf("%s recorded %d times, want once", kind, n)
		}
	}
	if n := countKind(evs, "session_ended", ""); n != 0 {
		t.Fatal("the restart ended the session")
	}
	for i, ev := range evs {
		if ev.GetSeq() != int64(i+1) {
			t.Fatalf("event %d has seq %d; the log has a gap", i, ev.GetSeq())
		}
	}

	next, err := second.svc.Prompt(ctx, &pb.PromptRequest{Ref: ref, Content: "say still here"})
	if err != nil {
		t.Fatalf("Prompt after the restart: %v", err)
	}
	waitForEvent(t, h, ref, kindOf("turn_completed", next.GetTurnId()))
	if got := turnText(history(t, h, ref), next.GetTurnId()); got != "still here" {
		t.Errorf("reply after the restart = %q", got)
	}

	if _, err := second.svc.EndSession(ctx, &pb.EndSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	waitForStoredState(t, h, ref, api.StateEnded)
}
