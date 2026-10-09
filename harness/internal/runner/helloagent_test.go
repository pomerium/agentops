package runner_test

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func helloAgent(t *testing.T) []string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "..", "deploy", "charts", "agentops-quickstart", "files", "hello-agent.py"))
	if err != nil {
		t.Fatalf("hello agent path: %v", err)
	}
	return []string{"/usr/bin/env", "HOSTNAME=hello-pod", python, "-u", script}
}

func turnText(t *testing.T, p *peer, turnID string) string {
	t.Helper()
	evs := p.until(finished(turnID))
	last := evs[len(evs)-1].GetTurnFinished()
	if last.GetStopReason() != "end_turn" || last.GetError() != "" {
		t.Fatalf("turn %s finished with stop=%q error=%q, want end_turn (events %v)", turnID, last.GetStopReason(), last.GetError(), describe(evs))
	}
	var text strings.Builder
	for _, ev := range evs {
		text.WriteString(ev.GetMessage().GetText())
	}
	return text.String()
}

func TestTheQuickstartHelloAgentAnswersEveryPrompt(t *testing.T) {
	client := serve(t, helloAgent(t))
	p, _ := mustSpawn(t, client, &agentlinkpb.SessionParams{Cwd: "/workspace", SystemPrompt: "ignored"})
	p.replay(0)
	ready := readyFrom(t, p).GetSessionReady()
	if !strings.HasPrefix(ready.GetAcpSessionId(), "hello-") || !ready.GetResumable() {
		t.Fatalf("ready = %v, want a hello- session that can be resumed", ready)
	}

	for i, prompt := range []string{"hi there", "and again"} {
		turnID := fmt.Sprintf("t%d", i+1)
		p.prompt(turnID, uint64(i+1), prompt)
		text := turnText(t, p, turnID)
		for _, want := range []string{"Hello from AgentOps!", "sandbox pod `hello-pod`", `Your prompt was: "` + prompt + `"`, "Next steps:"} {
			if !strings.Contains(text, want) {
				t.Errorf("turn %s answer does not contain %q:\n%s", turnID, want, text)
			}
		}
	}
}

func TestTheQuickstartHelloAgentResumesASession(t *testing.T) {
	client := serve(t, helloAgent(t))
	p, _ := mustSpawn(t, client, &agentlinkpb.SessionParams{Cwd: "/workspace", ResumeSessionId: "hello-earlier"})
	p.replay(0)
	if id := readyFrom(t, p).GetSessionReady().GetAcpSessionId(); id != "hello-earlier" {
		t.Fatalf("resumed session id = %q, want hello-earlier", id)
	}
	p.prompt("t1", 1, "still there?")
	if text := turnText(t, p, "t1"); !strings.Contains(text, "Hello from AgentOps!") {
		t.Fatalf("answer after resume:\n%s", text)
	}
}
