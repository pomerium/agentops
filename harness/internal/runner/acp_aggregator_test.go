package runner

import (
	"strings"
	"testing"
	"unicode/utf8"

	acp "github.com/coder/acp-go-sdk"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func collect() (*Aggregator, *[]*agentlinkpb.AgentEvent) {
	var evs []*agentlinkpb.AgentEvent
	return NewAggregator(func(_ string, ev *agentlinkpb.AgentEvent) { evs = append(evs, ev) }), &evs
}

func TestALongReplyIsSplitIntoBoundedParts(t *testing.T) {
	agg, evs := collect()
	agg.Begin("t1")
	reply := strings.Repeat("é", 3*partMax/2)
	agg.Update(acp.UpdateAgentMessageText(reply))
	agg.End()

	var got strings.Builder
	var parts []string
	for _, ev := range *evs {
		m := ev.GetMessage()
		if m == nil {
			continue
		}
		if len(m.GetText()) > partMax || !utf8.ValidString(m.GetText()) {
			t.Fatalf("part %s has %d bytes (max %d), valid UTF-8 %v", m.GetPartId(), len(m.GetText()), partMax, utf8.ValidString(m.GetText()))
		}
		parts = append(parts, m.GetPartId())
		got.WriteString(m.GetText())
		if want := m == (*evs)[len(*evs)-1].GetMessage(); m.GetFinal() != want {
			t.Errorf("part %s final = %v, want %v", m.GetPartId(), m.GetFinal(), want)
		}
	}
	if got.String() != reply {
		t.Errorf("the parts do not join to the reply (%d bytes, want %d)", got.Len(), len(reply))
	}
	if len(parts) < 3 || parts[0] != "t1.1" || parts[1] != "t1.2" {
		t.Errorf("parts = %v, want at least three numbered from t1.1", parts)
	}
}

func TestALongThoughtIsSplitIntoBoundedEvents(t *testing.T) {
	agg, evs := collect()
	agg.Begin("t1")
	thought := strings.Repeat("x", 2*partMax+1)
	agg.Update(acp.UpdateAgentThoughtText(thought))
	agg.End()

	var got strings.Builder
	for _, ev := range *evs {
		if th := ev.GetThought(); th != nil {
			if len(th.GetText()) > partMax {
				t.Fatalf("thought has %d bytes, max %d", len(th.GetText()), partMax)
			}
			got.WriteString(th.GetText())
		}
	}
	if got.String() != thought {
		t.Errorf("the thoughts do not join to the thought (%d bytes, want %d)", got.Len(), len(thought))
	}
}

func TestAToolInputOverTheCapIsLeftOut(t *testing.T) {
	agg, evs := collect()
	agg.Begin("t1")
	agg.Update(acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
		ToolCallId: "c1", Title: "write", RawInput: map[string]any{"data": strings.Repeat("x", partMax)},
	}})
	agg.Update(acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
		ToolCallId: "c2", Title: "read", RawInput: map[string]any{"path": "/tmp/a"},
	}})
	var calls []*agentlinkpb.ToolCall
	for _, ev := range *evs {
		if c := ev.GetToolCall(); c != nil {
			calls = append(calls, c)
		}
	}
	if len(calls) != 2 {
		t.Fatalf("tool calls = %d, want 2", len(calls))
	}
	if len(calls[0].GetRawInput()) != 0 {
		t.Errorf("an input of %d bytes was sent, want it left out", len(calls[0].GetRawInput()))
	}
	if string(calls[1].GetRawInput()) != `{"path":"/tmp/a"}` {
		t.Errorf("small input = %s", calls[1].GetRawInput())
	}
}
