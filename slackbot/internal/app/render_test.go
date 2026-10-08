package app

import (
	"strings"
	"testing"

	"github.com/slack-go/slack"
)

func roomThread(flipped bool) *thread {
	t := &thread{ownerUserID: "U2", teamID: "T1", channel: "C1", threadTS: "168.1", sessionID: "sess-2"}
	t.room = &room{}
	t.room.flipped.Store(flipped)
	return t
}

func contextText(blocks []slack.Block) string {
	if len(blocks) == 0 {
		return ""
	}
	cb, ok := blocks[0].(*slack.ContextBlock)
	if !ok || len(cb.ContextElements.Elements) == 0 {
		return ""
	}
	el, ok := cb.ContextElements.Elements[0].(*slack.TextBlockObject)
	if !ok {
		return ""
	}
	return el.Text
}

func TestAnswerBlocksAttributeTheTurnInARoom(t *testing.T) {
	got := answerBlocks(roomThread(true), "staging is green", true)
	if !strings.Contains(contextText(got), "<@U2>") {
		t.Errorf("a turn's first message should name whose request it answers, got %q", contextText(got))
	}
	if len(got) < 2 {
		t.Fatalf("the answer's own blocks went missing: %d blocks", len(got))
	}
}

func TestAnswerBlocksAttributeOnlyTheTurnsFirstMessage(t *testing.T) {
	if got := answerBlocks(roomThread(true), "part two", false); contextText(got) != "" {
		t.Errorf("only a turn's first message is attributed, got %q", contextText(got))
	}
}

func TestAnswerBlocksLeaveASoloThreadClean(t *testing.T) {
	if got := answerBlocks(roomThread(false), "staging is green", true); contextText(got) != "" {
		t.Errorf("a solo thread must not be attributed, got %q", contextText(got))
	}
}

func TestOwnerPrefixedKeepsTheStatusMarkerFirst(t *testing.T) {
	got := ownerPrefixed("U2", msgStatusPreparing)
	if !strings.HasPrefix(got, ":hourglass_flowing_sand: <@U2>'s agent: ") {
		t.Errorf("ownerPrefixed = %q", got)
	}
	if !chromeLike(got) {
		t.Errorf("a prefixed status line must still read as chrome: %q", got)
	}
	if got := ownerPrefixed("", msgStatusPreparing); got != msgStatusPreparing {
		t.Errorf("no owner must leave the line alone, got %q", got)
	}
}

func TestPriorTS(t *testing.T) {
	replies := []slack.Message{
		human("168.0001", "root"),
		human("168.0004", "later"),
		human("168.0002", "out of order"),
		human("168.0009", "the trigger"),
	}
	if got := priorTS(replies, "168.0009"); got != "168.0004" {
		t.Errorf("priorTS = %q, want the newest message before the trigger", got)
	}
	if got := priorTS(replies, "168.0001"); got != "168.0001" {
		t.Errorf("priorTS with nothing before = %q, want the trigger itself", got)
	}
}
