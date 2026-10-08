package app_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/slackbot/internal/gateway"
	"google.golang.org/protobuf/types/known/durationpb"
)

const contentEventType = "agentops_content"

func twoParticipants(t *testing.T, f *fixture) {
	t.Helper()
	ctx := context.Background()
	liveThread(t, f)
	f.app.HandleMention(ctx, mentionIn(f, "U2", "what about staging?"))
	f.poster.waitForPost(t, "joined with their own agent")
	f.api.setState("sess-2", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-2", api.StateRunning)
}

func TestSecondJoinFlipsTheRoomOnce(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)

	f.app.HandleMention(ctx, mentionIn(f, "U3", "and prod?"))
	waitFor(t, "the third session", func() bool { return len(f.api.createRequests()) == 3 })

	if n := len(f.poster.postsContaining("now multiplayer")); n != 1 {
		t.Errorf("the flip was announced %d times, want exactly one", n)
	}
	if meta := f.slackState("sess-1"); meta["multiplayer"] != true {
		t.Errorf("the original session was not marked multiplayer: %v", meta)
	}
	if meta := f.slackState("sess-1"); meta["last_seen_ts"] == "" || meta["last_seen_ts"] == nil {
		t.Errorf("the original session got no catch-up cursor: %v", meta)
	}
}

func TestMultiplayerPlainReplyIsAComment(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)

	before := len(f.api.promptRequests())
	f.app.HandleMessage(ctx, replyIn(f, "U1", "actually, hold on"))

	if got := len(f.api.promptRequests()); got != before {
		t.Fatalf("a plain message in a multiplayer thread became a turn: prompts %d -> %d", before, got)
	}

	f.app.HandleMention(ctx, mentionIn(f, "U1", "ok, carry on"))
	waitFor(t, "the owner's next turn", func() bool { return len(f.api.promptRequests()) > before })
	content := f.api.promptRequests()[before].Content
	if !strings.Contains(content, "[earlier comment from your requester] actually, hold on") {
		t.Errorf("the comment did not reach its own agent as context:\n%s", content)
	}
}

func TestCatchupDeltaOnTheNextMention(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)

	f.api.emit("sess-2", "t1", &pb.AgentMessage{
		PartId: "t1.1", Text: "staging is green", Final: true,
	})
	f.poster.waitForPost(t, "staging is green")
	f.humanSaid("U9", "we should roll back anyway")

	before := len(f.api.promptRequests())
	f.app.HandleMention(ctx, mentionIn(f, "U1", "promote it"))
	waitFor(t, "U1's turn", func() bool { return len(f.api.promptRequests()) > before })
	content := f.api.promptRequests()[before].Content

	for _, want := range []string{
		"Since your last turn",
		"[earlier message from another person] what about staging?",
		"[earlier reply from another person's agent, quoted from Slack] staging is green",
		"[earlier message from another person] we should roll back anyway",
		"The request: promote it",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("the catch-up is missing %q:\n%s", want, content)
		}
	}
	if strings.Contains(content, "Getting ready") || strings.Contains(content, "now multiplayer") {
		t.Errorf("chrome must not be carried into a catch-up:\n%s", content)
	}
	if ts, _ := f.slackState("sess-1")["last_seen_ts"].(string); ts == "" {
		t.Fatal("the cursor did not advance")
	}
	before = len(f.api.promptRequests())
	f.app.HandleMention(ctx, mentionIn(f, "U1", "and the changelog"))
	waitFor(t, "U1's second turn", func() bool { return len(f.api.promptRequests()) > before })
	if got := f.api.promptRequests()[before].Content; got != "and the changelog" {
		t.Errorf("an immediate second mention must carry no delta, got:\n%s", got)
	}
}

func TestAFailedCatchupReadIsCarriedByTheNextTurn(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)
	f.humanSaid("U9", "do not deploy production")

	before := len(f.api.promptRequests())
	f.poster.failNextReads(1)
	f.app.HandleMention(ctx, mentionIn(f, "U1", "check staging"))
	waitFor(t, "U1's turn without a catch-up", func() bool { return len(f.api.promptRequests()) > before })

	before = len(f.api.promptRequests())
	f.app.HandleMention(ctx, mentionIn(f, "U1", "continue"))
	waitFor(t, "U1's next turn", func() bool { return len(f.api.promptRequests()) > before })
	if content := f.api.promptRequests()[before].Content; !strings.Contains(content, "do not deploy production") {
		t.Errorf("the discussion the failed read missed never reached the agent:\n%s", content)
	}
}

func TestTheOpeningTurnsEndKeepsAFollowUpBusy(t *testing.T) {
	f := newFixture(t)
	liveThread(t, f)
	f.app.HandleMessage(context.Background(), replyIn(f, "U1", "also run the tests"))
	waitFor(t, "the follow-up turn", func() bool { return len(f.api.promptRequests()) == 1 })

	f.api.emit("sess-1", "opening", &pb.TurnCompleted{})
	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: "follow-up still running", Final: true})
	f.poster.waitForPost(t, "follow-up still running")

	for _, r := range f.poster.reactionsOn(threadRoot, "waiting") {
		if !r.add {
			t.Fatal("the opening turn's end cleared the busy marker of a follow-up that is still running")
		}
	}
}

func TestAJoinedSessionsReadyStatusAsksForAMention(t *testing.T) {
	f := newFixture(t)
	liveThread(t, f)
	f.app.HandleMention(context.Background(), mentionIn(f, "U2", ""))
	f.poster.waitForPost(t, "joined with their own agent")
	f.api.setState("sess-2", api.StatePending, api.StateRunning, noReason)
	ready := f.poster.waitForUpdate(t, "<@U2>'s agent: Ready")
	if !strings.Contains(ready.text, "@mention") || strings.Contains(ready.text, "reply in this thread") {
		t.Errorf("a multiplayer ready status must ask for an @mention, since plain replies are discussion: %q", ready.text)
	}
}

func TestOwnOutputIsFilteredFromTheDelta(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)

	f.api.emit("sess-1", "t1", &pb.AgentMessage{
		PartId: "t1.1", Text: "rollout staged by my own agent", Final: true,
	})
	answer := f.poster.waitForPost(t, "rollout staged by my own agent")
	if answer.meta.EventType != contentEventType {
		t.Fatalf("agent output was not tagged as content: %+v", answer.meta)
	}
	if answer.meta.EventPayload["session_id"] != "sess-1" {
		t.Errorf("the content tag names %v, want sess-1", answer.meta.EventPayload["session_id"])
	}

	before := len(f.api.promptRequests())
	f.app.HandleMention(ctx, mentionIn(f, "U1", "now prod"))
	waitFor(t, "U1's turn", func() bool { return len(f.api.promptRequests()) > before })
	if content := f.api.promptRequests()[before].Content; strings.Contains(content, "rollout staged by my own agent") {
		t.Errorf("a session was quoted its own answer back at it:\n%s", content)
	}
}

func finishOpeningTurns(t *testing.T, f *fixture) int {
	t.Helper()
	f.api.emit("sess-1", "opening-1", &pb.TurnCompleted{})
	f.api.emit("sess-2", "opening-2", &pb.TurnCompleted{})
	waitFor(t, "the opening turns to end", func() bool {
		ops := f.poster.reactionsOn(threadRoot, "waiting")
		return len(ops) > 0 && !ops[len(ops)-1].add
	})
	return len(f.poster.reactionsOn(threadRoot, "waiting"))
}

func busyOpsSince(f *fixture, mark int) []reactionOp {
	return f.poster.reactionsOn(threadRoot, "waiting")[mark:]
}

func TestBusyReactionAggregatesAcrossSessions(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)
	mark := finishOpeningTurns(t, f)

	f.app.HandleMention(ctx, mentionIn(f, "U1", "start something long"))
	f.app.HandleMention(ctx, mentionIn(f, "U2", "me too"))
	waitFor(t, "both turns to start", func() bool { return len(f.api.promptRequests()) >= 2 })
	waitFor(t, "the busy reaction", func() bool { return len(busyOpsSince(f, mark)) >= 1 })

	f.api.emit("sess-1", "t1", &pb.TurnCompleted{})
	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.2", Text: "sess-1 is done", Final: true})
	f.poster.waitForPost(t, "sess-1 is done")
	for _, r := range busyOpsSince(f, mark) {
		if !r.add {
			t.Fatal("the busy reaction came off while another session was still working")
		}
	}

	f.api.emit("sess-2", "t2", &pb.TurnCompleted{})
	waitFor(t, "the busy reaction to come off", func() bool {
		for _, r := range busyOpsSince(f, mark) {
			if !r.add {
				return true
			}
		}
		return false
	})
}

func TestASessionEndingMidTurnReleasesTheRoomsBusyMarker(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)
	mark := finishOpeningTurns(t, f)

	f.app.HandleMention(ctx, mentionIn(f, "U1", "start something long"))
	f.app.HandleMention(ctx, mentionIn(f, "U2", "me too"))
	waitFor(t, "both turns to start", func() bool { return len(f.api.promptRequests()) >= 2 })
	waitFor(t, "the busy reaction", func() bool { return len(busyOpsSince(f, mark)) >= 1 })

	f.api.setState("sess-1", api.StateRunning, api.StateEnded, noReason)
	f.api.emit("sess-1", "", &pb.SessionEnded{Reason: api.EndAgentExit})
	f.poster.waitForUpdate(t, "The agent stopped")

	f.api.emit("sess-2", "t2", &pb.TurnCompleted{})
	waitFor(t, "the busy reaction to come off", func() bool {
		for _, r := range busyOpsSince(f, mark) {
			if !r.add {
				return true
			}
		}
		return false
	})
}

func TestRevivingIntoARoomStampsTheReviver(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": "168.1", "user_id": "U1", "team_id": "T1",
	})

	f.app.HandleMention(ctx, mentionIn(f, "U2", "what about staging?"))
	waitFor(t, "the join", func() bool { return len(f.api.createRequests()) == 1 })
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	f.app.HandleMention(ctx, mentionIn(f, "U1", "still here"))
	waitFor(t, "the revive", func() bool { return len(f.api.promptRequests()) >= 1 })
	f.poster.waitForPost(t, "now multiplayer")

	waitFor(t, "the reviver to be marked multiplayer", func() bool {
		return f.slackState("old-1")["multiplayer"] == true
	})
	if meta := f.slackState("sess-1"); meta["multiplayer"] != true {
		t.Errorf("the joined session lost its multiplayer flag: %v", meta)
	}
}

func TestMultiplayerPausedNoticeIsEphemeral(t *testing.T) {
	f := newFixture(t)
	twoParticipants(t, f)
	posted := len(f.poster.allPosts())

	f.api.setState("sess-2", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-2", "", &pb.Suspended{Reason: api.ReasonIdle})

	anchor := f.poster.waitForUpdate(t, "paused this session")
	if !strings.Contains(anchor.text, "<@U2>'s agent:") {
		t.Errorf("the anchor should say whose session paused: %q", anchor.text)
	}
	assertEphemeral(t, f, "U2", "paused this session")
	if got := len(f.poster.allPosts()); got != posted {
		for _, p := range f.poster.allPosts()[posted:] {
			t.Errorf("a room got a public housekeeping message: %q", p.text)
		}
	}
}

func TestSoloStatusRestatementUnchanged(t *testing.T) {
	f := newFixture(t)
	liveThread(t, f)
	before := len(f.poster.deleteRecords())

	f.api.setState("sess-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-1", "", &pb.Suspended{Reason: api.ReasonIdle})

	paused := f.poster.waitForPost(t, "paused this session")
	if paused.threadTS != threadRoot {
		t.Errorf("the restated status should be posted in the thread, got thread_ts %q", paused.threadTS)
	}
	if strings.Contains(paused.text, "'s agent:") {
		t.Errorf("a solo thread has nobody to disambiguate from: %q", paused.text)
	}
	waitFor(t, "the superseded status to be taken down", func() bool {
		return len(f.poster.deleteRecords()) > before
	})
	if n := len(f.poster.ephemeralTo("U1")); n != 0 {
		t.Errorf("a solo thread should say this out loud, not privately: %d ephemerals", n)
	}
}

func TestMultiplayerIdleWarningGoesToDM(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)

	f.api.emit("sess-2", "", &pb.IdleWarning{Lead: durationpb.New(2 * time.Minute)})
	warning := f.poster.waitForPost(t, "I'll close its session")
	if warning.channel != "U2" {
		t.Fatalf("the idle warning went to %q, want the owner's DM", warning.channel)
	}
	if !strings.Contains(warning.text, "@mention me") {
		t.Errorf("in a room a plain reply keeps nothing going, so the warning must ask for a mention: %q", warning.text)
	}

	f.app.HandleMention(ctx, mentionIn(f, "U2", "still here"))
	waitFor(t, "the idle warning to be withdrawn", func() bool {
		for _, u := range f.poster.updatesTo(warning.ts) {
			if strings.Contains(u.text, "Never mind") {
				return true
			}
		}
		return false
	})
	for _, d := range f.poster.deleteRecords() {
		if d.ts == warning.ts {
			t.Error("a DM must be edited, not deleted")
		}
	}
}

func TestPermissionClickIsRoutedBySessionNotClicker(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)

	f.app.HandleInteraction(ctx, gateway.Interaction{
		TeamID: "T1", UserID: "U2", ChannelID: "C1", ThreadTS: threadRoot,
		ActionID:    gateway.ActionPermission,
		Value:       gateway.EncodePermissionValue("sess-1", "tc-1", "allow"),
		ResponseURL: "https://slack.example/respond",
	})
	if n := len(f.api.permissionResponses()); n != 0 {
		t.Fatalf("a click on somebody else's button was relayed (%d responses)", n)
	}

	f.app.HandleInteraction(ctx, gateway.Interaction{
		TeamID: "T1", UserID: "U1", ChannelID: "C1", ThreadTS: threadRoot,
		ActionID:    gateway.ActionPermission,
		Value:       gateway.EncodePermissionValue("sess-1", "tc-1", "allow"),
		ResponseURL: "https://slack.example/respond",
	})
	waitFor(t, "the owner's decision to be relayed", func() bool {
		return len(f.api.permissionResponses()) == 1
	})
	if got := f.api.permissionResponses()[0]; got.GetRef().GetSessionId() != "sess-1" || got.GetOptionId() != "allow" {
		t.Errorf("relayed decision = %+v", got)
	}
}

func TestStatusLinesNameTheirOwnerInARoom(t *testing.T) {
	f := newFixture(t)
	twoParticipants(t, f)

	posts := f.poster.postsContaining("<@U2>'s agent:")
	if len(posts) == 0 {
		t.Fatal("the joiner's status line does not say whose agent it is")
	}
	if !strings.HasPrefix(posts[0].text, ":") {
		t.Errorf("the owner prefix displaced the status marker: %q", posts[0].text)
	}
}

func TestAnswerAttributionStaysOutOfTheText(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoParticipants(t, f)

	f.api.emit("sess-2", "t1", &pb.AgentMessage{
		PartId: "t1.1", Text: "staging is green", Final: true,
	})
	answer := f.poster.waitForPost(t, "staging is green")
	if strings.Contains(answer.text, "<@U2>") {
		t.Errorf("attribution must not reach the text, which is what a thread read carries: %q", answer.text)
	}

	before := len(f.api.promptRequests())
	f.app.HandleMention(ctx, mentionIn(f, "U1", "promote it"))
	waitFor(t, "U1's turn", func() bool { return len(f.api.promptRequests()) > before })
	content := f.api.promptRequests()[before].Content
	if !strings.Contains(content, "staging is green") {
		t.Fatalf("the other agent's answer should have been carried:\n%s", content)
	}
	if strings.Contains(content, "↳") {
		t.Errorf("attribution chrome reached a prompt:\n%s", content)
	}
}

func TestSoloThreadBehaviorIsUnchanged(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	liveThread(t, f)

	f.app.HandleMessage(ctx, replyIn(f, "U1", "and the changelog"))
	waitFor(t, "the owner's turn", func() bool { return len(f.api.promptRequests()) == 1 })
	if got := f.api.promptRequests()[0].Content; got != "and the changelog" {
		t.Errorf("a solo turn must carry the words alone, got:\n%s", got)
	}
	if meta := f.slackState("sess-1"); meta["multiplayer"] == true || meta["last_seen_ts"] != nil {
		t.Errorf("a solo thread should keep no multiplayer state: %v", meta)
	}

	f.api.setState("sess-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-1", "", &pb.Suspended{Reason: api.ReasonIdle})
	f.poster.waitForPost(t, "paused this session")
	waitFor(t, "the paused session's stream to close", func() bool {
		return f.api.openStreams("sess-1") == 0
	})

	f.app.HandleMessage(ctx, replyIn(f, "U1", "still there?"))
	waitFor(t, "the revive", func() bool { return len(f.api.promptRequests()) == 2 })
	if got := f.api.promptRequests()[1].Content; got != "still there?" {
		t.Errorf("a solo revive must carry the words alone, got:\n%s", got)
	}
}
