package app_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	slackapp "github.com/pomerium/agentops/slackbot/internal/app"
	"github.com/pomerium/agentops/slackbot/internal/gateway"
)

const threadRoot = "168.1"

type tsClock struct {
	mu sync.Mutex
	n  int
}

func (c *tsClock) next() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return fmt.Sprintf("168.%04d", 2000+c.n)
}

type fixture struct {
	app      *slackapp.App
	api      *fakeAPI
	poster   *fakePoster
	resolver *fakeResolver
	clock    *tsClock
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clock := &tsClock{}
	logs := newTestWriter(t)
	f := &fixture{
		api: newFakeAPI(), poster: &fakePoster{clock: clock},
		resolver: boundResolver(testTemplate), clock: clock,
	}
	f.app = slackapp.New(f.api.serve(t), f.poster, f.resolver,
		slackapp.WithBotUserID("UBOT"), slackapp.WithHomeTeamID("T1"),
		slackapp.WithLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelError}))))
	t.Cleanup(f.app.Shutdown)
	return f
}

func (f *fixture) ts() string { return f.clock.next() }

func (f *fixture) seedSession(id, convRef string, state api.SessionState, meta map[string]any) {
	f.api.seedSession(id, convRef, state)
	threadTS, _ := meta["thread_ts"].(string)
	f.poster.mu.Lock()
	defer f.poster.mu.Unlock()
	f.poster.posted = append(f.poster.posted, botMessage(f.clock.next(), threadTS, "status",
		slack.SlackMetadata{EventType: "agentops_chrome", EventPayload: map[string]any{
			"session_id": id, "state": meta,
		}}))
}

func (f *fixture) slackState(sessionID string) map[string]any {
	f.poster.mu.Lock()
	defer f.poster.mu.Unlock()
	for i := len(f.poster.posted) - 1; i >= 0; i-- {
		md := f.poster.posted[i].Metadata
		if md.EventType != "agentops_chrome" || md.EventPayload["session_id"] != sessionID {
			continue
		}
		if st, ok := md.EventPayload["state"].(map[string]any); ok {
			return st
		}
	}
	return nil
}

func (f *fixture) record(msg slack.Message) {
	f.poster.mu.Lock()
	defer f.poster.mu.Unlock()
	f.poster.replies = append(f.poster.replies, msg)
}

func (f *fixture) humanSaid(user, text string) {
	f.record(userMessage(user, f.ts(), text))
}

type testWriter struct {
	t    *testing.T
	mu   sync.Mutex
	done bool
}

func newTestWriter(t *testing.T) *testWriter {
	w := &testWriter{t: t}
	t.Cleanup(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.done = true
	})
	return w
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.done {
		w.t.Logf("%s", p)
	}
	return len(p), nil
}

func mention(f *fixture, text string) gateway.MentionInvocation {
	f.record(userMessage("U1", threadRoot, text))
	return gateway.MentionInvocation{
		TeamID: "T1", UserID: "U1", ChannelID: "C1",
		MessageTS: threadRoot, ThreadTS: threadRoot,
		Text: text, Prompt: text,
	}
}

func TestMentionCreatesASession(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))

	reqs := f.api.createRequests()
	if len(reqs) != 1 {
		t.Fatalf("CreateSession calls = %d, want 1", len(reqs))
	}
	req := reqs[0]
	if req.ConversationRef != "slack:C1:168.1:T1:U1" {
		t.Errorf("conversation_ref = %q", req.ConversationRef)
	}
	if req.Template != "deploy" {
		t.Errorf("template = %q", req.Template)
	}
	if req.ApprovalPrompt != "ship it" || req.InitialPrompt != "ship it" {
		t.Errorf("prompts = %q / %q", req.ApprovalPrompt, req.InitialPrompt)
	}
	if !strings.Contains(strings.ToLower(req.SystemPromptAppendix), "slack") {
		t.Errorf("the client's rendering rules did not travel with the session: %q", req.SystemPromptAppendix)
	}

	f.poster.waitForPost(t, "Getting ready")
	meta := f.slackState("sess-1")
	if meta["channel_id"] != "C1" || meta["thread_ts"] != "168.1" ||
		meta["user_id"] != "U1" || meta["team_id"] != "T1" {
		t.Errorf("session metadata = %v", meta)
	}
}

func TestMentionInUnboundChannel(t *testing.T) {
	f := newFixture(t)
	in := mention(f, "hello")
	in.ChannelID = "C-OTHER"
	f.app.HandleMention(context.Background(), in)

	if n := len(f.api.createRequests()); n != 0 {
		t.Errorf("an unbound channel launched %d sessions", n)
	}
	f.poster.waitForPost(t, "No agent is configured")
}

func TestApprovalIsDeliveredByDM(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	f.api.emit("sess-1", "", &pb.ApprovalRequired{
		ApprovalUrl: "https://pom.example/approve?run_id=run-1",
	})
	dm := f.poster.waitForPost(t, "Approve it and I'll start")
	if dm.channel != "U1" {
		t.Errorf("the approval went to %q, want the owner's DM", dm.channel)
	}
	if strings.Contains(dm.text, "https://pom.example/approve?run_id=run-1") &&
		!strings.Contains(dm.text, "|Review & approve>") {
		t.Error("the consent URL should ride on a labeled link, never displayed raw")
	}
}

func TestPlainReplyFromANonParticipantIsDiscussion(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	for range 2 {
		f.app.HandleMessage(ctx, replyIn(f, "U2", "me too"))
	}
	if n := len(f.api.promptRequests()); n != 0 {
		t.Errorf("a non-participant's message became %d turns", n)
	}
	hints := f.poster.ephemeralTo("U2")
	if len(hints) != 1 {
		t.Fatalf("a side conversation got %d nudges, want exactly one", len(hints))
	}
	if !strings.Contains(hints[0].text, "@mention me") {
		t.Errorf("the nudge should teach the gesture that works: %q", hints[0].text)
	}
	f.app.HandleMessage(ctx, replyIn(f, "U1", "and deploy it"))
	prompts := f.api.promptRequests()
	if len(prompts) != 1 || prompts[0].Content != "and deploy it" {
		t.Errorf("the owner's reply = %+v", prompts)
	}
}

func TestPromptIsKeyedByItsMessage(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	reply := replyIn(f, "U1", "and deploy it")
	f.app.HandleMessage(ctx, reply)
	prompts := f.api.promptRequests()
	if len(prompts) != 1 {
		t.Fatalf("the owner's reply became %d prompts", len(prompts))
	}
	if want := "C1/" + reply.MessageTS; prompts[0].IdempotencyKey != want {
		t.Errorf("idempotency key = %q, want %q", prompts[0].IdempotencyKey, want)
	}
}

func TestPermissionClickIsOwnerOnly(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	value := gateway.EncodePermissionValue("sess-1", "tc-1", "allow")

	f.app.HandleInteraction(ctx, gateway.Interaction{
		TeamID: "T1", UserID: "U2", ChannelID: "C1", ThreadTS: "168.1",
		ActionID: gateway.ActionPermission, Value: value, ResponseURL: "https://slack.example/respond",
	})
	if n := len(f.api.permissionResponses()); n != 0 {
		t.Fatalf("a non-owner's click was relayed (%d responses)", n)
	}

	f.app.HandleInteraction(ctx, gateway.Interaction{
		TeamID: "T1", UserID: "U1", ChannelID: "C1", ThreadTS: "168.1",
		ActionID: gateway.ActionPermission, Value: value, ResponseURL: "https://slack.example/respond",
	})
	got := f.api.permissionResponses()
	if len(got) != 1 {
		t.Fatalf("the owner's click produced %d responses, want 1", len(got))
	}
	if got[0].GetRequestId() != "tc-1" || got[0].GetOptionId() != "allow" {
		t.Errorf("relayed decision = %+v", got[0])
	}
}

func TestAgentOutputUsesTheRightDeliveryPath(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	f.api.emit("sess-1", "t1", &pb.AgentMessage{
		PartId: "t1.1", Text: "looking at it", Final: false,
	})
	inProgress := f.poster.waitForPost(t, "looking at it")
	if !strings.HasSuffix(strings.TrimSpace(inProgress.text), "…") {
		t.Errorf("an in-progress segment should read as unfinished: %q", inProgress.text)
	}

	f.api.emit("sess-1", "t1", &pb.AgentMessage{
		PartId: "t1.2", Text: "still working", Final: false,
	})
	streamed := f.poster.waitForUpdate(t, "still working")
	if !streamed.debounced {
		t.Error("a mid-turn replacement must use the droppable path")
	}

	f.api.emit("sess-1", "t1", &pb.AgentMessage{
		PartId: "t1.3", Text: "shipped it", Final: true,
	})
	final := f.poster.waitForUpdate(t, "shipped it")
	if final.debounced {
		t.Error("a final answer must never be droppable")
	}
	if final.ts != inProgress.ts {
		t.Errorf("the final answer replaced %q, want the turn's own message %q", final.ts, inProgress.ts)
	}
}

func TestATurnsMessageKeepsEveryPartOfTheAnswer(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: "First finding.", Final: false})
	first := f.poster.waitForPost(t, "First finding.")
	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.2", Text: "Second finding.", Final: true})
	final := f.poster.waitForUpdate(t, "Second finding.")

	if final.ts != first.ts {
		t.Errorf("the final part went to %q, want the turn's message %q", final.ts, first.ts)
	}
	if !strings.Contains(final.text, "First finding.") {
		t.Errorf("the turn's message lost its earlier part: %q", final.text)
	}

	f.api.emit("sess-1", "t2", &pb.AgentMessage{PartId: "t2.1", Text: "A new turn.", Final: true})
	next := f.poster.waitForPost(t, "A new turn.")
	if strings.Contains(next.text, "finding") {
		t.Errorf("a new turn carried the last turn's parts: %q", next.text)
	}
}

func TestATurnThatEndsWithoutAFinalPartFinishesItsMessage(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: "checked the logs", Final: false})
	inProgress := f.poster.waitForPost(t, "checked the logs")
	f.api.emit("sess-1", "t1", &pb.TurnCompleted{})

	waitFor(t, "the finished message", func() bool {
		for _, u := range f.poster.updatesTo(inProgress.ts) {
			if !u.debounced && u.text == "checked the logs" {
				return true
			}
		}
		return false
	})
}

func TestATurnThatEndsWithoutAFinalPartPostsItsWholeAnswer(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	long := strings.Repeat("word ", 3000) + "the very end"
	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: long, Final: false})
	f.poster.waitForPost(t, "word word")
	f.api.emit("sess-1", "t1", &pb.TurnCompleted{})

	waitFor(t, "the rest of the answer in a second message", func() bool {
		return len(f.poster.postsContaining("word word")) >= 2
	})
}

func TestSessionEndedIsRenderedAndReleasesTheThread(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	f.api.setState("sess-1", api.StateRunning, api.StateEnded, noReason)
	f.api.emit("sess-1", "", &pb.SessionEnded{Reason: api.EndRevoked})
	f.poster.waitForPost(t, "approval for this session was withdrawn")

	deadline := time.Now().Add(2 * time.Second)
	for {
		f.app.HandleMessage(ctx, gateway.ThreadMessage{
			TeamID: "T1", UserID: "U1", ChannelID: "C1", ThreadTS: "168.1", Text: "still there?",
		})
		if len(f.api.promptRequests()) > 0 {
			t.Fatal("a reply to an ended session became a turn")
		}
		if hints := f.poster.ephemeralTo("U1"); len(hints) > 0 {
			if !strings.Contains(hints[0].text, "@mention me to start a new session here") {
				t.Errorf("the ended-thread hint should teach the mention: %q", hints[0].text)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a reply to an ended thread said nothing at all")
		}
		time.Sleep(10 * time.Millisecond)
	}

	f.app.HandleMention(ctx, mentionIn(f, "U1", "pick it back up"))
	waitFor(t, "the fresh session", func() bool { return len(f.api.createRequests()) == 2 })
	if ref := f.api.createRequests()[1].ConversationRef; ref != "slack:C1:168.1:T1:U1" {
		t.Errorf("the fresh session must be in the same thread: %q", ref)
	}
}

func TestASessionsEventsArriveAfterTheHarnessWasBrieflyUnavailable(t *testing.T) {
	f := newFixture(t)
	f.api.failSubscribes(api.ErrUnavailable)

	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: "made it through", Final: true})
	f.poster.waitForPost(t, "made it through")
}

func TestStartupFollowsALiveSessionFromWhereItsThreadLeftOff(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateRunning, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1", "last_seq": 2,
	})
	f.api.emit("old-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: "already shown", Final: true})
	f.api.emit("old-1", "t1", &pb.TurnCompleted{})

	f.app.ReconcileOnStartup(context.Background())
	waitFor(t, "the session's stream", func() bool { return f.api.openStreams("old-1") == 1 })
	f.api.emit("old-1", "t2", &pb.AgentMessage{PartId: "t2.1", Text: "said while the bot was down", Final: true})

	f.poster.waitForPost(t, "said while the bot was down")
	if n := len(f.poster.postsContaining("already shown")); n != 0 {
		t.Errorf("an answer the thread already had was posted again %d times", n)
	}
	if n := len(f.poster.postsContaining("restart")); n != 0 {
		t.Errorf("a session the bot can follow again was reported lost: %v", f.poster.postsContaining("restart"))
	}
	if ends := f.api.endRequests(); len(ends) != 0 {
		t.Errorf("startup ended a live session: %v", ends)
	}

	f.app.HandleMessage(context.Background(), replyIn(f, "U1", "and the tests?"))
	waitFor(t, "the owner's next turn", func() bool { return len(f.api.promptRequests()) == 1 })
}

func TestStartupFollowsASessionPastAPauseItMissed(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateRunning, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1", "last_seq": 1,
	})
	f.api.emit("old-1", "t1", &pb.TurnCompleted{})
	f.api.setState("old-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("old-1", "", &pb.Suspended{Reason: api.ReasonIdle})
	f.api.setState("old-1", api.StateSuspended, api.StateRunning, noReason)
	f.api.emit("old-1", "t2", &pb.AgentMessage{PartId: "t2.1", Text: "continued after the pause", Final: true})

	f.app.ReconcileOnStartup(context.Background())
	f.poster.waitForPost(t, "continued after the pause")
	f.api.emit("old-1", "t3", &pb.AgentMessage{PartId: "t3.1", Text: "and still followed", Final: true})
	f.poster.waitForPost(t, "and still followed")
}

func TestStartupWatchesASessionThatPausedWhileTheBotWasDown(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateRunning, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1",
	})
	f.api.setState("old-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("old-1", "", &pb.Suspended{Reason: api.ReasonIdle})

	f.app.ReconcileOnStartup(context.Background())
	f.poster.waitForPost(t, "paused this session")
	waitFor(t, "the watch", func() bool { return f.slackState("old-1")["watching"] == true })

	f.api.setState("old-1", api.StateSuspended, api.StateEnded, noReason)
	f.api.emit("old-1", "", &pb.SessionEnded{Reason: api.EndExpired})
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go f.app.RunSweeper(ctx, 10*time.Millisecond)
	f.poster.waitForPost(t, "approval ran out")
}

func TestARestartMidAnswerKeepsTheAnswerInOneMessage(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: "First finding.", Final: false})
	answer := f.poster.waitForPost(t, "First finding.")
	f.api.emit("sess-1", "", &pb.LaunchStalled{})
	f.poster.waitForUpdate(t, "hasn't connected back")
	f.app.Shutdown()
	waitFor(t, "the old consumer to stop", func() bool { return f.api.openStreams("sess-1") == 0 })

	restarted := slackapp.New(f.api.serve(t), f.poster, f.resolver,
		slackapp.WithBotUserID("UBOT"), slackapp.WithHomeTeamID("T1"))
	t.Cleanup(restarted.Shutdown)
	restarted.ReconcileOnStartup(context.Background())
	waitFor(t, "the new consumer", func() bool { return f.api.openStreams("sess-1") == 1 })
	f.api.emit("sess-1", "t1", &pb.AgentMessage{PartId: "t1.2", Text: "Second finding.", Final: true})

	waitFor(t, "the whole answer in the turn's message", func() bool {
		for _, u := range f.poster.updatesTo(answer.ts) {
			if !u.debounced && strings.Contains(u.text, "First finding.") && strings.Contains(u.text, "Second finding.") {
				return true
			}
		}
		return false
	})
	if n := len(f.poster.postsContaining("finding")); n != 1 {
		t.Errorf("the answer was split across %d messages", n)
	}
}

func TestStartupReplaysATurnItsCursorStopsInside(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateRunning, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1", "last_seq": 2,
	})
	f.api.emit("old-1", "t1", &pb.AgentMessage{PartId: "t1.1", Text: "First finding."})
	f.api.emit("old-1", "", &pb.LaunchStalled{})
	f.api.emit("old-1", "t1", &pb.AgentMessage{PartId: "t1.2", Text: "Second finding.", Final: true})

	f.app.ReconcileOnStartup(context.Background())
	answer := f.poster.waitForPost(t, "First finding.")
	waitFor(t, "the whole answer in one message", func() bool {
		for _, u := range f.poster.updatesTo(answer.ts) {
			if !u.debounced && strings.Contains(u.text, "First finding.") && strings.Contains(u.text, "Second finding.") {
				return true
			}
		}
		return false
	})
}

func TestTheSweeperFollowsALiveSessionStartupCouldNotRead(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateRunning, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1",
	})
	f.poster.failNextReads(1)
	f.app.ReconcileOnStartup(context.Background())
	if n := f.api.openStreams("old-1"); n != 0 {
		t.Fatalf("startup followed a session whose thread it could not read: %d streams", n)
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go f.app.RunSweeper(ctx, 10*time.Millisecond)
	waitFor(t, "the sweeper to follow the session", func() bool { return f.api.openStreams("old-1") == 1 })
}

func TestTheSweeperShowsAnEndingThatCameWhileTheBotWasDown(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)
	f.app.Shutdown()
	waitFor(t, "the old consumer to stop", func() bool { return f.api.openStreams("sess-1") == 0 })

	f.api.setState("sess-1", api.StateRunning, api.StateEnded, noReason)
	f.api.emit("sess-1", "", &pb.SessionEnded{Reason: api.EndExpired})

	restarted := slackapp.New(f.api.serve(t), f.poster, f.resolver,
		slackapp.WithBotUserID("UBOT"), slackapp.WithHomeTeamID("T1"))
	t.Cleanup(restarted.Shutdown)
	restarted.ReconcileOnStartup(context.Background())
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go restarted.RunSweeper(ctx, 10*time.Millisecond)
	f.poster.waitForPost(t, "approval ran out")
}

func TestTheSweeperDoesNotRepeatAnEnding(t *testing.T) {
	f := newFixture(t)
	liveThread(t, f)
	f.api.setState("sess-1", api.StateRunning, api.StateEnded, noReason)
	f.api.emit("sess-1", "", &pb.SessionEnded{Reason: api.EndEnded})
	f.poster.waitForPost(t, "Finished")
	f.seedSession("old-1", "slack:C1:168.1:T1:U9", api.StateEnded, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U9", "team_id": "T1",
	})
	f.api.emit("old-1", "", &pb.SessionEnded{Reason: api.EndExpired})
	waitFor(t, "the first consumer to stop", func() bool { return f.api.openStreams("sess-1") == 0 })
	before := len(f.poster.allPosts())

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go f.app.RunSweeper(ctx, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	if posts := f.poster.allPosts(); len(posts) != before {
		t.Errorf("the sweeper posted about sessions whose endings were shown or never owed: %v", posts[before:])
	}
}

func TestStartupLeavesTheSessionsItAlreadyFollowsAlone(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	waitFor(t, "the session's stream", func() bool { return f.api.openStreams("sess-1") == 1 })

	f.app.ReconcileOnStartup(context.Background())

	if n := f.api.openStreams("sess-1"); n != 1 {
		t.Errorf("startup opened a second stream for a session it already follows: %d streams", n)
	}
	if n := len(f.poster.postsContaining("restart")); n != 0 {
		t.Errorf("a fresh session was reported lost: %v", f.poster.postsContaining("restart"))
	}
}

func TestATurnEndRecordsHowFarTheThreadGot(t *testing.T) {
	f := newFixture(t)
	liveThread(t, f)
	f.api.emit("sess-1", "t1", &pb.TurnCompleted{})
	waitFor(t, "the cursor", func() bool {
		seq, _ := f.slackState("sess-1")["last_seq"].(float64)
		return seq >= 2
	})
}

func TestAFailedLaunchMarksTheRequestFailed(t *testing.T) {
	f := newFixture(t)
	f.app.HandleMention(context.Background(), mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	f.api.setState("sess-1", api.StatePending, api.StateEnded, noReason)
	f.api.emit("sess-1", "", &pb.SessionEnded{Reason: api.EndPrepareFailed})
	f.poster.waitForPost(t, "couldn't get a workspace ready")

	waitFor(t, "the failure reaction", func() bool {
		for _, r := range f.poster.reactionsOn(threadRoot, "x") {
			if r.add {
				return true
			}
		}
		return false
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForState(t *testing.T, f *fixture, sessionID string, want api.SessionState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if state, ok := f.api.state(sessionID); ok && state == want {
			time.Sleep(20 * time.Millisecond)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %s never reached %q", sessionID, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPausedSessionHoldsNoStream(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)

	f.api.setState("sess-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-1", "", &pb.Suspended{Reason: api.ReasonIdle})
	f.poster.waitForPost(t, "paused this session")

	waitFor(t, "the paused session's stream to close", func() bool {
		return f.api.openStreams("sess-1") == 0
	})
	meta := f.slackState("sess-1")
	if meta["watching"] != true {
		t.Errorf("a paused session should be marked watched: %v", meta)
	}
	if seq, _ := meta["last_seq"].(float64); seq <= 0 {
		t.Errorf("a paused session should record where rendering stopped: %v", meta)
	}
}

func TestSweepReadsEveryPageOfAPausedSessionsEvents(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)
	f.api.setState("sess-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-1", "", &pb.Suspended{Reason: api.ReasonIdle})
	f.poster.waitForPost(t, "paused this session")
	waitFor(t, "the paused session's stream to close", func() bool {
		return f.api.openStreams("sess-1") == 0
	})

	f.api.mu.Lock()
	f.api.eventPage = 1
	f.api.mu.Unlock()
	f.api.setState("sess-1", api.StateSuspended, api.StateEnded, noReason)
	f.api.emit("sess-1", "", &pb.SessionEnded{Reason: api.EndExpired})

	sweepCtx, stop := context.WithCancel(ctx)
	defer stop()
	go f.app.RunSweeper(sweepCtx, 20*time.Millisecond)

	f.poster.waitForPost(t, "approval ran out")
}

func TestSweepRendersTheReleaseCopy(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)
	f.api.setState("sess-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-1", "", &pb.Suspended{Reason: api.ReasonIdle})
	f.poster.waitForPost(t, "paused this session")
	waitFor(t, "the paused session's stream to close", func() bool {
		return f.api.openStreams("sess-1") == 0
	})

	f.api.emit("sess-1", "", &pb.Released{RetainedFor: durationpb.New(5 * time.Minute)})
	f.api.setState("sess-1", api.StateSuspended, api.StateEnded, noReason)
	f.api.emit("sess-1", "", &pb.SessionEnded{Reason: api.EndExpired})

	sweepCtx, stop := context.WithCancel(ctx)
	defer stop()
	go f.app.RunSweeper(sweepCtx, 20*time.Millisecond)

	f.poster.waitForUpdate(t, "released the workspace")
	waitFor(t, "the watch to end once the session can produce nothing further", func() bool {
		return f.slackState("sess-1")["watching"] != true
	})
}
