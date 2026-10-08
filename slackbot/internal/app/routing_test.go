package app_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/slackbot/internal/gateway"
)

func mentionIn(f *fixture, user, text string) gateway.MentionInvocation {
	ts := f.ts()
	f.record(userMessage(user, ts, text))
	return gateway.MentionInvocation{
		TeamID: "T1", UserID: user, ChannelID: "C1",
		MessageTS: ts, ThreadTS: threadRoot, OriginThreadTS: threadRoot,
		Text: text, Prompt: text,
	}
}

func replyIn(f *fixture, user, text string) gateway.ThreadMessage {
	ts := f.ts()
	f.record(userMessage(user, ts, text))
	return gateway.ThreadMessage{
		TeamID: "T1", UserID: user, ChannelID: "C1", ThreadTS: threadRoot, MessageTS: ts, Text: text,
	}
}

func liveThread(t *testing.T, f *fixture) {
	t.Helper()
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")
	f.api.setState("sess-1", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-1", api.StateRunning)
}

func TestSecondUserMentionJoinsTheSameThread(t *testing.T) {
	f := newFixture(t)
	liveThread(t, f)

	f.app.HandleMention(context.Background(), mentionIn(f, "U2", "what about staging?"))

	reqs := f.api.createRequests()
	if len(reqs) != 2 {
		t.Fatalf("CreateSession calls = %d, want 2 (the original and the join)", len(reqs))
	}
	first, second := reqs[0], reqs[1]
	if second.ConversationRef == first.ConversationRef {
		t.Errorf("the joiner reused the other person's session: %q", second.ConversationRef)
	}
	if !strings.HasPrefix(second.ConversationRef, "slack:C1:168.1:") {
		t.Errorf("a join must be bound to the thread it happened in: %q", second.ConversationRef)
	}
	if !strings.HasSuffix(second.ConversationRef, ":U2") {
		t.Errorf("a join's ref must name the joiner: %q", second.ConversationRef)
	}
	if second.ParentSessionId != "sess-1" {
		t.Errorf("parent_session_id = %q, want sess-1", second.ParentSessionId)
	}
	if !strings.Contains(second.ApprovalPrompt, "what about staging?") {
		t.Errorf("approval prompt lost the requester's own words: %q", second.ApprovalPrompt)
	}
	f.poster.waitForPost(t, "joined with their own agent")
	for _, p := range f.poster.allPosts() {
		if p.threadTS == "" && p.channel == "C1" {
			t.Errorf("a join must not post a thread root: %q", p.text)
		}
	}
	if !strings.Contains(second.SystemPromptAppendix, "workspace is yours alone") {
		t.Errorf("a joining session was not told it has its own workspace: %q", second.SystemPromptAppendix)
	}
}

func TestJoinIsOwnedByTheJoiner(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	liveThread(t, f)
	f.app.HandleMention(ctx, mentionIn(f, "U2", "what about staging?"))
	f.poster.waitForPost(t, "joined with their own agent")
	f.api.setState("sess-2", api.StatePending, api.StateRunning, noReason)
	waitForState(t, f, "sess-2", api.StateRunning)

	before := len(f.api.promptRequests())
	f.app.HandleMention(ctx, mentionIn(f, "U2", "go on"))
	waitFor(t, "the joiner's own next turn", func() bool {
		return len(f.api.promptRequests()) == before+1
	})
	if got := f.api.promptRequests()[before]; got.GetRef().GetSessionId() != "sess-2" {
		t.Errorf("the joiner's mention drove %q, want their own session", got.GetRef().GetSessionId())
	}
	if n := len(f.api.createRequests()); n != 2 {
		t.Errorf("a repeat mention launched another session: %d", n)
	}
}

func TestTwoPeopleJoinIndependently(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	liveThread(t, f)

	f.app.HandleMention(ctx, mentionIn(f, "U2", "what about staging?"))
	f.poster.waitForPost(t, "joined with their own agent")
	f.app.HandleMention(ctx, mentionIn(f, "U3", "and prod?"))

	reqs := f.api.createRequests()
	if len(reqs) != 3 {
		t.Fatalf("CreateSession calls = %d, want 3", len(reqs))
	}
	if reqs[1].ConversationRef == reqs[2].ConversationRef {
		t.Errorf("two people shared one session: %q", reqs[1].ConversationRef)
	}
}

func TestFirstMentionInAForeignThreadJoinsInPlace(t *testing.T) {
	f := newFixture(t)
	f.humanSaid("U9", "we should roll back")
	f.humanSaid("U8", "agreed")

	f.app.HandleMention(context.Background(), mentionIn(f, "U2", "can you help?"))

	reqs := f.api.createRequests()
	if len(reqs) != 1 {
		t.Fatalf("CreateSession calls = %d, want 1", len(reqs))
	}
	if reqs[0].ParentSessionId != "" {
		t.Errorf("a join with nobody to descend from has no parent session: %q", reqs[0].ParentSessionId)
	}
	if !strings.HasPrefix(reqs[0].ConversationRef, "slack:C1:168.1:") {
		t.Errorf("the session must run in the thread it was summoned into: %q", reqs[0].ConversationRef)
	}
	if !strings.Contains(reqs[0].InitialPrompt, "we should roll back") {
		t.Errorf("the discussion did not reach the agent: %q", reqs[0].InitialPrompt)
	}
	if !strings.Contains(reqs[0].ApprovalPrompt, "can you help?") ||
		!strings.Contains(reqs[0].ApprovalPrompt, "carried") {
		t.Errorf("approval prompt = %q, want the request plus a provenance clause", reqs[0].ApprovalPrompt)
	}
}

func TestJoinSeedCarriesTheVisibleThread(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	liveThread(t, f)
	f.api.emit("sess-1", "t1", &pb.AgentMessage{
		PartId: "t1.1", Text: "staged, waiting on approval", Final: true,
	})
	f.poster.waitForPost(t, "staged, waiting on approval")

	f.app.HandleMention(ctx, mentionIn(f, "U2", "what about staging?"))
	waitFor(t, "the join to be created", func() bool { return len(f.api.createRequests()) == 2 })

	seed := f.api.createRequests()[1].InitialPrompt
	for _, want := range []string{
		"joining a Slack thread",
		"[earlier request] ship it",
		"[earlier reply, quoted from Slack] staged, waiting on approval",
		"The request: what about staging?",
	} {
		if !strings.Contains(seed, want) {
			t.Errorf("the seed is missing %q:\n%s", want, seed)
		}
	}
	if strings.Contains(seed, "Getting ready") {
		t.Errorf("chrome must not be carried into a seed:\n%s", seed)
	}
	f.poster.waitForPost(t, "I've read the")
}

func TestOwnerMentionWhileLaunchingWaits(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	before := len(f.api.createRequests())
	f.app.HandleMention(ctx, mentionIn(f, "U1", "hurry up"))

	if got := len(f.api.createRequests()); got != before {
		t.Errorf("the owner's own mention launched a second session: %d -> %d", before, got)
	}
	assertEphemeral(t, f, "U1", "Not running yet")
}

func TestSecondUserMentionWhileLaunchingJoins(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.app.HandleMention(ctx, mention(f, "ship it"))
	f.poster.waitForPost(t, "Getting ready")

	f.app.HandleMention(ctx, mentionIn(f, "U2", "me too"))

	waitFor(t, "the join to be created", func() bool { return len(f.api.createRequests()) == 2 })
	reqs := f.api.createRequests()
	if !strings.HasPrefix(reqs[1].ConversationRef, "slack:C1:168.1:") {
		t.Errorf("the join left the thread: %q", reqs[1].ConversationRef)
	}
}

func TestRetemplatedJoinIsRefused(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	liveThread(t, f)
	f.resolver.bindings["C1"] = "audit"

	f.app.HandleMention(ctx, mentionIn(f, "U2", "what about staging?"))

	refusal := f.poster.waitForPost(t, "This channel's agent changed")
	if refusal.threadTS != threadRoot {
		t.Errorf("the refusal belongs in the thread it answers, got thread %q", refusal.threadTS)
	}
	for _, want := range []string{"*audit*", "*deploy*", "new top-level message"} {
		if !strings.Contains(refusal.text, want) {
			t.Errorf("the refusal is missing %q: %q", want, refusal.text)
		}
	}
	if n := len(f.api.createRequests()); n != 1 {
		t.Errorf("a retemplated join created %d sessions, want only the original", n)
	}
	for _, p := range f.poster.allPosts() {
		if p.threadTS == "" && p.channel == "C1" {
			t.Errorf("a retemplated join must not post a thread root: %q", p.text)
		}
	}
}

func TestRetemplatedJoinIsRefusedWithNothingInMemory(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": "168.1", "user_id": "U1", "team_id": "T1",
	})
	f.resolver.bindings["C1"] = "audit"

	f.app.HandleMention(context.Background(), mentionIn(f, "U2", "what about staging?"))

	f.poster.waitForPost(t, "This channel's agent changed")
	if n := len(f.api.createRequests()); n != 0 {
		t.Errorf("a retemplated join created %d sessions", n)
	}
}

func TestUnrevivableSessionStartsOverInTheSameThread(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.humanSaid("U1", "we should roll back")
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": "168.1", "user_id": "U1", "team_id": "T1",
	})
	f.api.promptErr = api.ErrNotRevivable

	f.app.HandleMessage(ctx, replyIn(f, "U1", "still there?"))

	f.poster.waitForPost(t, "couldn't pick this conversation back up")
	waitFor(t, "the fresh session", func() bool { return len(f.api.createRequests()) == 1 })
	if ends := f.api.endRequests(); len(ends) != 1 || ends[0].GetRef().GetSessionId() != "old-1" {
		t.Errorf("the unrevivable session was not ended first: %v", ends)
	}
	fresh := f.api.createRequests()[0]
	if fresh.ConversationRef != "slack:C1:168.1:T1:U1" {
		t.Errorf("the fresh session must be the owner's, in the same thread: %q", fresh.ConversationRef)
	}
	for _, want := range []string{"we should roll back", "The request: still there?"} {
		if !strings.Contains(fresh.InitialPrompt, want) {
			t.Errorf("the fresh session's seed is missing %q:\n%s", want, fresh.InitialPrompt)
		}
	}
	waitFor(t, "the fresh session's Slack state", func() bool { return f.slackState("sess-1") != nil })
	if f.slackState("sess-1")["multiplayer"] == true {
		t.Errorf("a plain reply in a solo thread must not leave its owner in a room: %v", f.slackState("sess-1"))
	}
	for _, p := range f.poster.allPosts() {
		if p.threadTS == "" && p.channel == "C1" {
			t.Errorf("starting over must not post a thread root: %q", p.text)
		}
	}
}

func TestFailedContinuationStartsOverInTheSameThread(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	liveThread(t, f)
	f.api.setState("sess-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-1", "", &pb.Suspended{Reason: api.ReasonIdle})
	f.poster.waitForPost(t, "paused this session")
	waitFor(t, "the paused session's stream to close", func() bool {
		return f.api.openStreams("sess-1") == 0
	})

	f.app.HandleMention(ctx, mentionIn(f, "U1", "still there?"))
	waitFor(t, "the continuation to be asked for", func() bool { return len(f.api.promptRequests()) == 1 })
	f.poster.waitForPost(t, "Picking this back up")
	f.api.setState("sess-1", api.StateLaunching, api.StateSuspended, api.ReasonResumeUnavailable)

	f.poster.waitForPost(t, "couldn't pick this conversation back up")
	waitFor(t, "the fresh session", func() bool { return len(f.api.createRequests()) == 2 })
	if ends := f.api.endRequests(); len(ends) != 1 || ends[0].GetRef().GetSessionId() != "sess-1" {
		t.Errorf("the session that could not continue was not ended first: %v", ends)
	}
	if ref := f.api.createRequests()[1].ConversationRef; ref != "slack:C1:168.1:T1:U1" {
		t.Errorf("the fresh session must be the owner's, in the same thread: %q", ref)
	}
	f.poster.waitForPost(t, "joined with their own agent")
	time.Sleep(50 * time.Millisecond)
	if got := f.poster.postsContaining("Finished"); len(got) != 0 {
		t.Errorf("the old session's ending was drawn after starting over: %q", got[0].text)
	}
}

func TestFailedContinuationStaysPausedForARetry(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	liveThread(t, f)
	f.api.setState("sess-1", api.StateRunning, api.StateSuspended, api.ReasonIdle)
	f.api.emit("sess-1", "", &pb.Suspended{Reason: api.ReasonIdle})
	f.poster.waitForPost(t, "paused this session")

	f.app.HandleMention(ctx, mentionIn(f, "U1", "still there?"))
	waitFor(t, "the continuation to be asked for", func() bool { return len(f.api.promptRequests()) == 1 })
	f.api.setState("sess-1", api.StateLaunching, api.StateSuspended, api.ReasonReviveFailed)

	f.poster.waitForPost(t, "this time")
	if n := len(f.api.endRequests()); n != 0 {
		t.Errorf("a retryable failure ended %d sessions", n)
	}
	if n := len(f.api.createRequests()); n != 1 {
		t.Errorf("a retryable failure created a session: %d creates", n)
	}

	f.app.HandleMention(ctx, mentionIn(f, "U1", "try again"))
	waitFor(t, "the retry to revive the same session", func() bool {
		reqs := f.api.promptRequests()
		return len(reqs) == 2 && reqs[1].GetRef().GetSessionId() == "sess-1"
	})
}

func TestUnadoptableSessionStartsOver(t *testing.T) {
	f := newFixture(t)
	f.api.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended)

	f.app.HandleMention(context.Background(), mentionIn(f, "U1", "still there?"))

	f.poster.waitForPost(t, "couldn't pick this conversation back up")
	waitFor(t, "the fresh session", func() bool { return len(f.api.createRequests()) == 1 })
	if ends := f.api.endRequests(); len(ends) != 1 || ends[0].GetRef().GetSessionId() != "old-1" {
		t.Errorf("the unadoptable session was not ended first: %v", ends)
	}
	if ref := f.api.createRequests()[0].ConversationRef; ref != "slack:C1:168.1:T1:U1" {
		t.Errorf("the fresh session must be the owner's, in the same thread: %q", ref)
	}
	if n := len(f.api.promptRequests()); n != 0 {
		t.Errorf("an unadoptable session was prompted %d times", n)
	}
}

func TestChannelBoundToAnUnavailableTemplate(t *testing.T) {
	f := newFixture(t)
	f.api.createErr = api.ErrForbidden

	f.app.HandleMention(context.Background(), mention(f, "ship it"))

	f.poster.waitForPost(t, "no such agent exists")
	f.poster.waitForPost(t, "configuration problem")
}

func TestForeignWorkspaceMentionIsIgnored(t *testing.T) {
	f := newFixture(t)
	in := mention(f, "ship it")
	in.TeamID = "T-OTHER"

	f.app.HandleMention(context.Background(), in)

	if got := len(f.api.createRequests()); got != 0 {
		t.Errorf("a mention from another workspace launched %d sessions", got)
	}
}

func TestStartingOverQuotesTheTriggeringReplyOnce(t *testing.T) {
	for name, setup := range map[string]func(t *testing.T, f *fixture){
		"held in memory": func(t *testing.T, f *fixture) {
			liveThread(t, f)
			f.api.setState("sess-1", api.StateRunning, api.StateSuspended, noReason)
			waitForState(t, f, "sess-1", api.StateSuspended)
		},
		"after a restart": func(t *testing.T, f *fixture) {
			f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended, map[string]any{
				"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1",
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			setup(t, f)
			f.api.promptErr = api.ErrNotRevivable
			before := len(f.api.createRequests())
			f.app.HandleMessage(context.Background(), replyIn(f, "U1", "still there?"))
			waitFor(t, "the fresh session", func() bool { return len(f.api.createRequests()) > before })
			seed := f.api.createRequests()[before].InitialPrompt
			if n := strings.Count(seed, "still there?"); n != 1 {
				t.Errorf("the triggering reply appears %d times in the fresh session's seed, want 1:\n%s", n, seed)
			}
		})
	}
}

func TestAFailedContinuationClosesItsStream(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1",
	})
	f.app.HandleMention(context.Background(), mentionIn(f, "U1", "continue"))
	waitFor(t, "the continuation's turn", func() bool { return len(f.api.promptRequests()) == 1 })
	waitFor(t, "the session's stream", func() bool { return f.api.openStreams("old-1") == 1 })

	f.api.setState("old-1", api.StateSuspended, api.StateLaunching, noReason)
	f.api.emit("old-1", "t1", &pb.TurnFailed{Reason: "the workspace did not come back"})
	f.api.setState("old-1", api.StateLaunching, api.StateSuspended, api.ReasonReviveFailed)
	f.poster.waitForPost(t, "this time")

	waitFor(t, "the failed continuation's stream to close", func() bool { return f.api.openStreams("old-1") == 0 })
	if meta := f.slackState("old-1"); meta["watching"] != true {
		t.Errorf("a session paused again should be watched for its ending: %v", meta)
	}
}

func TestAFailedStateReadDoesNotTurnDiscussionIntoATurn(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1",
		"multiplayer": true, "last_seen_ts": threadRoot,
	})
	f.poster.failNextReads(1)

	f.app.HandleMessage(context.Background(), replyIn(f, "U1", "this is discussion, not a request"))

	if n := len(f.api.promptRequests()); n != 0 {
		t.Fatalf("a plain reply in a paused multiplayer thread started %d turns", n)
	}
	if n := len(f.api.createRequests()); n != 0 {
		t.Fatalf("a plain reply in a paused multiplayer thread started %d sessions", n)
	}
}

func TestASlackReadFailureLeavesAPausedSessionPaused(t *testing.T) {
	f := newFixture(t)
	f.seedSession("old-1", "slack:C1:168.1:T1:U1", api.StateSuspended, map[string]any{
		"v": 1, "channel_id": "C1", "thread_ts": threadRoot, "user_id": "U1", "team_id": "T1",
	})
	f.poster.failNextReads(1000)

	f.app.HandleMention(context.Background(), mentionIn(f, "U1", "continue"))

	f.poster.waitForPost(t, "couldn't send that to the agent")
	if ends := f.api.endRequests(); len(ends) != 0 {
		t.Errorf("a failed Slack read ended the paused session: %v", ends)
	}
	if n := len(f.api.createRequests()); n != 0 {
		t.Errorf("a failed Slack read started %d fresh sessions", n)
	}
}

func userMessage(user, ts, text string) slack.Message {
	m := slack.Message{}
	m.User = user
	m.Text = text
	m.Timestamp = ts
	m.ThreadTimestamp = "168.1"
	return m
}

func assertEphemeral(t *testing.T, f *fixture, user, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		for _, e := range f.poster.ephemeralTo(user) {
			if strings.Contains(e.text, want) {
				return
			}
		}
		if time.Now().After(deadline) {
			var seen []string
			for _, e := range f.poster.ephemeralTo(user) {
				seen = append(seen, e.text)
			}
			t.Fatalf("no ephemeral to %s containing %q; saw %v", user, want, seen)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
