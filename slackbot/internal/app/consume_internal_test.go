package app

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
)

type failingSubscriber struct {
	harnessapipbconnect.HarnessAPIServiceClient
	err   error
	calls atomic.Int32
}

func (s *failingSubscriber) Subscribe(context.Context, *pb.SubscribeRequest) (*connect.ServerStreamForClient[pb.SubscribeResponse], error) {
	s.calls.Add(1)
	return nil, api.ToConnect(s.err)
}

func heldThread(a *App) *thread {
	t := &thread{sessionID: "s1", channel: "C1", threadTS: "1.0", teamID: "T1", ownerUserID: "U1"}
	a.registerThread(t)
	return t
}

func TestAConsumerThatCannotSubscribeReleasesItsThread(t *testing.T) {
	a := New(&failingSubscriber{err: api.ErrNotFound}, nil, nil)
	th := heldThread(a)

	done := make(chan struct{})
	go func() {
		defer close(done)
		a.consume(context.Background(), th, "", 0)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a consumer whose session is gone kept running")
	}
	if a.lookup("C1", "1.0", "T1", "U1") != nil {
		t.Fatal("a consumer that never subscribed left its thread registered")
	}
}

func TestAConsumerRetriesAnUnavailableHarnessUntilStopped(t *testing.T) {
	sub := &failingSubscriber{err: api.ErrUnavailable}
	a := New(sub, nil, nil)
	th := heldThread(a)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.consume(ctx, th, "", 0)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for sub.calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("an unavailable harness was not retried")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if a.lookup("C1", "1.0", "T1", "U1") == nil {
		t.Fatal("a consumer that is still retrying gave up its thread")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a stopped consumer kept retrying")
	}
	if a.lookup("C1", "1.0", "T1", "U1") != nil {
		t.Fatal("a stopped consumer left its thread registered")
	}
}

func TestAnswerPartsJoinExactlyUnlessAToolCallCameBetween(t *testing.T) {
	r := newRenderer()
	r.beginTurn("t1")
	r.appendPart("pre")
	if got := r.appendPart("fix"); got != "prefix" {
		t.Fatalf("a part split by size changed the text: %q", got)
	}
	r.toolCall()
	if got := r.appendPart("Checked the logs."); got != "prefix\n\nChecked the logs." {
		t.Fatalf("text after a tool call should start a new paragraph: %q", got)
	}
	r.toolCall()
	if got := r.appendPart("\nDone."); got != "prefix\n\nChecked the logs.\nDone." {
		t.Fatalf("a part that brings its own whitespace gets no more: %q", got)
	}
}

type countingPoster struct {
	Poster
	posts int
}

func (p *countingPoster) PostMessage(context.Context, string, ...slack.MsgOption) (string, error) {
	p.posts++
	return "prompt-ts", nil
}

func TestAReplayedPermissionRequestReusesItsPrompt(t *testing.T) {
	p := &countingPoster{}
	a := New(nil, p, nil)
	view := &pb.SessionView{Id: "s1", State: api.StateRunning}
	m := sessionMeta{ChannelID: "C1", ThreadTS: "168.1", UserID: "U1", TeamID: "T1"}
	request := &pb.PermissionRequest{RequestId: "r1", Summary: "Run a command"}
	ctx := context.Background()

	original := threadFromMeta(view, m)
	a.askPermission(ctx, original, request)
	restarted := threadFromMeta(view, original.meta())
	a.askPermission(ctx, restarted, request)

	if p.posts != 1 {
		t.Fatalf("one permission request produced %d prompts", p.posts)
	}
}

type flakyPoster struct {
	Poster
	failUpdates int
	updates     int
}

func (*flakyPoster) PostMessage(context.Context, string, ...slack.MsgOption) (string, error) {
	return "answer", nil
}

func (*flakyPoster) AddReaction(context.Context, string, string, string) error { return nil }

func (*flakyPoster) RemoveReaction(context.Context, string, string, string) error { return nil }

func (p *flakyPoster) UpdateMessage(context.Context, string, string, ...slack.MsgOption) (string, error) {
	p.updates++
	if p.updates <= p.failUpdates {
		return "", errors.New("connection lost")
	}
	return "answer", nil
}

func answerPart(final bool) *pb.Event {
	return &pb.Event{TurnId: "t1", Payload: &pb.Event_AgentMessage{
		AgentMessage: &pb.AgentMessage{PartId: "t1.1", Text: "the answer", Final: final},
	}}
}

func TestTheTurnEndRetriesAFinalAnswerSlackRejected(t *testing.T) {
	p := &flakyPoster{failUpdates: 1}
	a := New(nil, p, nil)
	th := &thread{channel: "C1", threadTS: "1.0", render: newRenderer()}
	ctx := context.Background()
	a.renderEvent(ctx, th, "", answerPart(false))
	a.renderEvent(ctx, th, "", answerPart(true))
	a.endTurn(ctx, th, "t1")
	if p.updates < 2 {
		t.Fatal("a final answer Slack rejected was never sent again")
	}
}

func TestAPermissionPromptIsKeptUntilItCloses(t *testing.T) {
	p := &flakyPoster{failUpdates: 1}
	a := New(nil, p, nil)
	th := &thread{channel: "C1", render: newRenderer()}
	th.render.permTS["r1"] = "prompt"
	resolved := &pb.PermissionResolved{RequestId: "r1"}
	ctx := context.Background()
	a.closePermission(ctx, th, resolved)
	a.closePermission(ctx, th, resolved)
	if p.updates != 2 {
		t.Fatalf("a prompt Slack failed to close was forgotten: %d close attempts", p.updates)
	}
}

func TestTSBefore(t *testing.T) {
	for in, want := range map[string]string{
		"1791496605.543209": "1791493005.543209",
		"1791496605":        "1791493005.000000",
		"168.2001":          "0.000000",
		"not a ts":          "not a ts",
	} {
		if got := tsBefore(in, time.Hour); got != want {
			t.Errorf("tsBefore(%q) = %q, want %q", in, got, want)
		}
	}
}

type partialFinalPoster struct {
	Poster
	updates, posts int
}

func (p *partialFinalPoster) UpdateMessage(context.Context, string, string, ...slack.MsgOption) (string, error) {
	p.updates++
	if p.updates == 1 {
		return "", errors.New("connection lost")
	}
	return "answer", nil
}

func (p *partialFinalPoster) PostMessage(context.Context, string, ...slack.MsgOption) (string, error) {
	p.posts++
	return "tail", nil
}

func (*partialFinalPoster) RemoveReaction(context.Context, string, string, string) error { return nil }

func TestARetriedFinalAnswerDoesNotRepostAPieceThatWentOut(t *testing.T) {
	p := &partialFinalPoster{}
	a := New(nil, p, nil)
	th := &thread{channel: "C1", threadTS: "1.0", render: newRenderer()}
	th.render.beginTurn("t1")
	th.render.curTS = "answer"
	ctx := context.Background()
	a.renderEvent(ctx, th, "", &pb.Event{TurnId: "t1", Payload: &pb.Event_AgentMessage{
		AgentMessage: &pb.AgentMessage{Text: strings.Repeat("a", maxMessageChars+1), Final: true},
	}})
	a.endTurn(ctx, th, "t1")
	if p.posts != 1 {
		t.Fatalf("the answer's second piece was posted %d times", p.posts)
	}
}

type interruptedHistory struct {
	harnessapipbconnect.HarnessAPIServiceClient
	calls int
}

func (h *interruptedHistory) ListEvents(_ context.Context, r *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	h.calls++
	if h.calls == 2 {
		return nil, errors.New("connection lost")
	}
	all := []*pb.Event{
		{Seq: 1, TurnId: "t1", Payload: &pb.Event_AgentMessage{AgentMessage: &pb.AgentMessage{PartId: "t1.1", Text: "First finding. "}}},
		{Seq: 2, TurnId: "t1", Payload: &pb.Event_AgentMessage{AgentMessage: &pb.AgentMessage{PartId: "t1.2", Text: "Second finding.", Final: true}}},
		{Seq: 3, TurnId: "t1", Payload: &pb.Event_TurnCompleted{TurnCompleted: &pb.TurnCompleted{}}},
	}
	out := &pb.ListEventsResponse{}
	for _, ev := range all {
		if ev.Seq > r.GetAfterSeq() {
			out.Events = append(out.Events, ev)
		}
	}
	if h.calls == 1 {
		out.Events = out.Events[:1]
	}
	return out, nil
}

type historyPoster struct {
	Poster
	answer string
}

func (*historyPoster) PostMessage(context.Context, string, ...slack.MsgOption) (string, error) {
	return "answer", nil
}

func (p *historyPoster) UpdateMessage(_ context.Context, channel, ts string, opts ...slack.MsgOption) (string, error) {
	_, values, err := slack.UnsafeApplyMsgOptions("token", channel, "https://slack.example/", opts...)
	if err != nil {
		return "", err
	}
	if ts == "answer" {
		p.answer = values.Get("text")
	}
	return ts, nil
}

func (*historyPoster) UpdateMessageDebounced(context.Context, string, string, ...slack.MsgOption) {}

func (*historyPoster) AddReaction(context.Context, string, string, string) error { return nil }

func (*historyPoster) RemoveReaction(context.Context, string, string, string) error { return nil }

func TestARetriedCatchupKeepsTheAnswersEarlierText(t *testing.T) {
	p := &historyPoster{}
	a := New(&interruptedHistory{}, p, nil)
	view := &pb.SessionView{Id: "s1", State: api.StateEnded, LastSeq: 3}
	m := sessionMeta{ChannelID: "C1", ThreadTS: "1.0", UserID: "U1", Watching: true}
	ctx := context.Background()
	if a.renderMissed(ctx, view, m, false) {
		t.Fatal("the second page should have failed")
	}
	m.LastSeq, m.AnswerTurn, m.AnswerTS = 1, "t1", "answer"
	if !a.renderMissed(ctx, view, m, false) {
		t.Fatal("the retry failed")
	}
	if !strings.Contains(p.answer, "First finding.") || !strings.Contains(p.answer, "Second finding.") {
		t.Fatalf("the retry lost part of the answer: %q", p.answer)
	}
}

type failedHeadPoster struct {
	Poster
	attempts, accepted int
}

func (p *failedHeadPoster) PostMessage(context.Context, string, ...slack.MsgOption) (string, error) {
	p.attempts++
	if p.attempts == 1 {
		return "", errors.New("connection lost")
	}
	p.accepted++
	if p.accepted == 1 {
		return "tail", nil
	}
	return "head", nil
}

func (*failedHeadPoster) UpdateMessage(_ context.Context, _, ts string, _ ...slack.MsgOption) (string, error) {
	return ts, nil
}

func TestARetriedFinalAnswerPostsTheMissingBeginning(t *testing.T) {
	p := &failedHeadPoster{}
	a := New(nil, p, nil)
	th := &thread{channel: "C1", threadTS: "1.0", render: newRenderer()}
	th.render.beginTurn("t1")
	text := strings.Repeat("a", maxMessageChars+1)
	ctx := context.Background()
	if a.showFinal(ctx, th, text) {
		t.Fatal("the first piece should have failed")
	}
	if !a.showFinal(ctx, th, text) {
		t.Fatal("the retry failed")
	}
	if p.accepted != 2 {
		t.Fatalf("only %d of the answer's two pieces reached Slack", p.accepted)
	}
}

type replayApprovalAPI struct {
	harnessapipbconnect.HarnessAPIServiceClient
	ends int
}

func (c *replayApprovalAPI) EndSession(context.Context, *pb.EndSessionRequest) (*pb.EndSessionResponse, error) {
	c.ends++
	return &pb.EndSessionResponse{}, nil
}

type replayApprovalPoster struct {
	Poster
	dms int
}

func (p *replayApprovalPoster) PostDM(context.Context, string, ...slack.MsgOption) (string, string, error) {
	p.dms++
	return "", "", errors.New("connection lost")
}

func (*replayApprovalPoster) PostMessage(context.Context, string, ...slack.MsgOption) (string, error) {
	return "status", nil
}

func TestAReplayedApprovalRequestDoesNotTouchARunningSession(t *testing.T) {
	c := &replayApprovalAPI{}
	p := &replayApprovalPoster{}
	a := New(c, p, nil)
	th := threadFromMeta(&pb.SessionView{Id: "s1", State: api.StateRunning, LastSeq: 3},
		sessionMeta{ChannelID: "C1", ThreadTS: "1.0", TeamID: "T1", UserID: "U1"})
	th.threadLink = "https://slack.example/thread"
	th.replayThrough = 3
	ctx := context.Background()

	a.renderEvent(ctx, th, "", &pb.Event{Seq: 1, Payload: &pb.Event_StateChanged{
		StateChanged: &pb.StateChanged{Old: api.StateLaunching, New: api.StateAwaitingApproval},
	}})
	a.renderEvent(ctx, th, "", &pb.Event{Seq: 2, Payload: &pb.Event_ApprovalRequired{
		ApprovalRequired: &pb.ApprovalRequired{ApprovalUrl: "https://approval.example/run"},
	}})
	if c.ends != 0 || p.dms != 0 {
		t.Fatalf("replaying an old approval request sent %d DMs and ended the running session %d times", p.dms, c.ends)
	}
}

type catchupApprovalAPI struct {
	harnessapipbconnect.HarnessAPIServiceClient
	ends int
}

func (c *catchupApprovalAPI) ListEvents(_ context.Context, r *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	all := []*pb.Event{
		{Seq: 2, Payload: &pb.Event_ApprovalRequired{ApprovalRequired: &pb.ApprovalRequired{ApprovalUrl: "https://approval.example/run"}}},
		{Seq: 3, Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{Old: api.StateRunning, New: api.StateSuspended}}},
	}
	out := &pb.ListEventsResponse{}
	for _, ev := range all {
		if ev.GetSeq() > r.GetAfterSeq() {
			out.Events = append(out.Events, ev)
		}
	}
	return out, nil
}

func (c *catchupApprovalAPI) EndSession(context.Context, *pb.EndSessionRequest) (*pb.EndSessionResponse, error) {
	c.ends++
	return &pb.EndSessionResponse{}, nil
}

func TestACatchupDoesNotReplayAnApprovalRequestIntoAPausedSession(t *testing.T) {
	c := &catchupApprovalAPI{}
	p := &replayApprovalPoster{}
	a := New(c, p, nil)
	view := &pb.SessionView{Id: "s1", State: api.StateSuspended, LastSeq: 3}
	m := sessionMeta{ChannelID: "C1", ThreadTS: "1.0", TeamID: "T1", UserID: "U1", LastSeq: 1}
	if !a.renderMissed(context.Background(), view, m, true) {
		t.Fatal("the catch-up failed")
	}
	if c.ends != 0 || p.dms != 0 {
		t.Fatalf("a catch-up sent %d approval DMs and ended the paused session %d times", p.dms, c.ends)
	}
}
