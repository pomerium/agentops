package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/slack-go/slack"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/agentops/harness/api"
	"github.com/pomerium/agentops/harness/api/client"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/api/server"
	"github.com/pomerium/agentops/slackbot/internal/channelmap"
)

type fakeAPI struct {
	mu           sync.Mutex
	creates      []*pb.CreateSessionRequest
	prompts      []*pb.PromptRequest
	perms        []*pb.RespondPermissionRequest
	ends         []*pb.EndSessionRequest
	sessions     map[string]*pb.SessionView
	updated      map[string]time.Time
	byConv       map[string]string
	feeds        map[string]*fakeFeed
	seq          int64
	createErr    error
	promptErr    error
	templates    []string
	templatesErr error
	subscribeErr []error
	eventPage    int
}

var _ harnessapipbconnect.HarnessAPIServiceHandler = (*fakeAPI)(nil)

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		sessions:  map[string]*pb.SessionView{},
		updated:   map[string]time.Time{},
		byConv:    map[string]string{},
		feeds:     map[string]*fakeFeed{},
		templates: []string{testTemplate},
	}
}

func (f *fakeAPI) serve(t *testing.T) harnessapipbconnect.HarnessAPIServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(harnessapipbconnect.NewHarnessAPIServiceHandler(f,
		connect.WithInterceptors(server.ErrorInterceptor())))
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
	})
	c, err := client.New(srv.URL, client.WithRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *fakeAPI) lookup(ref *pb.SessionRef) string {
	if id := ref.GetSessionId(); id != "" {
		return id
	}
	return f.byConv[ref.GetConversationRef()]
}

func (f *fakeAPI) CreateSession(_ context.Context, req *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	if id, ok := f.byConv[req.GetConversationRef()]; ok && api.Live(f.sessions[id].GetState()) {
		return nil, api.ErrConflict
	}
	f.creates = append(f.creates, req)
	id := fmt.Sprintf("sess-%d", len(f.creates))
	view := &pb.SessionView{
		Id: id, ConversationRef: req.GetConversationRef(),
		State: api.StatePending, Template: req.GetTemplate(),
	}
	f.sessions[id] = view
	f.byConv[req.GetConversationRef()] = id
	f.feeds[id] = newFakeFeed()
	f.updated[id] = time.Now()
	return &pb.CreateSessionResponse{Session: proto.CloneOf(view)}, nil
}

func (f *fakeAPI) Prompt(_ context.Context, req *pb.PromptRequest) (*pb.PromptResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.promptErr != nil {
		return nil, f.promptErr
	}
	f.prompts = append(f.prompts, req)
	return &pb.PromptResponse{TurnId: fmt.Sprintf("t%d", len(f.prompts))}, nil
}

func (f *fakeAPI) RespondPermission(_ context.Context, req *pb.RespondPermissionRequest) (*pb.RespondPermissionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.perms = append(f.perms, req)
	return &pb.RespondPermissionResponse{}, nil
}

func (f *fakeAPI) EndSession(_ context.Context, req *pb.EndSessionRequest) (*pb.EndSessionResponse, error) {
	f.mu.Lock()
	f.ends = append(f.ends, req)
	id := f.lookup(req.GetRef())
	view, ok := f.sessions[id]
	from := view.GetState()
	f.mu.Unlock()
	if !ok || !api.Live(from) {
		return &pb.EndSessionResponse{}, nil
	}
	f.setState(id, from, api.StateEnded, noReason)
	f.emit(id, "", &pb.SessionEnded{Reason: api.EndEnded})
	return &pb.EndSessionResponse{}, nil
}

func (f *fakeAPI) GetSession(_ context.Context, req *pb.GetSessionRequest) (*pb.GetSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ref := req.GetRef()
	view, ok := f.sessions[f.lookup(ref)]
	if !ok {
		return nil, api.ErrNotFound
	}
	if ref.GetConversationRef() != "" && !ref.GetIncludeTerminal() && !api.Live(view.GetState()) {
		return nil, api.ErrNotFound
	}
	return &pb.GetSessionResponse{Session: proto.CloneOf(view)}, nil
}

func (f *fakeAPI) ListSessions(_ context.Context, req *pb.ListSessionsRequest) (*pb.ListSessionsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &pb.ListSessionsResponse{}
	for id, v := range f.sessions {
		if req.GetLiveOnly() && !api.Live(v.GetState()) {
			continue
		}
		if f.updated[id].Before(api.Time(req.GetUpdatedSince())) {
			continue
		}
		out.Sessions = append(out.Sessions, proto.CloneOf(v))
	}
	return out, nil
}

func (f *fakeAPI) ListTemplates(context.Context, *pb.ListTemplatesRequest) (*pb.ListTemplatesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.templatesErr != nil {
		return nil, f.templatesErr
	}
	out := &pb.ListTemplatesResponse{}
	for _, name := range f.templates {
		out.Templates = append(out.Templates, &pb.TemplateSummary{Name: name})
	}
	return out, nil
}

func (f *fakeAPI) ListEvents(_ context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	f.mu.Lock()
	feed := f.feeds[f.lookup(req.GetRef())]
	page := f.eventPage
	f.mu.Unlock()
	if feed == nil {
		return nil, api.ErrNotFound
	}
	events := feed.after(req.GetAfterSeq())
	if page > 0 && len(events) > page {
		events = events[:page]
	}
	return &pb.ListEventsResponse{Events: events}, nil
}

func (f *fakeAPI) failSubscribes(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribeErr = append(f.subscribeErr, errs...)
}

func (f *fakeAPI) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream *connect.ServerStream[pb.SubscribeResponse]) error {
	f.mu.Lock()
	if len(f.subscribeErr) > 0 {
		err := f.subscribeErr[0]
		f.subscribeErr = f.subscribeErr[1:]
		f.mu.Unlock()
		return err
	}
	feed, ok := f.feeds[req.GetRef().GetSessionId()]
	f.mu.Unlock()
	if !ok {
		return api.ErrNotFound
	}
	sub := feed.subscribe(req.GetAfterSeq())
	defer sub.Close()
	return server.Stream(ctx, stream, sub.events())
}

func (f *fakeAPI) openStreams(sessionID string) int {
	f.mu.Lock()
	feed := f.feeds[sessionID]
	f.mu.Unlock()
	if feed == nil {
		return 0
	}
	return feed.openSubs()
}

func (f *fakeAPI) state(sessionID string) (api.SessionState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	view, ok := f.sessions[sessionID]
	return view.GetState(), ok
}

func (f *fakeAPI) emit(sessionID, turnID string, payload proto.Message) {
	f.mu.Lock()
	f.seq++
	ev := &pb.Event{SessionId: sessionID, Seq: f.seq, TurnId: turnID, Timestamp: timestamppb.New(fakeEventTime)}
	setPayload(ev, payload)
	if view, ok := f.sessions[sessionID]; ok {
		f.updated[sessionID] = time.Now()
		view.LastSeq = ev.Seq
		if p := ev.GetStateChanged(); p != nil {
			view.State = p.GetNew()
		}
	}
	feed := f.feeds[sessionID]
	f.mu.Unlock()
	if feed != nil {
		feed.publish(ev)
	}
}

var fakeEventTime = time.Unix(160, 0)

func tsValue(ts string) float64 {
	v, _ := strconv.ParseFloat(ts, 64)
	return v
}

func setPayload(ev *pb.Event, payload proto.Message) {
	m := ev.ProtoReflect()
	fields := m.Descriptor().Oneofs().ByName("payload").Fields()
	want := payload.ProtoReflect().Descriptor().FullName()
	for i := range fields.Len() {
		if fd := fields.Get(i); fd.Message().FullName() == want {
			m.Set(fd, protoreflect.ValueOfMessage(payload.ProtoReflect()))
			return
		}
	}
	panic(fmt.Sprintf("%s is not an event payload", want))
}

const noReason = pb.Reason_REASON_UNSPECIFIED

func (f *fakeAPI) setState(sessionID string, from, to api.SessionState, reason api.Reason) {
	f.emit(sessionID, "", &pb.StateChanged{Old: from, New: to, Reason: reason})
}

func (f *fakeAPI) seedSession(id, convRef string, state api.SessionState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[id] = &pb.SessionView{
		Id: id, ConversationRef: convRef,
		State: state, Template: testTemplate,
	}
	f.byConv[convRef] = id
	f.feeds[id] = newFakeFeed()
	f.updated[id] = time.Now()
}

func (f *fakeAPI) createRequests() []*pb.CreateSessionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.CreateSessionRequest(nil), f.creates...)
}

func (f *fakeAPI) promptRequests() []*pb.PromptRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.PromptRequest(nil), f.prompts...)
}

func (f *fakeAPI) endRequests() []*pb.EndSessionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.EndSessionRequest(nil), f.ends...)
}

func (f *fakeAPI) permissionResponses() []*pb.RespondPermissionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.RespondPermissionRequest(nil), f.perms...)
}

type fakeFeed struct {
	mu      sync.Mutex
	subs    []*fakeSub
	history []*pb.Event
}

func newFakeFeed() *fakeFeed { return &fakeFeed{} }

func (f *fakeFeed) subscribe(afterSeq int64) *fakeSub {
	f.mu.Lock()
	s := &fakeSub{ch: make(chan *pb.Event, len(f.history)+256), done: make(chan struct{})}
	for _, ev := range f.history {
		if ev.Seq > afterSeq {
			s.send(ev)
		}
	}
	f.subs = append(f.subs, s)
	f.mu.Unlock()
	return s
}

func (f *fakeFeed) after(afterSeq int64) []*pb.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*pb.Event
	for _, ev := range f.history {
		if ev.Seq > afterSeq {
			out = append(out, ev)
		}
	}
	return out
}

func (f *fakeFeed) openSubs() int {
	f.mu.Lock()
	subs := append([]*fakeSub(nil), f.subs...)
	f.mu.Unlock()
	n := 0
	for _, s := range subs {
		s.mu.Lock()
		if !s.closed {
			n++
		}
		s.mu.Unlock()
	}
	return n
}

func (f *fakeFeed) publish(ev *pb.Event) {
	f.mu.Lock()
	f.history = append(f.history, ev)
	subs := append([]*fakeSub(nil), f.subs...)
	f.mu.Unlock()
	for _, s := range subs {
		s.send(ev)
	}
}

type fakeSub struct {
	mu     sync.Mutex
	sendMu sync.Mutex
	ch     chan *pb.Event
	done   chan struct{}
	closed bool
}

func (s *fakeSub) events() <-chan *pb.Event { return s.ch }

func (s *fakeSub) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.done)
	s.mu.Unlock()
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	close(s.ch)
}

func (s *fakeSub) send(ev *pb.Event) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	select {
	case <-s.done:
		return
	default:
	}
	select {
	case <-s.done:
		return
	case s.ch <- ev:
	}
	if ev.GetSessionEnded() == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.done)
		close(s.ch)
	}
}

type fakeResolver struct {
	bindings map[string]string
}

func boundResolver(template string) *fakeResolver {
	return &fakeResolver{bindings: map[string]string{"C1": template}}
}

func (f *fakeResolver) TemplateFor(_ context.Context, channelID string) (string, error) {
	if name, ok := f.bindings[channelID]; ok {
		return name, nil
	}
	return "", fmt.Errorf("%w: %q", channelmap.ErrChannelNotBound, channelID)
}

const testTemplate = "deploy"

type postRecord struct {
	channel  string
	threadTS string
	ts       string
	text     string
	meta     slack.SlackMetadata
	blocks   string
}

type updateRecord struct {
	channel   string
	ts        string
	text      string
	meta      slack.SlackMetadata
	blocks    string
	debounced bool
}

type ephemeralPost struct {
	channel string
	userID  string
	text    string
}

type reactionOp struct {
	add     bool
	channel string
	ts      string
	emoji   string
}

type fakePoster struct {
	clock     *tsClock
	mu        sync.Mutex
	posts     []postRecord
	updates   []updateRecord
	deletes   []updateRecord
	ephemeral []ephemeralPost
	responds  []string
	reactions []reactionOp
	replies   []slack.Message
	posted    []slack.Message
	failReads int
	tooLong   bool
}

func (p *fakePoster) PostMessage(_ context.Context, channelID string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ts := p.clock.next()
	sent := renderOptions(channelID, opts)
	p.posts = append(p.posts, postRecord{
		channel: channelID, threadTS: sent.threadTS, ts: ts,
		text: sent.text, meta: sent.meta, blocks: sent.blocks,
	})
	if sent.threadTS != "" {
		p.posted = append(p.posted, botMessage(ts, sent.threadTS, sent.text, sent.meta))
	}
	return ts, nil
}

func (p *fakePoster) PostDM(ctx context.Context, userID string, opts ...slack.MsgOption) (string, string, error) {
	ts, err := p.PostMessage(ctx, userID, opts...)
	if err != nil {
		return "", "", err
	}
	return "D" + userID, ts, nil
}

func botMessage(ts, threadTS, text string, meta slack.SlackMetadata) slack.Message {
	m := slack.Message{}
	m.BotID = "B1"
	m.User = "UBOT"
	m.Timestamp = ts
	m.ThreadTimestamp = threadTS
	m.Text = text
	m.Metadata = meta
	return m
}

func (p *fakePoster) PostEphemeral(_ context.Context, channelID, userID string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sent := renderOptions(channelID, opts)
	p.ephemeral = append(p.ephemeral, ephemeralPost{channel: channelID, userID: userID, text: sent.text})
	return "ephemeral", nil
}

func (p *fakePoster) UpdateMessage(_ context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if strings.HasPrefix(channelID, "U") {
		return "", errors.New("message_not_found")
	}
	sent := renderOptions(channelID, opts)
	p.updates = append(p.updates, updateRecord{
		channel: channelID, ts: ts, text: sent.text, meta: sent.meta, blocks: sent.blocks,
	})
	for i := range p.posted {
		if p.posted[i].Timestamp == ts {
			p.posted[i].Text = sent.text
			p.posted[i].Metadata = sent.meta
			p.posted[i].Edited = &slack.Edited{User: "UBOT", Timestamp: p.clock.next()}
		}
	}
	return ts, nil
}

func (p *fakePoster) UpdateMessageDebounced(_ context.Context, channelID, ts string, opts ...slack.MsgOption) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sent := renderOptions(channelID, opts)
	p.updates = append(p.updates, updateRecord{
		channel: channelID, ts: ts, text: sent.text, meta: sent.meta, blocks: sent.blocks,
		debounced: true,
	})
}

func (p *fakePoster) DeleteMessage(_ context.Context, channelID, ts string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deletes = append(p.deletes, updateRecord{channel: channelID, ts: ts})
	for i := range p.posted {
		if p.posted[i].Timestamp == ts {
			p.posted = append(p.posted[:i], p.posted[i+1:]...)
			break
		}
	}
	return nil
}

func (p *fakePoster) Respond(_ context.Context, _ string, _ bool, text string, _ []slack.Block) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.responds = append(p.responds, text)
	return nil
}

func (p *fakePoster) AddReaction(_ context.Context, channelID, ts, emoji string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reactions = append(p.reactions, reactionOp{add: true, channel: channelID, ts: ts, emoji: emoji})
	return nil
}

func (p *fakePoster) RemoveReaction(_ context.Context, channelID, ts, emoji string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reactions = append(p.reactions, reactionOp{channel: channelID, ts: ts, emoji: emoji})
	return nil
}

func (p *fakePoster) ThreadReplies(_ context.Context, _, threadTS, since string, max int) ([]slack.Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failReads > 0 {
		p.failReads--
		return nil, errors.New("slack is unavailable")
	}
	if p.tooLong && since == "" {
		return nil, errors.New("the thread has more replies than one read covers")
	}
	out := append([]slack.Message(nil), p.replies...)
	for _, m := range p.posted {
		if m.ThreadTimestamp == threadTS {
			out = append(out, m)
		}
	}
	if since != "" {
		kept := out[:0]
		for _, m := range out {
			if tsValue(m.Timestamp) >= tsValue(since) {
				kept = append(kept, m)
			}
		}
		out = kept
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	if len(out) > max {
		out = out[len(out)-max:]
	}
	return out, nil
}

func (p *fakePoster) failNextReads(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failReads = n
}

func (p *fakePoster) Permalink(_ context.Context, channelID, ts string) (string, error) {
	return "https://slack.example/" + channelID + "/" + ts, nil
}

type sentMessage struct {
	text     string
	threadTS string
	meta     slack.SlackMetadata
	blocks   string
}

func renderOptions(channelID string, opts []slack.MsgOption) sentMessage {
	_, values, err := slack.UnsafeApplyMsgOptions("token", channelID, "https://slack.example/", opts...)
	if err != nil {
		return sentMessage{}
	}
	sent := sentMessage{
		text:     values.Get("text"),
		threadTS: values.Get("thread_ts"),
		blocks:   values.Get("blocks"),
	}
	if raw := values.Get("metadata"); raw != "" {
		_ = json.Unmarshal([]byte(raw), &sent.meta)
	}
	return sent
}

func (p *fakePoster) waitForPost(t *testing.T, want string) postRecord {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		p.mu.Lock()
		for _, r := range p.posts {
			if strings.Contains(r.text, want) {
				p.mu.Unlock()
				return r
			}
		}
		var seen []string
		for _, r := range p.posts {
			seen = append(seen, r.text)
		}
		p.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a post containing %q; saw %q", want, seen)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *fakePoster) waitForUpdate(t *testing.T, want string) updateRecord {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		p.mu.Lock()
		for _, r := range p.updates {
			if strings.Contains(r.text, want) {
				p.mu.Unlock()
				return r
			}
		}
		var seen []string
		for _, r := range p.updates {
			seen = append(seen, r.text)
		}
		p.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for an update containing %q; saw %q", want, seen)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *fakePoster) allPosts() []postRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]postRecord(nil), p.posts...)
}

func (p *fakePoster) postsContaining(want string) []postRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []postRecord
	for _, r := range p.posts {
		if strings.Contains(r.text, want) {
			out = append(out, r)
		}
	}
	return out
}

func (p *fakePoster) deleteRecords() []updateRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]updateRecord(nil), p.deletes...)
}

func (p *fakePoster) updatesTo(ts string) []updateRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []updateRecord
	for _, u := range p.updates {
		if u.ts == ts {
			out = append(out, u)
		}
	}
	return out
}

func (p *fakePoster) reactionsOn(ts, emoji string) []reactionOp {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []reactionOp
	for _, r := range p.reactions {
		if r.ts == ts && r.emoji == emoji {
			out = append(out, r)
		}
	}
	return out
}

func (p *fakePoster) ephemeralTo(userID string) []ephemeralPost {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []ephemeralPost
	for _, e := range p.ephemeral {
		if e.userID == userID {
			out = append(out, e)
		}
	}
	return out
}

func TestTheFakeListsSessionsChangedSinceATime(t *testing.T) {
	f := newFakeAPI()
	f.seedSession("s1", "slack:C1:168.1:T1:U1", api.StateSuspended)
	since := time.Now().Add(-time.Second)
	f.setState("s1", api.StateSuspended, api.StateEnded, noReason)

	got, err := f.ListSessions(context.Background(), &pb.ListSessionsRequest{UpdatedSince: api.Timestamp(since)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.GetSessions()) != 1 {
		t.Fatalf("got %d sessions, want the one that changed", len(got.GetSessions()))
	}
}

func TestTheFakeReplaysMoreThanOneBuffer(t *testing.T) {
	feed := newFakeFeed()
	for seq := int64(1); seq <= 300; seq++ {
		feed.publish(&pb.Event{Seq: seq})
	}
	done := make(chan *fakeSub, 1)
	go func() { done <- feed.subscribe(0) }()
	select {
	case sub := <-done:
		defer sub.Close()
		for seq := int64(1); seq <= 300; seq++ {
			if got := <-sub.events(); got.GetSeq() != seq {
				t.Fatalf("got seq %d, want %d", got.GetSeq(), seq)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("subscribe blocked before its reader could start")
	}
}

func TestClosingTheFakeUnblocksAFullFeed(t *testing.T) {
	feed := newFakeFeed()
	sub := feed.subscribe(0)
	for seq := 1; seq <= cap(sub.ch); seq++ {
		feed.publish(&pb.Event{Seq: int64(seq)})
	}
	go feed.publish(&pb.Event{Seq: 1000})
	time.Sleep(20 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		sub.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close could not interrupt a send blocked on a full feed")
	}
}
