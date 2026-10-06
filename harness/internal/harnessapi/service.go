package harnessapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	acp "github.com/coder/acp-go-sdk"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/api/server"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	"github.com/pomerium/agentops/harness/internal/agenttemplate"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

type LiveSession interface {
	ID() string
	Prompt(ctx context.Context, text string) (acp.StopReason, error)
	Cancel(ctx context.Context) error
	Close() error
}

type Launcher interface {
	Prepare(ctx context.Context, spec sandbox.LaunchSpec) (*sandbox.Prepared, error)
	Expect(runID string, prepared *sandbox.Prepared, opts ...sandbox.SupervisionOption) (*sandbox.Attachment, error)
	Activate(ctx context.Context, sink sandbox.EventSink, prepared *sandbox.Prepared, att *sandbox.Attachment) (LiveSession, error)
	Teardown(ctx context.Context, claimName string) error

	Suspend(ctx context.Context, claimName string) error

	Revive(ctx context.Context, claimName string, spec sandbox.LaunchSpec) (*sandbox.Prepared, error)

	ExtendLease(ctx context.Context, claimName string) (time.Time, error)
	LeaseLength() time.Duration
}

type Templates interface {
	Resolve(ctx context.Context, name string) (*v1alpha1.AgentTemplate, error)
	List(ctx context.Context) ([]v1alpha1.AgentTemplate, error)
	ClientBinding(ctx context.Context, subject string) (*v1alpha1.ClientBinding, error)
}

const runPollInterval = 5 * time.Second

type Option func(*options)

type options struct {
	sessionTTL        time.Duration
	sessionIdleTTL    time.Duration
	idleWarnLead      time.Duration
	suspendedTTL      time.Duration
	permissionTimeout time.Duration
	runTTL            time.Duration
	logger            *slog.Logger
}

func WithSessionTTL(d time.Duration) Option { return func(o *options) { o.sessionTTL = d } }

func WithSessionIdleTTL(d time.Duration) Option { return func(o *options) { o.sessionIdleTTL = d } }

func WithIdleWarnLead(d time.Duration) Option { return func(o *options) { o.idleWarnLead = d } }

func WithSuspendedTTL(d time.Duration) Option { return func(o *options) { o.suspendedTTL = d } }

func WithPermissionTimeout(d time.Duration) Option {
	return func(o *options) { o.permissionTimeout = d }
}

func WithRunTTL(d time.Duration) Option { return func(o *options) { o.runTTL = d } }

func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

func (c *options) applyDefaults() {
	if c.sessionTTL == 0 {
		c.sessionTTL = time.Hour
	}

	if c.sessionIdleTTL == 0 {
		c.sessionIdleTTL = 15 * time.Minute
	}
	if c.idleWarnLead == 0 {
		c.idleWarnLead = 2 * time.Minute
	}
	if c.suspendedTTL == 0 {
		c.suspendedTTL = 24 * time.Hour
	}
	if c.permissionTimeout == 0 {
		c.permissionTimeout = 5 * time.Minute
	}
	if c.runTTL == 0 {
		c.runTTL = 15 * time.Minute
	}
}

type Service struct {
	cfg       options
	store     sessionstore.Sessions
	events    EventLog
	launcher  Launcher
	templates Templates
	runs      agenticrun.RunClient
	log       *slog.Logger
	tel       *telemetry.Component

	rates *rateLimiters

	resolved *resolvedRequests

	prompts *promptKeys

	mu        sync.Mutex
	live      map[string]*binding
	launching map[string]*launchSlot
}

var _ harnessapipbconnect.HarnessAPIServiceHandler = (*Service)(nil)

func New(st sessionstore.Sessions, ev EventLog, l Launcher, t Templates, runs agenticrun.RunClient, opts ...Option) *Service {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()
	log := cfg.logger
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		cfg: cfg, store: st, events: ev, launcher: l, templates: t, runs: runs, log: log,
		tel:       telemetry.New(log, "harnessapi", slog.LevelDebug),
		rates:     newRateLimiters(),
		resolved:  newResolvedRequests(),
		prompts:   newPromptKeys(),
		live:      map[string]*binding{},
		launching: map[string]*launchSlot{},
	}
}

type binding struct {
	sessionID string
	claimName string
	session   LiveSession
	sink      *logSink

	ready chan struct{}

	busy atomic.Int32

	turn sync.Mutex

	lastActivity atomic.Int64

	idleWarned atomic.Bool

	leaseUntil atomic.Int64
}

func (b *binding) touch() {
	b.lastActivity.Store(time.Now().UnixNano())
	b.idleWarned.Store(false)
}

func (b *binding) idleFor(now time.Time) time.Duration {
	if b.busy.Load() > 0 {
		return 0
	}
	return now.Sub(time.Unix(0, b.lastActivity.Load()))
}

func (s *Service) CreateSession(ctx context.Context, req *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return nil, err
	}
	switch {
	case req.GetTemplate() == "":
		return nil, api.Errorf(api.ErrInvalidArgument, "template is required")
	case req.GetConversationRef() == "":
		return nil, api.Errorf(api.ErrInvalidArgument, "conversation_ref is required")
	case strings.TrimSpace(req.GetApprovalPrompt()) == "":

		return nil, api.Errorf(api.ErrInvalidArgument,
			"approval_prompt is required: it is what the human approver reads")
	}
	ctx = telemetry.With(ctx, "client_id", clientID, "template", req.GetTemplate())

	clientBinding, err := s.authorize(ctx, clientID)
	if err != nil {
		return nil, err
	}

	if err := s.checkQuotas(ctx, clientBinding, clientID, spend{newSession: true}); err != nil {
		return nil, err
	}

	tmpl, err := s.allowedTemplate(ctx, clientBinding, clientID, req.GetTemplate())
	if err != nil {
		return nil, err
	}

	snapshot, err := json.Marshal(templateSnapshot{
		Name:            tmpl.Name,
		Spec:            tmpl.Spec,
		PromptAppendix:  req.GetSystemPromptAppendix(),
		SnapshotVersion: snapshotVersion,
	})
	if err != nil {
		return nil, api.Errorf(api.ErrUnavailable, "snapshot template %q: %v", tmpl.Name, err)
	}

	row := sessionstore.Session{
		ID:              newID(),
		ClientID:        clientID,
		ConversationRef: req.GetConversationRef(),
		TemplateName:    tmpl.Name,
		TemplateSpec:    string(snapshot),
		InitialPrompt:   req.GetApprovalPrompt(),
		ParentSessionID: req.GetParentSessionId(),
		Status:          api.StatePending,
	}
	if err := s.store.CreateSession(ctx, row); err != nil {
		if errors.Is(err, sessionstore.ErrConflict) {
			return nil, api.Errorf(api.ErrConflict, "conversation %q already has a live session", req.GetConversationRef())
		}
		return nil, api.Errorf(api.ErrUnavailable, "create session: %v", err)
	}
	ctx = telemetry.With(ctx, "session_id", row.ID)
	s.emit(ctx, row.ID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{New: api.StatePending}}})

	live, launching := s.counts()
	s.log.InfoContext(ctx, "session created",
		"session_id", row.ID, "client_id", clientID, "template", tmpl.Name,
		"live_sessions", live, "launching_sessions", launching)

	go s.launch(context.WithoutCancel(ctx), row.ID, launchOpts{
		approvalPrompt: req.GetApprovalPrompt(),
		agentPrompt:    req.GetInitialPrompt(),
	})
	return &pb.CreateSessionResponse{Session: viewOf(row)}, nil
}

func (s *Service) Prompt(ctx context.Context, req *pb.PromptRequest) (*pb.PromptResponse, error) {
	sess, err := s.resolveLive(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	if req.GetContent() == "" {
		return nil, api.Errorf(api.ErrInvalidArgument, "content is required")
	}
	ctx = telemetry.With(ctx, "session_id", sess.ID)

	if req.GetIdempotencyKey() == "" {
		turnID, err := s.startTurn(ctx, sess, req.GetContent())
		if err != nil {
			return nil, err
		}
		return &pb.PromptResponse{TurnId: turnID}, nil
	}

	turnID, err := s.prompts.do(ctx, sess.ID, req.GetIdempotencyKey(), func() (string, error) {
		return s.startTurn(ctx, sess, req.GetContent())
	})
	if err != nil {
		return nil, err
	}
	return &pb.PromptResponse{TurnId: turnID}, nil
}

func (s *Service) startTurn(ctx context.Context, sess sessionstore.Session, content string) (string, error) {
	switch sess.Status {
	case api.StateRunning:
		b := s.lookup(sess.ID)
		if b == nil {
			return "", api.Errorf(api.ErrInvalidState, "session %s is not attached to this process", sess.ID)
		}
		turnID, err := s.nextTurnID(ctx, sess.ID)
		if err != nil {
			return "", err
		}
		go s.runTurn(context.WithoutCancel(ctx), b, turnID, content)
		return turnID, nil

	case api.StateSuspended:
		if err := s.revivable(sess); err != nil {
			return "", err
		}

		binding, err := s.authorize(ctx, sess.ClientID)
		if err != nil {
			return "", err
		}
		if err := s.checkQuotas(ctx, binding, sess.ClientID, spend{}); err != nil {
			return "", err
		}
		turnID, err := s.nextTurnID(ctx, sess.ID)
		if err != nil {
			return "", err
		}
		go s.launch(context.WithoutCancel(ctx), sess.ID, launchOpts{
			revive: true,

			approvalPrompt: content + continuationClause,
			agentPrompt:    content,
			turnID:         turnID,
		})
		return turnID, nil

	default:
		return "", api.Errorf(api.ErrInvalidState,
			"session %s is %s; a prompt is only accepted while running or suspended", sess.ID, sess.Status)
	}
}

const continuationClause = "\n\n(Continuing an earlier conversation, with its workspace and transcript.)"

func (s *Service) revivable(sess sessionstore.Session) error {
	switch {
	case sess.ACPSessionID == "":
		return api.Errorf(api.ErrNotRevivable, "session %s has no recorded conversation to continue", sess.ID)
	case sess.SandboxClaimName == "":
		return api.Errorf(api.ErrNotRevivable, "session %s has no workspace recorded", sess.ID)
	case sess.ApproverSubject == "":
		return api.Errorf(api.ErrNotRevivable,
			"session %s has no recorded approver, so a continuation could not be pinned to them", sess.ID)
	case sess.TemplateSpec == "":
		return api.Errorf(api.ErrNotRevivable, "session %s has no template snapshot to run", sess.ID)
	}
	return nil
}

func (s *Service) RespondPermission(ctx context.Context, req *pb.RespondPermissionRequest) (*pb.RespondPermissionResponse, error) {
	sess, err := s.resolveLive(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	if req.GetRequestId() == "" {
		return nil, api.Errorf(api.ErrInvalidArgument, "request_id is required")
	}

	if s.resolved.seen(sess.ID, req.GetRequestId()) {
		return &pb.RespondPermissionResponse{}, nil
	}
	b := s.lookup(sess.ID)
	if b == nil {
		return nil, api.Errorf(api.ErrUnknownRequest, "session %s is not live", sess.ID)
	}
	if err := b.sink.resolvePermission(req.GetRequestId(), sandbox.PermissionDecision{OptionID: req.GetOptionId()}); err != nil {
		return nil, err
	}
	s.resolved.record(sess.ID, req.GetRequestId())
	return &pb.RespondPermissionResponse{}, nil
}

func (s *Service) EndSession(ctx context.Context, req *pb.EndSessionRequest) (*pb.EndSessionResponse, error) {
	sess, err := s.resolveLive(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	if api.Terminal(sess.Status) {
		return &pb.EndSessionResponse{}, nil
	}

	s.stopSession(ctx, sess.ID, stopSpec{end: req.GetReason()})
	return &pb.EndSessionResponse{}, nil
}

func (s *Service) GetSession(ctx context.Context, req *pb.GetSessionRequest) (*pb.GetSessionResponse, error) {
	sess, err := s.resolve(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	return &pb.GetSessionResponse{Session: viewOf(sess)}, nil
}

func (s *Service) ListSessions(ctx context.Context, req *pb.ListSessionsRequest) (*pb.ListSessionsResponse, error) {
	clientID, _, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.store.ListSessionsByClient(ctx, clientID, req.GetLiveOnly(), api.Time(req.GetUpdatedSince()))
	if err != nil {
		return nil, api.Errorf(api.ErrUnavailable, "list sessions: %v", err)
	}
	out := &pb.ListSessionsResponse{Sessions: make([]*pb.SessionView, 0, len(rows))}
	for _, r := range rows {
		out.Sessions = append(out.Sessions, viewOf(r))
	}
	return out, nil
}

func (s *Service) ListTemplates(ctx context.Context, _ *pb.ListTemplatesRequest) (*pb.ListTemplatesResponse, error) {
	_, binding, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	all, err := s.templates.List(ctx)
	if err != nil {
		return nil, api.Errorf(api.ErrUnavailable, "list templates: %v", err)
	}
	out := &pb.ListTemplatesResponse{Templates: make([]*pb.TemplateSummary, 0, len(all))}
	for _, t := range all {
		if !s.mayRun(binding, t.Name) {
			continue
		}
		out.Templates = append(out.Templates, &pb.TemplateSummary{Name: t.Name})
	}
	return out, nil
}

func (s *Service) ListEvents(ctx context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	sess, err := s.resolve(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	events, err := s.events.History(ctx, sess.ID, req.GetAfterSeq(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &pb.ListEventsResponse{Events: events}, nil
}

func (s *Service) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream *connect.ServerStream[pb.SubscribeResponse]) error {
	sess, err := s.resolve(ctx, req.GetRef())
	if err != nil {
		return err
	}
	sub, err := s.events.Subscribe(ctx, sess.ID, req.GetAfterSeq())
	if err != nil {
		return err
	}
	defer sub.Close()
	return server.Stream(ctx, stream, sub.Events())
}

func (s *Service) caller(ctx context.Context) (string, *v1alpha1.ClientBinding, error) {
	clientID, err := apiserver.ClientID(ctx)
	if err != nil {
		return "", nil, err
	}
	binding, err := s.authorize(ctx, clientID)
	return clientID, binding, err
}

func (s *Service) resolveLive(ctx context.Context, ref *pb.SessionRef) (sessionstore.Session, error) {
	return s.resolveAs(ctx, ref, false)
}

func (s *Service) resolve(ctx context.Context, ref *pb.SessionRef) (sessionstore.Session, error) {
	return s.resolveAs(ctx, ref, ref.GetIncludeTerminal())
}

func (s *Service) resolveAs(ctx context.Context, ref *pb.SessionRef, includeTerminal bool) (sessionstore.Session, error) {
	clientID, _, err := s.caller(ctx)
	if err != nil {
		return sessionstore.Session{}, err
	}
	var sess sessionstore.Session
	switch {
	case ref.GetSessionId() != "":
		sess, err = s.store.GetSession(ctx, ref.GetSessionId())
	case ref.GetConversationRef() != "" && includeTerminal:
		sess, err = s.store.GetLatestSessionByConversation(ctx, clientID, ref.GetConversationRef())
	case ref.GetConversationRef() != "":
		sess, err = s.store.GetLiveSessionByConversation(ctx, clientID, ref.GetConversationRef())
	default:
		return sessionstore.Session{}, api.Errorf(api.ErrInvalidArgument, "session_id or conversation_ref is required")
	}
	if errors.Is(err, sessionstore.ErrNotFound) {
		return sessionstore.Session{}, api.Errorf(api.ErrNotFound, "no such session")
	}
	if err != nil {
		return sessionstore.Session{}, api.Errorf(api.ErrUnavailable, "read session: %v", err)
	}
	if sess.ClientID != clientID {
		s.log.WarnContext(ctx, "cross-client session access refused",
			"session", sess.ID, "owner", sess.ClientID, "caller", clientID)
		return sessionstore.Session{}, api.Errorf(api.ErrNotFound, "no such session")
	}
	return sess, nil
}

func (s *Service) mayRun(binding *v1alpha1.ClientBinding, name string) bool {
	return slices.Contains(binding.Spec.Templates, name)
}

func (s *Service) allowedTemplate(ctx context.Context, binding *v1alpha1.ClientBinding, clientID, name string) (*v1alpha1.AgentTemplate, error) {
	if !s.mayRun(binding, name) {
		return nil, api.Errorf(api.ErrForbidden, "client %q is not bound to template %q", clientID, name)
	}
	tmpl, err := s.templates.Resolve(ctx, name)
	if errors.Is(err, agenttemplate.ErrNotFound) {
		return nil, api.Errorf(api.ErrNotFound, "no agent template %q", name)
	}
	if err != nil {
		return nil, api.Errorf(api.ErrUnavailable, "resolve template %q: %v", name, err)
	}
	return tmpl, nil
}

const snapshotVersion = 1

type templateSnapshot struct {
	SnapshotVersion int                        `json:"v"`
	Name            string                     `json:"name"`
	Spec            v1alpha1.AgentTemplateSpec `json:"spec"`

	PromptAppendix string `json:"prompt_appendix,omitempty"`
}

func storedTemplate(sess sessionstore.Session) (*v1alpha1.AgentTemplate, string, error) {
	if sess.TemplateSpec == "" {
		return nil, "", api.Errorf(api.ErrInvalidState, "session %s has no template snapshot", sess.ID)
	}
	var snap templateSnapshot
	if err := json.Unmarshal([]byte(sess.TemplateSpec), &snap); err != nil {
		return nil, "", api.Errorf(api.ErrInvalidState, "session %s has an unreadable template snapshot: %v", sess.ID, err)
	}
	name := snap.Name
	if name == "" {
		name = sess.TemplateName
	}
	tmpl := &v1alpha1.AgentTemplate{Spec: snap.Spec}
	tmpl.Name = name
	return tmpl, snap.PromptAppendix, nil
}

func (s *Service) emit(ctx context.Context, sessionID string, ev *pb.Event) {
	ev.SessionId = sessionID
	if err := s.events.Append(context.WithoutCancel(ctx), ev); err != nil {
		s.log.ErrorContext(ctx, "could not record a session event; clients will see a gap",
			"session", sessionID, "event", api.Kind(ev), "err", err)
	}
}

func (s *Service) setState(ctx context.Context, sessionID string, from, to api.SessionState, reason api.Reason) {
	if err := s.store.UpdateSessionStatus(ctx, sessionID, to); err != nil {
		s.log.WarnContext(ctx, "update session status failed",
			"session", sessionID, "status", to, "err", err)
	}
	s.emit(ctx, sessionID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{Old: from, New: to, Reason: reason}}})
}

func (s *Service) nextTurnID(ctx context.Context, sessionID string) (string, error) {
	n, err := s.store.NextTurnSeq(ctx, sessionID)
	if err != nil {
		return "", api.Errorf(api.ErrUnavailable, "allocate a turn for session %s: %v", sessionID, err)
	}
	return "t" + strconv.FormatInt(n, 10), nil
}

func viewOf(sess sessionstore.Session) *pb.SessionView {
	return &pb.SessionView{
		Id:              sess.ID,
		ConversationRef: sess.ConversationRef,
		State:           sess.Status,
		Template:        sess.TemplateName,
		LastSeq:         sess.EventSeq,
	}
}

func (s *Service) lookup(sessionID string) *binding {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live[sessionID]
}

func (s *Service) counts() (live, launching int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.live), len(s.launching)
}

type launchSlot struct {
	cancel  context.CancelFunc
	outcome *runOutcome
}

func (s *Service) reserve(sessionID string, slot *launchSlot) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, live := s.live[sessionID]; live {
		return false
	}
	if _, launching := s.launching[sessionID]; launching {
		return false
	}
	s.launching[sessionID] = slot
	return true
}

func (s *Service) release(sessionID string, slot *launchSlot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.launching[sessionID] == slot {
		delete(s.launching, sessionID)
	}
}

func (s *Service) register(b *binding, slot *launchSlot) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot.outcome.isStopped() {
		return false
	}
	s.live[b.sessionID] = b
	delete(s.launching, b.sessionID)
	return true
}

func (s *Service) detach(sessionID string, spec stopSpec) (*binding, *launchSlot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.live[sessionID]; ok {
		delete(s.live, sessionID)
		return b, nil
	}
	if slot, ok := s.launching[sessionID]; ok {
		if slot.cancel != nil {
			slot.outcome.stop(spec)
			slot.cancel()
		}
		return nil, nil
	}
	held := &launchSlot{}
	s.launching[sessionID] = held
	return nil, held
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
