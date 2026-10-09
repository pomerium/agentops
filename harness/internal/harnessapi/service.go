package harnessapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/pb/harnessapipbconnect"
	"github.com/pomerium/agentops/harness/api/server"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/agenticrun"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/agenttemplate"
	"github.com/pomerium/agentops/harness/internal/apiserver"
	"github.com/pomerium/agentops/harness/internal/sandbox"
	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/telemetry"
)

type LiveSession interface {
	ID() string
	StreamID() []byte
	ReadySeq() uint64
	Inbox() <-chan *agentlinkpb.AgentIOFrame
	Done() <-chan struct{}
	Ack(seq uint64)
	Prompt(turnID string, turnSeq uint64, text string)
	Decide(requestID, optionID string, cancelled bool)
	Close() error
}

type Launcher interface {
	Prepare(ctx context.Context, spec sandbox.LaunchSpec) (*sandbox.Prepared, error)
	Expect(runID string, prepared *sandbox.Prepared, opts ...sandbox.SupervisionOption) (*sandbox.Attachment, error)
	Activate(ctx context.Context, prepared *sandbox.Prepared, att *sandbox.Attachment) (LiveSession, error)
	Adopt(ctx context.Context, spec sandbox.AdoptSpec, opts ...sandbox.SupervisionOption) (LiveSession, error)
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

type Option func(*options)

type options struct {
	sessionTTL        time.Duration
	sessionIdleTTL    time.Duration
	idleWarnLead      time.Duration
	suspendedTTL      time.Duration
	permissionTimeout time.Duration
	runTTL            time.Duration
	runPollInterval   time.Duration
	runWatchInterval  time.Duration
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
	if c.runPollInterval == 0 {
		c.runPollInterval = 5 * time.Second
	}
	if c.runWatchInterval == 0 {
		c.runWatchInterval = time.Minute
	}
}

type Store interface {
	sessionstore.Sessions
	sessionstore.PodCommands
}

type Service struct {
	cfg       options
	store     Store
	events    EventLog
	launcher  Launcher
	templates Templates
	runs      agenticrun.RunClient
	log       *slog.Logger
	tel       *telemetry.Component

	rates *rateLimiters

	resolved *resolvedRequests

	prompts *promptKeys

	admission sync.Mutex

	mu     sync.Mutex
	owners map[string]*owner

	emitLocks [emitStripes]sync.Mutex
}

var _ harnessapipbconnect.HarnessAPIServiceHandler = (*Service)(nil)

func New(st Store, ev EventLog, l Launcher, t Templates, runs agenticrun.RunClient, opts ...Option) *Service {
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
		tel:      telemetry.New(log, "harnessapi", slog.LevelDebug),
		rates:    newRateLimiters(),
		resolved: newResolvedRequests(),
		prompts:  newPromptKeys(),
		owners:   map[string]*owner{},
	}
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
	launchCtx, o := s.newLaunch(ctx, clientID)
	if err := s.admit(ctx, clientBinding, clientID, spend{newSession: true}, func() error {
		if err := s.store.CreateSession(ctx, row); err != nil {
			if errors.Is(err, sessionstore.ErrConflict) {
				return api.Errorf(api.ErrConflict, "conversation %q already has a live session", req.GetConversationRef())
			}
			return api.Errorf(api.ErrUnavailable, "create session: %v", err)
		}
		s.claim(row.ID, o)
		return nil
	}); err != nil {
		o.cancel()
		return nil, err
	}
	ctx = telemetry.With(ctx, "session_id", row.ID)
	s.emit(ctx, row.ID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{New: api.StatePending}}})

	live, launching := s.counts()
	s.log.InfoContext(ctx, "session created",
		"session_id", row.ID, "client_id", clientID, "template", tmpl.Name,
		"live_sessions", live, "launching_sessions", launching)

	go s.launch(launchCtx, o, row.ID, launchOpts{
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
		b.sendMu.Lock()
		turnID, n, err := s.nextTurn(ctx, sess.ID)
		if err != nil {
			b.sendMu.Unlock()
			return "", err
		}
		if !b.enter(turnID) {
			b.sendMu.Unlock()
			return "", api.Errorf(api.ErrInvalidState, "session %s is not attached to this process", sess.ID)
		}
		ctx = context.WithoutCancel(ctx)
		if err := s.sendPrompt(ctx, b, turnID, uint64(n), content); err != nil {
			b.sendMu.Unlock()
			b.leave(turnID)
			return "", api.Errorf(api.ErrUnavailable, "save turn %s of session %s: %v", turnID, sess.ID, err)
		}
		b.sendMu.Unlock()
		s.extendLease(ctx, b)
		return turnID, nil

	case api.StateSuspended:
		if err := s.revivable(sess); err != nil {
			return "", err
		}

		binding, err := s.authorize(ctx, sess.ClientID)
		if err != nil {
			return "", err
		}
		if !s.mayRun(binding, sess.TemplateName) {
			return "", api.Errorf(api.ErrForbidden,
				"client %q is no longer bound to template %q, so session %s cannot be continued",
				sess.ClientID, sess.TemplateName, sess.ID)
		}
		launchCtx, o := s.newLaunch(ctx, sess.ClientID)
		if err := s.admit(ctx, binding, sess.ClientID, spend{}, func() error {
			return s.claimRevive(ctx, sess.ID, o)
		}); err != nil {
			o.cancel()
			return "", err
		}
		turnID, n, err := s.nextTurn(ctx, sess.ID)
		if err != nil {
			o.cancel()
			s.settle(ctx, sess.ID, o)
			return "", err
		}
		go s.launch(launchCtx, o, sess.ID, launchOpts{
			revive: true,

			approvalPrompt: content + continuationClause,
			agentPrompt:    content,
			turnID:         turnID,
			turnSeq:        uint64(n),
		})
		return turnID, nil

	default:
		return "", api.Errorf(api.ErrInvalidState,
			"session %s is %s; a prompt is only accepted while running or suspended", sess.ID, sess.Status)
	}
}

func (s *Service) claimRevive(ctx context.Context, sessionID string, o *owner) error {
	if !s.claim(sessionID, o) {
		return api.Errorf(api.ErrInvalidState, "session %s is already being continued or stopped", sessionID)
	}
	current, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		s.settle(ctx, sessionID, o)
		return api.Errorf(api.ErrUnavailable, "read session: %v", err)
	}
	if current.Status != api.StateSuspended {
		s.settle(ctx, sessionID, o)
		return api.Errorf(api.ErrInvalidState, "session %s is %s; it can no longer be continued", sessionID, current.Status)
	}
	return nil
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
	b.permMu.Lock()
	defer b.permMu.Unlock()
	if err := b.sink.validate(req.GetRequestId(), req.GetOptionId()); err != nil {
		return nil, err
	}
	w, ok := b.sink.take(req.GetRequestId())
	if !ok {
		return nil, api.Errorf(api.ErrUnknownRequest, "permission request %q is unknown or already resolved", req.GetRequestId())
	}
	ctx = context.WithoutCancel(ctx)
	if err := s.saveDecision(ctx, b, w.turnID, req.GetRequestId(), req.GetOptionId(), false); err != nil {
		b.sink.restore(req.GetRequestId(), w, time.Until(w.deadline))
		return nil, api.Errorf(api.ErrUnavailable, "save the decision for permission request %q: %v", req.GetRequestId(), err)
	}
	s.emit(ctx, sess.ID, &pb.Event{TurnId: w.turnID, Payload: &pb.Event_PermissionResolved{PermissionResolved: &pb.PermissionResolved{
		RequestId: req.GetRequestId(), Resolution: &pb.PermissionResolved_OptionId{OptionId: req.GetOptionId()},
	}}})
	b.session.Decide(req.GetRequestId(), req.GetOptionId(), false)
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

	if !s.stopSession(ctx, sess.ID, stopSpec{end: req.GetReason()}) {
		return nil, api.Errorf(api.ErrUnavailable, "the end of session %s could not be recorded; send EndSession again", sess.ID)
	}
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
	if err := server.Stream(ctx, stream, sub.Events()); err != nil {
		return err
	}
	return sub.Err()
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
	if err := s.emitPod(ctx, sessionID, ev, 0); err != nil {
		s.log.ErrorContext(ctx, "could not record a session event; clients will see a gap",
			"session", sessionID, "event", api.Kind(ev), "err", err)
	}
}

func (s *Service) emitPod(ctx context.Context, sessionID string, ev *pb.Event, podSeq uint64) error {
	if podSeq == 0 {
		ctx = context.WithoutCancel(ctx)
	}
	return s.recordEvent(ctx, sessionID, ev, podSeq)
}

func (s *Service) recordEvent(ctx context.Context, sessionID string, ev *pb.Event, podSeq uint64) error {
	ev.SessionId = sessionID
	defer s.lockEvents(sessionID)()
	current, err := s.store.GetSession(ctx, sessionID)
	if err == nil && api.Terminal(current.Status) {
		s.tel.Debug(ctx, "the session has ended; dropping a later event", "session", sessionID, "event", api.Kind(ev))
		return nil
	}
	if podSeq > 0 {
		return s.events.AppendFromPod(ctx, ev, podSeq)
	}
	return s.events.Append(ctx, ev)
}

func (s *Service) lockEvents(sessionID string) func() {
	lock := &s.emitLocks[emitStripe(sessionID)]
	lock.Lock()
	return lock.Unlock
}

const emitStripes = 64

func emitStripe(sessionID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sessionID))
	return int(h.Sum32() % emitStripes)
}

func (s *Service) write(ctx context.Context, sessionID string, update func(context.Context) error) bool {
	ctx = context.WithoutCancel(ctx)
	current, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		s.log.WarnContext(ctx, "read session before a state write failed", "session", sessionID, "err", err)
		return false
	}
	if api.Terminal(current.Status) {
		s.tel.Debug(ctx, "the session has ended; refusing a later state write", "session", sessionID)
		return false
	}
	if err := update(ctx); err != nil {
		s.log.ErrorContext(ctx, "update session state failed", "session", sessionID, "err", err)
		return false
	}
	return true
}

func (s *Service) beginLaunch(ctx context.Context, sessionID string, from api.SessionState, reason api.Reason) bool {
	if !s.write(ctx, sessionID, func(ctx context.Context) error {
		return s.store.UpdateSessionLaunched(ctx, sessionID, api.StateLaunching, time.Now())
	}) {
		return false
	}
	s.emit(ctx, sessionID, &pb.Event{Payload: &pb.Event_StateChanged{StateChanged: &pb.StateChanged{Old: from, New: api.StateLaunching, Reason: reason}}})
	return true
}

func (s *Service) nextTurn(ctx context.Context, sessionID string) (string, int64, error) {
	n, err := s.store.NextTurnSeq(ctx, sessionID)
	if err != nil {
		return "", 0, api.Errorf(api.ErrUnavailable, "allocate a turn for session %s: %v", sessionID, err)
	}
	return turnName(n), n, nil
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

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
