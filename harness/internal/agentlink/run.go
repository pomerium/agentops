package agentlink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pomerium/agentops/harness/internal/agenticrun"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

const ProtocolVersion uint32 = 2

const inboxSize = 256

type ExpectCallbacks struct {
	OnAttached func(attempt uint32, agentRunning bool)
	OnLost     func(cause error)
	OnError    func(reason string, cause error)

	StreamID    []byte
	ResumeAfter uint64
}

type ExpectOption func(*ExpectCallbacks)

func WithOnAttached(f func(attempt uint32, agentRunning bool)) ExpectOption {
	return func(o *ExpectCallbacks) { o.OnAttached = f }
}

func WithOnLost(f func(cause error)) ExpectOption { return func(o *ExpectCallbacks) { o.OnLost = f } }

func WithOnError(f func(reason string, cause error)) ExpectOption {
	return func(o *ExpectCallbacks) { o.OnError = f }
}

func WithStreamID(id []byte) ExpectOption { return func(o *ExpectCallbacks) { o.StreamID = id } }

func WithResumeAfter(seq uint64) ExpectOption {
	return func(o *ExpectCallbacks) { o.ResumeAfter = seq }
}

func newExpectCallbacks(opts []ExpectOption) ExpectCallbacks {
	var o ExpectCallbacks
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

var errForgotten = errors.New("harness: run expectation dropped")

type attachStream struct {
	send     chan *agentlinkpb.ManagerFrame
	closed   chan struct{}
	lastRecv atomic.Int64

	closeOnce sync.Once
	mu        sync.Mutex
	cause     error
}

func newAttachStream(now time.Time) *attachStream {
	s := &attachStream{
		send:   make(chan *agentlinkpb.ManagerFrame, 8),
		closed: make(chan struct{}),
	}
	s.lastRecv.Store(now.UnixNano())
	return s
}

func (s *attachStream) close(cause error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.cause = cause
		s.mu.Unlock()
		close(s.closed)
	})
}

func (s *attachStream) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

func (s *attachStream) touch(now time.Time) { s.lastRecv.Store(now.UnixNano()) }

func (s *attachStream) silentFor(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, s.lastRecv.Load()))
}

func (s *attachStream) dispatch(ctx context.Context, f *agentlinkpb.ManagerFrame) error {
	select {
	case s.send <- f:
		return nil
	case <-s.closed:
		return fmt.Errorf("attach stream closed: %w", s.err())
	case <-ctx.Done():
		return ctx.Err()
	}
}

type attachedRun struct {
	runID  string
	seal   agenticrun.Executor
	config *agentlinkpb.SandboxConfig
	opts   ExpectCallbacks
	log    *slog.Logger

	hbInterval  time.Duration
	hbMissLimit uint32

	streamID []byte
	inbox    chan *agentlinkpb.AgentIOFrame
	ioTurn   chan struct{}
	outReady chan struct{}

	attached chan struct{}
	ready    chan struct{}
	done     chan struct{}

	attachedOnce sync.Once
	readyOnce    sync.Once
	doneOnce     sync.Once

	notifyMu sync.Mutex

	mu        sync.Mutex
	live      *attachStream
	ioStream  *ioClaim
	attempts  uint32
	err       error
	delivered uint64
	acked     uint64
	commands  []queuedCommand
	commandID uint64
}

type queuedCommand struct {
	id    uint64
	frame *agentlinkpb.AgentIOFrame
}

type ioClaim struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newIOClaim() *ioClaim { return &ioClaim{closed: make(chan struct{})} }

func (c *ioClaim) close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() { close(c.closed) })
}

var errIOSuperseded = errors.New("superseded by a newer agent io stream")

func (r *attachedRun) claimIO(ctx context.Context) (*ioClaim, error) {
	claim := newIOClaim()
	r.mu.Lock()
	if r.finished() {
		r.mu.Unlock()
		return nil, errForgotten
	}
	prev := r.ioStream
	r.ioStream = claim
	r.mu.Unlock()
	prev.close()

	select {
	case r.ioTurn <- struct{}{}:
	case <-ctx.Done():
		r.dropIO(claim)
		return nil, ctx.Err()
	}
	select {
	case <-claim.closed:
		r.releaseIO(claim)
		if r.finished() {
			return nil, errForgotten
		}
		return nil, errIOSuperseded
	default:
		return claim, nil
	}
}

func (r *attachedRun) releaseIO(claim *ioClaim) {
	r.dropIO(claim)
	<-r.ioTurn
}

func (r *attachedRun) dropIO(claim *ioClaim) {
	claim.close()
	r.mu.Lock()
	if r.ioStream == claim {
		r.ioStream = nil
	}
	r.mu.Unlock()
}

func (r *attachedRun) notifyAttached(attempt uint32, agentRunning bool) {
	r.notifyMu.Lock()
	defer r.notifyMu.Unlock()
	if r.opts.OnAttached != nil {
		r.opts.OnAttached(attempt, agentRunning)
	}
}

func (r *attachedRun) markAttached() { r.attachedOnce.Do(func() { close(r.attached) }) }

func (r *attachedRun) markReady() { r.readyOnce.Do(func() { close(r.ready) }) }

func (r *attachedRun) signalOut() {
	select {
	case r.outReady <- struct{}{}:
	default:
	}
}

func (r *attachedRun) deliveredSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.delivered
}

func (r *attachedRun) setDelivered(seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if seq > r.delivered {
		r.delivered = seq
	}
}

func (r *attachedRun) ackedSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acked
}

func (r *attachedRun) nextCommand(after uint64) (queuedCommand, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.commands {
		if c.id > after {
			return c, true
		}
	}
	return queuedCommand{}, false
}

func (r *attachedRun) commitCommand(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = slices.DeleteFunc(r.commands, func(c queuedCommand) bool { return c.id == id })
}

func (r *attachedRun) finish(cause error) {
	r.doneOnce.Do(func() {
		r.mu.Lock()
		r.err = cause
		live, ioStream := r.live, r.ioStream
		r.live, r.ioStream = nil, nil
		close(r.done)
		r.mu.Unlock()
		if live != nil {
			live.close(cause)
		}
		ioStream.close()
	})
}

func (r *attachedRun) finished() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (r *attachedRun) current() *attachStream {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.live
}

type RunHandle struct {
	srv *Server
	run *attachedRun
}

func (h *RunHandle) RunID() string { return h.run.runID }

func (h *RunHandle) AwaitAttach(ctx context.Context) error {
	select {
	case <-h.run.attached:
		return nil
	case <-h.run.done:
		return h.terminalErr()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *RunHandle) AwaitReady(ctx context.Context) error {
	select {
	case <-h.run.ready:
		return nil
	case <-h.run.done:
		return h.terminalErr()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *RunHandle) StreamID() []byte { return h.run.streamID }

func (h *RunHandle) SpawnAgent(ctx context.Context, params *agentlinkpb.SessionParams) error {
	live := h.run.current()
	if live == nil {
		return fmt.Errorf("spawn agent for run %s: no live attach", h.run.runID)
	}
	return live.dispatch(ctx, &agentlinkpb.ManagerFrame{
		Msg: &agentlinkpb.ManagerFrame_Spawn{Spawn: &agentlinkpb.SpawnAgent{StreamId: h.run.streamID, Session: params}},
	})
}

func (h *RunHandle) Inbox() <-chan *agentlinkpb.AgentIOFrame { return h.run.inbox }

func (h *RunHandle) Ack(seq uint64) {
	h.run.mu.Lock()
	if seq > h.run.acked {
		h.run.acked = seq
	}
	h.run.mu.Unlock()
	h.run.signalOut()
}

func (h *RunHandle) Send(f *agentlinkpb.AgentIOFrame) {
	h.run.mu.Lock()
	h.run.commandID++
	h.run.commands = append(h.run.commands, queuedCommand{id: h.run.commandID, frame: f})
	h.run.mu.Unlock()
	h.run.signalOut()
}

func (h *RunHandle) Shutdown(reason string) {
	live := h.run.current()
	if live == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := live.dispatch(ctx, &agentlinkpb.ManagerFrame{
		Msg: &agentlinkpb.ManagerFrame_Shutdown{Shutdown: &agentlinkpb.Shutdown{Reason: reason}},
	}); err != nil {
		h.run.log.Debug("harness: shutdown directive not delivered", "run_id", h.run.runID, "err", err)
	}
}

func (h *RunHandle) Done() <-chan struct{} { return h.run.done }

func (h *RunHandle) Err() error {
	select {
	case <-h.run.done:
		return h.terminalErr()
	default:
		return nil
	}
}

func (h *RunHandle) terminalErr() error {
	h.run.mu.Lock()
	defer h.run.mu.Unlock()
	if h.run.err != nil {
		return h.run.err
	}
	return errForgotten
}
