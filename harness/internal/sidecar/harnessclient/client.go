package harnessclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/pomeriumtls"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

const (
	EnvURL         = "SIDECAR_HARNESS_URL"
	EnvDialAddress = "SIDECAR_HARNESS_DIAL_ADDRESS"
	EnvCAFile      = "SIDECAR_HARNESS_CA_FILE"
	EnvInsecure    = "SIDECAR_HARNESS_INSECURE"
)

const (
	defaultBaseBackoff         = 500 * time.Millisecond
	defaultMaxBackoff          = 30 * time.Second
	defaultTokenRefreshTimeout = 30 * time.Second
	joinTimeout                = 5 * time.Second
)

const deniedRetryLimit = 3

const ProtocolVersion uint32 = 2

type TokenSource interface {
	Bearer() string
	Refresh()
}

type Runner interface {
	Spawn(ctx context.Context, streamID []byte, params *agentlinkpb.SessionParams) (*AgentSession, error)
	Join(ctx context.Context) (*AgentSession, error)
}

type AgentSession struct {
	StreamID []byte
	PID      int64
	Frames   <-chan *runnerpb.RunnerServerFrame
	Lost     <-chan struct{}
	Send     func(*runnerpb.RunnerClientFrame) error
	Close    func()

	replays      uint64
	replayStarts uint64
}

type Config struct {
	URL                     string
	DialAddress             string
	CAFile                  string
	Insecure                bool
	Token                   TokenSource
	Runner                  Runner
	Configure               func(*agentlinkpb.SandboxConfig) error
	HeartbeatInterval       time.Duration
	HeartbeatMissLimit      uint32
	BaseBackoff, MaxBackoff time.Duration
	TokenRefreshTimeout     time.Duration
	Logger                  *slog.Logger
}

type TerminalError struct {
	Reason string
	Err    error
}

func (e *TerminalError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("harness attach terminal (%s): %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("harness attach terminal (%s)", e.Reason)
}

func (e *TerminalError) Unwrap() error { return e.Err }

const (
	ReasonUnknownRun     = "unknown_run"
	ReasonRejected       = "attach_rejected"
	ReasonAttachDenied   = "attach_denied"
	ReasonShutdown       = "shutdown"
	ReasonLocalFailure   = "local_failure"
	ReasonRunnerFailure  = "runner_unreachable"
	ReasonRunnerLost     = "runner_lost"
	ReasonConfigFailed   = "config_failed"
	ReasonStreamMismatch = "agentio_stream_mismatch"
)

var errRunnerLost = errors.New("the runner stream ended")

type Client struct {
	cfg    Config
	log    *slog.Logger
	conn   *grpc.ClientConn
	client agentlinkpb.AgentLinkServiceClient

	statusCh   chan *agentlinkpb.Status
	ioSlot     chan struct{}
	configured sync.Once
	joined     sync.Once

	mu      sync.Mutex
	agent   *AgentSession
	spawn   *pendingSpawn
	stopped bool
}

func New(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("harness client: URL is required")
	}
	if cfg.Token == nil {
		return nil, errors.New("harness client: a token source is required")
	}
	if cfg.Runner == nil {
		return nil, errors.New("harness client: a runner is required")
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = defaultBaseBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.TokenRefreshTimeout <= 0 {
		cfg.TokenRefreshTimeout = defaultTokenRefreshTimeout
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("harness client: invalid URL %q", cfg.URL)
	}
	target := u.Host
	if u.Port() == "" && !cfg.Insecure {
		target = net.JoinHostPort(u.Hostname(), "443")
	}

	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true,
		}),
		grpc.WithAuthority(u.Host),
	}
	if cfg.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsCfg, err := pomeriumtls.TLSConfig(cfg.CAFile, u.Hostname())
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	}
	if cfg.DialAddress != "" {
		dial := cfg.DialAddress
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return pomeriumtls.Dial(ctx, "tcp", dial)
		}))
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("harness client: dial %s: %w", target, err)
	}
	return &Client{
		cfg: cfg, log: log, conn: conn,
		client:   agentlinkpb.NewAgentLinkServiceClient(conn),
		statusCh: make(chan *agentlinkpb.Status, 4),
		ioSlot:   make(chan struct{}, 1),
	}, nil
}

func (c *Client) Close() {
	c.stopAgent()
	_ = c.conn.Close()
}

func (c *Client) Fail(reason string) {
	select {
	case c.statusCh <- &agentlinkpb.Status{State: agentlinkpb.Status_STATE_ERROR, Reason: reason}:
	default:
	}
}

func (c *Client) joinRunningAgent(ctx context.Context) {
	c.joined.Do(func() {
		ctx, cancel := context.WithTimeout(ctx, joinTimeout)
		defer cancel()
		ag, err := c.cfg.Runner.Join(ctx)
		switch {
		case err == nil:
			if c.adopt(ag) {
				c.log.Info("harness: joined the agent session that the runner holds", "pid", ag.PID)
			} else {
				ag.Close()
			}
		case errors.Is(err, ErrNoAgent):
		default:
			c.log.Debug("harness: could not ask the runner for an agent session", "err", err)
		}
	})
}

func (c *Client) Run(ctx context.Context) error {
	c.joinRunningAgent(ctx)

	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = c.cfg.BaseBackoff
	policy.MaxInterval = c.cfg.MaxBackoff
	policy.Multiplier = 2
	denied := 0
	var deniedBearer string
	var deniedAt time.Time
	var attempt uint32

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		attempt++
		bearer := c.cfg.Token.Bearer()
		err := c.session(ctx, attempt, bearer)
		if err == nil {
			return struct{}{}, nil
		}
		var terminal *TerminalError
		if errors.As(err, &terminal) {
			return struct{}{}, backoff.Permanent(terminal)
		}
		if ctx.Err() != nil {
			return struct{}{}, backoff.Permanent(ctx.Err())
		}

		switch status.Code(err) {
		case codes.NotFound:
			return struct{}{}, backoff.Permanent(&TerminalError{Reason: ReasonUnknownRun, Err: err})
		case codes.FailedPrecondition:
			return struct{}{}, backoff.Permanent(&TerminalError{Reason: ReasonRejected, Err: err})
		case codes.PermissionDenied:
			if bearer == deniedBearer {
				if waited := time.Since(deniedAt); waited > c.cfg.TokenRefreshTimeout {
					return struct{}{}, backoff.Permanent(&TerminalError{Reason: ReasonAttachDenied,
						Err: fmt.Errorf("no fresh run token %s after the denial: %w", waited.Round(time.Millisecond), err)})
				}
				return struct{}{}, backoff.RetryAfter(c.cfg.BaseBackoff, err)
			}
			denied++
			deniedBearer, deniedAt = bearer, time.Now()
			if denied > deniedRetryLimit {
				return struct{}{}, backoff.Permanent(&TerminalError{Reason: ReasonAttachDenied, Err: err})
			}
			c.log.Warn("harness attach denied; refreshing the run token",
				"attempt", denied, "limit", deniedRetryLimit, "err", err)
			c.cfg.Token.Refresh()
			return struct{}{}, backoff.RetryAfter(c.cfg.BaseBackoff, err)
		default:
			denied, deniedBearer = 0, ""
			return struct{}{}, err
		}
	}, backoff.WithBackOff(policy), backoff.WithMaxElapsedTime(0), backoff.WithNotify(func(err error, next time.Duration) {
		var after *backoff.RetryAfterError
		if !errors.As(err, &after) {
			c.log.Warn("harness attach lost; redialing", "backoff", next.String(), "code", status.Code(err).String(), "err", err)
		}
	}))
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if re := backoff.AsRetryError(err); re != nil {
		return re.LastErr
	}
	return err
}

func (c *Client) session(parent context.Context, attempt uint32, bearer string) error {
	if bearer == "" {
		return errors.New("no run token yet")
	}
	ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(parent, "authorization", bearer))
	defer cancel()

	stream, err := c.client.Attach(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&agentlinkpb.SidecarFrame{
		Msg: &agentlinkpb.SidecarFrame_Hello{Hello: &agentlinkpb.SidecarHello{
			ProtocolVersion: ProtocolVersion,
			Attempt:         attempt,
			AgentRunning:    c.agentRunning(),
		}},
	}); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	ack := first.GetHelloAck()
	if ack == nil {
		return status.Error(codes.FailedPrecondition, "the first manager frame must be HelloAck")
	}
	interval, missLimit, deadline := c.heartbeatContract(ack)
	c.log.Info("harness attached", "attempt", attempt, "heartbeat", interval.String(), "miss_limit", missLimit,
		"endpoints", len(ack.GetConfig().GetEndpoints()))

	if err := c.configure(ack.GetConfig()); err != nil {
		c.log.Error("harness: applying the manager's configuration failed", "err", err)
		recvCh := make(chan error, 1)
		go func() {
			for {
				if _, err := stream.Recv(); err != nil {
					recvCh <- err
					return
				}
			}
		}()
		sendTerminal(stream, statusFrame(ReasonConfigFailed), recvCh)
		return &TerminalError{Reason: ReasonConfigFailed, Err: err}
	}
	if err := stream.Send(readyFrame()); err != nil {
		return err
	}

	return c.serve(ctx, parent, stream, interval, deadline)
}

func (c *Client) heartbeatContract(ack *agentlinkpb.ManagerHelloAck) (time.Duration, uint32, time.Duration) {
	advertised := time.Duration(ack.GetHeartbeatSeconds()) * time.Second
	if advertised <= 0 {
		advertised = 20 * time.Second
	}
	interval := advertised
	if c.cfg.HeartbeatInterval > 0 {
		interval = min(c.cfg.HeartbeatInterval, advertised)
	}
	missLimit := c.cfg.HeartbeatMissLimit
	if missLimit == 0 {
		missLimit = ack.GetHeartbeatMissLimit()
	}
	if missLimit == 0 {
		missLimit = 3
	}
	return interval, missLimit, time.Duration(missLimit) * advertised
}

func (c *Client) serve(ctx, runCtx context.Context, stream agentlinkpb.AgentLinkService_AttachClient,
	interval, deadline time.Duration,
) error {
	type recvResult struct {
		frame *agentlinkpb.ManagerFrame
		err   error
	}
	recvCh := make(chan recvResult, 1)
	ended := make(chan error, 1)
	go func() {
		for {
			f, err := stream.Recv()
			select {
			case recvCh <- recvResult{f, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	terminal := func(f *agentlinkpb.SidecarFrame) {
		go func() {
			for r := range recvCh {
				if r.err != nil {
					ended <- r.err
					return
				}
			}
		}()
		sendTerminal(stream, f, ended)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastRecv := time.Now()
	lastSent := time.Now()

	var lost <-chan struct{}
	ioLost := make(chan error, 1)
	watch := func(ag *AgentSession) {
		lost = ag.Lost
		go c.pumpAgentIO(ctx, ag, ioLost)
	}
	var spawned <-chan struct{}
	ag, pending := c.agentState()
	switch {
	case ag != nil:
		watch(ag)
	case pending != nil:
		spawned = pending.done
	}

	for {
		select {
		case r := <-recvCh:
			if r.err != nil {
				return fmt.Errorf("attach stream ended: %w", r.err)
			}
			lastRecv = time.Now()
			switch {
			case r.frame.GetSpawn() != nil:
				spawn := r.frame.GetSpawn()
				if p := c.startSpawn(runCtx, spawn.GetStreamId(), spawn.GetSession()); p != nil {
					pending, spawned = p, p.done
				}
			case r.frame.GetShutdown() != nil:
				c.log.Info("harness: shutdown directive received", "reason", r.frame.GetShutdown().GetReason())
				c.stopAgent()
				return nil
			case r.frame.GetHeartbeat() != nil:
			}
		case <-spawned:
			spawned = nil
			if pending.err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				c.log.Error("harness: spawn failed", "err", pending.err)
				terminal(statusFrame(ReasonRunnerFailure))
				return &TerminalError{Reason: ReasonRunnerFailure, Err: pending.err}
			}
			if ag := c.currentAgent(); ag != nil {
				watch(ag)
			}
		case <-lost:
			if c.isStopped() {
				return nil
			}
			c.log.Error("harness: the runner stream ended; the agent is gone")
			terminal(statusFrame(ReasonRunnerLost))
			return &TerminalError{Reason: ReasonRunnerLost, Err: errRunnerLost}
		case err := <-ioLost:
			return status.Errorf(codes.Unavailable, "agent io lost: %v", err)
		case st := <-c.statusCh:
			terminal(&agentlinkpb.SidecarFrame{Msg: &agentlinkpb.SidecarFrame_Status{Status: st}})
			return &TerminalError{Reason: st.GetReason()}
		case now := <-ticker.C:
			if now.Sub(lastRecv) > deadline {
				return fmt.Errorf("no manager frame for %s", now.Sub(lastRecv).Round(time.Second))
			}
			if now.Sub(lastSent) < interval {
				continue
			}
			if err := stream.Send(readyFrame()); err != nil {
				return err
			}
			lastSent = now
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

const terminalFlushTimeout = 2 * time.Second

func sendTerminal(stream agentlinkpb.AgentLinkService_AttachClient, f *agentlinkpb.SidecarFrame, ended <-chan error) {
	if err := stream.Send(f); err != nil {
		return
	}
	_ = stream.CloseSend()
	select {
	case <-ended:
	case <-time.After(terminalFlushTimeout):
	}
}

func statusFrame(reason string) *agentlinkpb.SidecarFrame {
	return &agentlinkpb.SidecarFrame{Msg: &agentlinkpb.SidecarFrame_Status{
		Status: &agentlinkpb.Status{State: agentlinkpb.Status_STATE_ERROR, Reason: reason},
	}}
}

func readyFrame() *agentlinkpb.SidecarFrame {
	return &agentlinkpb.SidecarFrame{Msg: &agentlinkpb.SidecarFrame_Status{
		Status: &agentlinkpb.Status{State: agentlinkpb.Status_STATE_READY},
	}}
}

func (c *Client) configure(cfg *agentlinkpb.SandboxConfig) error {
	if c.cfg.Configure == nil {
		return nil
	}
	var err error
	c.configured.Do(func() { err = c.cfg.Configure(cfg) })
	return err
}

type pendingSpawn struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}

func (c *Client) startSpawn(ctx context.Context, streamID []byte, params *agentlinkpb.SessionParams) *pendingSpawn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent != nil || c.stopped {
		return nil
	}
	if c.spawn == nil {
		ctx, cancel := context.WithCancel(ctx)
		c.spawn = &pendingSpawn{done: make(chan struct{}), cancel: cancel}
		go c.runSpawn(ctx, c.spawn, streamID, params)
	}
	return c.spawn
}

func (c *Client) runSpawn(ctx context.Context, p *pendingSpawn, streamID []byte, params *agentlinkpb.SessionParams) {
	defer p.cancel()
	defer close(p.done)
	ag, err := c.cfg.Runner.Spawn(ctx, streamID, params)
	if err == nil && !c.adopt(ag) {
		stop(ag)
		err = errors.New("the agent started after the client stopped")
	}
	p.err = err
	if err == nil {
		c.log.Info("harness: agent spawned", "pid", ag.PID)
	}
}

func (c *Client) adopt(ag *AgentSession) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return false
	}
	c.agent, c.spawn = ag, nil
	return true
}

func (c *Client) agentState() (*AgentSession, *pendingSpawn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.agent, c.spawn
}

func (c *Client) currentAgent() *AgentSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.agent
}

func (c *Client) agentRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spawn != nil || c.agent != nil
}

func (c *Client) isStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped
}

func (c *Client) stopAgent() {
	c.mu.Lock()
	ag, pending := c.agent, c.spawn
	c.agent, c.stopped = nil, true
	c.mu.Unlock()
	if pending != nil {
		pending.cancel()
	}
	if ag != nil {
		stop(ag)
	}
}

func stop(ag *AgentSession) {
	_ = ag.Send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Stop{Stop: &runnerpb.Stop{}}})
	ag.Close()
}

func (c *Client) pumpAgentIO(ctx context.Context, ag *AgentSession, lost chan<- error) {
	select {
	case c.ioSlot <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-c.ioSlot }()
	if err := c.agentIO(ctx, ag); err != nil && ctx.Err() == nil && !errors.Is(err, errRunnerLost) {
		select {
		case lost <- err:
		default:
		}
	}
}

func (c *Client) agentIO(ctx context.Context, ag *AgentSession) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.client.AgentIO(ctx)
	if err != nil {
		return fmt.Errorf("open agent io: %w", err)
	}
	if err := stream.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Open{Open: &agentlinkpb.AgentIOOpen{
		StreamId: ag.StreamID,
	}}}); err != nil {
		return fmt.Errorf("send agent io open: %w", err)
	}
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("agent io closed before Open: %w", err)
	}
	open := first.GetOpen()
	if open == nil {
		return errors.New("the first manager AgentIO frame was not Open")
	}
	if !bytes.Equal(open.GetStreamId(), ag.StreamID) {
		c.log.Error("harness: agent io names a different stream", "manager", string(open.GetStreamId()), "runner", string(ag.StreamID))
		c.Fail(ReasonStreamMismatch)
		return nil
	}
	if err := ag.Send(&runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Replay{Replay: &runnerpb.Replay{
		After: open.GetConsumed(),
	}}}); err != nil {
		return fmt.Errorf("%w: %v", errRunnerLost, err)
	}
	ag.replays++
	c.log.Info("harness: agent io attached", "resume_after", open.GetConsumed())

	commands := make(chan error, 1)
	go func() { commands <- relayCommands(stream, ag) }()

	replaying := true
	for {
		select {
		case f := <-ag.Frames:
			switch {
			case f.GetReplayStart() != nil:
				ag.replayStarts++
				if ag.replayStarts < ag.replays {
					continue
				}
				replaying = false
				if err := stream.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_State{
					State: f.GetReplayStart().GetState(),
				}}); err != nil {
					return err
				}
			case f.GetEvent() != nil && !replaying:
				if err := stream.Send(&agentlinkpb.AgentIOFrame{Msg: &agentlinkpb.AgentIOFrame_Event{Event: f.GetEvent()}}); err != nil {
					return err
				}
			}
		case <-ag.Lost:
			return errRunnerLost
		case err := <-commands:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func relayCommands(stream agentlinkpb.AgentLinkService_AgentIOClient, ag *AgentSession) error {
	for {
		f, err := stream.Recv()
		if err != nil {
			return err
		}
		var out *runnerpb.RunnerClientFrame
		switch {
		case f.GetAck() != nil:
			out = &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Ack{Ack: f.GetAck()}}
		case f.GetPrompt() != nil:
			out = &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Prompt{Prompt: f.GetPrompt()}}
		case f.GetPermission() != nil:
			out = &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Permission{Permission: f.GetPermission()}}
		default:
			return fmt.Errorf("the manager sent an unexpected %T on AgentIO", f.GetMsg())
		}
		if err := ag.Send(out); err != nil {
			return fmt.Errorf("%w: %v", errRunnerLost, err)
		}
	}
}

func DialTargetFor(rawURL, dialAddress string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	if dialAddress != "" {
		return fmt.Sprintf("%s (via %s)", u.Host, dialAddress)
	}
	return u.Host
}
