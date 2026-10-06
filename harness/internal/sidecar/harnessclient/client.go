package harnessclient

import (
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

	"github.com/pomerium/agentops/harness/internal/agentio"
	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	"github.com/pomerium/agentops/harness/internal/pomeriumtls"
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
)

const deniedRetryLimit = 3

type TokenSource interface {
	Bearer() string
	Refresh()
}

type Runner interface {
	Spawn(ctx context.Context) (*AgentSession, error)
}

type AgentSession struct {
	IO     *agentio.Stream
	Exited <-chan AgentExit
	Stop   func()
	PID    int64

	acked ackLevel
}

type AgentExit struct {
	Code   int32
	Output uint64
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
	ReasonUnknownRun    = "unknown_run"
	ReasonRejected      = "attach_rejected"
	ReasonAttachDenied  = "attach_denied"
	ReasonAgentExited   = "agent_exited"
	ReasonShutdown      = "shutdown"
	ReasonLocalFailure  = "local_failure"
	ReasonRunnerFailure = "runner_unreachable"
	ReasonConfigFailed  = "config_failed"
)

type Client struct {
	cfg    Config
	log    *slog.Logger
	conn   *grpc.ClientConn
	client agentlinkpb.AgentLinkServiceClient

	statusCh   chan *agentlinkpb.Status
	ioSlot     chan struct{}
	configured sync.Once

	mu       sync.Mutex
	agent    *AgentSession
	spawn    *pendingSpawn
	exit     *AgentExit
	exitSent bool
	stopped  bool
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

func (c *Client) Run(ctx context.Context) error {
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
			ProtocolVersion: 1,
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
	interval, missLimit := c.heartbeatContract(ack)
	c.log.Info("harness attached", "attempt", attempt, "heartbeat", interval.String(), "miss_limit", missLimit,
		"endpoints", len(ack.GetConfig().GetEndpoints()))

	if err := c.configure(ack.GetConfig()); err != nil {
		c.log.Error("harness: applying the manager's configuration failed", "err", err)
		_ = stream.Send(statusFrame(ReasonConfigFailed))
		return &TerminalError{Reason: ReasonConfigFailed, Err: err}
	}
	if err := stream.Send(readyFrame()); err != nil {
		return err
	}

	return c.serve(ctx, parent, stream, interval, missLimit)
}

func (c *Client) heartbeatContract(ack *agentlinkpb.ManagerHelloAck) (time.Duration, uint32) {
	interval := c.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = time.Duration(ack.GetHeartbeatSeconds()) * time.Second
	}
	if interval <= 0 {
		interval = 20 * time.Second
	}
	missLimit := c.cfg.HeartbeatMissLimit
	if missLimit == 0 {
		missLimit = ack.GetHeartbeatMissLimit()
	}
	if missLimit == 0 {
		missLimit = 3
	}
	return interval, missLimit
}

func (c *Client) serve(ctx, runCtx context.Context, stream agentlinkpb.AgentLinkService_AttachClient,
	interval time.Duration, missLimit uint32,
) error {
	type recvResult struct {
		frame *agentlinkpb.ManagerFrame
		err   error
	}
	recvCh := make(chan recvResult, 1)
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

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastRecv := time.Now()
	lastSent := time.Now()
	deadline := time.Duration(missLimit) * interval

	var agentExited, exitAcked <-chan AgentExit
	var watched *AgentSession
	ioLost := make(chan error, 1)
	watch := func(ag *AgentSession) {
		watched = ag
		agentExited = c.agentExitChan()
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
				if p := c.startSpawn(runCtx); p != nil {
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
				_ = stream.Send(statusFrame(ReasonRunnerFailure))
				return &TerminalError{Reason: ReasonRunnerFailure, Err: pending.err}
			}
			if ag := c.currentAgent(); ag != nil {
				watch(ag)
			}
		case exit := <-agentExited:
			agentExited = nil
			c.recordExit(exit, false)
			c.log.Info("harness: agent process exited", "exit_code", exit.Code)
			exitAcked = watched.afterOutputAcked(ctx, exit)
		case exit := <-exitAcked:
			exitAcked = nil
			if err := stream.Send(&agentlinkpb.SidecarFrame{
				Msg: &agentlinkpb.SidecarFrame_Exited{Exited: &agentlinkpb.AgentExited{ExitCode: exit.Code}},
			}); err != nil {
				return err
			}
			c.recordExit(exit, true)
			lastSent = time.Now()
		case err := <-ioLost:
			return status.Errorf(codes.Unavailable, "agent io lost: %v", err)
		case st := <-c.statusCh:
			if err := stream.Send(&agentlinkpb.SidecarFrame{
				Msg: &agentlinkpb.SidecarFrame_Status{Status: st},
			}); err != nil {
				return err
			}
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

func (c *Client) startSpawn(ctx context.Context) *pendingSpawn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent != nil || c.stopped {
		return nil
	}
	if c.spawn == nil {
		ctx, cancel := context.WithCancel(ctx)
		c.spawn = &pendingSpawn{done: make(chan struct{}), cancel: cancel}
		go c.runSpawn(ctx, c.spawn)
	}
	return c.spawn
}

func (c *Client) runSpawn(ctx context.Context, p *pendingSpawn) {
	defer p.cancel()
	defer close(p.done)
	ag, err := c.cfg.Runner.Spawn(ctx)
	if err == nil && !c.adopt(ag) {
		ag.stop()
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
	c.agent, c.spawn, c.exit, c.exitSent = ag, nil, nil, false
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
	return c.spawn != nil || (c.agent != nil && !c.exitSent)
}

func (c *Client) agentExitChan() <-chan AgentExit {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.agent == nil || c.exitSent:
		return nil
	case c.exit != nil:
		unsent := make(chan AgentExit, 1)
		unsent <- *c.exit
		return unsent
	}
	return c.agent.Exited
}

func (c *Client) recordExit(exit AgentExit, sent bool) {
	c.mu.Lock()
	c.exit, c.exitSent = &exit, sent
	c.mu.Unlock()
}

type ackLevel struct {
	mu      sync.Mutex
	offset  uint64
	changed chan struct{}
}

func (a *ackLevel) advance(offset uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if offset <= a.offset {
		return
	}
	a.offset = offset
	if a.changed != nil {
		close(a.changed)
		a.changed = nil
	}
}

func (a *ackLevel) await(ctx context.Context, offset uint64) error {
	for {
		a.mu.Lock()
		if a.offset >= offset {
			a.mu.Unlock()
			return nil
		}
		if a.changed == nil {
			a.changed = make(chan struct{})
		}
		changed := a.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (ag *AgentSession) afterOutputAcked(ctx context.Context, exit AgentExit) <-chan AgentExit {
	acked := make(chan AgentExit, 1)
	go func() {
		if ag.acked.await(ctx, exit.Output) == nil {
			acked <- exit
		}
	}()
	return acked
}

type ackObserver struct {
	agentio.Transport
	acked *ackLevel
}

func (o ackObserver) Recv() (*agentlinkpb.AgentIOFrame, error) {
	f, err := o.Transport.Recv()
	if ack := f.GetAck(); ack != nil {
		o.acked.advance(ack.GetConsumed())
	}
	return f, err
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
		ag.stop()
	}
}

func (ag *AgentSession) stop() {
	ag.IO.Close(errors.New("session ended"))
	if ag.Stop != nil {
		ag.Stop()
	}
}

func (c *Client) pumpAgentIO(ctx context.Context, ag *AgentSession, lost chan<- error) {
	select {
	case c.ioSlot <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-c.ioSlot }()
	if err := c.agentIO(ctx, ag); err != nil && ctx.Err() == nil {
		select {
		case lost <- err:
		default:
		}
	}
}

func (c *Client) agentIO(ctx context.Context, ag *AgentSession) error {
	stream, err := c.client.AgentIO(ctx)
	if err != nil {
		return fmt.Errorf("open agent io: %w", err)
	}
	if err := stream.Send(agentio.OpenFrame(ag.IO.Consumed())); err != nil {
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
	if err := ag.IO.ValidateResume(open.GetConsumed()); err != nil {
		c.log.Error("harness: agent io resume is unserviceable", "err", err)
		c.Fail("agentio_buffer_overflow")
		return nil
	}
	ag.acked.advance(open.GetConsumed())
	ag.IO.StartRecorder()
	c.log.Info("harness: agent io attached", "peer_consumed", open.GetConsumed(), "our_consumed", ag.IO.Consumed())

	err = ag.IO.Pump(ctx, ackObserver{Transport: stream, acked: &ag.acked}, open.GetConsumed(), agentio.WithStop(ctx.Done()))
	if errors.Is(err, agentio.ErrProtocol) {
		c.log.Error("harness: agent io protocol violation", "err", err)
		c.Fail("agentio_protocol_violation")
		return nil
	}
	return err
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
