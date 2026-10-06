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
	defaultBaseBackoff = 500 * time.Millisecond
	defaultMaxBackoff  = 30 * time.Second
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
	Exited <-chan int32
	Stop   func()
	PID    int64
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
	configured sync.Once

	mu       sync.Mutex
	agent    *AgentSession
	exit     *int32
	exitSent bool
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
	var attempt uint32

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		attempt++
		err := c.session(ctx, attempt)
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
			denied++
			if denied > deniedRetryLimit {
				return struct{}{}, backoff.Permanent(&TerminalError{Reason: ReasonAttachDenied, Err: err})
			}
			c.log.Warn("harness attach denied; refreshing the run token",
				"attempt", denied, "limit", deniedRetryLimit, "err", err)
			c.cfg.Token.Refresh()
			return struct{}{}, backoff.RetryAfter(c.cfg.BaseBackoff, err)
		default:
			denied = 0
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

func (c *Client) session(parent context.Context, attempt uint32) error {
	bearer := c.cfg.Token.Bearer()
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

	return c.serve(ctx, stream, interval, missLimit)
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

func (c *Client) serve(ctx context.Context, stream agentlinkpb.AgentLinkService_AttachClient,
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

	agentExited := c.agentExitChan()
	ioLost := make(chan error, 1)
	if ag := c.currentAgent(); ag != nil {
		go c.pumpAgentIO(ctx, ag, ioLost)
	}
	var spawned chan spawnResult

	for {
		select {
		case r := <-recvCh:
			if r.err != nil {
				return fmt.Errorf("attach stream ended: %w", r.err)
			}
			lastRecv = time.Now()
			switch {
			case r.frame.GetSpawn() != nil:
				if spawned == nil {
					spawned = make(chan spawnResult, 1)
					go func(done chan<- spawnResult) {
						ag, err := c.spawnAgent(ctx)
						done <- spawnResult{ag, err}
					}(spawned)
				}
			case r.frame.GetShutdown() != nil:
				c.log.Info("harness: shutdown directive received", "reason", r.frame.GetShutdown().GetReason())
				c.stopAgent()
				return nil
			case r.frame.GetHeartbeat() != nil:
			}
		case res := <-spawned:
			spawned = nil
			if res.err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				c.log.Error("harness: spawn failed", "err", res.err)
				_ = stream.Send(statusFrame(ReasonRunnerFailure))
				return &TerminalError{Reason: ReasonRunnerFailure, Err: res.err}
			}
			if res.agent != nil {
				agentExited = c.agentExitChan()
				go c.pumpAgentIO(ctx, res.agent, ioLost)
			}
		case code := <-agentExited:
			agentExited = nil
			c.recordExit(code, false)
			c.log.Info("harness: agent process exited", "exit_code", code)
			if err := stream.Send(&agentlinkpb.SidecarFrame{
				Msg: &agentlinkpb.SidecarFrame_Exited{Exited: &agentlinkpb.AgentExited{ExitCode: code}},
			}); err != nil {
				return err
			}
			c.recordExit(code, true)
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

type spawnResult struct {
	agent *AgentSession
	err   error
}

func (c *Client) spawnAgent(ctx context.Context) (*AgentSession, error) {
	if c.currentAgent() != nil {
		return nil, nil
	}
	ag, err := c.cfg.Runner.Spawn(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.agent, c.exit, c.exitSent = ag, nil, false
	c.mu.Unlock()
	c.log.Info("harness: agent spawned", "pid", ag.PID)
	return ag, nil
}

func (c *Client) currentAgent() *AgentSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.agent
}

func (c *Client) agentRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.agent != nil && c.exit == nil
}

func (c *Client) agentExitChan() <-chan int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.agent == nil || c.exitSent:
		return nil
	case c.exit != nil:
		unsent := make(chan int32, 1)
		unsent <- *c.exit
		return unsent
	}
	return c.agent.Exited
}

func (c *Client) recordExit(code int32, sent bool) {
	c.mu.Lock()
	c.exit, c.exitSent = &code, sent
	c.mu.Unlock()
}

func (c *Client) stopAgent() {
	c.mu.Lock()
	ag := c.agent
	c.agent = nil
	c.mu.Unlock()
	if ag == nil {
		return
	}
	ag.IO.Close(errors.New("session ended"))
	if ag.Stop != nil {
		ag.Stop()
	}
}

func (c *Client) pumpAgentIO(ctx context.Context, ag *AgentSession, lost chan<- error) {
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
	ag.IO.StartRecorder()
	c.log.Info("harness: agent io attached", "peer_consumed", open.GetConsumed(), "our_consumed", ag.IO.Consumed())

	err = ag.IO.Pump(ctx, stream, open.GetConsumed(), agentio.WithStop(ctx.Done()))
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
