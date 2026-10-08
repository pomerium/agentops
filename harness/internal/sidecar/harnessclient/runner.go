package harnessclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

var ErrNoAgent = errors.New("the runner has no agent session")

const runnerFrameBuffer = 64

const closeGrace = 2 * time.Second

type UDSRunner struct {
	socket string
	log    *slog.Logger
	conn   *grpc.ClientConn
}

func NewUDSRunner(socket string, log *slog.Logger) (*UDSRunner, error) {
	if socket == "" {
		return nil, errors.New("harness client: runner socket path is empty")
	}
	if log == nil {
		log = slog.Default()
	}
	conn, err := grpc.NewClient("unix:"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial agent runner at %s: %w", socket, err)
	}
	return &UDSRunner{socket: socket, log: log, conn: conn}, nil
}

func (r *UDSRunner) Close() { _ = r.conn.Close() }

func (r *UDSRunner) Spawn(ctx context.Context, streamID []byte, params *agentlinkpb.SessionParams) (*AgentSession, error) {
	return r.open(ctx, &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Spawn{Spawn: &runnerpb.Spawn{
		StreamId: streamID, Session: params,
	}}})
}

func (r *UDSRunner) Join(ctx context.Context) (*AgentSession, error) {
	return r.open(ctx, &runnerpb.RunnerClientFrame{Msg: &runnerpb.RunnerClientFrame_Join{Join: &runnerpb.Join{}}})
}

func (r *UDSRunner) open(ctx context.Context, first *runnerpb.RunnerClientFrame) (*AgentSession, error) {
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopWatch := context.AfterFunc(ctx, cancel)
	fail := func(err error) (*AgentSession, error) {
		stopWatch()
		cancel()
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, err
	}
	stream, err := runnerpb.NewAgentRunnerServiceClient(r.conn).Run(streamCtx)
	if err != nil {
		return fail(fmt.Errorf("open runner stream: %w", err))
	}
	if err := stream.Send(first); err != nil {
		return fail(fmt.Errorf("send %T: %w", first.GetMsg(), err))
	}
	f, err := stream.Recv()
	if status.Code(err) == codes.NotFound {
		return fail(ErrNoAgent)
	}
	if err != nil {
		return fail(fmt.Errorf("await the runner: %w", err))
	}
	started := f.GetStarted()
	if started == nil {
		return fail(fmt.Errorf("first runner frame was %v, want Started", f))
	}
	if !stopWatch() {
		return fail(ctx.Err())
	}

	frames := make(chan *runnerpb.RunnerServerFrame, runnerFrameBuffer)
	lost := make(chan struct{})
	go func() {
		defer close(lost)
		for {
			f, err := stream.Recv()
			if err != nil {
				switch {
				case streamCtx.Err() != nil:
				case errors.Is(err, io.EOF):
					r.log.Debug("harness: runner stream closed")
				default:
					r.log.Warn("harness: runner stream ended", "err", err)
				}
				return
			}
			select {
			case frames <- f:
			case <-streamCtx.Done():
				return
			}
		}
	}()
	var mu sync.Mutex
	var closeOnce sync.Once
	return &AgentSession{
		StreamID: started.GetStreamId(),
		PID:      started.GetPid(),
		Frames:   frames,
		Lost:     lost,
		Send: func(f *runnerpb.RunnerClientFrame) error {
			mu.Lock()
			defer mu.Unlock()
			return stream.Send(f)
		},
		Close: func() {
			closeOnce.Do(func() {
				mu.Lock()
				_ = stream.CloseSend()
				mu.Unlock()
				select {
				case <-lost:
				case <-time.After(closeGrace):
				}
				cancel()
			})
		},
	}, nil
}
