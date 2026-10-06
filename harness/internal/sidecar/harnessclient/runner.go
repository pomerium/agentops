package harnessclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pomerium/agentops/harness/internal/agentio"
	runnerpb "github.com/pomerium/agentops/harness/internal/runner/pb"
)

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

func (r *UDSRunner) Spawn(ctx context.Context) (*AgentSession, error) {
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := runnerpb.NewAgentRunnerServiceClient(r.conn).Run(streamCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open runner stream: %w", err)
	}
	if err := stream.Send(&runnerpb.RunnerClientFrame{
		Msg: &runnerpb.RunnerClientFrame_Spawn{Spawn: &runnerpb.Spawn{}},
	}); err != nil {
		cancel()
		return nil, fmt.Errorf("send spawn: %w", err)
	}
	first, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("await agent start: %w", err)
	}
	started := first.GetStarted()
	if started == nil {
		cancel()
		return nil, fmt.Errorf("first runner frame was %v, want Started", first)
	}

	io := agentio.New()
	exited := make(chan int32, 1)
	session := &AgentSession{IO: io, Exited: exited, PID: started.GetPid()}
	session.Stop = func() {
		cancel()
	}

	go r.readRunner(stream, io, exited)
	go r.writeRunner(streamCtx, stream, io)
	return session, nil
}

func (r *UDSRunner) readRunner(stream runnerpb.AgentRunnerService_RunClient, tunnel *agentio.Stream, exited chan<- int32) {
	stderr := &lineLog{log: r.log}
	defer stderr.flush()
	for {
		frame, err := stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				r.log.Warn("harness: runner stream ended", "err", err)
			}
			select {
			case exited <- -1:
			default:
			}
			return
		}
		switch {
		case frame.GetStdout() != nil:
			if _, err := tunnel.Outbound().Write(frame.GetStdout()); err != nil {
				r.log.Debug("harness: tunnel closed while forwarding agent stdout", "err", err)
				return
			}
		case frame.GetStderr() != nil:
			stderr.write(frame.GetStderr())
		case frame.GetExited() != nil:
			stderr.flush()
			select {
			case exited <- frame.GetExited().GetExitCode():
			default:
			}
			return
		}
	}
}

func (r *UDSRunner) writeRunner(ctx context.Context, stream runnerpb.AgentRunnerService_RunClient, tunnel *agentio.Stream) {
	buf := make([]byte, agentio.FrameMax)
	for {
		n, err := tunnel.Inbound().Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if err := stream.Send(&runnerpb.RunnerClientFrame{
				Msg: &runnerpb.RunnerClientFrame_Stdin{Stdin: chunk},
			}); err != nil {
				r.log.Debug("harness: write to runner failed", "err", err)
				return
			}
		}
		if err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

type lineLog struct {
	log *slog.Logger
	buf bytes.Buffer
}

const maxStderrLine = 1 << 16

func (l *lineLog) write(p []byte) {
	l.buf.Write(p)
	for {
		line, err := l.buf.ReadString('\n')
		if err != nil {
			if len(line) > maxStderrLine {
				l.log.Debug("agent stderr", "line", line[:maxStderrLine]+"…(truncated)")
				l.buf.Reset()
				return
			}
			l.buf.WriteString(line)
			return
		}
		if t := strings.TrimRight(line, "\r\n"); t != "" {
			l.log.Debug("agent stderr", "line", t)
		}
	}
}

func (l *lineLog) flush() {
	if t := strings.TrimRight(l.buf.String(), "\r\n"); t != "" {
		l.log.Debug("agent stderr", "line", t)
	}
	l.buf.Reset()
}
