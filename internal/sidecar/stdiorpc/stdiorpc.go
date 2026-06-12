// Package stdiorpc adapts a stdio byte stream — the stdin/stdout of a process
// exec'd in a sandbox pod — into a transport gRPC can run over. The sidecar
// serves on its own stdin/stdout via ListenOnce; agentops dials the
// exec stream's pipes via Dial.
//
// Because stdout carries gRPC HTTP/2 framing, processes serving over stdio
// must write all logs to stderr.
package stdiorpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// stdioAddr is the static net.Addr reported for stdio connections.
type stdioAddr struct{}

func (stdioAddr) Network() string { return "stdio" }
func (stdioAddr) String() string  { return "stdio" }

// Conn is a net.Conn over an (io.Reader, io.WriteCloser) pair. Deadlines are
// no-ops: the underlying pipes have no deadline support, so cancellation is
// handled at the RPC layer (contexts) and by closing the connection.
type Conn struct {
	r io.Reader
	w io.WriteCloser

	closeOnce sync.Once
	closeErr  error
	closeFn   func() error
}

// NewConn wraps r/w as a net.Conn. closeFn, if non-nil, runs once on Close
// after the writer is closed (e.g. to release the underlying exec stream).
func NewConn(r io.Reader, w io.WriteCloser, closeFn func() error) *Conn {
	return &Conn{r: r, w: w, closeFn: closeFn}
}

func (c *Conn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *Conn) Write(p []byte) (int, error) { return c.w.Write(p) }

// Close closes the writer (signalling EOF to the peer) and runs closeFn once.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		err := c.w.Close()
		if rc, ok := c.r.(io.Closer); ok {
			if cerr := rc.Close(); err == nil {
				err = cerr
			}
		}
		if c.closeFn != nil {
			if ferr := c.closeFn(); err == nil {
				err = ferr
			}
		}
		c.closeErr = err
	})
	return c.closeErr
}

func (c *Conn) LocalAddr() net.Addr              { return stdioAddr{} }
func (c *Conn) RemoteAddr() net.Addr             { return stdioAddr{} }
func (c *Conn) SetDeadline(time.Time) error      { return nil }
func (c *Conn) SetReadDeadline(time.Time) error  { return nil }
func (c *Conn) SetWriteDeadline(time.Time) error { return nil }

// onceListener yields a single pre-established conn to grpc.Server.Serve,
// then blocks until closed.
type onceListener struct {
	conn      net.Conn
	accepted  chan struct{} // closed after the conn has been handed out
	closed    chan struct{}
	closeOnce sync.Once
}

// ListenOnce returns a net.Listener whose Accept yields conn exactly once.
// Subsequent Accepts block until the listener is closed. Closing the listener
// does not close conn: its lifetime belongs to the gRPC server/transport.
func ListenOnce(conn net.Conn) net.Listener {
	return &onceListener{
		conn:     conn,
		accepted: make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

func (l *onceListener) Accept() (net.Conn, error) {
	select {
	case <-l.accepted:
	case <-l.closed:
		return nil, errors.New("stdiorpc: listener closed")
	default:
		close(l.accepted)
		return l.conn, nil
	}
	<-l.closed
	return nil, errors.New("stdiorpc: listener closed")
}

func (l *onceListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *onceListener) Addr() net.Addr { return stdioAddr{} }

// Dial returns a gRPC client connection running over conn. The connection is
// established lazily on first RPC; conn is handed out exactly once and a
// reconnect attempt fails rather than silently reusing a dead stream.
func Dial(conn net.Conn) (*grpc.ClientConn, error) {
	var once sync.Once
	return grpc.NewClient("passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			var c net.Conn
			once.Do(func() { c = conn })
			if c == nil {
				return nil, errors.New("stdiorpc: stdio connection already consumed")
			}
			return c, nil
		}),
	)
}
