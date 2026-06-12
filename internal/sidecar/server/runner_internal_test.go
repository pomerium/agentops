package server

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureLogs returns a logger writing JSON lines into the returned buffer,
// guarded so concurrent writes from the writer-under-test are safe.
func captureLogs() (*slog.Logger, *syncBuf) {
	buf := &syncBuf{}
	return slog.New(slog.NewJSONHandler(buf, nil)), buf
}

type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// Finding #3: the old lineLogger used an io.Pipe whose writer was never
// closed, so the final unterminated line was silently dropped (and a scanner
// goroutine leaked). A synchronous line writer must flush the trailing partial
// line on Flush.
func TestLineWriterFlushesTrailingPartialLine(t *testing.T) {
	t.Parallel()
	log, buf := captureLogs()
	w := newLineWriter(log, "stderr")

	if _, err := w.Write([]byte("partial line with no newline")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if strings.Contains(buf.String(), "partial line") {
		t.Fatal("line logged before newline/flush; want buffered until Flush")
	}
	w.Flush()
	if !strings.Contains(buf.String(), "partial line with no newline") {
		t.Errorf("trailing partial line not flushed; log = %s", buf.String())
	}
}

// Finding #3: the old code capped bufio.Scanner at 1MB and, on a longer line,
// the scanner goroutine exited without draining the pipe — wedging envoy once
// the OS pipe buffer filled. A synchronous bounded writer must never block on a
// huge newline-free run and must keep logging subsequent lines.
func TestLineWriterBoundsOverlongLineAndKeepsGoing(t *testing.T) {
	t.Parallel()
	log, buf := captureLogs()
	w := newLineWriter(log, "stdout")

	huge := bytes.Repeat([]byte("x"), 4<<20) // 4 MiB, no newline
	done := make(chan struct{})
	go func() {
		_, _ = w.Write(huge)
		_, _ = w.Write([]byte("\nafter the giant line\n"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Write blocked on an overlong newline-free run (wedge)")
	}

	// The writer must not retain the whole 4 MiB; the buffer is bounded.
	if got := w.buffered(); got > maxLogLine {
		t.Errorf("writer retained %d bytes, want <= %d (bounded)", got, maxLogLine)
	}
	// A line after the giant one is still logged intact.
	if !strings.Contains(buf.String(), "after the giant line") {
		t.Errorf("line after overlong run not logged; log = %s", buf.String())
	}
}
