package server

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

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

func TestLineWriterBoundsOverlongLineAndKeepsGoing(t *testing.T) {
	t.Parallel()
	log, buf := captureLogs()
	w := newLineWriter(log, "stdout")

	huge := bytes.Repeat([]byte("x"), 4<<20)
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

	if got := len(w.buf); got > maxLogLine {
		t.Errorf("writer retained %d bytes, want <= %d (bounded)", got, maxLogLine)
	}
	if !strings.Contains(buf.String(), "after the giant line") {
		t.Errorf("line after overlong run not logged; log = %s", buf.String())
	}
}
