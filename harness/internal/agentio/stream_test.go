package agentio

import (
	"errors"
	"testing"
	"time"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func TestFramesAfterReplaysUnackedSuffixOnce(t *testing.T) {
	a := New()
	defer a.Close(nil)

	last := a.Record([]byte("hello world"))
	if last != 11 {
		t.Fatalf("record returned %d, want 11", last)
	}

	frames, cursor, err := a.FramesAfter(0)
	if err != nil {
		t.Fatalf("framesAfter: %v", err)
	}
	if got := collect(frames); got != "hello world" || cursor != 11 {
		t.Fatalf("first stream got %q cursor %d", got, cursor)
	}

	if err := a.ValidateResume(6); err != nil {
		t.Fatalf("validateResume: %v", err)
	}
	frames, cursor, err = a.FramesAfter(6)
	if err != nil {
		t.Fatalf("framesAfter after resume: %v", err)
	}
	if got := collect(frames); got != "world" || cursor != 11 {
		t.Fatalf("resumed stream got %q cursor %d", got, cursor)
	}
}

func TestValidateResumeRejectsEvictedAndFutureOffsets(t *testing.T) {
	a := New()
	defer a.Close(nil)
	a.Record([]byte("0123456789"))
	if err := a.Ack(8); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if err := a.ValidateResume(4); err == nil {
		t.Error("an offset below the acked point was evicted; resume must fail")
	}
	if err := a.ValidateResume(11); err == nil {
		t.Error("an offset beyond what was recorded must fail")
	}
	if err := a.ValidateResume(9); err != nil {
		t.Errorf("an offset inside the buffer must be serviceable: %v", err)
	}
}

func TestAckRejectsRegressionAndOverreach(t *testing.T) {
	a := New()
	defer a.Close(nil)
	a.Record([]byte("abcdef"))

	if err := a.Ack(4); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := a.Ack(3); err == nil {
		t.Error("a regressing ack must fail")
	}
	if err := a.Ack(7); err == nil {
		t.Error("an ack beyond what was recorded must fail")
	}
	if err := a.Ack(4); err != nil {
		t.Errorf("a repeated ack is a no-op: %v", err)
	}
}

func TestDeliverInboundRequiresContiguity(t *testing.T) {
	a := New()
	defer a.Close(nil)

	read := make(chan string, 1)
	go func() {
		buf := make([]byte, 3)
		n, _ := a.Inbound().Read(buf)
		read <- string(buf[:n])
	}()
	if err := a.DeliverInbound(3, []byte("abc")); err != nil {
		t.Fatalf("deliverInbound: %v", err)
	}
	if got := <-read; got != "abc" {
		t.Errorf("read %q", got)
	}
	if a.Consumed() != 3 {
		t.Errorf("consumed = %d, want 3", a.Consumed())
	}
	if err := a.DeliverInbound(10, []byte("xyz")); err == nil {
		t.Error("a non-contiguous frame must be rejected rather than silently accepted")
	}
}

func TestReplayCapBackpressuresTheWriter(t *testing.T) {
	a := New()
	defer a.Close(nil)

	a.mu.Lock()
	a.replay = make([]byte, ReplayMax)
	a.outSeq = uint64(ReplayMax)
	a.mu.Unlock()

	blocked := make(chan bool, 1)
	go func() { blocked <- a.awaitRoom() }()
	select {
	case <-blocked:
		t.Fatal("awaitRoom returned while the buffer was at its cap")
	case <-time.After(50 * time.Millisecond):
	}

	if err := a.Ack(uint64(ReplayMax)); err != nil {
		t.Fatalf("ack: %v", err)
	}
	select {
	case ok := <-blocked:
		if !ok {
			t.Error("awaitRoom reported closed after an ack freed room")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("awaitRoom did not wake after an ack freed room")
	}
}

func TestCloseWakesWaiters(t *testing.T) {
	a := New()

	waiting := make(chan error, 1)
	go func() {
		_, _, err := a.FramesAfter(0)
		waiting <- err
	}()
	time.Sleep(20 * time.Millisecond)
	a.Close(nil)
	select {
	case err := <-waiting:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("framesAfter err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("framesAfter did not wake on close")
	}
}

func TestDataFramesRespectTheFrameLimit(t *testing.T) {
	payload := make([]byte, FrameMax*2+7)
	for i := range payload {
		payload[i] = byte(i)
	}
	frames := DataFrames(payload, uint64(len(payload)))
	if len(frames) != 3 {
		t.Fatalf("got %d frames, want 3", len(frames))
	}
	var rebuilt []byte
	var last uint64
	for _, f := range frames {
		d := f.GetData()
		if len(d.GetPayload()) > FrameMax {
			t.Fatalf("frame carries %d bytes, over the %d limit", len(d.GetPayload()), FrameMax)
		}
		rebuilt = append(rebuilt, d.GetPayload()...)
		if d.GetSeq() != uint64(len(rebuilt)) {
			t.Fatalf("seq %d does not match the %d bytes so far", d.GetSeq(), len(rebuilt))
		}
		last = d.GetSeq()
	}
	if string(rebuilt) != string(payload) || last != uint64(len(payload)) {
		t.Error("frames do not reassemble to the original payload")
	}
}

func collect(frames []*agentlinkpb.AgentIOFrame) string {
	var out []byte
	for _, f := range frames {
		out = append(out, f.GetData().GetPayload()...)
	}
	return string(out)
}
