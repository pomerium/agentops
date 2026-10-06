package agentio

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

const (
	FrameMax    = 64 << 10
	ReplayMax   = 8 << 20
	AckBytes    = 512 << 10
	AckInterval = 500 * time.Millisecond
)

var ErrClosed = errors.New("agent io closed")

type Stream struct {
	outW *io.PipeWriter
	outR *io.PipeReader
	inW  *io.PipeWriter
	inR  *io.PipeReader

	recorderOnce sync.Once

	mu         sync.Mutex
	cond       *sync.Cond
	outSeq     uint64
	outAcked   uint64
	replay     []byte
	inConsumed uint64
	closed     bool
}

func New() *Stream {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	s := &Stream{outW: outW, outR: outR, inW: inW, inR: inR}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Stream) Outbound() io.Writer { return s.outW }

func (s *Stream) Inbound() io.Reader { return s.inR }

func (s *Stream) Close(cause error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
	if cause == nil {
		cause = ErrClosed
	}
	_ = s.outR.CloseWithError(cause)
	_ = s.outW.CloseWithError(cause)
	_ = s.inR.CloseWithError(cause)
	_ = s.inW.CloseWithError(cause)
}

func (s *Stream) StartRecorder() {
	s.recorderOnce.Do(func() {
		go func() {
			buf := make([]byte, FrameMax)
			for {
				if !s.awaitRoom() {
					return
				}
				n, err := s.outR.Read(buf)
				if n > 0 {
					s.Record(buf[:n])
				}
				if err != nil {
					return
				}
			}
		}()
	})
}

func (s *Stream) awaitRoom() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.replay) >= ReplayMax && !s.closed {
		s.cond.Wait()
	}
	return !s.closed
}

func (s *Stream) Record(p []byte) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replay = append(s.replay, p...)
	s.outSeq += uint64(len(p))
	s.cond.Broadcast()
	return s.outSeq
}

func (s *Stream) ValidateResume(offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case offset > s.outSeq:
		return fmt.Errorf("resume offset %d is beyond the %d bytes recorded", offset, s.outSeq)
	case offset < s.outAcked:
		return fmt.Errorf("resume offset %d was already evicted (acked through %d)", offset, s.outAcked)
	}
	if offset > s.outAcked {
		s.replay = s.replay[offset-s.outAcked:]
		s.outAcked = offset
		s.cond.Broadcast()
	}
	return nil
}

func (s *Stream) FramesAfter(cursor uint64) ([]*agentlinkpb.AgentIOFrame, uint64, error) {
	s.mu.Lock()
	for s.outSeq == cursor && !s.closed {
		s.cond.Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return nil, cursor, ErrClosed
	}
	if cursor < s.outAcked || cursor > s.outSeq {
		outAcked, outSeq := s.outAcked, s.outSeq
		s.mu.Unlock()
		return nil, cursor, fmt.Errorf("agentio cursor %d outside the buffered range (%d, %d]", cursor, outAcked, outSeq)
	}
	pending := make([]byte, s.outSeq-cursor)
	copy(pending, s.replay[cursor-s.outAcked:])
	last := s.outSeq
	s.mu.Unlock()
	return DataFrames(pending, last), last, nil
}

func (s *Stream) Ack(offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case offset > s.outSeq:
		return fmt.Errorf("agentio ack %d exceeds the %d bytes recorded", offset, s.outSeq)
	case offset < s.outAcked:
		return fmt.Errorf("agentio ack %d regressed below %d", offset, s.outAcked)
	case offset == s.outAcked:
		return nil
	}
	s.replay = s.replay[offset-s.outAcked:]
	s.outAcked = offset
	s.cond.Broadcast()
	return nil
}

func (s *Stream) DeliverInbound(seq uint64, payload []byte) error {
	s.mu.Lock()
	want := s.inConsumed + uint64(len(payload))
	s.mu.Unlock()
	if seq != want {
		return fmt.Errorf("agentio data seq %d is not contiguous (expected %d)", seq, want)
	}
	if len(payload) == 0 {
		return nil
	}
	if _, err := s.inW.Write(payload); err != nil {
		return err
	}
	s.mu.Lock()
	s.inConsumed = seq
	s.mu.Unlock()
	return nil
}

func (s *Stream) Consumed() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inConsumed
}

func DataFrames(payload []byte, lastSeq uint64) []*agentlinkpb.AgentIOFrame {
	if len(payload) == 0 {
		return nil
	}
	firstSeq := lastSeq - uint64(len(payload)) + 1
	frames := make([]*agentlinkpb.AgentIOFrame, 0, len(payload)/FrameMax+1)
	for off := 0; off < len(payload); off += FrameMax {
		end := min(off+FrameMax, len(payload))
		chunk := make([]byte, end-off)
		copy(chunk, payload[off:end])
		frames = append(frames, &agentlinkpb.AgentIOFrame{
			Msg: &agentlinkpb.AgentIOFrame_Data{Data: &agentlinkpb.AgentIOData{
				Seq:     firstSeq + uint64(end) - 1,
				Payload: chunk,
			}},
		})
	}
	return frames
}

func AckFrame(offset uint64) *agentlinkpb.AgentIOFrame {
	return &agentlinkpb.AgentIOFrame{
		Msg: &agentlinkpb.AgentIOFrame_Ack{Ack: &agentlinkpb.AgentIOAck{Consumed: offset}},
	}
}

func OpenFrame(offset uint64) *agentlinkpb.AgentIOFrame {
	return &agentlinkpb.AgentIOFrame{
		Msg: &agentlinkpb.AgentIOFrame_Open{Open: &agentlinkpb.AgentIOOpen{Consumed: offset}},
	}
}
