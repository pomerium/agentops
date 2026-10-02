package apistub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	HeaderClient = "X-Apistub-Client"

	HeaderScenario = "X-Apistub-Scenario"

	HeaderScenarioKey = "X-Apistub-Scenario-Key"
)

const DefaultClient = "stub"

const (
	ScenarioUnknownField = "unknown-field"

	ScenarioNoFrames = "no-frames"

	ScenarioSilent = "silent"

	ScenarioDropAfter = "drop-after"

	ScenarioMute = "mute"
)

const unknownJSONField = "aFieldFromALaterVersion"

const unknownFieldNumber = 1999

const (
	compressedFlag byte = 0x01
	endStreamFlag  byte = 0x02
)

type scenario struct {
	name string
	n    int
}

func parseScenario(raw string) (scenario, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return scenario{}, false
	}
	name, arg, hasArg := strings.Cut(raw, "=")
	sc := scenario{name: name}
	if hasArg {
		n, err := strconv.Atoi(arg)
		if err != nil {
			return scenario{}, false
		}
		sc.n = n
	}
	switch sc.name {
	case ScenarioUnknownField, ScenarioNoFrames, ScenarioSilent, ScenarioMute:
		return sc, true
	case ScenarioDropAfter:
		return sc, hasArg
	}
	return scenario{}, false
}

type scenarios struct {
	next http.Handler

	mu   sync.Mutex
	used map[string]struct{}
}

func newScenarios(next http.Handler) *scenarios {
	return &scenarios{next: next, used: map[string]struct{}{}}
}

func (m *scenarios) pick(r *http.Request) (scenario, bool) {
	sc, ok := parseScenario(r.Header.Get(HeaderScenario))
	if !ok {
		return scenario{}, false
	}
	key := r.Header.Get(HeaderScenarioKey)
	if key == "" {
		return sc, true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, spent := m.used[key]; spent {
		return sc, false
	}
	m.used[key] = struct{}{}
	return sc, true
}

func (m *scenarios) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sc, ok := m.pick(r)
	if !ok {
		m.next.ServeHTTP(w, r)
		return
	}

	r.Header.Del("Accept-Encoding")
	r.Header.Del("Connect-Accept-Encoding")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	rw := &rewriter{ResponseWriter: w, sc: sc, cancel: cancel}
	m.next.ServeHTTP(rw, r.WithContext(ctx))
	rw.finish()
}

type rewriter struct {
	http.ResponseWriter
	sc     scenario
	cancel context.CancelFunc

	wroteHeader bool

	streaming bool
	jsonCodec bool
	rewrite   bool

	pending []byte
	unary   []byte
	frames  int

	synthesized bool
}

func (w *rewriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	ct := w.Header().Get("Content-Type")
	w.streaming = strings.HasPrefix(ct, "application/connect+")
	w.jsonCodec = strings.HasSuffix(ct, "json")

	w.rewrite = code == http.StatusOK
	if w.rewrite {

		w.Header().Del("Content-Length")
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *rewriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !w.rewrite {
		return w.ResponseWriter.Write(p)
	}
	if !w.streaming {

		w.unary = append(w.unary, p...)
		return len(p), nil
	}
	w.pending = append(w.pending, p...)
	if err := w.drainEnvelopes(); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *rewriter) drainEnvelopes() error {
	for {
		if len(w.pending) < 5 {
			return nil
		}
		length := int(uint32(w.pending[1])<<24 | uint32(w.pending[2])<<16 |
			uint32(w.pending[3])<<8 | uint32(w.pending[4]))
		if len(w.pending) < 5+length {
			return nil
		}
		flags := w.pending[0]
		payload := w.pending[5 : 5+length]
		w.pending = w.pending[5+length:]

		endStream := flags&endStreamFlag != 0
		compressed := flags&compressedFlag != 0

		switch w.sc.name {
		case ScenarioMute:

			continue

		case ScenarioSilent:

			if w.frames > 0 {
				continue
			}
			w.frames++

		case ScenarioNoFrames:

			if !w.synthesized {
				w.synthesized = true

				if err := w.emit(endStreamFlag, []byte("{}")); err != nil {
					return err
				}
				w.Flush()
				w.cancel()
			}
			continue

		case ScenarioDropAfter:
			if endStream {
				break
			}
			if w.frames >= w.sc.n {

				w.cancel()
				continue
			}
			w.frames++

		case ScenarioUnknownField:
			if endStream || compressed {
				break
			}
			injected, err := w.inject(payload)
			if err != nil {
				return err
			}
			payload = injected
		}

		if err := w.emit(flags, payload); err != nil {
			return err
		}
	}
}

func (w *rewriter) emit(flags byte, payload []byte) error {
	head := [5]byte{flags,
		byte(len(payload) >> 24), byte(len(payload) >> 16),
		byte(len(payload) >> 8), byte(len(payload))}
	if _, err := w.ResponseWriter.Write(head[:]); err != nil {
		return err
	}
	_, err := w.ResponseWriter.Write(payload)
	return err
}

func (w *rewriter) inject(payload []byte) ([]byte, error) {
	if !w.jsonCodec {

		return protowire.AppendString(
			protowire.AppendTag(payload, unknownFieldNumber, protowire.BytesType),
			"a value from a later version"), nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, fmt.Errorf("apistub: rewrite a JSON message: %w", err)
	}
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	fields[unknownJSONField] = json.RawMessage(`"a value from a later version"`)
	return json.Marshal(fields)
}

func (w *rewriter) finish() {
	if !w.rewrite || w.streaming || len(w.unary) == 0 {
		return
	}
	body := w.unary
	if w.sc.name == ScenarioUnknownField {
		if injected, err := w.inject(body); err == nil {
			body = injected
		}
	}
	_, _ = w.ResponseWriter.Write(body)
}

func (w *rewriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
