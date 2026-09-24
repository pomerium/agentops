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

// The headers a conformance suite steers the stub with.
//
// Headers, rather than a control endpoint, because an SDK has to be able to set
// a request header anyway — the bearer token is one — so a suite can provoke
// every scenario through the SDK under test instead of through a side channel
// no real user exercises.
const (
	// HeaderClient carries the caller's identity, standing in for the subject the
	// production server reads off Pomerium's verified assertion.
	HeaderClient = "X-Apistub-Client"
	// HeaderScenario names a wire condition to produce; see parseScenario.
	HeaderScenario = "X-Apistub-Scenario"
	// HeaderScenarioKey scopes a scenario to the FIRST request bearing that key,
	// so a client that reconnects gets a working stream the second time. Without
	// it a "drop the stream" scenario would drop every attempt and a correct
	// client would reconnect forever.
	HeaderScenarioKey = "X-Apistub-Scenario-Key"
)

// DefaultClient is the identity a request with no HeaderClient is admitted
// under. Named rather than empty: an unidentified caller must not become a
// client with a blank id that collides with every other unidentified caller.
const DefaultClient = "stub"

// The scenario names.
const (
	// ScenarioUnknownField adds a field no build has ever heard of to every
	// response message — an extra JSON key on the JSON codec, an unknown
	// protobuf field on the binary one. It is the additive-compatibility promise
	// made concrete, and it is the one protobuf-es rejects by default.
	ScenarioUnknownField = "unknown-field"
	// ScenarioNoFrames ends a stream cleanly before it sends anything at all: the
	// first envelope a client sees is the end-of-stream. That is a log which is
	// already finished, and it is a SUCCESS with an empty feed rather than an
	// error — a client that reconnects on it re-opens a closed log forever.
	ScenarioNoFrames = "no-frames"
	// ScenarioSilent lets the opening keepalive through and then swallows
	// everything, keepalives included, leaving the connection open. Nothing is
	// wrong with the socket; the far end is simply gone, which is what dead-stream
	// detection exists for and what no healthy server will ever demonstrate.
	//
	// The opening frame is forwarded deliberately: a subscription is opened
	// synchronously and a client blocks on that first frame, so swallowing it
	// tests the open handshake rather than the liveness window. Both are worth
	// testing and they are not the same condition — this one is "the stream was
	// up and then died", which is the one the keepalive contract is about.
	ScenarioSilent = "silent"
	// ScenarioDropAfter ends the stream in mid-flight after N messages, as a
	// recycled proxy connection or a moved pod does. Written "drop-after=3".
	ScenarioDropAfter = "drop-after"
	// ScenarioMute swallows every envelope INCLUDING the opening keepalive and
	// holds the connection open: a hop that accepts a subscription and then
	// black-holes it.
	//
	// It is the one condition a client cannot ride out by waiting, because
	// opening a subscription is synchronous — so a client without a deadline on
	// the opening frame waits forever, with no error and no stream. Distinct from
	// ScenarioSilent, which lets the stream be established first and then tests
	// the liveness window.
	ScenarioMute = "mute"
)

// unknownJSONField is the key injected on the JSON path. protobuf-es v2 throws
// on an unrecognized key unless the transport sets ignoreUnknownFields, so this
// scenario is the difference between an SDK that survives the next release of
// the platform and one that stops parsing on the day a field is added.
const unknownJSONField = "aFieldFromALaterVersion"

// unknownFieldNumber is high enough that no plausible future field collides
// with it while staying inside the two-byte tag range.
const unknownFieldNumber = 1999

// The Connect envelope's flag bits: one byte, then four bytes of big-endian
// length, then the message.
const (
	compressedFlag byte = 0x01
	endStreamFlag  byte = 0x02
)

// scenario is one parsed instruction.
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

// scenarios is the middleware that produces the scripted wire conditions.
//
// It works on the response BYTES rather than through the service, because that
// is the only place these conditions live: api.API cannot express "this stream
// dies now", and the generated types cannot express "carrying a field that does
// not exist yet". Framing is the Connect envelope — one flags byte, four bytes
// of big-endian length, the message — in both directions.
type scenarios struct {
	next http.Handler

	mu   sync.Mutex
	used map[string]struct{}
}

func newScenarios(next http.Handler) *scenarios {
	return &scenarios{next: next, used: map[string]struct{}{}}
}

// pick reports the scenario a request should get, consuming its one-shot budget.
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
	// Rewriting a compressed body is not possible, so ask for an uncompressed
	// one. The server does not compress today; this keeps the scenario honest if
	// it ever starts.
	r.Header.Del("Accept-Encoding")
	r.Header.Del("Connect-Accept-Encoding")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	rw := &rewriter{ResponseWriter: w, sc: sc, cancel: cancel}
	m.next.ServeHTTP(rw, r.WithContext(ctx))
	rw.finish()
}

// rewriter is the ResponseWriter the scenarios act through.
type rewriter struct {
	http.ResponseWriter
	sc     scenario
	cancel context.CancelFunc

	wroteHeader bool
	// streaming and jsonCodec are read off the response content type, which is
	// the only place the codec in play is stated.
	streaming bool
	jsonCodec bool
	rewrite   bool

	// pending holds bytes of an envelope that has not arrived in full yet;
	// unary holds a whole small body, which is rewritten once it is complete.
	pending []byte
	unary   []byte
	frames  int
	// synthesized records that this response's end-of-stream was written by the
	// scenario rather than by the handler.
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
	// Only a successful response is rewritten. An error body is the transport's
	// own shape and a scenario about future fields has nothing to say about it.
	w.rewrite = code == http.StatusOK
	if w.rewrite {
		// The body length is about to change, and a stale Content-Length is worse
		// than none: the client would read a truncated message and blame its codec.
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
		// A unary body is one message, small, and only rewritable in full — so it
		// is buffered and written by finish().
		w.unary = append(w.unary, p...)
		return len(p), nil
	}
	w.pending = append(w.pending, p...)
	if err := w.drainEnvelopes(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// drainEnvelopes consumes every complete envelope now buffered and acts on it.
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
			// Nothing is ever forwarded, the opening frame included, and the
			// connection stays open behind the silence.
			continue

		case ScenarioSilent:
			// The opening frame goes through so the stream is established;
			// everything after it goes nowhere, behind a connection that stays open.
			if w.frames > 0 {
				continue
			}
			w.frames++

		case ScenarioNoFrames:
			// Every envelope is swallowed and replaced, once, by a clean
			// end-of-stream — so the first thing the client ever sees is the stream
			// finishing.
			//
			// Synthesized rather than forwarded, because the handler's own
			// end-of-stream only comes when the session's log is finished, and a
			// live session would keep the handler looping over swallowed keepalives
			// forever. The request is cancelled behind it so the handler unwinds.
			if !w.synthesized {
				w.synthesized = true
				// The end-of-stream frame is JSON in both codecs — that is the Connect
				// streaming protocol, not the message codec. An empty object is "the
				// stream ended and nothing went wrong".
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
				// Cancelling the request is what a recycled connection looks like from
				// the client's side: the stream ends mid-flight with an error rather
				// than with the clean end-of-stream that means "the log is over".
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

// emit writes one envelope with a length matching whatever the payload became.
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

// inject adds a field this build has never heard of to one encoded message.
func (w *rewriter) inject(payload []byte) ([]byte, error) {
	if !w.jsonCodec {
		// Protobuf's wire format is concatenative, so an unknown field is simply
		// appended: a conformant decoder keeps it aside and carries on.
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

// finish writes a buffered unary body, once it is whole enough to rewrite.
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

// Flush forwards the flush a stream depends on. Without it connect-go's
// type assertion fails and every subscription stalls in a buffer.
func (w *rewriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
