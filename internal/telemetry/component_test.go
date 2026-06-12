package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/pomerium/agentops/internal/telemetry"
)

// capture returns a logger writing JSON records into buf at the given level.
func capture(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	h := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})
	return slog.New(h), buf
}

func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestStartCompleteLogsAtComponentLevel(t *testing.T) {
	log, buf := capture(slog.LevelDebug)
	c := telemetry.New(log, "sess", slog.LevelDebug)

	_, op := c.Start(context.Background(), "HandleMessage")
	op.Complete()

	recs := records(t, buf)
	// Expect a start (debug) and a done record, both tagged with the component
	// and the operation, the done one carrying a duration.
	var done map[string]any
	for _, r := range recs {
		if r["event"] == "done" {
			done = r
		}
	}
	if done == nil {
		t.Fatalf("no done record in %v", recs)
	}
	if done["component"] != "sess" {
		t.Errorf("component = %v", done["component"])
	}
	if done["msg"] != "sess.HandleMessage" {
		t.Errorf("msg = %v", done["msg"])
	}
	if _, ok := done["duration_ms"]; !ok {
		t.Errorf("missing duration_ms in %v", done)
	}
}

func TestFailureLogsErrorAndWraps(t *testing.T) {
	log, buf := capture(slog.LevelDebug)
	c := telemetry.New(log, "sandbox", slog.LevelDebug)

	_, op := c.Start(context.Background(), "Launch")
	sentinel := errors.New("boom")
	err := op.Failure(sentinel)

	if !errors.Is(err, sentinel) {
		t.Errorf("Failure must wrap the original error; got %v", err)
	}
	if !strings.Contains(err.Error(), "sandbox.Launch") {
		t.Errorf("wrapped error should name the operation; got %q", err.Error())
	}
	var errRec map[string]any
	for _, r := range records(t, buf) {
		if r["level"] == "ERROR" {
			errRec = r
		}
	}
	if errRec == nil {
		t.Fatalf("expected an ERROR record")
	}
	if errRec["err"] == nil || errRec["msg"] != "sandbox.Launch" {
		t.Errorf("error record wrong: %v", errRec)
	}
	// Completing after a failure must not emit a second 'done' record.
	op.Complete()
	for _, r := range records(t, buf) {
		if r["event"] == "done" {
			t.Errorf("Complete after Failure should be a no-op, got done record")
		}
	}
}

func TestSuccessTraceGatedByHandlerLevel(t *testing.T) {
	// Component level is Debug, but the handler only admits Info+ — the success
	// trace must be suppressed while failures still surface.
	log, buf := capture(slog.LevelInfo)
	c := telemetry.New(log, "broker", slog.LevelDebug)

	_, op := c.Start(context.Background(), "AccessToken")
	op.Complete()
	if len(records(t, buf)) != 0 {
		t.Errorf("debug-level trace should be suppressed at Info handler, got %v", buf.String())
	}

	_, op2 := c.Start(context.Background(), "AccessToken")
	_ = op2.Failure(errors.New("x"))
	if n := len(records(t, buf)); n != 1 {
		t.Errorf("failure should always surface; got %d records", n)
	}
}

func TestContextFieldsPropagate(t *testing.T) {
	log, buf := capture(slog.LevelDebug)
	c := telemetry.New(log, "session", slog.LevelDebug)

	ctx := telemetry.With(context.Background(), "session_id", "s-123", "thread_ts", "1.2")
	_, op := c.Start(ctx, "runTurn")
	op.Complete()

	found := false
	for _, r := range records(t, buf) {
		if r["session_id"] == "s-123" && r["thread_ts"] == "1.2" {
			found = true
		}
	}
	if !found {
		t.Errorf("context fields should appear on records: %v", buf.String())
	}
}

func TestAdHocDebugUsesComponentAndContextFields(t *testing.T) {
	log, buf := capture(slog.LevelDebug)
	c := telemetry.New(log, "acp", slog.LevelDebug)
	ctx := telemetry.With(context.Background(), "session_id", "abc")

	c.Debug(ctx, "session update", "update_kind", "agent_message_chunk")

	recs := records(t, buf)
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	r := recs[0]
	if r["component"] != "acp" || r["session_id"] != "abc" || r["update_kind"] != "agent_message_chunk" {
		t.Errorf("ad-hoc debug record wrong: %v", r)
	}
}
