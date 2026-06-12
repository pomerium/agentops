// Package telemetry provides a lightweight, slog-based instrumentation helper
// modeled on Pomerium's telemetry.Component, but logging-only: no
// OpenTelemetry, no zerolog, no extra dependencies.
//
// A Component represents a subsystem (e.g. "gateway", "session", "acp"). Wrap a
// method with:
//
//	func (m *Manager) HandleMessage(ctx context.Context, in ...) (err error) {
//		ctx, op := m.tel.Start(ctx, "HandleMessage")
//		defer op.Complete()
//		...
//		if err != nil { return op.Failure(err) }
//	}
//
// Success/trace records are emitted at the Component's configured level
// (typically Debug, so they appear only when LOG_LEVEL=debug); failures always
// log at Error. Correlation fields attached with With() propagate to every
// record via the context.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Component instruments a named subsystem over an slog.Logger.
type Component struct {
	log   *slog.Logger
	name  string
	level slog.Level
}

// New returns a Component whose records are tagged with component=name (plus any
// base attrs). level is the level used for success/trace records; failures are
// always logged at Error.
func New(log *slog.Logger, name string, level slog.Level, attrs ...any) *Component {
	base := log.With(append([]any{"component", name}, attrs...)...)
	return &Component{log: base, name: name, level: level}
}

// Logger returns the component logger with any context correlation fields
// applied, for ad-hoc logging that isn't tied to an Operation.
func (c *Component) Logger(ctx context.Context) *slog.Logger {
	return c.log.With(fieldsFrom(ctx)...)
}

// Debug/Info/Warn/Error are convenience ad-hoc loggers (used e.g. for the ACP
// comms trace). They include the component and context correlation fields.
func (c *Component) Debug(ctx context.Context, msg string, args ...any) {
	c.Logger(ctx).DebugContext(ctx, msg, args...)
}
func (c *Component) Info(ctx context.Context, msg string, args ...any) {
	c.Logger(ctx).InfoContext(ctx, msg, args...)
}
func (c *Component) Warn(ctx context.Context, msg string, args ...any) {
	c.Logger(ctx).WarnContext(ctx, msg, args...)
}
func (c *Component) Error(ctx context.Context, msg string, args ...any) {
	c.Logger(ctx).ErrorContext(ctx, msg, args...)
}

// Start begins an operation: it logs a start record at the component level and
// returns a context (carrying op correlation fields) plus an Operation to close
// with Complete or Failure.
func (c *Component) Start(ctx context.Context, op string, attrs ...any) (context.Context, *Operation) {
	ctx = With(ctx, attrs...)
	l := c.log.With(fieldsFrom(ctx)...)
	msg := c.name + "." + op
	l.Log(ctx, c.level, msg, "event", "start")
	return ctx, &Operation{c: c, ctx: ctx, log: l, msg: msg, start: time.Now()}
}

// Operation tracks a single in-flight operation started by Start.
type Operation struct {
	c     *Component
	ctx   context.Context
	log   *slog.Logger
	msg   string
	start time.Time
	done  bool
}

// Complete marks the operation successful and logs a done record (with
// duration_ms) at the component level. It is idempotent and a no-op after
// Failure.
func (op *Operation) Complete(attrs ...any) {
	if op.done {
		return
	}
	op.done = true
	args := append([]any{"event", "done", "duration_ms", op.elapsedMS()}, attrs...)
	op.log.Log(op.ctx, op.c.level, op.msg, args...)
}

// Failure marks the operation failed, logs at Error with the error and
// duration, and returns the error wrapped with the operation name. It is a
// no-op (returns the wrapped error) if already closed.
func (op *Operation) Failure(err error, attrs ...any) error {
	wrapped := fmt.Errorf("%s: %w", op.msg, err)
	if op.done {
		return wrapped
	}
	op.done = true
	args := append([]any{"event", "error", "duration_ms", op.elapsedMS(), "err", err}, attrs...)
	op.log.LogAttrs(op.ctx, slog.LevelError, op.msg, slogArgs(args)...)
	return wrapped
}

func (op *Operation) elapsedMS() int64 { return time.Since(op.start).Milliseconds() }

// Active marks a long-running activity; call Done() when it ends.
func (c *Component) Active(ctx context.Context, name string, attrs ...any) (context.Context, *Active) {
	ctx = With(ctx, attrs...)
	l := c.log.With(fieldsFrom(ctx)...)
	msg := c.name + "." + name
	l.Log(ctx, c.level, msg, "event", "active")
	return ctx, &Active{ctx: ctx, log: l, level: c.level, msg: msg, start: time.Now()}
}

// Active is a handle to an ongoing activity.
type Active struct {
	ctx   context.Context
	log   *slog.Logger
	level slog.Level
	msg   string
	start time.Time
	done  bool
}

// Done logs the end of the activity with its duration.
func (a *Active) Done() {
	if a.done {
		return
	}
	a.done = true
	a.log.Log(a.ctx, a.level, a.msg, "event", "inactive", "duration_ms", time.Since(a.start).Milliseconds())
}

// --- context correlation fields ---------------------------------------------

type ctxKey struct{}

// With returns a context carrying the given key/value attrs, merged with any
// already present, so they appear on every telemetry record derived from it.
func With(ctx context.Context, attrs ...any) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	existing := fieldsFrom(ctx)
	merged := make([]any, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, ctxKey{}, merged)
}

func fieldsFrom(ctx context.Context) []any {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(ctxKey{}).([]any); ok {
		return v
	}
	return nil
}

// slogArgs converts a flat key/value arg slice into []slog.Attr for LogAttrs.
func slogArgs(args []any) []slog.Attr {
	var attrs []slog.Attr
	for i := 0; i+1 < len(args); i += 2 {
		key, _ := args[i].(string)
		attrs = append(attrs, slog.Any(key, args[i+1]))
	}
	return attrs
}
