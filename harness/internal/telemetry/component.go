package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type Component struct {
	log   *slog.Logger
	name  string
	level slog.Level
}

func New(log *slog.Logger, name string, level slog.Level, attrs ...any) *Component {
	base := log.With(append([]any{"component", name}, attrs...)...)
	return &Component{log: base, name: name, level: level}
}

func (c *Component) Logger(ctx context.Context) *slog.Logger {
	return c.log.With(fieldsFrom(ctx)...)
}

func (c *Component) Debug(ctx context.Context, msg string, args ...any) {
	c.Logger(ctx).DebugContext(ctx, msg, args...)
}
func (c *Component) Warn(ctx context.Context, msg string, args ...any) {
	c.Logger(ctx).WarnContext(ctx, msg, args...)
}
func (c *Component) Error(ctx context.Context, msg string, args ...any) {
	c.Logger(ctx).ErrorContext(ctx, msg, args...)
}

func (c *Component) Start(ctx context.Context, op string, attrs ...any) (context.Context, *Operation) {
	ctx = With(ctx, attrs...)
	l := c.log.With(fieldsFrom(ctx)...)
	msg := c.name + "." + op
	l.Log(ctx, c.level, msg, "event", "start")
	return ctx, &Operation{c: c, ctx: ctx, log: l, msg: msg, start: time.Now()}
}

type Operation struct {
	c     *Component
	ctx   context.Context
	log   *slog.Logger
	msg   string
	start time.Time
	done  bool
}

func (op *Operation) Complete(attrs ...any) {
	if op.done {
		return
	}
	op.done = true
	args := append([]any{"event", "done", "duration_ms", op.elapsedMS()}, attrs...)
	op.log.Log(op.ctx, op.c.level, op.msg, args...)
}

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

type ctxKey struct{}

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

func slogArgs(args []any) []slog.Attr {
	var attrs []slog.Attr
	for i := 0; i+1 < len(args); i += 2 {
		key, _ := args[i].(string)
		attrs = append(attrs, slog.Any(key, args[i+1]))
	}
	return attrs
}
