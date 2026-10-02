package observability

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Apply the same level to both JSON and OTEL; MultiHandler accepts either child.
type contextHandler struct {
	slog.Handler
	level slog.Level
}

func (h contextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level && h.Handler.Enabled(ctx, level)
}
func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}
func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs), h.level}
}
func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name), h.level}
}

// Exporter diagnostics bypass the bridge, preventing recursive export failures.
type diagnostics struct {
	mu         sync.Mutex
	logger     *slog.Logger
	last       time.Time
	suppressed int
}

func (d *diagnostics) Handle(_ error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Since(d.last) < 30*time.Second {
		d.suppressed++
		return
	}
	d.logger.Warn("telemetry export failed", "event", "telemetry.export.error", "error_class", "export_failure", "suppressed", d.suppressed)
	d.last = time.Now()
	d.suppressed = 0
}
