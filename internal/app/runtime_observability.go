package app

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"nats-sandbox-runtime/internal/observability"
)

// Direct in-process callers get JSON logs with no network export.
var localRuntimeTelemetry = sync.OnceValue(func() *observability.Runtime {
	r, err := observability.New(context.Background(), observability.Config{Level: slog.LevelInfo}, os.Stderr)
	if err != nil {
		panic(err)
	}
	return r
})

func runtimeTelemetry(cfg RuntimePythonConfig) *observability.Runtime {
	if cfg.Telemetry != nil {
		return cfg.Telemetry
	}
	return localRuntimeTelemetry()
}

func redactedNATSURL(value string) string {
	var redacted []string
	for _, part := range strings.Split(value, ",") {
		u, err := url.Parse(strings.TrimSpace(part))
		if err != nil || u.Host == "" {
			redacted = append(redacted, "invalid")
			continue
		}
		u.User = nil
		u.RawQuery = ""
		u.Fragment = ""
		redacted = append(redacted, u.String())
	}
	return strings.Join(redacted, ",")
}

func endRuntimeSpan(span trace.Span, err error, class string) {
	if err != nil {
		span.SetStatus(codes.Error, class)
		span.SetAttributes(attribute.String("error.type", class))
	}
	span.End()
}

func observeWorkers(t *observability.Runtime, pool *RuntimeWorkerPool) (metric.Registration, error) {
	return t.Meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		snapshot := pool.Snapshot()
		var busy int64
		for _, worker := range snapshot.Workers {
			if worker.Status == runtimeWorkerBusy {
				busy++
			}
		}
		total := int64(len(snapshot.Workers))
		for state, count := range map[string]int64{"desired": int64(snapshot.DesiredCount), "total": total, "idle": total - busy, "busy": busy} {
			o.ObserveInt64(t.Workers, count, metric.WithAttributes(attribute.String("state", state)))
		}
		return nil
	}, t.Workers)
}
