package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

func TestConfigFromEnv(t *testing.T) {
	for _, key := range []string{"LOG_LEVEL", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_INSECURE", "OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", "OTEL_METRIC_EXPORT_INTERVAL"} {
		t.Setenv(key, "")
	}
	c, err := ConfigFromEnv()
	if err != nil || c.Endpoint != "" || c.Level != slog.LevelInfo || c.SampleRatio != 1 || c.MetricInterval != 15*time.Second {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"LOG_LEVEL", "verbose"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "https://localhost:4317"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "http://user:secret@localhost:4317"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317/path"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost"}, {"OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"}, {"OTEL_EXPORTER_OTLP_INSECURE", "false"}, {"OTEL_TRACES_SAMPLER", "always_on"}, {"OTEL_TRACES_SAMPLER_ARG", "NaN"}, {"OTEL_TRACES_SAMPLER_ARG", "Inf"}, {"OTEL_TRACES_SAMPLER_ARG", "1.1"}, {"OTEL_TRACES_SAMPLER_ARG", "-1"}, {"OTEL_METRIC_EXPORT_INTERVAL", "0"}, {"OTEL_METRIC_EXPORT_INTERVAL", "9223372036854775807"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://[::1]:4317")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "1000")
	c, err = ConfigFromEnv()
	if err != nil || c.Endpoint != "[::1]:4317" || c.SampleRatio != .25 || c.MetricInterval != time.Second {
		t.Fatalf("valid config: %+v %v", c, err)
	}
}

func TestLocalJSONCorrelationLevelAndResourceIdentity(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "chosen")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=attribute,service.version=custom,service.instance.id=shared")
	var output bytes.Buffer
	r, err := New(context.Background(), Config{Level: slog.LevelWarn}, &output)
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled}))
	r.Logger.With("worker_id", "worker-1").InfoContext(ctx, "filtered", "event", "filtered")
	r.Logger.With("worker_id", "worker-1").WarnContext(ctx, "visible", "event", "test", "duration_ms", 1.5)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["service.name"] != "chosen" || record["service.instance.id"] != r.InstanceID || record["event"] != "test" || record["worker_id"] != "worker-1" || record["trace_id"] == nil || record["span_id"] == nil || record["duration_ms"] != 1.5 {
		t.Fatalf("record: %v", record)
	}
	second, err := New(context.Background(), Config{}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if second.InstanceID == r.InstanceID || r.InstanceID == "shared" {
		t.Fatal("process identities collapsed")
	}
	d := diagnostics{logger: r.Logger}
	d.Handle(nil)
	d.Handle(nil)
	if d.suppressed != 1 {
		t.Fatal("diagnostics not rate limited")
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
