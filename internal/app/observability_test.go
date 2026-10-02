package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	collectlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collecttrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"nats-sandbox-runtime/internal/observability"
)

type otlpReceipt struct {
	mu            sync.Mutex
	spans         []*tracepb.ResourceSpans
	logs          []*logpb.ResourceLogs
	metrics       []*metricpb.ResourceMetrics
	mode          atomic.Int32 // 1 transient, 2 permanent, 3 stalled
	attempts      atomic.Int64
	traceAttempts atomic.Int64
}

func (r *otlpReceipt) failure(ctx context.Context) error {
	r.attempts.Add(1)
	switch r.mode.Load() {
	case 1:
		return status.Error(codes.Unavailable, "temporary")
	case 2:
		return status.Error(codes.PermissionDenied, "permanent")
	case 3:
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

type traceReceiver struct {
	collecttrace.UnimplementedTraceServiceServer
	r *otlpReceipt
}

func (s traceReceiver) Export(ctx context.Context, q *collecttrace.ExportTraceServiceRequest) (*collecttrace.ExportTraceServiceResponse, error) {
	s.r.traceAttempts.Add(1)
	if err := s.r.failure(ctx); err != nil {
		return nil, err
	}
	s.r.mu.Lock()
	s.r.spans = append(s.r.spans, q.ResourceSpans...)
	s.r.mu.Unlock()
	return &collecttrace.ExportTraceServiceResponse{}, nil
}

type logReceiver struct {
	collectlog.UnimplementedLogsServiceServer
	r *otlpReceipt
}

func (s logReceiver) Export(ctx context.Context, q *collectlog.ExportLogsServiceRequest) (*collectlog.ExportLogsServiceResponse, error) {
	if err := s.r.failure(ctx); err != nil {
		return nil, err
	}
	s.r.mu.Lock()
	s.r.logs = append(s.r.logs, q.ResourceLogs...)
	s.r.mu.Unlock()
	return &collectlog.ExportLogsServiceResponse{}, nil
}

type metricReceiver struct {
	collectmetric.UnimplementedMetricsServiceServer
	r *otlpReceipt
}

func (s metricReceiver) Export(ctx context.Context, q *collectmetric.ExportMetricsServiceRequest) (*collectmetric.ExportMetricsServiceResponse, error) {
	if err := s.r.failure(ctx); err != nil {
		return nil, err
	}
	s.r.mu.Lock()
	s.r.metrics = append(s.r.metrics, q.ResourceMetrics...)
	s.r.mu.Unlock()
	return &collectmetric.ExportMetricsServiceResponse{}, nil
}
func serveOTLP(t *testing.T, address string, r *otlpReceipt) (*grpc.Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	collecttrace.RegisterTraceServiceServer(server, traceReceiver{r: r})
	collectlog.RegisterLogsServiceServer(server, logReceiver{r: r})
	collectmetric.RegisterMetricsServiceServer(server, metricReceiver{r: r})
	go server.Serve(ln)
	t.Cleanup(server.Stop)
	return server, ln.Addr().String()
}
func newTestTelemetry(t *testing.T, endpoint string, ratio float64) *observability.Runtime {
	t.Helper()
	r, err := observability.New(context.Background(), observability.Config{Endpoint: endpoint, SampleRatio: ratio, MetricInterval: 100 * time.Millisecond}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	})
	return r
}
func awaitObservation(t *testing.T, d time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("observation deadline exceeded")
}
func pbAttr(attrs []*commonpb.KeyValue, key string) string {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value.GetStringValue()
		}
	}
	return ""
}
func (r *otlpReceipt) allSpans() []*tracepb.Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	var all []*tracepb.Span
	for _, rs := range r.spans {
		for _, scope := range rs.ScopeSpans {
			all = append(all, scope.Spans...)
		}
	}
	return all
}
func (r *otlpReceipt) metric(name string) *metricpb.Metric {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.metrics) - 1; i >= 0; i-- {
		for _, scope := range r.metrics[i].ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name == name {
					return m
				}
			}
		}
		return nil
	}
	return nil
}

func TestObservabilityPlaintextPropagationAndRuntime(t *testing.T) {
	rc := &otlpReceipt{}
	_, endpoint := serveOTLP(t, "127.0.0.1:0", rc)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://ignored.invalid:9999")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://ignored.invalid:9999")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "https://ignored.invalid:9999")
	r := newTestTelemetry(t, endpoint, 0) // sampled parent still records
	r.Logger.Debug("filtered debug", "event", "debug")
	_, root := r.Tracer.Start(context.Background(), "unsampled-root")
	if root.IsRecording() {
		t.Fatal("root sampler ignored")
	}
	root.End()
	server := startEmbeddedJetStreamNATSServer(t, -1)
	t.Cleanup(server.Shutdown)
	cfg := defaultRuntimePythonConfig()
	cfg.URL = server.ClientURL()
	cfg.Telemetry = r
	cfg.LocalPython.SnapshotDir = t.TempDir()
	cfg.ArtifactTTL = 0
	registration, err := startRuntimePythonService(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close()
	service := registration.runtime
	service.execute = func(ctx context.Context, cfg LocalPythonConfig) (LocalPythonExecResult, error) {
		if !trace.SpanFromContext(ctx).IsRecording() {
			t.Fatal("execution lost parent")
		}
		if err := os.WriteFile(filepath.Join(cfg.WorkspaceDir, "result.txt"), []byte("artifact"), 0600); err != nil {
			return LocalPythonExecResult{}, err
		}
		return LocalPythonExecResult{Stdout: []byte("guest output")}, nil
	}
	parent := "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
	run := newRuntimePythonFakeRequest([]byte(`{"run_id":"fixture","code":"private source"}`))
	nats.Header(run.headers).Set("traceparent", parent)
	nats.Header(run.headers).Set("tracestate", "vendor=value")
	observeMicroRequest(r, runtimePythonEndpointSubject, service.handleRun)(run)
	requireNoServiceError(t, run)
	var response PythonRunResponse
	if err := json.Unmarshal(run.response, &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Artifacts) != 1 {
		t.Fatalf("artifacts: %+v", response)
	}
	failedReply := newRuntimePythonFakeRequest([]byte(`{"run_id":"reply-failure","code":"fixture"}`))
	nats.Header(failedReply.headers).Set("Traceparent", parent)
	nats.Header(failedReply.headers).Set("Tracestate", "vendor=value")
	observeMicroRequest(r, runtimePythonEndpointSubject, service.handleRun)(replyFailure{failedReply})
	bad := newRuntimePythonFakeRequest([]byte(`{`))
	observeMicroRequest(r, runtimePythonEndpointSubject, service.handleRun)(bad)
	requireServiceError(t, bad, "400")
	worker, ok := service.workerPool.AcquireWorker()
	if !ok {
		t.Fatal("worker unavailable")
	}
	busy := newRuntimePythonFakeRequest(nil)
	observeMicroRequest(r, runtimePythonEndpointSubject, service.handleRun)(busy)
	requireServiceError(t, busy, "503")
	service.workerPool.ReleaseWorker(worker.ID)
	for _, tc := range []struct {
		name      string
		header    string
		recording bool
	}{{"remote-unsampled", strings.TrimSuffix(parent, "01") + "00", false}, {"malformed", "garbage", false}} {
		req := newRuntimePythonFakeRequest(nil)
		nats.Header(req.headers).Set("traceparent", tc.header)
		observeMicroRequest(r, runtimePythonControlSettingsListSubject, func(ctx context.Context, req micro.Request) {
			if trace.SpanFromContext(ctx).IsRecording() != tc.recording {
				t.Fatal("parent sampling mismatch")
			}
			_ = req.Respond(nil)
		})(req)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/settings/", func(w http.ResponseWriter, q *http.Request) {
		w.WriteHeader(201)
		w.WriteHeader(500)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/api/failure", func(w http.ResponseWriter, q *http.Request) { w.WriteHeader(500) })
	h := observeHTTPRequest(r, mux)
	req := httptest.NewRequest("GET", "/api/settings/private-key", nil)
	req.Header.Set("traceparent", parent)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatal("first status lost")
	}
	failureRequest := httptest.NewRequest("GET", "/api/failure", nil)
	failureRequest.Header.Set("traceparent", parent)
	h.ServeHTTP(httptest.NewRecorder(), failureRequest)
	awaitObservation(t, 8*time.Second, func() bool { return len(rc.allSpans()) >= 5 && rc.metric("sandbox.runtime.runs") != nil })
	spans := rc.allSpans()
	names := map[string]*tracepb.Span{}
	for _, span := range spans {
		names[span.Name] = span
		if strings.Contains(span.Name, "private-key") {
			t.Fatal("raw path escaped")
		}
	}
	consumer := names["nats.consume "+runtimePythonEndpointSubject]
	if consumer == nil || !bytes.Equal(consumer.ParentSpanId, []byte{1, 2, 3, 4, 5, 6, 7, 8}) || consumer.TraceState != "vendor=value" {
		t.Fatalf("remote parent: %+v", consumer)
	}
	for _, name := range []string{"runtime.prepare", "runtime.execute", "runtime.upload"} {
		span := names[name]
		if span == nil || !bytes.Equal(span.ParentSpanId, consumer.SpanId) {
			t.Fatalf("child %s missing parent", name)
		}
		if span.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
			t.Fatal("reply failure changed execution span result")
		}
	}
	if consumer.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR || names["http /api/failure"].GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
		t.Fatal("transport failure did not mark span error")
	}
	count := rc.metric("sandbox.runtime.runs").GetSum().DataPoints
	if len(count) != 1 || count[0].GetAsInt() != 2 {
		t.Fatalf("rejections counted as runs: %v", count)
	}
	hist := rc.metric("sandbox.runtime.run.duration").GetHistogram()
	if hist == nil || len(hist.DataPoints[0].ExplicitBounds) != len(observability.DurationBuckets) {
		t.Fatal("duration histogram boundaries missing")
	}
	awaitObservation(t, 2*time.Second, func() bool {
		rc.mu.Lock()
		defer rc.mu.Unlock()
		for _, res := range rc.logs {
			for _, scope := range res.ScopeLogs {
				for _, record := range scope.LogRecords {
					if bytes.Equal(record.TraceId, consumer.TraceId) {
						return true
					}
				}
			}
		}
		return false
	})
	rc.mu.Lock()
	defer rc.mu.Unlock()
	var correlated bool
	for _, res := range rc.logs {
		if pbAttr(res.Resource.Attributes, "service.instance.id") != r.InstanceID {
			t.Fatal("log resource identity")
		}
		for _, scope := range res.ScopeLogs {
			for _, record := range scope.LogRecords {
				if record.Body.GetStringValue() == "filtered debug" {
					t.Fatal("level filter failed for OTEL")
				}
				if bytes.Equal(record.TraceId, consumer.TraceId) {
					correlated = true
				}
				if strings.Contains(record.Body.GetStringValue(), "private source") {
					t.Fatal("source exported")
				}
			}
		}
	}
	if !correlated {
		t.Fatal("logs not correlated")
	}
}

type replyFailure struct{ *runtimePythonFakeRequest }

func (r replyFailure) RespondJSON(any, ...micro.RespondOpt) error {
	return errors.New("sensitive send detail")
}
func TestObservabilityRequestErrorClassification(t *testing.T) {
	var output bytes.Buffer
	r, err := observability.New(context.Background(), observability.Config{Level: slog.LevelInfo}, &output)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		req     micro.Request
		handler func(context.Context, micro.Request)
		code    string
	}{
		{newRuntimePythonFakeRequest(nil), func(_ context.Context, q micro.Request) { _ = q.Respond(nil) }, "none"},
		{newRuntimePythonFakeRequest(nil), func(_ context.Context, q micro.Request) { _ = q.Error("invalid-secret-code", "private message", nil) }, "other"},
		{replyFailure{newRuntimePythonFakeRequest(nil)}, func(_ context.Context, q micro.Request) { _ = q.RespondJSON(nil) }, "reply_error"},
		{newRuntimePythonFakeRequest(nil), func(context.Context, micro.Request) {}, "no_response"},
	} {
		output.Reset()
		observeMicroRequest(r, runtimePythonEndpointSubject, tc.handler)(tc.req)
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["code"] != tc.code || record["duration_ms"] == nil || strings.Contains(output.String(), "private") {
			t.Fatalf("classification: %v", record)
		}
	}
	if redactedNATSURL("nats://user:token@localhost:4222") != "nats://localhost:4222" {
		t.Fatal("credentials not removed")
	}
}

func TestObservabilityOutageRecoveryAndBoundedShutdown(t *testing.T) {
	rc := &otlpReceipt{}
	server, endpoint := serveOTLP(t, "127.0.0.1:0", rc)
	server.Stop()
	start := time.Now()
	r := newTestTelemetry(t, endpoint, 1)
	if time.Since(start) > time.Second {
		t.Fatal("startup waited for Collector")
	}
	identity := r.InstanceID
	start = time.Now()
	for i := 0; i < 5000; i++ {
		ctx, s := r.Tracer.Start(context.Background(), "outage")
		r.Logger.InfoContext(ctx, "outage", "event", "outage")
		s.End()
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("queue saturation blocked callers")
	}
	r.Runs.Add(context.Background(), 7, metric.WithAttributes(attribute.String("result", "success")))
	server, _ = serveOTLP(t, endpoint, rc)
	awaitObservation(t, 15*time.Second, func() bool { return rc.metric("sandbox.runtime.runs") != nil })
	var span trace.Span
	awaitObservation(t, 10*time.Second, func() bool {
		_, span = r.Tracer.Start(context.Background(), "recovered")
		r.Logger.Info("recovered", "event", "recovered")
		span.End()
		for _, s := range rc.allSpans() {
			if s.Name == "recovered" {
				return true
			}
		}
		return false
	})
	if r.InstanceID != identity {
		t.Fatal("identity changed during recovery")
	}
	if got := rc.metric("sandbox.runtime.runs").GetSum().DataPoints[0].GetAsInt(); got != 7 {
		t.Fatalf("cumulative metric lost: %d", got)
	}
	server.Stop()
	rc.mode.Store(3)
	server, _ = serveOTLP(t, endpoint, rc)
	_, span = r.Tracer.Start(context.Background(), "stalled")
	span.End()
	r.Logger.Info("stalled", "event", "stalled")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start = time.Now()
	_ = r.Shutdown(ctx)
	if time.Since(start) > time.Second {
		t.Fatal("shutdown exceeded shared deadline")
	}
}

func TestObservabilityWorkersShrinkAndUnregister(t *testing.T) {
	rc := &otlpReceipt{}
	_, endpoint := serveOTLP(t, "127.0.0.1:0", rc)
	r := newTestTelemetry(t, endpoint, 1)
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 2
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := observeWorkers(r, pool)
	if err != nil {
		t.Fatal(err)
	}
	w1, _ := pool.AcquireWorker()
	w2, _ := pool.AcquireWorker()
	_, err = pool.SetWorkerCount(1)
	if err != nil {
		t.Fatal(err)
	}
	awaitObservation(t, time.Second, func() bool {
		m := rc.metric("sandbox.runtime.workers")
		if m == nil {
			return false
		}
		states := map[string]int64{}
		for _, dp := range m.GetGauge().DataPoints {
			states[pbAttr(dp.Attributes, "state")] = dp.GetAsInt()
		}
		return states["desired"] == 1 && states["total"] == 2 && states["busy"] == 2 && states["idle"] == 0
	})
	pool.ReleaseWorker(w1.ID)
	pool.ReleaseWorker(w2.ID)
	awaitObservation(t, time.Second, func() bool {
		m := rc.metric("sandbox.runtime.workers")
		for _, dp := range m.GetGauge().DataPoints {
			if pbAttr(dp.Attributes, "state") == "total" {
				return dp.GetAsInt() == 1
			}
		}
		return false
	})
	if err := cb.Unregister(); err != nil {
		t.Fatal(err)
	}
	awaitObservation(t, time.Second, func() bool {
		m := rc.metric("sandbox.runtime.workers")
		return m == nil || len(m.GetGauge().DataPoints) == 0
	})
}
