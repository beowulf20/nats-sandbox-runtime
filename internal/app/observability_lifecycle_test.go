package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"nats-sandbox-runtime/internal/observability"
)

func TestObservabilityExportTransientAndPermanentFailure(t *testing.T) {
	for _, mode := range []int32{1, 2} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			rc := &otlpReceipt{}
			rc.mode.Store(mode)
			_, endpoint := serveOTLP(t, "127.0.0.1:0", rc)
			r := newTestTelemetry(t, endpoint, 1)
			for i := 0; i < 512; i++ {
				_, span := r.Tracer.Start(context.Background(), "failed-batch")
				span.End()
			}
			awaitObservation(t, time.Second, func() bool { return rc.traceAttempts.Load() > 0 })
			time.Sleep(observability.ExportTimeout + 200*time.Millisecond)
			if mode == 1 && rc.traceAttempts.Load() < 2 {
				t.Fatal("transient export did not retry")
			}
			if mode == 2 && rc.traceAttempts.Load() != 1 {
				t.Fatalf("permanent export retried: %d", rc.traceAttempts.Load())
			}
			rc.mode.Store(0)
			for i := 0; i < 512; i++ {
				_, span := r.Tracer.Start(context.Background(), "future-batch")
				span.End()
			}
			awaitObservation(t, 3*time.Second, func() bool { return len(rc.allSpans()) > 0 })
			for _, span := range rc.allSpans() {
				if span.Name == "failed-batch" {
					t.Fatal("exhausted records replayed")
				}
			}
		})
	}
}

type cleanupSequence struct{ now time.Time }

func (s cleanupSequence) List(context.Context, ...jetstream.ListObjectsOpt) ([]*jetstream.ObjectInfo, error) {
	var result []*jetstream.ObjectInfo
	for _, name := range []string{"ok", "gone", "failed"} {
		result = append(result, &jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: "sdk-inputs/" + name}, ModTime: s.now.Add(-time.Hour)})
	}
	return result, nil
}
func (s cleanupSequence) Delete(_ context.Context, name string) error {
	switch name {
	case "sdk-inputs/gone":
		return jetstream.ErrObjectNotFound
	case "sdk-inputs/failed":
		return errors.New("private deletion detail")
	}
	return nil
}
func TestObservabilityCleanupCountsActualPartialDeletion(t *testing.T) {
	rc := &otlpReceipt{}
	_, endpoint := serveOTLP(t, "127.0.0.1:0", rc)
	r := newTestTelemetry(t, endpoint, 1)
	now := time.Now()
	deleted, err := cleanupRuntimeObjects(context.Background(), cleanupSequence{now}, now, r)
	if err == nil || deleted != 2 {
		t.Fatalf("existing count semantics changed: %d %v", deleted, err)
	}
	awaitObservation(t, time.Second, func() bool {
		m := rc.metric("sandbox.runtime.cleanup.deleted")
		return m != nil && m.GetSum().DataPoints[0].GetAsInt() == 1
	})
}

type failedUploadStore struct{ jetstream.ObjectStore }

func (s failedUploadStore) Put(context.Context, jetstream.ObjectMeta, io.Reader) (*jetstream.ObjectInfo, error) {
	return nil, errors.New("private upload detail")
}
func TestObservabilityRuntimeFailureSpans(t *testing.T) {
	rc := &otlpReceipt{}
	_, endpoint := serveOTLP(t, "127.0.0.1:0", rc)
	r := newTestTelemetry(t, endpoint, 1)
	ns := startEmbeddedJetStreamNATSServer(t, -1)
	defer ns.Shutdown()
	cfg := defaultRuntimePythonConfig()
	cfg.URL = ns.ClientURL()
	cfg.Telemetry = r
	cfg.ArtifactTTL = 0
	registration, err := startRuntimePythonService(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close()
	service := registration.runtime
	worker, ok := service.workerPool.AcquireWorker()
	if !ok {
		t.Fatal("worker missing")
	}
	defer service.workerPool.ReleaseWorker(worker.ID)
	if _, _, _, err := service.run(context.Background(), worker, PythonRunRequest{RunID: "prepare"}); err == nil {
		t.Fatal("missing code accepted")
	}
	service.execute = func(context.Context, LocalPythonConfig) (LocalPythonExecResult, error) {
		return LocalPythonExecResult{}, errors.New("private execution detail")
	}
	if _, _, _, err := service.run(context.Background(), worker, PythonRunRequest{RunID: "execute", Code: "fixture"}); err == nil {
		t.Fatal("execution failure accepted")
	}
	service.execute = func(_ context.Context, cfg LocalPythonConfig) (LocalPythonExecResult, error) {
		return LocalPythonExecResult{}, os.WriteFile(filepath.Join(cfg.WorkspaceDir, "artifact"), []byte("fixture"), 0600)
	}
	service.store = failedUploadStore{service.store}
	if _, _, _, err := service.run(context.Background(), worker, PythonRunRequest{RunID: "upload", Code: "fixture"}); err == nil {
		t.Fatal("upload failure accepted")
	}
	awaitObservation(t, 7*time.Second, func() bool {
		seen := map[string]bool{}
		for _, span := range rc.allSpans() {
			if span.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
				seen[span.Name] = true
			}
		}
		return seen["runtime.prepare"] && seen["runtime.execute"] && seen["runtime.upload"]
	})
}

func TestObservabilityNATSReconnectKeepsIdentity(t *testing.T) {
	rc := &otlpReceipt{}
	_, endpoint := serveOTLP(t, "127.0.0.1:0", rc)
	r := newTestTelemetry(t, endpoint, 1)
	ns := startEmbeddedJetStreamNATSServer(t, -1)
	t.Cleanup(ns.Shutdown)
	cfg := defaultRuntimePythonConfig()
	cfg.URL = ns.ClientURL()
	cfg.Telemetry = r
	cfg.ArtifactTTL = 0
	registration, err := startRuntimePythonService(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close()
	port := ns.Addr().(*net.TCPAddr).Port
	identity := r.InstanceID
	ns.Shutdown()
	ns.WaitForShutdown()
	ns = startEmbeddedJetStreamNATSServer(t, port)
	defer ns.Shutdown()
	awaitObservation(t, 5*time.Second, func() bool {
		m := rc.metric("sandbox.runtime.reconnects")
		return registration.conn.IsConnected() && m != nil && m.GetSum().DataPoints[0].GetAsInt() >= 1
	})
	if r.InstanceID != identity {
		t.Fatal("NATS reconnect recreated identity")
	}
	client := connectEmbeddedNATSClient(t, cfg.URL)
	defer client.Close()
	if _, err := client.Request(runtimePythonControlSettingsListSubject, nil, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestObservabilityRuntimeAPIClosesSSEAfterShutdownDeadline(t *testing.T) {
	ns := startEmbeddedJetStreamNATSServer(t, -1)
	defer ns.Shutdown()
	cfg := RuntimeAPIConfig{Listen: "127.0.0.1:0", WebDir: t.TempDir(), Runtime: defaultRuntimePythonConfig()}
	cfg.Runtime.URL = ns.ClientURL()
	cfg.Runtime.ArtifactTTL = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	result := make(chan error, 1)
	go func() { result <- RunRuntimeAPI(ctx, cfg, writer) }()
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var address string
	for _, part := range strings.Fields(line) {
		if strings.HasPrefix(part, "api=") {
			address = strings.TrimPrefix(part, "api=")
		}
	}
	resp, err := http.Get(address + "/api/workers/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("runner shutdown result changed: %v", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("HTTP shutdown did not close SSE")
	}
	if time.Since(start) > 6*time.Second {
		t.Fatal("HTTP shutdown exceeded its bound")
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("SSE stream still open")
	}
}

func TestObservabilitySignalChild(t *testing.T) {
	if os.Getenv("OBSERVABILITY_SIGNAL_CHILD") != "1" {
		t.Skip("signal child only")
	}
	err := runObserved(context.Background(), func(ctx context.Context, r *observability.Runtime) error {
		_, span := r.Tracer.Start(ctx, "signal shutdown")
		r.Logger.Info("signal child ready", "event", "runtime.ready")
		fmt.Fprintln(os.Stdout, "ready")
		<-ctx.Done()
		span.End()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestObservabilitySIGTERMAndInvalidConfig(t *testing.T) {
	t.Setenv("LOG_LEVEL", "invalid")
	called := false
	if err := runObserved(context.Background(), func(context.Context, *observability.Runtime) error { called = true; return nil }); err == nil || called {
		t.Fatal("invalid config reached runner")
	}
	t.Setenv("LOG_LEVEL", "info")
	cmd := exec.Command(os.Args[0], "-test.run=^TestObservabilitySignalChild$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "OBSERVABILITY_SIGNAL_CHILD=1", "GORACE=atexit_sleep_ms=0", "OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:1")
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	if line, err := bufio.NewReader(output).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("signal child readiness: %q %v", line, err)
	}
	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 6*time.Second {
		t.Fatal("SIGTERM export shutdown exceeded shared budget")
	}
}
