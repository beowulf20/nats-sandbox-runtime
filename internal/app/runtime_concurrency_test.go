package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"io"
	"nats-sandbox-runtime/internal/observability"
)

func concurrencyRuntime(t *testing.T, workers int, execute func(context.Context, LocalPythonConfig) (LocalPythonExecResult, error), telemetry ...*observability.Runtime) (*runtimePythonRegistration, *nats.Conn) {
	t.Helper()
	ns := startEmbeddedJetStreamNATSServer(t, -1)
	t.Cleanup(ns.Shutdown)
	cfg := defaultRuntimePythonConfig()
	cfg.URL, cfg.MaxParallel, cfg.ArtifactTTL = ns.ClientURL(), workers, 0
	cfg.LocalPython.SnapshotDir = t.TempDir()
	if len(telemetry) > 0 {
		cfg.Telemetry = telemetry[0]
	}
	r, err := startRuntimePythonService(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.runtime.execute = execute
	t.Cleanup(r.Close)
	nc := connectEmbeddedNATSClient(t, cfg.URL)
	return r, nc
}

func TestNATSDrainRepliesAndClosesAllSpans(t *testing.T) {
	for _, outcome := range []string{"success", "cancel", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer provider.Shutdown(context.Background())
			telemetry, err := observability.New(t.Context(), observability.Config{}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			telemetry.Tracer = provider.Tracer("drain-test")
			started, finish := make(chan struct{}, 1), make(chan struct{})
			r, nc := concurrencyRuntime(t, 1, func(ctx context.Context, _ LocalPythonConfig) (LocalPythonExecResult, error) {
				started <- struct{}{}
				if outcome == "cancel" {
					<-ctx.Done()
					return LocalPythonExecResult{}, ctx.Err()
				}
				<-finish
				if outcome == "panic" {
					panic("drain panic")
				}
				return LocalPythonExecResult{}, nil
			}, telemetry)
			reply := asyncPython(nc, "drain-thread", "fixture")
			awaitStarted(t, started)
			if len(recorder.Started()) == 0 || len(recorder.Started()) == len(recorder.Ended()) {
				t.Fatal("request observation ended before execution")
			}
			if outcome == "cancel" {
				r.runtime.drain(0, time.Second)
			} else {
				close(finish)
			}
			code := ""
			if outcome != "success" {
				code = "500"
			}
			awaitReply(t, reply, code)
			r.runtime.drain(time.Second, time.Second)
			if len(recorder.Started()) != len(recorder.Ended()) {
				t.Fatalf("open spans: started=%d ended=%d", len(recorder.Started()), len(recorder.Ended()))
			}
			if r.runtime.workspaces.isActive("drain-thread") {
				t.Fatal("workspace leaked after drain")
			}
			for _, w := range r.runtime.workerPool.ListWorkers() {
				if w.Status != runtimeWorkerIdle {
					t.Fatal("busy worker leaked")
				}
			}
		})
	}
}

func TestRegistrationCloseKeepsConnectionForReply(t *testing.T) {
	started, finish := make(chan struct{}, 1), make(chan struct{})
	r, nc := concurrencyRuntime(t, 1, func(ctx context.Context, _ LocalPythonConfig) (LocalPythonExecResult, error) {
		started <- struct{}{}
		select {
		case <-finish:
			return LocalPythonExecResult{}, nil
		case <-ctx.Done():
			return LocalPythonExecResult{}, ctx.Err()
		}
	})
	reply := asyncPython(nc, "close-thread", "fixture")
	awaitStarted(t, started)
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	for {
		r.runtime.mu.Lock()
		stopping := r.runtime.stopping
		r.runtime.mu.Unlock()
		if stopping {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !r.conn.IsConnected() {
		t.Fatal("connection closed during grace period")
	}
	close(finish)
	awaitReply(t, reply, "")
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("registration close did not finish")
	}
}

type concurrentReply struct {
	msg *nats.Msg
	err error
}

func asyncPython(nc *nats.Conn, thread, code string) <-chan concurrentReply {
	result := make(chan concurrentReply, 1)
	go func() {
		data, _ := json.Marshal(PythonRunRequest{ThreadID: thread, Code: code})
		msg, err := nc.Request(runtimePythonEndpointSubject, data, 5*time.Second)
		result <- concurrentReply{msg, err}
	}()
	return result
}

func awaitReply(t *testing.T, ch <-chan concurrentReply, code string) *nats.Msg {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if got := r.msg.Header.Get(micro.ErrorCodeHeader); got != code {
			t.Fatalf("code=%q want=%q body=%s", got, code, r.msg.Data)
		}
		return r.msg
	case <-time.After(6 * time.Second):
		t.Fatal("reply timeout")
		return nil
	}
}

func awaitStarted(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("execution did not start concurrently")
	}
}

func TestNATSConcurrentAdmissionResizeAndStats(t *testing.T) {
	started, release := make(chan struct{}, 3), make(chan struct{})
	r, nc := concurrencyRuntime(t, 2, func(ctx context.Context, _ LocalPythonConfig) (LocalPythonExecResult, error) {
		started <- struct{}{}
		select {
		case <-release:
			return LocalPythonExecResult{}, nil
		case <-ctx.Done():
			return LocalPythonExecResult{}, ctx.Err()
		}
	})
	defer close(release)
	a := asyncPython(nc, "a", "fixture")
	b := asyncPython(nc, "b", "fixture")
	awaitStarted(t, started)
	awaitStarted(t, started)
	awaitReply(t, asyncPython(nc, "excess", "fixture"), "503")
	if _, err := r.runtime.workerPool.SetWorkerCount(1); err != nil {
		t.Fatal(err)
	}
	awaitReply(t, asyncPython(nc, "resize-excess", "fixture"), "503")
	// Release one job; shrinking to one still forbids another admission.
	release <- struct{}{}
	select {
	case reply := <-a:
		if reply.err != nil {
			t.Fatal(reply.err)
		}
		a = nil
	case reply := <-b:
		if reply.err != nil {
			t.Fatal(reply.err)
		}
		b = nil
	}
	awaitReply(t, asyncPython(nc, "still-full", "fixture"), "503")
	release <- struct{}{}
	if a != nil {
		awaitReply(t, a, "")
	}
	if b != nil {
		awaitReply(t, b, "")
	}
	// drain joins response observation as well as execution.
	r.runtime.drain(time.Second, time.Second)
	msg, err := nc.Request("$SRV.STATS.python-runtime", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var stats struct {
		Endpoints []struct {
			Name string `json:"name"`
			Data struct {
				Execution runtimeExecutionStats `json:"execution"`
			} `json:"data"`
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(msg.Data, &stats); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ep := range stats.Endpoints {
		if ep.Name == "run" {
			found = true
			s := ep.Data.Execution
			if s.Completed != 2 || s.InFlight != 0 || s.RejectedBusy != 3 || s.ProcessingTimeNS <= 0 || s.AverageProcessingTimeNS != s.ProcessingTimeNS/2 {
				t.Fatalf("stats=%+v", s)
			}
		}
	}
	if !found {
		t.Fatalf("missing execution stats: %s", msg.Data)
	}
	r.service.Reset()
	reset, err := nc.Request("$SRV.STATS.python-runtime", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var afterReset struct {
		Endpoints []struct {
			Name string `json:"name"`
			Data struct {
				Execution runtimeExecutionStats `json:"execution"`
			} `json:"data"`
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(reset.Data, &afterReset); err != nil {
		t.Fatal(err)
	}
	for _, ep := range afterReset.Endpoints {
		if ep.Name == "run" && ep.Data.Execution.Completed != 2 {
			t.Fatalf("native reset changed execution counters: %s", reset.Data)
		}
	}
	awaitReply(t, asyncPython(nc, "closed", "fixture"), "503")
}

func TestNATSSameThreadSerialization(t *testing.T) {
	started, release := make(chan struct{}, 2), make(chan struct{})
	r, nc := concurrencyRuntime(t, 2, func(ctx context.Context, _ LocalPythonConfig) (LocalPythonExecResult, error) {
		started <- struct{}{}
		select {
		case <-release:
			return LocalPythonExecResult{}, nil
		case <-ctx.Done():
			return LocalPythonExecResult{}, ctx.Err()
		}
	})
	defer close(release)
	a := asyncPython(nc, "same", "fixture")
	awaitStarted(t, started)
	b := asyncPython(nc, "same", "fixture")
	deadline := time.After(time.Second)
	for {
		r.runtime.mu.Lock()
		accepted := r.runtime.stats.InFlight
		r.runtime.mu.Unlock()
		if accepted == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("second job not admitted")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case <-started:
		t.Fatal("same workspace executed concurrently")
	default:
	}
	release <- struct{}{}
	awaitReply(t, a, "")
	awaitStarted(t, started)
	release <- struct{}{}
	awaitReply(t, b, "")
}

func TestNATSPanicFailureAndInvalidPrefixReleaseWorker(t *testing.T) {
	_, nc := concurrencyRuntime(t, 1, func(_ context.Context, cfg LocalPythonConfig) (LocalPythonExecResult, error) {
		if strings.Contains(cfg.InlineCommand, "panic-fixture") {
			panic("private panic fixture")
		}
		return LocalPythonExecResult{}, errors.New("private failure fixture")
	})
	for _, code := range []string{"panic-fixture", "failure-fixture"} {
		msg := awaitReply(t, asyncPython(nc, "same", code), "500")
		if strings.Contains(msg.Header.Get(micro.ErrorHeader), "private") {
			t.Fatal("private cause leaked")
		}
	}
	msg, err := nc.Request(runtimePythonEndpointSubject, []byte(`{"code":"fixture","artifact_exclude_prefixes":["../escape"]}`), time.Second)
	if err != nil || msg.Header.Get(micro.ErrorCodeHeader) != "400" {
		t.Fatalf("msg=%v err=%v", msg, err)
	}
}

func TestDrainDeadlinesAndCancellableWorkspace(t *testing.T) {
	for _, finishAfter := range []time.Duration{20 * time.Second, 31 * time.Second} {
		t.Run(finishAfter.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s := &runtimePythonService{jobContext: ctx, cancelJobs: cancel}
				s.jobs.Add(1)
				cancelled := false
				go func() {
					defer s.jobs.Done()
					select {
					case <-time.After(finishAfter):
					case <-ctx.Done():
						cancelled = true
					}
				}()
				started := time.Now()
				s.drain(30*time.Second, 5*time.Second)
				synctest.Wait()
				if got, want := time.Since(started), min(finishAfter, 30*time.Second); got != want {
					t.Fatalf("drain=%s want=%s", got, want)
				}
				if cancelled != (finishAfter > 30*time.Second) {
					t.Fatalf("cancelled=%v", cancelled)
				}
			})
		})
	}
	m := NewRuntimeWorkspaceManager(t.TempDir())
	first, err := m.Begin("same")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.BeginContext(ctx, "same"); !errors.Is(err, context.Canceled) {
		t.Fatalf("lock wait=%v", err)
	}
	first.Release()
	first.Release()
	if m.isActive("same") {
		t.Fatal("workspace lease leaked")
	}
}

func TestRuntimeArtifactExclusionBeforePublishing(t *testing.T) {
	r, _ := concurrencyRuntime(t, 1, nil)
	root := t.TempDir()
	for i := range 65 {
		p := filepath.Join(root, "artifacts", fmt.Sprintf("%d.json", i))
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("input"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "output.txt"), []byte("output"), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := uploadRuntimeArtifacts(t.Context(), r.runtime.store, "selected", root, "artifacts/")
	if err != nil || len(files) != 1 || files[0].Path != "output.txt" {
		t.Fatalf("files=%v err=%v", files, err)
	}
	objects, err := r.runtime.store.List(t.Context())
	if err != nil || len(objects) != 1 {
		t.Fatalf("excluded objects published: %v err=%v", objects, err)
	}
	files, err = uploadRuntimeArtifacts(t.Context(), r.runtime.store, "legacy", root)
	if err != nil || len(files) != 66 {
		t.Fatalf("legacy files=%d err=%v", len(files), err)
	}
}
