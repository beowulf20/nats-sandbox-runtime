package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"nats-sandbox-runtime/internal/observability"
)

// Separate OS processes exercise resource identity and cumulative recovery.
func TestObservabilitySmokeEmitter(t *testing.T) {
	if os.Getenv("OBSERVABILITY_EMITTER") != "1" {
		t.Skip("smoke child process only")
	}
	r, err := observability.New(context.Background(), observability.Config{Endpoint: "127.0.0.1:4317", SampleRatio: 1, MetricInterval: 200 * time.Millisecond}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	}()
	ns := startEmbeddedJetStreamNATSServer(t, -1)
	t.Cleanup(ns.Shutdown)
	cfg := defaultRuntimePythonConfig()
	cfg.URL = ns.ClientURL()
	cfg.Telemetry = r
	cfg.LocalPython.SnapshotDir = t.TempDir()
	cfg.ArtifactTTL = 0
	registration, err := startRuntimePythonService(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close()
	registration.runtime.execute = func(ctx context.Context, cfg LocalPythonConfig) (LocalPythonExecResult, error) {
		err := os.WriteFile(filepath.Join(cfg.WorkspaceDir, "artifact.txt"), []byte("smoke"), 0600)
		return LocalPythonExecResult{}, err
	}
	apiCfg := RuntimeAPIConfig{Runtime: cfg}
	handler := newRuntimeAPIHTTPHandler(registration.runtime.controlPlane, registration.runtime.workerPool, registration.runtime.workspaces, runtimeAPIServiceOverviewProvider{cfg: apiCfg, conn: registration.conn, microService: registration.service}, t.TempDir(), r)
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	client, err := nats.Connect(cfg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	fmt.Fprintln(os.Stdout, r.InstanceID)
	scanner := bufio.NewScanner(os.Stdin)
	phase := 0
	for scanner.Scan() {
		if scanner.Text() != "emit" {
			return
		}
		phase++
		r.Logger.Info(fmt.Sprintf("observability smoke phase %d", phase), "event", "smoke.phase", "phase", phase)
		for _, body := range []string{`{"code":"fixture"}`, `{`} {
			if _, err := client.Request(runtimePythonEndpointSubject, []byte(body), time.Second); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := client.Request(runtimePythonControlSettingsListSubject, nil, time.Second); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/api/overview", "/api/workers", "/api/settings/private-key", "/static.css"} {
			resp, err := http.Get(httpServer.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, "GET", httpServer.URL+"/api/workers/events", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		_, err = bufio.NewReader(resp.Body).ReadString('\n')
		_ = resp.Body.Close()
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  []json.RawMessage `json:"value"`
}

func prometheusSamples(query string) []promSample {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:9090/api/v1/query?query=" + url.QueryEscape(query))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []promSample `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || result.Status != "success" {
		return nil
	}
	return result.Data.Result
}
func promValue(s promSample) float64 {
	if len(s.Value) != 2 {
		return 0
	}
	var raw string
	_ = json.Unmarshal(s.Value[1], &raw)
	v, _ := strconv.ParseFloat(raw, 64)
	return v
}

func TestObservabilityCollectorPrometheusSmoke(t *testing.T) {
	if os.Getenv("OBSERVABILITY_SMOKE") != "1" {
		t.Skip("run make observability-smoke")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	compose := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("docker", append([]string{"compose", "-f", filepath.Join(root, "observability/compose.yaml")}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("compose: %v: %s", err, out)
		}
		return string(out)
	}
	type emitter struct {
		id    string
		input io.WriteCloser
		cmd   *exec.Cmd
	}
	var emitters []emitter
	for i := 0; i < 2; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestObservabilitySmokeEmitter$", "-test.timeout=150s")
		cmd.Env = append(os.Environ(), "OBSERVABILITY_EMITTER=1")
		input, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		output, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = input.Close()
			if err := cmd.Wait(); err != nil {
				t.Errorf("emitter: %v", err)
			}
		})
		id, err := bufio.NewReader(output).ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		id = strings.TrimSpace(id)
		if id == "" {
			t.Fatal("missing emitter identity")
		}
		emitters = append(emitters, emitter{id, input, cmd})
	}
	if emitters[0].id == emitters[1].id {
		t.Fatal("separate processes merged")
	}
	verify := func(min float64) {
		for _, query := range []string{"sandbox_nats_requests_total", "sandbox_nats_requests_total{result=\"error\",code=\"400\"}", "sandbox_http_requests_total", "sandbox_runtime_runs_total", "sandbox_runtime_run_duration_seconds_count", "sandbox_nats_request_duration_seconds_count", "sandbox_http_request_duration_seconds_count", "sandbox_http_sse_connections_total{event=\"open\"}", "sandbox_http_sse_connections_total{event=\"close\"}"} {
			awaitObservation(t, 45*time.Second, func() bool {
				seen := map[string]float64{}
				for _, s := range prometheusSamples(query) {
					seen[s.Metric["instance"]] += promValue(s)
					if s.Metric["route"] == "/static.css" || s.Metric["route"] == "/api/workers/events" || strings.Contains(s.Metric["route"], "private-key") {
						t.Fatal("unbounded/static metric route")
					}
				}
				return seen[emitters[0].id] >= min && seen[emitters[1].id] >= min
			})
		}
		for _, e := range emitters {
			for _, state := range []string{"desired", "total", "idle", "busy"} {
				query := fmt.Sprintf("sandbox_runtime_workers_ratio{instance=%q,state=%q}", e.id, state)
				if len(prometheusSamples(query)) != 1 {
					t.Fatalf("worker metric missing: %s", query)
				}
			}
		}
		logs := compose("logs", "--no-color", "otel-collector")
		awaitObservation(t, 10*time.Second, func() bool {
			logs = compose("logs", "--no-color", "otel-collector")
			return strings.Contains(logs, fmt.Sprintf("observability smoke phase %.0f", min))
		})
		for _, value := range []string{emitters[0].id, emitters[1].id, "runtime.execute", "request.completed", "http.sse.open", "http.sse.close"} {
			if !strings.Contains(logs, value) {
				t.Fatalf("Collector receipt missing %q", value)
			}
		}
	}
	for _, e := range emitters {
		_, _ = fmt.Fprintln(e.input, "emit")
	}
	verify(1)
	compose("restart", "otel-collector")
	for _, e := range emitters {
		_, _ = fmt.Fprintln(e.input, "emit")
	}
	verify(2)
	t.Logf("plaintext signals, Prometheus counts/histograms/gauges, two process identities, and Collector recovery passed: %s %s", emitters[0].id, emitters[1].id)
}
