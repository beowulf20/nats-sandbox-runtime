package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

func TestRuntimePythonServiceEndToEnd(t *testing.T) {
	if os.Getenv("NATS_RUNTIME_E2E") == "" {
		t.Skip("set NATS_RUNTIME_E2E=1 to run against a live NATS server with JetStream")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	bucket := "PY_RUNTIME_E2E_" + strings.ReplaceAll(nats.NewInbox()[5:], ".", "_")
	natsURL := os.Getenv("NATS_RUNTIME_URL")
	if natsURL == "" {
		natsURL = LocalNATSURL
	}
	cfg := RuntimePythonConfig{
		URL:            natsURL,
		Bucket:         bucket,
		MaxParallel:    1,
		LocalPython:    defaultLocalPythonConfig(),
		StdoutHeader:   "X-Python-Stdout-B64",
		StderrHeader:   "X-Python-Stderr-B64",
		TruncateLogMiB: 1,
	}
	cfg.LocalPython.HideFirecrackerLog = true
	cfg.LocalPython.ExecTimeout = 10 * time.Second
	cfg.LocalPython.WorkspaceMiB = 16
	repoRoot := filepath.Join(mustGetwd(t), "..", "..")
	cfg.LocalPython.KernelPath = filepath.Join(repoRoot, defaultLocalKernelPath)
	cfg.LocalPython.RootfsPath = filepath.Join(repoRoot, defaultLocalRootfsPath)
	cfg.LocalPython.FirecrackerPath = filepath.Join(repoRoot, defaultFirecrackerPath)

	var serviceOutput bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRuntimePython(ctx, cfg, &serviceOutput)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Logf("runtime service returned error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Log("runtime service did not stop within cleanup timeout")
		}
	})
	waitForString(t, &serviceOutput, "ready:", 10*time.Second)

	nc, err := nats.Connect(natsURL, nats.Name("python-runtime-e2e-client"))
	if err != nil {
		t.Fatalf("connect nats client returned error: %v", err)
	}
	defer nc.Close()

	request := []byte(`{"run_id":"e2e","code":"from pathlib import Path\nPath('/workspace/artifact.txt').write_text('ok')\nprint('hello from service')"}`)
	msg, err := nc.Request(runtimePythonEndpointSubject, request, 45*time.Second)
	if err != nil {
		t.Fatalf("request python.run returned error: %v", err)
	}
	if msg.Header.Get(micro.ErrorCodeHeader) != "" || msg.Header.Get(micro.ErrorHeader) != "" {
		t.Fatalf("python.run returned service error code=%q error=%q body=%q", msg.Header.Get(micro.ErrorCodeHeader), msg.Header.Get(micro.ErrorHeader), msg.Data)
	}

	stdoutBytes, err := base64.StdEncoding.DecodeString(msg.Header.Get("X-Python-Stdout-B64"))
	if err != nil {
		t.Fatalf("decode stdout metadata returned error: %v", err)
	}
	if string(stdoutBytes) != "hello from service" {
		t.Fatalf("stdout metadata = %q, want user output", stdoutBytes)
	}
	stderrBytes, err := base64.StdEncoding.DecodeString(msg.Header.Get("X-Python-Stderr-B64"))
	if err != nil {
		t.Fatalf("decode stderr metadata returned error: %v", err)
	}
	if len(stderrBytes) != 0 {
		t.Fatalf("stderr metadata = %q, want empty", stderrBytes)
	}

	var response PythonRunResponse
	if err := json.Unmarshal(msg.Data, &response); err != nil {
		t.Fatalf("unmarshal runtime response returned error: %v: %s", err, msg.Data)
	}
	if response.RunID != "e2e" || response.Status != "completed" {
		t.Fatalf("response = %#v, want completed e2e run", response)
	}
	if len(response.Artifacts) != 1 || response.Artifacts[0].Path != "artifact.txt" {
		t.Fatalf("artifacts = %#v, want artifact.txt", response.Artifacts)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("create jetstream context returned error: %v", err)
	}
	store, err := js.ObjectStore(t.Context(), bucket)
	if err != nil {
		t.Fatalf("open object store returned error: %v", err)
	}
	artifact, err := store.GetBytes(t.Context(), response.Artifacts[0].Object)
	if err != nil {
		t.Fatalf("get artifact object returned error: %v", err)
	}
	if string(artifact) != "ok" {
		t.Fatalf("artifact = %q, want ok", artifact)
	}
	_ = js.DeleteObjectStore(t.Context(), bucket)
}

func TestRuntimePythonControlSettingsEndToEnd(t *testing.T) {
	if os.Getenv("NATS_RUNTIME_E2E") == "" {
		t.Skip("set NATS_RUNTIME_E2E=1 to run against a live NATS server with JetStream")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	bucket := "PY_RUNTIME_CONTROL_E2E_" + strings.ReplaceAll(nats.NewInbox()[5:], ".", "_")
	natsURL := os.Getenv("NATS_RUNTIME_URL")
	if natsURL == "" {
		natsURL = LocalNATSURL
	}
	cfg := RuntimePythonConfig{
		URL:            natsURL,
		Bucket:         bucket,
		MaxParallel:    1,
		LocalPython:    defaultLocalPythonConfig(),
		StdoutHeader:   "X-Python-Stdout-B64",
		StderrHeader:   "X-Python-Stderr-B64",
		TruncateLogMiB: 1,
	}

	var serviceOutput bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRuntimePython(ctx, cfg, &serviceOutput)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Logf("runtime service returned error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Log("runtime service did not stop within cleanup timeout")
		}
	})
	waitForString(t, &serviceOutput, "ready:", 10*time.Second)

	nc, err := nats.Connect(natsURL, nats.Name("python-runtime-control-e2e-client"))
	if err != nil {
		t.Fatalf("connect nats client returned error: %v", err)
	}
	defer nc.Close()

	setMsg := requestRuntimeControlSetting(t, nc, runtimePythonControlSettingsSetSubject, []byte(`{"key":"runtime.default_memory_mib","value":256}`))
	requireJSONEqual(t, setMsg.Data, `{"key":"runtime.default_memory_mib","status":"ok"}`)

	getMsg := requestRuntimeControlSetting(t, nc, runtimePythonControlSettingsGetSubject, []byte(`{"key":"runtime.default_memory_mib"}`))
	requireJSONEqual(t, getMsg.Data, `{"key":"runtime.default_memory_mib","found":true,"value":256}`)

	listMsg := requestRuntimeControlSetting(t, nc, runtimePythonControlSettingsListSubject, nil)
	var listResp controlSettingsListResponse
	if err := json.Unmarshal(listMsg.Data, &listResp); err != nil {
		t.Fatalf("unmarshal settings list returned error: %v: %s", err, listMsg.Data)
	}
	if len(listResp.Settings) != 4 {
		t.Fatalf("settings list returned %d settings, want 4: %#v", len(listResp.Settings), listResp.Settings)
	}
	memory := requireSetting(t, listResp.Settings, "runtime.default_memory_mib")
	if string(memory.Value) != "256" || memory.Source != "override" {
		t.Fatalf("memory setting = %#v, want override 256", memory)
	}

	deleteMsg := requestRuntimeControlSetting(t, nc, runtimePythonControlSettingsDeleteSubject, []byte(`{"key":"runtime.default_memory_mib"}`))
	requireJSONEqual(t, deleteMsg.Data, `{"key":"runtime.default_memory_mib","status":"deleted"}`)

	resetMsg := requestRuntimeControlSetting(t, nc, runtimePythonControlSettingsGetSubject, []byte(`{"key":"runtime.default_memory_mib"}`))
	requireJSONEqual(t, resetMsg.Data, `{"key":"runtime.default_memory_mib","found":true,"value":128}`)

	workersSetMsg := requestRuntimeControlSetting(t, nc, runtimePythonControlWorkersSetSubject, []byte(`{"count":2}`))
	var workersSetResp controlWorkersSetResponse
	if err := json.Unmarshal(workersSetMsg.Data, &workersSetResp); err != nil {
		t.Fatalf("unmarshal workers set returned error: %v: %s", err, workersSetMsg.Data)
	}
	if workersSetResp.Status != "ok" || workersSetResp.DesiredCount != 2 || len(workersSetResp.Workers) != 2 {
		t.Fatalf("workers set response = %#v, want ok desired count 2", workersSetResp)
	}

	workersListMsg := requestRuntimeControlSetting(t, nc, runtimePythonControlWorkersListSubject, nil)
	var workersListResp controlWorkersListResponse
	if err := json.Unmarshal(workersListMsg.Data, &workersListResp); err != nil {
		t.Fatalf("unmarshal workers list returned error: %v: %s", err, workersListMsg.Data)
	}
	if len(workersListResp.Workers) != 2 {
		t.Fatalf("workers list returned %d workers, want 2: %#v", len(workersListResp.Workers), workersListResp.Workers)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("create jetstream context returned error: %v", err)
	}
	_ = js.DeleteObjectStore(t.Context(), bucket)
}

func TestRuntimeAPIEndToEnd(t *testing.T) {
	if os.Getenv("NATS_RUNTIME_E2E") == "" {
		t.Skip("set NATS_RUNTIME_E2E=1 to run against a live NATS server with JetStream")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	bucket := "PY_RUNTIME_API_E2E_" + strings.ReplaceAll(nats.NewInbox()[5:], ".", "_")
	natsURL := os.Getenv("NATS_RUNTIME_URL")
	if natsURL == "" {
		natsURL = LocalNATSURL
	}
	cfg := RuntimeAPIConfig{
		Listen: "127.0.0.1:0",
		WebDir: t.TempDir(),
		Runtime: RuntimePythonConfig{
			URL:            natsURL,
			Bucket:         bucket,
			MaxParallel:    1,
			LocalPython:    defaultLocalPythonConfig(),
			StdoutHeader:   "X-Python-Stdout-B64",
			StderrHeader:   "X-Python-Stderr-B64",
			TruncateLogMiB: 1,
		},
	}
	if err := os.WriteFile(filepath.Join(cfg.WebDir, "index.html"), []byte("<html>runtime api</html>"), 0o644); err != nil {
		t.Fatalf("WriteFile index returned error: %v", err)
	}

	var serviceOutput bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRuntimeAPI(ctx, cfg, &serviceOutput)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Logf("runtime api returned error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Log("runtime api did not stop within cleanup timeout")
		}
	})
	waitForString(t, &serviceOutput, "api=http://", 10*time.Second)
	baseURL := runtimeAPIURLFromOutput(t, serviceOutput.String())

	overviewResp, err := http.Get(baseURL + "/api/overview")
	if err != nil {
		t.Fatalf("GET overview returned error: %v", err)
	}
	defer overviewResp.Body.Close()
	if overviewResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(overviewResp.Body)
		t.Fatalf("GET overview status = %d body = %s, want 200", overviewResp.StatusCode, body)
	}
	var overview RuntimeAPIOverviewResponse
	if err := json.NewDecoder(overviewResp.Body).Decode(&overview); err != nil {
		t.Fatalf("decode overview returned error: %v", err)
	}
	if !overview.NATS.Connected || !overview.Runtime.Online {
		t.Fatalf("overview = %#v, want connected NATS and online runtime", overview)
	}
	if overview.Workers == nil || overview.Workers.Total != 1 {
		t.Fatalf("overview workers = %#v, want one runtime worker", overview.Workers)
	}

	settingsPut, err := http.NewRequest(http.MethodPut, baseURL+"/api/settings/runtime.default_memory_mib", strings.NewReader(`{"value":256}`))
	if err != nil {
		t.Fatalf("NewRequest settings put returned error: %v", err)
	}
	settingsPut.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(settingsPut)
	if err != nil {
		t.Fatalf("PUT setting returned error: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(putResp.Body)
		t.Fatalf("PUT setting status = %d body = %s, want 200", putResp.StatusCode, body)
	}

	getResp, err := http.Get(baseURL + "/api/settings/runtime.default_memory_mib")
	if err != nil {
		t.Fatalf("GET setting returned error: %v", err)
	}
	defer getResp.Body.Close()
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll setting returned error: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET setting status = %d body = %s, want 200", getResp.StatusCode, body)
	}
	requireJSONEqual(t, body, `{"key":"runtime.default_memory_mib","found":true,"value":256}`)

	workersPost, err := http.NewRequest(http.MethodPut, baseURL+"/api/workers", strings.NewReader(`{"count":2}`))
	if err != nil {
		t.Fatalf("NewRequest workers put returned error: %v", err)
	}
	workersPost.Header.Set("Content-Type", "application/json")
	workersPostResp, err := http.DefaultClient.Do(workersPost)
	if err != nil {
		t.Fatalf("PUT workers returned error: %v", err)
	}
	defer workersPostResp.Body.Close()
	if workersPostResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(workersPostResp.Body)
		t.Fatalf("PUT workers status = %d body = %s, want 200", workersPostResp.StatusCode, body)
	}

	workersGetResp, err := http.Get(baseURL + "/api/workers")
	if err != nil {
		t.Fatalf("GET workers returned error: %v", err)
	}
	defer workersGetResp.Body.Close()
	var workersGetBody controlWorkersListResponse
	if err := json.NewDecoder(workersGetResp.Body).Decode(&workersGetBody); err != nil {
		t.Fatalf("decode workers list returned error: %v", err)
	}
	if len(workersGetBody.Workers) != 2 {
		t.Fatalf("workers list returned %d workers, want 2: %#v", len(workersGetBody.Workers), workersGetBody.Workers)
	}

	nc, err := nats.Connect(natsURL, nats.Name("python-runtime-api-e2e-cleanup"))
	if err != nil {
		t.Fatalf("connect nats client returned error: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("create jetstream context returned error: %v", err)
	}
	_ = js.DeleteObjectStore(t.Context(), bucket)
}

func runtimeAPIURLFromOutput(t *testing.T, output string) string {
	t.Helper()
	start := strings.Index(output, "api=http://")
	if start < 0 {
		t.Fatalf("output %q does not contain api URL", output)
	}
	rest := output[start+len("api="):]
	if end := strings.IndexAny(rest, " \n\t"); end >= 0 {
		rest = rest[:end]
	}
	return rest
}

func requestRuntimeControlSetting(t *testing.T, nc *nats.Conn, subject string, payload []byte) *nats.Msg {
	t.Helper()
	msg, err := nc.Request(subject, payload, 5*time.Second)
	if err != nil {
		t.Fatalf("request %s returned error: %v", subject, err)
	}
	if msg.Header.Get(micro.ErrorCodeHeader) != "" || msg.Header.Get(micro.ErrorHeader) != "" {
		t.Fatalf("%s returned service error code=%q error=%q body=%q", subject, msg.Header.Get(micro.ErrorCodeHeader), msg.Header.Get(micro.ErrorHeader), msg.Data)
	}
	return msg
}

func waitForString(t *testing.T, buf *bytes.Buffer, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), needle) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %q in %q", needle, buf.String())
}

func mustGetwd(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd returned error: %v", err)
	}
	return wd
}
