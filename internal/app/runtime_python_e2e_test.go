package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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
