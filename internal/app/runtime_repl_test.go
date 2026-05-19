package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

type fakeRuntimeREPLRequester struct {
	subjects []string
	payloads [][]byte
	msgs     []*nats.Msg
	err      error
}

func (f *fakeRuntimeREPLRequester) RequestWithContext(ctx context.Context, subject string, data []byte) (*nats.Msg, error) {
	f.subjects = append(f.subjects, subject)
	f.payloads = append(f.payloads, append([]byte(nil), data...))
	if f.err != nil {
		return nil, f.err
	}
	if len(f.msgs) == 0 {
		return &nats.Msg{Header: nats.Header{}}, nil
	}
	msg := f.msgs[0]
	f.msgs = f.msgs[1:]
	return msg, nil
}

func TestRuntimeREPLSendsRawLineWithoutThreadID(t *testing.T) {
	requester := &fakeRuntimeREPLRequester{}
	var stdout, stderr bytes.Buffer

	err := runRuntimeREPLWithRequester(context.Background(), defaultRuntimeREPLConfig(), requester, strings.NewReader("print(42)\n.exit\n"), &stdout, &stderr)
	if err != nil {
		t.Fatalf("runRuntimeREPLWithRequester returned error: %v", err)
	}

	if len(requester.payloads) != 1 {
		t.Fatalf("requests = %d, want 1", len(requester.payloads))
	}
	if requester.subjects[0] != runtimePythonEndpointSubject {
		t.Fatalf("subject = %q, want %q", requester.subjects[0], runtimePythonEndpointSubject)
	}
	var payload map[string]any
	if err := json.Unmarshal(requester.payloads[0], &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["code"] != "print(42)" {
		t.Fatalf("code = %#v, want raw entered line", payload["code"])
	}
	if _, ok := payload["thread_id"]; ok {
		t.Fatalf("payload includes thread_id: %s", requester.payloads[0])
	}
	if _, ok := payload["memory_mib"]; ok {
		t.Fatalf("payload includes default memory_mib: %s", requester.payloads[0])
	}
	if _, ok := payload["workspace_mib"]; ok {
		t.Fatalf("payload includes default workspace_mib: %s", requester.payloads[0])
	}
	if _, ok := payload["exec_timeout"]; ok {
		t.Fatalf("payload includes default exec_timeout: %s", requester.payloads[0])
	}
}

func TestRuntimeREPLIncludesResourceOverridesOnlyWhenSet(t *testing.T) {
	requester := &fakeRuntimeREPLRequester{}
	cfg := defaultRuntimeREPLConfig()
	cfg.MemoryMiB = 256
	cfg.WorkspaceMiB = 32
	cfg.ExecTimeout = "10s"

	err := runRuntimeREPLWithRequester(context.Background(), cfg, requester, strings.NewReader("print(42)\nquit\n"), ioDiscard{}, ioDiscard{})
	if err != nil {
		t.Fatalf("runRuntimeREPLWithRequester returned error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(requester.payloads[0], &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["memory_mib"] != float64(256) {
		t.Fatalf("memory_mib = %#v, want 256", payload["memory_mib"])
	}
	if payload["workspace_mib"] != float64(32) {
		t.Fatalf("workspace_mib = %#v, want 32", payload["workspace_mib"])
	}
	if payload["exec_timeout"] != "10s" {
		t.Fatalf("exec_timeout = %#v, want 10s", payload["exec_timeout"])
	}
}

func TestRuntimeREPLDecodesStdoutAndStderrHeaders(t *testing.T) {
	headers := nats.Header{}
	headers.Set("Nats-Sandbox-Runtime-Python-Stdout-B64", base64.StdEncoding.EncodeToString([]byte("hello\n")))
	headers.Set("Nats-Sandbox-Runtime-Python-Stderr-B64", base64.StdEncoding.EncodeToString([]byte("warning\n")))
	requester := &fakeRuntimeREPLRequester{msgs: []*nats.Msg{{Header: headers}}}
	var stdout, stderr bytes.Buffer

	err := runRuntimeREPLWithRequester(context.Background(), defaultRuntimeREPLConfig(), requester, strings.NewReader("print('hello')\n.exit\n"), &stdout, &stderr)
	if err != nil {
		t.Fatalf("runRuntimeREPLWithRequester returned error: %v", err)
	}

	if got := stdout.String(); !containsAll(got, runtimeREPLPrompt, "hello\n") {
		t.Fatalf("stdout = %q, want prompt and decoded stdout", got)
	}
	if got := stderr.String(); got != "warning\n" {
		t.Fatalf("stderr = %q, want decoded stderr", got)
	}
}

func TestRuntimeREPLExitDoesNotSendRequest(t *testing.T) {
	requester := &fakeRuntimeREPLRequester{}

	err := runRuntimeREPLWithRequester(context.Background(), defaultRuntimeREPLConfig(), requester, strings.NewReader(".exit\n"), ioDiscard{}, ioDiscard{})
	if err != nil {
		t.Fatalf("runRuntimeREPLWithRequester returned error: %v", err)
	}
	if len(requester.payloads) != 0 {
		t.Fatalf("requests = %d, want 0", len(requester.payloads))
	}
}

func TestRuntimeREPLIgnoresEmptyLines(t *testing.T) {
	requester := &fakeRuntimeREPLRequester{}

	err := runRuntimeREPLWithRequester(context.Background(), defaultRuntimeREPLConfig(), requester, strings.NewReader("\nprint(1)\nexit\n"), ioDiscard{}, ioDiscard{})
	if err != nil {
		t.Fatalf("runRuntimeREPLWithRequester returned error: %v", err)
	}
	if len(requester.payloads) != 1 {
		t.Fatalf("requests = %d, want 1", len(requester.payloads))
	}
}

func TestRuntimeREPLContinuesAfterRuntimeMicroError(t *testing.T) {
	errHeaders := nats.Header{}
	errHeaders.Set("Nats-Sandbox-Runtime-Python-Stdout-B64", base64.StdEncoding.EncodeToString([]byte("before error\n")))
	errHeaders.Set("Nats-Sandbox-Runtime-Python-Stderr-B64", base64.StdEncoding.EncodeToString([]byte("traceback\n")))
	errHeaders.Set(micro.ErrorCodeHeader, "500")
	errHeaders.Set(micro.ErrorHeader, "boom")
	requester := &fakeRuntimeREPLRequester{msgs: []*nats.Msg{
		{Header: errHeaders, Data: []byte("detail")},
		{Header: nats.Header{}},
	}}
	var stdout, stderr bytes.Buffer

	err := runRuntimeREPLWithRequester(context.Background(), defaultRuntimeREPLConfig(), requester, strings.NewReader("bad()\nprint(1)\n.exit\n"), &stdout, &stderr)
	if err != nil {
		t.Fatalf("runRuntimeREPLWithRequester returned error: %v", err)
	}

	if len(requester.payloads) != 2 {
		t.Fatalf("requests = %d, want 2", len(requester.payloads))
	}
	if got := stdout.String(); !containsAll(got, "before error\n") {
		t.Fatalf("stdout = %q, want decoded stdout from failed run", got)
	}
	if got := stderr.String(); !containsAll(got, "traceback\n", "runtime_error: code=500 description=boom data=detail") {
		t.Fatalf("stderr = %q, want decoded stderr and runtime error", got)
	}
}

func TestRuntimeREPLValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  RuntimeREPLConfig
	}{
		{name: "empty url", cfg: RuntimeREPLConfig{Timeout: time.Second}},
		{name: "zero timeout", cfg: RuntimeREPLConfig{URL: LocalNATSURL}},
		{name: "negative memory", cfg: RuntimeREPLConfig{URL: LocalNATSURL, Timeout: time.Second, MemoryMiB: -1}},
		{name: "negative workspace", cfg: RuntimeREPLConfig{URL: LocalNATSURL, Timeout: time.Second, WorkspaceMiB: -1}},
		{name: "invalid exec timeout", cfg: RuntimeREPLConfig{URL: LocalNATSURL, Timeout: time.Second, ExecTimeout: "bogus"}},
		{name: "zero exec timeout", cfg: RuntimeREPLConfig{URL: LocalNATSURL, Timeout: time.Second, ExecTimeout: "0s"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateRuntimeREPLConfig(tt.cfg); err == nil {
				t.Fatal("validateRuntimeREPLConfig returned nil, want error")
			}
		})
	}
}

func TestRuntimeREPLReturnsRequestErrors(t *testing.T) {
	requester := &fakeRuntimeREPLRequester{err: fmt.Errorf("no responders")}

	err := runRuntimeREPLWithRequester(context.Background(), defaultRuntimeREPLConfig(), requester, strings.NewReader("print(1)\n"), ioDiscard{}, ioDiscard{})
	if err == nil || !strings.Contains(err.Error(), "request python.run") {
		t.Fatalf("error = %v, want request error", err)
	}
}

func TestRuntimeREPLStopsWhenContextCanceledWhileWaitingForInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &blockingREPLReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	errCh := make(chan error, 1)

	go func() {
		errCh <- runRuntimeREPLWithRequester(ctx, defaultRuntimeREPLConfig(), &fakeRuntimeREPLRequester{}, reader, ioDiscard{}, ioDiscard{})
	}()

	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("REPL did not start reading input")
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("REPL did not stop after context cancellation")
	}
	close(reader.release)
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}

type blockingREPLReader struct {
	started chan struct{}
	release chan struct{}
}

func (r *blockingREPLReader) Read(p []byte) (int, error) {
	close(r.started)
	<-r.release
	return 0, io.EOF
}
