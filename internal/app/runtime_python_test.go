package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

func TestRuntimePythonLocalConfigForRunAppliesRequestOverrides(t *testing.T) {
	service := &runtimePythonService{
		cfg: RuntimePythonConfig{
			LocalPython: defaultLocalPythonConfig(),
		},
	}

	worker := RuntimeWorker{ID: "worker-1", SnapshotDir: "/tmp/worker-1/snapshot"}
	got, err := service.localPythonConfigForRun(worker, PythonRunRequest{
		MemoryMiB:    256,
		SwapMiB:      64,
		WorkspaceMiB: 32,
		ExecTimeout:  "12s",
	}, "/tmp/run", "/tmp/run/workspace", "/tmp/workspaces/run-a/workspace.ext4", "print(42)")
	if err != nil {
		t.Fatalf("localPythonConfigForRun returned error: %v", err)
	}

	if got.InlineCommand != "print(42)" {
		t.Fatalf("InlineCommand = %q, want code", got.InlineCommand)
	}
	if got.WorkspaceDir != "/tmp/run/workspace" {
		t.Fatalf("WorkspaceDir = %q, want run workspace", got.WorkspaceDir)
	}
	if got.WorkspaceImagePath != "/tmp/workspaces/run-a/workspace.ext4" {
		t.Fatalf("WorkspaceImagePath = %q, want run workspace image", got.WorkspaceImagePath)
	}
	if got.SnapshotDir != "/tmp/worker-1/snapshot" {
		t.Fatalf("SnapshotDir = %q, want worker snapshot", got.SnapshotDir)
	}
	if got.MemoryMiB != 256 || got.SwapMiB != 64 || got.WorkspaceMiB != 32 {
		t.Fatalf("resources = memory:%d swap:%d workspace:%d, want request overrides", got.MemoryMiB, got.SwapMiB, got.WorkspaceMiB)
	}
	if got.ExecTimeout != 12*time.Second {
		t.Fatalf("ExecTimeout = %s, want 12s", got.ExecTimeout)
	}
	if !got.HideFirecrackerLog {
		t.Fatal("HideFirecrackerLog = false, want runtime runs to hide firecracker logs")
	}
}

func TestRuntimePythonLocalConfigForRunUsesEffectiveControlPlaneDefaults(t *testing.T) {
	baseCfg := defaultLocalPythonConfig()
	baseCfg.MemoryMiB = 128
	baseCfg.SwapMiB = 0
	baseCfg.WorkspaceMiB = 16
	baseCfg.ExecTimeout = 5 * time.Second
	control := NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), baseCfg)
	ctx := context.Background()
	if err := control.SetSetting(ctx, "runtime.default_memory_mib", json.RawMessage(`256`)); err != nil {
		t.Fatalf("SetSetting memory returned error: %v", err)
	}
	if err := control.SetSetting(ctx, "runtime.default_swap_mib", json.RawMessage(`32`)); err != nil {
		t.Fatalf("SetSetting swap returned error: %v", err)
	}
	if err := control.SetSetting(ctx, "runtime.default_workspace_mib", json.RawMessage(`64`)); err != nil {
		t.Fatalf("SetSetting workspace returned error: %v", err)
	}
	if err := control.SetSetting(ctx, "runtime.default_exec_timeout", json.RawMessage(`"20s"`)); err != nil {
		t.Fatalf("SetSetting timeout returned error: %v", err)
	}
	service := &runtimePythonService{
		cfg:          RuntimePythonConfig{LocalPython: baseCfg},
		controlPlane: control,
	}

	worker := RuntimeWorker{ID: "worker-1", SnapshotDir: "/tmp/worker-1/snapshot"}
	got, err := service.localPythonConfigForRun(worker, PythonRunRequest{}, "/tmp/run", "/tmp/run/workspace", "", "print(42)")
	if err != nil {
		t.Fatalf("localPythonConfigForRun returned error: %v", err)
	}
	if got.MemoryMiB != 256 || got.SwapMiB != 32 || got.WorkspaceMiB != 64 || got.ExecTimeout != 20*time.Second {
		t.Fatalf("resources = memory:%d swap:%d workspace:%d timeout:%s, want control-plane defaults", got.MemoryMiB, got.SwapMiB, got.WorkspaceMiB, got.ExecTimeout)
	}

	got, err = service.localPythonConfigForRun(worker, PythonRunRequest{
		MemoryMiB:    512,
		SwapMiB:      48,
		WorkspaceMiB: 96,
		ExecTimeout:  "30s",
	}, "/tmp/run", "/tmp/run/workspace", "", "print(42)")
	if err != nil {
		t.Fatalf("localPythonConfigForRun with request overrides returned error: %v", err)
	}
	if got.MemoryMiB != 512 || got.SwapMiB != 48 || got.WorkspaceMiB != 96 || got.ExecTimeout != 30*time.Second {
		t.Fatalf("resources = memory:%d swap:%d workspace:%d timeout:%s, want request overrides", got.MemoryMiB, got.SwapMiB, got.WorkspaceMiB, got.ExecTimeout)
	}
}

func TestRuntimePythonLocalConfigForRunRejectsInvalidTimeout(t *testing.T) {
	service := &runtimePythonService{cfg: RuntimePythonConfig{LocalPython: defaultLocalPythonConfig()}}

	_, err := service.localPythonConfigForRun(RuntimeWorker{ID: "worker-1", SnapshotDir: "/tmp/worker-1/snapshot"}, PythonRunRequest{ExecTimeout: "bogus"}, "/tmp/run", "/tmp/run/workspace", "", "print(42)")
	if err == nil {
		t.Fatal("localPythonConfigForRun returned nil, want invalid timeout error")
	}
}

func TestRuntimeWorkspaceKeyOnlyPersistsWithThreadID(t *testing.T) {
	if got := runtimeWorkspaceKey(PythonRunRequest{RunID: "run-a"}, "run-a"); got != "" {
		t.Fatalf("runtimeWorkspaceKey without thread_id = %q, want empty ephemeral key", got)
	}
	if got := runtimeWorkspaceKey(PythonRunRequest{RunID: "run-a", ThreadID: "thread-a"}, "run-a"); got != "thread-a" {
		t.Fatalf("runtimeWorkspaceKey with thread_id = %q, want thread-a", got)
	}
}

func TestRuntimeWorkspaceLeaseForRunSkipsManagerWithoutThreadID(t *testing.T) {
	service := &runtimePythonService{
		cfg:        RuntimePythonConfig{LocalPython: defaultLocalPythonConfig()},
		workspaces: NewRuntimeWorkspaceManager(t.TempDir()),
	}

	lease, err := service.workspaceLeaseForRun(PythonRunRequest{RunID: "run-a"}, "run-a")
	if err != nil {
		t.Fatalf("workspaceLeaseForRun without thread_id returned error: %v", err)
	}
	if lease.ImagePath != "" || lease.Key != "" {
		t.Fatalf("workspaceLeaseForRun without thread_id = %#v, want empty lease", lease)
	}
	if got := service.workspaces.List(16).Workspaces; len(got) != 0 {
		t.Fatalf("workspace manager list after run without thread_id = %#v, want empty", got)
	}

	lease, err = service.workspaceLeaseForRun(PythonRunRequest{RunID: "run-a", ThreadID: "thread-a"}, "run-a")
	if err != nil {
		t.Fatalf("workspaceLeaseForRun with thread_id returned error: %v", err)
	}
	defer lease.Release()
	if lease.Key != "thread-a" || lease.ImagePath == "" {
		t.Fatalf("workspaceLeaseForRun with thread_id = %#v, want managed lease", lease)
	}
}

func TestCleanWorkspaceRelativePathRejectsEscapes(t *testing.T) {
	for _, path := range []string{"", "../secret", "/abs/path", "."} {
		if _, err := cleanWorkspaceRelativePath(path); err == nil {
			t.Fatalf("cleanWorkspaceRelativePath(%q) returned nil, want error", path)
		}
	}

	got, err := cleanWorkspaceRelativePath("data/input.json")
	if err != nil {
		t.Fatalf("cleanWorkspaceRelativePath returned error: %v", err)
	}
	if got != "data/input.json" {
		t.Fatalf("clean path = %q, want data/input.json", got)
	}
}

func TestRuntimePythonLogHeadersBase64EncodeAndMarkTruncation(t *testing.T) {
	service := &runtimePythonService{cfg: RuntimePythonConfig{
		StdoutHeader:   "X-Stdout",
		StderrHeader:   "X-Stderr",
		TruncateLogMiB: 1,
	}}

	headers := service.logHeaders([]byte("hello"), []byte("oops"))
	stdout, err := base64.StdEncoding.DecodeString(headers.Get("X-Stdout"))
	if err != nil {
		t.Fatalf("Decode stdout returned error: %v", err)
	}
	if string(stdout) != "hello" {
		t.Fatalf("stdout header = %q, want hello", stdout)
	}
	stderr, err := base64.StdEncoding.DecodeString(headers.Get("X-Stderr"))
	if err != nil {
		t.Fatalf("Decode stderr returned error: %v", err)
	}
	if string(stderr) != "oops" {
		t.Fatalf("stderr header = %q, want oops", stderr)
	}
	if headers.Get("X-Stdout-Truncated") != "false" || headers.Get("X-Stderr-Truncated") != "false" {
		t.Fatalf("truncation headers = %q/%q, want false/false", headers.Get("X-Stdout-Truncated"), headers.Get("X-Stderr-Truncated"))
	}
}

func TestTruncateForMetadata(t *testing.T) {
	data := make([]byte, 2*1024*1024)

	got, truncated := truncateForMetadata(data, 1)
	if !truncated {
		t.Fatal("truncated = false, want true")
	}
	if len(got) != 1024*1024 {
		t.Fatalf("truncated len = %d, want 1 MiB", len(got))
	}
}

func TestExtractRuntimePythonStdoutUsesServiceMarkers(t *testing.T) {
	output := []byte("repl echo\n__START__\nhello\nworld\n__END__\nvm runtime_ms=1\n")

	got := extractRuntimePythonStdout(output, "__START__", "__END__")
	if string(got) != "hello\nworld" {
		t.Fatalf("stdout = %q, want user output only", got)
	}
}

func TestWrapRuntimePythonCodeAddsMarkers(t *testing.T) {
	got := wrapRuntimePythonCode("print(42)", "START", "END")

	if !containsAll(got, `print("START")`, "print(42)", `print("END")`) {
		t.Fatalf("wrapped code = %q, want start marker, code, end marker", got)
	}
}
