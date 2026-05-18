package app

import (
	"context"
	"testing"
	"time"
)

func TestRootCommandDefaultsToOneLocalInstance(t *testing.T) {
	var got Config
	cmd := NewRootCommand(func(cfg Config) error {
		got = cfg
		return nil
	}, nil)
	cmd.SetArgs(nil)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.Instances != 1 {
		t.Fatalf("Instances = %d, want 1", got.Instances)
	}
	if got.URL != LocalNATSURL {
		t.Fatalf("URL = %q, want %q", got.URL, LocalNATSURL)
	}
}

func TestRootCommandAcceptsInstanceFlag(t *testing.T) {
	var got Config
	cmd := NewRootCommand(func(cfg Config) error {
		got = cfg
		return nil
	}, nil)
	cmd.SetArgs([]string{"--instances", "3"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.Instances != 3 {
		t.Fatalf("Instances = %d, want 3", got.Instances)
	}
}

func TestRootCommandRejectsZeroInstances(t *testing.T) {
	cmd := NewRootCommand(func(cfg Config) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	}, nil)
	cmd.SetArgs([]string{"--instances", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandUsesDefaultAssetsAndInteractivePython(t *testing.T) {
	var got LocalPythonConfig
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"local", "python"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.KernelPath != "firecracker-assets/vmlinux.bin" {
		t.Fatalf("KernelPath = %q, want default asset path", got.KernelPath)
	}
	if got.RootfsPath != "firecracker-assets/rootfs.ext4" {
		t.Fatalf("RootfsPath = %q, want default asset path", got.RootfsPath)
	}
	if got.FirecrackerPath != "bin/firecracker" {
		t.Fatalf("FirecrackerPath = %q, want repo-local Firecracker path", got.FirecrackerPath)
	}
	if got.SnapshotDir != "firecracker-assets/python-snapshot" {
		t.Fatalf("SnapshotDir = %q, want default snapshot cache path", got.SnapshotDir)
	}
	if got.WorkspaceDir != "tmp/workspace" {
		t.Fatalf("WorkspaceDir = %q, want default workspace path", got.WorkspaceDir)
	}
	if got.InlineCommand != "" {
		t.Fatalf("InlineCommand = %q, want empty interactive REPL command", got.InlineCommand)
	}
	if got.MemoryMiB != 128 {
		t.Fatalf("MemoryMiB = %d, want 128", got.MemoryMiB)
	}
	if got.SwapMiB != 0 {
		t.Fatalf("SwapMiB = %d, want 0", got.SwapMiB)
	}
	if got.WorkspaceMiB != 16 {
		t.Fatalf("WorkspaceMiB = %d, want 16", got.WorkspaceMiB)
	}
	if got.VCPUs != 1 {
		t.Fatalf("VCPUs = %d, want 1", got.VCPUs)
	}
	if got.MaxVCPUs != 1 {
		t.Fatalf("MaxVCPUs = %d, want 1", got.MaxVCPUs)
	}
	if got.ExecTimeout != 5*time.Second {
		t.Fatalf("ExecTimeout = %s, want 5s", got.ExecTimeout)
	}
	if got.ParallelRuns != 1 {
		t.Fatalf("ParallelRuns = %d, want 1", got.ParallelRuns)
	}
}

func TestLocalPythonCommandAcceptsInlineCommand(t *testing.T) {
	var got LocalPythonConfig
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--exec", "print(42)", "--memory-mib", "2048", "--swap-mib", "512", "--workspace-mib", "64", "--vcpus", "2", "--max-vcpus", "2", "--exec-timeout", "30s", "--parallel-runs", "3"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.InlineCommand != "print(42)" {
		t.Fatalf("InlineCommand = %q, want print command", got.InlineCommand)
	}
	if got.MemoryMiB != 2048 {
		t.Fatalf("MemoryMiB = %d, want 2048", got.MemoryMiB)
	}
	if got.SwapMiB != 512 {
		t.Fatalf("SwapMiB = %d, want 512", got.SwapMiB)
	}
	if got.WorkspaceMiB != 64 {
		t.Fatalf("WorkspaceMiB = %d, want 64", got.WorkspaceMiB)
	}
	if got.VCPUs != 2 {
		t.Fatalf("VCPUs = %d, want 2", got.VCPUs)
	}
	if got.MaxVCPUs != 2 {
		t.Fatalf("MaxVCPUs = %d, want 2", got.MaxVCPUs)
	}
	if got.ExecTimeout != 30*time.Second {
		t.Fatalf("ExecTimeout = %s, want 30s", got.ExecTimeout)
	}
	if got.ParallelRuns != 3 {
		t.Fatalf("ParallelRuns = %d, want 3", got.ParallelRuns)
	}
}

func TestRuntimePythonCommandAcceptsServiceConfig(t *testing.T) {
	var got RuntimePythonConfig
	cmd := NewRootCommand(nil, nil, func(ctx context.Context, cfg RuntimePythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"runtime", "python", "--url", "nats://demo:4222", "--bucket", "PY", "--workers", "3", "--memory-mib", "256", "--workspace-mib", "32", "--exec-timeout", "20s", "--truncate-log-mib", "2"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.URL != "nats://demo:4222" {
		t.Fatalf("URL = %q, want override", got.URL)
	}
	if got.Bucket != "PY" {
		t.Fatalf("Bucket = %q, want override", got.Bucket)
	}
	if got.MaxParallel != 3 {
		t.Fatalf("MaxParallel = %d, want 3", got.MaxParallel)
	}
	if got.LocalPython.MemoryMiB != 256 {
		t.Fatalf("MemoryMiB = %d, want 256", got.LocalPython.MemoryMiB)
	}
	if got.LocalPython.WorkspaceMiB != 32 {
		t.Fatalf("WorkspaceMiB = %d, want 32", got.LocalPython.WorkspaceMiB)
	}
	if got.LocalPython.ExecTimeout != 20*time.Second {
		t.Fatalf("ExecTimeout = %s, want 20s", got.LocalPython.ExecTimeout)
	}
	if got.TruncateLogMiB != 2 {
		t.Fatalf("TruncateLogMiB = %d, want 2", got.TruncateLogMiB)
	}
}

func TestRuntimeAPICommandAcceptsServiceAndHTTPConfig(t *testing.T) {
	var got RuntimeAPIConfig
	cmd := NewRootCommandWithRuntimeAPI(nil, nil, nil, func(ctx context.Context, cfg RuntimeAPIConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"runtime", "api", "--listen", "127.0.0.1:9090", "--web-dir", "web/build", "--url", "nats://demo:4222", "--bucket", "PY", "--workers", "3", "--memory-mib", "256", "--workspace-mib", "32", "--exec-timeout", "20s", "--truncate-log-mib", "2"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.Listen != "127.0.0.1:9090" {
		t.Fatalf("Listen = %q, want override", got.Listen)
	}
	if got.WebDir != "web/build" {
		t.Fatalf("WebDir = %q, want override", got.WebDir)
	}
	if got.Runtime.URL != "nats://demo:4222" {
		t.Fatalf("URL = %q, want override", got.Runtime.URL)
	}
	if got.Runtime.Bucket != "PY" {
		t.Fatalf("Bucket = %q, want override", got.Runtime.Bucket)
	}
	if got.Runtime.MaxParallel != 3 {
		t.Fatalf("MaxParallel = %d, want 3", got.Runtime.MaxParallel)
	}
	if got.Runtime.LocalPython.MemoryMiB != 256 {
		t.Fatalf("MemoryMiB = %d, want 256", got.Runtime.LocalPython.MemoryMiB)
	}
	if got.Runtime.LocalPython.WorkspaceMiB != 32 {
		t.Fatalf("WorkspaceMiB = %d, want 32", got.Runtime.LocalPython.WorkspaceMiB)
	}
	if got.Runtime.LocalPython.ExecTimeout != 20*time.Second {
		t.Fatalf("ExecTimeout = %s, want 20s", got.Runtime.LocalPython.ExecTimeout)
	}
	if got.Runtime.TruncateLogMiB != 2 {
		t.Fatalf("TruncateLogMiB = %d, want 2", got.Runtime.TruncateLogMiB)
	}
}

func TestRuntimeAPICommandRejectsInvalidConfig(t *testing.T) {
	for _, args := range [][]string{
		{"runtime", "api", "--listen", ""},
		{"runtime", "api", "--web-dir", ""},
		{"runtime", "api", "--workers", "0"},
	} {
		cmd := NewRootCommandWithRuntimeAPI(nil, nil, nil, func(ctx context.Context, cfg RuntimeAPIConfig) error {
			t.Fatalf("runner should not be called for invalid config")
			return nil
		})
		cmd.SetArgs(args)

		if err := cmd.Execute(); err == nil {
			t.Fatalf("Execute(%v) returned nil, want error", args)
		}
	}
}

func TestRuntimePythonCommandRejectsInvalidWorkers(t *testing.T) {
	cmd := NewRootCommand(nil, nil, func(ctx context.Context, cfg RuntimePythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"runtime", "python", "--workers", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandAcceptsExecFile(t *testing.T) {
	var got LocalPythonConfig
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--exec-file", "tmp/test-file.py"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.ExecFilePath != "tmp/test-file.py" {
		t.Fatalf("ExecFilePath = %q, want script path", got.ExecFilePath)
	}
}

func TestLocalPythonCommandRejectsExecAndExecFileTogether(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--exec", "print(42)", "--exec-file", "tmp/test-file.py"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandAcceptsRunsFlag(t *testing.T) {
	var got LocalPythonConfig
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--exec", "print(42)", "--runs", "5"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.Runs != 5 {
		t.Fatalf("Runs = %d, want 5", got.Runs)
	}
}

func TestLocalPythonCommandRejectsInvalidRuns(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--runs", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandRejectsInvalidParallelRuns(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--parallel-runs", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandRejectsInvalidExecTimeout(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--exec-timeout", "0s"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandAcceptsSnapshotDir(t *testing.T) {
	var got LocalPythonConfig
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--snapshot-dir", "/tmp/python-snapshot"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.SnapshotDir != "/tmp/python-snapshot" {
		t.Fatalf("SnapshotDir = %q, want override", got.SnapshotDir)
	}
}

func TestLocalPythonCommandAcceptsWorkspaceDir(t *testing.T) {
	var got LocalPythonConfig
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--workspace-dir", "/tmp/workspace"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if got.WorkspaceDir != "/tmp/workspace" {
		t.Fatalf("WorkspaceDir = %q, want override", got.WorkspaceDir)
	}
}

func TestLocalPythonCommandCanHideFirecrackerLog(t *testing.T) {
	var got LocalPythonConfig
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--hide-firecracker-log"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if !got.HideFirecrackerLog {
		t.Fatal("HideFirecrackerLog = false, want true")
	}
}

func TestLocalPythonCommandRejectsInvalidMemory(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--memory-mib", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandRejectsInvalidSwap(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--swap-mib", "-1"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandRejectsInvalidWorkspaceSize(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--workspace-mib", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandRejectsInvalidVCPUs(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--vcpus", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandRejectsVCPUsAboveMax(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--vcpus", "2"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}

func TestLocalPythonCommandRejectsInvalidMaxVCPUs(t *testing.T) {
	cmd := NewRootCommand(nil, func(ctx context.Context, cfg LocalPythonConfig) error {
		t.Fatalf("runner should not be called for invalid config")
		return nil
	})
	cmd.SetArgs([]string{"local", "python", "--max-vcpus", "0"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute returned nil, want error")
	}
}
