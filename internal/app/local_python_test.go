package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLocalPythonBootArgsStartInteractivePythonAsInit(t *testing.T) {
	got := localPythonBootArgs("", 123, false)

	want := "console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda ro init=/usr/local/bin/fc-python-init fc_start_ns=123"
	if got != want {
		t.Fatalf("boot args = %q, want %q", got, want)
	}
}

func TestLocalPythonBootArgsRunInlineCommand(t *testing.T) {
	got := localPythonBootArgs(`print("hello")`, 456, false)

	want := `console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda ro init=/usr/local/bin/fc-python-init fc_start_ns=456 fc_py_b64=cHJpbnQoImhlbGxvIik=`
	if got != want {
		t.Fatalf("boot args = %q, want %q", got, want)
	}
}

func TestLocalPythonBootArgsCanHideGuestBootLog(t *testing.T) {
	got := localPythonBootArgs(`print("hello")`, 456, true)

	if !containsAll(got, "quiet", "loglevel=0") {
		t.Fatalf("boot args = %q, want quiet kernel logging", got)
	}
}

func TestLocalPythonVMConfigUsesKernelRootfsAndMemory(t *testing.T) {
	cfg := LocalPythonConfig{
		KernelPath:      "kernel.bin",
		RootfsPath:      "rootfs.ext4",
		FirecrackerPath: "bin/firecracker",
		WorkspaceDir:    "workspace",
		InlineCommand:   "print(42)",
		MemoryMiB:       256,
		SwapMiB:         512,
		WorkspaceMiB:    16,
		VCPUs:           2,
	}
	paths := localPythonSnapshotPaths("snapshot")

	got := localPythonVMConfig(cfg, paths, 789)

	if got.BootSource.KernelImagePath != cfg.KernelPath {
		t.Fatalf("kernel path = %q, want %q", got.BootSource.KernelImagePath, cfg.KernelPath)
	}
	if got.Drives[0].PathOnHost != cfg.RootfsPath {
		t.Fatalf("rootfs path = %q, want %q", got.Drives[0].PathOnHost, cfg.RootfsPath)
	}
	if !got.Drives[0].IsReadOnly || got.Drives[1].IsReadOnly || got.Drives[2].IsReadOnly {
		t.Fatalf("drive access = %#v, want read-only rootfs and writable workspace/swap", got.Drives)
	}
	if got.Drives[1].DriveID != "workspace" {
		t.Fatalf("workspace drive id = %q, want workspace", got.Drives[1].DriveID)
	}
	if got.Drives[1].PathOnHost != localPythonWorkspaceDrivePath {
		t.Fatalf("workspace path = %q, want %q", got.Drives[1].PathOnHost, localPythonWorkspaceDrivePath)
	}
	if got.Drives[2].DriveID != "swap" {
		t.Fatalf("swap drive id = %q, want swap", got.Drives[2].DriveID)
	}
	if got.Drives[2].PathOnHost != localPythonSwapDrivePath {
		t.Fatalf("swap path = %q, want %q", got.Drives[2].PathOnHost, localPythonSwapDrivePath)
	}
	if got.MachineConfig.MemSizeMiB != cfg.MemoryMiB {
		t.Fatalf("memory = %d, want %d", got.MachineConfig.MemSizeMiB, cfg.MemoryMiB)
	}
	if got.MachineConfig.VcpuCount != cfg.VCPUs {
		t.Fatalf("vcpus = %d, want %d", got.MachineConfig.VcpuCount, cfg.VCPUs)
	}
	if got.BootSource.BootArgs == "" {
		t.Fatal("boot args are empty")
	}
}

func TestSparseBootSnapshotTrackingAndCacheIsolation(t *testing.T) {
	full := defaultLocalPythonConfig()
	sparse := full
	sparse.SparseBootSnapshot = true
	paths := localPythonSnapshotPaths(t.TempDir())
	if localPythonVMConfig(full, paths, 0).MachineConfig.TrackDirtyPages {
		t.Fatal("full snapshots unexpectedly enable dirty-page tracking")
	}
	if !localPythonVMConfig(sparse, paths, 0).MachineConfig.TrackDirtyPages {
		t.Fatal("sparse boot snapshot does not track pages dirtied during boot")
	}
	for _, path := range []string{paths.StatePath, paths.MemoryPath} {
		if err := os.WriteFile(path, []byte("snapshot"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	full.SwapMiB, sparse.SwapMiB = 0, 0
	for _, cfg := range []LocalPythonConfig{full, sparse} {
		if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(cfg)), 0o600); err != nil {
			t.Fatal(err)
		}
		if check := localPythonSnapshotCheck(paths, cfg); !check.OK {
			t.Fatal(check.Reason)
		}
		other := cfg
		other.SparseBootSnapshot = !cfg.SparseBootSnapshot
		if localPythonSnapshotCheck(paths, other).OK {
			t.Fatal("full and sparse modes reused the same cache")
		}
	}
}


func TestRunLocalPythonRejectsInvalidMemoryBeforeAssetChecks(t *testing.T) {
	err := RunLocalPython(t.Context(), LocalPythonConfig{
		KernelPath: "missing-kernel",
		RootfsPath: "missing-rootfs",
		MemoryMiB:  0,
		VCPUs:      1,
		MaxVCPUs:   1,
	}, nil, nil, nil)

	if err == nil || err.Error() != "memory-mib must be at least 1" {
		t.Fatalf("RunLocalPython error = %v, want invalid memory error", err)
	}
}

func TestRunLocalPythonRejectsInvalidSwapBeforeAssetChecks(t *testing.T) {
	err := RunLocalPython(t.Context(), LocalPythonConfig{
		KernelPath: "missing-kernel",
		RootfsPath: "missing-rootfs",
		MemoryMiB:  128,
		SwapMiB:    -1,
		VCPUs:      1,
		MaxVCPUs:   1,
	}, nil, nil, nil)

	if err == nil || err.Error() != "swap-mib must be at least 0" {
		t.Fatalf("RunLocalPython error = %v, want invalid swap error", err)
	}
}

func TestRunLocalPythonRejectsInvalidWorkspaceSizeBeforeAssetChecks(t *testing.T) {
	err := RunLocalPython(t.Context(), LocalPythonConfig{
		KernelPath:   "missing-kernel",
		RootfsPath:   "missing-rootfs",
		MemoryMiB:    128,
		WorkspaceMiB: 0,
		VCPUs:        1,
		MaxVCPUs:     1,
		ParallelRuns: 1,
		ExecTimeout:  time.Second,
	}, nil, nil, nil)

	if err == nil || err.Error() != "workspace-mib must be at least 1" {
		t.Fatalf("RunLocalPython error = %v, want invalid workspace size error", err)
	}
}

func TestRunLocalPythonRejectsVCPUsAboveMaxBeforeAssetChecks(t *testing.T) {
	err := RunLocalPython(t.Context(), LocalPythonConfig{
		KernelPath:   "missing-kernel",
		RootfsPath:   "missing-rootfs",
		MemoryMiB:    128,
		WorkspaceMiB: 16,
		VCPUs:        2,
		MaxVCPUs:     1,
	}, nil, nil, nil)

	if err == nil || err.Error() != "vcpus must be less than or equal to max-vcpus (1)" {
		t.Fatalf("RunLocalPython error = %v, want max vcpus error", err)
	}
}

func TestEnsureLocalPythonWorkspaceImageUsesConfiguredSize(t *testing.T) {
	requireCommand(t, "mke2fs")

	dir := t.TempDir()
	workspaceDir := filepath.Join(dir, "workspace")
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		t.Fatalf("MkdirAll workspace returned error: %v", err)
	}
	paths := localPythonSnapshotPaths(filepath.Join(dir, "snapshot"))

	err := ensureLocalPythonWorkspaceImage(t.Context(), LocalPythonConfig{
		WorkspaceDir: workspaceDir,
		WorkspaceMiB: 16,
	}, paths)
	if err != nil {
		t.Fatalf("ensureLocalPythonWorkspaceImage returned error: %v", err)
	}
	info, err := os.Stat(paths.WorkspacePath)
	if err != nil {
		t.Fatalf("Stat workspace image returned error: %v", err)
	}
	if info.Size() != 16*1024*1024 {
		t.Fatalf("workspace image size = %d, want 16 MiB", info.Size())
	}
}

func TestEnsureLocalPythonWorkspaceImageRejectsContentAboveConfiguredSize(t *testing.T) {
	dir := t.TempDir()
	workspaceDir := filepath.Join(dir, "workspace")
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		t.Fatalf("MkdirAll workspace returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceDir, "too-large.bin"), bytes.Repeat([]byte("x"), 2*1024*1024), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	paths := localPythonSnapshotPaths(filepath.Join(dir, "snapshot"))

	err := ensureLocalPythonWorkspaceImage(t.Context(), LocalPythonConfig{
		WorkspaceDir: workspaceDir,
		WorkspaceMiB: 1,
	}, paths)
	if err == nil || !strings.Contains(err.Error(), "exceeding workspace-mib limit of 1 MiB") {
		t.Fatalf("ensureLocalPythonWorkspaceImage error = %v, want workspace size limit error", err)
	}
}

func TestLocalPythonSnapshotPaths(t *testing.T) {
	got := localPythonSnapshotPaths("cache/python")

	if got.StatePath != filepath.Join("cache/python", "snapshot.bin") {
		t.Fatalf("StatePath = %q, want snapshot.bin in cache dir", got.StatePath)
	}
	if got.MemoryPath != filepath.Join("cache/python", "memory.bin") {
		t.Fatalf("MemoryPath = %q, want memory.bin in cache dir", got.MemoryPath)
	}
	if got.WorkspacePath != filepath.Join("cache/python", "workspace.ext4") {
		t.Fatalf("WorkspacePath = %q, want workspace.ext4 in cache dir", got.WorkspacePath)
	}
	if got.SwapPath != filepath.Join("cache/python", "swap.raw") {
		t.Fatalf("SwapPath = %q, want swap.raw in cache dir", got.SwapPath)
	}
	if got.VersionPath != filepath.Join("cache/python", "version") {
		t.Fatalf("VersionPath = %q, want version in cache dir", got.VersionPath)
	}
}

func TestSyncLocalPythonWorkspaceImageReplacesExistingNestedDirectories(t *testing.T) {
	requireCommand(t, "mke2fs")
	requireCommand(t, "debugfs")

	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "source")
	if err := os.MkdirAll(filepath.Join(sourceDir, "charts"), 0o755); err != nil {
		t.Fatalf("MkdirAll source returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "charts", "status_counts.png"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("WriteFile source returned error: %v", err)
	}
	imagePath := filepath.Join(dir, "workspace.ext4")
	cmd := exec.Command("mke2fs", "-q", "-t", "ext4", "-F", "-d", sourceDir, imagePath, "8192")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("mke2fs returned error: %v: %s", err, strings.TrimSpace(string(output)))
	}

	workspaceDir := filepath.Join(dir, "workspace")
	if err := os.MkdirAll(filepath.Join(workspaceDir, "charts"), 0o755); err != nil {
		t.Fatalf("MkdirAll workspace returned error: %v", err)
	}

	err := syncLocalPythonWorkspaceImage(t.Context(), LocalPythonConfig{WorkspaceDir: workspaceDir}, localPythonSnapshotFiles{WorkspacePath: imagePath})
	if err != nil {
		t.Fatalf("syncLocalPythonWorkspaceImage returned error: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(workspaceDir, "charts", "status_counts.png"))
	if err != nil {
		t.Fatalf("ReadFile synced chart returned error: %v", err)
	}
	if string(content) != "ok" {
		t.Fatalf("synced chart = %q, want ok", content)
	}
}

func TestLocalPythonSnapshotCompleteRequiresBothFiles(t *testing.T) {
	dir := t.TempDir()
	paths := localPythonSnapshotPaths(dir)
	cfg := LocalPythonConfig{MemoryMiB: 128, VCPUs: 1, WorkspaceMiB: 16}

	if localPythonSnapshotComplete(paths, cfg) {
		t.Fatal("snapshot complete with no files, want false")
	}
	if err := os.WriteFile(paths.StatePath, []byte("state"), 0o644); err != nil {
		t.Fatalf("WriteFile state returned error: %v", err)
	}
	if localPythonSnapshotComplete(paths, cfg) {
		t.Fatal("snapshot complete with only state file, want false")
	}
	if err := os.WriteFile(paths.MemoryPath, []byte("memory"), 0o644); err != nil {
		t.Fatalf("WriteFile memory returned error: %v", err)
	}
	if localPythonSnapshotComplete(paths, cfg) {
		t.Fatal("snapshot complete without version file, want false")
	}
	if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(cfg)+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile version returned error: %v", err)
	}
	if !localPythonSnapshotComplete(paths, cfg) {
		t.Fatal("snapshot complete with state and memory files, want true")
	}
}

func TestLocalPythonSnapshotCompleteRejectsDifferentResourceVersion(t *testing.T) {
	dir := t.TempDir()
	paths := localPythonSnapshotPaths(dir)
	if err := os.WriteFile(paths.StatePath, []byte("state"), 0o644); err != nil {
		t.Fatalf("WriteFile state returned error: %v", err)
	}
	if err := os.WriteFile(paths.MemoryPath, []byte("memory"), 0o644); err != nil {
		t.Fatalf("WriteFile memory returned error: %v", err)
	}
	if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(LocalPythonConfig{MemoryMiB: 1024, VCPUs: 1, SwapMiB: 0, WorkspaceMiB: 16})+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile version returned error: %v", err)
	}

	if localPythonSnapshotComplete(paths, LocalPythonConfig{MemoryMiB: 128, VCPUs: 1, WorkspaceMiB: 16}) {
		t.Fatal("snapshot complete with different memory version, want false")
	}
}

func TestLocalPythonSnapshotCheckReportsVersionMismatch(t *testing.T) {
	dir := t.TempDir()
	paths := localPythonSnapshotPaths(dir)
	if err := os.WriteFile(paths.StatePath, []byte("state"), 0o644); err != nil {
		t.Fatalf("WriteFile state returned error: %v", err)
	}
	if err := os.WriteFile(paths.MemoryPath, []byte("memory"), 0o644); err != nil {
		t.Fatalf("WriteFile memory returned error: %v", err)
	}
	if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(LocalPythonConfig{MemoryMiB: 1024, VCPUs: 1, SwapMiB: 0, WorkspaceMiB: 16})+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile version returned error: %v", err)
	}

	got := localPythonSnapshotCheck(paths, LocalPythonConfig{MemoryMiB: 128, VCPUs: 1, WorkspaceMiB: 16})
	if got.OK || !containsAll(got.Reason, "snapshot version mismatch:", "memory_mib=1024", "memory_mib=128") {
		t.Fatalf("snapshot check = %#v, want resource version mismatch", got)
	}
}

func TestLocalPythonSnapshotRejectsChangedAssets(t *testing.T) {
	for _, tc := range []struct {
		name string
		grow bool
	}{
		{name: "rootfs", grow: true},
		{name: "kernel"},
		{name: "firecracker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := LocalPythonConfig{
				MemoryMiB: 128, VCPUs: 1, WorkspaceMiB: 16,
				KernelPath: filepath.Join(dir, "kernel"), RootfsPath: filepath.Join(dir, "rootfs"), FirecrackerPath: filepath.Join(dir, "firecracker"),
			}
			for _, name := range []string{"kernel", "rootfs", "firecracker", "snapshot.bin", "memory.bin"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("original"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			paths := localPythonSnapshotPaths(dir)
			if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(cfg)), 0o644); err != nil {
				t.Fatal(err)
			}
			if !localPythonSnapshotCheck(paths, cfg).OK {
				t.Fatal("unchanged assets rejected")
			}
			asset := filepath.Join(dir, tc.name)
			info, err := os.Stat(asset)
			if err != nil {
				t.Fatal(err)
			}
			if tc.grow {
				if err := os.WriteFile(asset, []byte("replacement rootfs"), 0o755); err != nil {
					t.Fatal(err)
				}
				// Size alone must detect replacement even when mtime is preserved.
				if err := os.Chtimes(asset, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			} else {
				modified := info.ModTime().Add(time.Second)
				if err := os.Chtimes(asset, modified, modified); err != nil {
					t.Fatal(err)
				}
			}
			if got := localPythonSnapshotCheck(paths, cfg); got.OK || !strings.Contains(got.Reason, "snapshot version mismatch") {
				t.Fatalf("changed %s accepted: %#v", tc.name, got)
			}
		})
	}
}

func TestLocalPythonSnapshotRootfsVersionUsesPackagedChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rootfs.ext4")
	if err := os.WriteFile(path, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".sha256", []byte("first-build\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	version := localPythonSnapshotRootfsVersion(path)
	// Normal guest filesystem writes must not invalidate every worker's cache.
	if err := os.WriteFile(path, []byte("modified"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := localPythonSnapshotRootfsVersion(path); got != version {
		t.Fatalf("guest write changed rootfs version: %q != %q", got, version)
	}
	if err := os.WriteFile(path+".sha256", []byte("next-build\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := localPythonSnapshotRootfsVersion(path); got == version {
		t.Fatal("same-size rootfs rebuild did not change snapshot version")
	}
}

func TestLocalPythonSnapshotCheckRequiresSwapFileWhenEnabled(t *testing.T) {
	dir := t.TempDir()
	paths := localPythonSnapshotPaths(dir)
	cfg := LocalPythonConfig{MemoryMiB: 128, VCPUs: 1, SwapMiB: 512, WorkspaceMiB: 16}
	if err := os.WriteFile(paths.StatePath, []byte("state"), 0o644); err != nil {
		t.Fatalf("WriteFile state returned error: %v", err)
	}
	if err := os.WriteFile(paths.MemoryPath, []byte("memory"), 0o644); err != nil {
		t.Fatalf("WriteFile memory returned error: %v", err)
	}
	if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(cfg)+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile version returned error: %v", err)
	}

	got := localPythonSnapshotCheck(paths, cfg)
	if got.OK || !strings.Contains(got.Reason, "missing snapshot swap file") {
		t.Fatalf("snapshot check = %#v, want missing swap reason", got)
	}
}

func TestLocalPythonSnapshotCheckAcceptsMatchingVersion(t *testing.T) {
	dir := t.TempDir()
	paths := localPythonSnapshotPaths(dir)
	cfg := LocalPythonConfig{MemoryMiB: 128, VCPUs: 1, WorkspaceMiB: 16}
	if err := os.WriteFile(paths.StatePath, []byte("state"), 0o644); err != nil {
		t.Fatalf("WriteFile state returned error: %v", err)
	}
	if err := os.WriteFile(paths.MemoryPath, []byte("memory"), 0o644); err != nil {
		t.Fatalf("WriteFile memory returned error: %v", err)
	}
	if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(cfg)+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile version returned error: %v", err)
	}

	got := localPythonSnapshotCheck(paths, cfg)
	if !got.OK || got.Reason != "" {
		t.Fatalf("snapshot check = %#v, want ok", got)
	}
}

func TestInvalidLocalPythonSnapshotLoadErrorDetectsFirecrackerDecodeFailure(t *testing.T) {
	err := errors.New(`load snapshot: firecracker API PUT /snapshot/load returned 400 : {"fault_message":"Load snapshot error: Failed to restore from snapshot: Failed to get snapshot state from file: Failed to load snapshot state from file: An error occured during bincode decoding: LimitExceeded"}`)

	if !isInvalidLocalPythonSnapshotLoadError(err) {
		t.Fatalf("isInvalidLocalPythonSnapshotLoadError(%v) = false, want true", err)
	}
	if isInvalidLocalPythonSnapshotLoadError(errors.New("run firecracker snapshot: signal: killed")) {
		t.Fatal("isInvalidLocalPythonSnapshotLoadError accepted unrelated error")
	}
}

func TestInvalidateLocalPythonSnapshotRemovesStateMemoryAndVersion(t *testing.T) {
	dir := t.TempDir()
	paths := localPythonSnapshotPaths(dir)
	for _, path := range []string{paths.StatePath, paths.MemoryPath, paths.VersionPath, paths.WorkspacePath} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("WriteFile %s returned error: %v", path, err)
		}
	}

	if err := invalidateLocalPythonSnapshot(paths); err != nil {
		t.Fatalf("invalidateLocalPythonSnapshot returned error: %v", err)
	}
	for _, path := range []string{paths.StatePath, paths.MemoryPath, paths.VersionPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("snapshot file %s stat err = %v, want not exist", path, err)
		}
	}
	if _, err := os.Stat(paths.WorkspacePath); err != nil {
		t.Fatalf("workspace image stat err = %v, want retained", err)
	}
}

func TestLocalPythonPrepareSnapshotInputLeavesWorkspaceUnmounted(t *testing.T) {
	got := localPythonPrepareSnapshotInput(LocalPythonConfig{SwapMiB: 64})

	if !containsAll(got, "mkswap", "swapon", "/dev/vdc") || strings.Contains(got, "/dev/vdb") {
		t.Fatalf("prepare snapshot input = %q, want swap setup and an unmounted workspace", got)
	}
	if strings.Contains(got, "__NATS_SANDBOX_RUNTIME_SNAPSHOT_READY__") {
		t.Fatal("serial input contains the readiness marker before setup executes")
	}
}

func TestLocalPythonPrepareSnapshotInputDoesNotReportSuccessAfterFailure(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required to exercise the guest REPL")
	}
	for _, fail := range []bool{false, true} {
		// Independent REPL lines continue after an exception; one exec block
		// must prevent the marker on failure, but print it after successful setup.
		mock := "None"
		if fail {
			mock = "(_ for _ in ()).throw(RuntimeError('mount failed'))"
		}
		input := "import os, subprocess\nos.makedirs = lambda *a, **kw: None\n" +
			"subprocess.run = lambda *a, **kw: " + mock + "\n" +
			localPythonPrepareSnapshotInput(LocalPythonConfig{SwapMiB: 64}) + "print('REPL_STILL_RUNNING')\n"
		cmd := exec.CommandContext(t.Context(), python, "-i", "-q")
		cmd.Stdin = strings.NewReader(input)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(output, []byte("REPL_STILL_RUNNING")) || (fail && !bytes.Contains(output, []byte("mount failed"))) {
			t.Fatalf("did not exercise the REPL setup: %s", output)
		}
		if bytes.Contains(output, []byte("__NATS_SANDBOX_RUNTIME_SNAPSHOT_READY__")) == fail {
			t.Fatalf("readiness mismatch (setup failure=%t): %s", fail, output)
		}
	}
}

func TestLocalPythonSnapshotCheckRejectsWritableRootDeviceCache(t *testing.T) {
	paths := localPythonSnapshotPaths(t.TempDir())
	cfg := LocalPythonConfig{MemoryMiB: 128, VCPUs: 1, WorkspaceMiB: 16}
	for _, path := range []string{paths.StatePath, paths.MemoryPath} {
		if err := os.WriteFile(path, []byte("old cache"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	oldVersion := strings.Replace(localPythonSnapshotVersion(cfg), "workspace-v8", "workspace-v7", 1)
	oldVersion = strings.Replace(oldVersion, " root_device_ro=1 workspace_mount=on_restore", "", 1)
	if err := os.WriteFile(paths.VersionPath, []byte(oldVersion), 0o644); err != nil {
		t.Fatal(err)
	}
	if check := localPythonSnapshotCheck(paths, cfg); check.OK {
		t.Fatal("accepted a snapshot that restores a writable root device")
	}
}

func TestLocalPythonStartupOutputConcurrentReadinessAndBoundedTail(t *testing.T) {
	var output localPythonStartupOutput
	var writers sync.WaitGroup
	writers.Go(func() {
		for range 1000 {
			_, _ = output.Write([]byte("booting\n"))
		}
		_, _ = output.Write([]byte("python repl ready"))
	})
	err := waitForBuffer(t.Context(), &output, "python repl ready", time.Second)
	writers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = output.Write(bytes.Repeat([]byte("x"), 2*localPythonStartupOutputLimit))
	_, _ = output.Write([]byte("last diagnostic"))
	if got := output.String(); len(got) != localPythonStartupOutputLimit || !strings.HasSuffix(got, "last diagnostic") {
		t.Fatalf("startup output tail: length=%d, suffix=%t", len(got), strings.HasSuffix(got, "last diagnostic"))
	}
}

func TestLocalPythonStartupWaitReturnsCancellationCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("firecracker failed before readiness")
	cancel(cause)
	var output localPythonStartupOutput
	if err := waitForBuffer(ctx, &output, "ready", time.Second); !errors.Is(err, cause) {
		t.Fatalf("wait error = %v, want process failure", err)
	}
	if err := waitForUnixSocket(ctx, filepath.Join(t.TempDir(), "missing.socket"), time.Second); !errors.Is(err, cause) {
		t.Fatalf("socket wait error = %v, want process failure", err)
	}
}

func TestLocalPythonSnapshotStartupReapsProcessAndKeepsDiagnosticsPrivate(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancel), func(t *testing.T) {
			dir := t.TempDir()
			binary := filepath.Join(dir, "firecracker")
			script := "#!/bin/sh\nprintf 'private console failure'\nprintf 'private stderr failure' >&2\nexit 23\n"
			if cancel {
				script = "#!/bin/sh\nprintf 'waiting for cancellation' >&2\nexec sleep 30\n"
			}
			if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			cfg := LocalPythonConfig{FirecrackerPath: binary, SnapshotDir: dir, HideFirecrackerLog: true}
			paths := localPythonSnapshotPaths(dir)
			if err := os.WriteFile(paths.WorkspacePath, []byte("workspace"), 0o644); err != nil {
				t.Fatal(err)
			}
			ctx, stop := context.WithTimeout(t.Context(), time.Second)
			defer stop()
			var responseStderr bytes.Buffer
			started := time.Now()
			err := ensureLocalPythonSnapshot(ctx, cfg, paths, &responseStderr)
			if cancel {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("error = %v, want cancellation", err)
				}
			} else if err == nil || !containsAll(err.Error(), "exit status 23", "private console failure", "private stderr failure") {
				t.Fatalf("error = %v, want captured process failure", err)
			}
			if time.Since(started) > 3*time.Second {
				t.Fatal("startup waited for the readiness timeout after exit/cancellation")
			}
			if responseStderr.Len() != 0 {
				t.Fatalf("private startup diagnostics leaked into response stderr: %s", responseStderr.String())
			}
			if _, err := os.Stat(paths.VersionPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed startup left a cache version: %v", err)
			}
		})
	}
}

func TestLocalPythonExecInputMountsCurrentWorkspaceAndMakesTreeWritableByRunner(t *testing.T) {
	got := localPythonExecInput("pass", "<exec>", false)

	if !containsAll(got, "--flushbufs", "/dev/vdb", "/usr/bin/mount", "chown", "-R", "65534:65534", "/workspace") {
		t.Fatalf("exec input = %q, want current workspace mounted and writable by runner", got)
	}
}

func TestLocalPythonSnapshotStdinMountsWorkspaceBeforeInteractiveInput(t *testing.T) {
	command := "print('interactive command')\n"
	input, err := io.ReadAll(localPythonSnapshotStdin(LocalPythonConfig{}, "", strings.NewReader(command)))
	if err != nil {
		t.Fatal(err)
	}
	if !containsAll(string(input), "--flushbufs", "/usr/bin/mount", "/workspace") || !strings.HasSuffix(string(input), command) {
		t.Fatalf("interactive input = %q, want workspace setup before user input", input)
	}
	if bytes.Contains(input, []byte("raise SystemExit")) {
		t.Fatal("workspace setup closes the interactive session")
	}
}

func TestLocalPythonExecInputRunsUserCodeUnprivileged(t *testing.T) {
	got := localPythonExecInput("print('hello')\n", "/workspace/script.py", true)

	if !containsAll(got, "os.setgid(65534)", "os.setuid(65534)", "os.chdir('/workspace')", "compile(") {
		t.Fatalf("exec input = %q, want user code to run unprivileged from workspace", got)
	}
	if strings.Contains(got, "mkswap") || strings.Contains(got, "swapon") {
		t.Fatalf("exec input = %q, want swap already prepared in the snapshot", got)
	}
	if strings.Index(got, "/usr/bin/mount") > strings.Index(got, "os.setuid(65534)") {
		t.Fatal("workspace must be mounted before dropping privileges")
	}
}

func TestLocalPythonExecInputDoesNotRunUserCodeAfterSetupFailure(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required to exercise the guest REPL")
	}
	input := "import os, subprocess\n" +
		"subprocess.run = lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('workspace setup failed'))\n" +
		localPythonExecInput("print('USER_CODE_EXECUTED')", "<exec>", false)
	cmd := exec.CommandContext(t.Context(), python, "-i", "-q")
	cmd.Stdin = strings.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(output, []byte("workspace setup failed")) || bytes.Contains(output, []byte("USER_CODE_EXECUTED")) {
		t.Fatalf("user-code execution was not stopped after setup failed: %s", output)
	}
}

func TestLocalPythonExecSourceUsesInlineCommand(t *testing.T) {
	source, err := localPythonExecSource(LocalPythonConfig{InlineCommand: "print(42)"})
	if err != nil {
		t.Fatalf("localPythonExecSource returned error: %v", err)
	}

	if source != "print(42)" {
		t.Fatalf("source = %q, want inline command", source)
	}
}

func TestLocalPythonExecSourceReadsScriptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "script.py")
	if err := os.WriteFile(path, []byte("print('from file')\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	source, err := localPythonExecSource(LocalPythonConfig{ExecFilePath: path})
	if err != nil {
		t.Fatalf("localPythonExecSource returned error: %v", err)
	}

	if source != "print('from file')\n" {
		t.Fatalf("source = %q, want file contents", source)
	}
}

func TestLocalPythonExecInputWrapsSourceAsCompiledScript(t *testing.T) {
	got := localPythonExecInput("print('hello')\n", "script.py", true)

	if !containsAll(got, "import base64", "compile(", "script.py", "raise SystemExit") {
		t.Fatalf("exec input = %q, want compiled script wrapper", got)
	}
}

func TestLocalPythonExecInputCanSkipGuestSync(t *testing.T) {
	got := localPythonExecInput("print('hello')\n", "script.py", false)

	if strings.Contains(got, "/usr/bin/sync") {
		t.Fatalf("exec input = %q, want no guest sync command", got)
	}
}

func TestLocalPythonCompletionWriterDetectsMarker(t *testing.T) {
	var stdout bytes.Buffer
	writer := newLocalPythonCompletionWriter(&stdout, "DONE")

	if _, err := writer.Write([]byte("hello\nDO")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	select {
	case <-writer.Done():
		t.Fatal("Done closed before full marker")
	default:
	}
	if _, err := writer.Write([]byte("NE\n")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	select {
	case <-writer.Done():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for marker detection")
	}
	if stdout.String() != "hello\nDONE\n" {
		t.Fatalf("stdout = %q, want copied writes", stdout.String())
	}
}

func TestLocalPythonCompletionRequiresMarkerAfterCleanExit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		output string
		failed bool
	}{
		{name: "python traceback", output: "SyntaxError: source code string cannot contain null bytes\n", failed: true},
		{name: "empty output", failed: true},
		{name: "completed script", output: "__TEST_COMPLETE__\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			writer := newLocalPythonCompletionWriter(&stdout, "__TEST_COMPLETE__")
			cmd := exec.Command("sh", "-c", "printf '%s' \"$1\"", "completion-test", tc.output)
			cmd.Stdout = writer
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			err := waitForLocalPythonSnapshotCompletion(context.Background(), LocalPythonConfig{KillAfterCompletion: true}, cmd, writer, nil)
			if tc.failed {
				if err == nil || !strings.Contains(err.Error(), "completion marker") {
					t.Fatalf("error = %v, want missing completion marker", err)
				}
			} else if err != nil {
				t.Fatalf("completed script returned error: %v", err)
			}
		})
	}
}

func TestLocalPythonExecFilenameMapsWorkspaceScriptIntoGuest(t *testing.T) {
	got := localPythonExecFilename(LocalPythonConfig{
		WorkspaceDir: "tmp/workspace",
		ExecFilePath: "tmp/workspace/test-file.py",
	})

	if got != "/workspace/test-file.py" {
		t.Fatalf("filename = %q, want guest workspace path", got)
	}
}

func TestLocalPythonExecFilenameMapsExternalScriptIntoWorkspaceRoot(t *testing.T) {
	got := localPythonExecFilename(LocalPythonConfig{
		WorkspaceDir: "tmp/workspace",
		ExecFilePath: "tmp/test-file.py",
	})

	if got != "/workspace/test-file.py" {
		t.Fatalf("filename = %q, want guest workspace basename", got)
	}
}

func TestLocalPythonFirecrackerExtraFilesOpenWorkspaceAndSwapInDriveOrder(t *testing.T) {
	dir := t.TempDir()
	workspacePath := filepath.Join(dir, "workspace.ext4")
	swapPath := filepath.Join(dir, "swap.raw")
	if err := os.WriteFile(workspacePath, []byte("workspace"), 0o600); err != nil {
		t.Fatalf("WriteFile workspace returned error: %v", err)
	}
	if err := os.WriteFile(swapPath, []byte("swap"), 0o600); err != nil {
		t.Fatalf("WriteFile swap returned error: %v", err)
	}

	files, closeFiles, err := localPythonFirecrackerExtraFiles(LocalPythonConfig{SwapMiB: 64}, workspacePath, swapPath)
	if err != nil {
		t.Fatalf("localPythonFirecrackerExtraFiles returned error: %v", err)
	}
	defer closeFiles()

	if len(files) != 2 {
		t.Fatalf("extra files length = %d, want 2", len(files))
	}
	if filepath.Base(files[0].Name()) != "workspace.ext4" {
		t.Fatalf("fd 3 file = %q, want workspace image", files[0].Name())
	}
	if filepath.Base(files[1].Name()) != "swap.raw" {
		t.Fatalf("fd 4 file = %q, want swap image", files[1].Name())
	}
}

func TestPrintLocalPythonBenchmarkProgress(t *testing.T) {
	var stdout bytes.Buffer

	printLocalPythonBenchmarkProgress(&stdout, 2, 5)

	if got := stdout.String(); got != "progress completed=2 total=5\n" {
		t.Fatalf("progress output = %q, want completed progress line", got)
	}
}

func TestLocalPythonBenchmarkStats(t *testing.T) {
	got := localPythonBenchmarkStats([]localPythonBenchmarkRun{
		{Restore: 10 * time.Millisecond, Exec: 77 * time.Millisecond, Metrics: processMetrics{MaxRSSBytes: 10 << 20, CPU: 4 * time.Millisecond, MaxCPUPercent: 90}},
		{Restore: 5 * time.Millisecond, Exec: 31 * time.Millisecond, Metrics: processMetrics{MaxRSSBytes: 20 << 20, CPU: 7 * time.Millisecond, MaxCPUPercent: 110}},
		{Restore: 7 * time.Millisecond, Exec: 60 * time.Millisecond, Metrics: processMetrics{MaxRSSBytes: 15 << 20, CPU: 6 * time.Millisecond, MaxCPUPercent: 80}},
		{Restore: 12 * time.Millisecond, Exec: 89 * time.Millisecond, Metrics: processMetrics{MaxRSSBytes: 18 << 20, CPU: 8 * time.Millisecond, MaxCPUPercent: 130}},
		{Restore: 6 * time.Millisecond, Exec: 52 * time.Millisecond, Metrics: processMetrics{MaxRSSBytes: 12 << 20, CPU: 5 * time.Millisecond, MaxCPUPercent: 100}},
	})

	want := localPythonBenchmarkResult{
		Runs:          5,
		RestoreAvg:    8 * time.Millisecond,
		RestoreMin:    5 * time.Millisecond,
		RestoreP50:    7 * time.Millisecond,
		RestoreP90:    12 * time.Millisecond,
		RestoreMax:    12 * time.Millisecond,
		ExecAvg:       61800 * time.Microsecond,
		ExecMin:       31 * time.Millisecond,
		ExecP50:       60 * time.Millisecond,
		ExecP90:       89 * time.Millisecond,
		ExecMax:       89 * time.Millisecond,
		MaxRSSBytes:   20 << 20,
		MaxCPU:        8 * time.Millisecond,
		MaxCPUPercent: 130,
		RestoreTimes:  []time.Duration{5 * time.Millisecond, 6 * time.Millisecond, 7 * time.Millisecond, 10 * time.Millisecond, 12 * time.Millisecond},
		ExecTimes:     []time.Duration{31 * time.Millisecond, 52 * time.Millisecond, 60 * time.Millisecond, 77 * time.Millisecond, 89 * time.Millisecond},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stats = %#v, want %#v", got, want)
	}
}

func TestParseProcStatMetrics(t *testing.T) {
	stat := "25873 (firecracker) R 1 1 1 0 -1 0 0 0 0 0 7 3 0 0 20 0 1 0 0 3211264 384"

	got, err := parseProcStatMetrics(stat, 4096, 100)
	if err != nil {
		t.Fatalf("parseProcStatMetrics returned error: %v", err)
	}

	if got.MaxRSSBytes != 384*4096 {
		t.Fatalf("MaxRSSBytes = %d, want %d", got.MaxRSSBytes, 384*4096)
	}
	if got.CPU != 100*time.Millisecond {
		t.Fatalf("CPU = %s, want 100ms", got.CPU)
	}
}

func TestLocalPythonFirecrackerBinaryPrefersConfiguredPath(t *testing.T) {
	dir := t.TempDir()
	localBinary := filepath.Join(dir, "bin", "firecracker")
	if err := os.MkdirAll(filepath.Dir(localBinary), 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(localBinary, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	got := localPythonFirecrackerBinary(LocalPythonConfig{FirecrackerPath: localBinary})

	if got != localBinary {
		t.Fatalf("firecracker binary = %q, want configured path", got)
	}
}

func TestLocalPythonFirecrackerBinaryFallsBackToPathName(t *testing.T) {
	got := localPythonFirecrackerBinary(LocalPythonConfig{FirecrackerPath: "missing/firecracker"})

	if got != "firecracker" {
		t.Fatalf("firecracker binary = %q, want PATH fallback", got)
	}
}

func TestLocalPythonFirecrackerArgsHideLogsToFile(t *testing.T) {
	got := localPythonFirecrackerArgs(LocalPythonConfig{HideFirecrackerLog: true}, "api.sock", "vm.json", "firecracker.log")

	want := []string{"--api-sock", "api.sock", "--config-file", "vm.json", "--log-path", "firecracker.log"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestLocalPythonFirecrackerStderrUsesProvidedWriterByDefault(t *testing.T) {
	var stderr bytes.Buffer

	writer := localPythonFirecrackerStderr(LocalPythonConfig{}, &stderr)
	if _, err := writer.Write([]byte("firecracker log")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	if got := stderr.String(); got != "firecracker log" {
		t.Fatalf("stderr = %q, want firecracker log", got)
	}
}

func TestLocalPythonFirecrackerStderrCanHideFirecrackerLog(t *testing.T) {
	var stderr bytes.Buffer

	writer := localPythonFirecrackerStderr(LocalPythonConfig{HideFirecrackerLog: true}, &stderr)
	if _, err := writer.Write([]byte("firecracker log")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	if got := stderr.String(); got != "" {
		t.Fatalf("stderr = %q, want hidden firecracker log", got)
	}
}

func TestWriteLocalPythonFirecrackerLogCopiesNonEmptyLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "firecracker.log")
	if err := os.WriteFile(logPath, []byte("fatal runtime detail\n"), 0o644); err != nil {
		t.Fatalf("write log returned error: %v", err)
	}

	var stderr bytes.Buffer
	writeLocalPythonFirecrackerLog(&stderr, logPath)

	if got := stderr.String(); !containsAll(got, "firecracker log:", "fatal runtime detail") {
		t.Fatalf("stderr = %q, want firecracker log contents", got)
	}
}

func TestCheckKVMDeviceReportsOpenFailure(t *testing.T) {
	err := checkKVMDevice("/path/that/does/not/exist")
	if err == nil {
		t.Fatal("checkKVMDevice returned nil, want error")
	}
	if got := err.Error(); got == "" || !containsAll(got, "/path/that/does/not/exist", "--device=/dev/kvm") {
		t.Fatalf("error = %q, want path and container hint", got)
	}
}

func containsAll(value string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(value, needle) {
			return false
		}
	}
	return true
}

func requireCommand(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s is required for this test: %v", name, err)
	}
}
