package app

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLocalPythonBootArgsStartInteractivePythonAsInit(t *testing.T) {
	got := localPythonBootArgs("", 123, false)

	want := "console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda rw init=/usr/local/bin/fc-python-init fc_start_ns=123"
	if got != want {
		t.Fatalf("boot args = %q, want %q", got, want)
	}
}

func TestLocalPythonBootArgsRunInlineCommand(t *testing.T) {
	got := localPythonBootArgs(`print("hello")`, 456, false)

	want := `console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda rw init=/usr/local/bin/fc-python-init fc_start_ns=456 fc_py_b64=cHJpbnQoImhlbGxvIik=`
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
	want := `snapshot version mismatch: got "workspace-v6 memory_mib=1024 vcpus=1 swap_mib=0 workspace_mib=16 drive_fd=1 system_ro=1 user_uid=65534", want "workspace-v6 memory_mib=128 vcpus=1 swap_mib=0 workspace_mib=16 drive_fd=1 system_ro=1 user_uid=65534"`
	if got.OK || got.Reason != want {
		t.Fatalf("snapshot check = %#v, want mismatch reason %q", got, want)
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

func TestLocalPythonPrepareSnapshotInputMountsWorkspaceAndProtectsSystem(t *testing.T) {
	got := localPythonPrepareSnapshotInput(LocalPythonConfig{SwapMiB: 64})

	if !containsAll(got, "mount", "/dev/vdb", "/workspace", "mkswap", "swapon", "remount,ro", "__NATS_SERVICE_TESTS_SNAPSHOT_READY__") {
		t.Fatalf("prepare snapshot input = %q, want workspace mount, swap setup, and root remount before snapshot", got)
	}
}

func TestLocalPythonPrepareSnapshotInputMakesWorkspaceTreeWritableByRunner(t *testing.T) {
	got := localPythonPrepareSnapshotInput(LocalPythonConfig{})

	if !containsAll(got, "chown", "-R", "65534:65534", "/workspace") {
		t.Fatalf("prepare snapshot input = %q, want recursive workspace ownership for runner", got)
	}
}

func TestLocalPythonExecInputRunsUserCodeUnprivileged(t *testing.T) {
	got := localPythonExecInput("print('hello')\n", "/workspace/script.py")

	if !containsAll(got, "os.setgid(65534)", "os.setuid(65534)", "os.chdir('/workspace')", "compile(") {
		t.Fatalf("exec input = %q, want user code to run unprivileged from workspace", got)
	}
	if strings.Contains(got, "mkswap") || strings.Contains(got, "swapon") || strings.Contains(got, "/usr/bin/mount") {
		t.Fatalf("exec input = %q, want no privileged guest setup in user-code wrapper", got)
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
	got := localPythonExecInput("print('hello')\n", "script.py")

	if !containsAll(got, "import base64", "compile(", "script.py", "raise SystemExit") {
		t.Fatalf("exec input = %q, want compiled script wrapper", got)
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
