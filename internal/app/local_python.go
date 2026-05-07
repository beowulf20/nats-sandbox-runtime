package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type firecrackerVMConfig struct {
	BootSource    firecrackerBootSource    `json:"boot-source"`
	Drives        []firecrackerDrive       `json:"drives"`
	MachineConfig firecrackerMachineConfig `json:"machine-config"`
}

type firecrackerBootSource struct {
	KernelImagePath string `json:"kernel_image_path"`
	BootArgs        string `json:"boot_args"`
}

type firecrackerDrive struct {
	DriveID      string `json:"drive_id"`
	PathOnHost   string `json:"path_on_host"`
	IsRootDevice bool   `json:"is_root_device"`
	IsReadOnly   bool   `json:"is_read_only"`
}

type firecrackerMachineConfig struct {
	VcpuCount  int64 `json:"vcpu_count"`
	MemSizeMiB int64 `json:"mem_size_mib"`
}

const (
	localPythonWorkspaceDrivePath = "/proc/self/fd/3"
	localPythonSwapDrivePath      = "/proc/self/fd/4"
)

type localPythonSnapshotFiles struct {
	StatePath     string
	MemoryPath    string
	WorkspacePath string
	SwapPath      string
	VersionPath   string
}

type localPythonSnapshotCheckResult struct {
	OK     bool
	Reason string
}

type localPythonBenchmarkResult struct {
	Runs          int
	RestoreAvg    time.Duration
	RestoreMin    time.Duration
	RestoreP50    time.Duration
	RestoreP90    time.Duration
	RestoreMax    time.Duration
	ExecAvg       time.Duration
	ExecMin       time.Duration
	ExecP50       time.Duration
	ExecP90       time.Duration
	ExecMax       time.Duration
	MaxRSSBytes   int64
	MaxCPU        time.Duration
	MaxCPUPercent float64
	RestoreTimes  []time.Duration
	ExecTimes     []time.Duration
}

type localPythonBenchmarkRun struct {
	Restore time.Duration
	Exec    time.Duration
	Metrics processMetrics
}

type LocalPythonExecResult struct {
	Elapsed time.Duration
	Stdout  []byte
	Stderr  []byte
}

type processMetrics struct {
	MaxRSSBytes   int64
	CPU           time.Duration
	MaxCPUPercent float64
}

func RunLocalPython(ctx context.Context, cfg LocalPythonConfig, stdin io.Reader, stdout, stderr io.Writer) error {
	if err := validateLocalPythonConfig(cfg); err != nil {
		return err
	}
	if _, err := os.Stat(cfg.KernelPath); err != nil {
		return fmt.Errorf("kernel path %q: %w", cfg.KernelPath, err)
	}
	if _, err := os.Stat(cfg.RootfsPath); err != nil {
		return fmt.Errorf("rootfs path %q: %w", cfg.RootfsPath, err)
	}
	if err := checkKVMDevice("/dev/kvm"); err != nil {
		return err
	}
	paths := localPythonSnapshotPathsForConfig(cfg)
	if err := ensureLocalPythonWorkspaceImage(ctx, cfg, paths); err != nil {
		return err
	}
	if err := ensureLocalPythonSnapshot(ctx, cfg, paths, stderr); err != nil {
		return err
	}
	if cfg.Runs > 1 {
		return benchmarkLocalPythonSnapshot(ctx, cfg, paths, stdout, stderr)
	}
	return runLocalPythonSnapshot(ctx, cfg, paths, stdin, stdout, stderr)
}

func RunLocalPythonExec(ctx context.Context, cfg LocalPythonConfig) (LocalPythonExecResult, error) {
	if cfg.InlineCommand == "" && cfg.ExecFilePath == "" {
		return LocalPythonExecResult{}, fmt.Errorf("local python exec requires inline command or exec file")
	}
	cfg.Runs = 1
	if cfg.ParallelRuns == 0 {
		cfg.ParallelRuns = 1
	}
	if err := validateLocalPythonConfig(cfg); err != nil {
		return LocalPythonExecResult{}, err
	}
	if cfg.ExecTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ExecTimeout+15*time.Second)
		defer cancel()
	}
	var stdout, stderr bytes.Buffer
	if _, err := os.Stat(cfg.KernelPath); err != nil {
		return LocalPythonExecResult{}, fmt.Errorf("kernel path %q: %w", cfg.KernelPath, err)
	}
	if _, err := os.Stat(cfg.RootfsPath); err != nil {
		return LocalPythonExecResult{}, fmt.Errorf("rootfs path %q: %w", cfg.RootfsPath, err)
	}
	if err := checkKVMDevice("/dev/kvm"); err != nil {
		return LocalPythonExecResult{}, err
	}
	paths := localPythonSnapshotPathsForConfig(cfg)
	if err := ensureLocalPythonWorkspaceImage(ctx, cfg, paths); err != nil {
		return LocalPythonExecResult{}, err
	}
	if err := ensureLocalPythonSnapshot(ctx, cfg, paths, &stderr); err != nil {
		return LocalPythonExecResult{}, err
	}
	elapsed, err := runLocalPythonSnapshotOnce(ctx, cfg, paths, nil, &stdout, &stderr)
	if err != nil {
		return LocalPythonExecResult{}, err
	}
	return LocalPythonExecResult{
		Elapsed: elapsed,
		Stdout:  stdout.Bytes(),
		Stderr:  stderr.Bytes(),
	}, nil
}

func ensureLocalPythonSnapshot(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles, stderr io.Writer) error {
	check := localPythonSnapshotCheck(paths, cfg)
	if check.OK {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(paths.StatePath), 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	_ = os.Remove(paths.StatePath)
	_ = os.Remove(paths.MemoryPath)
	_ = os.Remove(paths.VersionPath)
	if cfg.SwapMiB > 0 {
		if err := ensureLocalPythonSwapImage(paths, cfg); err != nil {
			return err
		}
	}

	if !cfg.HideFirecrackerLog {
		if check.Reason != "" {
			fmt.Fprintf(stderr, "python snapshot cache invalid: %s\n", check.Reason)
		}
		fmt.Fprintf(stderr, "creating python snapshot: %s\n", filepath.Dir(paths.StatePath))
	}

	workDir, err := os.MkdirTemp("", "nats-service-tests-firecracker-*")
	if err != nil {
		return fmt.Errorf("create firecracker work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	startedAt := time.Now()
	configPath := filepath.Join(workDir, "vm.json")
	snapshotCfg := cfg
	snapshotCfg.InlineCommand = ""
	snapshotCfg.HideFirecrackerLog = true
	if err := writeLocalPythonConfig(configPath, snapshotCfg, paths, 0); err != nil {
		return err
	}

	apiSock := filepath.Join(workDir, "firecracker.socket")
	cmd := exec.CommandContext(ctx, localPythonFirecrackerBinary(cfg), localPythonFirecrackerArgs(snapshotCfg, apiSock, configPath, filepath.Join(workDir, "firecracker.log"))...)
	extraFiles, closeExtraFiles, err := localPythonFirecrackerExtraFiles(snapshotCfg, paths.WorkspacePath, paths.SwapPath)
	if err != nil {
		return err
	}
	defer closeExtraFiles()
	cmd.ExtraFiles = extraFiles
	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create snapshot stdin pipe: %w", err)
	}
	defer stdinRead.Close()
	defer stdinWrite.Close()
	cmd.Stdin = stdinRead
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("create snapshot stdout pipe: %w", err)
	}
	cmd.Stderr = localPythonFirecrackerStderr(snapshotCfg, stderr)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start firecracker for snapshot: %w", err)
	}
	waitDone := make(chan struct{})
	var output bytes.Buffer
	go func() {
		_, _ = io.Copy(&output, stdoutPipe)
		close(waitDone)
	}()
	if err := waitForUnixSocket(ctx, apiSock, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	if err := waitForBuffer(ctx, &output, "python repl ready", 10*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("wait for python snapshot readiness: %w", err)
	}
	if _, err := io.WriteString(stdinWrite, localPythonPrepareSnapshotInput(cfg)); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("prepare snapshot guest: %w", err)
	}
	if err := waitForBuffer(ctx, &output, "__NATS_SERVICE_TESTS_SNAPSHOT_READY__", 10*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("wait for snapshot guest readiness: %w", err)
	}
	if err := firecrackerAPIRequest(ctx, apiSock, http.MethodPatch, "/vm", map[string]string{"state": "Paused"}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("pause snapshot VM: %w", err)
	}
	if err := firecrackerAPIRequest(ctx, apiSock, http.MethodPut, "/snapshot/create", map[string]string{
		"snapshot_type": "Full",
		"snapshot_path": paths.StatePath,
		"mem_file_path": paths.MemoryPath,
	}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("create snapshot: %w", err)
	}
	_ = cmd.Process.Kill()
	err = cmd.Wait()
	<-waitDone
	if !cfg.HideFirecrackerLog {
		fmt.Fprintf(stderr, "python snapshot ready after %s\n", time.Since(startedAt).Round(time.Millisecond))
	}
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("stop snapshot VM: %w", err)
	}
	if err := os.WriteFile(paths.VersionPath, []byte(localPythonSnapshotVersion(cfg)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write snapshot version: %w", err)
	}
	if check := localPythonSnapshotCheck(paths, cfg); !check.OK {
		return fmt.Errorf("snapshot validation failed after create: %s", check.Reason)
	}
	return nil
}

func ensureLocalPythonWorkspaceImage(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles) error {
	if err := os.MkdirAll(cfg.WorkspaceDir, 0o755); err != nil {
		return fmt.Errorf("create workspace dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.WorkspacePath), 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	sizeBytes, err := directoryRegularFileSize(cfg.WorkspaceDir)
	if err != nil {
		return err
	}
	sizeBytesLimit := cfg.WorkspaceMiB * 1024 * 1024
	if sizeBytes > sizeBytesLimit {
		return fmt.Errorf("workspace files use %d bytes, exceeding workspace-mib limit of %d MiB", sizeBytes, cfg.WorkspaceMiB)
	}
	_ = os.Remove(paths.WorkspacePath)
	cmd := exec.CommandContext(ctx, "mke2fs", "-q", "-t", "ext4", "-b", "1024", "-F", "-d", cfg.WorkspaceDir, paths.WorkspacePath, strconv.FormatInt(cfg.WorkspaceMiB*1024, 10))
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("create workspace ext4 image: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func ensureLocalPythonSwapImage(paths localPythonSnapshotFiles, cfg LocalPythonConfig) error {
	if cfg.SwapMiB == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(paths.SwapPath), 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	file, err := os.OpenFile(paths.SwapPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create swap image: %w", err)
	}
	defer file.Close()
	if err := file.Truncate(cfg.SwapMiB * 1024 * 1024); err != nil {
		return fmt.Errorf("size swap image: %w", err)
	}
	return nil
}

func directoryRegularFileSize(dir string) (int64, error) {
	var size int64
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("measure workspace dir: %w", err)
	}
	return size, nil
}

func runLocalPythonSnapshot(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles, stdin io.Reader, stdout, stderr io.Writer) error {
	_, err := runLocalPythonSnapshotOnce(ctx, cfg, paths, stdin, stdout, stderr)
	return err
}

func runLocalPythonSnapshotOnce(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles, stdin io.Reader, stdout, stderr io.Writer) (time.Duration, error) {
	execSource, err := localPythonExecSource(cfg)
	if err != nil {
		return 0, err
	}
	workDir, err := os.MkdirTemp("", "nats-service-tests-firecracker-*")
	if err != nil {
		return 0, fmt.Errorf("create firecracker work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	startedAt := time.Now()
	apiSock := filepath.Join(workDir, "firecracker.socket")
	cmd := exec.CommandContext(ctx, localPythonFirecrackerBinary(cfg), "--api-sock", apiSock, "--log-path", filepath.Join(workDir, "firecracker.log"))
	extraFiles, closeExtraFiles, err := localPythonFirecrackerExtraFiles(cfg, paths.WorkspacePath, paths.SwapPath)
	if err != nil {
		return 0, err
	}
	defer closeExtraFiles()
	cmd.ExtraFiles = extraFiles
	cmd.Stdin = localPythonSnapshotStdin(cfg, execSource, stdin)
	cmd.Stdout = stdout
	cmd.Stderr = localPythonFirecrackerStderr(cfg, stderr)
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start firecracker: %w", err)
	}
	if err := waitForUnixSocket(ctx, apiSock, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return 0, err
	}
	restoreStartedAt := time.Now()
	if err := firecrackerAPIRequest(ctx, apiSock, http.MethodPut, "/snapshot/load", map[string]any{
		"snapshot_path": paths.StatePath,
		"mem_backend": map[string]string{
			"backend_path": paths.MemoryPath,
			"backend_type": "File",
		},
		"track_dirty_pages": false,
		"resume_vm":         true,
	}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return 0, fmt.Errorf("load snapshot: %w", err)
	}
	err = cmd.Wait()
	elapsed := time.Since(restoreStartedAt)
	if !cfg.HideFirecrackerLog {
		fmt.Fprintf(stderr, "firecracker snapshot run completed after %s\n", time.Since(startedAt).Round(time.Millisecond))
	}
	if err != nil {
		return 0, fmt.Errorf("run firecracker snapshot: %w", err)
	}
	if err := syncLocalPythonWorkspaceImage(ctx, cfg, paths); err != nil {
		return 0, err
	}
	return elapsed, nil
}

func syncLocalPythonWorkspaceImage(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles) error {
	workspaceParent := filepath.Dir(cfg.WorkspaceDir)
	if err := os.MkdirAll(workspaceParent, 0o755); err != nil {
		return fmt.Errorf("create workspace parent dir: %w", err)
	}
	syncDir, err := os.MkdirTemp(workspaceParent, ".workspace-sync-*")
	if err != nil {
		return fmt.Errorf("create workspace sync dir: %w", err)
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = os.RemoveAll(syncDir)
		}
	}()
	cmd := exec.CommandContext(ctx, "debugfs", "-R", "rdump / "+syncDir, paths.WorkspacePath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("sync workspace image: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := os.RemoveAll(cfg.WorkspaceDir); err != nil {
		return fmt.Errorf("replace workspace dir: %w", err)
	}
	if err := os.Rename(syncDir, cfg.WorkspaceDir); err != nil {
		return fmt.Errorf("replace workspace dir: %w", err)
	}
	renamed = true
	return nil
}

type localPythonBenchmarkRunFiles struct {
	WorkspacePath string
	SwapPath      string
}

func prepareLocalPythonBenchmarkRunFiles(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles, workDir string, run int) (localPythonBenchmarkRunFiles, error) {
	runFiles := localPythonBenchmarkRunFiles{
		WorkspacePath: filepath.Join(workDir, fmt.Sprintf("workspace-%d.ext4", run)),
	}
	if err := copyLocalPythonImageFile(ctx, paths.WorkspacePath, runFiles.WorkspacePath); err != nil {
		return localPythonBenchmarkRunFiles{}, fmt.Errorf("prepare benchmark workspace image: %w", err)
	}
	if cfg.SwapMiB > 0 {
		runFiles.SwapPath = filepath.Join(workDir, fmt.Sprintf("swap-%d.raw", run))
		if err := copyLocalPythonImageFile(ctx, paths.SwapPath, runFiles.SwapPath); err != nil {
			return localPythonBenchmarkRunFiles{}, fmt.Errorf("prepare benchmark swap image: %w", err)
		}
	}
	return runFiles, nil
}

func copyLocalPythonImageFile(ctx context.Context, src, dst string) error {
	cmd := exec.CommandContext(ctx, "cp", "--reflink=auto", "--sparse=always", src, dst)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("copy %q to %q: %w: %s", src, dst, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func localPythonFirecrackerExtraFiles(cfg LocalPythonConfig, workspacePath, swapPath string) ([]*os.File, func(), error) {
	workspaceFile, err := os.OpenFile(workspacePath, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open workspace image for firecracker fd: %w", err)
	}
	files := []*os.File{workspaceFile}
	closeFiles := func() {
		for _, file := range files {
			_ = file.Close()
		}
	}
	if cfg.SwapMiB > 0 {
		swapFile, err := os.OpenFile(swapPath, os.O_RDWR, 0)
		if err != nil {
			closeFiles()
			return nil, nil, fmt.Errorf("open swap image for firecracker fd: %w", err)
		}
		files = append(files, swapFile)
	}
	return files, closeFiles, nil
}

func benchmarkLocalPythonSnapshot(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles, stdout, stderr io.Writer) error {
	runs := make([]localPythonBenchmarkRun, cfg.Runs)
	benchCfg := cfg
	benchCfg.HideFirecrackerLog = true

	parallelRuns := cfg.ParallelRuns
	if parallelRuns > cfg.Runs {
		parallelRuns = cfg.Runs
	}
	benchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	var progressMu sync.Mutex
	completedRuns := 0
	for worker := 0; worker < parallelRuns; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for run := range jobs {
				result, err := runLocalPythonBenchmarkOnce(benchCtx, benchCfg, paths, run, stderr)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("benchmark run %d: %w", run, err):
						cancel()
					default:
					}
					return
				}
				runs[run-1] = result
				progressMu.Lock()
				completedRuns++
				printLocalPythonBenchmarkProgress(stdout, completedRuns, cfg.Runs)
				progressMu.Unlock()
			}
		}()
	}

	for i := 1; i <= cfg.Runs; i++ {
		select {
		case jobs <- i:
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case <-benchCtx.Done():
			close(jobs)
			wg.Wait()
			if err := benchCtx.Err(); err != nil {
				return err
			}
			return ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}

	for i, result := range runs {
		fmt.Fprintf(stdout, "run=%d restore_ms=%d exec_ms=%d guest_ram_mib=%d guest_swap_mib=%d max_rss_mib=%.1f max_cpu_pct=%.0f cpu_ms=%d\n", i+1, result.Restore.Milliseconds(), result.Exec.Milliseconds(), cfg.MemoryMiB, cfg.SwapMiB, bytesToMiB(result.Metrics.MaxRSSBytes), result.Metrics.MaxCPUPercent, result.Metrics.CPU.Milliseconds())
	}
	printLocalPythonBenchmarkResult(stdout, localPythonBenchmarkStats(runs), cfg.MemoryMiB, cfg.SwapMiB)
	return nil
}

func runLocalPythonBenchmarkOnce(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles, run int, stderr io.Writer) (localPythonBenchmarkRun, error) {
	execSource, err := localPythonExecSource(cfg)
	if err != nil {
		return localPythonBenchmarkRun{}, err
	}
	workDir, err := os.MkdirTemp("", "nats-service-tests-firecracker-*")
	if err != nil {
		return localPythonBenchmarkRun{}, fmt.Errorf("create firecracker work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	marker := fmt.Sprintf("__NATS_SERVICE_TESTS_BENCH_%d__", run)
	apiSock := filepath.Join(workDir, "firecracker.socket")
	runPaths, err := prepareLocalPythonBenchmarkRunFiles(ctx, cfg, paths, workDir, run)
	if err != nil {
		return localPythonBenchmarkRun{}, err
	}
	cmd := exec.CommandContext(ctx, localPythonFirecrackerBinary(cfg), "--api-sock", apiSock, "--log-path", filepath.Join(workDir, "firecracker.log"))
	extraFiles, closeExtraFiles, err := localPythonFirecrackerExtraFiles(cfg, runPaths.WorkspacePath, runPaths.SwapPath)
	if err != nil {
		return localPythonBenchmarkRun{}, err
	}
	defer closeExtraFiles()
	cmd.ExtraFiles = extraFiles
	cmd.Stdin = strings.NewReader(localPythonExecInput(execSource+"\nprint("+strconv.Quote(marker)+")\n", localPythonExecFilename(cfg)))
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return localPythonBenchmarkRun{}, fmt.Errorf("create benchmark stdout pipe: %w", err)
	}
	cmd.Stderr = localPythonFirecrackerStderr(cfg, stderr)
	if err := cmd.Start(); err != nil {
		return localPythonBenchmarkRun{}, fmt.Errorf("start firecracker: %w", err)
	}
	processStartedAt := time.Now()
	metricsSampler := startProcessMetricsSampler(ctx, cmd.Process.Pid)
	metricsStopped := false
	defer func() {
		if !metricsStopped {
			_ = metricsSampler.Stop()
		}
	}()
	waitDone := make(chan struct{})
	var output bytes.Buffer
	go func() {
		_, _ = io.Copy(&output, stdoutPipe)
		close(waitDone)
	}()
	if err := waitForUnixSocket(ctx, apiSock, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return localPythonBenchmarkRun{}, err
	}
	restoreStartedAt := time.Now()
	if err := firecrackerAPIRequest(ctx, apiSock, http.MethodPut, "/snapshot/load", map[string]any{
		"snapshot_path": paths.StatePath,
		"mem_backend": map[string]string{
			"backend_path": paths.MemoryPath,
			"backend_type": "File",
		},
		"track_dirty_pages": false,
		"resume_vm":         true,
	}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return localPythonBenchmarkRun{}, fmt.Errorf("load snapshot: %w", err)
	}
	restoreElapsed := time.Since(restoreStartedAt)
	execStartedAt := time.Now()
	if err := waitForBuffer(ctx, &output, marker, cfg.ExecTimeout); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return localPythonBenchmarkRun{}, fmt.Errorf("wait for benchmark marker: %w", err)
	}
	execElapsed := time.Since(execStartedAt)
	err = cmd.Wait()
	<-waitDone
	metrics := metricsSampler.Stop()
	metricsStopped = true
	processWall := time.Since(processStartedAt)
	if processWall > 0 {
		metrics.MaxCPUPercent = float64(metrics.CPU) / float64(processWall) * 100
	}
	if err != nil {
		return localPythonBenchmarkRun{}, fmt.Errorf("run firecracker snapshot: %w", err)
	}
	return localPythonBenchmarkRun{Restore: restoreElapsed, Exec: execElapsed, Metrics: metrics}, nil
}

func localPythonSnapshotStdin(cfg LocalPythonConfig, execSource string, stdin io.Reader) io.Reader {
	if execSource == "" {
		return stdin
	}
	return strings.NewReader(localPythonExecInput(execSource, localPythonExecFilename(cfg)))
}

func localPythonExecSource(cfg LocalPythonConfig) (string, error) {
	if cfg.ExecFilePath == "" {
		return cfg.InlineCommand, nil
	}
	script, err := os.ReadFile(cfg.ExecFilePath)
	if err != nil {
		return "", fmt.Errorf("read exec file %q: %w", cfg.ExecFilePath, err)
	}
	return string(script), nil
}

func localPythonExecFilename(cfg LocalPythonConfig) string {
	if cfg.ExecFilePath != "" {
		if rel, err := filepath.Rel(cfg.WorkspaceDir, cfg.ExecFilePath); err == nil && !strings.HasPrefix(rel, "..") && rel != "." {
			return filepath.ToSlash(filepath.Join("/workspace", rel))
		}
		return filepath.ToSlash(filepath.Join("/workspace", filepath.Base(cfg.ExecFilePath)))
	}
	return "<exec>"
}

func localPythonExecInput(source, filename string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(source))
	return "import base64, os, subprocess\n" +
		"os.environ['HOME'] = '/workspace'\n" +
		"os.environ['TMPDIR'] = '/workspace'\n" +
		"os.chdir('/workspace')\n" +
		"__nats_code = base64.b64decode(" + strconv.Quote(encoded) + ").decode('utf-8')\n" +
		"try:\n" +
		"    os.setgroups([])\n" +
		"    os.setgid(65534)\n" +
		"    os.setuid(65534)\n" +
		"    exec(compile(__nats_code, " + strconv.Quote(filename) + ", 'exec'), {'__name__': '__main__', '__file__': " + strconv.Quote(filename) + "})\n" +
		"finally:\n" +
		"    subprocess.run(['/usr/bin/sync'], check=False)\n" +
		"\n" +
		"raise SystemExit\n"
}

func localPythonPrepareSnapshotInput(cfg LocalPythonConfig) string {
	var builder strings.Builder
	builder.WriteString("import os, subprocess\n")
	builder.WriteString("os.makedirs('/workspace', exist_ok=True)\n")
	builder.WriteString("subprocess.run(['/usr/bin/mount', '-t', 'ext4', '/dev/vdb', '/workspace'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)\n")
	builder.WriteString("subprocess.run(['/usr/bin/chown', '-R', '65534:65534', '/workspace'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)\n")
	builder.WriteString("subprocess.run(['/usr/bin/chmod', '-R', 'u+rwX', '/workspace'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)\n")
	if cfg.SwapMiB > 0 {
		builder.WriteString("subprocess.run(['/usr/sbin/mkswap', '/dev/vdc'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)\n")
		builder.WriteString("subprocess.run(['/usr/sbin/swapon', '/dev/vdc'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)\n")
	}
	builder.WriteString("subprocess.run(['/usr/bin/mount', '-o', 'remount,ro', '/'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)\n")
	builder.WriteString("print('__NATS_SERVICE_TESTS_SNAPSHOT_READY__')\n")
	return builder.String()
}

func localPythonBenchmarkStats(runs []localPythonBenchmarkRun) localPythonBenchmarkResult {
	restoreTimes := make([]time.Duration, 0, len(runs))
	execTimes := make([]time.Duration, 0, len(runs))
	var maxRSSBytes int64
	var maxCPU time.Duration
	var maxCPUPercent float64
	for _, run := range runs {
		restoreTimes = append(restoreTimes, run.Restore)
		execTimes = append(execTimes, run.Exec)
		if run.Metrics.MaxRSSBytes > maxRSSBytes {
			maxRSSBytes = run.Metrics.MaxRSSBytes
		}
		if run.Metrics.CPU > maxCPU {
			maxCPU = run.Metrics.CPU
		}
		if run.Metrics.MaxCPUPercent > maxCPUPercent {
			maxCPUPercent = run.Metrics.MaxCPUPercent
		}
	}
	restoreAvg, restoreMin, restoreP50, restoreP90, restoreMax := durationStats(restoreTimes)
	execAvg, execMin, execP50, execP90, execMax := durationStats(execTimes)
	result := localPythonBenchmarkResult{
		Runs:          len(runs),
		RestoreAvg:    restoreAvg,
		RestoreMin:    restoreMin,
		RestoreP50:    restoreP50,
		RestoreP90:    restoreP90,
		RestoreMax:    restoreMax,
		ExecAvg:       execAvg,
		ExecMin:       execMin,
		ExecP50:       execP50,
		ExecP90:       execP90,
		ExecMax:       execMax,
		MaxRSSBytes:   maxRSSBytes,
		MaxCPU:        maxCPU,
		MaxCPUPercent: maxCPUPercent,
		RestoreTimes:  restoreTimes,
		ExecTimes:     execTimes,
	}
	return result
}

func durationStats(values []time.Duration) (avg, min, p50, p90, max time.Duration) {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) == 0 {
		return 0, 0, 0, 0, 0
	}
	var sum time.Duration
	for _, value := range values {
		sum += value
	}
	return sum / time.Duration(len(values)),
		values[0],
		values[(len(values)-1)/2],
		values[int(float64(len(values))*0.9+0.999)-1],
		values[len(values)-1]
}

func printLocalPythonBenchmarkResult(stdout io.Writer, result localPythonBenchmarkResult, guestRAMMiB, guestSwapMiB int64) {
	fmt.Fprintf(
		stdout,
		"summary runs=%d restore_avg_ms=%d restore_min_ms=%d restore_p50_ms=%d restore_p90_ms=%d restore_max_ms=%d exec_avg_ms=%d exec_min_ms=%d exec_p50_ms=%d exec_p90_ms=%d exec_max_ms=%d guest_ram_mib=%d guest_swap_mib=%d max_rss_mib=%.1f max_cpu_pct=%.0f max_cpu_ms=%d\n",
		result.Runs,
		result.RestoreAvg.Milliseconds(),
		result.RestoreMin.Milliseconds(),
		result.RestoreP50.Milliseconds(),
		result.RestoreP90.Milliseconds(),
		result.RestoreMax.Milliseconds(),
		result.ExecAvg.Milliseconds(),
		result.ExecMin.Milliseconds(),
		result.ExecP50.Milliseconds(),
		result.ExecP90.Milliseconds(),
		result.ExecMax.Milliseconds(),
		guestRAMMiB,
		guestSwapMiB,
		bytesToMiB(result.MaxRSSBytes),
		result.MaxCPUPercent,
		result.MaxCPU.Milliseconds(),
	)
}

func printLocalPythonBenchmarkProgress(stdout io.Writer, completed, total int) {
	fmt.Fprintf(stdout, "progress completed=%d total=%d\n", completed, total)
}

type processMetricsSampler struct {
	cancel context.CancelFunc
	done   chan processMetrics
}

func startProcessMetricsSampler(ctx context.Context, pid int) *processMetricsSampler {
	sampleCtx, cancel := context.WithCancel(ctx)
	done := make(chan processMetrics, 1)
	sampler := &processMetricsSampler{cancel: cancel, done: done}
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		var max processMetrics
		for {
			if metrics, err := readProcessMetrics(pid); err == nil {
				if metrics.MaxRSSBytes > max.MaxRSSBytes {
					max.MaxRSSBytes = metrics.MaxRSSBytes
				}
				if metrics.CPU > max.CPU {
					max.CPU = metrics.CPU
				}
			}
			select {
			case <-sampleCtx.Done():
				done <- max
				return
			case <-ticker.C:
			}
		}
	}()
	return sampler
}

func (s *processMetricsSampler) Stop() processMetrics {
	s.cancel()
	return <-s.done
}

func readProcessMetrics(pid int) (processMetrics, error) {
	statBytes, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return processMetrics{}, err
	}
	return parseProcStatMetrics(string(statBytes), int64(os.Getpagesize()), 100)
}

func parseProcStatMetrics(stat string, pageSizeBytes, clockTicksPerSecond int64) (processMetrics, error) {
	closeParen := strings.LastIndex(stat, ")")
	if closeParen < 0 || closeParen+2 >= len(stat) {
		return processMetrics{}, fmt.Errorf("invalid proc stat")
	}
	fields := strings.Fields(stat[closeParen+2:])
	if len(fields) < 22 {
		return processMetrics{}, fmt.Errorf("invalid proc stat field count")
	}
	utimeTicks, err := strconv.ParseInt(fields[11], 10, 64)
	if err != nil {
		return processMetrics{}, fmt.Errorf("parse utime: %w", err)
	}
	stimeTicks, err := strconv.ParseInt(fields[12], 10, 64)
	if err != nil {
		return processMetrics{}, fmt.Errorf("parse stime: %w", err)
	}
	rssPages, err := strconv.ParseInt(fields[21], 10, 64)
	if err != nil {
		return processMetrics{}, fmt.Errorf("parse rss: %w", err)
	}
	cpuTicks := utimeTicks + stimeTicks
	cpu := time.Duration(cpuTicks) * time.Second / time.Duration(clockTicksPerSecond)
	return processMetrics{
		MaxRSSBytes: rssPages * pageSizeBytes,
		CPU:         cpu,
	}, nil
}

func bytesToMiB(value int64) float64 {
	return float64(value) / 1024 / 1024
}

func localPythonSnapshotPaths(snapshotDir string) localPythonSnapshotFiles {
	return localPythonSnapshotFiles{
		StatePath:     filepath.Join(snapshotDir, "snapshot.bin"),
		MemoryPath:    filepath.Join(snapshotDir, "memory.bin"),
		WorkspacePath: filepath.Join(snapshotDir, "workspace.ext4"),
		SwapPath:      filepath.Join(snapshotDir, "swap.raw"),
		VersionPath:   filepath.Join(snapshotDir, "version"),
	}
}

func localPythonSnapshotPathsForConfig(cfg LocalPythonConfig) localPythonSnapshotFiles {
	paths := localPythonSnapshotPaths(cfg.SnapshotDir)
	if cfg.WorkspaceImagePath != "" {
		paths.WorkspacePath = cfg.WorkspaceImagePath
	}
	return paths
}

func localPythonSnapshotComplete(paths localPythonSnapshotFiles, cfg LocalPythonConfig) bool {
	return localPythonSnapshotCheck(paths, cfg).OK
}

func localPythonSnapshotCheck(paths localPythonSnapshotFiles, cfg LocalPythonConfig) localPythonSnapshotCheckResult {
	if _, err := os.Stat(paths.StatePath); err != nil {
		return localPythonSnapshotCheckResult{Reason: fmt.Sprintf("missing snapshot state file %q", paths.StatePath)}
	}
	if _, err := os.Stat(paths.MemoryPath); err != nil {
		return localPythonSnapshotCheckResult{Reason: fmt.Sprintf("missing snapshot memory file %q", paths.MemoryPath)}
	}
	if cfg.SwapMiB > 0 {
		info, err := os.Stat(paths.SwapPath)
		if err != nil {
			return localPythonSnapshotCheckResult{Reason: fmt.Sprintf("missing snapshot swap file %q", paths.SwapPath)}
		}
		wantSize := cfg.SwapMiB * 1024 * 1024
		if info.Size() != wantSize {
			return localPythonSnapshotCheckResult{Reason: fmt.Sprintf("snapshot swap file size mismatch: got %d, want %d", info.Size(), wantSize)}
		}
	}
	version, err := os.ReadFile(paths.VersionPath)
	if err != nil {
		return localPythonSnapshotCheckResult{Reason: fmt.Sprintf("missing snapshot version file %q", paths.VersionPath)}
	}
	got := strings.TrimSpace(string(version))
	want := localPythonSnapshotVersion(cfg)
	if got != want {
		return localPythonSnapshotCheckResult{Reason: fmt.Sprintf("snapshot version mismatch: got %q, want %q", got, want)}
	}
	return localPythonSnapshotCheckResult{OK: true}
}

func localPythonSnapshotVersion(cfg LocalPythonConfig) string {
	return fmt.Sprintf("workspace-v6 memory_mib=%d vcpus=%d swap_mib=%d workspace_mib=%d drive_fd=1 system_ro=1 user_uid=65534", cfg.MemoryMiB, cfg.VCPUs, cfg.SwapMiB, cfg.WorkspaceMiB)
}

func waitForUnixSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if info, err := os.Stat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wait for firecracker socket %q: timeout", path)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func waitForBuffer(ctx context.Context, buf *bytes.Buffer, needle string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if strings.Contains(buf.String(), needle) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %q; output: %s", needle, buf.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func firecrackerAPIRequest(ctx context.Context, apiSock, method, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal firecracker API request: %w", err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", apiSock)
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://localhost"+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create firecracker API request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send firecracker API request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("firecracker API %s %s returned %s: %s", method, path, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func localPythonFirecrackerBinary(cfg LocalPythonConfig) string {
	if cfg.FirecrackerPath != "" {
		if _, err := os.Stat(cfg.FirecrackerPath); err == nil {
			return cfg.FirecrackerPath
		}
	}
	return "firecracker"
}

func localPythonFirecrackerArgs(cfg LocalPythonConfig, apiSock, configPath, logPath string) []string {
	args := []string{"--api-sock", apiSock, "--config-file", configPath}
	if cfg.HideFirecrackerLog {
		args = append(args, "--log-path", logPath)
	}
	return args
}

func localPythonFirecrackerStderr(cfg LocalPythonConfig, stderr io.Writer) io.Writer {
	if cfg.HideFirecrackerLog {
		return io.Discard
	}
	return stderr
}

func checkKVMDevice(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w; Firecracker needs KVM access from the host/container runtime (for Docker, start the container with --device=/dev/kvm and a seccomp profile that permits KVM ioctls, or use --privileged)", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

func writeLocalPythonConfig(configPath string, cfg LocalPythonConfig, paths localPythonSnapshotFiles, startUnixNano int64) error {
	configFile, err := os.Create(configPath)
	if err != nil {
		return fmt.Errorf("create firecracker config: %w", err)
	}
	if err := json.NewEncoder(configFile).Encode(localPythonVMConfig(cfg, paths, startUnixNano)); err != nil {
		_ = configFile.Close()
		return fmt.Errorf("write firecracker config: %w", err)
	}
	if err := configFile.Close(); err != nil {
		return fmt.Errorf("close firecracker config: %w", err)
	}
	return nil
}

func localPythonVMConfig(cfg LocalPythonConfig, paths localPythonSnapshotFiles, startUnixNano int64) firecrackerVMConfig {
	drives := []firecrackerDrive{
		{
			DriveID:      "rootfs",
			PathOnHost:   cfg.RootfsPath,
			IsRootDevice: true,
			IsReadOnly:   false,
		},
		{
			DriveID:      "workspace",
			PathOnHost:   localPythonWorkspaceDrivePath,
			IsRootDevice: false,
			IsReadOnly:   false,
		},
	}
	if cfg.SwapMiB > 0 {
		drives = append(drives, firecrackerDrive{
			DriveID:      "swap",
			PathOnHost:   localPythonSwapDrivePath,
			IsRootDevice: false,
			IsReadOnly:   false,
		})
	}

	return firecrackerVMConfig{
		BootSource: firecrackerBootSource{
			KernelImagePath: cfg.KernelPath,
			BootArgs:        localPythonBootArgs(cfg.InlineCommand, startUnixNano, cfg.HideFirecrackerLog),
		},
		Drives: drives,
		MachineConfig: firecrackerMachineConfig{
			VcpuCount:  cfg.VCPUs,
			MemSizeMiB: cfg.MemoryMiB,
		},
	}
}

func localPythonBootArgs(inlineCommand string, startUnixNano int64, hideLog bool) string {
	args := []string{
		"console=ttyS0",
		"reboot=k",
		"panic=1",
		"pci=off",
		"root=/dev/vda",
		"rw",
		"init=/usr/local/bin/fc-python-init",
		fmt.Sprintf("fc_start_ns=%d", startUnixNano),
	}
	if hideLog {
		args = append(args, "quiet", "loglevel=0")
	}
	if inlineCommand != "" {
		args = append(args, "fc_py_b64="+base64.StdEncoding.EncodeToString([]byte(inlineCommand)))
	}
	return strings.Join(args, " ")
}
