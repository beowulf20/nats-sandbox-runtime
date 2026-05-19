package app

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

const LocalNATSURL = "nats://localhost:4222"

const (
	defaultLocalKernelPath  = "firecracker-assets/vmlinux.bin"
	defaultLocalRootfsPath  = "firecracker-assets/rootfs.ext4"
	defaultFirecrackerPath  = "bin/firecracker"
	defaultSnapshotDir      = "firecracker-assets/python-snapshot"
	defaultWorkspaceDir     = "tmp/workspace"
	defaultLocalMemoryMiB   = 128
	defaultLocalVCPUs       = 1
	defaultMaxVCPUs         = 1
	defaultExecTimeout      = 5 * time.Second
	defaultSwapMiB          = 256
	defaultParallelRuns     = 1
	defaultWorkspaceMiB     = 16
	defaultRuntimeBucket    = "python-runtime-workspaces"
	defaultMaxParallelRuns  = 1
	defaultRuntimeAPIListen = "127.0.0.1:8080"
	defaultRuntimeAPIWebDir = "web/build"
)

type Config struct {
	Instances int
	URL       string
}

type LocalPythonConfig struct {
	KernelPath             string
	RootfsPath             string
	FirecrackerPath        string
	SnapshotDir            string
	WorkspaceDir           string
	WorkspaceImagePath     string
	CompletionMarker       string
	KillAfterCompletion    bool
	SkipGuestWorkspaceSync bool
	InlineCommand          string
	ExecFilePath           string
	HideFirecrackerLog     bool
	MemoryMiB              int64
	SwapMiB                int64
	WorkspaceMiB           int64
	VCPUs                  int64
	MaxVCPUs               int64
	Runs                   int
	ParallelRuns           int
	ExecTimeout            time.Duration
}

type RuntimePythonConfig struct {
	URL            string
	Bucket         string
	MaxParallel    int
	LocalPython    LocalPythonConfig
	StdoutHeader   string
	StderrHeader   string
	TruncateLogMiB int64
}

type RuntimeAPIConfig struct {
	Listen  string
	WebDir  string
	Runtime RuntimePythonConfig
}

func NewRootCommand(run func(Config) error, runLocalPython func(context.Context, LocalPythonConfig) error, runRuntimePython ...func(context.Context, RuntimePythonConfig) error) *cobra.Command {
	var runtimeRunner func(context.Context, RuntimePythonConfig) error
	if len(runRuntimePython) > 0 {
		runtimeRunner = runRuntimePython[0]
	}
	return NewRootCommandWithRuntimeAPI(run, runLocalPython, runtimeRunner, nil)
}

func NewRootCommandWithRuntimeAPI(run func(Config) error, runLocalPython func(context.Context, LocalPythonConfig) error, runRuntimePython func(context.Context, RuntimePythonConfig) error, runRuntimeAPI func(context.Context, RuntimeAPIConfig) error, runNATSDeploymentTest ...func(context.Context, NATSDeploymentTestConfig) error) *cobra.Command {
	var natsTestRunner func(context.Context, NATSDeploymentTestConfig) error
	if len(runNATSDeploymentTest) > 0 {
		natsTestRunner = runNATSDeploymentTest[0]
	}
	return NewRootCommandWithRuntimeAPIAndTools(run, runLocalPython, runRuntimePython, runRuntimeAPI, natsTestRunner, nil)
}

func NewRootCommandWithRuntimeAPIAndTools(run func(Config) error, runLocalPython func(context.Context, LocalPythonConfig) error, runRuntimePython func(context.Context, RuntimePythonConfig) error, runRuntimeAPI func(context.Context, RuntimeAPIConfig) error, runNATSDeploymentTest func(context.Context, NATSDeploymentTestConfig) error, runRuntimeREPL func(context.Context, RuntimeREPLConfig) error) *cobra.Command {
	cfg := Config{
		Instances: 1,
		URL:       LocalNATSURL,
	}

	cmd := &cobra.Command{
		Use:           "nats-sandbox-runtime",
		Short:         "Run NATS sandbox runtime services and local helpers",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.Instances < 1 {
				return fmt.Errorf("instances must be at least 1")
			}
			if run == nil {
				return fmt.Errorf("service runner is not configured")
			}
			return run(cfg)
		},
	}

	cmd.Flags().IntVarP(&cfg.Instances, "instances", "i", cfg.Instances, "number of service instances to register")
	cmd.AddCommand(newLocalCommand(runLocalPython))
	cmd.AddCommand(newRuntimeCommand(runRuntimePython, runRuntimeAPI))
	cmd.AddCommand(newTestCommand(runNATSDeploymentTest, runRuntimeREPL))

	return cmd
}

func newLocalCommand(runLocalPython func(context.Context, LocalPythonConfig) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "local",
		Short: "Run local development helpers",
	}
	cmd.AddCommand(newLocalPythonCommand(runLocalPython))
	return cmd
}

func newLocalPythonCommand(runLocalPython func(context.Context, LocalPythonConfig) error) *cobra.Command {
	cfg := defaultLocalPythonConfig()

	cmd := &cobra.Command{
		Use:   "python",
		Short: "Start a Firecracker microVM running Python on the serial console",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateLocalPythonConfig(cfg); err != nil {
				return err
			}
			if cfg.InlineCommand != "" && cfg.ExecFilePath != "" {
				return fmt.Errorf("--exec and --exec-file are mutually exclusive")
			}
			if cfg.Runs > 1 && cfg.InlineCommand == "" && cfg.ExecFilePath == "" {
				return fmt.Errorf("runs requires --exec or --exec-file")
			}
			if runLocalPython == nil {
				return fmt.Errorf("local python runner is not configured")
			}
			return runLocalPython(cmd.Context(), cfg)
		},
	}

	addLocalPythonFlags(cmd, &cfg)
	return cmd
}

func defaultLocalPythonConfig() LocalPythonConfig {
	return LocalPythonConfig{
		KernelPath:      defaultLocalKernelPath,
		RootfsPath:      defaultLocalRootfsPath,
		FirecrackerPath: defaultFirecrackerPath,
		SnapshotDir:     defaultSnapshotDir,
		WorkspaceDir:    defaultWorkspaceDir,
		MemoryMiB:       defaultLocalMemoryMiB,
		SwapMiB:         defaultSwapMiB,
		WorkspaceMiB:    defaultWorkspaceMiB,
		VCPUs:           defaultLocalVCPUs,
		MaxVCPUs:        defaultMaxVCPUs,
		Runs:            1,
		ParallelRuns:    defaultParallelRuns,
		ExecTimeout:     defaultExecTimeout,
	}
}

func validateLocalPythonConfig(cfg LocalPythonConfig) error {
	if cfg.MemoryMiB < 1 {
		return fmt.Errorf("memory-mib must be at least 1")
	}
	if cfg.SwapMiB < 0 {
		return fmt.Errorf("swap-mib must be at least 0")
	}
	if cfg.WorkspaceMiB < 1 {
		return fmt.Errorf("workspace-mib must be at least 1")
	}
	if cfg.VCPUs < 1 {
		return fmt.Errorf("vcpus must be at least 1")
	}
	if cfg.MaxVCPUs < 1 {
		return fmt.Errorf("max-vcpus must be at least 1")
	}
	if cfg.VCPUs > cfg.MaxVCPUs {
		return fmt.Errorf("vcpus must be less than or equal to max-vcpus (%d)", cfg.MaxVCPUs)
	}
	if cfg.Runs < 1 {
		return fmt.Errorf("runs must be at least 1")
	}
	if cfg.ParallelRuns < 1 {
		return fmt.Errorf("parallel-runs must be at least 1")
	}
	if cfg.ExecTimeout <= 0 {
		return fmt.Errorf("exec-timeout must be greater than 0")
	}
	return nil
}

func addLocalPythonFlags(cmd *cobra.Command, cfg *LocalPythonConfig) {
	cmd.Flags().StringVar(&cfg.KernelPath, "kernel", cfg.KernelPath, "Firecracker guest kernel path")
	cmd.Flags().StringVar(&cfg.RootfsPath, "rootfs", cfg.RootfsPath, "Firecracker rootfs path")
	cmd.Flags().StringVar(&cfg.FirecrackerPath, "firecracker", cfg.FirecrackerPath, "Firecracker binary path")
	cmd.Flags().StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "Firecracker Python snapshot cache directory")
	cmd.Flags().StringVar(&cfg.WorkspaceDir, "workspace-dir", cfg.WorkspaceDir, "directory copied into the VM at /workspace")
	cmd.Flags().StringVarP(&cfg.InlineCommand, "exec", "e", cfg.InlineCommand, "inline Python command to run instead of starting a REPL")
	cmd.Flags().StringVar(&cfg.ExecFilePath, "exec-file", cfg.ExecFilePath, "Python script file to run instead of starting a REPL")
	cmd.Flags().BoolVar(&cfg.HideFirecrackerLog, "hide-firecracker-log", cfg.HideFirecrackerLog, "hide Firecracker process logs")
	cmd.Flags().Int64Var(&cfg.MemoryMiB, "memory-mib", cfg.MemoryMiB, "microVM memory size in MiB")
	cmd.Flags().Int64Var(&cfg.SwapMiB, "swap-mib", cfg.SwapMiB, "dedicated guest swap image size in MiB")
	cmd.Flags().Int64Var(&cfg.WorkspaceMiB, "workspace-mib", cfg.WorkspaceMiB, "workspace filesystem size in MiB")
	cmd.Flags().Int64Var(&cfg.VCPUs, "vcpus", cfg.VCPUs, "microVM vCPU count")
	cmd.Flags().Int64Var(&cfg.MaxVCPUs, "max-vcpus", cfg.MaxVCPUs, "maximum allowed microVM vCPU count")
	cmd.Flags().IntVar(&cfg.Runs, "runs", 1, "number of snapshot restore exec runs to benchmark")
	cmd.Flags().IntVar(&cfg.ParallelRuns, "parallel-runs", cfg.ParallelRuns, "maximum number of benchmark runs to execute in parallel")
	cmd.Flags().DurationVar(&cfg.ExecTimeout, "exec-timeout", cfg.ExecTimeout, "maximum time to wait for Python exec completion in benchmark runs")
}

func newRuntimeCommand(runRuntimePython func(context.Context, RuntimePythonConfig) error, runRuntimeAPI func(context.Context, RuntimeAPIConfig) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runtime",
		Short: "Run NATS-backed runtimes",
	}
	cmd.AddCommand(newRuntimePythonCommand(runRuntimePython))
	cmd.AddCommand(newRuntimeAPICommand(runRuntimeAPI))
	return cmd
}

func newRuntimePythonCommand(runRuntimePython func(context.Context, RuntimePythonConfig) error) *cobra.Command {
	runtimeCfg := defaultRuntimePythonConfig()

	cmd := &cobra.Command{
		Use:   "python",
		Short: "Run a NATS service that executes Python in Firecracker",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateRuntimePythonConfig(runtimeCfg); err != nil {
				return err
			}
			if runRuntimePython == nil {
				return fmt.Errorf("runtime python runner is not configured")
			}
			return runRuntimePython(cmd.Context(), runtimeCfg)
		},
	}
	addRuntimePythonFlags(cmd, &runtimeCfg)
	return cmd
}

func newRuntimeAPICommand(runRuntimeAPI func(context.Context, RuntimeAPIConfig) error) *cobra.Command {
	cfg := RuntimeAPIConfig{
		Listen:  defaultRuntimeAPIListen,
		WebDir:  defaultRuntimeAPIWebDir,
		Runtime: defaultRuntimePythonConfig(),
	}

	cmd := &cobra.Command{
		Use:   "api",
		Short: "Run the Python runtime with a local API and web console",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.Listen == "" {
				return fmt.Errorf("listen must not be empty")
			}
			if cfg.WebDir == "" {
				return fmt.Errorf("web-dir must not be empty")
			}
			if err := validateRuntimePythonConfig(cfg.Runtime); err != nil {
				return err
			}
			if runRuntimeAPI == nil {
				return fmt.Errorf("runtime api runner is not configured")
			}
			return runRuntimeAPI(cmd.Context(), cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.Listen, "listen", cfg.Listen, "local HTTP listen address for the runtime API and web console")
	cmd.Flags().StringVar(&cfg.WebDir, "web-dir", cfg.WebDir, "built Horizon frontend directory to serve")
	addRuntimePythonFlags(cmd, &cfg.Runtime)
	return cmd
}

func defaultRuntimePythonConfig() RuntimePythonConfig {
	localCfg := defaultLocalPythonConfig()
	localCfg.HideFirecrackerLog = true
	return RuntimePythonConfig{
		URL:            LocalNATSURL,
		Bucket:         defaultRuntimeBucket,
		MaxParallel:    defaultMaxParallelRuns,
		LocalPython:    localCfg,
		StdoutHeader:   "Nats-Sandbox-Runtime-Python-Stdout-B64",
		StderrHeader:   "Nats-Sandbox-Runtime-Python-Stderr-B64",
		TruncateLogMiB: 1,
	}
}

func validateRuntimePythonConfig(cfg RuntimePythonConfig) error {
	if err := validateLocalPythonConfig(cfg.LocalPython); err != nil {
		return err
	}
	if cfg.MaxParallel < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if cfg.Bucket == "" {
		return fmt.Errorf("bucket must not be empty")
	}
	if cfg.TruncateLogMiB < 0 {
		return fmt.Errorf("truncate-log-mib must be at least 0")
	}
	return nil
}

func addRuntimePythonFlags(cmd *cobra.Command, runtimeCfg *RuntimePythonConfig) {
	cmd.Flags().StringVar(&runtimeCfg.LocalPython.KernelPath, "kernel", runtimeCfg.LocalPython.KernelPath, "Firecracker guest kernel path")
	cmd.Flags().StringVar(&runtimeCfg.LocalPython.RootfsPath, "rootfs", runtimeCfg.LocalPython.RootfsPath, "Firecracker rootfs path")
	cmd.Flags().StringVar(&runtimeCfg.LocalPython.FirecrackerPath, "firecracker", runtimeCfg.LocalPython.FirecrackerPath, "Firecracker binary path")
	cmd.Flags().BoolVar(&runtimeCfg.LocalPython.HideFirecrackerLog, "hide-firecracker-log", runtimeCfg.LocalPython.HideFirecrackerLog, "hide Firecracker process logs")
	cmd.Flags().Int64Var(&runtimeCfg.LocalPython.MemoryMiB, "memory-mib", runtimeCfg.LocalPython.MemoryMiB, "default microVM memory size in MiB")
	cmd.Flags().Int64Var(&runtimeCfg.LocalPython.SwapMiB, "swap-mib", runtimeCfg.LocalPython.SwapMiB, "default dedicated guest swap image size in MiB")
	cmd.Flags().Int64Var(&runtimeCfg.LocalPython.WorkspaceMiB, "workspace-mib", runtimeCfg.LocalPython.WorkspaceMiB, "default workspace filesystem size in MiB")
	cmd.Flags().Int64Var(&runtimeCfg.LocalPython.VCPUs, "vcpus", runtimeCfg.LocalPython.VCPUs, "microVM vCPU count")
	cmd.Flags().Int64Var(&runtimeCfg.LocalPython.MaxVCPUs, "max-vcpus", runtimeCfg.LocalPython.MaxVCPUs, "maximum allowed microVM vCPU count")
	cmd.Flags().DurationVar(&runtimeCfg.LocalPython.ExecTimeout, "exec-timeout", runtimeCfg.LocalPython.ExecTimeout, "default maximum time to wait for Python exec completion")
	cmd.Flags().StringVar(&runtimeCfg.URL, "url", runtimeCfg.URL, "NATS server URL")
	cmd.Flags().StringVar(&runtimeCfg.Bucket, "bucket", runtimeCfg.Bucket, "NATS Object Store bucket for runtime workspaces")
	cmd.Flags().IntVar(&runtimeCfg.MaxParallel, "workers", runtimeCfg.MaxParallel, "initial runtime worker count")
	cmd.Flags().IntVar(&runtimeCfg.MaxParallel, "max-parallel", runtimeCfg.MaxParallel, "deprecated alias for --workers")
	cmd.Flags().StringVar(&runtimeCfg.StdoutHeader, "stdout-header", runtimeCfg.StdoutHeader, "response header used for base64 stdout metadata")
	cmd.Flags().StringVar(&runtimeCfg.StderrHeader, "stderr-header", runtimeCfg.StderrHeader, "response header used for base64 stderr metadata")
	cmd.Flags().Int64Var(&runtimeCfg.TruncateLogMiB, "truncate-log-mib", runtimeCfg.TruncateLogMiB, "maximum stdout/stderr MiB returned in response metadata; 0 disables truncation")
}

func newTestCommand(runNATSDeploymentTest func(context.Context, NATSDeploymentTestConfig) error, runRuntimeREPL func(context.Context, RuntimeREPLConfig) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Run deployment smoke tests",
	}
	cmd.AddCommand(newTestNATSCommand(runNATSDeploymentTest))
	cmd.AddCommand(newTestREPLCommand(runRuntimeREPL))
	return cmd
}

func newTestNATSCommand(runNATSDeploymentTest func(context.Context, NATSDeploymentTestConfig) error) *cobra.Command {
	cfg := defaultNATSDeploymentTestConfig()

	cmd := &cobra.Command{
		Use:   "nats",
		Short: "Connect to NATS and optionally request a deployment subject",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateNATSDeploymentTestConfig(cfg); err != nil {
				return err
			}
			if runNATSDeploymentTest == nil {
				return fmt.Errorf("nats deployment test runner is not configured")
			}
			return runNATSDeploymentTest(cmd.Context(), cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.URL, "url", cfg.URL, "NATS server URL")
	cmd.Flags().StringVar(&cfg.Bucket, "bucket", cfg.Bucket, "NATS Object Store bucket to verify; empty skips bucket verification")
	cmd.Flags().StringVar(&cfg.Subject, "subject", cfg.Subject, "optional NATS subject to request")
	cmd.Flags().StringVar(&cfg.Payload, "payload", cfg.Payload, "request payload sent when --subject is set")
	cmd.Flags().DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "connection and request timeout")
	return cmd
}

func newTestREPLCommand(runRuntimeREPL func(context.Context, RuntimeREPLConfig) error) *cobra.Command {
	cfg := defaultRuntimeREPLConfig()

	cmd := &cobra.Command{
		Use:   "repl",
		Short: "Run a simple line-oriented Python runtime REPL over NATS",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateRuntimeREPLConfig(cfg); err != nil {
				return err
			}
			if runRuntimeREPL == nil {
				return fmt.Errorf("runtime repl runner is not configured")
			}
			return runRuntimeREPL(cmd.Context(), cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.URL, "url", cfg.URL, "NATS server URL")
	cmd.Flags().DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "connection and per-request timeout")
	cmd.Flags().Int64Var(&cfg.MemoryMiB, "memory-mib", cfg.MemoryMiB, "per-request guest memory override in MiB; 0 omits the field")
	cmd.Flags().Int64Var(&cfg.WorkspaceMiB, "workspace-mib", cfg.WorkspaceMiB, "per-request workspace size override in MiB; 0 omits the field")
	cmd.Flags().StringVar(&cfg.ExecTimeout, "exec-timeout", cfg.ExecTimeout, "per-request Python execution timeout; empty omits the field")
	return cmd
}
