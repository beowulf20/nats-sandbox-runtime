package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"nats-sandbox-runtime/internal/observability"
	"nats-sandbox-runtime/internal/transfer"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	runtimePythonServiceName        = "python-runtime"
	runtimePythonServiceVersion     = "0.0.1"
	runtimePythonServiceDescription = "Runs Python code in a Firecracker microVM"
	runtimePythonEndpointSubject    = "python.run"

	runtimePythonControlSettingsGetSubject    = "python.control.settings.get"
	runtimePythonControlSettingsSetSubject    = "python.control.settings.set"
	runtimePythonControlSettingsDeleteSubject = "python.control.settings.delete"
	runtimePythonControlSettingsListSubject   = "python.control.settings.list"
	runtimePythonControlWorkersSetSubject     = "python.control.workers.set"
	runtimePythonControlWorkersListSubject    = "python.control.workers.list"

	runtimePythonReconnectDelay = time.Second
)

type PythonRunRequest struct {
	ArtifactExcludePrefixes []string                 `json:"artifact_exclude_prefixes,omitempty"`
	RunID                   string                   `json:"run_id,omitempty"`
	ThreadID                string                   `json:"thread_id,omitempty"`
	Code                    string                   `json:"code,omitempty"`
	CodeObject              string                   `json:"code_object,omitempty"`
	Inputs                  []PythonRunObjectMapping `json:"inputs,omitempty"`
	MemoryMiB               int64                    `json:"memory_mib,omitempty"`
	SwapMiB                 int64                    `json:"swap_mib,omitempty"`
	WorkspaceMiB            int64                    `json:"workspace_mib,omitempty"`
	ExecTimeout             string                   `json:"exec_timeout,omitempty"`
}

type PythonRunObjectMapping struct {
	Object string `json:"object"`
	Path   string `json:"path"`
}

type PythonRunResponse struct {
	RunID           string              `json:"run_id"`
	Status          string              `json:"status"`
	RestoreExecMS   int64               `json:"restore_exec_ms"`
	GuestRAMMiB     int64               `json:"guest_ram_mib"`
	GuestSwapMiB    int64               `json:"guest_swap_mib"`
	WorkspaceMiB    int64               `json:"workspace_mib"`
	ArtifactBucket  string              `json:"artifact_bucket"`
	Artifacts       []PythonRunArtifact `json:"artifacts"`
	WorkerID        string              `json:"worker_id,omitempty"`
	StdoutHeader    string              `json:"stdout_header"`
	StderrHeader    string              `json:"stderr_header"`
	StdoutTruncated bool                `json:"stdout_truncated"`
	StderrTruncated bool                `json:"stderr_truncated"`
}

type PythonRunArtifact struct {
	Path   string `json:"path"`
	Object string `json:"object"`
	Size   uint64 `json:"size"`
	Digest string `json:"digest,omitempty"`
}

type runtimePythonService struct {
	mu           sync.Mutex
	jobs         sync.WaitGroup
	stopping     bool
	jobContext   context.Context
	cancelJobs   context.CancelFunc
	stats        runtimeExecutionStats
	cfg          RuntimePythonConfig
	store        jetstream.ObjectStore
	controlPlane *RuntimeControlPlane
	workerPool   *RuntimeWorkerPool
	workspaces   *RuntimeWorkspaceManager
	execute      func(context.Context, LocalPythonConfig) (LocalPythonExecResult, error)
}

func RunRuntimePython(ctx context.Context, cfg RuntimePythonConfig, out io.Writer) error {
	if err := validateRuntimePythonConfig(cfg); err != nil {
		return err
	}
	controlPlane := NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython)
	return runRuntimePythonSupervisor(ctx, cfg, out, controlPlane, startRuntimePythonService, runtimePythonReconnectDelay)
}

type runtimePythonRegistration struct {
	conn      *nats.Conn
	service   micro.Service
	runtime   *runtimePythonService
	cancel    context.CancelFunc
	closed    <-chan struct{}
	workers   metric.Registration
	closeOnce sync.Once
}

func (r *runtimePythonRegistration) Close() {
	r.closeOnce.Do(func() {
		closeDeadline := time.Now().Add(35 * time.Second)
		if r.runtime != nil {
			r.runtime.drain(30*time.Second, 5*time.Second)
		}
		if r.workers != nil {
			_ = r.workers.Unregister()
		}
		if r.cancel != nil {
			r.cancel()
		}
		if r.service != nil {
			_ = r.service.Stop()
		}
		if r.conn != nil {
			if remaining := min(time.Second, time.Until(closeDeadline)); remaining > 0 {
				_ = r.conn.FlushTimeout(remaining)
			}
			r.conn.Close()
		}
	})
}

type runtimePythonStartFunc func(context.Context, RuntimePythonConfig, *RuntimeControlPlane) (*runtimePythonRegistration, error)

func runRuntimePythonSupervisor(ctx context.Context, cfg RuntimePythonConfig, out io.Writer, controlPlane *RuntimeControlPlane, start runtimePythonStartFunc, retryDelay time.Duration) error {
	if retryDelay <= 0 {
		retryDelay = runtimePythonReconnectDelay
	}
	for {
		registration, err := start(ctx, cfg, controlPlane)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			runtimeTelemetry(cfg).Logger.WarnContext(ctx, "runtime registration failed", "event", "runtime.registration.error", "error_class", "registration_failure", "retry_ms", retryDelay.Milliseconds())
			if !sleepRuntimePythonRetry(ctx, retryDelay) {
				return nil
			}
			continue
		}

		fmt.Fprintf(out, "ready: service=%s endpoint=%s url=%s bucket=%s workers=%d\n", runtimePythonServiceName, runtimePythonEndpointSubject, redactedNATSURL(cfg.URL), cfg.Bucket, cfg.MaxParallel)
		runtimeTelemetry(cfg).Logger.InfoContext(ctx, "runtime ready", "event", "runtime.ready", "transport", "nats", "url", redactedNATSURL(cfg.URL), "workers", cfg.MaxParallel)

		select {
		case <-ctx.Done():
			registration.Close()
			return nil
		case <-registration.closed:
			registration.Close()
			if registration.runtime != nil {
				registration.runtime.jobs.Wait()
			}
			runtimeTelemetry(cfg).Logger.WarnContext(ctx, "runtime registration closed", "event", "runtime.registration.closed", "retry_ms", retryDelay.Milliseconds())
			if !sleepRuntimePythonRetry(ctx, retryDelay) {
				return nil
			}
		}
	}
}

func sleepRuntimePythonRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func startRuntimePythonService(ctx context.Context, cfg RuntimePythonConfig, controlPlane *RuntimeControlPlane) (*runtimePythonRegistration, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("url must not be empty")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket must not be empty")
	}
	if cfg.MaxParallel < 1 {
		return nil, fmt.Errorf("workers must be at least 1")
	}
	if err := validateLocalPythonConfig(cfg.LocalPython); err != nil {
		return nil, err
	}
	// The supervisor owns cancellation: accepted work survives a normal signal
	// until the drain deadline, while the connection remains available for replies.
	registrationCtx, cancelRegistration := context.WithCancel(context.WithoutCancel(ctx))
	closedCh := make(chan struct{})
	var closeOnce sync.Once
	telemetry := runtimeTelemetry(cfg)
	opts := append(natsConnectOptions("python-runtime-service", cfg.Token), nats.ClosedHandler(func(_ *nats.Conn) {
		closeOnce.Do(func() {
			close(closedCh)
		})
	}), nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		if err == nil && nc.IsClosed() {
			telemetry.Logger.Info("NATS connection closed", "event", "nats.closed")
			return
		}
		telemetry.Logger.Warn("NATS disconnected", "event", "nats.disconnected", "error_class", "connection_lost")
	}), nats.ReconnectHandler(func(nc *nats.Conn) {
		telemetry.Reconnects.Add(context.Background(), 1)
		telemetry.Logger.Info("NATS reconnected", "event", "nats.reconnected", "url", nc.ConnectedUrlRedacted())
	}))
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		cancelRegistration()
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		cancelRegistration()
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	store, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      cfg.Bucket,
		Description: "Python runtime workspace artifacts",
	})
	if err != nil {
		cancelRegistration()
		nc.Close()
		return nil, fmt.Errorf("create object store %q: %w", cfg.Bucket, err)
	}
	if controlPlane == nil {
		controlPlane = NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython)
	}
	workerPool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		cancelRegistration()
		nc.Close()
		return nil, err
	}
	runtime := &runtimePythonService{
		jobContext:   registrationCtx,
		cancelJobs:   cancelRegistration,
		cfg:          cfg,
		store:        store,
		controlPlane: controlPlane,
		workerPool:   workerPool,
		workspaces:   NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir),
	}
	srv, err := micro.AddService(nc, micro.Config{
		Name:         runtimePythonServiceName,
		Version:      runtimePythonServiceVersion,
		Description:  runtimePythonServiceDescription,
		StatsHandler: runtime.executionStats,
	})
	if err != nil {
		cancelRegistration()
		nc.Close()
		return nil, fmt.Errorf("add runtime service: %w", err)
	}
	if err := srv.AddEndpoint("run", micro.HandlerFunc(runtime.dispatchRun), micro.WithEndpointSubject(runtimePythonEndpointSubject)); err != nil {
		cancelRegistration()
		_ = srv.Stop()
		nc.Close()
		return nil, fmt.Errorf("add runtime endpoint: %w", err)
	}
	if err := registerRuntimePythonControlPlaneEndpoints(srv, runtime); err != nil {
		cancelRegistration()
		_ = srv.Stop()
		nc.Close()
		return nil, err
	}
	workers, err := observeWorkers(telemetry, workerPool)
	if err != nil {
		cancelRegistration()
		_ = srv.Stop()
		nc.Close()
		return nil, err
	}
	startRuntimeObjectCleanup(registrationCtx, runtime.store, cfg.ArtifactTTL, cfg.CleanupInterval, telemetry)
	return &runtimePythonRegistration{conn: nc, service: srv, runtime: runtime, cancel: cancelRegistration, closed: closedCh, workers: workers}, nil
}

func registerRuntimePythonControlPlaneEndpoints(srv micro.Service, runtime *runtimePythonService) error {
	endpoints := []struct {
		name    string
		subject string
		handler func(context.Context, micro.Request)
	}{
		{name: "control-settings-get", subject: runtimePythonControlSettingsGetSubject, handler: runtime.handleControlSettingsGet},
		{name: "control-settings-set", subject: runtimePythonControlSettingsSetSubject, handler: runtime.handleControlSettingsSet},
		{name: "control-settings-delete", subject: runtimePythonControlSettingsDeleteSubject, handler: runtime.handleControlSettingsDelete},
		{name: "control-settings-list", subject: runtimePythonControlSettingsListSubject, handler: runtime.handleControlSettingsList},
		{name: "control-workers-set", subject: runtimePythonControlWorkersSetSubject, handler: runtime.handleControlWorkersSet},
		{name: "control-workers-list", subject: runtimePythonControlWorkersListSubject, handler: runtime.handleControlWorkersList},
	}
	for _, endpoint := range endpoints {
		if err := srv.AddEndpoint(endpoint.name, observeMicroRequest(runtimeTelemetry(runtime.cfg), endpoint.subject, endpoint.handler), micro.WithEndpointSubject(endpoint.subject)); err != nil {
			return fmt.Errorf("add runtime endpoint %s: %w", endpoint.subject, err)
		}
	}
	return nil
}

func (s *runtimePythonService) handleRun(ctx context.Context, req micro.Request) {
	worker, ok := s.admit()
	if !ok {
		_ = req.Error("503", "python runtime is busy", nil)
		return
	}
	defer s.jobs.Done()
	s.handleAccepted(ctx, req, worker)
}

func (s *runtimePythonService) handleAccepted(ctx context.Context, req micro.Request, worker RuntimeWorker) {
	defer s.workerPool.ReleaseWorker(worker.ID)
	started := time.Now()
	failed := false
	attempted := false
	defer func() {
		if cause := recover(); cause != nil {
			failed = true
			runtimeTelemetry(s.cfg).Logger.ErrorContext(ctx, "runtime panic", "cause", fmt.Sprint(cause), "worker_id", worker.ID)
			if !attempted {
				attempted = true
				_ = req.Error("500", "python runtime execution failed", nil)
			}
		}
		s.mu.Lock()
		s.stats.InFlight--
		s.stats.Completed++
		if failed {
			s.stats.Errors++
		}
		s.stats.ProcessingTimeNS += time.Since(started).Nanoseconds()
		s.mu.Unlock()
	}()
	var runReq PythonRunRequest
	if len(req.Data()) > 0 {
		if err := json.Unmarshal(req.Data(), &runReq); err != nil {
			failed = true
			attempted = true
			_ = req.Error("400", "invalid python run request", nil)
			return
		}
	}
	if _, err := transfer.Prefixes(runReq.ArtifactExcludePrefixes); err != nil {
		failed = true
		attempted = true
		_ = req.Error("400", "invalid artifact exclusion prefixes", nil)
		return
	}
	if runReq.RunID == "" {
		runReq.RunID = nats.NewInbox()[5:]
	}
	resp, stdout, stderr, err := s.run(ctx, worker, runReq)
	if err != nil {
		failed = true
		runtimeTelemetry(s.cfg).Logger.ErrorContext(ctx, "runtime execution failed", "error", err, "run_id", runReq.RunID, "thread_id", runReq.ThreadID, "worker_id", worker.ID)
		attempted = true
		_ = req.Error("500", "python runtime execution failed", nil, micro.WithHeaders(micro.Headers(s.logHeaders(stdout, stderr))))
		return
	}
	attempted = true
	failed = req.RespondJSON(resp, micro.WithHeaders(micro.Headers(s.logHeaders(stdout, stderr)))) != nil
}

func (s *runtimePythonService) admit() (RuntimeWorker, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		s.stats.RejectedBusy++
		return RuntimeWorker{}, false
	}
	workerPool := s.workerPool
	if workerPool == nil {
		var err error
		workerPool, err = NewRuntimeWorkerPool(s.cfg)
		if err != nil {
			return RuntimeWorker{}, false
		}
		s.workerPool = workerPool
	}
	worker, ok := workerPool.AcquireWorker()
	if !ok {
		s.stats.RejectedBusy++
		return RuntimeWorker{}, false
	}
	s.jobs.Add(1)
	s.stats.InFlight++
	return worker, true
}

func (s *runtimePythonService) run(ctx context.Context, worker RuntimeWorker, runReq PythonRunRequest) (_ PythonRunResponse, _ []byte, _ []byte, err error) {
	telemetry := runtimeTelemetry(s.cfg)
	start := time.Now()
	runID := runReq.RunID
	if runID == "" {
		runID = nats.NewInbox()[5:]
	}
	ids := []attribute.KeyValue{attribute.String("run_id", runID), attribute.String("thread_id", runReq.ThreadID), attribute.String("worker_id", worker.ID)}
	parentCtx := ctx
	ctx, prepareSpan := telemetry.Tracer.Start(ctx, "runtime.prepare", trace.WithAttributes(ids...))
	defer func() {
		if prepareSpan != nil {
			endRuntimeSpan(prepareSpan, err, "preparation_failure")
		}
		result := "success"
		level := slog.LevelInfo
		if err != nil {
			result = "error"
			level = slog.LevelError
		}
		attrs := metric.WithAttributes(attribute.String("result", result))
		telemetry.Runs.Add(parentCtx, 1, attrs)
		telemetry.RunDuration.Record(parentCtx, time.Since(start).Seconds(), attrs)
		telemetry.Logger.Log(parentCtx, level, "runtime run completed", "event", "runtime.run.completed", "run_id", runID, "thread_id", runReq.ThreadID, "worker_id", worker.ID, "result", result, "duration_ms", float64(time.Since(start))/float64(time.Millisecond))
	}()
	workspaceLease, err := s.workspaceLeaseForRunContext(ctx, runReq, runID)
	if err != nil {
		return PythonRunResponse{}, nil, nil, err
	}
	defer workspaceLease.Release()
	workDir, err := os.MkdirTemp("", "nats-sandbox-runtime-*")
	if err != nil {
		return PythonRunResponse{}, nil, nil, fmt.Errorf("create runtime work dir: %w", err)
	}
	defer os.RemoveAll(workDir)
	workspaceDir := filepath.Join(workDir, "workspace")
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		return PythonRunResponse{}, nil, nil, fmt.Errorf("create runtime workspace: %w", err)
	}
	workspaceImagePath := workspaceLease.ImagePath
	if workspaceImagePath == "" {
		workspaceImagePath = filepath.Join(workDir, "workspace.ext4")
	}
	if workspaceLease.ImagePath != "" {
		if _, err := os.Stat(workspaceLease.ImagePath); err == nil {
			if err := syncLocalPythonWorkspaceImage(ctx, LocalPythonConfig{WorkspaceDir: workspaceDir}, localPythonSnapshotFiles{WorkspacePath: workspaceLease.ImagePath}); err != nil {
				return PythonRunResponse{}, nil, nil, fmt.Errorf("load workspace %q: %w", workspaceLease.Key, err)
			}
		} else if !os.IsNotExist(err) {
			return PythonRunResponse{}, nil, nil, fmt.Errorf("stat workspace %q: %w", workspaceLease.Key, err)
		}
	}
	cfg, err := s.localPythonConfigForRunContext(ctx, worker, runReq, workDir, workspaceDir, workspaceImagePath, "")
	if err != nil {
		return PythonRunResponse{}, nil, nil, err
	}
	// Reject aliases and overlapping destinations before concurrent writes.
	seen := map[string]bool{}
	for _, input := range runReq.Inputs {
		p, e := cleanWorkspaceRelativePath(input.Path)
		if e != nil {
			return PythonRunResponse{}, nil, nil, e
		}
		for other := range seen {
			if p == other || strings.HasPrefix(p, other+"/") || strings.HasPrefix(other, p+"/") {
				return PythonRunResponse{}, nil, nil, fmt.Errorf("overlapping input path %q", p)
			}
		}
		seen[p] = true
	}
	if err := transfer.Parallel(ctx, len(runReq.Inputs), func(ctx context.Context, i int) error {
		return fetchRuntimeObject(ctx, s.store, workspaceDir, runReq.Inputs[i], cfg.WorkspaceMiB*1024*1024)
	}); err != nil {
		return PythonRunResponse{}, nil, nil, err
	}
	code := runReq.Code
	if runReq.CodeObject != "" {
		err = transfer.Parallel(ctx, 1, func(ctx context.Context, _ int) error {
			var fetchErr error
			code, fetchErr = s.store.GetString(ctx, runReq.CodeObject)
			return fetchErr
		})
		if err != nil {
			return PythonRunResponse{}, nil, nil, fmt.Errorf("fetch code object %q: %w", runReq.CodeObject, err)
		}
	}
	if code == "" {
		return PythonRunResponse{}, nil, nil, fmt.Errorf("request requires code or code_object")
	}
	startMarker := "__NATS_SANDBOX_RUNTIME_STDOUT_START_" + sanitizeRunIDForMarker(runID) + "__"
	endMarker := "__NATS_SANDBOX_RUNTIME_STDOUT_END_" + sanitizeRunIDForMarker(runID) + "__"
	completionMarker := "__NATS_SANDBOX_RUNTIME_COMPLETE_" + sanitizeRunIDForMarker(runID) + "__"
	cfg.InlineCommand = wrapRuntimePythonCode(code, startMarker, endMarker, completionMarker)
	cfg.CompletionMarker = completionMarker
	cfg.KillAfterCompletion = true
	cfg.SkipGuestWorkspaceSync = true
	prepareSpan.End()
	prepareSpan = nil
	executeCtx, executeSpan := telemetry.Tracer.Start(parentCtx, "runtime.execute", trace.WithAttributes(ids...))
	execute := s.execute
	if execute == nil {
		execute = RunLocalPythonExec
	}
	result, err := func() (result LocalPythonExecResult, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("execution panic: %v", p)
			}
			endRuntimeSpan(executeSpan, err, "execution_failure")
		}()
		return execute(executeCtx, cfg)
	}()
	stdout := extractRuntimePythonStdout(result.Stdout, startMarker, endMarker)
	if err != nil {
		return PythonRunResponse{}, stdout, result.Stderr, err
	}
	uploadCtx, uploadSpan := telemetry.Tracer.Start(parentCtx, "runtime.upload", trace.WithAttributes(ids...))
	artifacts, err := uploadRuntimeArtifacts(uploadCtx, s.store, runID, workspaceDir, runReq.ArtifactExcludePrefixes...)
	endRuntimeSpan(uploadSpan, err, "upload_failure")
	if err != nil {
		return PythonRunResponse{}, stdout, result.Stderr, err
	}
	response := PythonRunResponse{
		RunID:          runID,
		Status:         "completed",
		RestoreExecMS:  result.Elapsed.Milliseconds(),
		GuestRAMMiB:    cfg.MemoryMiB,
		GuestSwapMiB:   cfg.SwapMiB,
		WorkspaceMiB:   cfg.WorkspaceMiB,
		ArtifactBucket: s.cfg.Bucket,
		Artifacts:      artifacts,
		WorkerID:       worker.ID,
		StdoutHeader:   s.cfg.StdoutHeader,
		StderrHeader:   s.cfg.StderrHeader,
	}
	_, response.StdoutTruncated = truncateForMetadata(stdout, s.cfg.TruncateLogMiB)
	_, response.StderrTruncated = truncateForMetadata(result.Stderr, s.cfg.TruncateLogMiB)
	return response, stdout, result.Stderr, nil
}

func runtimeWorkspaceKey(runReq PythonRunRequest, runID string) string {
	return strings.TrimSpace(runReq.ThreadID)
}

func (s *runtimePythonService) workspaceLeaseForRun(runReq PythonRunRequest, runID string) (RuntimeWorkspaceLease, error) {
	return s.workspaceLeaseForRunContext(context.Background(), runReq, runID)
}

func (s *runtimePythonService) workspaceLeaseForRunContext(ctx context.Context, runReq PythonRunRequest, runID string) (RuntimeWorkspaceLease, error) {
	key := runtimeWorkspaceKey(runReq, runID)
	if key == "" {
		return RuntimeWorkspaceLease{}, nil
	}
	s.mu.Lock()
	if s.workspaces == nil {
		s.workspaces = NewRuntimeWorkspaceManager(s.cfg.LocalPython.SnapshotDir)
	}
	m := s.workspaces
	s.mu.Unlock()
	return m.BeginContext(ctx, key)
}

func (s *runtimePythonService) localPythonConfigForRun(worker RuntimeWorker, runReq PythonRunRequest, workDir, workspaceDir, workspaceImagePath, code string) (LocalPythonConfig, error) {
	return s.localPythonConfigForRunContext(context.Background(), worker, runReq, workDir, workspaceDir, workspaceImagePath, code)
}

func (s *runtimePythonService) localPythonConfigForRunContext(ctx context.Context, worker RuntimeWorker, runReq PythonRunRequest, workDir, workspaceDir, workspaceImagePath, code string) (LocalPythonConfig, error) {
	cfg := s.cfg.LocalPython
	var err error
	if s.controlPlane != nil {
		cfg, err = s.controlPlane.ApplyToLocalPythonConfig(ctx, cfg)
		if err != nil {
			return LocalPythonConfig{}, err
		}
	}
	cfg.InlineCommand = code
	cfg.ExecFilePath = ""
	cfg.WorkspaceDir = workspaceDir
	cfg.WorkspaceImagePath = workspaceImagePath
	if worker.SnapshotDir != "" {
		cfg.SnapshotDir = worker.SnapshotDir
	} else {
		cfg.SnapshotDir = filepath.Join(workDir, "snapshot")
	}
	cfg.Runs = 1
	cfg.ParallelRuns = 1
	cfg.HideFirecrackerLog = true
	if worker.MemoryMiB != nil {
		cfg.MemoryMiB = *worker.MemoryMiB
	}
	if worker.SwapMiB != nil {
		cfg.SwapMiB = *worker.SwapMiB
	}
	if worker.WorkspaceMiB != nil {
		cfg.WorkspaceMiB = *worker.WorkspaceMiB
	}
	if worker.ExecTimeout != "" {
		timeout, err := time.ParseDuration(worker.ExecTimeout)
		if err != nil {
			return LocalPythonConfig{}, fmt.Errorf("invalid worker exec_timeout %q: %w", worker.ExecTimeout, err)
		}
		cfg.ExecTimeout = timeout
	}
	if runReq.MemoryMiB > 0 {
		cfg.MemoryMiB = runReq.MemoryMiB
	}
	if runReq.SwapMiB > 0 {
		cfg.SwapMiB = runReq.SwapMiB
	}
	if runReq.WorkspaceMiB > 0 {
		cfg.WorkspaceMiB = runReq.WorkspaceMiB
	}
	if runReq.ExecTimeout != "" {
		if timeout, err := time.ParseDuration(runReq.ExecTimeout); err == nil {
			cfg.ExecTimeout = timeout
		} else {
			return LocalPythonConfig{}, fmt.Errorf("invalid exec_timeout %q: %w", runReq.ExecTimeout, err)
		}
	}
	return cfg, nil
}

func fetchRuntimeObject(ctx context.Context, store jetstream.ObjectStore, workspaceDir string, input PythonRunObjectMapping, maxBytes int64) error {
	if input.Object == "" {
		return fmt.Errorf("input object must not be empty")
	}
	rel, err := cleanWorkspaceRelativePath(input.Path)
	if err != nil {
		return err
	}
	stream, err := store.Get(ctx, input.Object)
	if err != nil {
		return fmt.Errorf("fetch input object %q: %w", input.Object, err)
	}
	defer stream.Close()
	info, err := stream.Info()
	if err != nil {
		return fmt.Errorf("read input object metadata: %w", err)
	}
	if maxBytes <= 0 || info == nil || info.Size > uint64(maxBytes) {
		return fmt.Errorf("input object %q exceeds workspace limit of %d bytes", input.Object, maxBytes)
	}
	root, err := os.OpenRoot(workspaceDir)
	if err != nil {
		return fmt.Errorf("open input workspace: %w", err)
	}
	defer root.Close()
	if err := root.MkdirAll(filepath.Dir(rel), 0o755); err != nil {
		return fmt.Errorf("create input object dir: %w", err)
	}
	temp := filepath.Join(filepath.Dir(rel), ".input-"+nats.NewInbox()[5:])
	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create input object file: %w", err)
	}
	defer root.Remove(temp)
	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()
	// Stream to a new file: GetFile does not truncate existing destinations,
	// and an interrupted transfer must not replace a previously valid input.
	n, copyErr := io.Copy(file, io.LimitReader(stream, maxBytes+1))
	closeErr := file.Close()
	file = nil
	if copyErr != nil {
		return fmt.Errorf("fetch input object %q: %w", input.Object, copyErr)
	}
	if n > maxBytes {
		return fmt.Errorf("input object %q exceeds workspace limit of %d bytes", input.Object, maxBytes)
	}
	if closeErr != nil {
		return fmt.Errorf("close input object file: %w", closeErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Rename(temp, rel); err != nil {
		return fmt.Errorf("fetch input object %q: %w", input.Object, err)
	}
	return nil
}

func uploadRuntimeArtifacts(ctx context.Context, store jetstream.ObjectStore, runID, workspaceDir string, exclude ...string) ([]PythonRunArtifact, error) {
	prefixes, err := transfer.Prefixes(exclude)
	if err != nil {
		return nil, err
	}
	var paths []string
	err = filepath.WalkDir(workspaceDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(workspaceDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if transfer.Excluded(rel, prefixes) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if entry.Name() == "lost+found" {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		paths = append(paths, rel)
		return ctx.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("walk runtime workspace: %w", err)
	}
	artifacts := make([]PythonRunArtifact, len(paths))
	err = transfer.Parallel(ctx, len(paths), func(ctx context.Context, i int) error {
		rel := paths[i]
		objectName := "runs/" + runID + "/artifacts/" + rel
		file, err := os.Open(filepath.Join(workspaceDir, filepath.FromSlash(rel)))
		if err != nil {
			return fmt.Errorf("open artifact %q: %w", rel, err)
		}
		defer file.Close()
		info, err := store.Put(ctx, jetstream.ObjectMeta{Name: objectName}, file)
		closeErr := file.Close()
		if err != nil {
			return fmt.Errorf("upload artifact %q: %w", rel, err)
		}
		if closeErr != nil {
			return fmt.Errorf("close artifact %q: %w", rel, closeErr)
		}
		artifacts[i] = PythonRunArtifact{
			Path:   rel,
			Object: info.Name,
			Size:   info.Size,
			Digest: info.Digest,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk runtime workspace: %w", err)
	}
	return artifacts, nil
}

type runtimeObjectStore interface {
	List(ctx context.Context, opts ...jetstream.ListObjectsOpt) ([]*jetstream.ObjectInfo, error)
	Delete(ctx context.Context, name string) error
}

func startRuntimeObjectCleanup(ctx context.Context, store runtimeObjectStore, ttl, interval time.Duration, telemetry ...*observability.Runtime) {
	if store == nil || ttl <= 0 || interval <= 0 {
		return
	}
	go func() {
		runRuntimeObjectCleanupCheck(ctx, store, ttl, telemetry...)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runRuntimeObjectCleanupCheck(ctx, store, ttl, telemetry...)
			}
		}
	}()
}

func runRuntimeObjectCleanupCheck(ctx context.Context, store runtimeObjectStore, ttl time.Duration, telemetry ...*observability.Runtime) {
	t := localRuntimeTelemetry()
	if len(telemetry) > 0 {
		t = telemetry[0]
	}
	deleted, err := cleanupRuntimeObjects(ctx, store, time.Now().Add(-ttl), t)
	if err != nil {
		t.Logger.WarnContext(ctx, "runtime object cleanup failed", "event", "runtime.cleanup.error", "error_class", "cleanup_failure", "deleted", deleted)
		return
	}
	if deleted > 0 {
		t.Logger.InfoContext(ctx, "runtime objects cleaned", "event", "runtime.cleanup.completed", "deleted", deleted)
	}
}

func cleanupRuntimeObjects(ctx context.Context, store runtimeObjectStore, cutoff time.Time, telemetry ...*observability.Runtime) (int, error) {
	objects, err := store.List(ctx)
	if errors.Is(err, jetstream.ErrNoObjectsFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("list runtime objects: %w", err)
	}
	deleted := 0
	for _, object := range objects {
		if object == nil || !isRuntimeOwnedObjectName(object.Name) || object.ModTime.After(cutoff) {
			continue
		}
		err := store.Delete(ctx, object.Name)
		if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
			return deleted, fmt.Errorf("delete runtime object %q: %w", object.Name, err)
		}
		if err == nil && len(telemetry) > 0 {
			telemetry[0].CleanupDeleted.Add(ctx, 1)
		}
		deleted++
	}
	return deleted, nil
}

func isRuntimeOwnedObjectName(name string) bool {
	return strings.HasPrefix(name, "sdk-inputs/") ||
		(strings.HasPrefix(name, "runs/") && strings.Contains(name, "/artifacts/"))
}

func cleanWorkspaceRelativePath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("input path must not be empty")
	}
	cleaned := filepath.Clean(filepath.FromSlash(path))
	if filepath.IsAbs(cleaned) || cleaned == "." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) || cleaned == ".." {
		return "", fmt.Errorf("invalid workspace path %q", path)
	}
	return cleaned, nil
}

func (s *runtimePythonService) logHeaders(stdout, stderr []byte) nats.Header {
	stdout, stdoutTruncated := truncateForMetadata(stdout, s.cfg.TruncateLogMiB)
	stderr, stderrTruncated := truncateForMetadata(stderr, s.cfg.TruncateLogMiB)
	headers := nats.Header{}
	headers.Set(s.cfg.StdoutHeader, base64.StdEncoding.EncodeToString(stdout))
	headers.Set(s.cfg.StderrHeader, base64.StdEncoding.EncodeToString(stderr))
	headers.Set(s.cfg.StdoutHeader+"-Truncated", fmt.Sprintf("%t", stdoutTruncated))
	headers.Set(s.cfg.StderrHeader+"-Truncated", fmt.Sprintf("%t", stderrTruncated))
	headers.Set("Content-Type", "application/json")
	return headers
}

func truncateForMetadata(data []byte, limitMiB int64) ([]byte, bool) {
	if limitMiB <= 0 {
		return data, false
	}
	limit := limitMiB * 1024 * 1024
	if int64(len(data)) <= limit {
		return data, false
	}
	return data[:limit], true
}

func wrapRuntimePythonCode(code, startMarker, endMarker, completionMarker string) string {
	return "import subprocess as __nats_runtime_subprocess\n" +
		"print(" + strconv.Quote(startMarker) + ")\n" +
		code + "\n" +
		"print(" + strconv.Quote(endMarker) + ")\n" +
		"__nats_runtime_subprocess.run(['/usr/bin/sync'], check=True)\n" +
		"print(" + strconv.Quote(completionMarker) + ")\n"
}

func extractRuntimePythonStdout(output []byte, startMarker, endMarker string) []byte {
	text := string(output)
	start := strings.Index(text, startMarker)
	if start < 0 {
		return output
	}
	text = text[start+len(startMarker):]
	text = strings.TrimPrefix(text, "\r\n")
	text = strings.TrimPrefix(text, "\n")
	if end := strings.Index(text, endMarker); end >= 0 {
		text = text[:end]
	}
	text = strings.TrimSuffix(text, "\r\n")
	text = strings.TrimSuffix(text, "\n")
	return []byte(text)
}

func sanitizeRunIDForMarker(runID string) string {
	replacer := strings.NewReplacer(".", "_", "-", "_", "/", "_", ":", "_")
	return replacer.Replace(runID)
}
