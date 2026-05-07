package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
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
)

type PythonRunRequest struct {
	RunID        string                   `json:"run_id,omitempty"`
	Code         string                   `json:"code,omitempty"`
	CodeObject   string                   `json:"code_object,omitempty"`
	Inputs       []PythonRunObjectMapping `json:"inputs,omitempty"`
	MemoryMiB    int64                    `json:"memory_mib,omitempty"`
	SwapMiB      int64                    `json:"swap_mib,omitempty"`
	WorkspaceMiB int64                    `json:"workspace_mib,omitempty"`
	ExecTimeout  string                   `json:"exec_timeout,omitempty"`
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
	cfg          RuntimePythonConfig
	store        jetstream.ObjectStore
	controlPlane *RuntimeControlPlane
	workerPool   *RuntimeWorkerPool
}

func RunRuntimePython(ctx context.Context, cfg RuntimePythonConfig, out io.Writer) error {
	registration, err := startRuntimePythonService(ctx, cfg, NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython))
	if err != nil {
		return err
	}
	defer registration.Close()
	fmt.Fprintf(out, "ready: service=%s endpoint=%s url=%s bucket=%s workers=%d\n", runtimePythonServiceName, runtimePythonEndpointSubject, cfg.URL, cfg.Bucket, cfg.MaxParallel)
	<-ctx.Done()
	return nil
}

type runtimePythonRegistration struct {
	conn    *nats.Conn
	service micro.Service
	runtime *runtimePythonService
}

func (r *runtimePythonRegistration) Close() {
	if r.service != nil {
		_ = r.service.Stop()
	}
	if r.conn != nil {
		r.conn.Close()
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
	nc, err := nats.Connect(cfg.URL, nats.Name("python-runtime-service"))
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	store, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      cfg.Bucket,
		Description: "Python runtime workspace artifacts",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create object store %q: %w", cfg.Bucket, err)
	}
	if controlPlane == nil {
		controlPlane = NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython)
	}
	workerPool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		nc.Close()
		return nil, err
	}
	runtime := &runtimePythonService{
		cfg:          cfg,
		store:        store,
		controlPlane: controlPlane,
		workerPool:   workerPool,
	}
	srv, err := micro.AddService(nc, micro.Config{
		Name:        runtimePythonServiceName,
		Version:     runtimePythonServiceVersion,
		Description: runtimePythonServiceDescription,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("add runtime service: %w", err)
	}
	if err := srv.AddEndpoint("run", micro.HandlerFunc(runtime.handleRun), micro.WithEndpointSubject(runtimePythonEndpointSubject)); err != nil {
		_ = srv.Stop()
		nc.Close()
		return nil, fmt.Errorf("add runtime endpoint: %w", err)
	}
	if err := registerRuntimePythonControlPlaneEndpoints(srv, runtime); err != nil {
		_ = srv.Stop()
		nc.Close()
		return nil, err
	}
	return &runtimePythonRegistration{conn: nc, service: srv, runtime: runtime}, nil
}

func registerRuntimePythonControlPlaneEndpoints(srv micro.Service, runtime *runtimePythonService) error {
	endpoints := []struct {
		name    string
		subject string
		handler micro.HandlerFunc
	}{
		{name: "control-settings-get", subject: runtimePythonControlSettingsGetSubject, handler: runtime.handleControlSettingsGet},
		{name: "control-settings-set", subject: runtimePythonControlSettingsSetSubject, handler: runtime.handleControlSettingsSet},
		{name: "control-settings-delete", subject: runtimePythonControlSettingsDeleteSubject, handler: runtime.handleControlSettingsDelete},
		{name: "control-settings-list", subject: runtimePythonControlSettingsListSubject, handler: runtime.handleControlSettingsList},
		{name: "control-workers-set", subject: runtimePythonControlWorkersSetSubject, handler: runtime.handleControlWorkersSet},
		{name: "control-workers-list", subject: runtimePythonControlWorkersListSubject, handler: runtime.handleControlWorkersList},
	}
	for _, endpoint := range endpoints {
		if err := srv.AddEndpoint(endpoint.name, endpoint.handler, micro.WithEndpointSubject(endpoint.subject)); err != nil {
			return fmt.Errorf("add runtime endpoint %s: %w", endpoint.subject, err)
		}
	}
	return nil
}

func (s *runtimePythonService) handleRun(req micro.Request) {
	workerPool := s.workerPool
	if workerPool == nil {
		var err error
		workerPool, err = NewRuntimeWorkerPool(s.cfg)
		if err != nil {
			_ = req.Error("500", err.Error(), nil)
			return
		}
		s.workerPool = workerPool
	}
	worker, ok := workerPool.AcquireWorker()
	if !ok {
		_ = req.Error("503", "python runtime is busy", nil)
		return
	}
	defer workerPool.ReleaseWorker(worker.ID)
	var runReq PythonRunRequest
	if len(req.Data()) > 0 {
		if err := json.Unmarshal(req.Data(), &runReq); err != nil {
			_ = req.Error("400", "invalid python run request", []byte(err.Error()))
			return
		}
	}
	resp, stdout, stderr, err := s.run(req, worker, runReq)
	if err != nil {
		headers := s.logHeaders(stdout, stderr)
		_ = req.Error("500", err.Error(), nil, micro.WithHeaders(micro.Headers(headers)))
		return
	}
	headers := s.logHeaders(stdout, stderr)
	_ = req.RespondJSON(resp, micro.WithHeaders(micro.Headers(headers)))
}

func (s *runtimePythonService) run(_ micro.Request, worker RuntimeWorker, runReq PythonRunRequest) (PythonRunResponse, []byte, []byte, error) {
	ctx := context.Background()
	runID := runReq.RunID
	if runID == "" {
		runID = nats.NewInbox()[5:]
	}
	workDir, err := os.MkdirTemp("", "nats-python-runtime-*")
	if err != nil {
		return PythonRunResponse{}, nil, nil, fmt.Errorf("create runtime work dir: %w", err)
	}
	defer os.RemoveAll(workDir)
	workspaceDir := filepath.Join(workDir, "workspace")
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		return PythonRunResponse{}, nil, nil, fmt.Errorf("create runtime workspace: %w", err)
	}
	for _, input := range runReq.Inputs {
		if err := fetchRuntimeObject(ctx, s.store, workspaceDir, input); err != nil {
			return PythonRunResponse{}, nil, nil, err
		}
	}
	code := runReq.Code
	if runReq.CodeObject != "" {
		code, err = s.store.GetString(ctx, runReq.CodeObject)
		if err != nil {
			return PythonRunResponse{}, nil, nil, fmt.Errorf("fetch code object %q: %w", runReq.CodeObject, err)
		}
	}
	if code == "" {
		return PythonRunResponse{}, nil, nil, fmt.Errorf("request requires code or code_object")
	}
	startMarker := "__NATS_SERVICE_TESTS_RUNTIME_STDOUT_START_" + sanitizeRunIDForMarker(runID) + "__"
	endMarker := "__NATS_SERVICE_TESTS_RUNTIME_STDOUT_END_" + sanitizeRunIDForMarker(runID) + "__"
	cfg, err := s.localPythonConfigForRun(worker, runReq, workDir, workspaceDir, wrapRuntimePythonCode(code, startMarker, endMarker))
	if err != nil {
		return PythonRunResponse{}, nil, nil, err
	}
	result, err := RunLocalPythonExec(ctx, cfg)
	stdout := extractRuntimePythonStdout(result.Stdout, startMarker, endMarker)
	if err != nil {
		return PythonRunResponse{}, stdout, result.Stderr, err
	}
	artifacts, err := uploadRuntimeArtifacts(ctx, s.store, runID, workspaceDir)
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

func (s *runtimePythonService) localPythonConfigForRun(worker RuntimeWorker, runReq PythonRunRequest, workDir, workspaceDir, code string) (LocalPythonConfig, error) {
	cfg := s.cfg.LocalPython
	var err error
	if s.controlPlane != nil {
		cfg, err = s.controlPlane.ApplyToLocalPythonConfig(context.Background(), cfg)
		if err != nil {
			return LocalPythonConfig{}, err
		}
	}
	cfg.InlineCommand = code
	cfg.ExecFilePath = ""
	cfg.WorkspaceDir = workspaceDir
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

func fetchRuntimeObject(ctx context.Context, store jetstream.ObjectStore, workspaceDir string, input PythonRunObjectMapping) error {
	if input.Object == "" {
		return fmt.Errorf("input object must not be empty")
	}
	rel, err := cleanWorkspaceRelativePath(input.Path)
	if err != nil {
		return err
	}
	target := filepath.Join(workspaceDir, rel)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("create input object dir: %w", err)
	}
	if err := store.GetFile(ctx, input.Object, target); err != nil {
		return fmt.Errorf("fetch input object %q: %w", input.Object, err)
	}
	return nil
}

func uploadRuntimeArtifacts(ctx context.Context, store jetstream.ObjectStore, runID, workspaceDir string) ([]PythonRunArtifact, error) {
	var artifacts []PythonRunArtifact
	err := filepath.WalkDir(workspaceDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
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
		rel, err := filepath.Rel(workspaceDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		objectName := "runs/" + runID + "/artifacts/" + rel
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open artifact %q: %w", rel, err)
		}
		info, err := store.Put(ctx, jetstream.ObjectMeta{Name: objectName}, file)
		closeErr := file.Close()
		if err != nil {
			return fmt.Errorf("upload artifact %q: %w", rel, err)
		}
		if closeErr != nil {
			return fmt.Errorf("close artifact %q: %w", rel, closeErr)
		}
		artifacts = append(artifacts, PythonRunArtifact{
			Path:   rel,
			Object: info.Name,
			Size:   info.Size,
			Digest: info.Digest,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk runtime workspace: %w", err)
	}
	return artifacts, nil
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

func wrapRuntimePythonCode(code, startMarker, endMarker string) string {
	return "print(" + strconv.Quote(startMarker) + ")\n" + code + "\nprint(" + strconv.Quote(endMarker) + ")\n"
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
