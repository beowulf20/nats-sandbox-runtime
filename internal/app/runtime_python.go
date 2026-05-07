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
	cfg       RuntimePythonConfig
	store     jetstream.ObjectStore
	semaphore chan struct{}
}

func RunRuntimePython(ctx context.Context, cfg RuntimePythonConfig, out io.Writer) error {
	if cfg.URL == "" {
		return fmt.Errorf("url must not be empty")
	}
	if cfg.Bucket == "" {
		return fmt.Errorf("bucket must not be empty")
	}
	if cfg.MaxParallel < 1 {
		return fmt.Errorf("max-parallel must be at least 1")
	}
	if err := validateLocalPythonConfig(cfg.LocalPython); err != nil {
		return err
	}
	nc, err := nats.Connect(cfg.URL, nats.Name("python-runtime-service"))
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("create jetstream context: %w", err)
	}
	store, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      cfg.Bucket,
		Description: "Python runtime workspace artifacts",
	})
	if err != nil {
		return fmt.Errorf("create object store %q: %w", cfg.Bucket, err)
	}
	runtime := &runtimePythonService{
		cfg:       cfg,
		store:     store,
		semaphore: make(chan struct{}, cfg.MaxParallel),
	}
	srv, err := micro.AddService(nc, micro.Config{
		Name:        runtimePythonServiceName,
		Version:     runtimePythonServiceVersion,
		Description: runtimePythonServiceDescription,
	})
	if err != nil {
		return fmt.Errorf("add runtime service: %w", err)
	}
	defer func() { _ = srv.Stop() }()
	if err := srv.AddEndpoint("run", micro.HandlerFunc(runtime.handleRun), micro.WithEndpointSubject(runtimePythonEndpointSubject)); err != nil {
		return fmt.Errorf("add runtime endpoint: %w", err)
	}
	fmt.Fprintf(out, "ready: service=%s endpoint=%s url=%s bucket=%s max_parallel=%d\n", runtimePythonServiceName, runtimePythonEndpointSubject, cfg.URL, cfg.Bucket, cfg.MaxParallel)
	<-ctx.Done()
	return nil
}

func (s *runtimePythonService) handleRun(req micro.Request) {
	select {
	case s.semaphore <- struct{}{}:
		defer func() { <-s.semaphore }()
	default:
		_ = req.Error("503", "python runtime is busy", nil)
		return
	}
	var runReq PythonRunRequest
	if len(req.Data()) > 0 {
		if err := json.Unmarshal(req.Data(), &runReq); err != nil {
			_ = req.Error("400", "invalid python run request", []byte(err.Error()))
			return
		}
	}
	resp, stdout, stderr, err := s.run(req, runReq)
	if err != nil {
		headers := s.logHeaders(stdout, stderr)
		_ = req.Error("500", err.Error(), nil, micro.WithHeaders(micro.Headers(headers)))
		return
	}
	headers := s.logHeaders(stdout, stderr)
	_ = req.RespondJSON(resp, micro.WithHeaders(micro.Headers(headers)))
}

func (s *runtimePythonService) run(_ micro.Request, runReq PythonRunRequest) (PythonRunResponse, []byte, []byte, error) {
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
	cfg, err := s.localPythonConfigForRun(runReq, workDir, workspaceDir, wrapRuntimePythonCode(code, startMarker, endMarker))
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
		StdoutHeader:   s.cfg.StdoutHeader,
		StderrHeader:   s.cfg.StderrHeader,
	}
	_, response.StdoutTruncated = truncateForMetadata(stdout, s.cfg.TruncateLogMiB)
	_, response.StderrTruncated = truncateForMetadata(result.Stderr, s.cfg.TruncateLogMiB)
	return response, stdout, result.Stderr, nil
}

func (s *runtimePythonService) localPythonConfigForRun(runReq PythonRunRequest, workDir, workspaceDir, code string) (LocalPythonConfig, error) {
	cfg := s.cfg.LocalPython
	cfg.InlineCommand = code
	cfg.ExecFilePath = ""
	cfg.WorkspaceDir = workspaceDir
	cfg.SnapshotDir = filepath.Join(workDir, "snapshot")
	cfg.Runs = 1
	cfg.ParallelRuns = 1
	cfg.HideFirecrackerLog = true
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
