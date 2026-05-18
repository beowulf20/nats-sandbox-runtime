package pyruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

const (
	defaultURL    = "nats://localhost:4222"
	defaultBucket = "python-runtime-workspaces"

	defaultMaxInputFileBytes   int64 = 10 * 1024 * 1024
	defaultMaxInputTotalBytes  int64 = 32 * 1024 * 1024
	defaultMaxOutputFileBytes  int64 = 10 * 1024 * 1024
	defaultMaxOutputTotalBytes int64 = 32 * 1024 * 1024

	runSubject   = "python.run"
	stdoutHeader = "Nats-Sandbox-Runtime-Python-Stdout-B64"
	stderrHeader = "Nats-Sandbox-Runtime-Python-Stderr-B64"
)

// Config controls the SDK connection, Object Store bucket, and byte safeguards.
type Config struct {
	URL    string
	Bucket string

	MaxInputFileBytes   int64
	MaxInputTotalBytes  int64
	MaxOutputFileBytes  int64
	MaxOutputTotalBytes int64
}

// Client runs byte-only Python runtime requests over NATS.
type Client struct {
	nc    *nats.Conn
	req   requester
	store objectStore
	cfg   Config
}

// Request is the V0 byte-only runtime request.
type Request struct {
	ThreadID string
	Code     string
	Files    map[string][]byte
}

// Result contains runtime logs and all workspace artifacts downloaded into memory.
type Result struct {
	RunID  string
	Stdout string
	Stderr string
	Files  map[string][]byte
}

// SizeLimitError reports an SDK input or output byte limit violation.
type SizeLimitError struct {
	Kind  string
	Path  string
	Size  int64
	Limit int64
}

func (e SizeLimitError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s %q size %d exceeds limit %d", e.Kind, e.Path, e.Size, e.Limit)
	}
	return fmt.Sprintf("%s size %d exceeds limit %d", e.Kind, e.Size, e.Limit)
}

// New connects to NATS, opens the configured Object Store bucket, and returns an SDK client.
func New(ctx context.Context, cfg Config) (*Client, error) {
	cfg = withDefaults(cfg)
	nc, err := nats.Connect(cfg.URL, nats.Name("python-runtime-sdk"))
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	store, err := js.ObjectStore(ctx, cfg.Bucket)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("open object store %q: %w", cfg.Bucket, err)
	}
	return &Client{nc: nc, req: nc, store: store, cfg: cfg}, nil
}

// Close closes the NATS connection owned by the client.
func (c *Client) Close() {
	if c == nil || c.nc == nil {
		return
	}
	c.nc.Close()
}

// Run uploads input files, executes python.run, and downloads every returned artifact.
func (c *Client) Run(ctx context.Context, req Request) (*Result, error) {
	if c == nil {
		return nil, fmt.Errorf("pyruntime client is nil")
	}
	if c.req == nil {
		return nil, fmt.Errorf("pyruntime requester is nil")
	}
	if c.store == nil {
		return nil, fmt.Errorf("pyruntime object store is nil")
	}
	cfg := withDefaults(c.cfg)
	if strings.TrimSpace(req.ThreadID) == "" {
		return nil, fmt.Errorf("thread_id must not be empty")
	}
	if strings.TrimSpace(req.Code) == "" {
		return nil, fmt.Errorf("code must not be empty")
	}
	files, err := validateInputFiles(req.Files, cfg)
	if err != nil {
		return nil, err
	}

	runID := newRunID()
	uploaded := make([]string, 0, len(files))
	defer cleanupObjects(ctx, c.store, &uploaded)

	inputs := make([]pythonRunObjectMapping, 0, len(files))
	for _, file := range files {
		objectName := "sdk-inputs/" + runID + "/" + file.path
		if _, err := c.store.PutBytes(ctx, objectName, file.data); err != nil {
			return nil, fmt.Errorf("upload input %q: %w", file.path, err)
		}
		uploaded = append(uploaded, objectName)
		inputs = append(inputs, pythonRunObjectMapping{
			Object: objectName,
			Path:   file.path,
		})
	}

	wireReq := pythonRunRequest{
		RunID:    runID,
		ThreadID: strings.TrimSpace(req.ThreadID),
		Code:     req.Code,
		Inputs:   inputs,
	}
	data, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal python run request: %w", err)
	}
	msg, err := c.req.RequestWithContext(ctx, runSubject, data)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", runSubject, err)
	}
	if err := microError(msg); err != nil {
		return nil, err
	}

	var wireResp pythonRunResponse
	if err := json.Unmarshal(msg.Data, &wireResp); err != nil {
		return nil, fmt.Errorf("decode python run response: %w", err)
	}
	filesOut, err := downloadArtifacts(ctx, c.store, wireResp.Artifacts, cfg)
	if err != nil {
		return nil, err
	}
	stdout, err := decodeLogHeader(msg.Header, stdoutHeader)
	if err != nil {
		return nil, err
	}
	stderr, err := decodeLogHeader(msg.Header, stderrHeader)
	if err != nil {
		return nil, err
	}
	return &Result{
		RunID:  wireResp.RunID,
		Stdout: string(stdout),
		Stderr: string(stderr),
		Files:  filesOut,
	}, nil
}

type requester interface {
	RequestWithContext(ctx context.Context, subject string, data []byte) (*nats.Msg, error)
}

type objectStore interface {
	PutBytes(ctx context.Context, name string, data []byte) (*jetstream.ObjectInfo, error)
	GetBytes(ctx context.Context, name string, opts ...jetstream.GetObjectOpt) ([]byte, error)
	Delete(ctx context.Context, name string) error
}

type pythonRunRequest struct {
	RunID    string                   `json:"run_id"`
	ThreadID string                   `json:"thread_id"`
	Code     string                   `json:"code"`
	Inputs   []pythonRunObjectMapping `json:"inputs,omitempty"`
}

type pythonRunObjectMapping struct {
	Object string `json:"object"`
	Path   string `json:"path"`
}

type pythonRunResponse struct {
	RunID     string              `json:"run_id"`
	Artifacts []pythonRunArtifact `json:"artifacts"`
}

type pythonRunArtifact struct {
	Path   string `json:"path"`
	Object string `json:"object"`
	Size   uint64 `json:"size"`
}

type inputFile struct {
	path string
	data []byte
}

func withDefaults(cfg Config) Config {
	if cfg.URL == "" {
		cfg.URL = defaultURL
	}
	if cfg.Bucket == "" {
		cfg.Bucket = defaultBucket
	}
	if cfg.MaxInputFileBytes == 0 {
		cfg.MaxInputFileBytes = defaultMaxInputFileBytes
	}
	if cfg.MaxInputTotalBytes == 0 {
		cfg.MaxInputTotalBytes = defaultMaxInputTotalBytes
	}
	if cfg.MaxOutputFileBytes == 0 {
		cfg.MaxOutputFileBytes = defaultMaxOutputFileBytes
	}
	if cfg.MaxOutputTotalBytes == 0 {
		cfg.MaxOutputTotalBytes = defaultMaxOutputTotalBytes
	}
	return cfg
}

func validateInputFiles(files map[string][]byte, cfg Config) ([]inputFile, error) {
	paths := make([]string, 0, len(files))
	for filePath := range files {
		paths = append(paths, filePath)
	}
	sort.Strings(paths)

	result := make([]inputFile, 0, len(paths))
	var total int64
	for _, filePath := range paths {
		cleaned, err := cleanRelativePath(filePath)
		if err != nil {
			return nil, err
		}
		data := files[filePath]
		size := int64(len(data))
		if size > cfg.MaxInputFileBytes {
			return nil, SizeLimitError{Kind: "input_file", Path: cleaned, Size: size, Limit: cfg.MaxInputFileBytes}
		}
		total += size
		if total > cfg.MaxInputTotalBytes {
			return nil, SizeLimitError{Kind: "input_total", Size: total, Limit: cfg.MaxInputTotalBytes}
		}
		result = append(result, inputFile{path: cleaned, data: data})
	}
	return result, nil
}

func cleanRelativePath(filePath string) (string, error) {
	if filePath == "" {
		return "", fmt.Errorf("file path must not be empty")
	}
	if strings.Contains(filePath, "\\") {
		return "", fmt.Errorf("invalid file path %q", filePath)
	}
	cleaned := path.Clean(filePath)
	if path.IsAbs(cleaned) || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("invalid file path %q", filePath)
	}
	return cleaned, nil
}

func downloadArtifacts(ctx context.Context, store objectStore, artifacts []pythonRunArtifact, cfg Config) (map[string][]byte, error) {
	checked := make([]pythonRunArtifact, 0, len(artifacts))
	var total int64
	for _, artifact := range artifacts {
		cleaned, err := cleanRelativePath(artifact.Path)
		if err != nil {
			return nil, fmt.Errorf("invalid artifact path %q: %w", artifact.Path, err)
		}
		size := int64(artifact.Size)
		if size > cfg.MaxOutputFileBytes {
			return nil, SizeLimitError{Kind: "output_file", Path: cleaned, Size: size, Limit: cfg.MaxOutputFileBytes}
		}
		total += size
		if total > cfg.MaxOutputTotalBytes {
			return nil, SizeLimitError{Kind: "output_total", Size: total, Limit: cfg.MaxOutputTotalBytes}
		}
		artifact.Path = cleaned
		checked = append(checked, artifact)
	}

	result := make(map[string][]byte, len(checked))
	var downloadedTotal int64
	for _, artifact := range checked {
		data, err := store.GetBytes(ctx, artifact.Object)
		if err != nil {
			return nil, fmt.Errorf("download artifact %q: %w", artifact.Path, err)
		}
		if int64(len(data)) > cfg.MaxOutputFileBytes {
			return nil, SizeLimitError{Kind: "output_file", Path: artifact.Path, Size: int64(len(data)), Limit: cfg.MaxOutputFileBytes}
		}
		downloadedTotal += int64(len(data))
		if downloadedTotal > cfg.MaxOutputTotalBytes {
			return nil, SizeLimitError{Kind: "output_total", Size: downloadedTotal, Limit: cfg.MaxOutputTotalBytes}
		}
		result[artifact.Path] = data
	}
	return result, nil
}

func decodeLogHeader(headers nats.Header, name string) ([]byte, error) {
	value := headers.Get(name)
	if value == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	return decoded, nil
}

func microError(msg *nats.Msg) error {
	if msg == nil {
		return fmt.Errorf("empty response from %s", runSubject)
	}
	code := msg.Header.Get(micro.ErrorCodeHeader)
	description := msg.Header.Get(micro.ErrorHeader)
	if code == "" && description == "" {
		return nil
	}
	if code == "" {
		return fmt.Errorf("python runtime error: %s", description)
	}
	if description == "" {
		return fmt.Errorf("python runtime error %s", code)
	}
	return fmt.Errorf("python runtime error %s: %s", code, description)
}

func cleanupObjects(ctx context.Context, store objectStore, names *[]string) {
	for _, name := range *names {
		_ = store.Delete(ctx, name)
	}
}

func newRunID() string {
	return nats.NewInbox()[5:]
}
