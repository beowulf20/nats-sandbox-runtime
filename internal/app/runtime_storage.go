package app

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type runtimeStorageFile struct {
	Path       string `json:"path"`
	Exists     bool   `json:"exists"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	ModifiedAt string `json:"modified_at,omitempty"`
}

type runtimeSnapshotStatus struct {
	WorkerID    string                        `json:"worker_id"`
	Busy        bool                          `json:"busy"`
	SnapshotDir string                        `json:"snapshot_dir"`
	OK          bool                          `json:"ok"`
	Reason      string                        `json:"reason,omitempty"`
	Version     string                        `json:"version,omitempty"`
	Files       map[string]runtimeStorageFile `json:"files"`
}

type runtimeWorkspaceStatus struct {
	Key          string             `json:"key"`
	Busy         bool               `json:"busy"`
	WorkspaceMiB int64              `json:"workspace_mib"`
	ExpiresAt    string             `json:"expires_at,omitempty"`
	File         runtimeStorageFile `json:"file"`
}

type runtimeSnapshotsResponse struct {
	Snapshots []runtimeSnapshotStatus `json:"snapshots"`
}

type runtimeWorkspacesResponse struct {
	Workspaces []runtimeWorkspaceStatus `json:"workspaces"`
}

type runtimeStorageStatusResponse struct {
	WorkerID string `json:"worker_id,omitempty"`
	Key      string `json:"key,omitempty"`
	Status   string `json:"status"`
}

func runtimeSnapshotStatuses(pool *RuntimeWorkerPool) runtimeSnapshotsResponse {
	snapshot := pool.Snapshot()
	statuses := make([]runtimeSnapshotStatus, 0, len(snapshot.Workers))
	for _, worker := range snapshot.Workers {
		cfg := runtimeStorageWorkerConfig(pool, worker)
		paths := localPythonSnapshotPaths(worker.SnapshotDir)
		check := localPythonSnapshotCheck(paths, cfg)
		statuses = append(statuses, runtimeSnapshotStatus{
			WorkerID:    worker.ID,
			Busy:        worker.Status == runtimeWorkerBusy,
			SnapshotDir: worker.SnapshotDir,
			OK:          check.OK,
			Reason:      check.Reason,
			Version:     readRuntimeStorageVersion(paths.VersionPath),
			Files: map[string]runtimeStorageFile{
				"snapshot": runtimeStorageFileInfo(paths.StatePath),
				"memory":   runtimeStorageFileInfo(paths.MemoryPath),
				"version":  runtimeStorageFileInfo(paths.VersionPath),
				"swap":     runtimeStorageFileInfo(paths.SwapPath),
			},
		})
	}
	return runtimeSnapshotsResponse{Snapshots: statuses}
}

func resetRuntimeSnapshot(pool *RuntimeWorkerPool, workerID string) error {
	worker, err := runtimeStorageWorker(pool, workerID)
	if err != nil {
		return err
	}
	if worker.Status == runtimeWorkerBusy {
		return runtimeStorageConflictError{message: fmt.Sprintf("worker %q is busy", worker.ID)}
	}
	paths := localPythonSnapshotPaths(worker.SnapshotDir)
	for _, path := range []string{paths.StatePath, paths.MemoryPath, paths.VersionPath, paths.SwapPath} {
		if err := removeRuntimeStorageFile(path); err != nil {
			return err
		}
	}
	return nil
}

func runtimeStorageWorker(pool *RuntimeWorkerPool, workerID string) (RuntimeWorker, error) {
	if workerID == "" {
		return RuntimeWorker{}, fmt.Errorf("worker id must not be empty")
	}
	for _, worker := range pool.Snapshot().Workers {
		if worker.ID == workerID {
			return worker, nil
		}
	}
	return RuntimeWorker{}, runtimeStorageNotFoundError{message: fmt.Sprintf("worker %q not found", workerID)}
}

func runtimeStorageWorkerConfig(pool *RuntimeWorkerPool, worker RuntimeWorker) LocalPythonConfig {
	cfg := pool.base.LocalPython
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
		if timeout, err := time.ParseDuration(worker.ExecTimeout); err == nil {
			cfg.ExecTimeout = timeout
		}
	}
	cfg.SnapshotDir = worker.SnapshotDir
	return cfg
}

func runtimeStorageFileInfo(path string) runtimeStorageFile {
	file := runtimeStorageFile{Path: path}
	info, err := os.Stat(path)
	if err != nil {
		return file
	}
	file.Exists = true
	file.SizeBytes = info.Size()
	file.ModifiedAt = info.ModTime().UTC().Format(time.RFC3339)
	return file
}

func readRuntimeStorageVersion(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func removeRuntimeStorageFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %q: %w", path, err)
	}
	return nil
}

func runtimeStorageWorkerID(path, prefix string) (string, error) {
	workerID := strings.TrimPrefix(path, prefix)
	if workerID == "" || workerID == path {
		return "", fmt.Errorf("worker id must not be empty")
	}
	workerID, err := url.PathUnescape(filepath.Base(workerID))
	if err != nil {
		return "", err
	}
	return workerID, nil
}

func runtimeStorageWorkspaceKey(path string) (string, error) {
	key := strings.TrimPrefix(path, "/api/workspaces/")
	if key == "" || key == path {
		return "", fmt.Errorf("workspace key must not be empty")
	}
	key, err := url.PathUnescape(key)
	if err != nil {
		return "", err
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("workspace key must not be empty")
	}
	return key, nil
}

type runtimeStorageNotFoundError struct {
	message string
}

func (e runtimeStorageNotFoundError) Error() string {
	return e.message
}

type runtimeStorageConflictError struct {
	message string
}

func (e runtimeStorageConflictError) Error() string {
	return e.message
}
