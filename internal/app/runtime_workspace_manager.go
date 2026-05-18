package app

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const runtimeWorkspaceKeyFile = "key"
const runtimeWorkspaceTTL = time.Hour

type RuntimeWorkspaceManager struct {
	mu      sync.Mutex
	baseDir string
	locks   map[string]*sync.Mutex
	active  map[string]int
}

type RuntimeWorkspaceLease struct {
	Key       string
	ImagePath string
	release   func()
}

func NewRuntimeWorkspaceManager(snapshotDir string) *RuntimeWorkspaceManager {
	return &RuntimeWorkspaceManager{
		baseDir: filepath.Join(snapshotDir, "workspaces"),
		locks:   map[string]*sync.Mutex{},
		active:  map[string]int{},
	}
}

func (m *RuntimeWorkspaceManager) Begin(key string) (RuntimeWorkspaceLease, error) {
	if m == nil {
		return RuntimeWorkspaceLease{}, fmt.Errorf("workspace manager is not configured")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return RuntimeWorkspaceLease{}, fmt.Errorf("workspace key must not be empty")
	}
	lock := m.lockFor(key)
	m.mu.Lock()
	m.active[key]++
	m.mu.Unlock()
	lock.Lock()
	release := func() {
		m.mu.Lock()
		if m.active[key] <= 1 {
			delete(m.active, key)
		} else {
			m.active[key]--
		}
		m.mu.Unlock()
		lock.Unlock()
	}
	dir := m.workspaceDir(key)
	imagePath := filepath.Join(dir, "workspace.ext4")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		release()
		return RuntimeWorkspaceLease{}, fmt.Errorf("create workspace dir: %w", err)
	}
	if err := m.pruneExpiredWorkspaceDir(key, dir, imagePath, true); err != nil {
		release()
		return RuntimeWorkspaceLease{}, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		release()
		return RuntimeWorkspaceLease{}, fmt.Errorf("create workspace dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, runtimeWorkspaceKeyFile), []byte(key+"\n"), 0o644); err != nil {
		release()
		return RuntimeWorkspaceLease{}, fmt.Errorf("write workspace key: %w", err)
	}

	return RuntimeWorkspaceLease{
		Key:       key,
		ImagePath: imagePath,
		release:   release,
	}, nil
}

func (l RuntimeWorkspaceLease) Release() {
	if l.release != nil {
		l.release()
	}
}

func (m *RuntimeWorkspaceManager) List(workspaceMiB int64) runtimeWorkspacesResponse {
	if m == nil {
		return runtimeWorkspacesResponse{Workspaces: []runtimeWorkspaceStatus{}}
	}
	workspaces := []runtimeWorkspaceStatus{}
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		return runtimeWorkspacesResponse{Workspaces: workspaces}
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(m.baseDir, entry.Name())
		key := m.readKey(dir, entry.Name())
		imagePath := filepath.Join(dir, "workspace.ext4")
		if err := m.pruneExpiredWorkspaceDir(key, dir, imagePath, false); err == nil {
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				continue
			}
		}
		file := runtimeStorageFileInfo(imagePath)
		workspaces = append(workspaces, runtimeWorkspaceStatus{
			Key:          key,
			Busy:         m.isActive(key),
			WorkspaceMiB: workspaceMiB,
			ExpiresAt:    runtimeWorkspaceExpiresAt(file),
			File:         file,
		})
	}
	sort.Slice(workspaces, func(i, j int) bool {
		return workspaces[i].Key < workspaces[j].Key
	})
	return runtimeWorkspacesResponse{Workspaces: workspaces}
}

func (m *RuntimeWorkspaceManager) Delete(key string) error {
	if m == nil {
		return fmt.Errorf("workspace manager is not configured")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("workspace key must not be empty")
	}
	if m.isActive(key) {
		return runtimeStorageConflictError{message: fmt.Sprintf("workspace %q is busy", key)}
	}
	if err := os.RemoveAll(m.workspaceDir(key)); err != nil {
		return fmt.Errorf("remove workspace %q: %w", key, err)
	}
	return nil
}

func (m *RuntimeWorkspaceManager) lockFor(key string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock := m.locks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		m.locks[key] = lock
	}
	return lock
}

func (m *RuntimeWorkspaceManager) isActive(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active[key] > 0
}

func (m *RuntimeWorkspaceManager) workspaceDir(key string) string {
	return filepath.Join(m.baseDir, base64.RawURLEncoding.EncodeToString([]byte(key)))
}

func (m *RuntimeWorkspaceManager) readKey(dir, encoded string) string {
	data, err := os.ReadFile(filepath.Join(dir, runtimeWorkspaceKeyFile))
	if err == nil {
		if key := strings.TrimSpace(string(data)); key != "" {
			return key
		}
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return encoded
	}
	return string(decoded)
}

func (m *RuntimeWorkspaceManager) pruneExpiredWorkspaceDir(key, dir, imagePath string, force bool) error {
	if !force && m.isActive(key) {
		return nil
	}
	info, err := os.Stat(imagePath)
	if err != nil {
		return nil
	}
	if time.Since(info.ModTime()) <= runtimeWorkspaceTTL {
		return nil
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove expired workspace %q: %w", key, err)
	}
	return nil
}

func runtimeWorkspaceExpiresAt(file runtimeStorageFile) string {
	if !file.Exists || file.ModifiedAt == "" {
		return ""
	}
	modifiedAt, err := time.Parse(time.RFC3339, file.ModifiedAt)
	if err != nil {
		return ""
	}
	return modifiedAt.Add(runtimeWorkspaceTTL).UTC().Format(time.RFC3339)
}
