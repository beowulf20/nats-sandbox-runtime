package app

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeAPIHTTPOverview(t *testing.T) {
	handler := newRuntimeAPIHTTPHandler(
		NewRuntimeControlPlane(NewInMemorySettingsStore()),
		nil,
		nil,
		staticRuntimeAPIOverviewProvider{overview: RuntimeAPIOverviewResponse{
			NATS: RuntimeAPINATSStatus{
				URL:           "nats://demo:4222",
				Connected:     true,
				ConnectedURL:  "nats://127.0.0.1:4222",
				ServerName:    "demo",
				ServerVersion: "2.11.0",
				JetStream:     true,
			},
			Runtime: RuntimeAPIRuntimeStatus{
				ServiceName:    runtimePythonServiceName,
				ServiceVersion: runtimePythonServiceVersion,
				Online:         true,
				ID:             "svc-1",
				Endpoints: []RuntimeAPIEndpointStatus{
					{Name: "run", Subject: runtimePythonEndpointSubject},
				},
			},
			Config: RuntimeAPIConfigStatus{
				Bucket: "PY",
			},
			CheckedAt: "2026-05-07T12:00:00Z",
		}},
		t.TempDir(),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", resp.Code, resp.Body.String())
	}
	requireJSONEqual(t, resp.Body.Bytes(), `{
		"nats":{
			"url":"nats://demo:4222",
			"connected":true,
			"connected_url":"nats://127.0.0.1:4222",
			"server_name":"demo",
			"server_version":"2.11.0",
			"jetstream":true
		},
		"runtime":{
			"service_name":"python-runtime",
			"service_version":"0.0.1",
			"online":true,
			"id":"svc-1",
			"endpoints":[{"name":"run","subject":"python.run"}]
		},
		"config":{"bucket":"PY"},
		"checked_at":"2026-05-07T12:00:00Z"
	}`)
}

func TestRuntimeAPIHTTPWorkersHandlers(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = t.TempDir()
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	handler := newRuntimeAPIHTTPHandler(
		NewRuntimeControlPlane(NewInMemorySettingsStore()),
		pool,
		NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir),
		staticRuntimeAPIOverviewProvider{},
		t.TempDir(),
	)

	listReq := httptest.NewRequest(http.MethodGet, "/api/workers", nil)
	listResp := httptest.NewRecorder()
	handler.ServeHTTP(listResp, listReq)
	if listResp.Code != http.StatusOK {
		t.Fatalf("GET workers status = %d body = %s, want 200", listResp.Code, listResp.Body.String())
	}
	var listBody controlWorkersListResponse
	if err := json.Unmarshal(listResp.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("unmarshal workers list returned error: %v: %s", err, listResp.Body.String())
	}
	if len(listBody.Workers) != 1 || listBody.Workers[0].ID != "worker-1" {
		t.Fatalf("workers list = %#v, want initial worker-1", listBody.Workers)
	}

	putReq := httptest.NewRequest(http.MethodPut, "/api/workers", jsonBody(`{"count":3}`))
	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)
	if putResp.Code != http.StatusOK {
		t.Fatalf("PUT workers status = %d body = %s, want 200", putResp.Code, putResp.Body.String())
	}
	var putBody controlWorkersSetResponse
	if err := json.Unmarshal(putResp.Body.Bytes(), &putBody); err != nil {
		t.Fatalf("unmarshal workers set returned error: %v: %s", err, putResp.Body.String())
	}
	if putBody.Status != "ok" || putBody.DesiredCount != 3 || len(putBody.Workers) != 3 {
		t.Fatalf("workers set response = %#v, want ok desired count 3", putBody)
	}
}

func TestRuntimeAPIHTTPWorkerEventsStreamsSnapshots(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = t.TempDir()
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	handler := newRuntimeAPIHTTPHandler(
		NewRuntimeControlPlane(NewInMemorySettingsStore()),
		pool,
		NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir),
		staticRuntimeAPIOverviewProvider{},
		t.TempDir(),
	)
	server := httptest.NewServer(handler)
	defer server.Close()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, server.URL+"/api/workers/events", nil)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET worker events returned error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("worker events status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}
	reader := bufio.NewReader(resp.Body)

	initial := readWorkerSSESnapshot(t, reader)
	if initial.DesiredCount != 1 || len(initial.Workers) != 1 {
		t.Fatalf("initial SSE snapshot = %#v, want one worker", initial)
	}

	if _, err := pool.SetWorkerCount(2); err != nil {
		t.Fatalf("SetWorkerCount returned error: %v", err)
	}
	updated := readWorkerSSESnapshot(t, reader)
	if updated.DesiredCount != 2 || len(updated.Workers) != 2 {
		t.Fatalf("updated SSE snapshot = %#v, want two workers", updated)
	}
}

func TestRuntimeAPIHTTPWorkersRejectInvalidRequests(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	handler := newRuntimeAPIHTTPHandler(
		NewRuntimeControlPlane(NewInMemorySettingsStore()),
		pool,
		NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir),
		staticRuntimeAPIOverviewProvider{},
		t.TempDir(),
	)

	for _, test := range []struct {
		name   string
		method string
		body   string
	}{
		{name: "bad json body", method: http.MethodPut, body: `{"count":`},
		{name: "invalid count", method: http.MethodPut, body: `{"count":0}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(test.method, "/api/workers", jsonBody(test.body))
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body = %s, want 400", resp.Code, resp.Body.String())
			}
		})
	}
}

func TestRuntimeAPIHTTPSnapshotHandlers(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	worker := pool.ListWorkers()[0]
	paths := localPythonSnapshotPaths(worker.SnapshotDir)
	writeRuntimeAPITestFile(t, paths.StatePath, "state")
	writeRuntimeAPITestFile(t, paths.MemoryPath, "memory")
	writeRuntimeAPITestFile(t, paths.VersionPath, localPythonSnapshotVersion(cfg.LocalPython)+"\n")
	writeRuntimeAPITestFile(t, paths.WorkspacePath, "workspace")

	handler := newRuntimeAPIHTTPHandler(NewRuntimeControlPlane(nil), pool, NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir), staticRuntimeAPIOverviewProvider{}, t.TempDir())
	listReq := httptest.NewRequest(http.MethodGet, "/api/snapshots", nil)
	listResp := httptest.NewRecorder()
	handler.ServeHTTP(listResp, listReq)
	if listResp.Code != http.StatusOK {
		t.Fatalf("GET snapshots status = %d body = %s, want 200", listResp.Code, listResp.Body.String())
	}
	var listBody runtimeSnapshotsResponse
	if err := json.Unmarshal(listResp.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("unmarshal snapshots returned error: %v: %s", err, listResp.Body.String())
	}
	if len(listBody.Snapshots) != 1 || !listBody.Snapshots[0].OK || listBody.Snapshots[0].Version == "" {
		t.Fatalf("snapshots = %#v, want one ready snapshot with version", listBody.Snapshots)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/snapshots/workers/"+worker.ID, nil)
	deleteResp := httptest.NewRecorder()
	handler.ServeHTTP(deleteResp, deleteReq)
	if deleteResp.Code != http.StatusOK {
		t.Fatalf("DELETE snapshot status = %d body = %s, want 200", deleteResp.Code, deleteResp.Body.String())
	}
	for _, path := range []string{paths.StatePath, paths.MemoryPath, paths.VersionPath, paths.SwapPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("snapshot file %s exists after delete, err=%v", path, err)
		}
	}
	if _, err := os.Stat(paths.WorkspacePath); err != nil {
		t.Fatalf("workspace path after snapshot delete stat err = %v, want still present", err)
	}
}

func TestRuntimeAPIHTTPWorkspaceHandlers(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	cfg.LocalPython.WorkspaceMiB = 32
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	workspaces := NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir)
	lease, err := workspaces.Begin("thread-a")
	if err != nil {
		t.Fatalf("Begin workspace returned error: %v", err)
	}
	writeRuntimeAPITestFile(t, lease.ImagePath, "workspace")
	lease.Release()

	handler := newRuntimeAPIHTTPHandler(NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython), pool, workspaces, staticRuntimeAPIOverviewProvider{}, t.TempDir())
	listReq := httptest.NewRequest(http.MethodGet, "/api/workspaces", nil)
	listResp := httptest.NewRecorder()
	handler.ServeHTTP(listResp, listReq)
	if listResp.Code != http.StatusOK {
		t.Fatalf("GET workspaces status = %d body = %s, want 200", listResp.Code, listResp.Body.String())
	}
	var listBody runtimeWorkspacesResponse
	if err := json.Unmarshal(listResp.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("unmarshal workspaces returned error: %v: %s", err, listResp.Body.String())
	}
	if len(listBody.Workspaces) != 1 || listBody.Workspaces[0].Key != "thread-a" || !listBody.Workspaces[0].File.Exists || listBody.Workspaces[0].WorkspaceMiB != 32 {
		t.Fatalf("workspaces = %#v, want one existing 32 MiB thread-a workspace", listBody.Workspaces)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/workspaces/thread-a", nil)
	deleteResp := httptest.NewRecorder()
	handler.ServeHTTP(deleteResp, deleteReq)
	if deleteResp.Code != http.StatusOK {
		t.Fatalf("DELETE workspace status = %d body = %s, want 200", deleteResp.Code, deleteResp.Body.String())
	}
	if _, err := os.Stat(lease.ImagePath); !os.IsNotExist(err) {
		t.Fatalf("workspace file exists after delete, err=%v", err)
	}
}

func TestRuntimeAPIHTTPWorkspaceHandlersReturnEmptyList(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	handler := newRuntimeAPIHTTPHandler(
		NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython),
		pool,
		NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir),
		staticRuntimeAPIOverviewProvider{},
		t.TempDir(),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/workspaces", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("GET workspaces status = %d body = %s, want 200", resp.Code, resp.Body.String())
	}
	requireJSONEqual(t, resp.Body.Bytes(), `{"workspaces":[]}`)
}

func TestRuntimeAPIHTTPWorkspaceHandlersPruneExpiredWorkspaces(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	workspaces := NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir)
	lease, err := workspaces.Begin("thread-a")
	if err != nil {
		t.Fatalf("Begin workspace returned error: %v", err)
	}
	writeRuntimeAPITestFile(t, lease.ImagePath, "workspace")
	lease.Release()
	expiredAt := time.Now().Add(-runtimeWorkspaceTTL - time.Minute)
	if err := os.Chtimes(lease.ImagePath, expiredAt, expiredAt); err != nil {
		t.Fatalf("Chtimes workspace returned error: %v", err)
	}

	handler := newRuntimeAPIHTTPHandler(NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython), pool, workspaces, staticRuntimeAPIOverviewProvider{}, t.TempDir())
	req := httptest.NewRequest(http.MethodGet, "/api/workspaces", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("GET workspaces status = %d body = %s, want 200", resp.Code, resp.Body.String())
	}
	requireJSONEqual(t, resp.Body.Bytes(), `{"workspaces":[]}`)
	if _, err := os.Stat(lease.ImagePath); !os.IsNotExist(err) {
		t.Fatalf("expired workspace file still exists, err=%v", err)
	}
}

func TestRuntimeAPIHTTPWorkspaceHandlersKeepBusyExpiredWorkspace(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	workspaces := NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir)
	lease, err := workspaces.Begin("thread-a")
	if err != nil {
		t.Fatalf("Begin workspace returned error: %v", err)
	}
	defer lease.Release()
	writeRuntimeAPITestFile(t, lease.ImagePath, "workspace")
	expiredAt := time.Now().Add(-runtimeWorkspaceTTL - time.Minute)
	if err := os.Chtimes(lease.ImagePath, expiredAt, expiredAt); err != nil {
		t.Fatalf("Chtimes workspace returned error: %v", err)
	}

	handler := newRuntimeAPIHTTPHandler(NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.LocalPython), pool, workspaces, staticRuntimeAPIOverviewProvider{}, t.TempDir())
	req := httptest.NewRequest(http.MethodGet, "/api/workspaces", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("GET workspaces status = %d body = %s, want 200", resp.Code, resp.Body.String())
	}
	var body runtimeWorkspacesResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal workspaces returned error: %v: %s", err, resp.Body.String())
	}
	if len(body.Workspaces) != 1 || !body.Workspaces[0].Busy {
		t.Fatalf("workspaces = %#v, want busy expired workspace to remain listed", body.Workspaces)
	}
}

func TestRuntimeWorkspaceBeginRemovesExpiredImageBeforeReuse(t *testing.T) {
	workspaces := NewRuntimeWorkspaceManager(t.TempDir())
	lease, err := workspaces.Begin("thread-a")
	if err != nil {
		t.Fatalf("Begin workspace returned error: %v", err)
	}
	writeRuntimeAPITestFile(t, lease.ImagePath, "stale workspace")
	lease.Release()
	expiredAt := time.Now().Add(-runtimeWorkspaceTTL - time.Minute)
	if err := os.Chtimes(lease.ImagePath, expiredAt, expiredAt); err != nil {
		t.Fatalf("Chtimes workspace returned error: %v", err)
	}

	reused, err := workspaces.Begin("thread-a")
	if err != nil {
		t.Fatalf("Begin expired workspace returned error: %v", err)
	}
	defer reused.Release()
	if _, err := os.Stat(reused.ImagePath); !os.IsNotExist(err) {
		t.Fatalf("expired workspace image stat err = %v, want removed before sync", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(reused.ImagePath), runtimeWorkspaceKeyFile)); err != nil {
		t.Fatalf("workspace key file stat returned error: %v, want workspace metadata recreated", err)
	}
}

func TestRuntimeAPIHTTPStorageRejectsBusyWorkerReset(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	worker, ok := pool.AcquireWorker()
	if !ok {
		t.Fatal("AcquireWorker returned false, want worker")
	}
	defer pool.ReleaseWorker(worker.ID)
	workspaces := NewRuntimeWorkspaceManager(cfg.LocalPython.SnapshotDir)
	lease, err := workspaces.Begin("thread-a")
	if err != nil {
		t.Fatalf("Begin workspace returned error: %v", err)
	}
	defer lease.Release()
	handler := newRuntimeAPIHTTPHandler(NewRuntimeControlPlane(nil), pool, workspaces, staticRuntimeAPIOverviewProvider{}, t.TempDir())

	for _, path := range []string{"/api/snapshots/workers/" + worker.ID, "/api/workspaces/thread-a"} {
		req := httptest.NewRequest(http.MethodDelete, path, nil)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusConflict {
			t.Fatalf("DELETE %s status = %d body = %s, want 409", path, resp.Code, resp.Body.String())
		}
	}
}

func writeRuntimeAPITestFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll %s returned error: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile %s returned error: %v", path, err)
	}
}

func readWorkerSSESnapshot(t *testing.T, reader *bufio.Reader) RuntimeWorkerSnapshot {
	t.Helper()
	type result struct {
		snapshot RuntimeWorkerSnapshot
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		var event string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
				continue
			}
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			if event != "" && event != "workers" {
				continue
			}
			var snapshot RuntimeWorkerSnapshot
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &snapshot); err != nil {
				resultCh <- result{err: err}
				return
			}
			resultCh <- result{snapshot: snapshot}
			return
		}
	}()
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("read SSE snapshot returned error: %v", result.err)
		}
		return result.snapshot
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for SSE snapshot")
		return RuntimeWorkerSnapshot{}
	}
}

func TestRuntimeAPIHTTPSettingsHandlers(t *testing.T) {
	handler := newRuntimeAPIHTTPHandler(
		NewRuntimeControlPlane(NewInMemorySettingsStore()),
		nil,
		nil,
		staticRuntimeAPIOverviewProvider{},
		t.TempDir(),
	)

	putReq := httptest.NewRequest(http.MethodPut, "/api/settings/runtime.default_memory_mib", jsonBody(`{"value":256}`))
	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)
	if putResp.Code != http.StatusOK {
		t.Fatalf("PUT status = %d body = %s, want 200", putResp.Code, putResp.Body.String())
	}
	requireJSONEqual(t, putResp.Body.Bytes(), `{"key":"runtime.default_memory_mib","status":"ok"}`)

	getReq := httptest.NewRequest(http.MethodGet, "/api/settings/runtime.default_memory_mib", nil)
	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("GET status = %d body = %s, want 200", getResp.Code, getResp.Body.String())
	}
	requireJSONEqual(t, getResp.Body.Bytes(), `{"key":"runtime.default_memory_mib","found":true,"value":256}`)

	listReq := httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	listResp := httptest.NewRecorder()
	handler.ServeHTTP(listResp, listReq)
	if listResp.Code != http.StatusOK {
		t.Fatalf("LIST status = %d body = %s, want 200", listResp.Code, listResp.Body.String())
	}
	var listBody controlSettingsListResponse
	if err := json.Unmarshal(listResp.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("unmarshal settings list returned error: %v: %s", err, listResp.Body.String())
	}
	if len(listBody.Settings) != 4 {
		t.Fatalf("settings list returned %d settings, want 4: %#v", len(listBody.Settings), listBody.Settings)
	}
	memory := requireSetting(t, listBody.Settings, "runtime.default_memory_mib")
	if string(memory.Value) != "256" || memory.Source != "override" || memory.Type != "integer" {
		t.Fatalf("memory setting = %#v, want override integer 256", memory)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/settings/runtime.default_memory_mib", nil)
	deleteResp := httptest.NewRecorder()
	handler.ServeHTTP(deleteResp, deleteReq)
	if deleteResp.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d body = %s, want 200", deleteResp.Code, deleteResp.Body.String())
	}
	requireJSONEqual(t, deleteResp.Body.Bytes(), `{"key":"runtime.default_memory_mib","status":"deleted"}`)
}

func TestRuntimeAPIHTTPSettingsRejectInvalidRequests(t *testing.T) {
	handler := newRuntimeAPIHTTPHandler(
		NewRuntimeControlPlane(NewInMemorySettingsStore()),
		nil,
		nil,
		staticRuntimeAPIOverviewProvider{},
		t.TempDir(),
	)

	for _, test := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "bad key", method: http.MethodPut, path: "/api/settings/bad%20key", body: `{"value":128}`},
		{name: "bad json body", method: http.MethodPut, path: "/api/settings/runtime.default_memory_mib", body: `{"value":`},
		{name: "missing value", method: http.MethodPut, path: "/api/settings/runtime.default_memory_mib", body: `{}`},
		{name: "invalid json value", method: http.MethodPut, path: "/api/settings/runtime.default_memory_mib", body: `{"value":{`},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(test.method, test.path, jsonBody(test.body))
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body = %s, want 400", resp.Code, resp.Body.String())
			}
		})
	}
}

func TestRuntimeAPIHTTPServesStaticBuildAndFallback(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("<html>app</html>"), 0o644); err != nil {
		t.Fatalf("WriteFile index returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "asset.txt"), []byte("asset"), 0o644); err != nil {
		t.Fatalf("WriteFile asset returned error: %v", err)
	}
	handler := newRuntimeAPIHTTPHandler(NewRuntimeControlPlane(nil), nil, nil, staticRuntimeAPIOverviewProvider{}, webDir)

	assetReq := httptest.NewRequest(http.MethodGet, "/asset.txt", nil)
	assetResp := httptest.NewRecorder()
	handler.ServeHTTP(assetResp, assetReq)
	if assetResp.Code != http.StatusOK || assetResp.Body.String() != "asset" {
		t.Fatalf("asset response = %d %q, want 200 asset", assetResp.Code, assetResp.Body.String())
	}

	spaReq := httptest.NewRequest(http.MethodGet, "/settings", nil)
	spaResp := httptest.NewRecorder()
	handler.ServeHTTP(spaResp, spaReq)
	if spaResp.Code != http.StatusOK || spaResp.Body.String() != "<html>app</html>" {
		t.Fatalf("SPA response = %d %q, want index fallback", spaResp.Code, spaResp.Body.String())
	}
}

func TestRuntimeAPIHTTPMissingBuildReturnsDevelopmentMessage(t *testing.T) {
	handler := newRuntimeAPIHTTPHandler(NewRuntimeControlPlane(nil), nil, nil, staticRuntimeAPIOverviewProvider{}, filepath.Join(t.TempDir(), "missing"))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d body = %s, want 404", resp.Code, resp.Body.String())
	}
	if !containsAll(resp.Body.String(), "frontend build is missing", "npm run build") {
		t.Fatalf("body = %q, want development message", resp.Body.String())
	}
}

type staticRuntimeAPIOverviewProvider struct {
	overview RuntimeAPIOverviewResponse
	err      error
}

func (p staticRuntimeAPIOverviewProvider) Overview(context.Context) (RuntimeAPIOverviewResponse, error) {
	return p.overview, p.err
}

func jsonBody(value string) *strings.Reader {
	return strings.NewReader(value)
}
