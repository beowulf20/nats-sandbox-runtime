package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

type RuntimeAPIOverviewResponse struct {
	NATS      RuntimeAPINATSStatus    `json:"nats"`
	Runtime   RuntimeAPIRuntimeStatus `json:"runtime"`
	Config    RuntimeAPIConfigStatus  `json:"config"`
	Workers   *RuntimeAPIWorkerStatus `json:"workers,omitempty"`
	CheckedAt string                  `json:"checked_at"`
}

type RuntimeAPINATSStatus struct {
	URL           string `json:"url"`
	Connected     bool   `json:"connected"`
	ConnectedURL  string `json:"connected_url,omitempty"`
	ServerID      string `json:"server_id,omitempty"`
	ServerName    string `json:"server_name,omitempty"`
	ServerVersion string `json:"server_version,omitempty"`
	JetStream     bool   `json:"jetstream"`
	Error         string `json:"error,omitempty"`
}

type RuntimeAPIRuntimeStatus struct {
	ServiceName    string                     `json:"service_name"`
	ServiceVersion string                     `json:"service_version"`
	Online         bool                       `json:"online"`
	ID             string                     `json:"id,omitempty"`
	Endpoints      []RuntimeAPIEndpointStatus `json:"endpoints,omitempty"`
	Error          string                     `json:"error,omitempty"`
}

type RuntimeAPIEndpointStatus struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
}

type RuntimeAPIConfigStatus struct {
	Bucket string `json:"bucket"`
}

type RuntimeAPIWorkerStatus struct {
	Desired int `json:"desired"`
	Total   int `json:"total"`
	Idle    int `json:"idle"`
	Busy    int `json:"busy"`
}

type runtimeAPIOverviewProvider interface {
	Overview(context.Context) (RuntimeAPIOverviewResponse, error)
}

func RunRuntimeAPI(ctx context.Context, cfg RuntimeAPIConfig, out io.Writer) error {
	if cfg.Listen == "" {
		return fmt.Errorf("listen must not be empty")
	}
	if cfg.WebDir == "" {
		return fmt.Errorf("web-dir must not be empty")
	}
	if err := validateRuntimePythonConfig(cfg.Runtime); err != nil {
		return err
	}

	controlPlane := NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg.Runtime.LocalPython)
	registration, err := startRuntimePythonService(ctx, cfg.Runtime, controlPlane)
	if err != nil {
		return err
	}
	defer registration.Close()

	handler := newRuntimeAPIHTTPHandler(controlPlane, registration.runtime.workerPool, registration.runtime.workspaces, runtimeAPIServiceOverviewProvider{
		cfg:          cfg,
		conn:         registration.conn,
		microService: registration.service,
	}, cfg.WebDir)
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	server := &http.Server{
		Handler: handler,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()

	fmt.Fprintf(out, "ready: service=%s endpoint=%s api=http://%s url=%s bucket=%s workers=%d web_dir=%s\n", runtimePythonServiceName, runtimePythonEndpointSubject, listener.Addr().String(), cfg.Runtime.URL, cfg.Runtime.Bucket, cfg.Runtime.MaxParallel, cfg.WebDir)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown runtime api: %w", err)
		}
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve runtime api: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve runtime api: %w", err)
		}
		return nil
	}
}

type runtimeAPIServiceOverviewProvider struct {
	cfg          RuntimeAPIConfig
	conn         *nats.Conn
	microService micro.Service
}

func (p runtimeAPIServiceOverviewProvider) Overview(ctx context.Context) (RuntimeAPIOverviewResponse, error) {
	response := RuntimeAPIOverviewResponse{
		NATS: RuntimeAPINATSStatus{
			URL: p.cfg.Runtime.URL,
		},
		Runtime: RuntimeAPIRuntimeStatus{
			ServiceName:    runtimePythonServiceName,
			ServiceVersion: runtimePythonServiceVersion,
		},
		Config: RuntimeAPIConfigStatus{
			Bucket: p.cfg.Runtime.Bucket,
		},
		CheckedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if p.conn == nil {
		response.NATS.Error = "nats connection is not initialized"
		response.Runtime.Error = "runtime service is not initialized"
		return response, nil
	}
	response.NATS.Connected = p.conn.IsConnected()
	if !response.NATS.Connected {
		if err := p.conn.LastError(); err != nil {
			response.NATS.Error = err.Error()
		} else {
			response.NATS.Error = "nats connection is not connected"
		}
		return response, nil
	}
	response.NATS.ConnectedURL = p.conn.ConnectedUrlRedacted()
	response.NATS.ServerID = p.conn.ConnectedServerId()
	response.NATS.ServerName = p.conn.ConnectedServerName()
	response.NATS.ServerVersion = p.conn.ConnectedServerVersion()
	jetStream, _ := p.conn.ConnectedServerJetStream()
	response.NATS.JetStream = jetStream

	if p.microService != nil {
		info := p.microService.Info()
		response.Runtime.Online = true
		response.Runtime.ID = info.ID
		response.Runtime.ServiceName = info.Name
		response.Runtime.ServiceVersion = info.Version
		response.Runtime.Endpoints = runtimeAPIEndpointStatuses(info.Endpoints)
	}
	subject, err := micro.ControlSubject(micro.InfoVerb, runtimePythonServiceName, "")
	if err != nil {
		response.Runtime.Error = err.Error()
		return response, nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	msg, err := p.conn.RequestWithContext(requestCtx, subject, nil)
	if err != nil {
		response.Runtime.Error = err.Error()
		return response, nil
	}
	var info micro.Info
	if err := json.Unmarshal(msg.Data, &info); err != nil {
		response.Runtime.Error = err.Error()
		return response, nil
	}
	response.Runtime.Online = true
	response.Runtime.ID = info.ID
	response.Runtime.ServiceName = info.Name
	response.Runtime.ServiceVersion = info.Version
	response.Runtime.Endpoints = runtimeAPIEndpointStatuses(info.Endpoints)
	return response, nil
}

func runtimeAPIEndpointStatuses(endpoints []micro.EndpointInfo) []RuntimeAPIEndpointStatus {
	statuses := make([]RuntimeAPIEndpointStatus, 0, len(endpoints))
	for _, endpoint := range endpoints {
		statuses = append(statuses, RuntimeAPIEndpointStatus{
			Name:    endpoint.Name,
			Subject: endpoint.Subject,
		})
	}
	return statuses
}

func newRuntimeAPIHTTPHandler(controlPlane *RuntimeControlPlane, workerPool *RuntimeWorkerPool, workspaces *RuntimeWorkspaceManager, overviewProvider runtimeAPIOverviewProvider, webDir string) http.Handler {
	if controlPlane == nil {
		controlPlane = NewRuntimeControlPlane(NewInMemorySettingsStore())
	}
	mux := http.NewServeMux()
	api := &runtimeAPIHTTPServer{
		controlPlane:     controlPlane,
		workerPool:       workerPool,
		workspaces:       workspaces,
		overviewProvider: overviewProvider,
		webDir:           webDir,
	}
	mux.HandleFunc("/api/overview", api.handleOverview)
	mux.HandleFunc("/api/workers/events", api.handleWorkerEvents)
	mux.HandleFunc("/api/workers", api.handleWorkers)
	mux.HandleFunc("/api/snapshots", api.handleSnapshots)
	mux.HandleFunc("/api/snapshots/workers/", api.handleSnapshot)
	mux.HandleFunc("/api/workspaces", api.handleWorkspaces)
	mux.HandleFunc("/api/workspaces/", api.handleWorkspace)
	mux.HandleFunc("/api/settings", api.handleSettingsList)
	mux.HandleFunc("/api/settings/", api.handleSetting)
	mux.HandleFunc("/", api.handleStatic)
	return mux
}

type runtimeAPIHTTPServer struct {
	controlPlane     *RuntimeControlPlane
	workerPool       *RuntimeWorkerPool
	workspaces       *RuntimeWorkspaceManager
	overviewProvider runtimeAPIOverviewProvider
	webDir           string
}

func (s *runtimeAPIHTTPServer) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.overviewProvider == nil {
		writeRuntimeAPIError(w, http.StatusServiceUnavailable, "overview provider is not configured")
		return
	}
	overview, err := s.overviewProvider.Overview(r.Context())
	if err != nil {
		writeRuntimeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if s.workerPool != nil {
		overview.Workers = runtimeAPIWorkerStatus(s.workerPool.DesiredCount(), s.workerPool.ListWorkers())
	}
	writeRuntimeAPIJSON(w, http.StatusOK, overview)
}

func runtimeAPIWorkerStatus(desired int, workers []RuntimeWorker) *RuntimeAPIWorkerStatus {
	status := &RuntimeAPIWorkerStatus{Desired: desired, Total: len(workers)}
	for _, worker := range workers {
		switch worker.Status {
		case runtimeWorkerBusy:
			status.Busy++
		default:
			status.Idle++
		}
	}
	return status
}

func (s *runtimeAPIHTTPServer) handleWorkers(w http.ResponseWriter, r *http.Request) {
	if s.workerPool == nil {
		writeRuntimeAPIError(w, http.StatusServiceUnavailable, "worker pool is not configured")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeRuntimeAPIJSON(w, http.StatusOK, s.workerPool.Snapshot())
	case http.MethodPut:
		var payload controlWorkersSetRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		workers, err := s.workerPool.SetWorkerCount(payload.Count)
		if err != nil {
			writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeRuntimeAPIJSON(w, http.StatusOK, controlWorkersSetResponse{Status: "ok", DesiredCount: s.workerPool.DesiredCount(), Workers: workers})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *runtimeAPIHTTPServer) handleWorkerEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.workerPool == nil {
		writeRuntimeAPIError(w, http.StatusServiceUnavailable, "worker pool is not configured")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeRuntimeAPIError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	events, cancel := s.workerPool.Subscribe()
	defer cancel()
	for {
		select {
		case <-r.Context().Done():
			return
		case snapshot, ok := <-events:
			if !ok {
				return
			}
			if err := writeRuntimeAPISSE(w, "workers", snapshot); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *runtimeAPIHTTPServer) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	if s.workerPool == nil {
		writeRuntimeAPIError(w, http.StatusServiceUnavailable, "worker pool is not configured")
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeRuntimeAPIJSON(w, http.StatusOK, runtimeSnapshotStatuses(s.workerPool))
}

func (s *runtimeAPIHTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.workerPool == nil {
		writeRuntimeAPIError(w, http.StatusServiceUnavailable, "worker pool is not configured")
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	workerID, err := runtimeStorageWorkerID(r.URL.Path, "/api/snapshots/workers/")
	if err != nil {
		writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := resetRuntimeSnapshot(s.workerPool, workerID); err != nil {
		writeRuntimeAPIStorageError(w, err)
		return
	}
	writeRuntimeAPIJSON(w, http.StatusOK, runtimeStorageStatusResponse{WorkerID: workerID, Status: "reset"})
}

func (s *runtimeAPIHTTPServer) handleWorkspaces(w http.ResponseWriter, r *http.Request) {
	if s.workspaces == nil {
		writeRuntimeAPIError(w, http.StatusServiceUnavailable, "workspace manager is not configured")
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	workspaceMiB := int64(0)
	if s.workerPool != nil {
		cfg := s.workerPool.base.LocalPython
		if s.controlPlane != nil {
			if effective, err := s.controlPlane.ApplyToLocalPythonConfig(r.Context(), cfg); err == nil {
				cfg = effective
			}
		}
		workspaceMiB = cfg.WorkspaceMiB
	}
	writeRuntimeAPIJSON(w, http.StatusOK, s.workspaces.List(workspaceMiB))
}

func (s *runtimeAPIHTTPServer) handleWorkspace(w http.ResponseWriter, r *http.Request) {
	if s.workspaces == nil {
		writeRuntimeAPIError(w, http.StatusServiceUnavailable, "workspace manager is not configured")
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key, err := runtimeStorageWorkspaceKey(r.URL.Path)
	if err != nil {
		writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.workspaces.Delete(key); err != nil {
		writeRuntimeAPIStorageError(w, err)
		return
	}
	writeRuntimeAPIJSON(w, http.StatusOK, runtimeStorageStatusResponse{Key: key, Status: "reset"})
}

func (s *runtimeAPIHTTPServer) handleSettingsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	settings, err := s.controlPlane.ListSettings(r.Context())
	if err != nil {
		writeRuntimeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeRuntimeAPIJSON(w, http.StatusOK, controlSettingsListResponse{Settings: settings})
}

func (s *runtimeAPIHTTPServer) handleSetting(w http.ResponseWriter, r *http.Request) {
	key, err := runtimeAPISettingKey(r.URL.Path)
	if err != nil {
		writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch r.Method {
	case http.MethodGet:
		value, found, err := s.controlPlane.GetSetting(r.Context(), key)
		if err != nil {
			writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		response := controlSettingsGetResponse{Key: key, Found: found}
		if found {
			response.Value = value
		}
		writeRuntimeAPIJSON(w, http.StatusOK, response)
	case http.MethodPut:
		var payload struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.controlPlane.SetSetting(r.Context(), key, payload.Value); err != nil {
			writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeRuntimeAPIJSON(w, http.StatusOK, controlSettingsStatusResponse{Key: key, Status: "ok"})
	case http.MethodDelete:
		if err := s.controlPlane.DeleteSetting(r.Context(), key); err != nil {
			writeRuntimeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeRuntimeAPIJSON(w, http.StatusOK, controlSettingsStatusResponse{Key: key, Status: "deleted"})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *runtimeAPIHTTPServer) handleStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	indexPath := filepath.Join(s.webDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		http.Error(w, "frontend build is missing; run npm run build in web/", http.StatusNotFound)
		return
	}
	rel := strings.TrimPrefix(r.URL.Path, "/")
	if rel == "" {
		http.ServeFile(w, r, indexPath)
		return
	}
	cleanRel := filepath.Clean(filepath.FromSlash(rel))
	if cleanRel == "." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) || cleanRel == ".." || filepath.IsAbs(cleanRel) {
		http.NotFound(w, r)
		return
	}
	candidate := filepath.Join(s.webDir, cleanRel)
	if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
		http.ServeFile(w, r, candidate)
		return
	}
	http.ServeFile(w, r, indexPath)
}

func runtimeAPISettingKey(path string) (string, error) {
	key := strings.TrimPrefix(path, "/api/settings/")
	if key == "" || key == path {
		return "", fmt.Errorf("setting key must not be empty")
	}
	key, err := url.PathUnescape(key)
	if err != nil {
		return "", err
	}
	return key, nil
}

func writeRuntimeAPIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeRuntimeAPIError(w http.ResponseWriter, status int, message string) {
	writeRuntimeAPIJSON(w, status, map[string]string{"error": message})
}

func writeRuntimeAPIStorageError(w http.ResponseWriter, err error) {
	var notFound runtimeStorageNotFoundError
	if errors.As(err, &notFound) {
		writeRuntimeAPIError(w, http.StatusNotFound, err.Error())
		return
	}
	var conflict runtimeStorageConflictError
	if errors.As(err, &conflict) {
		writeRuntimeAPIError(w, http.StatusConflict, err.Error())
		return
	}
	writeRuntimeAPIError(w, http.StatusInternalServerError, err.Error())
}

func writeRuntimeAPISSE(w io.Writer, event string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	return err
}
