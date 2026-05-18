package app

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go/micro"
)

var settingKeyRegexp = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

type Setting struct {
	Key          string          `json:"key"`
	Label        string          `json:"label,omitempty"`
	Description  string          `json:"description,omitempty"`
	Type         string          `json:"type,omitempty"`
	Value        json.RawMessage `json:"value"`
	DefaultValue json.RawMessage `json:"default_value,omitempty"`
	Source       string          `json:"source,omitempty"`
	Min          *int64          `json:"min,omitempty"`
}

type SettingsStore interface {
	Get(ctx context.Context, key string) (json.RawMessage, bool, error)
	Set(ctx context.Context, key string, value json.RawMessage) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context) ([]Setting, error)
}

type InMemorySettingsStore struct {
	mu       sync.RWMutex
	settings map[string]json.RawMessage
}

func NewInMemorySettingsStore() *InMemorySettingsStore {
	return &InMemorySettingsStore{settings: map[string]json.RawMessage{}}
}

func (s *InMemorySettingsStore) Get(_ context.Context, key string) (json.RawMessage, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, found := s.settings[key]
	if !found {
		return nil, false, nil
	}
	return cloneRawJSON(value), true, nil
}

func (s *InMemorySettingsStore) Set(_ context.Context, key string, value json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.settings[key] = cloneRawJSON(value)
	return nil
}

func (s *InMemorySettingsStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.settings, key)
	return nil
}

func (s *InMemorySettingsStore) List(_ context.Context) ([]Setting, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	settings := make([]Setting, 0, len(s.settings))
	for key, value := range s.settings {
		settings = append(settings, Setting{Key: key, Value: cloneRawJSON(value)})
	}
	sort.Slice(settings, func(i, j int) bool {
		return settings[i].Key < settings[j].Key
	})
	return settings, nil
}

type RuntimeControlPlane struct {
	settings    SettingsStore
	definitions []runtimeSettingDefinition
}

func NewRuntimeControlPlane(settings SettingsStore) *RuntimeControlPlane {
	return NewRuntimeControlPlaneWithConfig(settings, defaultLocalPythonConfig())
}

func NewRuntimeControlPlaneWithConfig(settings SettingsStore, cfg LocalPythonConfig) *RuntimeControlPlane {
	if settings == nil {
		settings = NewInMemorySettingsStore()
	}
	return &RuntimeControlPlane{
		settings:    settings,
		definitions: runtimeSettingDefinitions(cfg),
	}
}

func (p *RuntimeControlPlane) GetSetting(ctx context.Context, key string) (json.RawMessage, bool, error) {
	definition, err := p.settingDefinition(key)
	if err != nil {
		return nil, false, err
	}
	if value, found, err := p.settings.Get(ctx, key); err != nil || found {
		return value, found, err
	}
	return cloneRawJSON(definition.DefaultValue), true, nil
}

func (p *RuntimeControlPlane) SetSetting(ctx context.Context, key string, value json.RawMessage) error {
	definition, err := p.settingDefinition(key)
	if err != nil {
		return err
	}
	if err := definition.Validate(value); err != nil {
		return err
	}
	return p.settings.Set(ctx, key, value)
}

func (p *RuntimeControlPlane) DeleteSetting(ctx context.Context, key string) error {
	if _, err := p.settingDefinition(key); err != nil {
		return err
	}
	return p.settings.Delete(ctx, key)
}

func (p *RuntimeControlPlane) ListSettings(ctx context.Context) ([]Setting, error) {
	settings := make([]Setting, 0, len(p.definitions))
	for _, definition := range p.definitions {
		value, source, err := p.effectiveSetting(ctx, definition)
		if err != nil {
			return nil, err
		}
		settings = append(settings, definition.Setting(value, source))
	}
	return settings, nil
}

func validateSettingKey(key string) error {
	if key == "" {
		return fmt.Errorf("setting key must not be empty")
	}
	if !settingKeyRegexp.MatchString(key) {
		return fmt.Errorf("setting key %q must match [A-Za-z0-9_.-]+", key)
	}
	return nil
}

func (p *RuntimeControlPlane) ApplyToLocalPythonConfig(ctx context.Context, cfg LocalPythonConfig) (LocalPythonConfig, error) {
	for _, definition := range p.definitions {
		value, _, err := p.effectiveSetting(ctx, definition)
		if err != nil {
			return LocalPythonConfig{}, err
		}
		if err := definition.Apply(value, &cfg); err != nil {
			return LocalPythonConfig{}, err
		}
	}
	return cfg, nil
}

func (p *RuntimeControlPlane) settingDefinition(key string) (runtimeSettingDefinition, error) {
	if err := validateSettingKey(key); err != nil {
		return runtimeSettingDefinition{}, err
	}
	for _, definition := range p.definitions {
		if definition.Key == key {
			return definition, nil
		}
	}
	return runtimeSettingDefinition{}, fmt.Errorf("unknown runtime setting %q", key)
}

func (p *RuntimeControlPlane) effectiveSetting(ctx context.Context, definition runtimeSettingDefinition) (json.RawMessage, string, error) {
	value, found, err := p.settings.Get(ctx, definition.Key)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return cloneRawJSON(definition.DefaultValue), "default", nil
	}
	if err := definition.Validate(value); err != nil {
		return nil, "", err
	}
	return value, "override", nil
}

func cloneRawJSON(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}

type runtimeSettingDefinition struct {
	Key          string
	Label        string
	Description  string
	Type         string
	DefaultValue json.RawMessage
	Min          *int64
	Validate     func(json.RawMessage) error
	Apply        func(json.RawMessage, *LocalPythonConfig) error
}

func (d runtimeSettingDefinition) Setting(value json.RawMessage, source string) Setting {
	return Setting{
		Key:          d.Key,
		Label:        d.Label,
		Description:  d.Description,
		Type:         d.Type,
		Value:        cloneRawJSON(value),
		DefaultValue: cloneRawJSON(d.DefaultValue),
		Source:       source,
		Min:          cloneInt64Pointer(d.Min),
	}
}

func runtimeSettingDefinitions(cfg LocalPythonConfig) []runtimeSettingDefinition {
	return []runtimeSettingDefinition{
		{
			Key:          "runtime.default_memory_mib",
			Label:        "Default memory",
			Description:  "Default guest memory for future Python runs when a request does not override memory_mib.",
			Type:         "integer",
			DefaultValue: mustRawJSON(cfg.MemoryMiB),
			Min:          int64Pointer(1),
			Validate:     validateIntegerSetting("runtime.default_memory_mib", 1),
			Apply: func(value json.RawMessage, cfg *LocalPythonConfig) error {
				parsed, err := parseIntegerSetting("runtime.default_memory_mib", value, 1)
				if err != nil {
					return err
				}
				cfg.MemoryMiB = parsed
				return nil
			},
		},
		{
			Key:          "runtime.default_swap_mib",
			Label:        "Default swap",
			Description:  "Default guest swap size for future Python runs when a request does not override swap_mib.",
			Type:         "integer",
			DefaultValue: mustRawJSON(cfg.SwapMiB),
			Min:          int64Pointer(0),
			Validate:     validateIntegerSetting("runtime.default_swap_mib", 0),
			Apply: func(value json.RawMessage, cfg *LocalPythonConfig) error {
				parsed, err := parseIntegerSetting("runtime.default_swap_mib", value, 0)
				if err != nil {
					return err
				}
				cfg.SwapMiB = parsed
				return nil
			},
		},
		{
			Key:          "runtime.default_workspace_mib",
			Label:        "Default workspace",
			Description:  "Default writable workspace size for future Python runs when a request does not override workspace_mib.",
			Type:         "integer",
			DefaultValue: mustRawJSON(cfg.WorkspaceMiB),
			Min:          int64Pointer(1),
			Validate:     validateIntegerSetting("runtime.default_workspace_mib", 1),
			Apply: func(value json.RawMessage, cfg *LocalPythonConfig) error {
				parsed, err := parseIntegerSetting("runtime.default_workspace_mib", value, 1)
				if err != nil {
					return err
				}
				cfg.WorkspaceMiB = parsed
				return nil
			},
		},
		{
			Key:          "runtime.default_exec_timeout",
			Label:        "Default exec timeout",
			Description:  "Default maximum Python execution duration for future runs when a request does not override exec_timeout.",
			Type:         "duration",
			DefaultValue: mustRawJSON(cfg.ExecTimeout.String()),
			Validate:     validateDurationSetting("runtime.default_exec_timeout"),
			Apply: func(value json.RawMessage, cfg *LocalPythonConfig) error {
				parsed, err := parseDurationSetting("runtime.default_exec_timeout", value)
				if err != nil {
					return err
				}
				cfg.ExecTimeout = parsed
				return nil
			},
		},
	}
}

func validateIntegerSetting(key string, min int64) func(json.RawMessage) error {
	return func(value json.RawMessage) error {
		_, err := parseIntegerSetting(key, value, min)
		return err
	}
}

func parseIntegerSetting(key string, value json.RawMessage, min int64) (int64, error) {
	if !json.Valid(value) {
		return 0, fmt.Errorf("setting value must be valid JSON")
	}
	var parsed int64
	if err := json.Unmarshal(value, &parsed); err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if parsed < min {
		return 0, fmt.Errorf("%s must be at least %d", key, min)
	}
	return parsed, nil
}

func validateDurationSetting(key string) func(json.RawMessage) error {
	return func(value json.RawMessage) error {
		_, err := parseDurationSetting(key, value)
		return err
	}
}

func parseDurationSetting(key string, value json.RawMessage) (time.Duration, error) {
	if !json.Valid(value) {
		return 0, fmt.Errorf("setting value must be valid JSON")
	}
	var raw string
	if err := json.Unmarshal(value, &raw); err != nil {
		return 0, fmt.Errorf("%s must be a duration string: %w", key, err)
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}
	return parsed, nil
}

func mustRawJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func int64Pointer(value int64) *int64 {
	return &value
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

type controlSettingsKeyRequest struct {
	Key string `json:"key"`
}

type controlSettingsSetRequest struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type controlSettingsGetResponse struct {
	Key   string          `json:"key"`
	Found bool            `json:"found"`
	Value json.RawMessage `json:"value,omitempty"`
}

type controlSettingsStatusResponse struct {
	Key    string `json:"key"`
	Status string `json:"status"`
}

type controlSettingsListResponse struct {
	Settings []Setting `json:"settings"`
}

type controlWorkersSetRequest struct {
	Count int `json:"count"`
}

type controlWorkersSetResponse struct {
	Status       string          `json:"status"`
	DesiredCount int             `json:"desired_count"`
	Workers      []RuntimeWorker `json:"workers"`
}

type controlWorkersListResponse = RuntimeWorkerSnapshot

func (s *runtimePythonService) handleControlSettingsGet(req micro.Request) {
	var payload controlSettingsKeyRequest
	if err := decodeControlSettingsRequest(req.Data(), &payload); err != nil {
		_ = req.Error("400", "invalid control settings get request", []byte(err.Error()))
		return
	}
	value, found, err := s.controlPlane.GetSetting(context.Background(), payload.Key)
	if err != nil {
		_ = req.Error("400", err.Error(), nil)
		return
	}
	response := controlSettingsGetResponse{Key: payload.Key, Found: found}
	if found {
		response.Value = value
	}
	_ = req.RespondJSON(response)
}

func (s *runtimePythonService) handleControlSettingsSet(req micro.Request) {
	var payload controlSettingsSetRequest
	if err := decodeControlSettingsRequest(req.Data(), &payload); err != nil {
		_ = req.Error("400", "invalid control settings set request", []byte(err.Error()))
		return
	}
	if err := s.controlPlane.SetSetting(context.Background(), payload.Key, payload.Value); err != nil {
		_ = req.Error("400", err.Error(), nil)
		return
	}
	_ = req.RespondJSON(controlSettingsStatusResponse{Key: payload.Key, Status: "ok"})
}

func (s *runtimePythonService) handleControlSettingsDelete(req micro.Request) {
	var payload controlSettingsKeyRequest
	if err := decodeControlSettingsRequest(req.Data(), &payload); err != nil {
		_ = req.Error("400", "invalid control settings delete request", []byte(err.Error()))
		return
	}
	if err := s.controlPlane.DeleteSetting(context.Background(), payload.Key); err != nil {
		_ = req.Error("400", err.Error(), nil)
		return
	}
	_ = req.RespondJSON(controlSettingsStatusResponse{Key: payload.Key, Status: "deleted"})
}

func (s *runtimePythonService) handleControlSettingsList(req micro.Request) {
	if len(req.Data()) > 0 {
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(req.Data(), &payload); err != nil {
			_ = req.Error("400", "invalid control settings list request", []byte(err.Error()))
			return
		}
		if len(payload) > 0 {
			_ = req.Error("400", "control settings list request must be empty", nil)
			return
		}
	}
	settings, err := s.controlPlane.ListSettings(context.Background())
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.RespondJSON(controlSettingsListResponse{Settings: settings})
}

func decodeControlSettingsRequest(data []byte, payload any) error {
	if len(data) == 0 {
		return fmt.Errorf("request body must not be empty")
	}
	return json.Unmarshal(data, payload)
}

func (s *runtimePythonService) handleControlWorkersSet(req micro.Request) {
	var payload controlWorkersSetRequest
	if err := decodeControlSettingsRequest(req.Data(), &payload); err != nil {
		_ = req.Error("400", "invalid control workers set request", []byte(err.Error()))
		return
	}
	pool, err := s.ensureWorkerPool()
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	workers, err := pool.SetWorkerCount(payload.Count)
	if err != nil {
		_ = req.Error("400", err.Error(), nil)
		return
	}
	_ = req.RespondJSON(controlWorkersSetResponse{Status: "ok", DesiredCount: pool.DesiredCount(), Workers: workers})
}

func (s *runtimePythonService) handleControlWorkersList(req micro.Request) {
	if len(req.Data()) > 0 {
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(req.Data(), &payload); err != nil {
			_ = req.Error("400", "invalid control workers list request", []byte(err.Error()))
			return
		}
		if len(payload) > 0 {
			_ = req.Error("400", "control workers list request must be empty", nil)
			return
		}
	}
	pool, err := s.ensureWorkerPool()
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.RespondJSON(pool.Snapshot())
}

func (s *runtimePythonService) ensureWorkerPool() (*RuntimeWorkerPool, error) {
	if s.workerPool != nil {
		return s.workerPool, nil
	}
	pool, err := NewRuntimeWorkerPool(s.cfg)
	if err != nil {
		return nil, err
	}
	s.workerPool = pool
	return pool, nil
}
