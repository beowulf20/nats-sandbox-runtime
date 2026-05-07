package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

func TestInMemorySettingsStoreSetGetListDelete(t *testing.T) {
	store := NewInMemorySettingsStore()
	ctx := context.Background()

	if got, found, err := store.Get(ctx, "missing"); err != nil || found || got != nil {
		t.Fatalf("Get missing = (%s, %t, %v), want nil false nil", got, found, err)
	}

	value := json.RawMessage(`128`)
	if err := store.Set(ctx, "runtime.default_memory_mib", value); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	value[0] = '9'

	got, found, err := store.Get(ctx, "runtime.default_memory_mib")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !found || string(got) != "128" {
		t.Fatalf("Get = (%s, %t), want 128 true", got, found)
	}
	got[0] = '9'
	got, found, err = store.Get(ctx, "runtime.default_memory_mib")
	if err != nil {
		t.Fatalf("Get after mutation returned error: %v", err)
	}
	if !found || string(got) != "128" {
		t.Fatalf("Get after mutation = (%s, %t), want copied 128 true", got, found)
	}

	if err := store.Set(ctx, "feature.flag", json.RawMessage(`true`)); err != nil {
		t.Fatalf("Set second value returned error: %v", err)
	}
	settings, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(settings) != 2 {
		t.Fatalf("List returned %d settings, want 2: %#v", len(settings), settings)
	}
	if settings[0].Key != "feature.flag" || string(settings[0].Value) != "true" {
		t.Fatalf("settings[0] = %#v, want feature.flag true", settings[0])
	}
	if settings[1].Key != "runtime.default_memory_mib" || string(settings[1].Value) != "128" {
		t.Fatalf("settings[1] = %#v, want runtime.default_memory_mib 128", settings[1])
	}

	if err := store.Delete(ctx, "runtime.default_memory_mib"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if got, found, err := store.Get(ctx, "runtime.default_memory_mib"); err != nil || found || got != nil {
		t.Fatalf("Get deleted = (%s, %t, %v), want nil false nil", got, found, err)
	}
}

func TestRuntimeControlPlaneValidatesSettings(t *testing.T) {
	control := NewRuntimeControlPlane(NewInMemorySettingsStore())
	ctx := context.Background()

	for _, key := range []string{"", "bad key", "bad/key", "bad$key"} {
		if err := control.SetSetting(ctx, key, json.RawMessage(`128`)); err == nil {
			t.Fatalf("SetSetting(%q) returned nil, want invalid key error", key)
		}
		if _, _, err := control.GetSetting(ctx, key); err == nil {
			t.Fatalf("GetSetting(%q) returned nil, want invalid key error", key)
		}
		if err := control.DeleteSetting(ctx, key); err == nil {
			t.Fatalf("DeleteSetting(%q) returned nil, want invalid key error", key)
		}
	}

	if err := control.SetSetting(ctx, "runtime.default_memory_mib", json.RawMessage(`{`)); err == nil {
		t.Fatal("SetSetting invalid JSON returned nil, want error")
	}
	if err := control.SetSetting(ctx, "runtime.default_memory_mib", nil); err == nil {
		t.Fatal("SetSetting missing JSON returned nil, want error")
	}
	if err := control.SetSetting(ctx, "runtime.default_memory_mib", json.RawMessage(`null`)); err == nil {
		t.Fatal("SetSetting null returned nil, want validation error")
	}
}

func TestRuntimeControlPlaneListsDiscoverableEffectiveSettings(t *testing.T) {
	cfg := defaultLocalPythonConfig()
	cfg.MemoryMiB = 256
	cfg.SwapMiB = 32
	cfg.WorkspaceMiB = 64
	cfg.ExecTimeout = 20 * time.Second
	control := NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), cfg)
	ctx := context.Background()

	settings, err := control.ListSettings(ctx)
	if err != nil {
		t.Fatalf("ListSettings returned error: %v", err)
	}
	if len(settings) != 4 {
		t.Fatalf("ListSettings returned %d settings, want 4: %#v", len(settings), settings)
	}
	memory := requireSetting(t, settings, "runtime.default_memory_mib")
	if string(memory.Value) != "256" || string(memory.DefaultValue) != "256" || memory.Source != "default" || memory.Type != "integer" {
		t.Fatalf("memory setting = %#v, want default integer 256", memory)
	}
	if memory.Min == nil || *memory.Min != 1 {
		t.Fatalf("memory min = %#v, want 1", memory.Min)
	}
	timeout := requireSetting(t, settings, "runtime.default_exec_timeout")
	if string(timeout.Value) != `"20s"` || timeout.Type != "duration" {
		t.Fatalf("timeout setting = %#v, want duration 20s", timeout)
	}

	if err := control.SetSetting(ctx, "runtime.default_memory_mib", json.RawMessage(`512`)); err != nil {
		t.Fatalf("SetSetting memory returned error: %v", err)
	}
	value, found, err := control.GetSetting(ctx, "runtime.default_memory_mib")
	if err != nil {
		t.Fatalf("GetSetting memory returned error: %v", err)
	}
	if !found || string(value) != "512" {
		t.Fatalf("GetSetting memory = %s found %t, want 512 true", value, found)
	}
	settings, err = control.ListSettings(ctx)
	if err != nil {
		t.Fatalf("ListSettings after override returned error: %v", err)
	}
	memory = requireSetting(t, settings, "runtime.default_memory_mib")
	if string(memory.Value) != "512" || string(memory.DefaultValue) != "256" || memory.Source != "override" {
		t.Fatalf("memory override = %#v, want override value 512 default 256", memory)
	}

	if err := control.DeleteSetting(ctx, "runtime.default_memory_mib"); err != nil {
		t.Fatalf("DeleteSetting memory returned error: %v", err)
	}
	value, found, err = control.GetSetting(ctx, "runtime.default_memory_mib")
	if err != nil {
		t.Fatalf("GetSetting reset memory returned error: %v", err)
	}
	if !found || string(value) != "256" {
		t.Fatalf("GetSetting reset memory = %s found %t, want default 256 true", value, found)
	}
}

func TestRuntimeControlPlaneRejectsUnknownAndInvalidEffectiveSettings(t *testing.T) {
	control := NewRuntimeControlPlane(NewInMemorySettingsStore())
	ctx := context.Background()

	for _, key := range []string{"unknown.setting", "bad key"} {
		if err := control.SetSetting(ctx, key, json.RawMessage(`1`)); err == nil {
			t.Fatalf("SetSetting(%q) returned nil, want error", key)
		}
		if _, _, err := control.GetSetting(ctx, key); err == nil {
			t.Fatalf("GetSetting(%q) returned nil, want error", key)
		}
		if err := control.DeleteSetting(ctx, key); err == nil {
			t.Fatalf("DeleteSetting(%q) returned nil, want error", key)
		}
	}
	for _, test := range []struct {
		key   string
		value string
	}{
		{key: "runtime.default_memory_mib", value: `0`},
		{key: "runtime.default_memory_mib", value: `"128"`},
		{key: "runtime.default_swap_mib", value: `-1`},
		{key: "runtime.default_workspace_mib", value: `0`},
		{key: "runtime.default_exec_timeout", value: `"bogus"`},
		{key: "runtime.default_exec_timeout", value: `"0s"`},
	} {
		if err := control.SetSetting(ctx, test.key, json.RawMessage(test.value)); err == nil {
			t.Fatalf("SetSetting(%q, %s) returned nil, want validation error", test.key, test.value)
		}
	}
}

func TestRuntimePythonControlSettingsHandlers(t *testing.T) {
	service := &runtimePythonService{
		controlPlane: NewRuntimeControlPlane(NewInMemorySettingsStore()),
	}

	setReq := newRuntimePythonFakeRequest([]byte(`{"key":"runtime.default_memory_mib","value":256}`))
	service.handleControlSettingsSet(setReq)
	requireNoServiceError(t, setReq)
	requireJSONEqual(t, setReq.response, `{"key":"runtime.default_memory_mib","status":"ok"}`)

	getReq := newRuntimePythonFakeRequest([]byte(`{"key":"runtime.default_memory_mib"}`))
	service.handleControlSettingsGet(getReq)
	requireNoServiceError(t, getReq)
	requireJSONEqual(t, getReq.response, `{"key":"runtime.default_memory_mib","found":true,"value":256}`)

	listReq := newRuntimePythonFakeRequest(nil)
	service.handleControlSettingsList(listReq)
	requireNoServiceError(t, listReq)
	var listResp controlSettingsListResponse
	if err := json.Unmarshal(listReq.response, &listResp); err != nil {
		t.Fatalf("unmarshal list response returned error: %v: %s", err, listReq.response)
	}
	if len(listResp.Settings) != 4 {
		t.Fatalf("list response returned %d settings, want 4: %#v", len(listResp.Settings), listResp.Settings)
	}
	memory := requireSetting(t, listResp.Settings, "runtime.default_memory_mib")
	if string(memory.Value) != "256" || memory.Source != "override" || memory.Type != "integer" {
		t.Fatalf("memory setting = %#v, want override integer 256", memory)
	}

	deleteReq := newRuntimePythonFakeRequest([]byte(`{"key":"runtime.default_memory_mib"}`))
	service.handleControlSettingsDelete(deleteReq)
	requireNoServiceError(t, deleteReq)
	requireJSONEqual(t, deleteReq.response, `{"key":"runtime.default_memory_mib","status":"deleted"}`)

	resetReq := newRuntimePythonFakeRequest([]byte(`{"key":"runtime.default_memory_mib"}`))
	service.handleControlSettingsGet(resetReq)
	requireNoServiceError(t, resetReq)
	requireJSONEqual(t, resetReq.response, `{"key":"runtime.default_memory_mib","found":true,"value":128}`)
}

func TestRuntimePythonControlSettingsHandlersRejectInvalidRequests(t *testing.T) {
	service := &runtimePythonService{
		controlPlane: NewRuntimeControlPlane(NewInMemorySettingsStore()),
	}

	setInvalidJSON := newRuntimePythonFakeRequest([]byte(`{"key":"runtime.default_memory_mib","value":`))
	service.handleControlSettingsSet(setInvalidJSON)
	requireServiceError(t, setInvalidJSON, "400")

	setMissingValue := newRuntimePythonFakeRequest([]byte(`{"key":"runtime.default_memory_mib"}`))
	service.handleControlSettingsSet(setMissingValue)
	requireServiceError(t, setMissingValue, "400")

	getInvalidKey := newRuntimePythonFakeRequest([]byte(`{"key":"bad key"}`))
	service.handleControlSettingsGet(getInvalidKey)
	requireServiceError(t, getInvalidKey, "400")
}

func TestRuntimePythonControlWorkerHandlers(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = t.TempDir()
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	service := &runtimePythonService{workerPool: pool}

	listReq := newRuntimePythonFakeRequest(nil)
	service.handleControlWorkersList(listReq)
	requireNoServiceError(t, listReq)
	var listResp controlWorkersListResponse
	if err := json.Unmarshal(listReq.response, &listResp); err != nil {
		t.Fatalf("unmarshal workers list returned error: %v: %s", err, listReq.response)
	}
	if len(listResp.Workers) != 1 || listResp.Workers[0].ID != "worker-1" {
		t.Fatalf("workers list = %#v, want initial worker-1", listResp.Workers)
	}

	setReq := newRuntimePythonFakeRequest([]byte(`{"count":3}`))
	service.handleControlWorkersSet(setReq)
	requireNoServiceError(t, setReq)
	var setResp controlWorkersSetResponse
	if err := json.Unmarshal(setReq.response, &setResp); err != nil {
		t.Fatalf("unmarshal workers set returned error: %v: %s", err, setReq.response)
	}
	if setResp.Status != "ok" || setResp.DesiredCount != 3 || len(setResp.Workers) != 3 {
		t.Fatalf("set response = %#v, want ok desired count 3 with workers", setResp)
	}
}

func TestRuntimePythonControlWorkerHandlersRejectInvalidRequests(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	service := &runtimePythonService{workerPool: pool}

	setInvalidJSON := newRuntimePythonFakeRequest([]byte(`{"count":`))
	service.handleControlWorkersSet(setInvalidJSON)
	requireServiceError(t, setInvalidJSON, "400")

	setInvalidCount := newRuntimePythonFakeRequest([]byte(`{"count":0}`))
	service.handleControlWorkersSet(setInvalidCount)
	requireServiceError(t, setInvalidCount, "400")

	listInvalid := newRuntimePythonFakeRequest([]byte(`{"extra":true}`))
	service.handleControlWorkersList(listInvalid)
	requireServiceError(t, listInvalid, "400")
}

type runtimePythonFakeRequest struct {
	data             []byte
	response         []byte
	errorCode        string
	errorDescription string
	errorData        []byte
	headers          micro.Headers
}

func newRuntimePythonFakeRequest(data []byte) *runtimePythonFakeRequest {
	return &runtimePythonFakeRequest{data: data, headers: micro.Headers{}}
}

func (r *runtimePythonFakeRequest) Respond(data []byte, opts ...micro.RespondOpt) error {
	msg := &nats.Msg{Data: data}
	for _, opt := range opts {
		opt(msg)
	}
	r.response = msg.Data
	r.headers = micro.Headers(msg.Header)
	return nil
}

func (r *runtimePythonFakeRequest) RespondJSON(value any, opts ...micro.RespondOpt) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.Respond(data, opts...)
}

func (r *runtimePythonFakeRequest) Error(code, description string, data []byte, opts ...micro.RespondOpt) error {
	msg := &nats.Msg{Data: data}
	for _, opt := range opts {
		opt(msg)
	}
	r.errorCode = code
	r.errorDescription = description
	r.errorData = msg.Data
	r.headers = micro.Headers(msg.Header)
	return nil
}

func (r *runtimePythonFakeRequest) Data() []byte {
	return r.data
}

func (r *runtimePythonFakeRequest) Headers() micro.Headers {
	return r.headers
}

func (r *runtimePythonFakeRequest) Subject() string {
	return "test"
}

func (r *runtimePythonFakeRequest) Reply() string {
	return "reply"
}

func requireNoServiceError(t *testing.T, req *runtimePythonFakeRequest) {
	t.Helper()
	if req.errorCode != "" || req.errorDescription != "" {
		t.Fatalf("service error = code %q description %q data %q, want none", req.errorCode, req.errorDescription, req.errorData)
	}
}

func requireServiceError(t *testing.T, req *runtimePythonFakeRequest, code string) {
	t.Helper()
	if req.errorCode != code {
		t.Fatalf("service error code = %q description %q data %q, want %q", req.errorCode, req.errorDescription, req.errorData, code)
	}
}

func requireJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal got returned error: %v: %s", err, got)
	}
	var wantValue any
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("unmarshal want returned error: %v: %s", err, want)
	}
	if !jsonValuesEqual(gotValue, wantValue) {
		t.Fatalf("response = %s, want JSON %s", got, want)
	}
}

func requireSetting(t *testing.T, settings []Setting, key string) Setting {
	t.Helper()
	for _, setting := range settings {
		if setting.Key == key {
			return setting
		}
	}
	t.Fatalf("setting %q not found in %#v", key, settings)
	return Setting{}
}

func jsonValuesEqual(a, b any) bool {
	return jsonString(a) == jsonString(b)
}

func jsonString(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "<invalid>"
	}
	return string(data)
}
