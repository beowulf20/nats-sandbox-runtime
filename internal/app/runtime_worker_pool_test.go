package app

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeWorkerPoolInitialWorkersDispatchFirstAvailable(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 2
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")

	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}

	workers := pool.ListWorkers()
	if len(workers) != 2 {
		t.Fatalf("ListWorkers returned %d workers, want 2: %#v", len(workers), workers)
	}
	if workers[0].ID != "worker-1" || workers[0].Status != "idle" {
		t.Fatalf("workers[0] = %#v, want idle worker-1", workers[0])
	}
	if workers[0].SnapshotDir != filepath.Join(cfg.LocalPython.SnapshotDir, "workers", "worker-1") {
		t.Fatalf("worker-1 snapshot = %q, want configured worker snapshot", workers[0].SnapshotDir)
	}

	first, ok := pool.AcquireWorker()
	if !ok {
		t.Fatal("AcquireWorker first returned false, want worker")
	}
	if first.ID != "worker-1" {
		t.Fatalf("first worker = %q, want worker-1", first.ID)
	}
	second, ok := pool.AcquireWorker()
	if !ok {
		t.Fatal("AcquireWorker second returned false, want worker")
	}
	if second.ID != "worker-2" {
		t.Fatalf("second worker = %q, want worker-2", second.ID)
	}
	if _, ok := pool.AcquireWorker(); ok {
		t.Fatal("AcquireWorker third returned true, want pool busy")
	}

	pool.ReleaseWorker(first.ID)
	reacquired, ok := pool.AcquireWorker()
	if !ok {
		t.Fatal("AcquireWorker after release returned false, want worker")
	}
	if reacquired.ID != "worker-1" {
		t.Fatalf("reacquired worker = %q, want first available worker-1", reacquired.ID)
	}
}

func TestRuntimeWorkerPoolSetsDesiredWorkerCount(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	cfg.LocalPython.SnapshotDir = filepath.Join(t.TempDir(), "snapshots")

	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}

	workers, err := pool.SetWorkerCount(3)
	if err != nil {
		t.Fatalf("SetWorkerCount grow returned error: %v", err)
	}
	if len(workers) != 3 {
		t.Fatalf("SetWorkerCount grow returned %d workers, want 3: %#v", len(workers), workers)
	}
	if workers[2].ID != "worker-3" || workers[2].SnapshotDir != filepath.Join(cfg.LocalPython.SnapshotDir, "workers", "worker-3") {
		t.Fatalf("workers[2] = %#v, want worker-3 with default snapshot", workers[2])
	}

	workers, err = pool.SetWorkerCount(1)
	if err != nil {
		t.Fatalf("SetWorkerCount shrink returned error: %v", err)
	}
	if len(workers) != 1 || workers[0].ID != "worker-1" {
		t.Fatalf("SetWorkerCount shrink workers = %#v, want only worker-1", workers)
	}

	if _, err := pool.SetWorkerCount(0); err == nil {
		t.Fatal("SetWorkerCount(0) returned nil, want error")
	}
}

func TestRuntimeWorkerPoolShrinksAfterBusyWorkerRelease(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 2
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	first, ok := pool.AcquireWorker()
	if !ok {
		t.Fatal("AcquireWorker first returned false, want worker")
	}
	second, ok := pool.AcquireWorker()
	if !ok {
		t.Fatal("AcquireWorker second returned false, want worker")
	}

	workers, err := pool.SetWorkerCount(1)
	if err != nil {
		t.Fatalf("SetWorkerCount shrink busy returned error: %v", err)
	}
	if len(workers) != 2 {
		t.Fatalf("SetWorkerCount while busy returned %d workers, want busy workers retained", len(workers))
	}

	pool.ReleaseWorker(second.ID)
	workers = pool.ListWorkers()
	if len(workers) != 1 || workers[0].ID != first.ID || workers[0].Status != runtimeWorkerBusy {
		t.Fatalf("workers after releasing excess busy worker = %#v, want only busy %s", workers, first.ID)
	}

	pool.ReleaseWorker(first.ID)
	workers = pool.ListWorkers()
	if len(workers) != 1 || workers[0].ID != first.ID || workers[0].Status != runtimeWorkerIdle {
		t.Fatalf("workers after final release = %#v, want idle desired worker", workers)
	}
}

func TestRuntimeWorkerPoolPublishesSnapshotsOnStatusChanges(t *testing.T) {
	cfg := defaultRuntimePythonConfig()
	cfg.MaxParallel = 1
	pool, err := NewRuntimeWorkerPool(cfg)
	if err != nil {
		t.Fatalf("NewRuntimeWorkerPool returned error: %v", err)
	}
	events, cancel := pool.Subscribe()
	defer cancel()

	initial := receiveWorkerSnapshot(t, events)
	if initial.DesiredCount != 1 || len(initial.Workers) != 1 || initial.Workers[0].Status != runtimeWorkerIdle {
		t.Fatalf("initial snapshot = %#v, want one idle worker", initial)
	}

	worker, ok := pool.AcquireWorker()
	if !ok {
		t.Fatal("AcquireWorker returned false, want worker")
	}
	busy := receiveWorkerSnapshot(t, events)
	if len(busy.Workers) != 1 || busy.Workers[0].Status != runtimeWorkerBusy {
		t.Fatalf("busy snapshot = %#v, want one busy worker", busy)
	}

	pool.ReleaseWorker(worker.ID)
	idle := receiveWorkerSnapshot(t, events)
	if len(idle.Workers) != 1 || idle.Workers[0].Status != runtimeWorkerIdle {
		t.Fatalf("idle snapshot = %#v, want one idle worker", idle)
	}
}

func TestRuntimePythonLocalConfigForRunUsesWorkerSnapshotAndOverrides(t *testing.T) {
	baseCfg := defaultLocalPythonConfig()
	baseCfg.MemoryMiB = 128
	baseCfg.SwapMiB = 0
	baseCfg.WorkspaceMiB = 16
	baseCfg.ExecTimeout = 5 * time.Second
	control := NewRuntimeControlPlaneWithConfig(NewInMemorySettingsStore(), baseCfg)
	ctx := context.Background()
	if err := control.SetSetting(ctx, "runtime.default_memory_mib", json.RawMessage(`256`)); err != nil {
		t.Fatalf("SetSetting memory returned error: %v", err)
	}
	service := &runtimePythonService{
		cfg:          RuntimePythonConfig{LocalPython: baseCfg},
		controlPlane: control,
	}
	worker := RuntimeWorker{
		ID:          "burst-1",
		SnapshotDir: "/runtime/workers/burst-1/snapshot",
		MemoryMiB:   int64Pointer(384),
		SwapMiB:     int64Pointer(32),
	}

	got, err := service.localPythonConfigForRun(worker, PythonRunRequest{
		WorkspaceMiB: 64,
		ExecTimeout:  "20s",
	}, "/tmp/run", "/tmp/run/workspace", "", "print(42)")
	if err != nil {
		t.Fatalf("localPythonConfigForRun returned error: %v", err)
	}

	if got.SnapshotDir != "/runtime/workers/burst-1/snapshot" {
		t.Fatalf("SnapshotDir = %q, want worker snapshot", got.SnapshotDir)
	}
	if got.MemoryMiB != 384 || got.SwapMiB != 32 || got.WorkspaceMiB != 64 || got.ExecTimeout != 20*time.Second {
		t.Fatalf("resources = memory:%d swap:%d workspace:%d timeout:%s, want setting < worker < request precedence", got.MemoryMiB, got.SwapMiB, got.WorkspaceMiB, got.ExecTimeout)
	}
}

func receiveWorkerSnapshot(t *testing.T, events <-chan RuntimeWorkerSnapshot) RuntimeWorkerSnapshot {
	t.Helper()
	select {
	case snapshot := <-events:
		return snapshot
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for worker snapshot")
		return RuntimeWorkerSnapshot{}
	}
}
