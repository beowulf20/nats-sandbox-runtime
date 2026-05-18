package app

import (
	"fmt"
	"path/filepath"
	"sync"
)

const runtimeWorkerIdle = "idle"
const runtimeWorkerBusy = "busy"

type RuntimeWorker struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	SnapshotDir  string `json:"snapshot_dir"`
	MemoryMiB    *int64 `json:"memory_mib,omitempty"`
	SwapMiB      *int64 `json:"swap_mib,omitempty"`
	WorkspaceMiB *int64 `json:"workspace_mib,omitempty"`
	ExecTimeout  string `json:"exec_timeout,omitempty"`
}

type RuntimeWorkerSnapshot struct {
	DesiredCount int             `json:"desired_count"`
	Workers      []RuntimeWorker `json:"workers"`
}

type RuntimeWorkerPool struct {
	mu           sync.Mutex
	base         RuntimePythonConfig
	nextID       int
	desiredCount int
	workers      []*RuntimeWorker
	byID         map[string]*RuntimeWorker
	inFlight     map[string]bool
	subscribers  map[chan RuntimeWorkerSnapshot]struct{}
}

func NewRuntimeWorkerPool(cfg RuntimePythonConfig) (*RuntimeWorkerPool, error) {
	if cfg.MaxParallel < 1 {
		return nil, fmt.Errorf("workers must be at least 1")
	}
	if err := validateLocalPythonConfig(cfg.LocalPython); err != nil {
		return nil, err
	}
	pool := &RuntimeWorkerPool{
		base:         cfg,
		nextID:       1,
		desiredCount: cfg.MaxParallel,
		byID:         map[string]*RuntimeWorker{},
		inFlight:     map[string]bool{},
		subscribers:  map[chan RuntimeWorkerSnapshot]struct{}{},
	}
	pool.reconcileWorkerCountLocked()
	return pool, nil
}

func (p *RuntimeWorkerPool) SetWorkerCount(count int) ([]RuntimeWorker, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if count < 1 {
		return nil, fmt.Errorf("worker count must be at least 1")
	}
	p.desiredCount = count
	p.reconcileWorkerCountLocked()
	snapshot := p.snapshotLocked()
	p.publishLocked(snapshot)
	return snapshot.Workers, nil
}

func (p *RuntimeWorkerPool) ListWorkers() []RuntimeWorker {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.listWorkersLocked()
}

func (p *RuntimeWorkerPool) Snapshot() RuntimeWorkerSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.snapshotLocked()
}

func (p *RuntimeWorkerPool) DesiredCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.desiredCount
}

func (p *RuntimeWorkerPool) Subscribe() (<-chan RuntimeWorkerSnapshot, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch := make(chan RuntimeWorkerSnapshot, 1)
	ch <- p.snapshotLocked()
	p.subscribers[ch] = struct{}{}
	cancel := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, found := p.subscribers[ch]; found {
			delete(p.subscribers, ch)
			close(ch)
		}
	}
	return ch, cancel
}

func (p *RuntimeWorkerPool) snapshotLocked() RuntimeWorkerSnapshot {
	return RuntimeWorkerSnapshot{
		DesiredCount: p.desiredCount,
		Workers:      p.listWorkersLocked(),
	}
}

func (p *RuntimeWorkerPool) listWorkersLocked() []RuntimeWorker {
	workers := make([]RuntimeWorker, 0, len(p.workers))
	for _, worker := range p.workers {
		workers = append(workers, cloneRuntimeWorker(worker))
	}
	return workers
}

func (p *RuntimeWorkerPool) AcquireWorker() (RuntimeWorker, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, worker := range p.workers {
		if !p.inFlight[worker.ID] {
			p.inFlight[worker.ID] = true
			worker.Status = runtimeWorkerBusy
			cloned := cloneRuntimeWorker(worker)
			p.publishLocked(p.snapshotLocked())
			return cloned, true
		}
	}
	return RuntimeWorker{}, false
}

func (p *RuntimeWorkerPool) ReleaseWorker(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if worker, found := p.byID[id]; found {
		delete(p.inFlight, id)
		if len(p.workers) > p.desiredCount {
			p.removeWorkerLocked(worker.ID)
			p.publishLocked(p.snapshotLocked())
			return
		}
		worker.Status = runtimeWorkerIdle
		p.reconcileWorkerCountLocked()
		p.publishLocked(p.snapshotLocked())
	}
}

func (p *RuntimeWorkerPool) publishLocked(snapshot RuntimeWorkerSnapshot) {
	for ch := range p.subscribers {
		select {
		case ch <- snapshot:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- snapshot:
			default:
			}
		}
	}
}

func (p *RuntimeWorkerPool) reconcileWorkerCountLocked() {
	for len(p.workers) < p.desiredCount {
		p.addDefaultWorkerLocked()
	}
	for len(p.workers) > p.desiredCount {
		id := p.lastIdleWorkerIDLocked()
		if id == "" {
			return
		}
		p.removeWorkerLocked(id)
	}
}

func (p *RuntimeWorkerPool) addDefaultWorkerLocked() {
	id := p.nextWorkerIDLocked()
	worker := &RuntimeWorker{
		ID:          id,
		Status:      runtimeWorkerIdle,
		SnapshotDir: p.defaultSnapshotDir(id),
	}
	p.workers = append(p.workers, worker)
	p.byID[worker.ID] = worker
}

func (p *RuntimeWorkerPool) lastIdleWorkerIDLocked() string {
	for i := len(p.workers) - 1; i >= 0; i-- {
		worker := p.workers[i]
		if !p.inFlight[worker.ID] {
			return worker.ID
		}
	}
	return ""
}

func (p *RuntimeWorkerPool) removeWorkerLocked(id string) {
	for i, worker := range p.workers {
		if worker.ID == id {
			p.workers = append(p.workers[:i], p.workers[i+1:]...)
			break
		}
	}
	delete(p.byID, id)
	delete(p.inFlight, id)
}

func (p *RuntimeWorkerPool) nextWorkerIDLocked() string {
	for {
		id := fmt.Sprintf("worker-%d", p.nextID)
		p.nextID++
		if _, found := p.byID[id]; !found {
			return id
		}
	}
}

func (p *RuntimeWorkerPool) defaultSnapshotDir(id string) string {
	return filepath.Join(p.base.LocalPython.SnapshotDir, "workers", id)
}

func cloneRuntimeWorker(worker *RuntimeWorker) RuntimeWorker {
	return RuntimeWorker{
		ID:           worker.ID,
		Status:       worker.Status,
		SnapshotDir:  worker.SnapshotDir,
		MemoryMiB:    cloneInt64Pointer(worker.MemoryMiB),
		SwapMiB:      cloneInt64Pointer(worker.SwapMiB),
		WorkspaceMiB: cloneInt64Pointer(worker.WorkspaceMiB),
		ExecTimeout:  worker.ExecTimeout,
	}
}
