// Package lease binds scheduled tasks to worker leases with expiry and takeover.
package lease

import (
	"errors"
	"sync"
	"time"

	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/idgen"
	"github.com/dyl-04/sched/internal/metrics"
	"github.com/dyl-04/sched/internal/model"
	"github.com/dyl-04/sched/internal/queue"
	"github.com/dyl-04/sched/internal/store"
)

// ErrNoReadyTask is returned when there is nothing to lease.
var ErrNoReadyTask = errors.New("no ready task")

// WorkerPicker selects a live worker for a lease.
type WorkerPicker interface {
	Pick(now time.Time) (*model.WorkerNode, error)
}

// Manager owns the lease lifecycle.
type Manager struct {
	mu       sync.Mutex
	leases   map[string]*model.Lease
	byTask   map[string]string
	store    *store.TaskStore
	ready    *queue.ReadyQueue
	picker   WorkerPicker
	metrics  *metrics.Metrics
	clock    clock.Clock
	idgen    *idgen.Generator
	ttl      time.Duration
}

// NewManager wires a lease manager to the given dependencies.
func NewManager(st *store.TaskStore, ready *queue.ReadyQueue, picker WorkerPicker, m *metrics.Metrics, c clock.Clock, gen *idgen.Generator, ttl time.Duration) *Manager {
	return &Manager{
		leases:   make(map[string]*model.Lease),
		byTask:   make(map[string]string),
		store:    st,
		ready:    ready,
		picker:   picker,
		metrics:  m,
		clock:    c,
		idgen:    gen,
		ttl:      ttl,
	}
}

// Assign takes the next ready task and leases it to a live worker.
func (m *Manager) Assign(now time.Time) (*model.Lease, error) {
	for {
		entry := m.ready.Pop()
		if entry == nil {
			return nil, ErrNoReadyTask
		}
		t, ok, err := m.store.Transition(entry.TaskID, model.StateReady, model.StateLeased, -1)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		node, err := m.picker.Pick(now)
		if err != nil {
			m.store.Transition(entry.TaskID, model.StateLeased, model.StateReady, t.Version)
			m.ready.Push(entry.TaskID, t.Priority, now)
			return nil, err
		}
		lease := &model.Lease{
			ID:          m.idgen.Next("lease"),
			TaskID:      t.ID,
			WorkerID:    node.ID,
			TaskVersion: t.Version,
			Deadline:    now.Add(m.ttl),
			CreatedAt:   now,
		}
		m.mu.Lock()
		m.leases[lease.ID] = lease
		m.byTask[t.ID] = lease.ID
		m.mu.Unlock()
		return lease, nil
	}
}

// Renew extends a lease on heartbeat when ownership and version still match.
func (m *Manager) Renew(leaseID, workerID string, now time.Time) error {
	m.mu.Lock()
	lease, ok := m.leases[leaseID]
	if !ok {
		m.mu.Unlock()
		return errors.New("lease not found")
	}
	if lease.WorkerID != workerID {
		m.mu.Unlock()
		return errors.New("lease owned by another worker")
	}
	task, err := m.store.Get(lease.TaskID)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if task.Version != lease.TaskVersion {
		delete(m.leases, leaseID)
		delete(m.byTask, task.ID)
		m.mu.Unlock()
		return errors.New("lease version is stale")
	}
	lease.Deadline = now.Add(m.ttl)
	m.mu.Unlock()
	return nil
}

// Current returns the active lease for a task, or nil.
func (m *Manager) Current(taskID string) *model.Lease {
	m.mu.Lock()
	defer m.mu.Unlock()
	leaseID, ok := m.byTask[taskID]
	if !ok {
		return nil
	}
	lease, ok := m.leases[leaseID]
	if !ok {
		return nil
	}
	cp := *lease
	return &cp
}

// ByWorker returns all active leases owned by a worker.
func (m *Manager) ByWorker(workerID string) []*model.Lease {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*model.Lease
	for _, lease := range m.leases {
		if lease.WorkerID == workerID {
			cp := *lease
			out = append(out, &cp)
		}
	}
	return out
}

// ReapExpired releases leases whose deadline passed and re-queues their tasks.
func (m *Manager) ReapExpired(now time.Time) []string {
	m.mu.Lock()
	var expired []*model.Lease
	for id, lease := range m.leases {
		if lease.Deadline.Before(now) {
			expired = append(expired, lease)
			delete(m.leases, id)
			delete(m.byTask, lease.TaskID)
		}
	}
	m.mu.Unlock()
	var reaped []string
	for _, lease := range expired {
		t, err := m.store.Get(lease.TaskID)
		if err != nil {
			continue
		}
		if t.State != model.StateLeased || t.Version != lease.TaskVersion {
			continue
		}
		updated, ok, err := m.store.Transition(lease.TaskID, model.StateLeased, model.StateReady, t.Version)
		if err != nil || !ok {
			continue
		}
		m.ready.Push(updated.ID, updated.Priority, now)
		m.metrics.LeasesExpired.Add(1)
		reaped = append(reaped, lease.TaskID)
	}
	return reaped
}

// Release removes a lease when a worker finishes or is canceled.
func (m *Manager) Release(leaseID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, ok := m.leases[leaseID]
	if !ok {
		return
	}
	delete(m.leases, leaseID)
	delete(m.byTask, lease.TaskID)
}
