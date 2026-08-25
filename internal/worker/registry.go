// Package worker tracks registered executors and their heartbeat liveness.
package worker

import (
	"errors"
	"sync"
	"time"

	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/model"
)

// ErrNoWorker is returned when no live worker can be selected.
var ErrNoWorker = errors.New("no live worker")

// Registry keeps the worker roster.
type Registry struct {
	mu     sync.Mutex
	nodes  map[string]*model.WorkerNode
	order  []string
	clock  clock.Clock
	window time.Duration
}

// NewRegistry creates an empty worker roster.
func NewRegistry(c clock.Clock, window time.Duration) *Registry {
	return &Registry{nodes: make(map[string]*model.WorkerNode), clock: c, window: window}
}

// Register adds a worker or refreshes an existing one.
func (r *Registry) Register(node *model.WorkerNode) error {
	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if node == nil || node.ID == "" {
		return errors.New("worker id is required")
	}
	existing, ok := r.nodes[node.ID]
	if ok {
		existing.Generation++
		existing.Address = node.Address
		existing.LastHeartbeat = now
		return nil
	}
	cp := *node
	cp.RegisteredAt = now
	cp.LastHeartbeat = now
	cp.Generation = 1
	r.nodes[node.ID] = &cp
	r.order = append(r.order, node.ID)
	return nil
}

// Heartbeat marks a worker alive.
func (r *Registry) Heartbeat(id string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[id]
	if !ok {
		return errors.New("worker not registered")
	}
	node.LastHeartbeat = now
	return nil
}

// Pick returns a live worker using round-robin over the roster.
func (r *Registry) Pick(now time.Time) (*model.WorkerNode, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := now.Add(-r.window)
	for _, id := range r.order {
		node, ok := r.nodes[id]
		if !ok {
			continue
		}
		if node.LastHeartbeat.Before(cutoff) {
			continue
		}
		cp := *node
		return &cp, nil
	}
	return nil, ErrNoWorker
}

// OfflineNodes returns ids whose heartbeat is older than the window.
func (r *Registry) OfflineNodes(now time.Time) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := now.Add(-r.window)
	var out []string
	for id, node := range r.nodes {
		if node.LastHeartbeat.Before(cutoff) {
			out = append(out, id)
		}
	}
	return out
}

// List returns a snapshot of the roster.
func (r *Registry) List() []*model.WorkerNode {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*model.WorkerNode, 0, len(r.nodes))
	for _, node := range r.nodes {
		cp := *node
		out = append(out, &cp)
	}
	return out
}
