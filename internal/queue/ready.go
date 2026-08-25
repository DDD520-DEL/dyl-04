// Package queue implements priority and delayed queues for scheduled tasks.
package queue

import (
	"container/heap"
	"sync"
	"time"
)

// ReadyEntry is a task waiting to be leased.
type ReadyEntry struct {
	TaskID     string
	Priority   int
	EnqueuedAt time.Time
	index      int
}

// ReadyQueue is a priority heap guarded by a mutex with per-task dedup.
type ReadyQueue struct {
	mu      sync.Mutex
	entries readyHeap
	present map[string]struct{}
}

// NewReadyQueue returns an empty ready queue.
func NewReadyQueue() *ReadyQueue {
	return &ReadyQueue{present: make(map[string]struct{})}
}

// Push enqueues a task unless it is already present.
func (q *ReadyQueue) Push(taskID string, priority int, at time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.present[taskID] = struct{}{}
	entry := &ReadyEntry{TaskID: taskID, Priority: priority, EnqueuedAt: at}
	heap.Push(&q.entries, entry)
	return true
}

// Pop removes the highest priority entry.
func (q *ReadyQueue) Pop() *ReadyEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return nil
	}
	entry := heap.Pop(&q.entries).(*ReadyEntry)
	delete(q.present, entry.TaskID)
	return entry
}

// Remove drops a task from the queue if present.
func (q *ReadyQueue) Remove(taskID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.present[taskID]; !ok {
		return false
	}
	for i, entry := range q.entries {
		if entry.TaskID == taskID {
			heap.Remove(&q.entries, i)
			delete(q.present, taskID)
			return true
		}
	}
	return false
}

// Len returns the number of queued tasks.
func (q *ReadyQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

type readyHeap []*ReadyEntry

func (h readyHeap) Len() int { return len(h) }

func (h readyHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].EnqueuedAt.Before(h[j].EnqueuedAt)
}

func (h readyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *readyHeap) Push(x any) {
	entry := x.(*ReadyEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*h = old[:n-1]
	return entry
}
