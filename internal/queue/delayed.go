// Package queue implements priority and delayed queues for scheduled tasks.
package queue

import (
	"container/heap"
	"sync"
	"time"
)

// DelayedEntry is a task scheduled to run at a future instant.
type DelayedEntry struct {
	TaskID     string
	NextRunAt  time.Time
	Priority   int
	EnqueuedAt time.Time
	index      int
}

// DelayedQueue keeps retry and future tasks ordered by next run time.
type DelayedQueue struct {
	mu      sync.Mutex
	entries delayedHeap
	present map[string]struct{}
}

// NewDelayedQueue returns an empty delayed queue.
func NewDelayedQueue() *DelayedQueue {
	return &DelayedQueue{present: make(map[string]struct{})}
}

// Push schedules a task; an existing entry is replaced with the earlier time.
func (q *DelayedQueue) Push(entry *DelayedEntry) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.present[entry.TaskID]; ok {
		for i, e := range q.entries {
			if e.TaskID == entry.TaskID {
				if e.NextRunAt.After(entry.NextRunAt) {
					e.NextRunAt = entry.NextRunAt
					heap.Fix(&q.entries, i)
				}
				return false
			}
		}
	}
	q.present[entry.TaskID] = struct{}{}
	heap.Push(&q.entries, entry)
	return true
}

// PopDue returns tasks whose time has arrived, earliest first.
func (q *DelayedQueue) PopDue(now time.Time) []*DelayedEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []*DelayedEntry
	for len(q.entries) > 0 {
		top := q.entries[0]
		if top.NextRunAt.After(now) {
			break
		}
		entry := heap.Pop(&q.entries).(*DelayedEntry)
		delete(q.present, entry.TaskID)
		out = append(out, entry)
	}
	return out
}

// Remove deletes a task from the queue if scheduled.
func (q *DelayedQueue) Remove(taskID string) bool {
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

// Len returns the number of scheduled entries.
func (q *DelayedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

type delayedHeap []*DelayedEntry

func (h delayedHeap) Len() int { return len(h) }

func (h delayedHeap) Less(i, j int) bool {
	// Order by next run time so the earliest-due task is at the heap root;
	// PopDue then drains due tasks earliest-first regardless of enqueue order.
	// Ties fall back to enqueue order for stable, FIFO-like behavior.
	if !h[i].NextRunAt.Equal(h[j].NextRunAt) {
		return h[i].NextRunAt.Before(h[j].NextRunAt)
	}
	return h[i].EnqueuedAt.Before(h[j].EnqueuedAt)
}

func (h delayedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *delayedHeap) Push(x any) {
	entry := x.(*DelayedEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*h = old[:n-1]
	return entry
}
