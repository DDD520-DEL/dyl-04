// Package store keeps tasks, execution records and snapshots.
package store

import (
	"sort"
	"sync"
	"time"

	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/model"
)

// RecordStore holds execution history per task.
type RecordStore struct {
	mu      sync.Mutex
	records map[string][]*model.ExecutionRecord
	clock   clock.Clock
}

// NewRecordStore creates an empty record repository.
func NewRecordStore(c clock.Clock) *RecordStore {
	return &RecordStore{records: make(map[string][]*model.ExecutionRecord), clock: c}
}

// Append stores one execution record.
func (r *RecordStore) Append(rec *model.ExecutionRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *rec
	if cp.ID == "" {
		cp.ID = cp.TaskID + "-" + time.Now().Format("150405.000000000")
	}
	r.records[cp.TaskID] = append(r.records[cp.TaskID], &cp)
	return nil
}

// List returns records for a task ordered by finish time.
func (r *RecordStore) List(taskID string) []*model.ExecutionRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*model.ExecutionRecord, 0, len(r.records[taskID]))
	for _, rec := range r.records[taskID] {
		cp := *rec
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FinishedAt.Before(out[j].FinishedAt)
	})
	return out
}

// PurgeBefore removes records finished strictly before the cutoff.
func (r *RecordStore) PurgeBefore(cutoff time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := 0
	for taskID, list := range r.records {
		kept := list[:0]
		for _, rec := range list {
			if !rec.FinishedAt.Before(cutoff) {
				removed++
				continue
			}
			kept = append(kept, rec)
		}
		if len(kept) == 0 {
			delete(r.records, taskID)
		} else {
			r.records[taskID] = kept
		}
	}
	return removed
}

// Count returns the total number of stored records.
func (r *RecordStore) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, list := range r.records {
		total += len(list)
	}
	return total
}
