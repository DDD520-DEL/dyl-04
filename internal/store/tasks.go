// Package store keeps tasks, execution records and snapshots.
package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/model"
)

// Sentinel errors returned by the task store.
var (
	ErrNotFound        = errors.New("task not found")
	ErrDuplicateName   = errors.New("task name already exists")
	ErrConflict        = errors.New("task state conflict")
	ErrAlreadyCanceled = errors.New("task already canceled")
)

// TaskStore is the in-memory task repository.
type TaskStore struct {
	mu      sync.Mutex
	tasks   map[string]*model.Task
	byName  map[string]string
	byTime  map[int64][]string
	clock   clock.Clock
	version int64
}

// NewTaskStore creates an empty store.
func NewTaskStore(c clock.Clock) *TaskStore {
	return &TaskStore{
		tasks:  make(map[string]*model.Task),
		byName: make(map[string]string),
		byTime: make(map[int64][]string),
		clock:  c,
	}
}

// Save inserts a new task or overwrites an existing one.
func (s *TaskStore) Save(t *model.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	if t.ID == "" {
		return errors.New("task id is required")
	}
	if existing, ok := s.tasks[t.ID]; ok && existing.State == model.StateCanceled {
		return ErrAlreadyCanceled
	}
	if owner, ok := s.byName[t.Name]; ok && owner != t.ID {
		return ErrDuplicateName
	}
	prev := s.tasks[t.ID]
	if prev != nil {
		prev.Version++
		prev.Name = t.Name
		prev.Handler = t.Handler
		prev.Payload = t.Payload
		prev.Priority = t.Priority
		prev.Timeout = t.Timeout
		prev.MaxRetries = t.MaxRetries
		prev.RetriesLeft = t.RetriesLeft
		prev.Trigger = t.Trigger
		prev.NextRunAt = t.NextRunAt
		prev.UpdatedAt = now
		s.reindexTime(prev)
		return nil
	}
	cp := t.Clone()
	cp.Version = 1
	cp.CreatedAt = now
	cp.UpdatedAt = now
	s.tasks[cp.ID] = cp
	s.byName[cp.Name] = cp.ID
	s.indexTime(cp)
	return nil
}

func (s *TaskStore) indexTime(t *model.Task) {
	if t.State == model.StatePending || t.State == model.StateReady {
		key := t.NextRunAt.UnixNano()
		s.byTime[key] = append(s.byTime[key], t.ID)
	}
}

func (s *TaskStore) reindexTime(t *model.Task) {
	for key, ids := range s.byTime {
		filtered := ids[:0]
		for _, id := range ids {
			if id != t.ID {
				filtered = append(filtered, id)
			}
		}
		if len(filtered) == 0 {
			delete(s.byTime, key)
		} else {
			s.byTime[key] = filtered
		}
	}
	s.indexTime(t)
}

// Get returns a clone of the task with the given id.
func (s *TaskStore) Get(id string) (*model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	return t.Clone(), nil
}

// List returns up to limit tasks sorted by creation time.
func (s *TaskStore) List(limit, offset int) []*model.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]*model.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		all = append(all, t.Clone())
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].CreatedAt.Before(all[j].CreatedAt)
	})
	if offset > len(all) {
		return nil
	}
	end := offset + limit
	if limit <= 0 || end > len(all) {
		end = len(all)
	}
	return all[offset:end]
}

// ScanDue returns pending tasks whose next run time has arrived.
func (s *TaskStore) ScanDue(now time.Time) []*model.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*model.Task
	for key, ids := range s.byTime {
		if time.Unix(0, key).After(now) {
			continue
		}
		for _, id := range ids {
			t, ok := s.tasks[id]
			if !ok {
				continue
			}
			if t.State == model.StatePending && !t.NextRunAt.After(now) {
				out = append(out, t.Clone())
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Priority > out[j].Priority
	})
	return out
}

// MarkReady transitions a task from pending to ready without version checks.
func (s *TaskStore) MarkReady(id string) (*model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	if t.State != model.StatePending {
		return nil, ErrConflict
	}
	t.State = model.StateReady
	t.Version++
	t.UpdatedAt = s.clock.Now()
	return t.Clone(), nil
}

// Transition performs a guarded state change with an optional version check.
func (s *TaskStore) Transition(id string, from, to model.TaskState, expectedVersion int64) (*model.Task, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false, ErrNotFound
	}
	if t.State != from {
		return nil, false, nil
	}
	if expectedVersion >= 0 && t.Version != expectedVersion {
		return nil, false, nil
	}
	t.State = to
	t.Version++
	t.UpdatedAt = s.clock.Now()
	return t.Clone(), true, nil
}

// SetState changes state unconditionally; used by the buggy path.
func (s *TaskStore) SetState(id string, to model.TaskState) (*model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	t.State = to
	t.Version++
	t.UpdatedAt = s.clock.Now()
	return t.Clone(), nil
}

// SetNextRun updates the next run time and re-indexes the task.
func (s *TaskStore) SetNextRun(id string, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return ErrNotFound
	}
	t.NextRunAt = next
	t.UpdatedAt = s.clock.Now()
	s.reindexTime(t)
	return nil
}

// Retry atomically consumes one retry and returns the task to ready.
func (s *TaskStore) Retry(id string, nextRunAt time.Time, expectedVersion int64) (*model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	if t.Version != expectedVersion {
		return nil, ErrConflict
	}
	if t.RetriesLeft <= 0 {
		return nil, ErrConflict
	}
	t.RetriesLeft--
	t.State = model.StateReady
	t.NextRunAt = nextRunAt
	t.LastAttemptAt = s.clock.Now()
	t.Version++
	t.UpdatedAt = s.clock.Now()
	s.reindexTime(t)
	return t.Clone(), nil
}

// Delete removes a task and its name binding.
func (s *TaskStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return ErrNotFound
	}
	delete(s.tasks, id)
	delete(s.byName, t.Name)
	for key, ids := range s.byTime {
		filtered := ids[:0]
		for _, item := range ids {
			if item != id {
				filtered = append(filtered, item)
			}
		}
		if len(filtered) == 0 {
			delete(s.byTime, key)
		} else {
			s.byTime[key] = filtered
		}
	}
	return nil
}

// Reset clears all tasks; used by tests.
func (s *TaskStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = make(map[string]*model.Task)
	s.byName = make(map[string]string)
	s.byTime = make(map[int64][]string)
}
