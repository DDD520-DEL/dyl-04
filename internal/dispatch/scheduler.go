// Package dispatch scans due tasks, enqueues them and schedules retries.
package dispatch

import (
	"errors"
	"sync"
	"time"

	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/cron"
	"github.com/dyl-04/sched/internal/model"
	"github.com/dyl-04/sched/internal/queue"
	"github.com/dyl-04/sched/internal/store"
)

// ErrNotTriggerable is returned when a task cannot be manually triggered.
var ErrNotTriggerable = errors.New("task is not triggerable")

// Scheduler owns the fire-time index and enqueue paths.
type Scheduler struct {
	store    *store.TaskStore
	ready    *queue.ReadyQueue
	delayed  *queue.DelayedQueue
	clock    clock.Clock
	mu       sync.Mutex
	nextFire map[string]time.Time
}

// NewScheduler creates the dispatch engine.
func NewScheduler(st *store.TaskStore, ready *queue.ReadyQueue, delayed *queue.DelayedQueue, c clock.Clock) *Scheduler {
	return &Scheduler{
		store:    st,
		ready:    ready,
		delayed:  delayed,
		clock:    c,
		nextFire: make(map[string]time.Time),
	}
}

// Track registers a task's next fire time in the dispatch index.
func (s *Scheduler) Track(task *model.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextFire[task.ID] = task.NextRunAt
}


// SetNextFire updates the index for a single task.
func (s *Scheduler) SetNextFire(taskID string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextFire[taskID] = at
}

// NextCron computes the next fire time for a cron expression after the given instant.
func (s *Scheduler) NextCron(expr string, after time.Time) time.Time {
	schedule, err := cron.Parse(expr)
	if err != nil {
		return after.Add(365 * 24 * time.Hour)
	}
	return schedule.Next(after)
}

// Tick moves due pending and delayed tasks into the ready queue.
func (s *Scheduler) Tick(now time.Time) int {
	s.mu.Lock()
	var due []string
	for id, at := range s.nextFire {
		if !at.After(now) {
			due = append(due, id)
		}
	}
	s.mu.Unlock()
	enqueued := 0
	for _, id := range due {
		t, err := s.store.Get(id)
		if err != nil {
			s.mu.Lock()
			delete(s.nextFire, id)
			s.mu.Unlock()
			continue
		}
		if t.State != model.StatePending {
			continue
		}
		updated, err := s.store.MarkReady(id)
		if err != nil || updated == nil {
			continue
		}
		s.ready.Push(updated.ID, updated.Priority, now)
		enqueued++
		if updated.Trigger.Kind == "cron" {
			schedule, perr := cron.Parse(updated.Trigger.Cron)
			if perr == nil {
				next := schedule.Next(now)
				if err := s.store.SetNextRun(id, next); err == nil {
					s.SetNextFire(id, next)
				}
			}
		}
	}
	for _, entry := range s.delayed.PopDue(now) {
		t, err := s.store.Get(entry.TaskID)
		if err != nil {
			continue
		}
		if t.State != model.StateReady {
			continue
		}
		s.ready.Push(t.ID, t.Priority, now)
		enqueued++
	}
	return enqueued
}

// TriggerNow immediately enqueues a pending task.
func (s *Scheduler) TriggerNow(taskID string, now time.Time) error {
	t, err := s.store.Get(taskID)
	if err != nil {
		return err
	}
	if t.State != model.StatePending && t.State != model.StateReady {
		return ErrNotTriggerable
	}
	updated, ok, err := s.store.Transition(taskID, model.StatePending, model.StateReady, -1)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	s.ready.Push(updated.ID, updated.Priority, now)
	return nil
}

// ScheduleRetry places a task into the delayed queue for a future retry.
func (s *Scheduler) ScheduleRetry(taskID string, nextRunAt time.Time, priority int, now time.Time) {
	s.delayed.Push(&queue.DelayedEntry{
		TaskID:     taskID,
		NextRunAt:  nextRunAt,
		Priority:   priority,
		EnqueuedAt: now,
	})
}

// Cancel stops a pending or ready task and clears its queue references.
func (s *Scheduler) Cancel(taskID string) error {
	t, err := s.store.Get(taskID)
	if err != nil {
		return err
	}
	if t.State == model.StateSucceeded || t.State == model.StateDead || t.State == model.StateCanceled {
		return errors.New("task already terminal")
	}
	updated, ok, err := s.store.Transition(taskID, t.State, model.StateCanceled, t.Version)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("task state changed concurrently")
	}
	s.ready.Remove(taskID)
	s.delayed.Remove(taskID)
	s.mu.Lock()
	delete(s.nextFire, taskID)
	s.mu.Unlock()
	_ = updated
	return nil
}
