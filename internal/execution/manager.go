// Package execution applies task results, retries and dead-letter transitions.
package execution

import (
	"errors"
	"time"

	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/dispatch"
	"github.com/dyl-04/sched/internal/idgen"
	"github.com/dyl-04/sched/internal/lease"
	"github.com/dyl-04/sched/internal/metrics"
	"github.com/dyl-04/sched/internal/model"
	"github.com/dyl-04/sched/internal/store"
)

// ErrStaleResult indicates a result no longer matches the current lease.
var ErrStaleResult = errors.New("stale execution result")

// Manager records results and drives state transitions.
type Manager struct {
	store      *store.TaskStore
	records    *store.RecordStore
	leases     *lease.Manager
	scheduler  *dispatch.Scheduler
	metrics    *metrics.Metrics
	clock      clock.Clock
	idgen      *idgen.Generator
	retryDelay time.Duration
}

// NewManager wires the result pipeline.
func NewManager(st *store.TaskStore, rec *store.RecordStore, lm *lease.Manager, sched *dispatch.Scheduler, m *metrics.Metrics, c clock.Clock, gen *idgen.Generator, retryDelay time.Duration) *Manager {
	return &Manager{
		store:      st,
		records:    rec,
		leases:     lm,
		scheduler:  sched,
		metrics:    m,
		clock:      c,
		idgen:      gen,
		retryDelay: retryDelay,
	}
}

// RecordResult validates a lease-bound result and applies it to the task.
func (m *Manager) RecordResult(lease *model.Lease, status, message string, startedAt, finishedAt time.Time) (*model.Task, error) {
	task, err := m.store.Get(lease.TaskID)
	if err != nil {
		return nil, err
	}
	current := m.leases.Current(lease.TaskID)
	if current == nil || current.ID != lease.ID || current.TaskVersion != task.Version {
		return nil, ErrStaleResult
	}
	rec := &model.ExecutionRecord{
		ID:         m.idgen.Next("exec"),
		TaskID:     task.ID,
		Attempt:    task.AttemptCount,
		WorkerID:   lease.WorkerID,
		Status:     status,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		Message:    message,
	}
	if err := m.records.Append(rec); err != nil {
		return nil, err
	}
	m.metrics.ResultsRecorded.Add(1)
	if status == "success" {
		updated, ok, err := m.store.Transition(task.ID, model.StateLeased, model.StateSucceeded, task.Version)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrStaleResult
		}
		m.metrics.TasksExecuted.Add(1)
		m.leases.Release(lease.ID)
		return updated, nil
	}
	m.metrics.TasksFailed.Add(1)
	if task.RetriesLeft > 0 {
		next := finishedAt.Add(m.retryDelay)
		updated, err := m.store.Retry(task.ID, next, task.Version)
		if err != nil {
			return nil, err
		}
		m.scheduler.ScheduleRetry(updated.ID, next, updated.Priority, finishedAt)
		m.leases.Release(lease.ID)
		return updated, nil
	}
	updated, ok, err := m.store.Transition(task.ID, model.StateLeased, model.StateDead, task.Version)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrStaleResult
	}
	m.leases.Release(lease.ID)
	return updated, nil
}
