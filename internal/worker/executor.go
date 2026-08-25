// Package worker tracks registered executors and their heartbeat liveness.
package worker

import (
	"context"
	"errors"

	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/execution"
	"github.com/dyl-04/sched/internal/lease"
	"github.com/dyl-04/sched/internal/model"
	"github.com/dyl-04/sched/internal/store"
)

// HandlerFunc executes one task and returns a human-readable outcome.
type HandlerFunc func(ctx context.Context, task *model.Task) (string, error)

// Executor claims leased tasks and records their results.
type Executor struct {
	store   *store.TaskStore
	leases  *lease.Manager
	results *execution.Manager
	clock   clock.Clock
}

// NewExecutor wires an executor to the delivery pipeline.
func NewExecutor(st *store.TaskStore, lm *lease.Manager, rm *execution.Manager, c clock.Clock) *Executor {
	return &Executor{store: st, leases: lm, results: rm, clock: c}
}

// RunNext leases one ready task, executes it and records the outcome.
func (e *Executor) RunNext(ctx context.Context, handler HandlerFunc) (*model.Task, error) {
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	now := e.clock.Now()
	lease, err := e.leases.Assign(now)
	if err != nil {
		return nil, err
	}
	task, err := e.store.Get(lease.TaskID)
	if err != nil {
		e.leases.Release(lease.ID)
		return nil, err
	}
	started := e.clock.Now()
	msg, runErr := handler(ctx, task)
	finished := e.clock.Now()
	status := "success"
	if runErr != nil {
		status = "failure"
		msg = runErr.Error()
	}
	updated, err := e.results.RecordResult(lease, status, msg, started, finished)
	if err != nil {
		e.leases.Release(lease.ID)
		return nil, err
	}
	return updated, nil
}
