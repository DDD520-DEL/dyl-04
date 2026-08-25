// Package harness wires the full scheduler stack with an injectable clock.
package harness

import (
	"context"
	"sync"
	"time"

	"github.com/dyl-04/sched/internal/audit"
	"github.com/dyl-04/sched/internal/config"
	"github.com/dyl-04/sched/internal/dispatch"
	"github.com/dyl-04/sched/internal/execution"
	"github.com/dyl-04/sched/internal/idgen"
	"github.com/dyl-04/sched/internal/lease"
	"github.com/dyl-04/sched/internal/metrics"
	"github.com/dyl-04/sched/internal/model"
	"github.com/dyl-04/sched/internal/queue"
	"github.com/dyl-04/sched/internal/store"
	"github.com/dyl-04/sched/internal/worker"
)

// Clock is a manually advanceable time source.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock starts the fake clock at the given instant.
func NewClock(at time.Time) *Clock {
	return &Clock{now: at}
}

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Wiring holds every component of the scheduler stack.
type Wiring struct {
	Clock    *Clock
	Store    *store.TaskStore
	Records  *store.RecordStore
	Ready    *queue.ReadyQueue
	Delayed  *queue.DelayedQueue
	Sched    *dispatch.Scheduler
	Registry *worker.Registry
	Leases   *lease.Manager
	Results  *execution.Manager
	Executor *worker.Executor
	Metrics  *metrics.Metrics
	IDGen    *idgen.Generator
	Audit    *audit.Log
}

// New builds the stack with sensible defaults for verification.
func New(at time.Time) *Wiring {
	return NewWith(at, 30*time.Second, 10*time.Second, 5*time.Second)
}

// NewWith builds the stack with explicit lease, heartbeat and retry settings.
func NewWith(at time.Time, leaseTTL, heartbeatWindow, retryDelay time.Duration) *Wiring {
	clk := NewClock(at)
	gen := &idgen.Generator{}
	metric := metrics.New()
	taskStore := store.NewTaskStore(clk)
	recordStore := store.NewRecordStore(clk)
	ready := queue.NewReadyQueue()
	delayed := queue.NewDelayedQueue()
	sched := dispatch.NewScheduler(taskStore, ready, delayed, clk)
	reg := worker.NewRegistry(clk, heartbeatWindow)
	lm := lease.NewManager(taskStore, ready, reg, metric, clk, gen, leaseTTL)
	rm := execution.NewManager(taskStore, recordStore, lm, sched, metric, clk, gen, retryDelay)
	executor := worker.NewExecutor(taskStore, lm, rm, clk)
	return &Wiring{
		Clock:    clk,
		Store:    taskStore,
		Records:  recordStore,
		Ready:    ready,
		Delayed:  delayed,
		Sched:    sched,
		Registry: reg,
		Leases:   lm,
		Results:  rm,
		Executor: executor,
		Metrics:  metric,
		IDGen:    gen,
		Audit:    audit.NewLog(1000),
	}
}

// RegisterOnce adds a one-time task due at the given instant.
func (w *Wiring) RegisterOnce(name string, at time.Time, priority, maxRetries int) *model.Task {
	task := &model.Task{
		ID:         w.IDGen.Next("task"),
		Name:       name,
		Handler:    "echo",
		Payload:    "payload",
		Priority:   priority,
		Timeout:    5 * time.Second,
		MaxRetries: maxRetries,
		RetriesLeft: maxRetries,
		State:      model.StatePending,
		NextRunAt:  at,
		Trigger:    model.TriggerSpec{Kind: "once", RunAt: at},
	}
	if err := w.Store.Save(task); err != nil {
		panic(err)
	}
	w.Sched.Track(task)
	return task
}

// RegisterCron adds a cron task whose next fire is computed from the clock.
func (w *Wiring) RegisterCron(name, expr string, priority, maxRetries int) *model.Task {
	task := &model.Task{
		ID:          w.IDGen.Next("task"),
		Name:        name,
		Handler:     "echo",
		Payload:     "payload",
		Priority:    priority,
		Timeout:     5 * time.Second,
		MaxRetries:  maxRetries,
		RetriesLeft: maxRetries,
		State:       model.StatePending,
		Trigger:     model.TriggerSpec{Kind: "cron", Cron: expr},
	}
	// The harness computes next fire inline via the exported scheduler helper.
	next := w.Sched.NextCron(expr, w.Clock.Now())
	task.NextRunAt = next
	if err := w.Store.Save(task); err != nil {
		panic(err)
	}
	w.Sched.Track(task)
	return task
}

// RunNext leases one ready task and executes it with the given handler.
func (w *Wiring) RunNext(handler worker.HandlerFunc) (*model.Task, error) {
	return w.Executor.RunNext(context.Background(), handler)
}

// Config returns the default runtime configuration.
func Config() config.Config {
	return config.Default()
}
