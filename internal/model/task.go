// Package model defines the domain types for the distributed task scheduler.
package model

import "time"

// TaskState is the lifecycle state of a scheduled task.
type TaskState string

const (
	StatePending   TaskState = "pending"
	StateReady     TaskState = "ready"
	StateLeased    TaskState = "leased"
	StateSucceeded TaskState = "succeeded"
	StateDead      TaskState = "dead"
	StateCanceled  TaskState = "canceled"
)

// TriggerSpec describes how a task is scheduled.
type TriggerSpec struct {
	Kind  string    // "once" or "cron"
	RunAt time.Time // used when Kind == "once"
	Cron  string    // used when Kind == "cron"
}

// Task is a unit of scheduled work.
type Task struct {
	ID            string
	Name          string
	Handler       string
	Payload       string
	Priority      int
	Timeout       time.Duration
	MaxRetries    int
	RetriesLeft   int
	State         TaskState
	Version       int64
	NextRunAt     time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	LastAttemptAt time.Time
	AttemptCount  int
	Trigger       TriggerSpec
}

// Clone returns a deep copy safe for concurrent readers.
func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

// ExecutionRecord is one observed execution of a task.
type ExecutionRecord struct {
	ID         string
	TaskID     string
	Attempt    int
	WorkerID   string
	Status     string // "success" or "failure"
	StartedAt  time.Time
	FinishedAt time.Time
	Message    string
}

// Lease binds a ready task to a worker for a bounded window.
type Lease struct {
	ID          string
	TaskID      string
	WorkerID    string
	TaskVersion int64
	Deadline    time.Time
	CreatedAt   time.Time
}

// WorkerNode is a registered executor worker.
type WorkerNode struct {
	ID            string
	Address       string
	LastHeartbeat time.Time
	RegisteredAt  time.Time
	Generation    int64
}
