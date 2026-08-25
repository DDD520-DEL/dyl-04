// Package metrics records lightweight counters for scheduler operations.
package metrics

import "sync/atomic"

// Metrics aggregates atomic counters.
type Metrics struct {
	TasksExecuted   atomic.Int64
	TasksFailed     atomic.Int64
	LeasesExpired   atomic.Int64
	ResultsRecorded atomic.Int64
}

// New returns an empty metrics set.
func New() *Metrics {
	return &Metrics{}
}
