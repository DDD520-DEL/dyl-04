// Package audit records an append-only operation log.
package audit

import (
	"sync"
	"time"
)

// Entry is one audited operation.
type Entry struct {
	At        time.Time
	Operator  string
	Action    string
	TargetID  string
	Detail    string
	RequestID string
}

// Log is a bounded in-memory audit trail.
type Log struct {
	mu      sync.Mutex
	entries []Entry
	limit   int
}

// NewLog creates an audit log capped at limit entries.
func NewLog(limit int) *Log {
	if limit <= 0 {
		limit = 5000
	}
	return &Log{limit: limit}
}

// Record appends one audit entry, dropping the oldest when full.
func (l *Log) Record(e Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
	if len(l.entries) > l.limit {
		l.entries = append([]Entry(nil), l.entries[len(l.entries)-l.limit:]...)
	}
}

// List returns entries newer than since, newest last.
func (l *Log) List(since time.Time) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, 0, len(l.entries))
	for _, e := range l.entries {
		if !e.At.Before(since) {
			out = append(out, e)
		}
	}
	return out
}
