// Package clock provides an injectable time source for the scheduler.
package clock

import "time"

// Clock abstracts wall time so tests can control scheduling deterministically.
type Clock interface {
	Now() time.Time
}

// Wall returns the real system clock.
type Wall struct{}

// Now returns the current wall time.
func (Wall) Now() time.Time {
	return time.Now()
}
