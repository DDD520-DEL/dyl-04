package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
)

func TestCancelClearsDelayedQueue(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	task := w.RegisterOnce("cancel", t0.Add(time.Hour), 5, 1)
	w.Sched.ScheduleRetry(task.ID, t0.Add(time.Hour), 5, t0)
	if err := w.Sched.Cancel(task.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got := w.Delayed.Len(); got != 0 {
		t.Fatalf("delayed entries %d, want 0", got)
	}
}
