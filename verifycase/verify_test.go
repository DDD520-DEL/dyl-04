package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
)

func TestRetryQueueOrdersByNextRun(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	a := w.RegisterOnce("A", t0.Add(time.Hour), 5, 1)
	b := w.RegisterOnce("B", t0.Add(time.Hour), 1, 1)
	w.Sched.ScheduleRetry(a.ID, t0.Add(10*time.Minute), 5, t0.Add(time.Minute))
	w.Sched.ScheduleRetry(b.ID, t0.Add(5*time.Minute), 1, t0.Add(2*time.Minute))
	w.Clock.Advance(11 * time.Minute)
	popped := w.Delayed.PopDue(w.Clock.Now())
	if len(popped) != 2 {
		t.Fatalf("popped %d, want 2", len(popped))
	}
	if popped[0].TaskID != b.ID {
		t.Fatalf("first pop %s, want %s", popped[0].TaskID, b.ID)
	}
}
