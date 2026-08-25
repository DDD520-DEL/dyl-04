package verifycase

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
	"github.com/dyl-04/sched/internal/model"
)

func TestTriggerNowDoesNotDuplicateEnqueue(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	w.Registry.Register(&model.WorkerNode{ID: "w1", Address: "a"})
	task := w.RegisterOnce("dup", t0, 5, 0)
	if n := w.Sched.Tick(t0); n != 1 {
		t.Fatalf("tick enqueued %d, want 1", n)
	}
	if err := w.Sched.TriggerNow(task.ID, t0); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if got := w.Ready.Len(); got != 1 {
		t.Fatalf("ready queue length %d, want 1", got)
	}
	var executed int32
	for i := 0; i < 3; i++ {
		_, _ = w.RunNext(func(_ context.Context, _ *model.Task) (string, error) {
			atomic.AddInt32(&executed, 1)
			return "ok", nil
		})
	}
	if got := atomic.LoadInt32(&executed); got != 1 {
		t.Fatalf("executed %d times, want 1", got)
	}
}
