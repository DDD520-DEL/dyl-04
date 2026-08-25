package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
	"github.com/dyl-04/sched/internal/model"
)

func TestStaleLeaseResultIgnored(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.NewWith(t0, 30*time.Second, 10*time.Second, 5*time.Second)
	w.Registry.Register(&model.WorkerNode{ID: "w1", Address: "a"})
	w.Registry.Register(&model.WorkerNode{ID: "w2", Address: "b"})
	task := w.RegisterOnce("stale", t0, 5, 0)
	w.Sched.Tick(t0)
	lease1, err := w.Leases.Assign(t0)
	if err != nil {
		t.Fatalf("assign1: %v", err)
	}
	w.Clock.Advance(31 * time.Second)
	w.Registry.Heartbeat("w2", w.Clock.Now())
	w.Leases.ReapExpired(w.Clock.Now())
	lease2, err := w.Leases.Assign(w.Clock.Now())
	if err != nil {
		t.Fatalf("assign2: %v", err)
	}
	if lease2.WorkerID == lease1.WorkerID {
		t.Fatalf("reassign picked the same worker")
	}
	_, _ = w.Results.RecordResult(lease1, "failure", "timeout", t0, t0.Add(time.Second))
	cur, err := w.Store.Get(task.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if cur.State != model.StateLeased {
		t.Fatalf("stale result moved task to %s, want leased", cur.State)
	}
}
