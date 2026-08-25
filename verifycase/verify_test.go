package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
	"github.com/dyl-04/sched/internal/model"
)

func TestLeaseExpiresAfterHeartbeatStops(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.NewWith(t0, 30*time.Second, 10*time.Second, 5*time.Second)
	w.Registry.Register(&model.WorkerNode{ID: "w1", Address: "a"})
	_ = w.RegisterOnce("lease", t0, 5, 0)
	w.Sched.Tick(t0)
	lease, err := w.Leases.Assign(t0)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	w.Clock.Advance(10 * time.Second)
	if err := w.Leases.Renew(lease.ID, lease.WorkerID, w.Clock.Now()); err != nil {
		t.Fatalf("renew: %v", err)
	}
	w.Clock.Advance(31 * time.Second)
	reaped := w.Leases.ReapExpired(w.Clock.Now())
	if len(reaped) != 1 {
		t.Fatalf("reaped %d, want 1", len(reaped))
	}
}
