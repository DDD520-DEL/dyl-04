package verifycase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
	"github.com/dyl-04/sched/internal/model"
)

func TestRetryBaselineUsesLastAttempt(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	w.Registry.Register(&model.WorkerNode{ID: "w1", Address: "a"})
	task := w.RegisterOnce("retry", t0.Add(time.Hour), 5, 1)
	w.Clock.Advance(time.Hour)
	w.Registry.Heartbeat("w1", w.Clock.Now())
	w.Sched.Tick(w.Clock.Now())
	updated, err := w.RunNext(func(_ context.Context, _ *model.Task) (string, error) {
		return "", errors.New("boom")
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if updated.State != model.StateReady {
		t.Fatalf("state %s, want ready", updated.State)
	}
	cur, _ := w.Store.Get(task.ID)
	if !cur.NextRunAt.After(w.Clock.Now()) {
		t.Fatalf("retry scheduled at %v, not after now %v", cur.NextRunAt, w.Clock.Now())
	}
}
