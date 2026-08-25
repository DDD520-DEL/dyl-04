package verifycase

import (
	"sync"
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
	"github.com/dyl-04/sched/internal/model"
)

func TestRegistryConcurrentWritesSafe(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	const n = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				_ = w.Registry.Register(&model.WorkerNode{ID: "shared", Address: "a"})
			} else {
				_ = w.Registry.Heartbeat("shared", t0)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if got := len(w.Registry.List()); got != 1 {
		t.Fatalf("workers %d, want 1", got)
	}
}
