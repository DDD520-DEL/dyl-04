package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/harness"
	"github.com/dyl-04/sched/internal/model"
)

func TestPurgeKeepsNewerRecords(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	w.Records.Append(&model.ExecutionRecord{ID: "r1", TaskID: "t", FinishedAt: t0.Add(time.Minute)})
	w.Records.Append(&model.ExecutionRecord{ID: "r2", TaskID: "t", FinishedAt: t0.Add(3 * time.Minute)})
	w.Records.PurgeBefore(t0.Add(2 * time.Minute))
	list := w.Records.List("t")
	if len(list) != 1 || !list[0].FinishedAt.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("kept wrong records: %+v", list)
	}
}
