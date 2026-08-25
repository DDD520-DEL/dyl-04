package verifycase

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dyl-04/sched/internal/api"
	"github.com/dyl-04/sched/internal/harness"
)

func TestRegisterBatchRollsBack(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	h := api.NewHandler(w.Store, w.Records, w.Sched, w.Registry, w.Leases, w.Audit, w.Clock, w.IDGen, harness.Config(), "verify")
	_ = w.RegisterOnce("dup", t0.Add(time.Hour), 5, 0)
	runAt := t0.Add(time.Hour).Format(time.RFC3339)
	batch := map[string]any{
		"tasks": []map[string]any{
			{"name": "a1", "handler": "echo", "payload": "p", "priority": 5, "timeout_ms": 5000, "max_retries": 1, "trigger": map[string]any{"kind": "once", "run_at": runAt}},
			{"name": "a2", "handler": "echo", "payload": "p", "priority": 5, "timeout_ms": 5000, "max_retries": 1, "trigger": map[string]any{"kind": "once", "run_at": runAt}},
			{"name": "dup", "handler": "echo", "payload": "p", "priority": 5, "timeout_ms": 5000, "max_retries": 1, "trigger": map[string]any{"kind": "once", "run_at": runAt}},
		},
	}
	body, _ := json.Marshal(batch)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/register-batch", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.RegisterBatch(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409", rec.Code)
	}
	if got := len(w.Store.List(1000, 0)); got != 1 {
		t.Fatalf("tasks %d, want 1 after rollback", got)
	}
}
