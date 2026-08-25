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

func TestCronUpdateRefreshesIndex(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	w := harness.New(t0)
	h := api.NewHandler(w.Store, w.Records, w.Sched, w.Registry, w.Leases, w.Audit, w.Clock, w.IDGen, harness.Config(), "verify")
	regBody := map[string]any{
		"name": "cron-a", "handler": "echo", "payload": "p", "priority": 5,
		"timeout_ms": 5000, "max_retries": 0,
		"trigger": map[string]any{"kind": "cron", "cron": "*/5 * * * *"},
	}
	body, _ := json.Marshal(regBody)
	rec := httptest.NewRecorder()
	h.RegisterTask(rec, httptest.NewRequest(http.MethodPost, "/api/tasks/register", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("register status %d: %s", rec.Code, rec.Body.String())
	}
	var regResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &regResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	upd := map[string]any{"id": regResp.ID, "cron": "0 0 1 1 *"}
	ub, _ := json.Marshal(upd)
	urec := httptest.NewRecorder()
	h.UpdateCron(urec, httptest.NewRequest(http.MethodPost, "/api/tasks/update-cron", bytes.NewReader(ub)))
	if urec.Code != http.StatusOK {
		t.Fatalf("update status %d: %s", urec.Code, urec.Body.String())
	}
	w.Clock.Advance(5*time.Minute + time.Second)
	if n := w.Sched.Tick(w.Clock.Now()); n != 0 {
		t.Fatalf("tick enqueued %d after cron update, want 0", n)
	}
}
