// Package api exposes the scheduler control plane over HTTP.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dyl-04/sched/internal/audit"
	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/config"
	"github.com/dyl-04/sched/internal/cron"
	"github.com/dyl-04/sched/internal/dispatch"
	"github.com/dyl-04/sched/internal/idgen"
	"github.com/dyl-04/sched/internal/lease"
	"github.com/dyl-04/sched/internal/model"
	"github.com/dyl-04/sched/internal/store"
	"github.com/dyl-04/sched/internal/validation"
	"github.com/dyl-04/sched/internal/worker"
)

// Handler exposes JSON endpoints for task and worker management.
type Handler struct {
	store    *store.TaskStore
	records  *store.RecordStore
	sched    *dispatch.Scheduler
	workers  *worker.Registry
	leases   *lease.Manager
	audit    *audit.Log
	clock    clock.Clock
	idgen    *idgen.Generator
	cfg      config.Config
	operator string
}

// NewHandler builds the HTTP facade.
func NewHandler(st *store.TaskStore, rec *store.RecordStore, sched *dispatch.Scheduler, reg *worker.Registry, lm *lease.Manager, alog *audit.Log, c clock.Clock, gen *idgen.Generator, cfg config.Config, operator string) *Handler {
	return &Handler{
		store:    st,
		records:  rec,
		sched:    sched,
		workers:  reg,
		leases:   lm,
		audit:    alog,
		clock:    c,
		idgen:    gen,
		cfg:      cfg,
		operator: operator,
	}
}

type registerRequest struct {
	Name      string        `json:"name"`
	Handler   string        `json:"handler"`
	Payload   string        `json:"payload"`
	Priority  int           `json:"priority"`
	TimeoutMS int64         `json:"timeout_ms"`
	MaxRetry  int           `json:"max_retries"`
	Trigger   triggerBody   `json:"trigger"`
}

type triggerBody struct {
	Kind  string `json:"kind"`
	RunAt string `json:"run_at"`
	Cron  string `json:"cron"`
}

// RegisterTask validates and persists a new task.
func (h *Handler) RegisterTask(w http.ResponseWriter, r *http.Request) {
	req, err := decodeRegister(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	task, err := h.buildTask(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.store.Save(task); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	h.sched.Track(task)
	h.audit.Record(audit.Entry{At: h.clock.Now(), Operator: h.operator, Action: "register", TargetID: task.ID})
	writeJSON(w, http.StatusOK, task)
}

// RegisterBatch atomically registers a group of tasks.
func (h *Handler) RegisterBatch(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(r.Body)
	var req struct {
		Tasks []registerRequest `json:"tasks"`
	}
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Tasks) == 0 || len(req.Tasks) > h.cfg.MaxBatchSize {
		writeError(w, http.StatusBadRequest, errors.New("batch size out of range"))
		return
	}
	built := make([]*model.Task, 0, len(req.Tasks))
	for _, item := range req.Tasks {
		task, err := h.buildTask(item)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		built = append(built, task)
	}
	saved := make([]string, 0, len(built))
	for _, task := range built {
		if err := h.store.Save(task); err != nil {
			for _, id := range saved {
				_ = h.store.Delete(id)
			}
			writeError(w, http.StatusConflict, err)
			return
		}
		saved = append(saved, task.ID)
		h.sched.Track(task)
	}
	for _, id := range saved {
		h.audit.Record(audit.Entry{At: h.clock.Now(), Operator: h.operator, Action: "register-batch", TargetID: id})
	}
	writeJSON(w, http.StatusOK, map[string]any{"registered": saved})
}

// TriggerNow manually enqueues a task.
func (h *Handler) TriggerNow(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	var req struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeError(w, http.StatusBadRequest, errors.New("task id is required"))
		return
	}
	if err := h.sched.TriggerNow(req.ID, h.clock.Now()); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	h.audit.Record(audit.Entry{At: h.clock.Now(), Operator: h.operator, Action: "trigger", TargetID: req.ID})
	writeJSON(w, http.StatusOK, map[string]any{"triggered": req.ID})
}

// Cancel stops a pending or ready task.
func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	var req struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeError(w, http.StatusBadRequest, errors.New("task id is required"))
		return
	}
	if err := h.sched.Cancel(req.ID); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	h.audit.Record(audit.Entry{At: h.clock.Now(), Operator: h.operator, Action: "cancel", TargetID: req.ID})
	writeJSON(w, http.StatusOK, map[string]any{"canceled": req.ID})
}

// UpdateCron changes a task's cron expression and refreshes dispatch state.
func (h *Handler) UpdateCron(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8192))
	var req struct {
		ID   string `json:"id"`
		Cron string `json:"cron"`
	}
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeError(w, http.StatusBadRequest, errors.New("task id is required"))
		return
	}
	schedule, err := cron.Parse(req.Cron)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	t, err := h.store.Get(req.ID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if t.Trigger.Kind != "cron" {
		writeError(w, http.StatusBadRequest, errors.New("task is not cron scheduled"))
		return
	}
	now := h.clock.Now()
	t.Trigger.Cron = req.Cron
	t.NextRunAt = schedule.Next(now)
	if err := h.store.Save(t); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	h.sched.RefreshCron(t)
	h.audit.Record(audit.Entry{At: now, Operator: h.operator, Action: "update-cron", TargetID: t.ID})
	writeJSON(w, http.StatusOK, t)
}

// GetTask returns one task by id.
func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusBadRequest, errors.New("invalid task id"))
		return
	}
	t, err := h.store.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// ListTasks returns a page of tasks.
func (h *Handler) ListTasks(w http.ResponseWriter, r *http.Request) {
	tasks := h.store.List(1000, 0)
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

// GetRecords returns execution history for a task.
func (h *Handler) GetRecords(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/tasks/records/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusBadRequest, errors.New("invalid task id"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": h.records.List(id)})
}

// RegisterWorker adds a worker to the roster.
func (h *Handler) RegisterWorker(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	var req struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeError(w, http.StatusBadRequest, errors.New("worker id is required"))
		return
	}
	if err := h.workers.Register(&model.WorkerNode{ID: req.ID, Address: req.Address}); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	h.audit.Record(audit.Entry{At: h.clock.Now(), Operator: h.operator, Action: "worker-register", TargetID: req.ID})
	writeJSON(w, http.StatusOK, map[string]any{"registered": req.ID})
}

// Heartbeat refreshes a worker's liveness.
func (h *Handler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	var req struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.ID) == "" {
		writeError(w, http.StatusBadRequest, errors.New("worker id is required"))
		return
	}
	if err := h.workers.Heartbeat(req.ID, h.clock.Now()); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	now := h.clock.Now()
	for _, l := range h.leases.ByWorker(req.ID) {
		_ = h.leases.Renew(l.ID, req.ID, now)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ListWorkers returns the roster.
func (h *Handler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"workers": h.workers.List()})
}

// Snapshot exports the full repository state.
func (h *Handler) Snapshot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	checksum, err := store.Export(h.store, h.records, w)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("X-Snapshot-Sha256", checksum)
}

// Restore imports a previously exported snapshot.
func (h *Handler) Restore(w http.ResponseWriter, r *http.Request) {
	if err := store.Import(h.store, h.records, r.Body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"restored": true})
}

// PurgeRecords removes execution history finished before a cutoff.
func (h *Handler) PurgeRecords(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	var req struct {
		Before string `json:"before"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	cutoff, err := time.Parse(time.RFC3339, req.Before)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	removed := h.records.PurgeBefore(cutoff)
	h.audit.Record(audit.Entry{At: h.clock.Now(), Operator: h.operator, Action: "purge-records", TargetID: cutoff.Format(time.RFC3339)})
	writeJSON(w, http.StatusOK, map[string]any{"removed": removed})
}

func (h *Handler) buildTask(req registerRequest) (*model.Task, error) {
	task := &model.Task{
		ID:         h.idgen.Next("task"),
		Name:       strings.TrimSpace(req.Name),
		Handler:    strings.TrimSpace(req.Handler),
		Payload:    req.Payload,
		Priority:   req.Priority,
		Timeout:    time.Duration(req.TimeoutMS) * time.Millisecond,
		MaxRetries: req.MaxRetry,
		State:      model.StatePending,
		Trigger: model.TriggerSpec{
			Kind: req.Trigger.Kind,
			Cron: req.Trigger.Cron,
		},
	}
	if req.Trigger.Kind == "once" {
		at, err := time.Parse(time.RFC3339, req.Trigger.RunAt)
		if err != nil {
			return nil, err
		}
		task.Trigger.RunAt = at
		task.NextRunAt = at
	} else {
		schedule, err := cron.Parse(req.Trigger.Cron)
		if err != nil {
			return nil, err
		}
		task.NextRunAt = schedule.Next(h.clock.Now())
	}
	task.RetriesLeft = task.MaxRetries
	if err := validation.ValidateTask(task, h.clock.Now()); err != nil {
		return nil, err
	}
	return task, nil
}

func decodeRegister(r *http.Request) (registerRequest, error) {
	var req registerRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		return req, err
	}
	return req, nil
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
