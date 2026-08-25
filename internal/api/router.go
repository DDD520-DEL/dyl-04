// Package api exposes the scheduler control plane over HTTP.
package api

import "net/http"

// Routes registers all scheduler endpoints on the mux.
func Routes(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/tasks/register", h.RegisterTask)
	mux.HandleFunc("POST /api/tasks/register-batch", h.RegisterBatch)
	mux.HandleFunc("POST /api/tasks/trigger", h.TriggerNow)
	mux.HandleFunc("POST /api/tasks/cancel", h.Cancel)
	mux.HandleFunc("POST /api/tasks/update-cron", h.UpdateCron)
	mux.HandleFunc("GET /api/tasks", h.ListTasks)
	mux.HandleFunc("GET /api/tasks/{id}", h.GetTask)
	mux.HandleFunc("GET /api/tasks/records/{id}", h.GetRecords)
	mux.HandleFunc("POST /api/workers/register", h.RegisterWorker)
	mux.HandleFunc("POST /api/workers/heartbeat", h.Heartbeat)
	mux.HandleFunc("GET /api/workers", h.ListWorkers)
	mux.HandleFunc("GET /api/snapshot", h.Snapshot)
	mux.HandleFunc("POST /api/snapshot/restore", h.Restore)
	mux.HandleFunc("POST /api/records/purge", h.PurgeRecords)
	return mux
}
