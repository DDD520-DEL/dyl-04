// Command schedd runs the distributed task scheduler service.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dyl-04/sched/internal/api"
	"github.com/dyl-04/sched/internal/audit"
	"github.com/dyl-04/sched/internal/clock"
	"github.com/dyl-04/sched/internal/config"
	"github.com/dyl-04/sched/internal/dispatch"
	"github.com/dyl-04/sched/internal/execution"
	"github.com/dyl-04/sched/internal/idgen"
	"github.com/dyl-04/sched/internal/lease"
	"github.com/dyl-04/sched/internal/metrics"
	"github.com/dyl-04/sched/internal/model"
	"github.com/dyl-04/sched/internal/queue"
	"github.com/dyl-04/sched/internal/store"
	"github.com/dyl-04/sched/internal/worker"
)

func main() {
	cfg := config.Default()
	wall := clock.Wall{}
	gen := &idgen.Generator{}
	metric := metrics.New()
	taskStore := store.NewTaskStore(wall)
	recordStore := store.NewRecordStore(wall)
	ready := queue.NewReadyQueue()
	delayed := queue.NewDelayedQueue()
	sched := dispatch.NewScheduler(taskStore, ready, delayed, wall)
	reg := worker.NewRegistry(wall, cfg.HeartbeatWindow)
	lm := lease.NewManager(taskStore, ready, reg, metric, wall, gen, cfg.LeaseTTL)
	rm := execution.NewManager(taskStore, recordStore, lm, sched, metric, wall, gen, cfg.RetryDelay)
	executor := worker.NewExecutor(taskStore, lm, rm, wall)
	alog := audit.NewLog(2000)
	handler := api.NewHandler(taskStore, recordStore, sched, reg, lm, alog, wall, gen, cfg, "schedd")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		ticker := time.NewTicker(cfg.TickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sched.Tick(wall.Now())
				lm.ReapExpired(wall.Now())
			}
		}
	}()

	reg.Register(&model.WorkerNode{ID: "worker-1", Address: "localhost:18090"})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = reg.Heartbeat("worker-1", wall.Now())
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				_, _ = executor.RunNext(ctx, func(_ context.Context, t *model.Task) (string, error) {
					return "echo:" + t.Payload, nil
				})
			}
		}
	}()

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: api.Routes(handler),
	}
	go func() {
		log.Printf("schedd listening on %s", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	fmt.Println("schedd stopped")
}
