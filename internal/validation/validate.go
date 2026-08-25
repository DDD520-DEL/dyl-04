// Package validation checks task registration payloads before persistence.
package validation

import (
	"errors"
	"strings"
	"time"

	"github.com/dyl-04/sched/internal/model"
)

// ErrInvalid indicates a registration payload failed validation.
var ErrInvalid = errors.New("invalid task registration")

// ValidateTask verifies a task is safe to register.
func ValidateTask(t *model.Task, now time.Time) error {
	if t == nil {
		return ErrInvalid
	}
	name := strings.TrimSpace(t.Name)
	if name == "" || len(name) > 128 {
		return ErrInvalid
	}
	if strings.TrimSpace(t.Handler) == "" {
		return ErrInvalid
	}
	if t.Priority < 0 || t.Priority > 100 {
		return ErrInvalid
	}
	if t.Timeout <= 0 || t.Timeout > 24*time.Hour {
		return ErrInvalid
	}
	if t.MaxRetries < 0 || t.MaxRetries > 20 {
		return ErrInvalid
	}
	switch t.Trigger.Kind {
	case "once":
		if t.Trigger.RunAt.Before(now) {
			return ErrInvalid
		}
	case "cron":
		if strings.TrimSpace(t.Trigger.Cron) == "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
