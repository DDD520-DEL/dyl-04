// Package cron parses five-field cron expressions and computes next fire time.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule represents a parsed five-field cron expression.
type Schedule struct {
	minute     []int
	hour       []int
	dayOfMonth []int
	month      []int
	dayOfWeek  []int
}

// Parse validates and compiles a five-field cron expression.
func Parse(expr string) (*Schedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have five fields: %q", expr)
	}
	s := &Schedule{}
	minute, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	}
	s.minute = minute
	hour, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	}
	s.hour = hour
	dayOfMonth, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("day-of-month: %w", err)
	}
	s.dayOfMonth = dayOfMonth
	month, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}
	s.month = month
	dayOfWeek, err := parseField(fields[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("day-of-week: %w", err)
	}
	s.dayOfWeek = dayOfWeek
	return s, nil
}

func parseField(raw string, min, max int) ([]int, error) {
	if raw == "*" {
		out := make([]int, 0, max-min+1)
		for v := min; v <= max; v++ {
			out = append(out, v)
		}
		return out, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		step := 1
		base := part
		if idx := strings.Index(part, "/"); idx >= 0 {
			base = part[:idx]
			n, err := strconv.Atoi(part[idx+1:])
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("invalid step %q", part)
			}
			step = n
		}
		lo, hi := min, max
		if base != "*" {
			rangeParts := strings.Split(base, "-")
			if len(rangeParts) == 1 {
				v, err := strconv.Atoi(base)
				if err != nil || v < min || v > max {
					return nil, fmt.Errorf("value %q out of range", base)
				}
				lo, hi = v, v
			} else if len(rangeParts) == 2 {
				a, errA := strconv.Atoi(rangeParts[0])
				b, errB := strconv.Atoi(rangeParts[1])
				if errA != nil || errB != nil || a < min || b > max || a > b {
					return nil, fmt.Errorf("invalid range %q", base)
				}
				lo, hi = a, b
			} else {
				return nil, fmt.Errorf("invalid field %q", base)
			}
		}
		for v := lo; v <= hi; v += step {
			out = append(out, v)
		}
	}
	return out, nil
}

// Next returns the first fire time strictly after the given instant.
func (s *Schedule) Next(after time.Time) time.Time {
	t := after.Truncate(time.Minute).Add(time.Minute)
	for y := t.Year(); y <= after.Year()+8; y++ {
		for m := 1; m <= 12; m++ {
			if !contains(s.month, m) {
				continue
			}
			days := daysInMonth(y, m)
			for d := 1; d <= days; d++ {
				if !contains(s.dayOfMonth, d) {
					continue
				}
				dow := int(time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Weekday())
				if !contains(s.dayOfWeek, dow) {
					continue
				}
				candidate := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
				for h := 0; h <= 23; h++ {
					if !contains(s.hour, h) {
						continue
					}
					for minute := 0; minute <= 59; minute++ {
						if !contains(s.minute, minute) {
							continue
						}
						c := candidate.Add(time.Duration(h)*time.Hour + time.Duration(minute)*time.Minute)
						if c.After(after) {
							return c
						}
					}
				}
			}
		}
	}
	return after.AddDate(8, 0, 0)
}

func contains(list []int, v int) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

func daysInMonth(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}
