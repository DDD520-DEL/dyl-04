// Package config loads runtime settings for the scheduler service.
package config

import "time"

// Config holds service-wide tuning parameters.
type Config struct {
	Addr            string
	TickInterval    time.Duration
	LeaseTTL        time.Duration
	HeartbeatWindow time.Duration
	RetryDelay      time.Duration
	MaxBatchSize    int
}

// Default returns the production defaults.
func Default() Config {
	return Config{
		Addr:            ":8090",
		TickInterval:    500 * time.Millisecond,
		LeaseTTL:        30 * time.Second,
		HeartbeatWindow: 10 * time.Second,
		RetryDelay:      5 * time.Second,
		MaxBatchSize:    100,
	}
}
