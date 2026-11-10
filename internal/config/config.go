// Package config reads process configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// String returns the env var or a default.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Duration parses a Go duration ("2s", "500ms") or returns the default.
func Duration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

// Float parses a float or returns the default.
func Float(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return f, nil
}

// Int parses an int or returns the default.
func Int(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

// MustDatabaseURL returns DATABASE_URL or fails.
func MustDatabaseURL() (string, error) {
	u := os.Getenv("DATABASE_URL")
	if u == "" {
		return "", fmt.Errorf("DATABASE_URL is required")
	}
	return u, nil
}
