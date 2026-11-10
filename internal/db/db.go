// Package db opens the Postgres pool with startup retries.
package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a pool and waits (up to wait) for the database to accept
// connections, so containers can start before Postgres is ready.
func Connect(ctx context.Context, url string, minConns, maxConns int32, appName string, wait time.Duration, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	// Keep connections warm: opening a Postgres backend costs milliseconds
	// (much more on Windows / under CPU pressure), and a burst of requests
	// that all dial at once turns into connect timeouts.
	cfg.MinConns = min(minConns, maxConns)
	cfg.ConnConfig.RuntimeParams["application_name"] = appName
	// Bound every statement server-side too: a stuck query must not hold a
	// row lock forever.
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "15000"
	cfg.ConnConfig.ConnectTimeout = 3 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	deadline := time.Now().Add(wait)
	for {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = pool.Ping(pctx)
		cancel()
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("database not reachable after %s: %w", wait, err)
		}
		log.Info("waiting for database", "err", err)
		time.Sleep(500 * time.Millisecond)
	}
}
