// Command server runs the HTTP API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jobqueue/internal/api"
	"jobqueue/internal/config"
	"jobqueue/internal/db"
	"jobqueue/internal/migrate"
	"jobqueue/internal/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("svc", "api")
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	url, err := config.MustDatabaseURL()
	if err != nil {
		return err
	}
	maxConns, err := config.Int("DB_MAX_CONNS", 20)
	if err != nil {
		return err
	}
	pool, err := db.Connect(ctx, url, int32(maxConns)/2, int32(maxConns), "jobq-api", 60*time.Second, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	srv := api.New(store.New(pool, store.Options{}), log, os.Getenv("API_TOKEN"), reg)

	addr := config.String("HTTP_ADDR", ":8080")
	hs := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", addr)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
}
