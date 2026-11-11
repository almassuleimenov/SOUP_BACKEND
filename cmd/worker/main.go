// Command worker runs one queue worker (one job at a time).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jobqueue/internal/config"
	"jobqueue/internal/db"
	"jobqueue/internal/migrate"
	"jobqueue/internal/store"
	"jobqueue/internal/worker"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	lvl := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})).With("svc", "worker")
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func loadConfig() (worker.Config, store.Options, error) {
	var errs []error
	dur := func(k string, d time.Duration) time.Duration {
		v, err := config.Duration(k, d)
		errs = append(errs, err)
		return v
	}
	cfg := worker.Config{
		ID:            config.String("WORKER_ID", defaultID()),
		Lease:         dur("LEASE", 3*time.Second),
		Heartbeat:     dur("HEARTBEAT", 750*time.Millisecond),
		PollMin:       dur("POLL_MIN", 20*time.Millisecond),
		PollMax:       dur("POLL_MAX", 500*time.Millisecond),
		ReapEvery:     dur("REAP_EVERY", time.Second),
		RetryBase:     dur("RETRY_BASE", 200*time.Millisecond),
		RetryMax:      dur("RETRY_MAX", 10*time.Second),
		FinishTimeout: dur("FINISH_TIMEOUT", 5*time.Second),
		EffectMode:    worker.EffectInTx,
	}
	if err := errors.Join(errs...); err != nil {
		return cfg, store.Options{}, err
	}
	if cfg.Heartbeat*3 > cfg.Lease {
		return cfg, store.Options{}, fmt.Errorf("HEARTBEAT (%s) must be at most LEASE/3 (%s)", cfg.Heartbeat, cfg.Lease)
	}
	// Fault injection and negative controls exist only in the `chaos` build
	// (chaos_on.go). The production build refuses to start if they are set.
	var opts store.Options
	if err := chaosConfig(&cfg, &opts); err != nil {
		return cfg, opts, err
	}
	return cfg, opts, nil
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, opts, err := loadConfig()
	if err != nil {
		return err
	}
	if opts != (store.Options{}) {
		log.Warn("UNSAFE MODE ENABLED — negative control, results are expected to be wrong", "opts", opts)
	}
	url, err := config.MustDatabaseURL()
	if err != nil {
		return err
	}
	// Exactly three connections: the claim/finish loop, the heartbeat and the
	// reaper. Every worker process holds its own pool, so this is what scales
	// with N (budget: workers*3 + API pool + headroom <= max_connections).
	pool, err := db.Connect(ctx, url, 3, 3, "jobq-worker", 60*time.Second, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	m := worker.NewMetrics(reg)
	if addr := os.Getenv("METRICS_ADDR"); addr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
			hs := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
			if err := hs.ListenAndServe(); err != nil {
				log.Warn("metrics server stopped", "err", err)
			}
		}()
	}

	return worker.New(cfg, store.New(pool, opts), log, m).Run(ctx)
}

func defaultID() string {
	host, _ := os.Hostname()
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
}
