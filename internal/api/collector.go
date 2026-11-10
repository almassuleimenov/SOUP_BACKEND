package api

import (
	"context"
	"time"

	"jobqueue/internal/store"

	"github.com/prometheus/client_golang/prometheus"
)

// queueCollector exposes queue depth per state at scrape time, plus the age
// of the oldest queued job (the SLI that actually tells you workers are
// keeping up) and the number of running jobs with an expired lease (dead
// workers not yet reaped).
type queueCollector struct {
	st       *store.Store
	depth    *prometheus.Desc
	oldest   *prometheus.Desc
	expired  *prometheus.Desc
	scrapeOK *prometheus.Desc
}

func newQueueCollector(st *store.Store) *queueCollector {
	return &queueCollector{
		st:       st,
		depth:    prometheus.NewDesc("jobq_jobs", "Jobs by state.", []string{"state"}, nil),
		oldest:   prometheus.NewDesc("jobq_oldest_queued_seconds", "Age of the oldest runnable queued job.", nil, nil),
		expired:  prometheus.NewDesc("jobq_expired_leases", "Running jobs whose lease has expired (unreaped dead workers).", nil, nil),
		scrapeOK: prometheus.NewDesc("jobq_queue_scrape_ok", "1 if the queue stats query succeeded.", nil, nil),
	}
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.oldest
	ch <- c.expired
	ch <- c.scrapeOK
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := c.st.CountByState(ctx)
	var oldest float64
	var expired int64
	if err == nil {
		err = c.st.Pool().QueryRow(ctx, `SELECT
			COALESCE(EXTRACT(EPOCH FROM now() - min(run_after)) FILTER (WHERE state = 'queued' AND run_after <= now()), 0),
			count(*) FILTER (WHERE state = 'running' AND lease_expires_at < now())
			FROM jobs WHERE state IN ('queued', 'running')`).Scan(&oldest, &expired)
	}
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.scrapeOK, prometheus.GaugeValue, 0)
		return
	}
	for _, s := range []store.State{store.StateQueued, store.StateRunning, store.StateSucceeded, store.StateFailed, store.StateCancelled} {
		ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(counts[s]), string(s))
	}
	ch <- prometheus.MustNewConstMetric(c.oldest, prometheus.GaugeValue, oldest)
	ch <- prometheus.MustNewConstMetric(c.expired, prometheus.GaugeValue, float64(expired))
	ch <- prometheus.MustNewConstMetric(c.scrapeOK, prometheus.GaugeValue, 1)
}
