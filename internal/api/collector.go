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
		depth:    prometheus.NewDesc("jobq_jobs", "Jobs in a live state (queued, running).", []string{"state"}, nil),
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
	// Only the live part of the queue is counted, via jobs_state_seq_idx, so a
	// scrape costs O(queued + running), not a full scan of a table that only
	// grows. EXPLAIN ANALYZE on 200k terminal + 500 live rows: Index Scan,
	// 1.0 ms (the old GROUP BY state over all rows: Seq Scan, 105 ms).
	// Terminal totals come from the workers' jobq_worker_finished_total.
	var queued, running, expired int64
	var oldest float64
	err := c.st.Pool().QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE state = 'queued'),
		count(*) FILTER (WHERE state = 'running'),
		COALESCE(EXTRACT(EPOCH FROM now() - min(run_after) FILTER (WHERE state = 'queued' AND run_after <= now())), 0)::float8,
		count(*) FILTER (WHERE state = 'running' AND lease_expires_at < now())
		FROM jobs WHERE state IN ('queued', 'running')`).Scan(&queued, &running, &oldest, &expired)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.scrapeOK, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(queued), string(store.StateQueued))
	ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(running), string(store.StateRunning))
	ch <- prometheus.MustNewConstMetric(c.oldest, prometheus.GaugeValue, oldest)
	ch <- prometheus.MustNewConstMetric(c.expired, prometheus.GaugeValue, float64(expired))
	ch <- prometheus.MustNewConstMetric(c.scrapeOK, prometheus.GaugeValue, 1)
}
