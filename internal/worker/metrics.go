package worker

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the worker's Prometheus counters.
type Metrics struct {
	Claimed       prometheus.Counter
	Finished      *prometheus.CounterVec
	Reaped        prometheus.Counter
	LeaseLost     prometheus.Counter
	StaleRejected prometheus.Counter
	Freezes       prometheus.Counter
	ExecSeconds   prometheus.Histogram
}

// NewMetrics registers worker metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Claimed:       prometheus.NewCounter(prometheus.CounterOpts{Name: "jobq_worker_claimed_total", Help: "Jobs claimed."}),
		Finished:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jobq_worker_finished_total", Help: "Attempts finished, by resulting job state."}, []string{"state"}),
		Reaped:        prometheus.NewCounter(prometheus.CounterOpts{Name: "jobq_worker_reaped_total", Help: "Expired leases reaped by this worker."}),
		LeaseLost:     prometheus.NewCounter(prometheus.CounterOpts{Name: "jobq_worker_lease_lost_total", Help: "Executions abandoned after losing the lease."}),
		StaleRejected: prometheus.NewCounter(prometheus.CounterOpts{Name: "jobq_worker_stale_rejected_total", Help: "Completions refused by fencing (zombie writes)."}),
		Freezes:       prometheus.NewCounter(prometheus.CounterOpts{Name: "jobq_worker_chaos_freezes_total", Help: "Injected stop-the-world pauses."}),
		ExecSeconds:   prometheus.NewHistogram(prometheus.HistogramOpts{Name: "jobq_worker_exec_seconds", Help: "Execution time per attempt.", Buckets: prometheus.DefBuckets}),
	}
	reg.MustRegister(m.Claimed, m.Finished, m.Reaped, m.LeaseLost, m.StaleRejected, m.Freezes, m.ExecSeconds)
	return m
}
