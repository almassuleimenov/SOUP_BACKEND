package api

import (
	"context"
	"os"
	"strings"
	"testing"

	"jobqueue/internal/migrate"
	"jobqueue/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The queue collector's SQL once had a syntax error (FILTER attached to
// EXTRACT instead of min()), so every scrape silently reported
// jobq_queue_scrape_ok 0 and no queue metrics. Nothing scraped /metrics in
// the harness, so it went unnoticed. This test runs the real query.
func TestQueueCollectorAgainstPostgres(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE job_effects, job_runs, job_transitions, jobs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	st := store.New(pool, store.Options{})
	for _, k := range []string{"a", "b", "c"} {
		if _, _, err := st.CreateJob(ctx, store.CreateParams{IdempotencyKey: k, Type: "train", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
			t.Fatal(err)
		}
	}
	if c, err := st.Claim(ctx, "w", 60e9); err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(newQueueCollector(st))
	want := `
# HELP jobq_jobs Jobs in a live state (queued, running).
# TYPE jobq_jobs gauge
jobq_jobs{state="queued"} 2
jobq_jobs{state="running"} 1
# HELP jobq_queue_scrape_ok 1 if the queue stats query succeeded.
# TYPE jobq_queue_scrape_ok gauge
jobq_queue_scrape_ok 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "jobq_jobs", "jobq_queue_scrape_ok"); err != nil {
		t.Fatal(err)
	}
}
