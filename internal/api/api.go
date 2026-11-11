// Package api is the HTTP front end of the job queue.
package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"jobqueue/internal/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	maxBodyBytes       = 64 << 10
	maxKeyLen          = 255
	defaultPageSize    = 50
	maxPageSize        = 200
	defaultMaxAttempts = 3
	maxMaxAttempts     = 20
)

var (
	typeRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)
	uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// Server holds handler dependencies.
type Server struct {
	st       *store.Store
	log      *slog.Logger
	apiToken string // optional bearer token; empty disables auth
	reqs     *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	gatherer prometheus.Gatherer
}

// New builds the server and registers its metrics.
func New(st *store.Store, log *slog.Logger, apiToken string, reg *prometheus.Registry) *Server {
	s := &Server{
		st: st, log: log, apiToken: apiToken, gatherer: reg,
		reqs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobq_http_requests_total", Help: "HTTP requests by route and status."}, []string{"route", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "jobq_http_request_seconds", Help: "HTTP latency by route.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}}, []string{"route"}),
	}
	reg.MustRegister(s.reqs, s.latency, newQueueCollector(st))
	return s
}

// Handler returns the routed handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/jobs", s.wrap("create", s.auth(s.createJob)))
	mux.Handle("GET /v1/jobs", s.wrap("list", s.auth(s.listJobs)))
	mux.Handle("GET /v1/jobs/{id}", s.wrap("get", s.auth(s.getJob)))
	mux.Handle("POST /v1/jobs/{id}/cancel", s.wrap("cancel", s.auth(s.cancelJob)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)
	// Same bearer token as the API when API_TOKEN is set (Prometheus supports
	// authorization.credentials). Queue sizes are not public information.
	mux.Handle("GET /metrics", s.auth(promhttp.HandlerFor(s.gatherer, promhttp.HandlerOpts{}).ServeHTTP))
	return mux
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

type createRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", err.Error())
		return
	}
	var req createRequest
	if err := decodeStrict(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if !typeRe.MatchString(req.Type) {
		writeError(w, http.StatusBadRequest, "invalid_type", "type must match "+typeRe.String())
		return
	}
	validate, known := payloadValidators[req.Type]
	if !known {
		writeError(w, http.StatusBadRequest, "unknown_type", "no worker can execute job type "+strconv.Quote(req.Type))
		return
	}
	payload, err := canonicalObject(req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_payload", err.Error())
		return
	}
	if err := validate(payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_payload", err.Error())
		return
	}
	maxAttempts := defaultMaxAttempts
	if req.MaxAttempts != nil {
		maxAttempts = *req.MaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > maxMaxAttempts {
		writeError(w, http.StatusBadRequest, "invalid_max_attempts", fmt.Sprintf("max_attempts must be 1..%d", maxMaxAttempts))
		return
	}

	job, created, err := s.st.CreateJob(r.Context(), store.CreateParams{
		IdempotencyKey: key, Type: req.Type, Payload: payload, MaxAttempts: maxAttempts})
	switch {
	case errors.Is(err, store.ErrIdempotencyMismatch):
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with a different request body")
		return
	case err != nil:
		s.internal(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+job.ID)
	if created {
		writeJSON(w, http.StatusCreated, job)
		return
	}
	w.Header().Set("Idempotent-Replayed", "true")
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	job, err := s.st.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

type listResponse struct {
	Items      []store.Job `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := store.ListParams{Limit: defaultPageSize}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPageSize {
			writeError(w, http.StatusBadRequest, "invalid_limit", fmt.Sprintf("limit must be 1..%d", maxPageSize))
			return
		}
		p.Limit = n
	}
	if v := q.Get("state"); v != "" {
		st := store.State(v)
		if !st.Valid() {
			writeError(w, http.StatusBadRequest, "invalid_state", "unknown state")
			return
		}
		p.State = st
	}
	if v := q.Get("cursor"); v != "" {
		seq, err := decodeCursor(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "malformed cursor")
			return
		}
		p.BeforeSeq = seq
	}
	jobs, err := s.st.ListJobs(r.Context(), p)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	resp := listResponse{Items: jobs}
	if len(jobs) == p.Limit {
		resp.NextCursor = encodeCursor(jobs[len(jobs)-1].Seq)
	}
	writeJSON(w, http.StatusOK, resp)
}

type cancelResponse struct {
	Outcome string    `json:"outcome"`
	Job     store.Job `json:"job"`
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	job, outcome, err := s.st.CancelJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	switch outcome {
	case store.CancelledNow:
		writeJSON(w, http.StatusOK, cancelResponse{"cancelled", job})
	case store.AlreadyCancelled:
		writeJSON(w, http.StatusOK, cancelResponse{"already_cancelled", job})
	case store.CancelRequested:
		// Accepted: the running worker will observe the flag within one
		// heartbeat; the final state is guaranteed to be cancelled.
		writeJSON(w, http.StatusAccepted, cancelResponse{"cancel_requested", job})
	default:
		writeJSON(w, http.StatusConflict, cancelResponse{"already_" + string(job.State), job})
	}
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.st.Pool().Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "db_unavailable", "database unreachable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ---------------------------------------------------------------------------
// middleware & helpers
// ---------------------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) { r.code = code; r.ResponseWriter.WriteHeader(code) }

func (s *Server) wrap(route string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic in handler", "route", route, "panic", p)
				writeError(rec, http.StatusInternalServerError, "internal", "internal error")
			}
			s.reqs.WithLabelValues(route, strconv.Itoa(rec.code)).Inc()
			s.latency.WithLabelValues(route).Observe(time.Since(start).Seconds())
		}()
		h(rec, r)
	})
}

func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	if s.apiToken == "" {
		return h
	}
	want := []byte("Bearer " + s.apiToken)
	return func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		h(w, r)
	}
}

func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) {
		// Client went away; nothing useful to send.
		return
	}
	s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	if store.IsRetryable(err) {
		// Transient (connection limit, failover, serialization): tell the client
		// a retry is safe. With the same Idempotency-Key it cannot duplicate.
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "temporarily unavailable, retry with the same Idempotency-Key")
		return
	}
	writeError(w, http.StatusInternalServerError, "internal", "internal error")
}

func validateKey(k string) error {
	if k == "" {
		return errors.New("Idempotency-Key header is required")
	}
	if len(k) > maxKeyLen {
		return fmt.Errorf("Idempotency-Key longer than %d bytes", maxKeyLen)
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return errors.New("Idempotency-Key must be printable ASCII without spaces")
		}
	}
	return nil
}

func decodeStrict(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("decode body: %w", err)
	}
	if dec.More() {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}

// canonicalObject re-encodes a JSON object with sorted keys, so semantically
// equal payloads hash equally for idempotency comparison.
func canonicalObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var obj map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, errors.New("payload must be a JSON object")
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return out, nil
}

func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("v1:" + strconv.FormatInt(seq, 10)))
}

func decodeCursor(c string) (int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, err
	}
	s, ok := strings.CutPrefix(string(b), "v1:")
	if !ok {
		return 0, errors.New("bad cursor version")
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.New("bad cursor")
	}
	return n, nil
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, code int, errCode, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = errCode, msg
	writeJSON(w, code, b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v) // client disconnects are not actionable
}
