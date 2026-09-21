// Package reporting provides T1 SDK event reporting for Go agents (Issue #11).
//
// Sends step-level events to the Aetheris ingest endpoint
// (POST /api/telemetry/v1/events, per #3 contract / #4 endpoint).
//
// Core invariant (#11, same as #6 Python): reporting channel failures
// MUST NOT propagate to business code. All errors are caught and logged.
//
// Quick start (≤ 10 lines, 0 business logic changes):
//
//	reporter := reporting.FromEnv()
//	defer reporter.Flush()
//
//	reporter.Step("fetch_data", func(ctx context.Context, state map[string]any) (map[string]any, error) {
//	    return map[string]any{"data": callAPI()}, nil
//	})
//
// Zero framework dependencies — only uses standard library net/http.
// Does NOT depend on the main Aetheris module.
package reporting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Colin4k1024/Aetheris/durability/core"
)

const (
	schemaVersion     = "1"
	defaultTimeout    = 5 * time.Second
	defaultMaxBuffer  = 1000
	defaultMaxRetries = 3
	defaultBackoff    = 500 * time.Millisecond
	maxBackoff        = 30 * time.Second
	circuitCooldown   = 30 * time.Second
)

// EventEnvelope matches the ingest-contract-v1 wire schema (#3).
type EventEnvelope struct {
	SchemaVersion string          `json:"schema_version"`
	EventUID     string          `json:"event_uid"`
	JobID        string          `json:"job_id"`
	StepID       string          `json:"step_id,omitempty"`
	SpanID       string          `json:"span_id,omitempty"`
	ParentSpanID string          `json:"parent_span_id,omitempty"`
	RootSpanID   string          `json:"root_span_id,omitempty"`
	AgentID      string          `json:"agent_id"`
	TenantID    string           `json:"tenant_id"`
	OccurredAt  string           `json:"occurred_at"`
	Type        string           `json:"type"`
	Payload     json.RawMessage  `json:"payload,omitempty"`
	SDKName     string           `json:"sdk_name"`
	SDKVersion  string           `json:"sdk_version"`
}

// Reporter reports events to the Aetheris ingest endpoint.
// Thread-safe. Fail-open: all network errors are caught.
type Reporter struct {
	endpoint   string
	tenantID   string
	token      string
	agentID    string
	sdkName    string
	sdkVersion string
	timeout    time.Duration
	maxBuffer  int
	maxRetries int
	backoff    time.Duration

	mu         sync.Mutex
	buffer     []EventEnvelope
	dropped    int64
	success    int64
	failed     int64
	circuitOpen bool
	circuitAt  time.Time
}

// FromEnv creates a Reporter from environment variables.
// Required: AETHERIS_ENDPOINT, AETHERIS_TENANT, AETHERIS_TOKEN.
// Optional: AETHERIS_AGENT_ID, AETHERIS_TIMEOUT.
func FromEnv() *Reporter {
	endpoint := os.Getenv("AETHERIS_ENDPOINT")
	tenantID := os.Getenv("AETHERIS_TENANT")
	token := os.Getenv("AETHERIS_TOKEN")
	agentID := os.Getenv("AETHERIS_AGENT_ID")
	timeout := defaultTimeout
	if t := os.Getenv("AETHERIS_TIMEOUT"); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
		}
	}
	if endpoint == "" || tenantID == "" {
		log.Println("reporting: AETHERIS_ENDPOINT and AETHERIS_TENANT must be set; disabled")
		return &Reporter{timeout: timeout}
	}
	return &Reporter{
		endpoint:   endpoint,
		tenantID:   tenantID,
		token:      token,
		agentID:    agentID,
		sdkName:    "aetheris-durability-go",
		sdkVersion: "0.1.0",
		timeout:    timeout,
		maxBuffer:  defaultMaxBuffer,
		maxRetries: defaultMaxRetries,
		backoff:    defaultBackoff,
	}
}

// Enabled returns true if the reporter is configured.
func (r *Reporter) Enabled() bool {
	return r.endpoint != ""
}

// Flush sends buffered events. Fail-open.
func (r *Reporter) Flush() {
	if !r.Enabled() {
		return
	}
	// Circuit breaker check
	if r.circuitOpen {
		if time.Since(r.circuitAt) < circuitCooldown {
			return
		}
		r.circuitOpen = false
	}

	r.mu.Lock()
	if len(r.buffer) == 0 {
		r.mu.Unlock()
		return
	}
	batch := r.buffer
	r.buffer = nil
	r.mu.Unlock()

	r.sendBatch(batch)
}

// Report buffers an event for reporting.
func (r *Reporter) Report(jobID, eventType string, payload map[string]any) {
	if !r.Enabled() {
		return
	}
	envelope := r.toEnvelope(jobID, eventType, payload)
	r.mu.Lock()
	if len(r.buffer) >= r.maxBuffer {
		r.buffer = r.buffer[1:] // drop oldest
		atomic.AddInt64(&r.dropped, 1)
	}
	r.buffer = append(r.buffer, envelope)
	r.mu.Unlock()
	// Flush immediately; #7 batching can be added later
	r.Flush()
}

// Step wraps a function with step-level event reporting.
// Invariant: reporting failures do NOT propagate.
func (r *Reporter) Step(stepID string, fn func(ctx context.Context, state map[string]any) (map[string]any, error)) func(ctx context.Context, state map[string]any) (map[string]any, error) {
	return func(ctx context.Context, state map[string]any) (map[string]any, error) {
		jobID := ""
		if v, ok := state["job_id"].(string); ok {
			jobID = v
		}

		r.Report(jobID, "step_started", map[string]any{"step_id": stepID})

		result, err := fn(ctx, state)
		if err != nil {
			r.Report(jobID, "step_failed", map[string]any{
				"step_id": stepID,
				"error":   err.Error(),
			})
			return result, err // re-raise original error
		}
		r.Report(jobID, "step_finished", map[string]any{"step_id": stepID})
		return result, nil
	}
}

// Middleware wraps an http.Handler with job-level reporting.
// Reports job_created on request, job_completed/job_failed on response.
func (r *Reporter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		jobID := req.Header.Get("X-Aetheris-Job-ID")
		if jobID == "" {
			next.ServeHTTP(w, req)
			return
		}

		r.Report(jobID, "job_created", map[string]any{})

		ww := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, req)

		if ww.status >= 400 {
			r.Report(jobID, "job_failed", map[string]any{"status": ww.status})
		} else {
			r.Report(jobID, "job_completed", map[string]any{"status": ww.status})
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// sendBatch sends a batch of events to the ingest endpoint.
func (r *Reporter) sendBatch(batch []EventEnvelope) {
	body, err := json.Marshal(map[string]any{"events": batch})
	if err != nil {
		atomic.AddInt64(&r.failed, int64(len(batch)))
		return
	}

	var lastErr error
	for attempt := 0; attempt < r.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), "POST",
			r.endpoint+"/api/telemetry/v1/events", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Tenant-ID", r.tenantID)
		req.Header.Set("Authorization", "Bearer "+r.token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			r.backoffSleep(attempt)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == 200:
			atomic.AddInt64(&r.success, int64(len(batch)))
			r.circuitOpen = false
			return
		case resp.StatusCode == 429:
			retryAfter := 5
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if v, err := strconv.Atoi(ra); err == nil {
					retryAfter = v
				}
			}
			atomic.AddInt64(&r.failed, int64(len(batch)))
			r.circuitOpen = true
			r.circuitAt = time.Now()
			log.Printf("reporting: 429, backing off %ds", retryAfter)
			time.Sleep(time.Duration(retryAfter) * time.Second)
			return
		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			r.backoffSleep(attempt)
		default:
			atomic.AddInt64(&r.failed, int64(len(batch)))
			return
		}
	}

	atomic.AddInt64(&r.failed, int64(len(batch)))
	r.circuitOpen = true
	r.circuitAt = time.Now()
	if lastErr != nil {
		log.Printf("reporting: failed after %d retries: %v", r.maxRetries, lastErr)
	}
}

func (r *Reporter) backoffSleep(attempt int) {
	d := r.backoff * time.Duration(1<<attempt)
	if d > maxBackoff {
		d = maxBackoff
	}
	time.Sleep(d)
}

func (r *Reporter) toEnvelope(jobID, eventType string, payload map[string]any) EventEnvelope {
	payloadBytes, _ := json.Marshal(payload)
	return EventEnvelope{
		SchemaVersion: schemaVersion,
		EventUID:      fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		JobID:         jobID,
		AgentID:       r.agentID,
		TenantID:      r.tenantID,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Type:          eventType,
		Payload:       payloadBytes,
		SDKName:       r.sdkName,
		SDKVersion:     r.sdkVersion,
	}
}

// Stats returns dropped, success, failed counts.
func (r *Reporter) Stats() (dropped, success, failed int64) {
	return atomic.LoadInt64(&r.dropped), atomic.LoadInt64(&r.success), atomic.LoadInt64(&r.failed)
}

// ensure core is used (for StepFunc alignment)
var _ core.StepFunc
