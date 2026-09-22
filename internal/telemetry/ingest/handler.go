// Copyright 2026 fanjia1024
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ingest implements the T1 SDK event reporting endpoint
// (Issue #4: POST /api/telemetry/v1/events).
//
// It receives batched events from T1 SDKs (Python #6, Go #11),
// validates them against the ingest-contract-v1 schema, deduplicates
// by (tenant_id, job_id, event_uid), and appends to the jobstore
// through the RedactingStore (#5).
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Colin4k1024/Aetheris/v2/internal/runtime/jobstore"
	"github.com/Colin4k1024/Aetheris/v2/pkg/auth"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// JobOwnershipChecker verifies that a job_id belongs to a tenant.
// Implemented by agent/job.JobStore.Get (returns job with TenantID).
// This interface avoids a direct dependency on internal/agent/job.
type JobOwnershipChecker interface {
	// CheckJobOwnership returns the tenant_id of the job, or empty
	// string if the job doesn't exist or belongs to a different tenant.
	CheckJobOwnership(ctx context.Context, jobID, tenantID string) bool
}

const (
	// maxBatchSize is the maximum number of events per batch request.
	maxBatchSize = 100
	// maxPayloadSize is the maximum size of a single event payload in bytes.
	maxPayloadSize = 256 * 1024 // 256KB per event
	// rateLimitWindow is the sliding window for rate limiting.
	rateLimitWindow = time.Minute
	// rateLimitMaxRequests is the maximum requests per window per tenant.
	rateLimitMaxRequests = 600
)

// EventEnvelope matches the ingest-contract-v1 wire schema (#3).
type EventEnvelope struct {
	SchemaVersion string          `json:"schema_version"`
	EventUID      string          `json:"event_uid"`
	JobID         string          `json:"job_id"`
	StepID        string          `json:"step_id,omitempty"`
	SpanID        string          `json:"span_id,omitempty"`
	ParentSpanID  string          `json:"parent_span_id,omitempty"`
	RootSpanID    string          `json:"root_span_id,omitempty"`
	AgentID       string          `json:"agent_id"`
	TenantID      string          `json:"tenant_id"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	OccurredAt    string          `json:"occurred_at"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	SDKName       string          `json:"sdk_name"`
	SDKVersion    string          `json:"sdk_version"`
}

// BatchRequest is the body of POST /api/telemetry/v1/events.
type BatchRequest struct {
	Events []EventEnvelope `json:"events"`
}

// BatchResponse is the response body.
type BatchResponse struct {
	AcceptedVersion string         `json:"accepted_version"`
	Accepted        int            `json:"accepted"`
	Deduped         int            `json:"deduped"`
	Rejected        int            `json:"rejected"`
	Rejections      []RejectionEntry `json:"rejections,omitempty"`
}

// RejectionEntry describes a rejected event.
type RejectionEntry struct {
	EventUID string `json:"event_uid"`
	Reason   string `json:"reason"`
}

// Handler handles telemetry event ingestion.
type Handler struct {
	store    jobstore.JobStore
	mu       sync.Mutex
	// dedup tracks (tenant, jobID, eventUID) -> seen.
	// In production this should be backed by Redis or the jobstore;
	// this in-memory map is for single-instance use.
	dedup       map[string]bool
	rateLimiter *rateLimiter
	// ownershipChecker verifies job_id belongs to the tenant (P0-A fix)
	ownershipChecker JobOwnershipChecker
}

// NewHandler creates an ingest handler. The store should be a
// RedactingStore-wrapped JobStore (#5) to ensure redaction on write.
func NewHandler(store jobstore.JobStore) *Handler {
	return &Handler{
		store:       store,
		dedup:       make(map[string]bool),
		rateLimiter: newRateLimiter(rateLimitWindow, rateLimitMaxRequests),
	}
}

// SetOwnershipChecker sets the job ownership checker (P0-A fix).
// Without this, cross-tenant job_id writes are not blocked.
func (h *Handler) SetOwnershipChecker(checker JobOwnershipChecker) {
	h.ownershipChecker = checker
}

// IngestEvents handles POST /api/telemetry/v1/events.
func (h *Handler) IngestEvents(ctx context.Context, c *app.RequestContext) {
	// Tenant from auth context (set by InjectAuthContext middleware)
	tenantID := auth.GetTenantID(ctx)
	if tenantID == "" {
		c.JSON(consts.StatusForbidden, map[string]string{
			"error": "tenant_id required",
		})
		return
	}

	// Rate limit check
	if !h.rateLimiter.allow(tenantID) {
		c.Header("Retry-After", "60")
		c.JSON(consts.StatusTooManyRequests, map[string]any{
			"error":       "rate limit exceeded",
			"retry_after": 60,
		})
		return
	}

	var req BatchRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("invalid request body: %v", err),
		})
		return
	}

	if len(req.Events) == 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": "empty batch",
		})
		return
	}
	if len(req.Events) > maxBatchSize {
		c.JSON(consts.StatusRequestEntityTooLarge, map[string]string{
			"error": fmt.Sprintf("batch size %d exceeds max %d", len(req.Events), maxBatchSize),
		})
		return
	}

	resp := h.processBatch(ctx, tenantID, req.Events)
	c.JSON(consts.StatusOK, resp)
}

// processBatch processes each event in the batch, returning aggregate stats.
func (h *Handler) processBatch(ctx context.Context, tenantID string, events []EventEnvelope) BatchResponse {
	resp := BatchResponse{
		AcceptedVersion: "1",
	}

	for _, ev := range events {
		entry := h.processEvent(ctx, tenantID, ev)
		switch entry.outcome {
		case "accepted":
			resp.Accepted++
		case "deduped":
			resp.Deduped++
		case "rejected":
			resp.Rejected++
			resp.Rejections = append(resp.Rejections, RejectionEntry{
				EventUID: ev.EventUID,
				Reason:   entry.reason,
			})
		}
	}
	return resp
}

type eventOutcome struct {
	outcome string // accepted, deduped, rejected
	reason  string
}

// processEvent validates, deduplicates, and appends a single event.
func (h *Handler) processEvent(ctx context.Context, tenantID string, ev EventEnvelope) eventOutcome {
	// Validate schema_version
	if ev.SchemaVersion == "" {
		// Legacy: accept with version 0
		// Fall through but mark accepted_version accordingly
	} else if ev.SchemaVersion != "1" {
		return eventOutcome{"rejected", "unsupported_schema_version"}
	}

	// Validate required fields
	if ev.EventUID == "" || ev.JobID == "" || ev.AgentID == "" ||
		ev.OccurredAt == "" || ev.Type == "" || ev.SDKName == "" {
		return eventOutcome{"rejected", "missing_required_fields"}
	}

	// Tenant consistency check
	if ev.TenantID != tenantID {
		return eventOutcome{"rejected", "tenant_mismatch"}
	}

	// P0-A: Verify job ownership — job_id must belong to this tenant
	if h.ownershipChecker != nil {
		if !h.ownershipChecker.CheckJobOwnership(ctx, ev.JobID, tenantID) {
			return eventOutcome{"rejected", "job_not_owned"}
		}
	}

	// Validate job_id format
	if !isValidJobID(ev.JobID) {
		return eventOutcome{"rejected", "invalid_job_id"}
	}

	// Validate payload size
	if len(ev.Payload) > maxPayloadSize {
		return eventOutcome{"rejected", "payload_too_large"}
	}

	// Validate event type is reportable
	if !isReportableType(ev.Type) {
		return eventOutcome{"rejected", "unknown_type"}
	}

	// Parse occurred_at
	occurredAt, err := time.Parse(time.RFC3339, ev.OccurredAt)
	if err != nil {
		return eventOutcome{"rejected", "invalid_timestamp"}
	}

	// Check for future timestamp (> 5min ahead of server time)
	if occurredAt.After(time.Now().Add(5 * time.Minute)) {
		return eventOutcome{"rejected", "future_timestamp"}
	}

	// Dedup check: (tenant, job_id, event_uid) — P1-A fix: hold lock
	// through check+mark to prevent TOCTOU race
	dedupKey := fmt.Sprintf("%s:%s:%s", tenantID, ev.JobID, ev.EventUID)
	h.mu.Lock()
	if h.dedup[dedupKey] {
		h.mu.Unlock()
		return eventOutcome{"deduped", ""}
	}
	// P1-A fix: mark as seen NOW (before Append) to prevent concurrent
	// duplicates from passing the check. If Append fails, we leave the
	// mark (duplicate retry will dedup, which is safe — the event just
	// didn't land, but a retry will get deduped; acceptable trade-off
	// vs the alternative of allowing duplicates).
	h.dedup[dedupKey] = true
	h.mu.Unlock()

	// Build JobEvent
	je := jobstore.JobEvent{
		JobID:     ev.JobID,
		Type:      jobstore.EventType(ev.Type),
		Payload:   []byte(ev.Payload),
		CreatedAt: time.Now(), // server-side recorded_at (F1 fix)
	}
	if len(je.Payload) == 0 {
		je.Payload = []byte(`{}`)
	}

	// P0-C: Check if job is in terminal state
	events, version, err := h.store.ListEvents(ctx, ev.JobID)
	if err != nil {
		return eventOutcome{"rejected", "store_error"}
	}
	if isJobTerminal(events) {
		return eventOutcome{"rejected", "job_terminal"}
	}

	// P0-B: Multi-round retry for version conflicts (up to 10 rounds)
	// P2-A: Only retry on ErrVersionMismatch, not on other errors
	maxRetries := 10
	for attempt := 0; attempt <= maxRetries; attempt++ {
		_, err = h.store.Append(ctx, ev.JobID, version, je)
		if err == nil {
			return eventOutcome{"accepted", ""}
		}
		if !errors.Is(err, jobstore.ErrVersionMismatch) {
			// Non-retryable error (store failure, redaction failure)
			return eventOutcome{"rejected", "append_failed"}
		}
		// Version mismatch: re-read version and retry
		_, version, err = h.store.ListEvents(ctx, ev.JobID)
		if err != nil {
			return eventOutcome{"rejected", "store_error"}
		}
		// Small jitter to reduce contention
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * time.Millisecond)
		}
	}
	return eventOutcome{"rejected", "append_failed"}
}

// isValidJobID checks the job_id format per ingest-contract-v1 §2.2.
func isValidJobID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// isReportableType checks if the event type is in the reportable set
// per ingest-contract-v1 §3.1 (checkpoint_loaded is NOT reportable).
// P1-B fix: added decision_snapshot, reasoning_snapshot.
func isReportableType(t string) bool {
	switch t {
	case "job_created", "job_started", "job_completed", "job_failed",
		"job_cancelled", "step_started", "step_finished", "step_failed",
		"step_retried", "step_skipped", "checkpoint_saved", "effect_recorded",
		"decision_snapshot", "reasoning_snapshot":
		return true
	default:
		return false
	}
}

// isJobTerminal checks if the job's latest event is a terminal state
// (P0-C fix: terminal jobs must not accept business events).
func isJobTerminal(events []jobstore.JobEvent) bool {
	if len(events) == 0 {
		return false
	}
	last := events[len(events)-1]
	switch last.Type {
	case jobstore.JobCompleted, jobstore.JobFailed, jobstore.JobCancelled:
		return true
	default:
		return false
	}
}

// rateLimiter is a simple sliding-window rate limiter per tenant.
type rateLimiter struct {
	mu       sync.Mutex
	window   time.Duration
	maxReqs  int
	requests map[string][]time.Time
}

func newRateLimiter(window time.Duration, max int) *rateLimiter {
	return &rateLimiter{
		window:   window,
		maxReqs:  max,
		requests: make(map[string][]time.Time),
	}
}

func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rl.window)

	// Filter out expired entries
	reqs := rl.requests[key]
	valid := reqs[:0]
	for _, t := range reqs {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}

	if len(valid) >= rl.maxReqs {
		rl.requests[key] = valid
		return false
	}

	valid = append(valid, now)
	rl.requests[key] = valid
	return true
}

// RegisterRoutes registers the telemetry routes on the Hertz server.
// Enabled by config flag api.telemetry.enabled (default false).
func (h *Handler) RegisterRoutes(group interface {
	POST(path string, handlers ...app.HandlerFunc)
}, authChainWith func(auth.Permission, app.HandlerFunc) []app.HandlerFunc) {
	group.POST("/v1/events", authChainWith(auth.PermissionJobCreate, h.IngestEvents)...)
}

// HealthCheck returns nil if the handler is ready to serve.
func (h *Handler) HealthCheck(ctx context.Context) error {
	if h.store == nil {
		return fmt.Errorf("ingest handler: store not initialized")
	}
	return nil
}
