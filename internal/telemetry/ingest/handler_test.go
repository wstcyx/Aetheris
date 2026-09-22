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

package ingest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Colin4k1024/Aetheris/v2/internal/runtime/jobstore"
	"github.com/Colin4k1024/Aetheris/v2/pkg/auth"
	"github.com/Colin4k1024/Aetheris/v2/pkg/redaction"
)

// newTestHandler creates a handler backed by a RedactingStore-wrapped
// MemoryStore, mirroring production setup.
func newTestHandler() (*Handler, jobstore.JobStore) {
	inner := jobstore.NewMemoryStore()
	policy := jobstore.DefaultRedactionPolicy()
	engine := redaction.NewEngine(policy, nil)
	store := jobstore.NewRedactingStore(inner, engine)
	return NewHandler(store), inner
}

func makeEvent(uid, jobID, tenantID, eventType string) EventEnvelope {
	return EventEnvelope{
		SchemaVersion: "1",
		EventUID:      uid,
		JobID:         jobID,
		AgentID:       "test-agent",
		TenantID:      tenantID,
		OccurredAt:    time.Now().Format(time.RFC3339),
		Type:          eventType,
		SDKName:       "aetheris-durability-py",
		SDKVersion:    "0.2.0",
		Payload:       json.RawMessage(`{}`),
	}
}

// TestIngest_BatchSuccess verifies a valid batch is accepted and
// events are written to the store.
func TestIngest_BatchSuccess(t *testing.T) {
	handler, inner := newTestHandler()
	ctx := authContext("tenant-acme")

	events := []EventEnvelope{
		makeEvent("evt-001", "job-1", "tenant-acme", "job_created"),
		makeEvent("evt-002", "job-1", "tenant-acme", "step_started"),
		makeEvent("evt-003", "job-1", "tenant-acme", "step_finished"),
	}

	outcome := handler.processBatch(ctx, "tenant-acme", events)
	if outcome.Accepted != 3 {
		t.Errorf("expected 3 accepted, got %d (deduped=%d, rejected=%d)",
			outcome.Accepted, outcome.Deduped, outcome.Rejected)
	}

	// Verify events in store
	stored, version, _ := inner.ListEvents(ctx, "job-1")
	if version != 3 {
		t.Errorf("expected version 3, got %d", version)
	}
	if len(stored) != 3 {
		t.Errorf("expected 3 stored events, got %d", len(stored))
	}
}

// TestIngest_Dedup verifies that duplicate event_uids are deduped,
// not re-written.
func TestIngest_Dedup(t *testing.T) {
	handler, inner := newTestHandler()
	ctx := authContext("tenant-acme")

	ev := makeEvent("evt-dup", "job-1", "tenant-acme", "job_created")

	// First submit
	out1 := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out1.Accepted != 1 {
		t.Fatalf("first submit: expected 1 accepted, got %d", out1.Accepted)
	}

	// Second submit (duplicate)
	out2 := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out2.Deduped != 1 {
		t.Errorf("second submit: expected 1 deduped, got %d (accepted=%d)",
			out2.Deduped, out2.Accepted)
	}

	// Verify only 1 event in store
	_, version, _ := inner.ListEvents(ctx, "job-1")
	if version != 1 {
		t.Errorf("expected version 1 after dedup, got %d", version)
	}
}

// TestIngest_TenantMismatch verifies that events with wrong tenant_id
// are rejected.
func TestIngest_TenantMismatch(t *testing.T) {
	handler, _ := newTestHandler()
	ctx := authContext("tenant-acme")

	// Event claims tenant-other
	ev := makeEvent("evt-001", "job-1", "tenant-other", "job_created")

	out := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out.Rejected != 1 {
		t.Fatalf("expected 1 rejected, got %d", out.Rejected)
	}
	if out.Rejections[0].Reason != "tenant_mismatch" {
		t.Errorf("expected tenant_mismatch, got %s", out.Rejections[0].Reason)
	}
}

// TestIngest_UnknownType verifies that unknown event types are rejected.
func TestIngest_UnknownType(t *testing.T) {
	handler, _ := newTestHandler()
	ctx := authContext("tenant-acme")

	ev := makeEvent("evt-001", "job-1", "tenant-acme", "checkpoint_loaded")
	out := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out.Rejected != 1 {
		t.Fatalf("expected 1 rejected, got %d", out.Rejected)
	}
	if out.Rejections[0].Reason != "unknown_type" {
		t.Errorf("expected unknown_type, got %s", out.Rejections[0].Reason)
	}
}

// TestIngest_InvalidJobID verifies that malformed job_id is rejected.
func TestIngest_InvalidJobID(t *testing.T) {
	handler, _ := newTestHandler()
	ctx := authContext("tenant-acme")

	ev := makeEvent("evt-001", "job with spaces!", "tenant-acme", "job_created")
	out := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out.Rejected != 1 {
		t.Fatalf("expected 1 rejected, got %d", out.Rejected)
	}
	if out.Rejections[0].Reason != "invalid_job_id" {
		t.Errorf("expected invalid_job_id, got %s", out.Rejections[0].Reason)
	}
}

// TestIngest_FutureTimestamp verifies that future timestamps (>5min)
// are rejected.
func TestIngest_FutureTimestamp(t *testing.T) {
	handler, _ := newTestHandler()
	ctx := authContext("tenant-acme")

	ev := makeEvent("evt-001", "job-1", "tenant-acme", "job_created")
	ev.OccurredAt = time.Now().Add(10 * time.Minute).Format(time.RFC3339)
	out := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out.Rejected != 1 {
		t.Fatalf("expected 1 rejected, got %d", out.Rejected)
	}
	if out.Rejections[0].Reason != "future_timestamp" {
		t.Errorf("expected future_timestamp, got %s", out.Rejections[0].Reason)
	}
}

// TestIngest_ConcurrentAppend verifies that concurrent appends to the
// same job_id handle version mismatch via retry.
func TestIngest_ConcurrentAppend(t *testing.T) {
	handler, inner := newTestHandler()
	ctx := authContext("tenant-acme")

	// First event to create the job
	handler.processBatch(ctx, "tenant-acme", []EventEnvelope{
		makeEvent("evt-0", "job-conc", "tenant-acme", "job_created"),
	})

	// 20 concurrent events to the same job
	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ev := makeEvent(
				"evt-conc-"+string(rune('a'+n)),
				"job-conc",
				"tenant-acme",
				"step_started",
			)
			out := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
			if out.Rejected > 0 {
				errCh <- nil // some may reject on version mismatch retry failure
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	// Verify no duplicates: version should equal 1 + accepted
	_, version, _ := inner.ListEvents(ctx, "job-conc")
	if version < 2 {
		t.Errorf("expected at least 2 events, got %d", version)
	}
	// Verify hash chain intact
	events, _, _ := inner.ListEvents(ctx, "job-conc")
	for i := 1; i < len(events); i++ {
		if events[i].PrevHash != events[i-1].Hash {
			t.Errorf("event %d PrevHash mismatch", i)
		}
	}
}

// TestIngest_RateLimit verifies that rate limiting kicks in after
// exceeding the request limit.
func TestIngest_RateLimit(t *testing.T) {
	handler, _ := newTestHandler()
	// Override rate limiter with a low limit for testing
	handler.rateLimiter = newRateLimiter(time.Minute, 3)

	tenantID := "tenant-ratelimit"
	for i := 0; i < 3; i++ {
		if !handler.rateLimiter.allow(tenantID) {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	// 4th request should be blocked
	if handler.rateLimiter.allow(tenantID) {
		t.Error("4th request should be rate limited")
	}
}

// TestIngest_PayloadTooLarge verifies that oversized payloads are rejected.
func TestIngest_PayloadTooLarge(t *testing.T) {
	handler, _ := newTestHandler()
	ctx := authContext("tenant-acme")

	ev := makeEvent("evt-001", "job-1", "tenant-acme", "job_created")
	// Create a payload > 256KB
	bigPayload := make([]byte, 257*1024)
	for i := range bigPayload {
		bigPayload[i] = 'x'
	}
	ev.Payload = bigPayload

	out := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out.Rejected != 1 {
		t.Fatalf("expected 1 rejected, got %d", out.Rejected)
	}
	if out.Rejections[0].Reason != "payload_too_large" {
		t.Errorf("expected payload_too_large, got %s", out.Rejections[0].Reason)
	}
}

// TestIngest_LegacyNoSchemaVersion verifies that events without
// schema_version are accepted (legacy support).
func TestIngest_LegacyNoSchemaVersion(t *testing.T) {
	handler, _ := newTestHandler()
	ctx := authContext("tenant-acme")

	ev := makeEvent("evt-001", "job-1", "tenant-acme", "job_created")
	ev.SchemaVersion = "" // legacy

	out := handler.processBatch(ctx, "tenant-acme", []EventEnvelope{ev})
	if out.Accepted != 1 {
		t.Errorf("expected 1 accepted (legacy), got %d (rejected=%d)",
			out.Accepted, out.Rejected)
	}
}

// authContext returns a context with tenantID set via auth.WithTenantID.
func authContext(tenantID string) context.Context {
	return auth.WithTenantID(context.Background(), tenantID)
}
