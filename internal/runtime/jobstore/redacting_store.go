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

package jobstore

import (
	"context"
	"fmt"

	"github.com/Colin4k1024/Aetheris/v2/pkg/redaction"
)

// RedactingStore wraps a JobStore, applying redaction to every event's
// Payload before it reaches the underlying store (Issue #5).
//
// Design constraint (#5 INV: "any path where events are written must
// go through the redaction pipeline"): the single chokepoint is
// JobStore.Append. By wrapping at this level, all write paths —
// direct Append callers (handler.go, worker, app.go, inbox.go,
// acp_handler.go, effect.go) and indirect sink callers (node_sink.go,
// plan_sink.go) — are covered.
//
// fail-closed semantics (#5): if the redaction engine errors or
// panics, Append returns an error and the event is NOT written.
// This is the opposite of #7's fail-open for the collection channel:
// collection can drop, plaintext must not land.
type RedactingStore struct {
	inner  JobStore
	engine *redaction.Engine
}

// NewRedactingStore wraps inner with a redaction layer. If engine is
// nil, returns inner unchanged (redaction disabled — for testing
// or when redaction is explicitly turned off).
func NewRedactingStore(inner JobStore, engine *redaction.Engine) JobStore {
	if engine == nil {
		return inner
	}
	return &RedactingStore{inner: inner, engine: engine}
}

// Append applies redaction to the event Payload, then delegates to
// the inner store. On redaction error, returns ErrRedactionFailed
// and does NOT write (fail-closed).
func (r *RedactingStore) Append(ctx context.Context, jobID string, expectedVersion int, event JobEvent) (int, error) {
	redacted, err := r.redactEvent(event)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrRedactionFailed, err)
	}
	return r.inner.Append(ctx, jobID, expectedVersion, redacted)
}

// redactEvent applies the redaction engine to the event payload.
// Returns a copy of the event with redacted payload. If the engine
// is nil or the payload is empty, returns the event unchanged.
// On engine error or panic, returns error (fail-closed).
func (r *RedactingStore) redactEvent(event JobEvent) (JobEvent, error) {
	if r.engine == nil || len(event.Payload) == 0 {
		return event, nil
	}

	redacted, err := r.engine.RedactData(string(event.Type), event.Payload)
	if err != nil {
		return JobEvent{}, fmt.Errorf("redaction failed for event type %s: %w", event.Type, err)
	}

	// Defensive copy: don't let the engine's internal buffer alias
	// the stored payload.
	cp := event
	cp.Payload = make([]byte, len(redacted))
	copy(cp.Payload, redacted)
	return cp, nil
}

// ErrRedactionFailed is returned when the redaction engine errors or
// panics. The event is NOT written to the store (fail-closed).
var ErrRedactionFailed = fmt.Errorf("jobstore: redaction failed, event not written (fail-closed)")

// --- Pass-through methods (no redaction needed) ---

func (r *RedactingStore) ListEvents(ctx context.Context, jobID string) ([]JobEvent, int, error) {
	return r.inner.ListEvents(ctx, jobID)
}

func (r *RedactingStore) Claim(ctx context.Context, workerID string) (string, int, string, error) {
	return r.inner.Claim(ctx, workerID)
}

func (r *RedactingStore) ClaimJob(ctx context.Context, workerID string, jobID string) (int, string, error) {
	return r.inner.ClaimJob(ctx, workerID, jobID)
}

func (r *RedactingStore) Heartbeat(ctx context.Context, workerID string, jobID string) error {
	return r.inner.Heartbeat(ctx, workerID, jobID)
}

func (r *RedactingStore) Watch(ctx context.Context, jobID string) (<-chan JobEvent, error) {
	return r.inner.Watch(ctx, jobID)
}

func (r *RedactingStore) ListJobIDsWithExpiredClaim(ctx context.Context) ([]string, error) {
	return r.inner.ListJobIDsWithExpiredClaim(ctx)
}

func (r *RedactingStore) GetCurrentAttemptID(ctx context.Context, jobID string) (string, error) {
	return r.inner.GetCurrentAttemptID(ctx, jobID)
}

func (r *RedactingStore) CreateSnapshot(ctx context.Context, jobID string, upToVersion int, snapshot []byte) error {
	return r.inner.CreateSnapshot(ctx, jobID, upToVersion, snapshot)
}

func (r *RedactingStore) GetLatestSnapshot(ctx context.Context, jobID string) (*JobSnapshot, error) {
	return r.inner.GetLatestSnapshot(ctx, jobID)
}

func (r *RedactingStore) DeleteSnapshotsBefore(ctx context.Context, jobID string, beforeVersion int) error {
	return r.inner.DeleteSnapshotsBefore(ctx, jobID, beforeVersion)
}
