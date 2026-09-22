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
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Colin4k1024/Aetheris/v2/pkg/redaction"
)

// TestRedactingStore_RedactsPII verifies that PII in step_started Input
// and step_finished Output does not appear in the underlying store.
// #5 acceptance criterion 1.
func TestRedactingStore_RedactsPII(t *testing.T) {
	inner := NewMemoryStore()
	policy := DefaultRedactionPolicy()
	engine := redaction.NewEngine(policy, nil)
	store := NewRedactingStore(inner, engine)

	ctx := context.Background()
	jobID := "job-pii-test"

	// Create job with initial event
	_, err := store.Append(ctx, jobID, 0, JobEvent{
		JobID:     jobID,
		Type:      JobCreated,
		Payload:   []byte(`{}`),
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("initial Append failed: %v", err)
	}

	// Write step_started with PII in Input
	email := "user@example.com"
	phone := "13800138000"
	startPayload, _ := json.Marshal(map[string]string{
		"step_id": "step-1",
		"input":   "contact: " + email + " phone: " + phone,
	})
	_, err = store.Append(ctx, jobID, 1, JobEvent{
		JobID:     jobID,
		Type:      StepStarted,
		Payload:   startPayload,
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("step_started Append failed: %v", err)
	}

	// Write step_finished with PII in Output
	finishPayload, _ := json.Marshal(map[string]string{
		"step_id": "step-1",
		"output":  "result: user@example.com sent to 13800138000",
	})
	_, err = store.Append(ctx, jobID, 2, JobEvent{
		JobID:     jobID,
		Type:      StepFinished,
		Payload:   finishPayload,
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("step_finished Append failed: %v", err)
	}

	// Read back from the INNER store (bypassing redaction on read)
	events, _, err := inner.ListEvents(ctx, jobID)
	if err != nil {
		t.Fatalf("ListEvents failed: %v", err)
	}

	// Verify PII does not appear in any stored event
	for _, ev := range events {
		payloadStr := string(ev.Payload)
		if strings.Contains(payloadStr, email) {
			t.Errorf("PII email found in stored %s event: %s", ev.Type, payloadStr)
		}
		if strings.Contains(payloadStr, phone) {
			t.Errorf("PII phone found in stored %s event: %s", ev.Type, payloadStr)
		}
	}

	// Verify the redacted value IS present (proves redaction ran, not deletion)
	stepStartedFound := false
	stepFinishedFound := false
	for _, ev := range events {
		if ev.Type == StepStarted {
			stepStartedFound = true
			var p map[string]string
			_ = json.Unmarshal(ev.Payload, &p)
			if p["input"] != "***REDACTED***" {
				t.Errorf("expected input redacted, got %q", p["input"])
			}
		}
		if ev.Type == StepFinished {
			stepFinishedFound = true
			var p map[string]string
			_ = json.Unmarshal(ev.Payload, &p)
			if p["output"] != "***REDACTED***" {
				t.Errorf("expected output redacted, got %q", p["output"])
			}
		}
	}
	if !stepStartedFound {
		t.Error("step_started event not found in store")
	}
	if !stepFinishedFound {
		t.Error("step_finished event not found in store")
	}
}

// TestRedactingStore_FailClosed verifies that when the redaction engine
// errors (malformed JSON payload), the event is NOT written (fail-closed).
// #5 core acceptance logic: "脱敏引擎 panic/正则超时时行为必须是 fail-closed"
func TestRedactingStore_FailClosed(t *testing.T) {
	inner := NewMemoryStore()
	policy := DefaultRedactionPolicy()
	engine := redaction.NewEngine(policy, nil)
	store := NewRedactingStore(inner, engine)

	ctx := context.Background()
	jobID := "job-fail-closed"

	// First event succeeds
	_, err := store.Append(ctx, jobID, 0, JobEvent{
		JobID:     jobID,
		Type:      JobCreated,
		Payload:   []byte(`{}`),
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("initial Append failed: %v", err)
	}

	// Malformed JSON payload — RedactData will return error from json.Unmarshal
	malformedPayload := []byte(`{"input": not valid json`)
	_, err = store.Append(ctx, jobID, 1, JobEvent{
		JobID:     jobID,
		Type:      StepStarted,
		Payload:   malformedPayload,
		CreatedAt: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error on malformed payload, got nil")
	}
	if !strings.Contains(err.Error(), "redaction failed") {
		t.Errorf("expected redaction error, got: %v", err)
	}

	// Verify the event was NOT written to the inner store
	events, version, err := inner.ListEvents(ctx, jobID)
	if err != nil {
		t.Fatalf("ListEvents failed: %v", err)
	}
	if version != 1 {
		t.Errorf("expected version 1 (only initial event), got %d", version)
	}
	for _, ev := range events {
		if ev.Type == StepStarted {
			t.Error("step_started event should NOT have been written (fail-closed)")
		}
	}
}

// TestRedactingStore_NilEngineNoOp verifies that a nil engine returns
// the inner store unchanged (redaction disabled, for testing).
func TestRedactingStore_NilEngineNoOp(t *testing.T) {
	inner := NewMemoryStore()
	store := NewRedactingStore(inner, nil)

	// Should be the same inner store
	if store != inner {
		t.Error("expected inner store when engine is nil")
	}
}

// TestRedactingStore_EmptyPayloadNoOp verifies that events with empty
// payload pass through without redaction (no-op).
func TestRedactingStore_EmptyPayloadNoOp(t *testing.T) {
	inner := NewMemoryStore()
	policy := DefaultRedactionPolicy()
	engine := redaction.NewEngine(policy, nil)
	store := NewRedactingStore(inner, engine)

	ctx := context.Background()
	jobID := "job-empty-payload"

	_, err := store.Append(ctx, jobID, 0, JobEvent{
		JobID:     jobID,
		Type:      JobCreated,
		Payload:   nil, // empty payload
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("Append with empty payload failed: %v", err)
	}

	events, _, _ := inner.ListEvents(ctx, jobID)
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

// TestRedactingStore_HashChainIntact verifies that after redaction,
// the proof chain (PrevHash/Hash) still works — events are written
// through Append which computes the hash. #5 acceptance criterion 2.
func TestRedactingStore_HashChainIntact(t *testing.T) {
	inner := NewMemoryStore()
	policy := DefaultRedactionPolicy()
	engine := redaction.NewEngine(policy, nil)
	store := NewRedactingStore(inner, engine)

	ctx := context.Background()
	jobID := "job-hash-chain"

	// Write a sequence of events
	for i, evType := range []EventType{JobCreated, StepStarted, StepFinished, JobCompleted} {
		payload := []byte(`{}`)
		if evType == StepStarted {
			payload = []byte(`{"step_id":"s1","input":"secret-data@example.com"}`)
		}
		if evType == StepFinished {
			payload = []byte(`{"step_id":"s1","output":"result@example.com"}`)
		}
		_, err := store.Append(ctx, jobID, i, JobEvent{
			JobID:     jobID,
			Type:      evType,
			Payload:   payload,
			CreatedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("Append event %d failed: %v", i, err)
		}
	}

	events, _, _ := inner.ListEvents(ctx, jobID)
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}

	// Each event should have a non-empty Hash
	for i, ev := range events {
		if ev.Hash == "" {
			t.Errorf("event %d has empty Hash", i)
		}
		if i > 0 && ev.PrevHash != events[i-1].Hash {
			t.Errorf("event %d PrevHash mismatch: got %s, want %s",
				i, ev.PrevHash, events[i-1].Hash)
		}
	}
}

// TestRedactingStore_DecisionSnapshotRedacted verifies that
// decision_snapshot events have goal redacted and reasoning hashed.
func TestRedactingStore_DecisionSnapshotRedacted(t *testing.T) {
	inner := NewMemoryStore()
	policy := DefaultRedactionPolicy()
	engine := redaction.NewEngine(policy, nil)
	store := NewRedactingStore(inner, engine)

	ctx := context.Background()
	jobID := "job-decision"

	_, err := store.Append(ctx, jobID, 0, JobEvent{
		JobID: jobID,
		Type:  JobCreated,
		Payload: []byte(`{}`),
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("initial Append failed: %v", err)
	}

	goal := "process user@example.com payment"
	reasoning := "user asked to pay"
	payload, _ := json.Marshal(map[string]string{
		"goal":      goal,
		"reasoning": reasoning,
	})
	_, err = store.Append(ctx, jobID, 1, JobEvent{
		JobID:     jobID,
		Type:      DecisionSnapshot,
		Payload:   payload,
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("decision_snapshot Append failed: %v", err)
	}

	events, _, _ := inner.ListEvents(ctx, jobID)
	for _, ev := range events {
		if ev.Type == DecisionSnapshot {
			var p map[string]string
			_ = json.Unmarshal(ev.Payload, &p)
			if p["goal"] == goal {
				t.Errorf("goal not redacted: %s", p["goal"])
			}
			if p["goal"] != "***REDACTED***" {
				t.Errorf("expected goal redacted, got %q", p["goal"])
			}
			if p["reasoning"] == reasoning {
				t.Errorf("reasoning not hashed: %s", p["reasoning"])
			}
			if !strings.HasPrefix(p["reasoning"], "hash:") {
				t.Errorf("expected reasoning hashed, got %q", p["reasoning"])
			}
		}
	}
}

// TestRedactingStore_Concurrent50 verifies 50 concurrent Append
// operations are race-free (#5: -race 下 50 并发无竞态).
func TestRedactingStore_Concurrent50(t *testing.T) {
	inner := NewMemoryStore()
	policy := DefaultRedactionPolicy()
	engine := redaction.NewEngine(policy, nil)
	store := NewRedactingStore(inner, engine)

	ctx := context.Background()

	// Create initial jobs
	for i := 0; i < 50; i++ {
		jobID := "job-conc-" + string(rune('a'+i))
		_, _ = store.Append(ctx, jobID, 0, JobEvent{
			JobID: jobID, Type: JobCreated, Payload: []byte(`{}`), CreatedAt: time.Now(),
		})
	}

	// 50 goroutines each appending to their own job
	var wg sync.WaitGroup
	errCh := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			jobID := "job-conc-" + string(rune('a'+n))
			payload, _ := json.Marshal(map[string]string{
				"input":  "user@example.com",
				"output": "result-" + string(rune('a'+n)),
			})
			_, err := store.Append(ctx, jobID, 1, JobEvent{
				JobID: jobID, Type: StepStarted, Payload: payload, CreatedAt: time.Now(),
			})
			if err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Errorf("concurrent Append failed: %v", err)
		}
	}

	// Verify PII redacted in all jobs
	for i := 0; i < 50; i++ {
		jobID := "job-conc-" + string(rune('a'+i))
		events, _, _ := inner.ListEvents(ctx, jobID)
		for _, ev := range events {
			if strings.Contains(string(ev.Payload), "user@example.com") {
				t.Errorf("PII leaked in job %s event %s", jobID, ev.Type)
			}
		}
	}
}
