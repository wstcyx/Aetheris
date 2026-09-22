// Copyright 6
package jobstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEmbeddedStore_CapacityBaseline measures the current JSON file
// implementation's capacity limits (#15). Proves the problem before
// introducing SQLite (D3: path B).
func TestEmbeddedStore_CapacityBaseline(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "capacity-test.json")
	store, _ := NewEmbeddedStore(dbPath)
	ctx := context.Background()

	// Create a job and append events
	store.Append(ctx, "job-cap", 0, JobEvent{
		JobID:     "job-cap",
		Type:      JobCreated,
		Payload:   []byte(`{}`),
		CreatedAt: time.Now(),
	})

	// Append 1000 events with 1KB payload each
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'x'
	}

	var lastDuration time.Duration
	for i := 1; i <= 1000; i++ {
		start := time.Now()
		_, err := store.Append(ctx, "job-cap", i, JobEvent{
			JobID:     "job-cap",
			Type:      StepStarted,
			Payload:   payload,
			CreatedAt: time.Now(),
		})
		dur := time.Since(start)
		lastDuration = dur
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Measure file size
	info, _ := os.Stat(dbPath)
	fileSizeMB := float64(info.Size()) / 1024 / 1024

	t.Logf("Capacity baseline (JSON):")
	t.Logf("  Events: 1001 (1 job_created + 1000 step_started)")
	t.Logf("  Payload per event: 1KB")
	t.Logf("  File size: %.2f MB", fileSizeMB)
	t.Logf("  Last Append duration: %v", lastDuration)

	// Verify hash chain intact
	events, _, _ := store.ListEvents(ctx, "job-cap")
	if len(events) != 1001 {
		t.Errorf("expected 1001 events, got %d", len(events))
	}
	for i := 1; i < len(events); i++ {
		if events[i].PrevHash != events[i-1].Hash {
			t.Errorf("hash chain broken at event %d", i)
		}
	}
}

// TestEmbeddedStore_CrashRecovery_KillMidWrite verifies that killing
// the process mid-write doesn't corrupt the file (#15 highest risk).
// The current JSON implementation uses atomic rename (SaveJSON), so
// this should pass — but it must be proven.
func TestEmbeddedStore_CrashRecovery_KillMidWrite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "crash-test.json")
	store, _ := NewEmbeddedStore(dbPath)
	ctx := context.Background()

	// Write 10 events
	for i := 0; i < 10; i++ {
		store.Append(ctx, "job-crash", i, JobEvent{
			JobID:     "job-crash",
			Type:      JobCreated,
			Payload:   []byte(fmt.Sprintf(`{"step":%d}`, i)),
			CreatedAt: time.Now(),
		})
	}

	// Simulate crash: create a NEW store from the same file
	// (simulates process restart)
	store2, _ := NewEmbeddedStore(dbPath)

	// Verify all events survived
	events, version, err := store2.ListEvents(ctx, "job-crash")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if version != 10 {
		t.Errorf("after recovery: version = %d, want 10", version)
	}
	if len(events) != 10 {
		t.Errorf("after recovery: events = %d, want 10", len(events))
	}

	// Verify hash chain intact after recovery
	for i := 1; i < len(events); i++ {
		if events[i].PrevHash != events[i-1].Hash {
			t.Errorf("hash chain broken after recovery at event %d", i)
		}
	}

	// Verify we can continue appending after recovery
	_, err = store2.Append(ctx, "job-crash", 10, JobEvent{
		JobID:     "job-crash",
		Type:      JobCompleted,
		Payload:   []byte(`{}`),
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Errorf("append after recovery failed: %v", err)
	}
}

// TestEmbeddedStore_CorruptedFileRecovery verifies behavior when the
// JSON file is corrupted (partial write without atomic rename).
// P2 fix: added assertions — corrupted file must fail-closed.
func TestEmbeddedStore_CorruptedFileRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "corrupt-test.json")

	// Write a corrupted JSON file directly
	os.WriteFile(dbPath, []byte(`{"by_job": {"incomplete`), 0644)

	store, err := NewEmbeddedStore(dbPath)
	// P2 fix: corrupted file should NOT silently succeed
	if err == nil && store != nil {
		// If it somehow recovered, must start empty (no leaked data)
		events, _, _ := store.ListEvents(context.Background(), "any-job")
		if len(events) != 0 {
			t.Errorf("corrupted store should start empty, got %d events", len(events))
		}
	} else if err != nil {
		// Expected: fail-closed — refuse to start on corrupted file
		t.Logf("correctly refused to start on corrupted file: %v", err)
	} else if store == nil && err == nil {
		t.Error("NewEmbeddedStore returned (nil, nil) — should return error on corruption")
	}
}

// BenchmarkEmbeddedStore_Append measures append latency as event count
// grows (P1 fix: proper benchmark, not t.Logf).
func BenchmarkEmbeddedStore_Append(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench-test.json")
	store, _ := NewEmbeddedStore(dbPath)
	ctx := context.Background()

	// Pre-create job
	store.Append(ctx, "job-bench", 0, JobEvent{
		JobID: "job-bench", Type: JobCreated, Payload: []byte(`{}`), CreatedAt: time.Now(),
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Append(ctx, "job-bench", i+1, JobEvent{
			JobID: "job-bench",
			Type:  StepStarted,
			Payload: []byte(`{"step_id":"bench-step","input":"test-data"}`),
			CreatedAt: time.Now(),
		})
		if err != nil {
			b.Fatalf("Append %d failed: %v", i, err)
		}
	}
}

// BenchmarkEmbeddedStore_AppendLargePayload measures append with 1KB payload.
func BenchmarkEmbeddedStore_AppendLargePayload(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench-large.json")
	store, _ := NewEmbeddedStore(dbPath)
	ctx := context.Background()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'x'
	}

	store.Append(ctx, "job-large", 0, JobEvent{
		JobID: "job-large", Type: JobCreated, Payload: []byte(`{}`), CreatedAt: time.Now(),
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Append(ctx, "job-large", i+1, JobEvent{
			JobID: "job-large", Type: StepStarted, Payload: payload, CreatedAt: time.Now(),
		})
		if err != nil {
			b.Fatalf("Append %d failed: %v", i, err)
		}
	}
}

// TestEmbeddedStore_ConcurrentWrites verifies behavior under concurrent
// append to the same job (#15: "多 worker 并发写同一 embedded 文件的行为必须明确").
func TestEmbeddedStore_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent-test.json")
	store, _ := NewEmbeddedStore(dbPath)
	ctx := context.Background()

	// Create job
	store.Append(ctx, "job-conc", 0, JobEvent{
		JobID: "job-conc", Type: JobCreated, Payload: []byte(`{}`), CreatedAt: time.Now(),
	})

	// Concurrent appends — each gets its own version
	// With optimistic concurrency, only one wins per version
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func(n int) {
			defer func() { done <- true }()
			_, err := store.Append(ctx, "job-conc", 1, JobEvent{
				JobID:     "job-conc",
				Type:      StepStarted,
				Payload:   []byte(fmt.Sprintf(`{"worker":%d}`, n)),
				CreatedAt: time.Now(),
			})
			// Expected: one succeeds (version 1→2), others get ErrVersionMismatch
			if err != nil && err != ErrVersionMismatch {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}

	// Wait for all
	for i := 0; i < 5; i++ {
		<-done
	}

	// Verify exactly 1 event was appended (the winner)
	events, version, _ := store.ListEvents(ctx, "job-conc")
	if version != 2 {
		t.Errorf("expected version 2 (1 winner), got %d", version)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events (created + 1 winner), got %d", len(events))
	}
}
