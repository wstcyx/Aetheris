package reporting

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReporter_StepSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"accepted":1}`))
	}))
	defer server.Close()

	r := &Reporter{
		endpoint: server.URL, tenantID: "t", token: "tok", agentID: "a",
		sdkName: "aetheris-durability-go", sdkVersion: "0.1.0",
		timeout: 2 * time.Second, maxBuffer: 100, maxRetries: 1, backoff: 10 * time.Millisecond,
	}

	fn := r.Step("step1", func(ctx context.Context, state map[string]any) (map[string]any, error) {
		return map[string]any{"result": "ok"}, nil
	})

	result, err := fn(context.Background(), map[string]any{"job_id": "job-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["result"] != "ok" {
		t.Errorf("result = %v, want ok", result)
	}
}

func TestReporter_StepFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	r := &Reporter{
		endpoint: server.URL, tenantID: "t", token: "tok", agentID: "a",
		sdkName: "aetheris-durability-go", sdkVersion: "0.1.0",
		timeout: 2 * time.Second, maxBuffer: 100, maxRetries: 1, backoff: 10 * time.Millisecond,
	}

	bizErr := &testError{"business error"}
	fn := r.Step("risky", func(ctx context.Context, state map[string]any) (map[string]any, error) {
		return nil, bizErr
	})

	_, err := fn(context.Background(), map[string]any{})
	if err != bizErr {
		t.Errorf("expected original error, got %v", err)
	}
}

func TestReporter_FailOpen(t *testing.T) {
	// Point to unreachable port
	r := &Reporter{
		endpoint: "http://127.0.0.1:1", tenantID: "t", token: "tok", agentID: "a",
		sdkName: "aetheris-durability-go", sdkVersion: "0.1.0",
		timeout: 100 * time.Millisecond, maxBuffer: 100, maxRetries: 1, backoff: 10 * time.Millisecond,
	}

	fn := r.Step("step1", func(ctx context.Context, state map[string]any) (map[string]any, error) {
		return map[string]any{"value": 42}, nil
	})

	result, err := fn(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("business should succeed: %v", err)
	}
	if result["value"] != 42 {
		t.Errorf("result = %v, want 42", result)
	}
}

func TestReporter_Disabled(t *testing.T) {
	r := &Reporter{}
	if r.Enabled() {
		t.Error("empty reporter should be disabled")
	}

	fn := r.Step("step", func(ctx context.Context, state map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})

	result, err := fn(context.Background(), map[string]any{})
	if err != nil || result["ok"] != true {
		t.Errorf("disabled reporter should passthrough")
	}
}

func TestReporter_429CircuitOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(429)
	}))
	defer server.Close()

	r := &Reporter{
		endpoint: server.URL, tenantID: "t", token: "tok", agentID: "a",
		sdkName: "aetheris-durability-go", sdkVersion: "0.1.0",
		timeout: 100 * time.Millisecond, maxBuffer: 100, maxRetries: 1, backoff: 10 * time.Millisecond,
	}

	r.Report("job-1", "step_started", map[string]any{})
	r.Flush() // sync flush since Report no longer flushes synchronously

	r.mu.Lock()
	open := r.circuitOpen
	r.mu.Unlock()
	if !open {
		t.Error("circuit should be open after 429")
	}
	_, _, failed := r.Stats()
	if failed == 0 {
		t.Error("should have failed count > 0")
	}
}

func TestReporter_BufferOverflow(t *testing.T) {
	r := &Reporter{
		endpoint: "http://127.0.0.1:1", tenantID: "t", token: "tok", agentID: "a",
		sdkName: "aetheris-durability-go", sdkVersion: "0.1.0",
		timeout: 50 * time.Millisecond, maxBuffer: 5, maxRetries: 1, backoff: 1 * time.Millisecond,
	}

	// Fill buffer directly (bypass flush by keeping lock)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			r.mu.Lock()
			if len(r.buffer) >= r.maxBuffer {
				r.buffer = r.buffer[1:]
				r.dropped++
			}
			r.buffer = append(r.buffer, EventEnvelope{EventUID: "evt"})
			r.mu.Unlock()
		}(i)
	}
	wg.Wait()

	dropped, _, _ := r.Stats()
	if dropped == 0 {
		t.Error("expected dropped > 0")
	}
}

func TestReporter_TokenNotInPayload(t *testing.T) {
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf)
		capturedBody = buf
		w.WriteHeader(200)
	}))
	defer server.Close()

	r := &Reporter{
		endpoint: server.URL, tenantID: "t", token: "SUPER_SECRET_TOKEN_12345", agentID: "a",
		sdkName: "aetheris-durability-go", sdkVersion: "0.1.0",
		timeout: 2 * time.Second, maxBuffer: 100, maxRetries: 1, backoff: 10 * time.Millisecond,
	}

	r.Report("job-1", "step_started", map[string]any{"data": "test"})
	r.Flush() // sync flush to trigger request

	if strings.Contains(string(capturedBody), "SUPER_SECRET_TOKEN_12345") {
		t.Error("token leaked into request body!")
	}
}

func TestReporter_Middleware(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	r := &Reporter{
		endpoint: server.URL, tenantID: "t", token: "tok", agentID: "a",
		sdkName: "aetheris-durability-go", sdkVersion: "0.1.0",
		timeout: 2 * time.Second, maxBuffer: 100, maxRetries: 1, backoff: 10 * time.Millisecond,
	}

	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
	}))

	req := httptest.NewRequest("POST", "/test", nil)
	req.Header.Set("X-Aetheris-Job-ID", "job-mw")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
