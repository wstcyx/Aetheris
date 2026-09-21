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

package job

import (
	"context"
	"testing"
	"time"
)

// TestListByTenant_NoAgentFilter verifies that queries without
// agent_filter return all jobs for the tenant (#8: removes forced requirement).
func TestListByTenant_NoAgentFilter(t *testing.T) {
	store := NewJobStoreMem()
	store.Create(context.Background(), &Job{
		ID: "job-1", AgentID: "agent-1", TenantID: "t", Status: StatusRunning,
	})
	store.Create(context.Background(), &Job{
		ID: "job-2", AgentID: "agent-2", TenantID: "t", Status: StatusRunning,
	})

	jobs, err := store.ListByTenant(context.Background(), "t", nil, TimeRange{}, 100)
	if err != nil {
		t.Fatalf("ListByTenant failed: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs (no agent filter), got %d", len(jobs))
	}
}

// TestListByTenant_TenantIsolation verifies forged tenant_id
// doesn't leak other tenants' data (#8 INV).
func TestListByTenant_TenantIsolation(t *testing.T) {
	store := NewJobStoreMem()
	store.Create(context.Background(), &Job{
		ID: "job-A", AgentID: "a", TenantID: "tenantA", Status: StatusRunning,
	})
	store.Create(context.Background(), &Job{
		ID: "job-B", AgentID: "a", TenantID: "tenantB", Status: StatusRunning,
	})

	jobsA, _ := store.ListByTenant(context.Background(), "tenantA", nil, TimeRange{}, 100)
	if len(jobsA) != 1 || jobsA[0].ID != "job-A" {
		t.Errorf("tenantA should see only job-A, got %v", jobsA)
	}

	jobsB, _ := store.ListByTenant(context.Background(), "tenantB", nil, TimeRange{}, 100)
	if len(jobsB) != 1 || jobsB[0].ID != "job-B" {
		t.Errorf("tenantB should see only job-B, got %v", jobsB)
	}

	// Forged tenant sees nothing
	jobsFake, _ := store.ListByTenant(context.Background(), "fake", nil, TimeRange{}, 100)
	if len(jobsFake) != 0 {
		t.Errorf("forged tenant should see 0 jobs, got %d", len(jobsFake))
	}
}

// TestListByTenant_TimeRangePushdown verifies time filtering is
// pushed down to the store (#8: filter pushdown).
func TestListByTenant_TimeRangePushdown(t *testing.T) {
	store := NewJobStoreMem()
	now := time.Now()
	old := now.Add(-2 * time.Hour)

	// Create directly in the map to control CreatedAt (Create overrides it)
	store.mu.Lock()
	store.byID["job-old"] = &Job{ID: "job-old", AgentID: "a", TenantID: "t", Status: StatusRunning, CreatedAt: old}
	store.byID["job-new"] = &Job{ID: "job-new", AgentID: "a", TenantID: "t", Status: StatusRunning, CreatedAt: now}
	store.mu.Unlock()

	jobs, _ := store.ListByTenant(context.Background(), "t", nil,
		TimeRange{Start: now.Add(-1 * time.Hour)}, 100)
	if len(jobs) != 1 {
		t.Errorf("expected 1 job in last hour, got %d", len(jobs))
	}
	if len(jobs) > 0 && jobs[0].ID != "job-new" {
		t.Errorf("expected job-new, got %s", jobs[0].ID)
	}
}

// TestListByTenant_AgentFilterOptional verifies agent_filter is
// applied when present but not required.
func TestListByTenant_AgentFilterOptional(t *testing.T) {
	store := NewJobStoreMem()
	store.Create(context.Background(), &Job{ID: "j1", AgentID: "a1", TenantID: "t", Status: StatusRunning})
	store.Create(context.Background(), &Job{ID: "j2", AgentID: "a2", TenantID: "t", Status: StatusRunning})

	// With agent filter
	jobs, _ := store.ListByTenant(context.Background(), "t", []string{"a1"}, TimeRange{}, 100)
	if len(jobs) != 1 || jobs[0].AgentID != "a1" {
		t.Errorf("expected 1 job for a1, got %v", jobs)
	}

	// Without agent filter
	jobs, _ = store.ListByTenant(context.Background(), "t", nil, TimeRange{}, 100)
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs without filter, got %d", len(jobs))
	}
}

// TestListByTenant_LimitEnforced verifies limit is respected.
func TestListByTenant_LimitEnforced(t *testing.T) {
	store := NewJobStoreMem()
	for i := 0; i < 50; i++ {
		store.Create(context.Background(), &Job{
			ID: "job-" + string(rune('a'+i)),
			AgentID: "a", TenantID: "t", Status: StatusRunning,
		})
	}

	jobs, _ := store.ListByTenant(context.Background(), "t", nil, TimeRange{}, 5)
	if len(jobs) != 5 {
		t.Errorf("expected 5 jobs with limit, got %d", len(jobs))
	}
}

// TestListByTenant_CursorPagination verifies limit+1 pattern for
// hasMore detection.
func TestListByTenant_CursorPagination(t *testing.T) {
	store := NewJobStoreMem()
	for i := 0; i < 25; i++ {
		store.Create(context.Background(), &Job{
			ID: "job-" + string(rune('a'+i)),
			AgentID: "a", TenantID: "t", Status: StatusRunning,
			CreatedAt: time.Now().Add(time.Duration(i) * time.Second),
		})
	}

	// Request limit+1 to detect hasMore
	jobs, _ := store.ListByTenant(context.Background(), "t", nil, TimeRange{}, 11)
	hasMore := len(jobs) > 10
	if !hasMore {
		t.Error("expected hasMore=true for 25 jobs with limit 10")
	}
	if hasMore {
		jobs = jobs[:10]
	}
	if len(jobs) != 10 {
		t.Errorf("page 1 should have 10 jobs, got %d", len(jobs))
	}
}

// TestListByTenant_EmptyTenantReturnsAll verifies behavior when
// tenantID is empty (handler should fill from auth context).
func TestListByTenant_EmptyTenantReturnsAll(t *testing.T) {
	store := NewJobStoreMem()
	store.Create(context.Background(), &Job{ID: "j1", AgentID: "a", TenantID: "t1", Status: StatusRunning})
	store.Create(context.Background(), &Job{ID: "j2", AgentID: "a", TenantID: "t2", Status: StatusRunning})

	// Empty tenant = no filter (returns all)
	jobs, _ := store.ListByTenant(context.Background(), "", nil, TimeRange{}, 100)
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs with empty tenant, got %d", len(jobs))
	}
}
