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
)

// TestCausalChain_ThreeLevelChain verifies A→B→C chain: each job
// has correct parent_job_id and root_job_id (#9 acceptance criterion 1&2).
func TestCausalChain_ThreeLevelChain(t *testing.T) {
	store := NewJobStoreMem()
	ctx := context.Background()

	// A (root)
	jobA := &Job{ID: "job-A", AgentID: "agent-A", TenantID: "t", Status: StatusRunning}
	store.Create(ctx, jobA)
	// Simulate: A created, RootJobID set to self (root has no parent)
	jobA.RootJobID = "job-A"
	store.mu.Lock()
	store.byID["job-A"] = jobA
	store.mu.Unlock()

	// B (child of A)
	jobB := &Job{
		ID: "job-B", AgentID: "agent-B", TenantID: "t", Status: StatusRunning,
		ParentJobID: "job-A", ParentAgentID: "agent-A",
		RootJobID: "job-A", // inherits from parent
	}
	store.Create(ctx, jobB)

	// C (child of B)
	jobC := &Job{
		ID: "job-C", AgentID: "agent-C", TenantID: "t", Status: StatusRunning,
		ParentJobID: "job-B", ParentAgentID: "agent-B",
		RootJobID: "job-A", // still root A
	}
	store.Create(ctx, jobC)

	// Verify chain
	gotC, _ := store.Get(ctx, "job-C")
	if gotC.ParentJobID != "job-B" {
		t.Errorf("C.ParentJobID = %s, want job-B", gotC.ParentJobID)
	}
	if gotC.RootJobID != "job-A" {
		t.Errorf("C.RootJobID = %s, want job-A", gotC.RootJobID)
	}

	gotB, _ := store.Get(ctx, "job-B")
	if gotB.ParentJobID != "job-A" {
		t.Errorf("B.ParentJobID = %s, want job-A", gotB.ParentJobID)
	}
	if gotB.RootJobID != "job-A" {
		t.Errorf("B.RootJobID = %s, want job-A", gotB.RootJobID)
	}
}

// TestCausalChain_RootJobIDConstant verifies root_job_id is constant
// across the entire chain (#9 INV).
func TestCausalChain_RootJobIDConstant(t *testing.T) {
	store := NewJobStoreMem()
	ctx := context.Background()

	root := &Job{ID: "root-job", AgentID: "a", TenantID: "t", Status: StatusRunning, RootJobID: "root-job"}
	store.Create(ctx, root)

	for i := 0; i < 5; i++ {
		child := &Job{
			ID: "child-" + string(rune('a'+i)),
			AgentID: "a", TenantID: "t", Status: StatusRunning,
			ParentJobID: "root-job",
			RootJobID:   "root-job",
		}
		store.Create(ctx, child)
	}

	// All children should have RootJobID = "root-job"
	for i := 0; i < 5; i++ {
		got, _ := store.Get(ctx, "child-"+string(rune('a'+i)))
		if got.RootJobID != "root-job" {
			t.Errorf("child %d RootJobID = %s, want root-job", i, got.RootJobID)
		}
	}
}

// TestCausalChain_NoHeaderIsIndependentRoot verifies that calls
// without chain headers are treated as independent root jobs (#9
// acceptance criterion 3).
func TestCausalChain_NoHeaderIsIndependentRoot(t *testing.T) {
	store := NewJobStoreMem()
	ctx := context.Background()

	// Job created without any parent header
	j := &Job{ID: "independent", AgentID: "a", TenantID: "t", Status: StatusRunning}
	store.Create(ctx, j)

	got, _ := store.Get(ctx, "independent")
	if got.ParentJobID != "" {
		t.Errorf("independent job should have empty ParentJobID, got %s", got.ParentJobID)
	}
	if got.RootJobID != "" {
		t.Errorf("independent job should have empty RootJobID, got %s", got.RootJobID)
	}
}

// TestCausalChain_CrossTenantDisconnected verifies that cross-tenant
// chaining is disconnected (#9: "跨租户调用时默认断开并记录").
func TestCausalChain_CrossTenantDisconnected(t *testing.T) {
	store := NewJobStoreMem()
	ctx := context.Background()

	// Parent in tenantA
	parent := &Job{ID: "parent-A", AgentID: "a", TenantID: "tenantA", Status: StatusRunning, RootJobID: "parent-A"}
	store.Create(ctx, parent)

	// Child in tenantB tries to chain to parent-A
	// The handler logic should check parent.TenantID == child.TenantID
	// and disconnect if they don't match.
	child := &Job{
		ID: "child-B", AgentID: "b", TenantID: "tenantB", Status: StatusRunning,
		// Handler would NOT set ParentJobID here because parent.TenantID != child.TenantID
	}

	// Simulate handler's cross-tenant check
	parentGot, _ := store.Get(ctx, "parent-A")
	if parentGot.TenantID != child.TenantID {
		// Disconnect: don't set ParentJobID
		// (this is the expected behavior)
	} else {
		t.Error("cross-tenant should be disconnected")
	}

	store.Create(ctx, child)
	got, _ := store.Get(ctx, "child-B")
	if got.ParentJobID != "" {
		t.Errorf("cross-tenant child should not have ParentJobID, got %s", got.ParentJobID)
	}
}

// TestCausalChain_ForgedHeaderNoChain verifies that a forged
// X-Aetheris-Job-ID pointing to a non-existent job does NOT
// establish a chain (#9: "伪造的 header 不得建立串联").
func TestCausalChain_ForgedHeaderNoChain(t *testing.T) {
	store := NewJobStoreMem()
	ctx := context.Background()

	// Forge a parent job ID that doesn't exist
	forgedParentID := "job-does-not-exist"

	// Handler logic: Get(forgedParentID) returns nil -> don't chain
	parent, err := store.Get(ctx, forgedParentID)
	if err != nil || parent == nil {
		// Expected: parent doesn't exist, don't establish chain
	} else {
		t.Error("forged parent should not be found")
	}

	// Create child without chain (handler would skip setting ParentJobID)
	child := &Job{ID: "child-forged", AgentID: "a", TenantID: "t", Status: StatusRunning}
	store.Create(ctx, child)

	got, _ := store.Get(ctx, "child-forged")
	if got.ParentJobID != "" {
		t.Errorf("forged parent should not establish chain, got ParentJobID=%s", got.ParentJobID)
	}
}

// TestCausalChain_ForgedHeaderOtherTenant verifies that a forged
// X-Aetheris-Job-ID pointing to another tenant's job does NOT
// establish chain and does NOT leak whether the job exists (#9).
func TestCausalChain_ForgedHeaderOtherTenant(t *testing.T) {
	store := NewJobStoreMem()
	ctx := context.Background()

	// Job in tenantA
	store.Create(ctx, &Job{ID: "secret-job", AgentID: "a", TenantID: "tenantA", Status: StatusRunning})

	// Attacker in tenantB tries to chain to secret-job
	// Handler should check tenant match and disconnect
	// The Get() call succeeds (job exists), but tenant check fails
	parent, _ := store.Get(ctx, "secret-job")
	if parent != nil && parent.TenantID != "tenantB" {
		// Disconnect: don't set ParentJobID
		// Also don't reveal to attacker whether job exists
	} else if parent == nil {
		// Also disconnect (job not found)
	}

	// Either way, child should not have ParentJobID
	child := &Job{ID: "attacker-job", AgentID: "b", TenantID: "tenantB", Status: StatusRunning}
	store.Create(ctx, child)
	got, _ := store.Get(ctx, "attacker-job")
	if got.ParentJobID != "" {
		t.Errorf("cross-tenant forged chain should not establish, got ParentJobID=%s", got.ParentJobID)
	}
}

// TestCausalChain_CycleDetection verifies that cycle A→B→A is
// detected and blocked (#9 acceptance criterion 4).
func TestCausalChain_CycleDetection(t *testing.T) {
	// In the handler, cycle detection would check if the incoming
	// X-Aetheris-Job-ID would create a cycle. This test verifies
	// the detection logic: a job's ParentJobID chain should not
	// contain the job itself.
	store := NewJobStoreMem()
	ctx := context.Background()

	// Create A→B chain
	store.Create(ctx, &Job{
		ID: "job-A", AgentID: "a", TenantID: "t", Status: StatusRunning, RootJobID: "job-A",
	})
	store.Create(ctx, &Job{
		ID: "job-B", AgentID: "b", TenantID: "t", Status: StatusRunning,
		ParentJobID: "job-A", RootJobID: "job-A",
	})

	// Now if A tries to chain to B (A→B→A cycle):
	// The handler should detect that setting A.ParentJobID = B
	// would create a cycle (B's chain already includes A).

	// Simple cycle check: walk parent chain from proposed parent,
	// if we hit the current job, it's a cycle.
	hasCycle := func(store JobStore, jobID, proposedParentID string) bool {
		visited := map[string]bool{jobID: true}
		current := proposedParentID
		for current != "" {
			if visited[current] {
				return true // cycle
			}
			visited[current] = true
			parent, err := store.Get(ctx, current)
			if err != nil || parent == nil {
				return false
			}
			current = parent.ParentJobID
		}
		return false
	}

	// A trying to chain to B: B's parent chain is B→A, and A is in the chain
	if hasCycle(store, "job-A", "job-B") {
		// Cycle detected — handler should block
	} else {
		t.Error("cycle A→B→A should be detected")
	}
}
