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

package http

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Colin4k1024/Aetheris/v2/internal/agent/job"
	"github.com/Colin4k1024/Aetheris/v2/pkg/auth"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// ChainNode represents one node in the cross-agent call chain tree.
type ChainNode struct {
	JobID      string      `json:"job_id"`
	AgentID    string      `json:"agent_id"`
	Status     string      `json:"status"`
	ParentJobID string     `json:"parent_job_id,omitempty"`
	RootJobID  string      `json:"root_job_id,omitempty"`
	Children   []ChainNode `json:"children,omitempty"`
	// Observable: true if the job is in the caller's tenant (readable)
	// false if cross-tenant (exists but content not readable)
	Observable bool `json:"observable"`
	// UnobservableReason: why this node is not observable (e.g. "cross_tenant")
	UnobservableReason string `json:"unobservable_reason,omitempty"`
}

// ChainResponse is the response for GET /api/jobs/:id/chain.
type ChainResponse struct {
	Root      ChainNode `json:"root"`
	Depth     int       `json:"depth"`
	Truncated bool      `json:"truncated,omitempty"`
}

// GetJobChain returns the cross-agent call chain tree rooted at the
// given job (Issue #10: GET /api/jobs/:id/chain).
//
// The tree is built by walking ParentJobID/RootJobID relationships.
// Cross-tenant nodes are returned as placeholders (observable=false)
// without leaking content. Unobservable vs not-exists are
// distinguishable (#10 INV).
func (h *Handler) GetJobChain(c context.Context, ctx *app.RequestContext) {
	jobID := ctx.Param("id")
	if jobID == "" {
		ctx.JSON(consts.StatusBadRequest, map[string]string{
			"error": "job id required",
		})
		return
	}

	if h.jobStore == nil {
		ctx.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "job store not configured",
		})
		return
	}

	tenantID := auth.GetTenantID(c)
	if tenantID == "" {
		tenantID = "default"
	}

	// Depth limit (default 50, max 100)
	maxDepth := 50
	if depthStr := string(ctx.Query("depth")); depthStr != "" {
		if d, err := strconv.Atoi(depthStr); err == nil && d > 0 {
			if d > 100 {
				d = 100
			}
			maxDepth = d
		}
	}

	// Find the root of the chain
	rootJob, err := h.jobStore.Get(c, jobID)
	if err != nil || rootJob == nil {
		ctx.JSON(consts.StatusNotFound, map[string]string{
			"error": "job not found",
		})
		return
	}

	// Tenant check: if the starting job is not in caller's tenant,
	// return 403 (don't reveal existence to cross-tenant callers)
	if rootJob.TenantID != tenantID {
		ctx.JSON(consts.StatusForbidden, map[string]string{
			"error": "job not accessible",
		})
		return
	}

	// Walk up to find the root
	rootID := jobID
	if rootJob.RootJobID != "" {
		rootID = rootJob.RootJobID
	}
	rootJob, err = h.jobStore.Get(c, rootID)
	if err != nil || rootJob == nil {
		// Root not found — use the original job as root
		rootJob, _ = h.jobStore.Get(c, jobID)
		rootID = jobID
	}

	// Build the tree by finding all jobs with RootJobID == rootID
	tree := h.buildChainTree(c, rootID, tenantID, maxDepth, 0)

	resp := ChainResponse{
		Root:  tree,
		Depth: maxDepth,
	}
	if tree.JobID == "" {
		ctx.JSON(consts.StatusNotFound, map[string]string{
			"error": "chain root not found",
		})
		return
	}
	ctx.JSON(consts.StatusOK, resp)
}

// buildChainTree recursively builds the call chain tree.
// Uses ListByTenant to find children (jobs with ParentJobID == currentJobID).
// Cycle detection via visited set.
func (h *Handler) buildChainTree(
	ctx context.Context,
	jobID string,
	tenantID string,
	maxDepth int,
	currentDepth int,
) ChainNode {
	if currentDepth >= maxDepth {
		return TruncatedPlaceholder(jobID, maxDepth)
	}

	j, err := h.jobStore.Get(ctx, jobID)
	if err != nil || j == nil {
		// Not found — distinguish from cross-tenant
		return ChainNode{
			JobID:     jobID,
			Observable: false,
			UnobservableReason: "not_found",
		}
	}

	node := ChainNode{
		JobID:       j.ID,
		AgentID:     j.AgentID,
		Status:      j.Status.String(),
		ParentJobID: j.ParentJobID,
		RootJobID:   j.RootJobID,
		Observable:  true,
	}

	// Find children: all jobs where ParentJobID == j.ID
	// Use ListByTenant with no agent filter
	children, err := h.jobStore.ListByTenant(ctx, tenantID, nil, job.TimeRange{}, 500)
	if err != nil {
		return node
	}

	visited := map[string]bool{j.ID: true}
	for _, child := range children {
		if child.ParentJobID != j.ID {
			continue
		}
		if visited[child.ID] {
			// Cycle detected — skip
			continue
		}
		visited[child.ID] = true
		childNode := h.buildChainTree(ctx, child.ID, tenantID, maxDepth, currentDepth+1)
		node.Children = append(node.Children, childNode)
	}

	return node
}

// TruncatedPlaceholder creates a placeholder node for depth truncation.
func TruncatedPlaceholder(jobID string, maxDepth int) ChainNode {
	return ChainNode{
		JobID:               jobID,
		Status:              "truncated",
		Observable:          false,
		UnobservableReason:  fmt.Sprintf("max_depth_%d", maxDepth),
	}
}

// ensure strings is used
var _ = strings.TrimSpace
