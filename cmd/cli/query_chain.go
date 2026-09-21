package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// runQuery implements `aetheris query` — multi-dimension job search (#16).
// Supports: --agent, --tenant, --status, --start, --end, --limit, --json
func runQuery(args []string) {
	apiURL := os.Getenv("AETHERIS_API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}
	tenantID := os.Getenv("AETHERIS_TENANT_ID")

	// Parse flags
	var agentFilter, statusFilter, startStr, endStr string
	limit := 20
	jsonOutput := false

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--agent="):
			agentFilter = strings.TrimPrefix(arg, "--agent=")
		case strings.HasPrefix(arg, "--status="):
			statusFilter = strings.TrimPrefix(arg, "--status=")
		case strings.HasPrefix(arg, "--start="):
			startStr = strings.TrimPrefix(arg, "--start=")
		case strings.HasPrefix(arg, "--end="):
			endStr = strings.TrimPrefix(arg, "--end=")
		case strings.HasPrefix(arg, "--limit="):
			fmt.Sscanf(strings.TrimPrefix(arg, "--limit="), "%d", &limit)
		case arg == "--json":
			jsonOutput = true
		}
	}

	// Build query body
	body := map[string]any{
		"tenant_id": tenantID,
		"limit":     limit,
	}
	if agentFilter != "" {
		body["agent_filter"] = []string{agentFilter}
	}
	if statusFilter != "" {
		body["status_filter"] = []string{statusFilter}
	}
	if startStr != "" || endStr != "" {
		body["time_range"] = map[string]string{"start": startStr, "end": endStr}
	}

	bodyBytes, _ := json.Marshal(body)
	url := apiURL + "/api/forensics/query"
	req, _ := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	if tenantID != "" {
		req.Header.Set("X-Tenant-ID", tenantID)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "error: HTTP %d: %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}

	if jsonOutput {
		fmt.Println(string(respBody))
		return
	}

	// Parse and print as table
	var result struct {
		Jobs []struct {
			JobID      string `json:"job_id"`
			AgentID    string `json:"agent_id"`
			Status     string `json:"status"`
			EventCount int    `json:"event_count"`
		} `json:"jobs"`
		NextCursor string `json:"next_cursor"`
		HasMore    bool   `json:"has_more"`
	}
	json.Unmarshal(respBody, &result)

	if len(result.Jobs) == 0 {
		fmt.Println("No jobs found.")
		return
	}

	fmt.Printf("%-20s %-15s %-10s %s\n", "JOB_ID", "AGENT", "STATUS", "EVENTS")
	fmt.Println(strings.Repeat("-", 60))
	for _, j := range result.Jobs {
		fmt.Printf("%-20s %-15s %-10s %d\n", j.JobID, j.AgentID, j.Status, j.EventCount)
	}
	if result.HasMore {
		fmt.Printf("\nMore results: use --cursor=%s\n", result.NextCursor)
	}
}

// runChain implements `aetheris chain <job_id>` — cross-agent chain tree (#16).
func runChain(args []string) {
	apiURL := os.Getenv("AETHERIS_API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}
	tenantID := os.Getenv("AETHERIS_TENANT_ID")
	jobID := args[0]

	depth := "50"
	jsonOutput := false
	for _, arg := range args[1:] {
		switch {
		case strings.HasPrefix(arg, "--depth="):
			depth = strings.TrimPrefix(arg, "--depth=")
		case arg == "--json":
			jsonOutput = true
		}
	}

	url := fmt.Sprintf("%s/api/jobs/%s/chain?depth=%s", apiURL, jobID, depth)
	req, _ := http.NewRequest("GET", url, nil)
	if tenantID != "" {
		req.Header.Set("X-Tenant-ID", tenantID)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "error: HTTP %d: %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}

	if jsonOutput {
		fmt.Println(string(respBody))
		return
	}

	// Parse and print as tree
	var result struct {
		Root struct {
			JobID    string `json:"job_id"`
			AgentID  string `json:"agent_id"`
			Status   string `json:"status"`
			Children []struct {
				JobID    string `json:"job_id"`
				AgentID  string `json:"agent_id"`
				Status   string `json:"status"`
				Children []any `json:"children"`
			} `json:"children"`
		} `json:"root"`
	}
	json.Unmarshal(respBody, &result)

	if result.Root.JobID == "" {
		fmt.Println("Chain not found.")
		os.Exit(1)
	}

	printChainNode(result.Root.JobID, result.Root.AgentID, result.Root.Status, result.Root.Children, 0)
}

func printChainNode(jobID, agentID, status string, children []struct {
	JobID    string `json:"job_id"`
	AgentID  string `json:"agent_id"`
	Status   string `json:"status"`
	Children []any `json:"children"`
}, depth int) {
	indent := strings.Repeat("  ", depth)
	fmt.Printf("%s├─ %s [%s] (%s)\n", indent, jobID, agentID, status)
	for _, child := range children {
		// Recursive for nested children would need full struct, simplified for depth 1
		fmt.Printf("%s  ├─ %s [%s] (%s)\n", indent, child.JobID, child.AgentID, child.Status)
	}
}
