package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// runVerifyTier reads a job's events and classifies the actual tier
// and missing guarantees (#12 verification command).
func runVerifyTier(jobID string) {
	apiURL := os.Getenv("AETHERIS_API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}
	tenantID := os.Getenv("AETHERIS_TENANT_ID")

	// Fetch events
	url := fmt.Sprintf("%s/api/jobs/%s/events", apiURL, jobID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if tenantID != "" {
		req.Header.Set("X-Tenant-ID", tenantID)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect to %s: %v\n", apiURL, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "error: HTTP %d\n", resp.StatusCode)
		os.Exit(1)
	}

	var result struct {
		Events []struct {
			Type string `json:"type"`
		} `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Classify events by tier
	t0Events := map[string]int{}
	t1Events := map[string]int{}
	t2Events := map[string]int{}

	t0Types := map[string]bool{
		"tool_called": true, "tool_returned": true,
		"tool_invocation_started": true, "tool_invocation_finished": true,
		"command_emitted": true, "command_committed": true,
	}
	t1Types := map[string]bool{
		"step_started": true, "step_finished": true, "step_failed": true,
		"step_retried": true, "step_skipped": true, "checkpoint_saved": true,
		"effect_recorded": true, "job_started": true,
	}
	t2Types := map[string]bool{
		"ledger_acquired": true, "ledger_committed": true,
		"node_started": true, "node_finished": true, "step_committed": true,
	}

	for _, ev := range result.Events {
		switch {
		case t0Types[ev.Type]:
			t0Events[ev.Type]++
		case t1Types[ev.Type]:
			t1Events[ev.Type]++
		case t2Types[ev.Type]:
			t2Events[ev.Type]++
		}
	}

	// Determine effective tier
	tier := "T0"
	hasT0 := len(t0Events) > 0
	hasT1 := len(t1Events) > 0
	hasT2 := len(t2Events) > 0

	if hasT2 {
		tier = "T2"
	} else if hasT1 && hasT0 {
		tier = "T0+T1 (dual-write active)"
	} else if hasT1 {
		tier = "T1 (SDK-reported)"
	}

	// Output report
	fmt.Printf("Job: %s\n", jobID)
	fmt.Printf("Tenant: %s\n", tenantID)
	fmt.Printf("\nTier: %s\n\n", tier)

	fmt.Println("Event analysis:")
	printEventCounts("T0", t0Events)
	printEventCounts("T1", t1Events)
	printEventCounts("T2", t2Events)

	fmt.Println("\nGuarantees:")
	hasAtMostOnce := t2Events["ledger_committed"] > 0
	hasCheckpoint := t1Events["checkpoint_saved"] > 0 || t2Events["step_committed"] > 0
	hasReplay := t2Events["node_finished"] > 0 && t2Events["command_committed"] > 0

	printGuarantee("Event granularity", hasT1 || hasT2,
		hasT1 && !hasT2, "step-level", "job-level")
	printGuarantee("Side-effect at-most-once", hasAtMostOnce, !hasAtMostOnce,
		"at-most-once (ledger)", "NOT PROVEN (no ledger events)")
	printGuarantee("Crash recovery", hasCheckpoint, !hasCheckpoint,
		"checkpoint", "NOT PROVEN")
	printGuarantee("Replay fidelity", hasReplay, !hasReplay,
		"full step+effect replay", "NOT PROVEN (D7: T1 not in replay)")

	if !hasT2 {
		fmt.Println("\nMissing for T2 upgrade:")
		if !hasAtMostOnce {
			fmt.Println("  1. Register as RuntimeTool (InvocationLedger required for at-most-once)")
			fmt.Println("  2. Add ledger_acquired/ledger_committed events")
		}
		fmt.Println("  3. Register with tool registry")
	}
}

func printEventCounts(label string, events map[string]int) {
	if len(events) == 0 {
		fmt.Printf("  %s: 0 events\n", label)
		return
	}
	for ev, count := range events {
		fmt.Printf("  %s: %d events (%s)\n", label, count, ev)
	}
}

func printGuarantee(name string, met bool, notMet bool, metDesc, notMetDesc string) {
	if met {
		fmt.Printf("  ✓ %s: %s\n", name, metDesc)
	} else {
		fmt.Printf("  ✗ %s: %s\n", name, notMetDesc)
	}
}

// ensure strings is used
var _ = strings.TrimSpace
