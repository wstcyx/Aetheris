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

import "github.com/Colin4k1024/Aetheris/v2/pkg/redaction"

// DefaultRedactionPolicy returns the redaction policy for the OBS
// integration (#5): covers StepStartedPayload.Input,
// StepFinishedPayload.Output, decision_snapshot, reasoning_snapshot,
// and LLM/tool-related payloads that may contain prompts, model
// outputs, or PII.
//
// The policy uses Redact mode (replace with "***REDACTED***") for
// input/output fields and Hash mode for reasoning fields that may
// need deterministic comparison during replay.
//
// This policy is the minimum required by #5. It can be extended or
// overridden via config (pkg/redaction.PolicyConfig).
func DefaultRedactionPolicy() *redaction.RedactionPolicy {
	return &redaction.RedactionPolicy{
		EventRules: map[string][]redaction.FieldMask{
			// step_started: redact Input (may contain prompt/PII)
			"step_started": {
				{FieldPath: "input", Mode: redaction.RedactionModeRedact},
			},
			// step_finished: redact Output (may contain model output/PII)
			"step_finished": {
				{FieldPath: "output", Mode: redaction.RedactionModeRedact},
			},
			// step_failed: redact Error (may contain stack traces with PII)
			"step_failed": {
				{FieldPath: "error", Mode: redaction.RedactionModeRedact},
			},
			// decision_snapshot: redact goal and reasoning (may contain user input)
			"decision_snapshot": {
				{FieldPath: "goal", Mode: redaction.RedactionModeRedact},
				{FieldPath: "reasoning", Mode: redaction.RedactionModeHash},
			},
			// reasoning_snapshot: hash reasoning (replay may need determinism)
			"reasoning_snapshot": {
				{FieldPath: "reasoning", Mode: redaction.RedactionModeHash},
			},
			// tool_invocation_finished: redact Result (may contain model output)
			"tool_invocation_finished": {
				{FieldPath: "result", Mode: redaction.RedactionModeRedact},
			},
		},
		// Global rules: apply to all event types
		GlobalRules: []redaction.FieldMask{
			// Redact any top-level "prompt" field
			{FieldPath: "prompt", Mode: redaction.RedactionModeRedact},
			// Redact any top-level "response" field
			{FieldPath: "response", Mode: redaction.RedactionModeRedact},
		},
	}
}
