# T0→T1→T2 Migration Guide (Issue #12)

> **Status**: active
> **Date**: 2026-09-21
> **关联**: ADR-0003 (#2), Epic #1; Issue #12

## 1. T0→T1 Migration

### 1.1 What changes

| Aspect | T0 (Blackbox) | T1 (SDK) |
| --- | --- | --- |
| Event granularity | Job + tool call (runtime framework) | Step + checkpoint (SDK) |
| Side-effect guarantee | None (external agent self-discipline) | Local idempotency (SDK store), not at-most-once |
| Crash recovery | Not provided | Local checkpoint recovery |
| Integration cost | ≤ 10 lines config | ≤ 10 lines code, ≤ 0.5 person-day |

### 1.2 Migration steps

1. **Keep T0 config**: The `external_http` agent config remains as-is.
2. **Add SDK import**: `pip install aetheris-durability` (Python) or `go get` (Go).
3. **Wrap entry point**: Add `@reporter.step("step_name")` decorator or `with reporter.job(...)` context manager.
4. **Set env vars**: `AETHERIS_ENDPOINT`, `AETHERIS_TENANT`, `AETHERIS_TOKEN`.
5. **Verify**: Run the agent, check `GET /api/jobs/:id/events` returns step-level events.

### 1.3 Dual-write compatibility period

During migration, both T0 (runtime framework events) and T1 (SDK events) may coexist:

- **T0 events**: `tool_called`, `tool_invocation_started`, `tool_invocation_finished`, `command_committed` (written by runtime framework)
- **T1 events**: `step_started`, `step_finished`, `step_failed`, `checkpoint_saved` (written by SDK)

**Dedup rule**: The same logical step must NOT appear twice in the event stream. Dedup is based on:
- `(tenant_id, job_id, event_uid)` at the ingest level (#4)
- `idempotency_key` for business-level dedup (SDK `IdempotentTool`)

**Detection**: If both `tool_invocation_finished` (T0) and `step_finished` (T1) exist for the same `job_id` + `step_id`, the migration tool flags it as "dual-write active" — this is expected during transition, not an error.

**Exit criteria**: When all steps have T1 events and no T0-only steps remain, dual-write can be disabled (remove the T0 `external_http` wrapper).

## 2. T1→T2 Migration

### 2.1 What changes

| Aspect | T1 (SDK) | T2 (Native Runtime) |
| --- | --- | --- |
| Side-effect guarantee | Local idempotency (not at-most-once) | At-most-once (InvocationLedger) |
| Replay fidelity | NOT PROVEN (D7: T1 events don't participate in runtime replay) | Full step + effect replay |
| Crash recovery | Local checkpoint | End-to-end durable recovery |
| Integration cost | ≤ 0.5 person-day | ≤ 3 person-days |

### 2.2 T1→T2 judgment checklist

A step MUST be upgraded to T2 (Runtime Tool) if ANY of these are true:

| # | Condition | Why T2 required |
| --- | --- | --- |
| 1 | Step has irreversible external side effects (payment, email, ticket creation) | At-most-once required; T1 cannot guarantee |
| 2 | Step must survive cross-worker failover | T1 checkpoint is local-only; T2 is durable in jobstore |
| 3 | Step result must be replayable without re-execution | T1 events don't participate in runtime replay (D7) |
| 4 | Step involves LLM calls that must be deterministic on replay | T2 EffectStore caches LLM responses |
| 5 | Step requires InvocationLedger acquire/commit | At-most-once tool execution |

Steps that are safe to keep at T1:
- Read-only operations (queries, data fetching)
- Stateless transformations
- Operations where at-least-once is acceptable

### 2.3 Upgrade procedure

1. **Register as RuntimeTool**: Implement the `RuntimeTool` interface (`Name()`, `Description()`, `Schema()`, `Execute()`).
2. **Register with tool registry**: `registry.Register("my_tool", &MyTool{})`.
3. **Update agent config**: Add the tool to `configs/agents.yaml` `tools:` list.
4. **Remove SDK wrapper**: The `@reporter.step()` decorator is no longer needed — runtime writes step events natively.
5. **Verify**: Run `aetheris verify <job_id>` — should show T2 guarantees (at-most-once, replay).

### 2.4 Side-effect handoff boundary

**Critical**: When upgrading from T1 to T2, the side-effect execution authority transfers from the SDK function to the Aetheris InvocationLedger. The transition point is:

```
T1: SDK function executes → side effect happens → SDK records event
T2: Ledger.Acquire → (if AllowExecute) function executes → Ledger.Commit
```

**Double execution window**: If T1 and T2 both execute the same step during transition, the side effect may execute twice. Prevention:
- Ensure the T1 wrapper is removed BEFORE the T2 tool is registered
- Use `idempotency_key` to deduplicate at the Ledger level
- Test with a mock side effect that counts executions

## 3. Verification Tool: `aetheris verify-tier`

The `aetheris verify-tier <job_id>` command outputs the actual tier and missing guarantees.

### Usage

```bash
aetheris verify-tier <job_id>
```

### Output format

```
Job: job-abc123
Agent: my-agent
Tenant: tenant-acme

Tier: T1 (SDK-reported)

Event analysis:
  - step_started: 5 events (T1 SDK)
  - step_finished: 4 events (T1 SDK)
  - step_failed: 1 event (T1 SDK)
  - checkpoint_saved: 4 events (T1 SDK)
  - tool_invocation_finished: 0 events (T0/T2 — not present)
  - ledger_acquired: 0 events (T2 only — not present)
  - ledger_committed: 0 events (T2 only — not present)

Guarantees:
  ✓ Event granularity: step-level (T1)
  ✗ Side-effect at-most-once: NOT PROVEN (no ledger events)
  ✓ Crash recovery: local checkpoint (T1)
  ✗ Replay fidelity: NOT PROVEN (D7: T1 events not in replay)
  ✗ Cross-agent chaining: NOT PROVEN (no parent_job_id)

Missing for T2 upgrade:
  1. Register as RuntimeTool (InvocationLedger required for at-most-once)
  2. Add ledger_acquired/ledger_committed events
  3. Register with tool registry
```

### Implementation

The verification tool reads the job's event stream and classifies events by tier:

- **T0 events**: `tool_called`, `tool_returned`, `tool_invocation_started`, `tool_invocation_finished`, `command_emitted`, `command_committed`
- **T1 events**: `step_started`, `step_finished`, `step_failed`, `step_retried`, `step_skipped`, `checkpoint_saved`, `effect_recorded`, `job_started`
- **T2 events**: `ledger_acquired`, `ledger_committed`, `node_started`, `node_finished`, `step_committed`

The tool counts each category and determines the effective tier:
- Only T0 events → Tier: T0
- T1 events present, no T2 → Tier: T1
- T2 events present → Tier: T2
- Both T0 and T1 → Tier: T0+T1 (dual-write active)

## 4. Rollback

Migration is reversible at each stage:
- T1→T0: Remove SDK wrapper, keep `external_http` config
- T2→T1: Remove RuntimeTool registration, add SDK wrapper back
- Historical jobs: replay capability must not decrease (#12 INV) — T0/T1 jobs remain replayable at their original tier level
