# SDK Version Policy & Compatibility Matrix (Issue #13)

> **Status**: accepted (D6 裁决：不拆包，durability + reporting 共存)
> **Date**: 2026-09-21
> **关联**: ADR-0003 (#2), Issue #13; Epic #1 D6

## D6 Decision: Don't Split Packages

**Decision**: `durability` (local persistence) and `reporting` (T1 upload) stay in the same package/module. No split.

**Rationale**:
- Python: `aetheris_durability` package already has both `runner.py` (local) and `reporting.py` (upload) as submodules. Splitting would require users to install two packages and import from two namespaces — violates "≤ 10 lines integration" constraint.
- Go: `sdk/durability` module already has `core/` and `reporting/` as subpackages. Splitting would change module path — violates "import path not changed" constraint.
- Reporting is optional (disabled by default, enabled via env vars). Keeping it in the same package doesn't force users to pull reporting deps if they don't use it — Python's `reporting.py` only imports stdlib `urllib`, Go's `reporting/` only imports `net/http`.

## Three-Package Responsibility Boundaries

| Package | Path | Language | Responsibility |
| --- | --- | --- | --- |
| `aetheris_durability` | `sdk/durability-py/` | Python | Crash recovery + checkpoint + idempotency (local) + T1 reporting (optional) |
| `durability` | `sdk/durability/` | Go | Same as above, Go equivalent |
| `pkg/agent/sdk` | `pkg/agent/sdk/` (main module) | Go | High-level Agent facade (runtime-coupled, requires AgentRuntime) |

**"Which should I use?"**:
- External agent wanting crash recovery → `sdk/durability` (Go) or `sdk/durability-py` (Python)
- External agent wanting T1 observability → same package, enable reporting via env
- Agent running inside Aetheris runtime → `pkg/agent/sdk` (native Step/Tool model)

## Semver Rules

| Rule | Enforcement |
| --- | --- |
| Major version: no breaking API changes | CI `sdk-go` + `sdk-python` jobs (#14) run on every PR |
| Minor version: new features, backward compatible | `go test -race` + `pytest` must pass |
| Patch version: bug fixes only | No new API surface |
| Deprecated API: marked for 1 minor, then removed | Deprecation notice in changelog |

### Current Versions

| Package | Version | Status |
| --- | --- | --- |
| `aetheris_durability` (Python) | 0.2.0 | pre-release (0.x.y: any breaking change allowed in minor) |
| `durability` (Go) | no tag | pre-release (module path has no /v2 suffix yet) |
| `pkg/agent/sdk` (Go) | follows main module v2.x | stable |

## Compatibility Matrix (New × Old SDK × New × Old Runtime)

| | New SDK | Old SDK |
| --- | --- | --- |
| **New Runtime** | Full T1 features. SDK reports via #3 contract v1. Events land in jobstore via #4 ingest. Redaction via #5. | Legacy mode. SDK without reporting. Runtime treats as T0 (external_http). No step events from SDK. |
| **Old Runtime** | Forward compatible. SDK sends `schema_version: "1"`. Old runtime ignores unknown fields (#3 §5). SDK reporting silently fails (fail-open). No crash. | No change. SDK is local-only (no reporting). Runtime is current behavior. |

### Failure Observability

| Cell | Failure Mode | Observable? |
| --- | --- | --- |
| New SDK × New Runtime | Network failure | Yes — #7 dropped_count, circuit_open |
| New SDK × Old Runtime | Ingest endpoint 404 | Yes — #7 failed_count |
| Old SDK × New Runtime | No reporting | N/A (no reporting to fail) |
| Old SDK × Old Runtime | N/A | N/A |

**Key invariant (#13)**: No cell has silent failure. Every failure mode is observable via SDK stats or server-side metrics.

## Version Negotiation Failure Behavior

| Scenario | Behavior |
| --- | --- |
| `schema_version` missing | Server treats as legacy (v0), accepts without dedup |
| `schema_version` invalid | Server rejects with `unsupported_schema_version` |
| Unknown event type | Server rejects with `unknown_type` |
| SDK newer than server | Server ignores unknown fields, accepts known ones (#3 §5 forward compat) |
| Server newer than SDK | Server accepts older version, response includes `accepted_version` |

## Pinning for Regression

A pinned-old-version sample project must exist to verify backward compatibility:

```
sdk/durability-py/tests/regression/
  pinned-0.1.0/  # pins to v0.1.0 (pre-reporting), verifies local-only still works
```

The CI `sdk-python` job runs this pinned project's tests alongside current version tests.
