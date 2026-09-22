# 6-Agent Pilot Onboarding Handbook (Issue #17)

> **Status**: active (收口 Issue — H0 input pending)
> **Date**: 2026-09-21
> **关联**: Epic #1, Issue #17; all #2–#16

## H0 Input Required (HUMAN_BLOCKED)

This handbook is a framework. The following human inputs are required before pilot execution:

1. **6 个存量 agent 的语言与框架分布** — determines #6/#11 priority and D5
2. **其中哪几个带不可逆副作用**（支付、发信、工单、对外写） — determines which need T2

Without these, the pilot cannot proceed. This is a genuine HUMAN_BLOCKED per Goal mode rule 8.

## Measurable Acceptance Criteria (from Epic #1)

| Criterion | Target | Verification Method |
| --- | --- | --- |
| 最低档接入 | ≤ 10 行代码、0 行业务逻辑改动、≤ 0.5 人日 | Per-agent diff + time record |
| 深度接入 | ≤ 3 人日 | At least 1 agent reaches T2 |
| 一次任务回答四个问题 | 执行到哪一步/为什么这么决策/失败在哪/能否恢复 | Real failure job, located WITHOUT contacting agent team |
| 按四维度检索 + trace + 无副作用 replay | tenant/agent/time/job + trace + replay | Real data, actual CLI output |
| 6 个 agent / 10 周 | timeline | Gantt chart |

**Criteria must NOT be rewritten.** If any is unmet, write `NOT PROVEN` and open a follow-up Issue. Do not lower the bar.

## Per-Agent Onboarding Template

For each of the 6 agents, fill this template:

```
Agent: [name]
Language: [Python/Go/Node/Java/...]
Framework: [FastAPI/Flask/gin/echo/...]
Side effects: [none/payment/email/ticket/external-write]
Recommended tier: [T0/T1/T2]
Reason for tier: [why this tier]

### Integration diff
[≤ 10 lines for T1, config-only for T0]

### Time spent
[hours, with breakdown]

### Obstacles encountered
[list]

### Four-question verification (on a real failure job)
1. 执行到哪一步: [answer + CLI command used]
2. 为什么这么决策: [answer + CLI command used]
3. 失败在哪: [answer + CLI command used]
4. 能否从断点恢复: [answer + CLI command used]

### Redaction spot-check
[抽查事件流确认无明文 prompt/PII — #5 实地回归]

### Degradation drill (for at least 1 agent)
[采集后端断电演练 — #7 实地回归]
```

## Per-Agent Acceptance Summary

| Agent | Language | Tier | Lines | Time | Four Qs | Redaction | Degrade |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [1] | | | | | | | |
| [2] | | | | | | | |
| [3] | | | | | | | |
| [4] | | | | | | | |
| [5] | | | | | | | |
| [6] | | | | | | | |

## Negative Sample Selection (must include at least one of each)

- [ ] At least 1 failed job (not happy path)
- [ ] At least 1 timeout job
- [ ] At least 1 mid-crash job

## Rollback

Each agent's integration is a disable switch. Single rollback doesn't affect others.

## Known Limitations

- **H0 input not provided**: 6 agent identities, languages, side-effect profiles
- **No production release**: pilot only, production deployment is human responsibility
- **No business logic migration**: agents stay in their current framework (constraint)
