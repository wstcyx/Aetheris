# ADR-0003: Tiered Integration Guarantees (T0/T1/T2)

> **Status**: accepted
> **Date**: 2026-09-21
> **Owner**: architect
> **关联**: Epic #1, Issue #2; 承接 ADR-0002 四层架构边界

## 背景与约束

集团内 6 个异构智能体要接入 Aetheris（见 Epic #1）。接入方语言不同、框架不同、部分代码不归我方控制，会问「我花多少成本能换到什么保证」。目前只有 `docs/guides/sdk.md` 一句话提到 T0 不保证 at-most-once，没有系统的分层保证矩阵。

Aetheris 已有三档接入能力（定义见 Epic #1）：

| 档位 | 定位 | 现有实现入口 |
| --- | --- | --- |
| **T0 零侵入** | 沿用 `external_http` / 旁路代理，不改智能体一行代码 | `internal/app/api/external_agent_tool.go` |
| **T1 最小侵入** | SDK 装饰器 / context manager / middleware 包裹，产出 step 级事件与 checkpoint | `sdk/durability-py`、`sdk/durability`（**当前无网络上报代码，#6/#11 从零建设**） |
| **T2 深度接入** | native Runtime Tool + Invocation Ledger + Effect Store，获得 at-most-once 与断点续跑 | `internal/agent/runtime/executor/node_adapter.go` + `ledger.go` |

本 ADR 把三档每一档的**保证与非保证**写死，作为 #3–#17 全部下游 Issue 的验收基准。

### 与 ADR-0002 的关系

ADR-0002 定义了 Aetheris 在四层架构（L0–L3）中的 L1 执行层定位，并声明不跨层定义 L2（编排）与 L3（治理）的职责。本 ADR **在 L1 内部**细分接入方拿到的保证，不跨层定义 L2/L3 职责。具体：

- **T0/T1/T2 均为 L1 能力**，不依赖 L2 `superagent-base` 或 L3 `hermesx` 的接口实现。
- 跨 agent 因果串联（`parent_job_id` / `root_job_id`）属 L1 事件流能力，但跨层治理联动（如 L3 策略下发）不在本 ADR 范围。
- `GovernanceProvider.ReportEvent`（ADR-0002 定义的上报路径）与 T1 SDK 上报（#6/#11）是不同通道：前者是 L1→L3 治理上报，后者是 SDK→L1 事件上报。两者在本 ADR 中不混用。

## 备选方案

### 方案 A: 不写矩阵，让接入方自行从代码推断

- **优点**: 零文档投入
- **风险**: 6 个接入方会反复问同一组问题；实现者在 #3–#17 中会各自解释保证边界，出现口径不一致；取证时「能不能 replay」「有没有 at-most-once」没有统一答案
- **不选原因**: Epic #1 明确要求口径可度量且不可改写；没有矩阵就没有验收基准

### 方案 B: 只写保证，不写非保证

- **优点**: 文档简洁，读起来正面
- **风险**: 接入方误以为 T1 能保证外部 agent 内部调用的 at-most-once（实际不能）；取证时发现某项不保证，但文档没说过不保证，导致信任断裂
- **不选原因**: Issue #2 核心验收逻辑 INV-1 要求「任何一格不得同时出现『保证』与『依赖外部 agent 自律』两种表述」——只写正面必然在副作用语义维度违反此不变量

### 方案 C: 完整矩阵，每格指向代码证据或标 NOT PROVEN（采用）

- **优点**: 接入方和实现者对同一套事实；NOT PROVEN 项明确列出需补的验证（对应下游 Issue）；满足 GWT-2「保证项可被指认」
- **风险**: 矩阵中 NOT PROVEN 项多（尤其 T1，因上报通道尚未建设），接入方可能觉得 T1 价值不足
- **选择原因**: Issue #2 验收标准 2 要求「每一格要么指向代码/测试证据，要么明标 NOT PROVEN 并列出要补的验证」；Epic #1 要求「口径不得被改写，某项未达标时写 NOT PROVEN 而非调低口径」

## 决策结果

**采用方案 C**: 完整保证矩阵，七维度 × 三档，每格给出代码证据或 `NOT PROVEN` + 需补的验证。

### 1. 事件粒度

| 维度 | T0 零侵入 | T1 最小侵入 | T2 深度接入 |
| --- | --- | --- | --- |
| **事件粒度** | **Job 级 + 工具调用级**。Aetheris 记录 job 创建、外部调用出入站（`tool_called`、`tool_returned`、`tool_invocation_started`、`tool_invocation_finished`、`command_committed`），但**不记录外部 agent 内部的 step**。<br>证据: `node_adapter.go:607,617,623,757,766,782` | **Step 级 + Checkpoint**。SDK 产出 `step_started`、`step_finished`、`step_failed`、`checkpoint_saved`。<br>证据: `sdk/durability-py/aetheris_durability/types.py` EventType 枚举已定义这些类型；`sdk/durability/core/types.go` EventStepStarted/Finished/Failed 常量已有。<br>**但**: SDK→runtime 上报通道**尚未建设**（#6/#11），当前这些事件只在 SDK 本地 `MemoryStore` 内流转，不进入 runtime jobstore。<br>**NOT PROVEN**: step 级事件经 #3 契约上报到 runtime 后端后的端到端完整性。需 #3 定稿 + #4 ingest 端点 + #6/#11 SDK 实现后补验。 | **Step 级 + Effect 级 + Checkpoint**。Runtime 原生写入 `node_started`、`node_finished`、`command_committed`、`tool_invocation_started`、`tool_invocation_finished`、`step_committed`、`checkpoint_saved`。<br>证据: `event.go:28-36`；`node_adapter.go:452-485`（Acquire→Execute→Commit 路径）。<br>**NOT PROVEN**: `ledger_acquired`/`ledger_committed` 事件类型已定义（`event.go:112-113`），但 `DefaultAtomicCommit`（`atomic_commit.go:52-94`）仅在测试中被调用，生产路径 `node_adapter.go` 的 `runNodeExecute` 不调用它——因此这两个事件当前**不在生产事件流中出现**。需将 `DefaultAtomicCommit` 接入 `node_adapter.go` 后补验。 |

### 2. 副作用语义

| 维度 | T0 零侵入 | T1 最小侵入 | T2 深度接入 |
| --- | --- | --- | --- |
| **副作用语义** | **无保证**。Aetheris 对外部 agent 的 HTTP 调用是 fire-and-forget：发送请求、记录响应，但**不控制也不观测外部 agent 内部的重试、副子调用、幂等性**。`Idempotency-Key` header 转发给外部 agent，但去重正确性是外部 agent 的责任。<br>证据: `external_agent_tool.go:270`（`client.Do(req)` 后直接返回响应）；`external_agent_tool.go:240`（转发 `Idempotency-Key`）；全文无 `InvocationLedger` 或 `EffectStore` 引用。<br>**不变量**: T0 下外部 agent 内部调用的 at-most-once **依赖外部 agent 自律**，Aetheris 不提供此保证。 | **At-least-once（SDK 本地）+ 无保证（外部副作用）**。SDK 的 `IdempotentTool` 在本地 `Store` 内做幂等去重（`sdk/durability-py/aetheris_durability/idempotent.py` wrap 方法），保证同一 `idempotency_key` 的本地记录只写一次。但被包裹的外部副作用（发邮件、支付）的 at-most-once **不由 SDK 保证**——SDK 不能阻止进程在副作用执行后崩溃导致重试。<br>证据: `sdk/durability/idempotent/tool.go` Wrap 方法用 `ledger_acquired`/`ledger_committed` 事件做本地去重；但无 `EffectStore` 或跨进程协调。<br>**NOT PROVEN**: 上报通道故障时 SDK 是否能保证不丢事件（需 #7 降级自保实现后补验）。 | **At-most-once**。`InvocationLedger.Acquire` 在工具执行前获取执行权：若已有 committed 成功记录则返回 `ReturnRecordedResult` 跳过执行；若 `AllowExecute` 则执行后 `Commit` 标记完成。Replay 时从事件流读取已提交结果并注入，**不重新执行工具**。<br>证据: `node_adapter.go:452`（Acquire）；`:457`（ReturnRecordedResult 跳过）；`:480-482`（AllowExecute 执行）；`:769`（Commit）；`ledger_store.go:87-122`（Acquire 实现）；`replay.go:202-221`（replay 注入已完成结果）。<br>**已知边界**：Acquire→Execute→Commit 之间崩溃时，DB 中存在 `started-not-committed` 记录（`ledger_store.go:99-103`），后续 `Acquire` 返回 `WaitOtherWorker`，job 进入不可自动恢复状态（需超时清理或人工介入）。At-most-once 成立但工具结果丢失。 |

### 3. 断点恢复

| 维度 | T0 零侵入 | T1 最小侵入 | T2 深度接入 |
| --- | --- | --- | --- |
| **断点恢复** | **不提供**。外部 agent 崩溃后，Aetheris 只能看到 HTTP 调用未返回或返回错误，无法定位外部 agent 执行到哪一步、无法从断点续跑。<br>证据: `external_agent_tool.go` 不写入 `checkpoint_saved` 或 step 级事件；外部 agent 的内部状态对 Aetheris 不透明。 | **本地 checkpoint 恢复**。SDK `Runner` 在每个 step 成功后写入 `checkpoint_saved`，崩溃重启后从最后 checkpoint 恢复，不重跑已完成 step。<br>证据: `sdk/durability-py/aetheris_durability/runner.py` execute 方法在每步成功后调 `store.save_checkpoint`；`test_runner.py::test_execute_crash_recovery` 验证 step1 不重跑、step2 重试。<br>**NOT PROVEN**: checkpoint 经 #3 契约上报到 runtime 后端的端到端恢复（当前只验本地 `MemoryStore`）。需 #6 实现 + #4 ingest 后补验。 | **端到端断点恢复**。Runtime 在每个 step 的 `step_committed` 屏障后写 checkpoint（`event.go:36` 注释：写入顺序 `command_committed → node_finished → step_committed`）；崩溃后 Replay 从事件流重建状态，已提交的工具调用不重执行。<br>证据: `event.go:36`（step_committed 屏障注释）；`replay.go:116-348`（BuildFromEvents 从事件流重建）；`node_adapter.go:449-451`（replay 结果注入）；`embedded_store.go` + `pgstore.go` 持久化。 |

### 4. Replay 保真度

| 维度 | T0 零侵入 | T1 最小侵入 | T2 深度接入 |
| --- | --- | --- | --- |
| **Replay 保真度** | **Job 级 replay，无 step 保真**。Aetheris 可 replay job 的事件流（`tool_invocation_finished` 记录了外部调用的响应），但外部 agent 内部的 step 决策、中间状态不可见。Replay 能告诉你「调了外部 agent，得到了这个响应」，不能告诉你「外部 agent 内部为什么这么决策」。<br>证据: `replay.go:202-221`（从 `tool_invocation_finished` 恢复结果）；外部 agent 内部状态不在事件流中。 | **Step 级 replay（SDK 本地）**。SDK `Runner` 的事件流包含 step 级决策，本地 replay 可重建。但 **SDK 事件流与 runtime jobstore 当前不连通**，runtime 侧无法 replay SDK 的 step 级事件。<br>证据: `sdk/durability-py` 有独立 `MemoryStore` 和 `list_events`，但 grep `requests\|urllib\|http` 全空，无上报通道。<br>**NOT PROVEN**: 经 #3 契约上报后，runtime 侧能否 replay T1 事件。**D7 待裁决**（见下方「待裁决」节）。 | **完整 step 级 + effect 级 replay**。Runtime 从事件流重建完整执行状态：`node_finished` → 已完成节点；`command_committed` → 已提交命令及结果；`tool_invocation_finished` → 已完成工具调用及结果；`step_committed` → step 屏障。LLM 调用在 replay 时从 EffectStore 注入缓存响应，**不重新调用 LLM**。<br>证据: `replay.go:158,185,202,222`（各类事件重建）；`node_adapter.go:132-156`（LLM replay 跳过）；`node_adapter.go:449-451`（tool replay 跳过）。 |

### 5. 因果串联

| 维度 | T0 零侵入 | T1 最小侵入 | T2 深度接入 |
| --- | --- | --- | --- |
| **因果串联** | **不提供**。出站注入 `X-Aetheris-Job-ID` 等 header，但**入站不消费**——外部 agent 被调用后，其 job 不会挂到调用方的 trace 上。跨 agent 轨迹在接收端断裂。<br>证据: `external_agent_tool.go:243-247`（出站 set）；全仓 grep `X-Aetheris-Job-ID` 入站读取 = **零**（仅 test 文件读）；`governance/provider.go:106` `ParentJobID` 字段定义但**从未赋值**（grep `PolicyContext{` 零次构造、`ParentJobID =` 零次赋值）；`RootJobID` 全仓不存在。 | **不提供**（同 T0）。SDK 不消费入站 header，不写 `parent_job_id`。跨 agent 串联需 #9 实现（入站消费 header + 落 `parent_job_id`/`root_job_id`）。<br>**NOT PROVEN**: #9 实现后是否覆盖 T1 SDK 上报的事件。需 #9 完成后补验。 | **不提供**（当前）。`ParentJobID` 字段已定义但未接线，`RootJobID` 不存在。同 T0/T1，需 #9 实现入站消费。<br>**NOT PROVEN**: #9 实现后 T2 的 native runtime tool 路径是否会正确传播 `parent_job_id`。需 #9 完成后补验。 |

### 6. 脱敏范围

| 维度 | T0 零侵入 | T1 最小侵入 | T2 深度接入 |
| --- | --- | --- | --- |
| **脱敏范围** | **不提供写入侧脱敏**。`pkg/redaction` 只被 `pkg/proof/export.go` 引用（导出时脱敏），事件写入路径（`internal/`）**无任何脱敏调用**。外部 agent 的 prompt 和响应以明文落库。<br>证据: grep `redaction\.` 在 `internal/` = 零命中；`pkg/redaction/engine.go:44` `RedactData` 函数存在但仅被 `proof/export.go:204,211` 调用。 | **不提供写入侧脱敏**（同 T0）。SDK 上报的事件 payload 在 #5 实现前以明文写入。**#5（P0 安全）将在 `Append` 前插入脱敏管线**，覆盖 `StepStartedPayload.Input`、`StepFinishedPayload.Output`、`decision_snapshot` 等，但 #5 当前 blocked on D2 裁决。<br>**NOT PROVEN**: #5 实现后 T1 上报事件的脱敏覆盖完整性。需 #5 完成后补验。 | **不提供写入侧脱敏**（当前，同 T0/T1）。T2 的 `node_sink.go` Append 方法写入 step 级 payload 时同样不脱敏。**#5 实现后覆盖 T2 路径**（#5 要求所有 `Append` 调用点经过脱敏管线，含 `node_sink.go`）。<br>**NOT PROVEN**: #5 实现后 T2 全部 Append 入口的脱敏覆盖完整性。需 #5 完成后补验。 |

### 7. 接入成本

| 维度 | T0 零侵入 | T1 最小侵入 | T2 深度接入 |
| --- | --- | --- | --- |
| **接入成本** | **≤ 10 行配置，0 行代码改动**。在 `configs/agents.yaml` 中声明一个 `external_http` 类型的 agent 即可，不改智能体一行代码。<br>证据: `AGENTS.md` 「Adding a New Agent (Config-Driven)」节；`pkg/config` `AgentExternalConfig` 类型；`external_agent_tool.go` 从 config 读 URL/TokenEnv/Framework。<br>**不变量**: 口径「最低档 ≤ 10 行代码、0 行业务逻辑改动」由配置声明满足。 | **目标 ≤ 10 行代码，0 行业务逻辑改动，≤ 0.5 人日**。SDK 提供装饰器 `@aetheris.step(...)` 或 context manager `with aetheris.job(...)`，包裹现有函数即产出 step 级事件。<br>**NOT PROVEN**: 上报通道（#6 Python / #11 Go）尚未实现，接入成本未实测。#6 验收标准 1 要求「接入 diff ≤ 10 行且不含业务逻辑修改，由 diff 本身作证」+「实测接入耗时并记录」。 | **≤ 3 人日**。需将智能体的工具注册为 Aetheris `RuntimeTool`、配置 agent 定义、可能需适配 Step 编程模型。<br>**NOT PROVEN**: 6 个存量 agent 的实际迁移成本未测（#17 收口 Issue 负责验证）。需 #12 迁移工具 + #17 试点后补验。 |

### 监控口径

每档应存在的监控指标（供 #4 / #6 / #7 / #11 落地）：

| 档位 | 指标名 | 含义 | 期望行为 |
| --- | --- | --- | --- |
| **T0** | `aetheris_t0_external_call_total{agent_id, outcome}` | 外部 HTTP 调用计数（成功/失败/超时） | 正常低失败率；超时或 5xx 增高时告警 |
| **T0** | `aetheris_t0_external_call_duration_seconds{agent_id}` | 外部调用耗时分布 | P95 在 agent SLA 内 |
| **T1** | `aetheris_t1_report_success_rate{sdk_name, sdk_version}` | SDK 上报成功率 | 正常 > 99.9%；下降时 #7 降级介入 |
| **T1** | `aetheris_t1_report_dropped_total{sdk_name, reason}` | 丢弃事件计数（缓冲溢出/超时/429） | 正常为 0；非 0 时 #7 背压生效 |
| **T1** | `aetheris_t1_clock_skew_seconds{sdk_name}` | SDK 时钟与服务端偏移分布 | 用于检测 `occurred_at` vs `recorded_at` 分离是否生效（#3 F1） |
| **T2** | `aetheris_t2_ledger_acquire_total{tool_name, decision}` | Ledger 裁决计数（allow/return/wait/reject） | 正常 `allow` 占比高；`wait` 增高说明并发冲突 |
| **T2** | `aetheris_t2_replay_skip_total{tool_name}` | Replay 时跳过执行的工具计数 | 正常 = 已 committed 的工具数；异常说明事件流断链 |
| **T2** | `aetheris_t2_checkpoint_duration_seconds` | Checkpoint 写入耗时 | P95 在可接受范围；增高影响吞吐 |

## 反向场景验证

三个预指定弱保证场景，ADR 矩阵必须能直接回答：

### 场景 1: T0 下外部 agent 内部重试

> Aetheris 通过 `external_http` 调用外部 agent，外部 agent 内部对某个副作用操作重试了 3 次。Aetheris 知道吗？能阻止吗？

**矩阵定位**: 副作用语义维度的 T0 格。**答案: 不知道，不能阻止。** T0 的副作用语义明写「无保证」，外部 agent 内部调用的 at-most-once 依赖外部 agent 自律。`Idempotency-Key` header 转发，但去重是外部 agent 的责任。`external_agent_tool.go:270` 只记录最终 HTTP 响应，不观测内部重试。

### 场景 2: T1 下进程被 `kill -9`

> SDK 上报模式下，进程在 step 3/5 被 `kill -9`。重启后会发生什么？

**矩阵定位**: 断点恢复维度的 T1 格。**答案: 本地 checkpoint 恢复，但上报到 runtime 的事件可能不完整。** SDK `Runner` 在每个 step 成功后写本地 checkpoint（`runner.py` save_checkpoint），重启后从最后 checkpoint 恢复，不重跑已完成 step。但 SDK→runtime 上报通道（#6/#11）尚未建设，已上报的事件可能停在 step 3 之前（取决于上报时机）。**NOT PROVEN**: 经 #3 契约上报后的端到端恢复完整性——需 #6 实现 + `kill -9` 注入测试（#6 验收逻辑明确要求此项）。

### 场景 3: T2 下 ledger acquire 后崩溃

> T2 模式下，`InvocationLedger.Acquire` 返回 `AllowExecute`，工具开始执行前进程崩溃。重启后会发生什么？

**矩阵定位**: 副作用语义维度的 T2 格 + 断点恢复维度的 T2 格。**答案: 不会双重执行，但 job 进入不可自动恢复状态。** 重启后，`Acquire` 发现 DB 中存在 `started-not-committed` 记录（`ledger_store.go:99-103`，`!rec.Committed` 检查），返回 `WaitOtherWorker`。后续所有 `Acquire` 调用均返回 `WaitOtherWorker`，job 卡住等待清理。

**关键细节**：
- **不会双重执行**：`WaitOtherWorker` 阻止了重新执行，at-most-once 成立。
- **但工具结果丢失**：由于未 `Commit`，已执行的工具结果未持久化，job 无法继续。
- **不可自动恢复**：当前代码无超时清理机制，job 永久卡在 `WaitOtherWorker`，需人工或超时清理介入。
- **NOT PROVEN**：`hasOrphanedAcquired`（`ledger_store.go:104-107`）依赖事件流中的 `ledger_acquired` 事件，但如 P0 所述，生产路径不写此事件——因此该检测机制当前是死代码。实际生效的是 DB 层面的 `!rec.Committed` 检查（`ledger_store.go:99-103`）。

## 后续动作

| 动作 | Owner | 完成条件 | 对应 Issue |
| --- | --- | --- | --- |
| #3 契约定稿后复查矩阵「保证」项与契约字段对照 | #3 实现者 | 矩阵每个「保证」在 #3 契约中有对应字段或事件类型承载 | #3（INV-2） |
| T1 上报通道建设完成后补验 NOT PROVEN 项 | #6/#11 实现者 | 所有 T1 NOT PROVEN 项有实测证据 | #6, #11 |
| #5 脱敏管线实现后补验写入侧覆盖 | #5 实现者 | T0/T1/T2 三档脱敏范围格有实测结果 | #5 |
| #9 入站消费实现后补验因果串联 | #9 实现者 | 三档因果串联格有端到端证据 | #9 |
| #17 试点时验证接入成本实测 | #17 执行者 | T0/T1/T2 接入成本格有实测数据 | #17 |
| D7 裁决: T1 上报事件是否参与 replay 重建 | 人 | 明确裁决并更新本 ADR 的 T1 replay 保真度格 | Epic #1 D7 |

## 不变量

- **INV-1**: 任何一格不得同时出现「保证」与「依赖外部 agent 自律」两种表述。本矩阵副作用语义 T0 格明写「无保证」，因果串联三档均明写「不提供」，不违反此不变量。
- **INV-2**: 矩阵的每个「保证」必须在 #3 的事件契约中有对应字段或事件类型承载。当前矩阵中标为保证的项（如 T2 的 at-most-once 由 `ledger_acquired`/`ledger_committed` 承载）已有对应事件类型。T1 的保证项（step 级事件、checkpoint）在 #3 契约定稿后需回头复查一次——当前标 `NOT PROVEN`。

## 待裁决事项

| 编号 | 问题 | 阻塞 | 当前处理 |
| --- | --- | --- | --- |
| D7 | T1 上报事件是否参与 replay 重建。若不参与，属产品能力降级，须人明确接受并记入本 ADR；不得由实现者在遇到困难时自行宣布 | #3 | 本 ADR 的 T1 replay 保真度格暂标 `NOT PROVEN`，待 D7 裁决后更新为「保证（参与）」或「非保证（不参与，记为降级）」 |
