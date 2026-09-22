# Ingest Contract v1 — 上报事件 Wire 契约

> **Status**: accepted (D4 裁决：自定义 HTTP JSON，非 OTLP)
> **Date**: 2026-09-21
> **关联**: Issue #3, Epic #1; 承接 ADR-0003 T1 保证矩阵
> **裁决记录**: D4（自定义 HTTP JSON vs OTLP）→ 自定义 HTTP JSON；D7（T1 事件是否参与 replay）→ 不参与

## 1. 决策

### D4: 自定义 HTTP JSON（非 OTLP）

**选择**: 自定义 HTTP JSON 契约，不使用 OTLP 扩展。

**理由**:
- Runtime jobstore 事件模型已是自定义 JSON（`JobEvent.Payload []byte`，`event.go:174-185`），OTLP 的 protobuf/gRPC 栈与现有 JSON 事件流不匹配
- HTTP API 栈是 Hertz JSON REST（`internal/api/http/`），SDK 定位"零框架依赖"（`sdk/durability/README.md` 明确宣称），引入 OTLP 会破坏这一定位
- SDK 侧（Python `sdk/durability-py`、Go `sdk/durability`）当前依赖为零，OTLP 需引入 protobuf 编译器和 gRPC 客户端，与"≤ 10 行接入"口径冲突
- 跨语言一致性（#11 验收标准 3）要求"同一 schema、同一映射结果"，自定义 JSON + JSON Schema 校验可直接满足，无需两套 protobuf 编译管线
- 集团 OTel 互通可通过 #7 的降级管线在需要时桥接，不作为 T1 契约本身的要求

**代价**: 放弃与集团现有 OTel collector 的直接管线复用；如未来需要 OTLP，可在 v2 契约中追加 OTLP 编码选项（向前兼容，见 §5）。

### D7: T1 上报事件不参与 replay 重建

**选择**: T1 上报事件不参与 runtime 的 replay 重建。

**理由**:
- Runtime replay（`replay.go:116-348`）依赖 T2 级事件：`node_finished`（已完成节点）、`command_committed`（已提交命令）、`tool_invocation_finished`（已提交工具调用）。这些事件由 runtime 的 `ToolNodeAdapter`（`node_adapter.go:452-485`）在 DAG 执行中写入，携带 DAG 节点 ID、命令 ID、幂等键等 runtime 内部概念
- SDK 的 `step_started`/`step_finished` 粒度是函数调用，不携带 DAG 节点 ID，无法对齐 replay 重建所需的 `CompletedNodeIDs`/`CompletedToolInvocations` 映射
- T1 事件经映射后插入事件流，不得破坏 `command_committed → node_finished → step_committed` 的写入顺序约束（`event.go:36` 注释）；T1 事件不写入这些屏障类型，因此不会破坏顺序

**能力降级声明**: T1 接入的 agent，其 job 的事件流可用于**读取**（trace、查询、导出），但**不能**用于 runtime 的 replay 重建——即不能在 runtime 侧从事件流恢复执行状态并续跑。这是 T1→T2 升级的核心动机（见 ADR-0003 T1 replay 保真度格的 NOT PROVEN 项）。此降级记入 ADR-0003。

## 2. 事件信封 Schema

### 2.1 信封结构

每条上报事件是一个 JSON 对象，封装在批量请求中。

```json
{
  "schema_version": "1",
  "event_uid": "evt-550e8400-e29b-41d4-a716-446655440000",
  "job_id": "job-abc123",
  "step_id": "step-fetch-data",
  "span_id": "span-001",
  "parent_span_id": "span-000",
  "root_span_id": "span-000",
  "agent_id": "my-agent",
  "tenant_id": "tenant-acme",
  "idempotency_key": "exec-key-001",
  "occurred_at": "2026-09-21T12:34:56.789Z",
  "type": "step_started",
  "payload": { ... },
  "sdk_name": "aetheris-durability-py",
  "sdk_version": "0.2.0"
}
```

### 2.2 字段定义

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `schema_version` | string | 是 | 契约版本，本版固定为 `"1"` |
| `event_uid` | string | 是 | 事件唯一 ID，客户端生成（UUID 推荐）；用于 `(tenant, job_id, event_uid)` 去重 |
| `job_id` | string | 是 | Job 标识。格式：非空字符串，`^[a-zA-Z0-9_-]{1,128}$` |
| `step_id` | string | 否 | Step 标识（step 级事件必填） |
| `span_id` | string | 否 | 当前 span 标识，用于 trace 树构建 |
| `parent_span_id` | string | 否 | 父 span 标识；跨 agent 调用时从入站 header 获取（见 §6） |
| `root_span_id` | string | 否 | 根 span 标识；整条调用链恒定 |
| `agent_id` | string | 是 | 上报 agent 的标识 |
| `tenant_id` | string | 是 | 租户标识，必须与鉴权 token 中的租户一致 |
| `idempotency_key` | string | 否 | 幂等键，仅用于上报去重（见 §7 外部副作用声明） |
| `occurred_at` | string | 是 | 事件发生时间（agent 时钟），RFC 3339 纳秒精度 |
| `type` | string | 是 | 事件类型，见 §3 映射表 |
| `payload` | object | 否 | 事件负载，类型由 `type` 决定 |
| `sdk_name` | string | 是 | SDK 名称（`aetheris-durability-py` / `aetheris-durability-go`） |
| `sdk_version` | string | 是 | SDK 版本（semver） |

### 2.3 时间字段分离（F1 修复）

契约分离两个时间概念：

| 字段 | 来源 | 用途 |
| --- | --- | --- |
| `occurred_at` | agent 本地时钟 | 仅作数据，记录事件在 agent 侧发生的时间 |
| `recorded_at` | 服务端时钟（ingest 端点写入） | 进入哈希链与调度排序，`JobEvent.CreatedAt` 的来源 |

**理由**（Epic #1 F1）: `Hash = SHA256(JobID|Type|Payload|Timestamp|PrevHash)`（`pgstore.go:388-400`），`Claim` 选 job 用 `ORDER BY e.created_at`（`pgstore.go:199`）。若 agent 自报时间成为 `CreatedAt`，时钟偏移会让签名证据时间失真且哈希链仍校验通过（取证时无法察觉），并让未来时间戳的 job 永不被 Claim 选中（饿死）。因此 agent 时钟只作 `occurred_at`（数据），服务端时钟进 `recorded_at`（哈希链与调度）。

## 3. 词汇映射表

### 3.1 SDK EventType → Runtime EventType 映射

SDK 侧（Python `sdk/durability-py/aetheris_durability/types.py` + Go `sdk/durability/core/types.go`）共定义 12+1=13 个 EventType。映射到 runtime `internal/runtime/jobstore/event.go` 的 57 个 EventType。

| SDK EventType | 映射策略 | Runtime EventType | 说明 |
| --- | --- | --- | --- |
| `job_created` | **直接映射** | `job_created` (`event.go:26`) | 名称与语义一致 |
| `job_started` | **需追加新 EventType** | `job_started`（新增） | Runtime 有 `job_running`(`:44`) 和 `job_queued`(`:42`)，但无 `job_started`。SDK 的 `job_started` 表示 agent 开始执行，语义不同于 `job_running`（runtime 内部 lease 后状态迁移）。需追加 `job_started` 常量，仅用于 T1 上报 |
| `job_completed` | **直接映射** | `job_completed` (`:37`) | 名称与语义一致 |
| `job_failed` | **直接映射** | `job_failed` (`:38`) | 名称与语义一致 |
| `job_cancelled` | **直接映射** | `job_cancelled` (`:39`) | 名称与语义一致 |
| `step_started` | **直接映射** | `step_started` (`:55`) | Runtime 已有此常量，名称语义一致 |
| `step_finished` | **直接映射** | `step_finished` (`:56`) | Runtime 已有此常量，名称语义一致 |
| `step_failed` | **直接映射** | `step_failed` (`:57`) | Runtime 已有此常量，名称语义一致 |
| `step_retried` | **直接映射** | `step_retried` (`:58`) | Runtime 已有此常量，名称语义一致 |
| `step_skipped` | **需追加新 EventType** | `step_skipped`（新增） | Go SDK 有 `EventStepSkipped`(`core/types.go`)，runtime 无此常量。Python SDK 无此类型。追加 `step_skipped` 常量 |
| `checkpoint_saved` | **直接映射** | `checkpoint_saved` (`:59`) | Runtime 已有此常量，名称语义一致 |
| `checkpoint_loaded` | **不上报** | — | `checkpoint_loaded` 是 SDK 本地恢复事件（从本地存储加载 checkpoint），语义上是 agent 内部行为，上报到 runtime 无意义——runtime 不需要知道 SDK 从哪加载了 checkpoint，只需知道 checkpoint 已保存（`checkpoint_saved`）|
| `effect_recorded` | **需追加新 EventType** | `effect_recorded`（新增） | SDK 有此类型（Python `EFFECT_RECORDED`、Go `EventEffectRecorded`），runtime 有 `command_committed`(`:31`) 和 `tool_invocation_finished`(`:35`) 覆盖类似语义，但 SDK 的 `effect_recorded` 粒度是函数副作用记录，不等同于 runtime 的 DAG 级 command。追加 `effect_recorded` 常量，仅用于 T1 |

### 3.2 映射总结

| 映射结果 | 数量 | 事件 |
| --- | --- | --- |
| 直接映射 | 9 | `job_created`, `job_completed`, `job_failed`, `job_cancelled`, `step_started`, `step_finished`, `step_failed`, `step_retried`, `checkpoint_saved` |
| 需追加新 EventType | 3 | `job_started`, `step_skipped`, `effect_recorded` |
| 不上报 | 1 | `checkpoint_loaded`（SDK 本地行为，上报无意义） |

### 3.3 追加 EventType 的约束

新增 `EventType` 常量**只能追加**，不得修改或删除现有常量值（Issue #3 实现边界）。新增常量在 `event.go` 的 `const` 块末尾追加，带注释标明"T1 上报专用"。

## 4. 版本协商

### 4.1 协商流程

客户端在批量上报请求中声明 `schema_version`，服务端在响应中返回接受的版本与降级指令。

```
客户端                          服务端
  | --- POST /api/telemetry/v1/events --->
  |      (schema_version: "1")
  |                                      |
  |                                      | 校验版本
  |                                      |
  | <--- 200 OK -------------------------
  |      { "accepted_version": "1",
  |        "deduped": 5, "rejected": 0 }
  |               或
  | <--- 200 OK (部分接受) --------------
  |      { "accepted_version": "1",
  |        "deduped": 3, "rejected": 2,
  |        "rejections": [...] }
  |               或
  | <--- 426 Upgrade Required ----------
  |      { "error": "unsupported_version",
  |        "min_supported": "1",
  |        "max_supported": "1" }
```

### 4.2 四种版本组合

| 组合 | 行为 |
| --- | --- |
| 新 SDK × 新 runtime | SDK 声明 `schema_version: "1"`，runtime 接受，返回 `accepted_version: "1"` |
| 新 SDK × 旧 runtime | SDK 声明 `schema_version: "1"`，旧 runtime 不识别此字段，**忽略未知字段而非整条拒绝**（向前兼容，见 §5）。SDK 正常上报 |
| 旧 SDK × 新 runtime | 旧 SDK 不发送 `schema_version`，新 runtime 收到缺失版本字段，按 `schema_version: "0"`（legacy）处理。降级到旧契约行为（不追加新 EventType，不校验 `event_uid` 去重） |
| 旧 SDK × 旧 runtime | 双方都不涉及版本字段，行为与当前完全一致 |

### 4.3 版本协商失败时的降级路径

版本协商失败**不得静默丢事件**。两种失败模式：

| 失败模式 | 客户端动作 |
| --- | --- |
| `426 Upgrade Required`（服务端不支持客户端版本） | SDK 进入本地缓冲（#7 降级管线），等待客户端升级后重发；缓冲溢出时丢弃并计数 |
| 版本字段缺失或非法 | 服务端按 legacy 处理并返回 `accepted_version: "0"`，客户端记日志但继续上报 |

## 5. 向前兼容规则

旧 runtime 收到带未知字段的新版事件时，**必须忽略未知字段而非整条拒绝**。

- JSON 反序列化时，未声明的字段保留在 `map[string]any` 中但不参与逻辑
- 新增字段必须 `omitempty`（Go）或 optional（Python），确保旧客户端不发送时不受影响
- 新增 `EventType` 常量不破坏现有事件流读取——旧 runtime 遇到未知 `type` 时，记录事件但标记 `type: "unknown_t1_event"`，不拒绝整批

## 6. 跨 agent 调用链传播规则

### 6.1 Header 消费（#9 实现后生效）

入站消费 `X-Aetheris-Job-ID` / `X-Aetheris-Agent-ID` / `traceparent` header（#9 实现后）：

| Header | 映射到信封字段 | 说明 |
| --- | --- | --- |
| `X-Aetheris-Job-ID` | `parent_span_id` 的来源 | 调用方的 job_id 成为被调用方的 parent 上下文 |
| `X-Aetheris-Agent-ID` | `agent_id` 的校验源 | 被调用方 agent_id 应与 header 一致 |
| `traceparent` (W3C) | `parent_span_id` | 标准 W3C trace context |

### 6.2 `job_id` 与 `idempotency_key` 传播规则

- `job_id`: 被调用方创建自己的 `job_id`（不继承调用方的 job_id），但 `parent_span_id` 指向调用方的 span
- `idempotency_key`: 仅用于上报去重（§7），**不等于**业务副作用的幂等键。跨 agent 调用时，调用方的 `idempotency_key` 通过 `Idempotency-Key` header 传递，但被调用方应生成自己的 `idempotency_key`（基于自身 `event_uid`），不复用调用方的 key

## 7. 外部副作用声明

`idempotency_key` **仅用于上报通道的去重**（防止网络重传导致事件重复写入），**不等于**业务副作用的幂等键。

- 上报通道的去重：服务端基于 `(tenant_id, job_id, event_uid)` 三元组去重，重复提交不增长事件数
- 业务副作用的幂等：由 T2 的 `InvocationLedger`（`ledger_store.go:87-122`）保证，不在本契约范围内

接入方**不得**将上报 `idempotency_key` 用作业务幂等保证——T0/T1 不提供业务副作用 at-most-once（见 ADR-0003 副作用语义维度）。

## 8. 负向场景与拒绝码

> **注意**：以下拒绝码分为 schema 层和 runtime 层两级。schema 层（JSON Schema）只能校验格式和结构，无法校验语义（如"未来时间"或"payload 超长"）。标 **[runtime]** 的场景由 ingest endpoint（#4）在运行时执行，schema 不覆盖。

| 场景 | 拒绝码 | 客户端动作 | 校验层 |
| --- | --- | --- | --- |
| 缺 `schema_version` | 200 + `accepted_version: "0"`（降级） | 记日志，继续上报 | [runtime] |
| 未知 `type` | 200 + `rejections[].reason: "unknown_type"` | 检查 SDK 版本是否需升级 | schema + runtime |
| `occurred_at` 为未来时间（> 服务端时钟 + 5min） | 200 + `rejections[].reason: "future_timestamp"` | 校正时钟后重发该条 | **[runtime]** — schema 仅校验 `format: date-time`（合法 RFC 3339 即通过），未来时间在 schema 层不可区分 |
| `payload` 超 256KB | 413 Payload Too Large | 拆分或截断 payload | **[runtime]** — schema 无 `maxLength`，需 ingest endpoint 检查 Content-Length |
| `job_id` 格式非法（不匹配 `^[a-zA-Z0-9_-]{1,128}$`） | 200 + `rejections[].reason: "invalid_job_id"` | 修正 job_id 格式 | schema (`pattern`) |
| `tenant_id` 与鉴权 token 不一致 | 403 Forbidden | 检查鉴权配置 | [runtime] |

### 8.1 Schema 校验时序

ingest endpoint 必须按以下顺序处理：

1. **先做版本检测**：检查 `schema_version` 字段。缺失 → 按 legacy (v0) 处理，跳过 v1 schema 校验。不匹配 → 返回 426。
2. **再做 schema 校验**：仅对 `schema_version: "1"` 的事件用 v1 schema 校验。
3. **最后做 runtime 语义校验**：未来时间、payload 超长、job 终态等需运行时判断的场景。

这一时序确保旧 SDK 事件不会被 v1 schema 误拒。
| Job 已终态（completed/failed/cancelled） | 200 + `rejections[].reason: "job_terminated"` | 停止向该 job 上报 |

## 9. JSON Schema

JSON Schema 文件位于 `pkg/telemetry/contract/event-v1.schema.json`，供两语言校验。

## 10. 样例

12 个样例（正向 6 + 负向 6）位于 `pkg/telemetry/contract/examples/`，可用 `jsonschema` 工具校验。
