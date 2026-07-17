---
title: Request ID 作为 TraceID 的统一关联方案
authors:
  - "@zhaomingshan"
reviewers:
  - "@TBD"
creation-date: 2026-07-09
last-updated: 2026-07-09
status: provisional
see-also:
  - "/docs/proposals/20260702-sandbox-otel-distributed-tracing.md"
---

# Request ID 作为 TraceID 的统一关联方案

## 目录

- [概述](#概述)
- [动机](#动机)
  - [目标](#目标)
  - [非目标 / 未来工作](#非目标--未来工作)
- [方案设计](#方案设计)
  - [核心变更](#核心变更)
  - [Root Span 创建位置变更](#root-span-创建位置变更)
  - [Request ID 作为 TraceID](#request-id-作为-traceid)
  - [OTel 非阻塞异步上报](#otel-非阻塞异步上报)
  - [Controller Span 噪音优化](#controller-span-噪音优化)
  - [Trace 与日志统一关联](#trace-与日志统一关联)
  - [不变的部分](#不变的部分)
- [风险与缓解措施](#风险与缓解措施)
- [备选方案](#备选方案)
- [实现历史](#实现历史)

## 概述

本提案对 [20260702-sandbox-otel-distributed-tracing.md](./20260702-sandbox-otel-distributed-tracing.md) 中的
trace context 生成方案进行四项变更：

1. **Root Span 创建位置上移**：从 `api.go` 的各操作函数（ClaimSandbox、DeleteSandbox 等）移到
   HTTP 中间件层（`framework.go`），统一管理 trace 生命周期。
2. **Request ID 作为 TraceID**：用 HTTP 请求已有的 request ID（UUID）直接作为 OTel TraceID，
   实现 trace 与日志的统一关联。通过自定义 `IDGenerator` 实现，无需手动构造 span context。
3. **OTel 非阻塞异步上报**：确认 `BatchSpanProcessor` 异步批量上报机制，tracing 开启不影响业务性能。
4. **Controller Span 噪音优化**：将 `StartReconcileSpan` 后移至 `shouldRequeue` 检查之后，
   避免为 μs 级查询型 Reconcile 创建无意义 span。

## 动机

### 当前问题

1. **Trace 与日志割裂**：0702 提案中 TraceID 由 OTel SDK 自动生成，与日志中的 `requestID` 字段
   无直接对应关系。排查问题时需要先从日志找到 requestID，再从 Jaeger 搜索对应的 trace，无法
   一个 ID 搞定。
2. **Root Span 位置分散**：`WithRootSpanContext(ctx)` 分散在 `api.go` 的 5 个操作函数中
   （Claim、Clone、Pause、Resume、Delete），每新增一个操作都要手动加一行，容易遗漏。
3. **中间件执行时间未覆盖**：Root Span 在操作函数内创建，不包含中间件（如 CheckApiKey）
   的执行时间。

### 目标

1. **一个 ID 关联 trace 和日志**：request ID 同时是 TraceID，直接在 Jaeger 搜索 request ID
   即可找到完整 trace，日志中也带相同 request ID
2. **Root Span 统一创建**：在中间件层自动创建，操作函数无需手动调用 `WithRootSpanContext`
3. **完整请求生命周期覆盖**：Root Span 覆盖中间件 + handler 的完整执行时间

### 非目标 / 未来工作

- 不改变 controller 侧的 trace context 提取逻辑（仍然通过 annotation 传播 W3C traceparent）
- 不改变 OTel Collector / Jaeger 的部署架构
- 不引入日志中的 TraceID 字段注入（通过 request ID 关联已足够）

## 方案设计

### 核心变更

| 维度 | 0702 方案 | 0709 方案 |
|------|----------|----------|
| TraceID 生成 | OTel SDK 自动生成 | Request ID（UUID 去横线）作为 TraceID |
| Root Span 位置 | `api.go` 各操作函数 | `framework.go` HTTP 中间件 |
| `WithRootSpanContext` | 每个操作函数手动调用 | 中间件层统一调用一次 |
| Trace-日志关联 | 无直接关联 | TraceID = Request ID，直接搜索 |

### Root Span 创建位置变更

**变更前**（0702 方案）：每个操作函数（`api.go`）手动调用 `WithRootSpanContext(ctx)`，
Root Span 在操作函数内创建，不包含中间件执行时间，且每新增操作都需手动添加。

**变更后**（0709 方案）：在 `framework.go` 中间件层统一创建 Root Span，覆盖中间件 + handler
的完整请求生命周期。操作函数（`api.go`）移除所有 `WithRootSpanContext` 调用，仅保留
业务子 span 的创建。

### Request ID 作为 TraceID

UUID（v4）去掉横线后为 32 个十六进制字符 = 16 字节，与 OTel TraceID 格式完全匹配。

**实现方式**：实现 OTel SDK 的 `IDGenerator` 接口（`pkg/tracing/idgenerator.go`），
在创建 Root Span 时从 context 中读取 request ID 作为 TraceID。中间件层通过
`WithRequestID(ctx, requestID)` 将 request ID 存入 context，SDK 自动调用
`IDGenerator.NewIDs(ctx)` 生成 TraceID。

**为什么不用手动构造 span context**：手动构造 span context 会导致 Jaeger 报
"missing parent span"（构造的 span context 被 root span 当作 parent，但该 parent
不存在）。自定义 `IDGenerator` 让 SDK 内部生成 TraceID，root span 无 parent，
不会产生此问题。

**Fallback**：当 request ID 不是标准 UUID 格式时，`IDGenerator` 回退到随机生成 TraceID，
span 仍带 `request.id` 属性可搜索。

### OTel 非阻塞异步上报

Tracing 功能的开启不会阻塞或拖慢业务逻辑：

1. **异步批量上报**：`provider.go` 使用 `BatchSpanProcessor`，`span.End()` 仅将 span 数据
   放入内存队列，立即返回。后台 goroutine 定时批量发送到 OTLP gRPC exporter。
2. **开销极低**：`tracer.Start()` / `span.End()` 仅涉及内存操作，无 I/O 阻塞。
3. **自动降级**：OTel Collector 不可用时，span 数据在队列堆积后丢弃，不影响业务功能。
4. **内存可控**：实测 manager Pod 内存 ~28Mi，远低于阈值。

### Controller Span 噪音优化

#### 问题

Controller 的 Reconcile 中有很多 `Ensure*` 子 span，其中大量是纯查询操作（如检查 Pod 状态、
计算新状态），执行时间仅几十 μs。这些 span 在 Jaeger 中显示为噪音。

#### 方案

将 `StartReconcileSpan` 后移至 `calculateStatus` + `shouldRequeue` 检查之后。
`shouldRequeue=true` 的 Reconcile（纯状态更新，无 Pod 操作）不创建 span。
Terminating 分支（删除操作）单独创建 span，因为 `EnsureSandboxTerminated` 子 span
需要 Reconcile span 作为 parent。

#### 效果

- 去除 μs 级查询型 Reconcile span
- 保留有实际 Pod 操作的 Reconcile span（Pending → Running、Running → Paused 等）

### Trace 与日志统一关联

排查问题时：
1. 从日志中获取 `requestID`
2. 在 Jaeger 中直接搜索该 request ID（即 TraceID）
3. 即可看到完整的 manager → controller 调用链

### 不变的部分

- **Annotation 传播机制**：仍然使用 `agents.kruise.io/trace-context` 注入 W3C traceparent
- **Controller 侧提取逻辑**：`ExtractTraceContext` 和 `StartReconcileSpan` 不变
- **OTel SDK 配置**：TracerProvider、BatchSpanProcessor、OTLP gRPC exporter 不变

## 风险与缓解措施

| 风险 | 缓解措施 |
|------|----------|
| 非标准 request ID 导致 TraceID 不匹配 | Fallback 到 OTel 自动生成，span 仍带 `request.id` 属性可搜索 |
| UUID v4 去横线后字节序与 OTel TraceID 不一致 | 两者均为 128-bit 随机数，字节序无关紧要，仅用于唯一标识 |
| 中间件层硬编码 `sandbox-manager` tracer name | 可接受：framework.go 已依赖 sandbox-manager/logs 包 |

## 备选方案

1. **保留 OTel 自动 TraceID + request ID 作为 span 属性**：不改变 TraceID 生成逻辑，
   仅在 span 中添加 `request.id` 属性。缺点：TraceID 和 request ID 是两个不同的值，
   排查时需要映射，不够直接。

2. **完全不用 traceparent，仅用 request ID 属性关联**：manager 和 controller 是独立 trace，
   通过 tag 搜索关联。缺点：Jaeger 中无法看到完整的调用链层级结构。

本提案选择了 **request ID 直接作为 TraceID**，兼顾了 trace 串联（同一 TraceID）和
日志关联（TraceID = request ID），改动最小且效果最优。

## 实现历史

- [x] 07/09/2026: 提出方案，修改 framework.go 和 api.go，编译验证通过
- [x] 07/09/2026: 实现自定义 `IDGenerator`，解决 "missing parent span" 问题
- [x] 07/09/2026: 构建 manager 镜像，部署到测试环境验证
- [x] 07/09/2026: Jaeger 中验证 TraceID = request ID（create 操作 5 span，无 Incomplete）
- [x] 07/09/2026: 确认 OTel BatchSpanProcessor 异步非阻塞，manager 内存 ~28Mi
- [x] 07/10/2026: Controller span 噪音优化（`StartReconcileSpan` 后移至 shouldRequeue 之后）
- [x] 07/10/2026: 移除 api.go 中 PauseSandbox/ResumeSandbox 残留的 `WithRootSpanContext`
