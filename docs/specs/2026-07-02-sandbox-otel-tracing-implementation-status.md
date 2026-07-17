# OpenTelemetry 分布式追踪埋点实现状态

> 对照 `docs/proposals/20260702-sandbox-otel-distributed-tracing.md` 设计方案
>
> 更新日期: 2026-07-06

## 一、核心包 (`pkg/tracing/`)

| 文件 | 设计项 | 状态 |
|------|--------|------|
| `provider.go` | TracerProvider 初始化、OTLP gRPC 导出器、Resource 配置 | ✅ 已实现 |
| `provider_test.go` | provider 单元测试 | ✅ 已实现 |
| `propagator.go` | trace context 注入/提取（annotation ↔ context） | ✅ 已实现 |
| `propagator_test.go` | annotation 传播单元测试 | ✅ 已实现 |
| `middleware.go` | sandbox-manager HTTP 追踪中间件 | ✅ 已实现 |
| `middleware_test.go` | HTTP 中间件单元测试 | ✅ 已实现 |
| `reconcile.go` | sandbox-controller Reconcile Span 辅助函数 | ✅ 已实现 |
| `reconcile_test.go` | Reconcile 辅助函数单元测试 | ✅ 已实现 |
| `spans.go` | Span 名称和属性 key 常量 | ✅ 已实现 |
| `doc.go` | 包文档 | ✅ 已实现 |

## 二、CMD 入口初始化

| 设计项 | 状态 | 位置 |
|--------|------|------|
| sandbox-manager TracerProvider 初始化 | ✅ | `cmd/sandbox-manager/main.go` |
| sandbox-controller TracerProvider 初始化 | ✅ | `cmd/agent-sandbox-controller/main.go` |
| `--tracing-enabled` CLI 参数 | ✅ | 两个 main.go |
| `--tracing-endpoint` CLI 参数 | ✅ | 两个 main.go |
| `--tracing-insecure` CLI 参数 | ✅ | 两个 main.go |
| `--tracing-sampling-ratio` CLI 参数 | ✅ | 两个 main.go |
| Shutdown 函数优雅关闭 | ✅ | 两个 main.go (defer) |

## 三、HTTP 中间件

| 设计项 | 状态 | 位置 |
|--------|------|------|
| `otelhttp` 包装 mux，自动为所有 HTTP 请求创建根 Span | ✅ | `pkg/servers/e2b/core.go` — `tracing.HTTPMiddleware(sc.mux, "sandbox-manager")` |

## 四、sandbox-manager Span

### Manager 层 Span

| Span 名称 | 设计属性 | 状态 | 位置 |
|-----------|---------|------|------|
| `manager.ClaimSandbox` | `claim.lock_type`, `claim.retries`, `claim.duration` | ✅ | `pkg/sandbox-manager/api.go` |
| `manager.CloneSandbox` | | ✅ | `pkg/sandbox-manager/api.go` |
| `manager.DeleteSandbox` | `reuse.triggered` | ✅ | `pkg/sandbox-manager/api.go` |
| `manager.PauseSandbox` | | ✅ | `pkg/sandbox-manager/api.go` |
| `manager.ResumeSandbox` | | ✅ | `pkg/sandbox-manager/api.go` |
| `manager.CreateSnapshot` | `snapshot.keep_running`, `snapshot.ttl` | ✅ | `pkg/servers/e2b/snapshot.go` |
| `manager.WaitForCheckpoint` | `checkpoint.name` | ✅ | `pkg/sandbox-manager/infra/sandboxcr/clone.go` — `createCheckpoint` 中 `Wait()` 调用 |
| `proxy.syncRoute` | `route.id`, `peers.synced` | ✅ | `pkg/sandbox-manager/api.go` |

### Infra 层 Span

| Span 名称 | 设计属性 | 状态 | 位置 |
|-----------|---------|------|------|
| `infra.ClaimSandbox` | | ✅ | `pkg/sandbox-manager/infra/sandboxcr/claim.go` — `TryClaimSandbox` |
| `infra.CloneSandbox` | `clone.checkpoint_id` | ✅ | `pkg/sandbox-manager/infra/sandboxcr/clone.go` — `CloneSandbox` |
| `infra.CreateCheckpoint` | `checkpoint.duration` | ✅ | `pkg/sandbox-manager/infra/sandboxcr/sandbox.go` — `CreateCheckpoint` |
| `infra.ProcessCSIMounts` | `csi.volume_count` | ✅ | `claim.go` (claim 路径) + `clone.go` (clone 路径) |
| `infra.Pause` | | ✅ | `pkg/sandbox-manager/infra/sandboxcr/sandbox.go` — `Pause` |
| `infra.Resume` | | ✅ | `pkg/sandbox-manager/infra/sandboxcr/sandbox.go` — `Resume` |
| `infra.Kill` | `sandbox.name` | ✅ | `pkg/sandbox-manager/infra/sandboxcr/sandbox.go` — `Kill` |

## 五、Annotation 注入点

| 操作 | 设计注入位置 | 状态 | 实际位置 |
|------|------------|------|---------|
| Create (Claim) | `TryClaimSandbox` 创建 Sandbox CR 前 | ✅ | `claim.go` — `createSandbox` 中 `c.Create` 前 |
| Create (Clone) | `CloneSandbox` 创建 Sandbox CR 前 | ✅ | `claim.go` — `createSandbox` (clone 复用 claim 路径) |
| Pause | `retryUpdate` modifier 中 | ✅ | `sandbox.go` — `retryUpdate` 统一注入 |
| Resume | `retryUpdate` modifier 中 | ✅ | `sandbox.go` — `retryUpdate` 统一注入 |
| Delete | `Kill` delete/update 前 | ✅ | `sandbox.go` — `Kill` 中 Patch 注入 |
| CreateSnapshot | `CreateCheckpoint` 创建 Checkpoint CR 前 | ✅ | `clone.go` — `createCheckpoint` 中 `c.Create` 前 |
| TriggerReuse | Patch Sandbox CR 前 | ✅ | `sandbox.go` — `TriggerReuse` 中 Patch 注入 |

## 六、sandbox-controller Span

### Reconcile Span

| 设计项 | 状态 | 位置 |
|--------|------|------|
| `controller.Reconcile` — 先判断后建 Span | ✅ | `sandbox_controller.go` — expectation 检查后、handleTerminating 前 |
| 从 annotation 提取 trace context | ✅ | `tracing.StartReconcileSpan` 内部 |
| `sandbox.phase` 属性 | ✅ | Reconcile span 上 |
| 终态/无工作 Reconcile 不创建 Span | ✅ | early return 在 span 创建前 |

### Phase 子 Span

| Span 名称 | 触发条件 | 状态 | 位置 |
|-----------|---------|------|------|
| `controller.EnsureSandboxRunning` | Phase = Pending | ✅ | `sandbox_controller.go` |
| `controller.EnsureSandboxUpdated` | Phase = Running | ✅ | `sandbox_controller.go` |
| `controller.EnsureSandboxPaused` | Phase = Paused | ✅ | `sandbox_controller.go` |
| `controller.EnsureSandboxResumed` | Phase = Resuming | ✅ | `sandbox_controller.go` |
| `controller.EnsureSandboxUpgraded` | Phase = Upgrading | ✅ | `sandbox_controller.go` |
| `controller.EnsureSandboxTerminated` | DeletionTimestamp 不为空 | ✅ | `sandbox_controller.go` |

### IO 子 Span

| Span 名称 | 设计属性 | 状态 | 位置 |
|-----------|---------|------|------|
| `controller.CreatePod` | `pod.name`, `sandbox.name`, `sandbox.namespace` | ✅ | `core/pod_control.go` — `c.Create(ctx, pod)` 前 |
| `controller.DeletePod` | `pod.name` | ✅ | `core/common_control.go` — 两处 `r.Delete(ctx, pod)` 前 |
| `controller.Checkpoint` | `checkpoint.name`, `sandbox.name`, `sandbox.namespace` | ✅ | `core/checkpoint.go` — `c.Create(ctx, cp)` 前 |
| `controller.updateSandboxStatus` | `phase.before`, `phase.after` | ✅ | `sandbox_controller.go` — `r.Status().Patch()` 前 |
| `controller.PatchPod` | `pod.name`, `patch.type` | ❌ 不适用 | 当前代码不适用（resume 已重构为重建 Pod，无直接 Pod annotation patch 操作） |

## 七、checkpoint-controller Span

| Span 名称 | 设计属性 | 状态 | 说明 |
|-----------|---------|------|------|
| `checkpoint.Reconcile` | `checkpoint.name`, `checkpoint.phase`, `sandbox.id` | ❌ 未实现 | checkpoint-controller 本身未实现（仅有 utils.go） |
| `checkpoint.Snapshot` | `checkpoint.id`, `persistent_contents` | ❌ 未实现 | 同上 |
| `checkpoint.updateStatus` | `phase.before`, `phase.after` | ❌ 未实现 | 同上 |

> `pkg/controller/checkpoint/` 目录当前只有 `utils.go`（工具函数）和 `utils_test.go`，没有 Reconcile 实现。
> Checkpoint CR 由 sandbox-controller 的 `CheckpointControl.createCheckpoint()` 创建，但处理 Checkpoint CR 的独立 controller 尚未实现。
> 三个 span 常量已在 `spans.go` 中定义，待 controller 实现后可直接使用。
>
> **替代方案**：当前使用 `manager.WaitForCheckpoint` span（位于 `clone.go` 的 `createCheckpoint` 函数中）
> 覆盖 sandbox-manager 同步等待 Checkpoint 完成的时间段。该 span 作为 `manager.CreateSnapshot` 的子 Span，
> 记录等待耗时和 `checkpoint.name` 属性。根 Span（`manager.CreateSnapshot`）减去 `infra.CreateCheckpoint`
> 即可估算 Checkpoint 实际执行耗时，瓶颈定位能力已足够。待 checkpoint-controller 实现后，可补充更细粒度的 span。

## 八、配置与部署

| 设计项 | 状态 | 位置 |
|--------|------|------|
| OTel Collector 部署清单 | ✅ | `config/otel-collector/otel-collector.yaml` |
| OTel Collector kustomization | ✅ | `config/otel-collector/kustomization.yaml` |
| sandbox-manager deployment tracing 参数 | ✅ | `config/sandbox-manager/deployment.yaml` (注释，取消注释即启用) |
| sandbox-controller manager tracing 参数 | ✅ | `config/manager/manager.yaml` (注释，取消注释即启用) |

## 九、Feature Gate

| 设计项 | 状态 | 说明 |
|--------|------|------|
| `SandboxTracingGate` feature gate | 🔄 设计变更 | proposal 原设计 feature gate，实际实现统一用 `--tracing-enabled` CLI 参数替代 |

> **设计变更原因**：
> 1. `InitTracerProvider` 在 `Enabled=false` 时设置 `NoopTracerProvider`，禁用时零开销，功能等价
> 2. CLI 参数更简洁，sandbox-manager 不需要额外导入 `pkg/features`（遵循 AGENTS.md 约束）
> 3. 两个组件使用相同的控制方式（CLI 参数），一致性更好
>
> Proposal 和 Specs 文档已同步更新为 CLI 参数方案。

## 十、未实现项汇总

| # | 项目 | 原因 | 优先级 |
|---|------|------|--------|
| 1 | `controller.PatchPod` span | 当前代码不适用（resume 已重构为重建 Pod，无直接 Pod annotation patch 操作） | 低 — 代码结构变化，span 不再适用 |
| 2 | `checkpoint.Reconcile` span | checkpoint-controller 未实现，当前由 `manager.WaitForCheckpoint` span 替代覆盖等待期间 | 中 — 待 controller 实现后埋点 |
| 3 | `checkpoint.Snapshot` span | 同上 | 中 |
| 4 | `checkpoint.updateStatus` span | 同上 | 中 |

> 注：`SandboxTracingGate` feature gate 已从“未实现”转为“设计变更”，proposal 和 specs 已同步更新，不再视为缺失项。

## 十一、实现统计

- **已实现 Span**: 29 个（sandbox-manager 15 个 + sandbox-controller 14 个）
- **已实现 Annotation 注入点**: 7 个
- **已实现 CLI 参数**: 4 个
- **未实现 Span**: 4 个（checkpoint-controller 3 个 + PatchPod 1 个不适用）
- **替代覆盖**: `manager.WaitForCheckpoint` span 覆盖 checkpoint-controller 等待期间
- **总体完成度**: ~93%（checkpoint-controller 埋点待 controller 实现后补齐，等待期间已有替代 span）
