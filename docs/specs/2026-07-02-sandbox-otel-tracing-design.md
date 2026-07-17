# 沙箱 OTel 追踪实现设计

## 背景

本文档为 [基于 OpenTelemetry 的沙箱生命周期分布式追踪提案](../proposals/20260702-sandbox-otel-distributed-tracing.md)
提供具体实现细节，涵盖精确的代码位置、函数签名和集成步骤。

## 目标

- 定义 `pkg/tracing/` 包结构和精确函数签名
- 指定 sandbox-manager 和 sandbox-controller 中 Span 创建的精确代码位置
- 定义 annotation 注入/提取机制的具体实现
- 指定 CLI 参数注册和初始化顺序
- 定义 feature gate 集成方式
- 提供测试计划和验收标准

## 非目标

- 不实现 OTel SDK 本身（使用上游 `go.opentelemetry.io/otel`）
- 不修改 Sandbox CRD schema（annotation 是透明的）
- 本期不埋点 agent-runtime 或 sandbox-gateway
- 不添加自定义采样策略

---

## 组件 1：`pkg/tracing/provider.go`

### 用途

初始化全局 OTel TracerProvider 和 OTLP gRPC 导出器。

### 函数签名

```go
package tracing

type Config struct {
    Enabled       bool
    Endpoint      string // OTLP gRPC 端点，如 "otel-collector:4317"
    ServiceName   string // 如 "sandbox-controller" 或 "sandbox-manager"
    SamplingRatio float64 // 0.0 到 1.0，默认 1.0
    Insecure      bool    // 使用非安全 gRPC（开发环境）
}

// InitTracerProvider 初始化全局 TracerProvider 并返回 shutdown 函数。
// 必须在启动时调用一次，在任何 controller 或 HTTP server 启动之前。
// 如果 cfg.Enabled 为 false，返回 no-op shutdown 函数。
func InitTracerProvider(ctx context.Context, cfg Config) (func(context.Context) error, error)

// Tracer 返回指定 instrumentation scope 的全局 tracer。
func Tracer(name string) trace.Tracer
```

### 实现要点

- 使用 `otlptracegrpc.NewClient` 创建 OTLP gRPC 导出器
- 使用 `sdktrace.NewTracerProvider` + `sdktrace.WithBatcher` 异步批量导出
- Resource 属性：`service.name`、`service.version`、`service.namespace`
- 采样器：`sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRatio))`
- `cfg.Enabled` 为 false 时，设置 `otel.SetTracerProvider(trace.NewNoopTracerProvider())`

---

## 组件 2：`pkg/tracing/propagator.go`

### 用途

在 `context.Context` 和 Sandbox CRD annotation 之间注入/提取 W3C Trace Context。

### 函数签名

```go
package tracing

const TraceContextAnnotationKey = "agents.kruise.io/trace-context"

// InjectTraceContext 将 ctx 中的当前 trace context 注入到 annotations。
// 如果 annotations 为 nil，初始化新 map。
// 如果 tracing 未启用或无活跃 span，返回 annotations 不变。
func InjectTraceContext(ctx context.Context, annotations map[string]string) map[string]string

// ExtractTraceContext 从 annotations 中提取 trace context，返回携带提取的
// span context 的 context。如果 annotation 不存在或无效，返回 ctx 不变。
func ExtractTraceContext(ctx context.Context, annotations map[string]string) context.Context
```

### 实现要点

- 使用 `otel.GetTextMapPropagator()` 配合自定义 `carrier` 类型
- 传播器为 `trace.TraceContext`（W3C Trace Context 格式）
- Carrier 实现 `propagation.TextMapCarrier` 接口，包装 `map[string]string`

### Carrier 实现

```go
type annotationCarrier struct {
    annotations map[string]string
}

func (c *annotationCarrier) Get(key string) string { return c.annotations[key] }
func (c *annotationCarrier) Set(key, value string) { c.annotations[key] = value }
func (c *annotationCarrier) Keys() []string { /* 返回所有 key */ }
```

---

## 组件 3：`pkg/tracing/middleware.go`

### 用途

sandbox-manager HTTP 中间件，为每个请求创建根 Span。使用 `otelhttp.NewHandler`
包装整个 mux，零侵入。

### 函数签名

```go
// HTTPMiddleware 使用 otelhttp 包装 http.Handler，为每个 HTTP 请求启动根 Span。
// Span 名称格式为 "{HTTP_METHOD} {HTTP_PATH}"（如 "POST /sandboxes"）。
// Span 属性：http.method、http.url、http.status_code 等。
// Span context 自动注入到 request context 中供下游使用。
func HTTPMiddleware(handler http.Handler, serviceName string) http.Handler
```

### 集成位置

在 HTTP server 启动前包装 mux：

```go
func (sc *Controller) Run() error {
    sc.registerRoutes()
    handler := tracing.HTTPMiddleware(sc.mux, "sandbox-manager")
    return http.ListenAndServe(":3000", handler)
}
```

### Span 命名

otelhttp 默认使用 `{HTTP_METHOD} {HTTP_PATH}` 格式（如 `POST /sandboxes`）。

| HTTP 路由 | Span 名称 |
|------------|----------|
| `POST /sandboxes` | `POST /sandboxes` |
| `DELETE /sandboxes/{id}` | `DELETE /sandboxes/{id}` |
| `POST /sandboxes/{id}/pause` | `POST /sandboxes/{id}/pause` |
| `POST /sandboxes/{id}/resume` | `POST /sandboxes/{id}/resume` |
| `POST /sandboxes/{id}/connect` | `POST /sandboxes/{id}/connect` |
| `POST /sandboxes/{id}/snapshots` | `POST /sandboxes/{id}/snapshots` |
| `GET /sandboxes/{id}` | `GET /sandboxes/{id}` |
| `GET /v2/sandboxes` | `GET /v2/sandboxes` |

### 后续优化

如需语义化 Span 命名（如 `sandbox-manager.CreateSandbox`），可在后续版本改为
`web.MiddleWare` 中间件方案，精确映射路由到命名 Span。

---

## 组件 4：`pkg/tracing/reconcile.go`

### 用途

为 controller-runtime Reconcile 迭代创建 Span。

### 函数签名

```go
// StartReconcileSpan 为 controller-runtime reconcile 迭代创建 Span。
// 从 Sandbox 对象的 annotation 中提取 trace context，建立与 sandbox-manager
// 根 Span 的父子关系。多次 Reconcile 产生兄弟 Span。
// 注意：调用方应先判断是否需要工作，确认后再调用此函数。
func StartReconcileSpan(ctx context.Context, obj client.Object, controllerName string) (context.Context, trace.Span)

// StartChildSpan 在 Reconcile 内部为特定 IO 操作创建子 Span。
func StartChildSpan(ctx context.Context, spanName string, attrs ...trace.SpanOption) (context.Context, trace.Span)
```

### 集成位置

在 `pkg/controller/sandbox/sandbox_controller.go` 的 `Reconcile` 方法中：

```go
func (r *SandboxReconciler) Reconcile(ctx context.Context, req ctrl.Request) (crl ctrl.Result, err error) {
    // ... 已有的提前返回路径（Sandbox 未找到、expectation、终态等）...

    box, err = r.addSandboxFinalizerAndHash(ctx, box)
    if err != nil { return reconcile.Result{}, err }

    // --- 追踪：创建 Reconcile Span ---
    reconcileCtx, reconcileSpan := tracing.StartReconcileSpan(ctx, box, "sandbox-controller")
    defer reconcileSpan.End()

    // calculate sandbox status
    var shouldRequeue bool
    newStatus, shouldRequeue = calculateStatus(args)
    // ...
}
```

**关键约束**：Span 在所有"无需工作"的提前返回路径**之后**创建：
- Sandbox 未找到
- Expectation 未满足
- 终态（Failed/Succeeded）
- 模板为空（termination 处理后）

Span 覆盖范围：`calculateStatus` → 阶段分发 → `updateSandboxStatus`。

---

## 组件 5：Annotation 注入点

### sandbox-manager 注入点

| 操作 | 文件 | 函数 | 注入方式 |
|------|------|------|---------|
| Create (Claim) | `infra/sandboxcr/claim.go` | `TryClaimSandbox`（创建/更新 Sandbox CR 时） | 写 CR 前注入 annotation |
| Create (Clone) | `infra/sandboxcr/clone.go` | `CloneSandbox`（创建 Sandbox CR 时） | 同上 |
| Pause | `infra/sandboxcr/sandbox.go` | `Sandbox.Pause` → `retryUpdate`（modifier 函数中） | update modifier 中注入 |
| Resume | `infra/sandboxcr/sandbox.go` | `Sandbox.Resume` → `retryUpdate`（modifier 函数中） | 同上 |
| Delete | `infra/sandboxcr/sandbox.go` | `Sandbox.Kill` → `deleteSandbox` 或 `retryUpdate` | delete/update 前注入 |

### 实现模式

每个注入点统一调用：

```go
sandbox.Annotations = tracing.InjectTraceContext(ctx, sandbox.Annotations)
```

对于 `retryUpdate` 操作（Pause/Resume）：

```go
func (s *Sandbox) Pause(ctx context.Context, opts infra.PauseOptions) error {
    err := retryUpdate(ctx, s, func(sbx *agentsv1alpha1.Sandbox) (bool, error) {
        sbx.Spec.Paused = true
        // 注入 trace context
        sbx.Annotations = tracing.InjectTraceContext(ctx, sbx.Annotations)
        return true, nil
    })
    // ...
}
```

---

## 组件 6：Controller 子 Span

### `common_control.go` 中的子 Span 位置

#### CreatePod

```go
func (r *commonControl) createPod(ctx context.Context, box *agentsv1alpha1.Sandbox, ...) (*corev1.Pod, error) {
    // ... 生成 Pod ...

    ctx, span := tracing.StartChildSpan(ctx, tracing.SpanCreatePod,
        trace.WithAttributes(attribute.String(tracing.AttrPodName, pod.Name)))
    defer span.End()

    err = r.Create(ctx, pod)
    // ...
}
```

#### DeletePod

在 `EnsureSandboxPaused` 和 `EnsureSandboxTerminated` 中：

```go
ctx, span := tracing.StartChildSpan(ctx, tracing.SpanDeletePod,
    trace.WithAttributes(attribute.String(tracing.AttrPodName, pod.Name)))
defer span.End()
err = r.Delete(ctx, pod, &client.DeleteOptions{...})
```

#### updateSandboxStatus

在 `sandbox_controller.go` 中：

```go
func (r *SandboxReconciler) updateSandboxStatus(ctx context.Context, ...) error {
    if reflect.DeepEqual(box.Status, newStatus) { return nil }

    ctx, span := tracing.StartChildSpan(ctx, tracing.SpanUpdateStatus,
        trace.WithAttributes(
            attribute.String(tracing.AttrPhaseBefore, string(box.Status.Phase)),
            attribute.String(tracing.AttrPhaseAfter, string(newStatus.Phase)),
        ))
    defer span.End()
    // ... status patch ...
}
```

### 子 Span 粒度规则

| 操作 | 创建子 Span？ | 原因 |
|------|--------------|------|
| `r.Create(ctx, pod)` | 是 | 重 IO（100-500ms） |
| `r.Delete(ctx, pod)` | 是 | Pod 优雅终止（100ms-6s） |
| `r.Status().Patch(...)` | 是 | 写操作（10-50ms） |
| `r.Get(ctx, key, pod)` | 否 | 轻量读（5-10ms） |
| 阶段分发逻辑 | 否 | 无 IO（<1ms） |
| `r.Patch(ctx, pod, ...)` | 否 | 当前代码不适用（resume 已重构为重建 Pod） |
| `c.Create(ctx, cp)` (Checkpoint CR) | 是 | 重异步操作（1-5s） |

---

## 组件 7：初始化序列

### sandbox-controller (`cmd/agent-sandbox-controller/main.go`)

```go
func main() {
    // ... 已有 flag 解析 ...

    // 在 ctrl.NewManager 之前：
    tracingShutdown, err := tracing.InitTracerProvider(ctx, tracing.Config{
        Enabled:       tracingEnabled,
        Endpoint:      tracingEndpoint,
        ServiceName:   "sandbox-controller",
        SamplingRatio: tracingSamplingRatio,
        Insecure:      tracingInsecure,
    })
    if err != nil { setupLog.Error(err, "..."); os.Exit(1) }
    defer func() { _ = tracingShutdown(context.Background()) }()

    // ... ctrl.NewManager, controller setup, mgr.Start ...
}
```

新增 CLI flag：

```go
flag.BoolVar(&tracingEnabled, "tracing-enabled", false, "启用 OpenTelemetry 分布式追踪")
flag.StringVar(&tracingEndpoint, "tracing-endpoint", "otel-collector:4317", "OTLP gRPC 导出端点")
flag.Float64Var(&tracingSamplingRatio, "tracing-sampling-ratio", 1.0, "Trace 采样率")
flag.BoolVar(&tracingInsecure, "tracing-insecure", true, "使用非安全 gRPC 导出")
```

### sandbox-manager (`cmd/sandbox-manager/main.go`)

sandbox-manager 使用 CLI 参数，不使用 feature gate（遵循 AGENTS.md 约束）：

```go
func main() {
    var tracingEnabled bool
    var tracingEndpoint string
    var tracingSamplingRatio float64
    var tracingInsecure bool

    pflag.BoolVar(&tracingEnabled, "tracing-enabled", false, "启用 OpenTelemetry 分布式追踪")
    pflag.StringVar(&tracingEndpoint, "tracing-endpoint", "otel-collector:4317", "OTLP gRPC 导出端点")
    pflag.Float64Var(&tracingSamplingRatio, "tracing-sampling-ratio", 1.0, "Trace 采样率")
    pflag.BoolVar(&tracingInsecure, "tracing-insecure", true, "使用非安全 gRPC 导出")

    // ... pflag.Parse() ...

    tracingShutdown, err := tracing.InitTracerProvider(ctx, tracing.Config{
        Enabled:       tracingEnabled,
        Endpoint:      tracingEndpoint,
        ServiceName:   "sandbox-manager",
        SamplingRatio: tracingSamplingRatio,
        Insecure:      tracingInsecure,
    })
    if err != nil { klog.Fatalf("...") }
    defer func() { _ = tracingShutdown(context.Background()) }()

    // ... sandboxController.Init() ...
}
```

### 追踪中间件注册

在 HTTP server 启动前条件性包装 mux：

```go
func (sc *Controller) Run() error {
    sc.registerRoutes()
    handler := sc.mux
    if /* tracing 已启用 */ {
        handler = tracing.HTTPMiddleware(handler, "sandbox-manager")
    }
    return http.ListenAndServe(":3000", handler)
}
```

---

## 组件 8：CLI 参数控制

两个组件统一使用 `--tracing-enabled` CLI 参数控制 tracing 的启用/禁用，不使用 feature gate。

原因：
1. `InitTracerProvider` 在 `Enabled=false` 时设置 `NoopTracerProvider`，禁用时零开销
2. CLI 参数更简洁，不需要额外导入 `pkg/features`（sandbox-manager 遵循 AGENTS.md 约束）
3. 两个组件使用相同的控制方式，一致性更好

CLI 参数列表（两个组件相同）：

```go
--tracing-enabled          // bool, 默认 false
--tracing-endpoint         // string, 默认 "otel-collector:4317"
--tracing-sampling-ratio   // float64, 默认 1.0
--tracing-insecure         // bool, 默认 true
```

---

## 组件 9：go.mod 依赖

从 indirect 提升为 direct：

```plaintext
go.opentelemetry.io/otel
go.opentelemetry.io/otel/sdk
go.opentelemetry.io/otel/trace
go.opentelemetry.io/otel/exporters/otlp/otlptrace
go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
```

添加后执行：

```bash
go mod tidy
go mod vendor
```

---

## 组件 10：跨操作上下文传播

### Checkpoint CR Annotation 传播

sandbox-controller 在 pause/upgrade 过程中创建 Checkpoint CR 时，传播当前 trace context：

```go
checkpoint := &agentsv1alpha1.Checkpoint{
    ObjectMeta: metav1.ObjectMeta{
        Name:      checkpointName,
        Namespace: box.Namespace,
        Annotations: map[string]string{},
    },
    // ...
}
// 将当前 Reconcile span 的 trace context 传播到 Checkpoint CR
checkpoint.Annotations = tracing.InjectTraceContext(ctx, checkpoint.Annotations)
```

### 多操作交叉规则

1. 每次操作写入自己的 `traceparent` 到 Sandbox annotation（覆盖前一次）
2. controller 始终使用**当前** annotation 的 traceparent
3. 如果 checkpoint 是当前操作的子步骤（如升级中），checkpoint CR 携带升级的 traceparent
4. 如果 checkpoint 是独立操作（如用户主动创建快照），Sandbox annotation 有快照操作的 traceparent

---

## 测试计划

### 单元测试

| 测试文件 | 覆盖目标 |
|---------|---------|
| `provider_test.go` | InitTracerProvider 启用/禁用配置；shutdown flush |
| `propagator_test.go` | InjectTraceContext 有/无活跃 span；ExtractTraceContext 有效/缺失/无效 annotation |
| `middleware_test.go` | 根 Span 创建；Span 属性（method、path、sandbox ID）；响应时 Span 结束 |
| `reconcile_test.go` | StartReconcileSpan 有/无 annotation；兄弟 Span 验证；StartChildSpan 属性 |

### 测试策略

- 使用 `trace.NewNoopTracerProvider()` 进行不需要验证 span 导出的测试
- 使用内存导出器（`tracetest.NewInMemoryExporter()`）验证 span 属性和层级
- 表驱动测试，使用描述性 `name` 字段（遵循 AGENTS.md）
- 使用 `expectError string` 模式（遵循 AGENTS.md 测试约定）

### 验收标准

1. **Feature gate 禁用**：无 span 导出，无 annotation 注入，零开销
2. **Feature gate 启用，无 Collector**：span 批量后超时丢弃，无功能影响
3. **创建沙箱 trace**：根 Span（sandbox-manager）+ 兄弟 Span（controller Reconcile）+ 子 Span
   （CreatePod），共享同一 TraceID
4. **Pause/Resume/Delete trace**：每次操作产生独立 trace，正确的父子关系
5. **kubectl 创建的 sandbox**：controller 启动新根 Span，可通过 sandbox UID 搜索
6. **多轮 Reconcile**：只有有实际工作的 Reconcile 创建 Span，终态 Reconcile 不创建

---

## 实施阶段

### 阶段 1：核心包 ✅ 已完成

1. ✅ 创建 `pkg/tracing/` 包（`provider.go`、`propagator.go`、`middleware.go`、`reconcile.go`、`spans.go`、`doc.go`）
2. ✅ 添加 go.mod 依赖并 `go mod vendor`
3. ✅ 编写全部单元测试（11 个测试全部通过）

### 阶段 2：sandbox-manager 集成 ✅ 已完成

1. ✅ 添加 `middleware.go`（HTTPMiddleware）
2. ✅ 在 `pkg/servers/e2b/core.go` 添加 tracing 中间件
3. ✅ 在 `pkg/sandbox-manager/api.go` 添加 Manager 层 span（Claim/Clone/Delete/Pause/Resume/CreateSnapshot/syncRoute）
4. ✅ 在 `pkg/servers/e2b/snapshot.go` 添加 CreateSnapshot span
5. ✅ 在 `pkg/sandbox-manager/infra/sandboxcr/` 添加 Infra 层 span + annotation 注入（claim.go、clone.go、sandbox.go）
6. ✅ 添加 `manager.WaitForCheckpoint` span（clone.go，替代未实现的 checkpoint-controller）
7. ✅ 在 `cmd/sandbox-manager/main.go` 添加 CLI 参数

### 阶段 3：sandbox-controller 集成 ✅ 已完成

1. ✅ 添加 `reconcile.go`（`StartReconcileSpan`、`StartChildSpan`）
2. ✅ 在 `pkg/controller/sandbox/sandbox_controller.go` 添加 Reconcile Span（先判断后建 Span）+ Phase 子 span
3. ✅ 在 `pkg/controller/sandbox/core/pod_control.go` 添加 CreatePod span
4. ✅ 在 `pkg/controller/sandbox/core/common_control.go` 添加 DeletePod span
5. ✅ 在 `pkg/controller/sandbox/core/checkpoint.go` 添加 Checkpoint span
6. ✅ 在 `cmd/agent-sandbox-controller/main.go` 添加 CLI 参数

### 阶段 4：部署和文档 ✅ 已完成

1. ✅ 添加部署清单参数（deployment.yaml、manager.yaml）
2. ✅ 添加 OTel Collector 部署示例（config/otel-collector/）
3. ⏳ E2E 验证（Jaeger/Tempo 后端）— 待进行
4. ✅ 更新文档（proposal、specs、实现状态文档）

### 待实现

- checkpoint-controller 埋点（`checkpoint.Reconcile`/`Snapshot`/`updateStatus`）— 待 controller 实现后启用
- E2E 验证（Jaeger/Tempo 后端）
