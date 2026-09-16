---
title: SandboxUpdateOps 并发 — 按沙箱粒度的印章式 Last-Writer-Wins
authors:
  - "@mahe"
reviewers:
  - "@zhaomingshan"
  - "@furykerry"
creation-date: 2026-08-12
last-updated: 2026-08-25
status: provisional
see-also:
  - "/docs/proposals/20260804-suo-inplace-strategy.md"
  - "/docs/proposals/20251218-sandbox-inplace-update.md"
---

# SandboxUpdateOps 并发 — 按沙箱粒度的印章式 Last-Writer-Wins

## 目录

- [摘要](#摘要)
- [动机](#动机)
- [提案](#提案)
  - [印章协议](#印章协议)
  - [决策 1：按沙箱粒度](#决策-1按沙箱粒度)
  - [决策 2：Last-Writer-Wins 要求完整模板](#决策-2last-writer-wins-要求完整模板)
  - [决策 3：安全更新点](#决策-3安全更新点)
  - [决策 4：基于印章的终态记账](#决策-4基于印章的终态记账)
  - [决策 5：轮询重新入队实现唤醒](#决策-5轮询重新入队实现唤醒)
  - [决策 6：接受中间 rollout](#决策-6接受中间-rollout)
  - [决策 7：状态形态——聚合在 SUO，细节在 Sandbox](#决策-7状态形态聚合在-suo细节在-sandbox)
  - [决策 8：双模式共存——混合模式准入屏障](#决策-8双模式共存混合模式准入屏障)
  - [字段参考](#字段参考)
  - [三个 SUO 交错场景](#三个-suo-交错场景)
- [已知局限](#已知局限)
- [风险与缓解](#风险与缓解)
- [备选方案](#备选方案)
- [测试计划](#测试计划)
- [实现历史](#实现历史)

## 摘要

`SandboxUpdateOps`（SUO）是本项目的批量更新 API：一个 SUO 对象声明“把这批 Sandbox 带到某个目标 spec”，选择策略（`Recreate` / `CheckpointRestore` / `InplaceUpdate`），控制器在滚动窗口内推进变更。

当前每个 namespace 只允许一个活跃 SUO。当用户并发提交更新时，较晚的重叠 SUO 要么被拒绝，要么静默地以空操作完成：**用户的变更没有落地，也没有明确提示。**

本提案用一个**基于印章的 Last-Writer-Wins 协议**让 template 模式的并发更新可控：

- **template 模式（新增）：** 携带完整目标模板（`spec.template` / `spec.templateRef`）的 SUO 可以与其他 template-mode SUO 并发运行。每个**成功完成**的 round 都会把交付 SUO 的创建时间戳作为“印章”盖到 Sandbox 上；**任何 SUO 都不能覆盖更新的印章**。因此每个 Sandbox 会单调收敛到选中它的最新模板，被取代的工作会被报告，不会静默丢弃。
- **patch 模式（保留）：** 携带增量 `spec.patch` 的 SUO 维持现有 namespace 级独占串行。
- **共同安全保证：** Sandbox 不会在临界区里被触碰；一个繁忙 Sandbox 不会拖住整个批次；删除 SUO 只取消未来工作，**不回滚已落地工作**——印章会保留。
- **接受的代价：** 在最新 SUO 抵达某个 Sandbox 之前，较旧 SUO 可能先落一个过时的中间 round（决策 6）。最终状态不受影响，但该 Sandbox 会多付一次或多次 rollout 成本。

不引入新的生命周期状态：Sandbox 保持 `Running` / `Upgrading`，SUO 保持 `Pending / Updating / Completed / Failed`。

## 动机

用户可能并发提交多个 SUO：CI/CD 流水线、自动化系统、多个人同时操作，这不受控制器控制。当前行为会导致较晚提交的意图静默丢失。用户连续提交多个更新时，真实期望通常是：**最后一次提交描述最终状态**；系统需要让每个 Sandbox 收敛到这个最终状态。

### 目标

- **收敛：** 每个 Sandbox 最终到达选中它的最新存活 SUO 的模板。
- **单调性：** Sandbox 已落地的模板 revision 不会倒退。
- **无静默丢弃：** 被取代的工作在 SUO 状态中可见。
- **按 Sandbox 推进：** 一个繁忙 Sandbox 不会拖住其余 Sandbox。
- **确定性最终状态：** 结果只取决于哪些 SUO 存活、哪些 round 已落地，不取决于 reconcile 调度时序。

### 非目标/未来工作

- **不保证最少 rollout。** 过时中间 round 被接受并有边界（决策 6）；彻底跳过它们需要基于 live SUO 集合做 winner 计算，这里刻意不采用（备选方案 6）。
- **不做严格 FIFO。** 较旧 SUO 可以被更新意图取代。
- **不做等待超时。** 活性来自快速失败和 round 完成。
- **不做事件驱动唤醒。** MVP 使用轮询重新入队。
- **不加固多 worker。** 默认单 worker；更高 worker 数仍是尽力而为。
- **不做临界区中途抢占。** 永不计划；见决策 3。

## 提案

### 印章协议

**印章**是已落地工作的记录：当一个 round **成功完成**时，Sandbox 会被盖上交付 SUO 的名字和 `creationTimestamp`——它的含义是“这个 Sandbox 已被带到该 SUO 的模板”，而不是“有某个 SUO 开始处理它”。

协议由两个标记承载，职责严格分离：

- **Pending record** — 在 **round 启动**时写入——round 启动是一个 SUO 对某个 Sandbox 发起一轮更新的那次原子 patch——内容是发起 SUO 的名字和 `creationTimestamp`。它只做两件事：标记*被占用*（有 round 在跑），以及充当印章的原材料——round 成功时晋升（拷贝到印章后移除），终态失败时清除且不写印章。它**永不**参与新旧比较。
- **印章（landed）** — 只由这次晋升写入；永不清除。是更新规则的**唯一输入**。

对于正在 reconcile 某个选中 Sandbox 的 SUO `S`：

```text
round 在跑（存在 pending record）          → 等待它的终态
没有印章                                   → 更新它（启动一轮 round）
印章比 S 旧（同时间按 name 打破平局）        → 更新它（启动一轮 round）
印章比 S 新                                → 跳过；计为 superseded
```

未完成的 round 谁都不能重定向：SUO 等它到终态，然后重读印章。这一条规则就是全部的反 livelock 机制——没人争抢 in-flight round，跑完的 round 的结果（有章或无章）决定下一个是谁。

“新旧”按 `(creationTimestamp, name)` 做字典序比较，从而形成确定的全序。

由此得到三个性质：

- **单调。** 印章只向更新的意图前进；已落地工作不会回退。最终状态因此确定：每个 Sandbox 会停在选中它的最新存活 SUO 的模板，因为这个 SUO 不会跳过，且没人能覆盖它。
- **自结账。** 旧 SUO 看到更新的印章时，这个 Sandbox 立即结清为 superseded；没有 handover 等待状态。
- **持久。** 删除 SUO 时印章不清理，in-flight round 的 pending record 也不清理。**实现要求：晋升不得依赖 SUO 对象仍然存在**——被删除 SUO 留下的 in-flight round（finish-present）在成功时由 Sandbox 侧的 round 完成处理凭 pending record 完成晋升。因此已落地工作永远不会被存活的旧 SUO 回滚；删除语义是 cancel-future、finish-present，不是 rollback。

### 决策 1：按沙箱粒度

| 方面 | 细节 |
|------|------|
| **行为** | 10 个 Sandbox 被选中，其中 3 个正被 in-flight round 占用 → 7 个立即更新，3 个等下一个安全点 |
| **理由** | 一个卡住的 Sandbox 不应拖住其余 Sandbox；完整模板契约下，用户意图是收敛到最终状态，而不是批次原子性 |
| **拒绝方案** | 整个 SUO all-or-nothing 等待（备选方案 2）——一个卡住的 Sandbox 会卡住整个批次 |

### 决策 2：Last-Writer-Wins 要求完整模板

Last-Writer-Wins 只有在每个 writer 都完整描述目标状态时才安全。若使用增量 patch（例如 `SUO-1: image`、`SUO-2: cpu`），跳过或覆盖都会静默丢失意图。

| 方面 | 细节 |
|------|------|
| **契约** | 参与印章协议的每个 SUO 都必须携带完整模板快照 |
| **API 形态** | 在 `spec.patch` 旁新增 `EmbeddedSandboxTemplate`（`template` \| `templateRef`），与 `SandboxSet`/`Sandbox` 一致；每个 SUO 只能设置一种模式（webhook 校验） |
| **`spec.patch`** | 保留为 legacy 模式；patch-mode SUO 不参与印章协议，仍使用 namespace 级独占串行（决策 8） |
| **Spec 可变性** | 仅 `maxUnavailable` 与 `paused` 可变；其余字段创建后不可变。用 `creationTimestamp` 绑定用户意图的新旧，新的意图必须创建新的 SUO |
| **拒绝方案** | 约定 patch 必须“完整”（不可验证）；移除 `spec.patch`（破坏现有流程）；patch-stacking（备选方案 3） |

### 决策 3：安全更新点

只有当一个 Sandbox 上**没有 round 在跑**时，SUO 才能对它启动新 round。被占用的 Sandbox 绝不 mid-round 重定向——即使 round 还没触碰 pod 也不行，临界区（`Checkpointing` 有 commit job 进行中、`PostUpgrade` hook 在新 pod 中运行、in-flight InplaceUpdate patch——完成判定依赖 ImageID baseline，re-patch 会破坏 baseline）更是绝对不行。SUO 等待该 round 的终态：

- **S2** — round 成功（`Succeeded`）：pending record 已晋升为印章；下一轮按印章规则启动。
- **S3** — round 以可判定 pod 状态终止失败（pending record 已清除，未写印章）：
  - *resize infeasible*：pod 稳定，可立即启动下一轮。
  - *image pull failed*：pod 卡在拉镜像途中；`ImagePullBackOff` 不会因为修改 `spec.image` 而重新拉取（E2E 已验证），因此只有 **Recreate/CheckpointRestore** SUO 能启动下一轮；InplaceUpdate SUO 只能把该 Sandbox 标记为 failed 并给出处理建议，不能降级或自动升级策略。

**放弃的优化：** 对还没触碰 pod 的 round 提前重定向（early takeover）可以省一轮 rollout，但需要 mid-flight 比较规则；为协议简洁被否决——过时的 in-flight round 直接跑到自己的终点（决策 6）。Checkpointing 完成后切换、半成品 pod rebuild 同理不在范围内。

**原子写入：** 一次更新必须在同一个 Sandbox patch 里写入新模板、pending record、`LabelSandboxUpdateOps`、upgrade policy 和 lifecycle，避免观察者看到 record/template 不一致。

**窗口记账：** 当前 SUO 自己造成的失败消耗其 `maxUnavailable` 窗口（熔断器）；继承来的 policy-mismatch 失败会被计数和报告，但不消耗窗口。

### 决策 4：基于印章的终态记账

当一个 SUO 选中的每个 Sandbox 都满足以下任一条件时，它进入 `Completed`：

- Sandbox 位于该 SUO 的模板，且携带该 SUO 的印章；
- Sandbox 携带更新的印章（该 SUO 被 superseded）。

状态消息记录拆分，例如 `5 updated, 3 superseded by ops-c`。

- **即时结清。** 看到更新的印章时，该 Sandbox 立即从旧 SUO 的账上结清——印章就是交付证据（更新 SUO 的 round 已实际完成），因此没有 awaiting-handover 状态，也不依赖更新 SUO 是否还存在。
- **历史记账。** 计数描述这个 SUO 曾交付什么。更新 SUO 之后可能覆盖一个该 SUO 已 reported as `updated` 的 Sandbox；这里的 `updated` 表示“曾被带到我的模板”，不是“现在仍在我的模板”。当前状态以 Sandbox 自身为准。
- **不新增 phase。** `Pending / Updating / Completed / Failed` 不变；没有 `Superseded` phase。Supersession 是 message，不是状态机状态。

### 决策 5：轮询重新入队实现唤醒

仍然需要轮询：`SandboxEventHandler` 只会把事件投递给 Sandbox ops label 指向的 SUO，因此等待占用 Sandbox 的更新 SUO不会被该 Sandbox 的事件唤醒；删除 SUO 也不会自动唤醒别人。

| 方面 | 细节 |
|------|------|
| **机制** | 有等待 Sandbox 的 SUO 按可配置延迟重新入队（MVP 默认 30s）；每次 poll 重新读取印章和占用状态，更新已经空闲的 Sandbox |
| **覆盖路径** | round 完成、终态失败（含 fast-fail）、其他 SUO 删除 |
| **未来** | 事件驱动唤醒，暂缓 |

### 决策 6：接受中间 rollout

印章协议不做 winner 计算：较旧 SUO 在更新 SUO 触达某个 Sandbox 之前不知道它存在。在这个窗口内，较旧 SUO 可能启动一个过时 round；更新 SUO 等它跑完，再更新一次该 Sandbox。若一批 N 个 template SUO 快速重叠，某个 Sandbox 最坏可能执行每个 SUO 各一轮。

**这是有意接受的代价**，用于换取协议简洁：不需要对 live SUO 集合做 winner 计算，不需要 handover-wait 状态，整个协调面只有 Sandbox 上的两个 marker（pending + landed）。最终状态不受影响（印章单调）。边界和逃生路径：

- **熔断器限制爆炸半径。** 失败的过时 round 消耗旧 SUO 自己的 `maxUnavailable` 窗口，从而限制一个过时 SUO 能损坏多少 Sandbox。
- **坏镜像卡死与救场。** 如果过时 InplaceUpdate round 的镜像不可拉取，pod 会卡在 `ImagePullBackOff`（容器不可用）。镜像拉取 fast-fail 会在数秒内把它变成终态；按决策 3，只有 Recreate/CheckpointRestore SUO 能救。救场路径是一条操作：提交一个更新的 Recreate-type SUO，按印章规则落地，不需要删除原 SUO。
- **未来工作。** 如果未来 kubelet 语义支持对 backoff 容器修正镜像后重新拉取，则可以通过 in-place 修补关闭这个卡死路径，无需 pod replacement。

**Paused SUO：** `paused` 停止启动新 round；in-flight round 完成（刹车不是 abort）。paused SUO 不冻结选择范围，旧 SUO 仍可能更新它的 Sandbox，代价是 unpause 后可能多一次 rollout。不存在 frozen-selection 语义；恢复方式是 unpause、创建更新 SUO 取代它，或删除。

### 决策 7：状态形态——聚合在 SUO，细节在 Sandbox

SUO status 只放聚合计数；每个 Sandbox 的细节以 Sandbox 自身为准。

```yaml
status:
  phase: Updating
  updatedReplicas: 750     # 已被带到我的模板（stamp = mine）
  updatingReplicas: 50     # 正在朝我的模板进行 round
  waitingReplicas: 180     # 被其他 round 占用，等待安全点
  supersededReplicas: 20   # 已携带更新印章，对我而言已 superseded
  failedReplicas: 0
```

| 方面 | 细节 |
|------|------|
| **细节查询** | `kubectl get sandbox -l <selector>`；ops label 表示当前 follow 的 round，stamp annotation 表示最新已落地 revision，`Upgrading` condition message 表示等待原因（如 `ImagePullBackOff`） |
| **事件** | 关键转换记录为 SUO event：round started、update blocked at critical section、policy-mismatch failure |
| **理由** | Sandbox 状态不复制到 SUO status；status 大小保持 O(1) |
| **拒绝方案** | 在 status 中保存 per-sandbox 列表——O(N) 抖动、etcd 膨胀、产生第二份 Sandbox truth |

### 决策 8：双模式共存——混合模式准入屏障

| 模式 | 字段 | 语义 | 并发 |
|------|------|------|------|
| **patch**（legacy） | `spec.patch` | 对每个 Sandbox 当前模板做增量 SMP | namespace 级独占串行，维持今天行为 |
| **template** | `spec.template` / `spec.templateRef` | 完整目标模板 | 按 Sandbox 粒度的印章式 Last-Writer-Wins |

每个 SUO 必须且只能设置一种模式（webhook 校验，字段不可变）。patch 是相对 Sandbox 当前状态的增量，不是可比较的最终状态，因此 patch-mode SUO 不参与印章协议。只要两种模式可能交错，namespace 就退化为独占串行：

| namespace 中活跃（非终态）SUO | 新 template SUO | 新 patch SUO |
|---|---|---|
| 无 | admit | admit |
| 仅 template-mode | admit（印章协议） | reject |
| 任意 patch-mode | reject | reject |

**准入规则：仅当没有活跃 SUO，或新 SUO 与所有活跃 SUO 都是 template-mode 时 admit。** 执行分两层：webhook 在创建时拒绝（按 phase 判定非终态，因此 finalizer 持有删除期间也阻塞）；controller 防御性复查（延迟 requeue + `Blocked` event；删除处理先于 barrier check，避免逃生路径卡死；无 label 的 `Upgrading` Sandbox——其 SUO mid-round 被删——在 round settled 前不更新）。

### 字段参考

#### SUO `spec`

| 字段 | 目的 | 可变性 |
|------|------|--------|
| `selector` | 选择目标 Sandbox（排除 SandboxSet 控制的 Sandbox） | 不可变 |
| `patch` | legacy 增量模式；不参与印章协议 | 不可变 |
| `template` / `templateRef` | **新增** — 完整目标模板快照（`EmbeddedSandboxTemplate`） | 不可变 |
| `updateStrategy.type` | `Recreate`（默认）/ `CheckpointRestore` / `InplaceUpdate` | 不可变 |
| `updateStrategy.maxUnavailable` | 滚动窗口大小，默认 1 | 可变 |
| `lifecycle` | 复制到每个 Sandbox round 的 pre/post-upgrade hooks | 不可变 |
| `paused` | 紧急刹车：停止启动新 round；in-flight round 完成 | 可变 |
| `stateFilter` | eligible new candidates 的 Sandbox phase（默认 `[Running]`） | 不可变 |

#### SUO `status`

包括 `phase`（`Pending / Updating / Completed / Failed`，不变）、`observedGeneration`、`replicas` 以及决策 7 中的五个计数器。计数器每次 reconcile 都从 live Sandbox 集合重新计算——status 是报告，不是持久账本；持久记录是 stamp。

#### Sandbox 侧写入（每个 Sandbox、每轮）

**Round 启动**是一个 SUO 对某个 Sandbox 发起一轮更新的那次原子 merge patch，同时携带模板、policy、lifecycle、label 和 pending record；stamp 稍后在 round 成功时由晋升写入：

| Sandbox 字段 | 时机 | 内容 |
|--------------|------|------|
| `spec.template` | Round 启动（Paused 两阶段流程的 phase 2） | 目标模板；template mode 为完整覆盖，patch mode 为 strategic merge |
| `spec.upgradePolicy` | Round 启动 | 从 `updateStrategy.type` 映射；round 成功后清除 |
| `spec.lifecycle` | Round 启动 | 深拷贝 SUO 的 `spec.lifecycle`，或移除 |
| `metadata.annotations[agents.kruise.io/update-ops-pending-revision]` | Round 启动 | **新增 — pending record**：发起 SUO 的名字 + `creationTimestamp`，与模板原子写入。仅作占用信号和晋升原材料——永不参与新旧比较。round 成功时晋升（拷贝到 stamp 后移除）；终态失败时清除；**SUO mid-round 被删时保留**，晋升仍会发生 |
| `metadata.annotations[agents.kruise.io/update-ops-revision]` | Round 成功（晋升） | **新增 — stamp**：由 Sandbox 侧的 round 完成处理从 pending record 复制；**永不清理，删除 SUO 也保留**；这是最新已落地 revision 的持久记录，也是更新规则的唯一输入 |
| `metadata.labels[agents.kruise.io/update-ops]` | Round 启动 | 表示 Sandbox 当前 follow 的 SUO，仅用于事件路由与 `kubectl` 过滤，不承担协议语义；ops 删除时清理 |
| `metadata.labels[agents.kruise.io/upgrade-failed]` | Round end（失败） | 标记终态失败 Sandbox；新 round 启动时清理 |
| `metadata.annotations[agents.kruise.io/upgrade-resume-trigger]` | Paused Sandbox 两阶段升级 | phase 1 设置；phase 2 移除；ops 删除时也移除 |

SUO 不写 Sandbox `status`；Sandbox controller 拥有 status。

### 三个 SUO 交错场景

SUO-A（Sandbox 1、2）、SUO-B（2、3）、SUO-C（1、3）；按 A、B、C 顺序创建，全部为 template-mode。一个可能的单 worker 执行：

```text
A reconcile: sbx-1,2 空闲、无印章 → 启动 rounds（pending=A）
B reconcile: sbx-2 被占用（round A 在跑）→ 等；
             sbx-3 空闲、无印章 → 启动 round（pending=B）
C reconcile: sbx-1 被占用 → 等；sbx-3 被占用 → 等
sbx-1 round (A) 成功（stamp=A）→ C：印章更旧 → 启动（pending=C）
sbx-2 round (A) 成功（stamp=A）→ B：印章更旧 → 启动（pending=B）
sbx-3 round (B) 成功（stamp=B）→ C：印章更旧 → 启动（pending=C）
剩余 round 成功 → sbx-1,3 stamp=C；sbx-2 stamp=B
A: Completed（"0 remaining, 2 superseded"）；B: Completed；C: Completed
```

最终状态 `sbx-1: C, sbx-2: B, sbx-3: C` 是确定的：stamp 单调，且每个 Sandbox 的最新 selector-matching survivor 不会跳过。只有**轨迹**依赖时序：如果 B 在 A 触达 sbx-2 之前 reconcile，sbx-2 会直接到 B.template，A 的中间 round 会被 best-effort 跳过。过时中间 round 是 best-effort skip，不保证 skip（决策 6）。

#### 同场景下的 SUO 删除

删除 SUO 是 **cancel-future, finish-present**，不是 rollback。Finalizer（`agents.kruise.io/sandboxupdateops-protection`）在清理 follow 该 SUO 的 Sandbox 上的 ops label 和 resume-trigger annotation 时持有删除；**stamp 和 in-flight round 的 pending record 都不清理**。In-flight round 保留 `spec.template` + `spec.upgradePolicy` 并继续完成；round 成功时 Sandbox 侧的完成处理仍会把 pending record 晋升为 stamp——晋升不需要 SUO 对象（Sandbox controller 根据 Sandbox spec 驱动 round，不读取 SUO 对象）。

- **A 的 sbx-1/2 round 正在跑时删除 A。** round 继续在 A.template 上完成，并经晋升盖上 A 的 stamp；B/C 随后看到更旧的印章，更新这些 Sandbox。如果 A 在某个 Sandbox 上启动 round 之前就被删，则没有印章，后续 SUO 直接落地——A 的中间 rollout 被完全跳过。
- **C 已在 sbx-3 落地、但还未触碰 sbx-1 时删除 C。** sbx-3 保留 stamp=C，永远停在 C.template；任何存活的旧 SUO 看到更新 stamp 都会把它结清为 superseded，不会回滚。sbx-1 只有 A 的 stamp，因此保持 A。
- **删除全部三个 SUO。** In-flight round 完成；每个 Sandbox 停在最后一次已落地 round 的模板上，stamp 保留。

结果是哪些 SUO 存活、哪些 round 已落地的纯函数。

## 已知局限

- **中间 rollout。** 数量受 burst size 和旧 SUO 自身窗口限制；每个额外 round 都是一次有状态 Sandbox 的服务中断。
- **坏镜像直到人工救场。** 使用不可拉取镜像的过时 round 会让容器不可用，直到用户提交 Recreate-type SUO；fast-fail 让它在数秒内终态，熔断器限制单个 SUO 影响数，但从发现到救场的停机时间是运维成本。
- **历史记账。** Completed SUO 的 `updatedReplicas` 描述交付历史，不描述当前 Sandbox 状态。
- **没有 paused freeze。** paused 最新 SUO 不阻止旧 SUO 更新它的 Sandbox；unpause 后可能多一次 rollout。
- **并发写者。** 两个 SUO 可能竞争更新同一个空闲 Sandbox；通过乐观冲突重试收敛（每次冲突后重新读取印章和占用状态）。印章顺序是全序，因此重试会收敛。默认仍为 1 worker；更高 worker 数启动时给出警告。
- **等待时长受 round duration 限制。** 更新 SUO 要等任何 in-flight round 跑完。InplaceUpdate 坏镜像路径通过 fast-fail 数秒到 S3；Recreate round 卡 `ImagePullBackOff` 只能靠升级自身失败检测到 S3。第一阶段接受。

## 风险与缓解

### 风险 1：用户把不完整 template 当 patch 用

省略字段意味着删除，不是“保持不变”。**缓解：** 使用独立 API 字段表达语义；webhook 要求 template 自包含（同 `SandboxSet.spec.template` 规则）；文档明确覆盖语义。真正需要增量语义的用户继续使用 patch mode（决策 8）。

### 风险 2：快速连续 SUO（Template Thrash）

大量 SUO 快速提交时，被取代的 SUO 仍可能在最新 SUO 触达之前落地部分中间 round（决策 6）。**缓解：** 接受；stamp 保证最终状态单调，每个被取代 SUO 的状态显式说明 superseded。

### 风险 3：InplaceUpdate 卡住 round 推迟更新

更新 SUO 绝不重定向 mid-flight round。**缓解：** 镜像拉取失败通过 fast-fail 在数秒内进入 S3；resize rejection 已通过现有 gate 终态；剩余等待是健康 round，最终会自己完成。

### 风险 4：handleDeletion 缓存竞态（既有）

Informer lag 可能导致 SUO 删除时漏掉 label cleanup。这个提案不改变该问题；`ResourceVersionExpectation` 与 requeue 处理最终一致。stamp 永不清理，因此该竞态不会导致 rollback。

## 备选方案

### 备选方案 1：Skip（当前行为）

较晚 SUO 静默 no-op。**拒绝：** 意图静默丢失。

### 备选方案 2：整个 SUO All-or-Nothing 排队（第一版）

整个 SUO FIFO 排队；任意被占用 Sandbox 都会 park 整个操作。**拒绝：** 一个卡住的 Sandbox 拖住整个批次；严格顺序会让每个 Sandbox 经历过时中间 rollout；需要新排队机制。

### 备选方案 3：Patch-Stacking

在每个安全点按时间顺序 merge 所有 pending patches。**拒绝：** 最终状态没有被任何对象直接声明；跨 SUO merge conflict 难以诊断。

### 备选方案 4：Waiting / Superseded Phase

**拒绝：** supersession 是 status message，不是状态机扩展。

### 备选方案 5：事件驱动唤醒

精确、零轮询延迟。**暂缓：** polling 用一套机制覆盖 round completion 与 deletion。

### 备选方案 6：基于 candidacy 的 winner 计算（上一版）

上一版按 Sandbox 在 *live* SUO 集合上计算 winner（`target(sbx)` = 最新 active selector-matching SUO），这样旧 SUO 不会启动过时 round；同时区分两类证据：candidacy 负责 admission，actual takeover（marker written）负责结清旧 SUO 账目。

**收益：** 防止而不是接受过时中间 rollout，包括可避免的坏镜像卡死；paused winner 会冻结选择范围；`updated` counters 描述当前状态；更新 SUO 落地前也能通过 `pendingTakeoverReplicas` 看到即将被取代。

**代价：** 每个 round 启动决策都要计算 winner；需要 handover-wait 记账（旧 SUO 在更新 SUO 实际落地前保持 `Updating`）；结清证据放在可被删除清理剥掉的 label 上，需要 durable-floor 修正，最终又靠近 stamp。

**被取代原因：** 印章协议用 Sandbox 上的两个 marker（pending + landed）达到同样确定的最终状态，并且天然删除不回滚；不需要 handover 状态。被防止的过时 round 被认为不值得引入额外机制；其损害有边界且可恢复（决策 6）。

## 测试计划

### 单元测试

1. **印章规则：** 有 round 在跑（存在 pending record）的 Sandbox 绝不被重定向——SUO 等待终态；否则无印章 / 印章更旧 → 更新；印章更新 → skip 并计入 superseded；同秒创建按 name 打破平局，顺序全序。
2. **原子 round 启动：** template、policy、lifecycle、label、pending record 一次 patch 落地；round 启动时不触碰 stamp；没有可观察的部分写入。
3. **安全更新点：** 被占用 Sandbox（含 Checkpointing / PostUpgrade / in-flight InplaceUpdate round）绝不写入；空闲和 round-end Sandbox 可推进；waiting Sandbox 反映到 `waitingReplicas`。
4. **S3 规则：** resize-infeasible → 立即更新；image-pull-failed → 仅 Recreate/CheckpointRestore SUO 可更新；InplaceUpdate SUO 报 failed 并给 guidance。
5. **记账：** completion iff 每个选中 Sandbox 携带我的 stamp 且在我的模板，或携带更新的 stamp；message 记录拆分；counter 从 live set 重算。
6. **晋升与持久性：** round 成功把 pending record 晋升为 stamp；终态失败清除 pending record、不写 stamp；删除只清理 label 和 resume-trigger，不清理 stamp 和 in-flight pending record——round 成功时即使 SUO 对象已不存在也完成晋升；存活旧 SUO 看到更新 stamp 后结清为 superseded，不回滚。
7. **Webhook：** `patch` 与 `template`/`templateRef` 必须二选一；immutability 规则；双模式准入矩阵含 finalizer-pending blocking。
8. **窗口记账：** 自身失败消耗窗口；继承来的 policy-mismatch failure 计数但不消耗窗口。

### E2E 测试

1. **同目标 pair：** SUO-1 后 SUO-2，选择同一批 Sandbox；最终全部到 SUO-2 template；SUO-1 以 supersession split 完成；SUO-1 尚未触达的 Sandbox 直接到 SUO-2（观察到 best-effort skip）。
2. **三 SUO 交错：** A(1,2)、B(2,3)、C(1,3) → 最终 `1:C, 2:B, 3:C`，与执行顺序无关；三者都进入终态。
3. **坏镜像救场：** SUO-1（InplaceUpdate，坏镜像）卡住 Sandbox；round 数秒内 fast-fail 到 S3；SUO-2（Recreate，好镜像）自动落地；无需删除 SUO-1。
4. **删除持久性：** 最新 SUO 已在部分 Sandbox 落地后被删除；已落地 Sandbox 保持其模板（stamp 保留，旧 SUO 不回滚）；未触达 Sandbox 收敛到存活 SUO。
5. **混合模式串行：** patch-mode SUO 活跃时 template SUO 创建被拒；patch SUO 终态或删除后 template SUO 才推进；Sandbox 不会同时接收两种模式的写入。

## 实现历史

- 2026-08-12：原始草案 — 整个 SUO all-or-nothing queueing（`Pending + WaitingFor`，FIFO admission）。
- 2026-08-19：经设计评审重构为 sandbox-centric latest-template-wins：按 Sandbox 粒度、完整模板契约、安全切换点、不新增 phase。queueing 草案保留为备选方案 2。
- 2026-08-21：文档补充字段参考与 SUO 删除语义（cancel-future, finish-present）。
- 2026-08-21：双模式修订：保留 `spec.patch` 作为 legacy namespace-exclusive mode；新增 mixed-mode admission barrier。
- 2026-08-21：强化记账：takeover-based completion gate 搭配 candidacy-based admission。
- 2026-08-24：经评审后，将协调模型从 candidacy-based winner computation 改为 stamp-based last-writer-wins：round 启动时写入的持久 stamp 成为唯一协调状态；接受并限制过时中间 rollout（决策 6），换取协议简洁和 deletion-proof no-rollback。candidacy 方案保留为备选方案 6。
- 2026-08-25：经评审把 stamp 从 round 启动时改为 completion 时：round 启动写入 pending record——它只是占用信号和晋升原材料，永不参与比较；round 成功时晋升为 landed stamp，终态失败时清除。更新规则只比较 stamp；in-flight round 绝不被重定向——SUO 等它到终态（放弃 early takeover）。明确实现要求：晋升由 Sandbox 侧的 round 完成处理执行，不得依赖 SUO 对象仍然存在——被删除 SUO 留下的 in-flight round 成功时仍会盖章。
