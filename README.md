# batch-job-scheduler-go

本地批量作业调度引擎：用一份内存资源账本（ledger）模拟集群算力，让作业的
提交、排队、放置、抢占、重试与重启恢复在所有入口下得到一致且可解释的结论。

代码结构：

- `scheduler/types.go` — 资源、作业、事件、重试策略等核心类型
- `scheduler/scheduler.go` — 调度器：校验、排队、放置、抢占、重试、恢复
- `scheduler/store.go` — JSON 快照持久化（原子写）
- `scheduler/scheduler_test.go` — 场景化单测

## 调度与放置规则

- 作业提交时声明 `TenantID`、`Priority`（越大越高）与资源需求 `Request{CPU, MemMB}`。
- 每轮 `Schedule()`：
  1. 退避到期的作业重新入队；
  2. 队列按 **(有效优先级降序, 提交时间升序, 作业 ID 升序)** 排序，完全确定，
     相同输入必然得到相同放置结果；
  3. 按节点 ID 升序做 first-fit 放置，账本逐节点扣减，绝不超卖。
- **有效优先级 = 基础优先级 + 老化加成**：每等待 `AgingInterval` +1，上限
  `MaxAgingBoost`，保证低优先级作业不会长期饿死。
- 每个决策都追加一条 `Event`（SUBMIT/REJECT/PLACE/PREEMPT/REQUEUE/RETRY/
  EXHAUSTED/COMPLETE/RECOVER），可通过 `Events()` 读取，用于解释调度结论。

## 配额语义

- 配额按租户配置（`Config.Quotas`），统计口径为**已准入资源**
  （排队 + 运行 + 退避中的作业），作业成功或彻底失败后释放。
- 提交即校验，拒绝原因用 `RejectError.Reason` 区分：
  `MISSING_FIELD`（缺字段）、`NEGATIVE_RESOURCE` / `ZERO_RESOURCE`（资源非法）、
  `INVALID_QUOTA`（租户无配额或配额配置非法）、`QUOTA_EXCEEDED`（超租户配额）、
  `OVERSIZED_FOR_FLEET`（超过任何单节点容量）、`DUPLICATE_JOB`（重复作业 ID）。
- 调度结果因此天然不超出租户配额与集群可用容量。

## 抢占与公平性

- 高有效优先级作业无法放置时，才允许抢占**有效优先级严格更低**的运行中作业；
  平级或更高者一律不动。
- 被抢占作业回到队列，**进度（Progress）保留**，资源立即归还账本。
- 被抢占者在 `PreemptCooldown` 内免疫再次抢占，配合老化加成，保证：
  反复调度不会来回抢占（无 thrash），低优先级不会饿死，容量恢复后
  被抢占作业自动回到运行态（稳定收敛）。

## 失败重试策略

- `Fail()` 立即释放节点资源（无泄漏、无双重占用），随后进入指数退避：
  `InitialBackoff * 2^(n-1)`，封顶 `MaxBackoff`。
- 重试次数上限 `MaxRetries`；耗尽后作业进入终态 `FAILED` 并释放配额，
  重试序列必然收敛。

## 持久化与重启恢复

- 配置 `SnapshotPath` 后，每次状态变更都会原子写 JSON 快照（tmp + rename）。
- 重启时用同一快照路径重建调度器：运行中作业恢复其节点占用（账本精确重建，
  不重复、不丢单），排队/退避作业继续参与后续调度。
- 所有入口共用一把互斥锁，并发提交与并发调度不会超卖资源。

## 验证方法

```sh
go test ./... -v        # 场景化日志：配额拒绝 / 抢占 / 重试 / 并发 / 重启恢复
go test -race ./...     # 并发安全（需 CGO_ENABLED=1）
```

测试与业务场景对应关系：

| 测试 | 覆盖业务 |
| --- | --- |
| `TestSubmitValidationAndQuota` | 缺字段/负资源/零资源/非法配额/超配额/重复 ID 的区分化拒绝 |
| `TestDeterministicPlacement` | 相同输入多次调度放置结果一致 |
| `TestPreemption` | 高优先级抢占、进度保留、容量恢复后回到稳定状态 |
| `TestNoPreemptThrashAndNoStarvation` | 冷却期防止来回抢占、老化防止饿死 |
| `TestFailureRetryConverges` | 失败释放资源、退避收敛、重试耗尽后无泄漏 |
| `TestConcurrentSubmitNoOversell` | 并发提交+调度下账本不超卖且与运行态一致 |
| `TestRestartRecovery` | 快照恢复：运行/排队/退避状态与资源占用不丢不重 |
