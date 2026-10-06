# batch-job-scheduler-go

本地批量作业调度引擎：用**资源账本**模拟集群算力，对作业的提交、排队、放置、抢占、重试与重启恢复给出一致且可解释的结论。核心代码在 `scheduler/`。

## 核心概念

- **集群容量**：`Scheduler` 持有全局 `Resources{CPU, MemMB}` 上限。
- **租户配额**：每个 tenant 有独立配额，作业运行期间同时占用集群容量与租户配额，两者任一超限即不可放置。
- **资源账本**：只有 `RUNNING` 状态的作业计入占用；完成、失败、被抢占立即释放，杜绝双重占用与泄漏。

## 提交校验与配额语义

`Submit` 按顺序校验并返回可区分的拒绝原因（`errors.Is` 可判定）：

| 拒绝原因 | 触发条件 |
|---|---|
| `ErrMissingField` | 缺 `ID` 或 `Tenant` |
| `ErrNegativeResource` | 资源请求为负，或超过集群总容量 |
| `ErrZeroResource` | 资源请求为 0 |
| `ErrUnknownTenant` | 租户无配额，或请求超过租户配额 |

相同 `ID` 重复提交幂等成功，不会重复入队。非法的集群容量/租户配额（非正）在 `New` 时即被拒绝。

## 调度与抢占规则

每轮 `Schedule()`：

1. **排序**：队列按（有效优先级降序，提交序号升序）排列，保证相同输入多次调度结果一致（确定性）。
2. **有效优先级 = 基础优先级 + 等待轮数 × AgingPerRound**：aging 机制保证低优先级作业最终能被调度，不会饿死。
3. **放置**：依次尝试放置，不得超出集群容量与租户配额。
4. **抢占**：放置失败时，高优先级作业可抢占**严格更低优先级**且**不在冷却期**的运行中作业；按“最低优先级优先、最小必要集合”选择牺牲者，抢占后仍放不下则放弃（不动账本）。
5. **被抢占作业**保留 `Progress` 回到队列，`Preempted+1`，并进入 `PreemptCooldownRounds` 轮冷却——冷却期内不可再被抢占，防止来回抖动。

## 失败重试策略

- `Fail(id)`：释放资源，进入 `BACKOFF`，退避时长 = `BackoffBase << (retries-1)`，封顶 `BackoffMax`。
- 退避到期后自动回到队列重试；`Retries` 达到 `MaxRetries` 后进入终态 `FAILED`。
- 重试不丢进度（`Progress` 保留），资源在失败瞬间即释放，无泄漏。

## 持久化与重启恢复

`New(capacity, quotas, storePath)` 传入非空路径即启用快照：每次状态变更原子写入（临时文件 + rename）。重启后用同一路径重建调度器，作业状态、进度、重试次数、资源账本全部恢复——不重复、不丢单、不超卖。

## 并发安全

所有公开方法由互斥锁保护；并发提交、并发调度不会超卖资源。

## 验证方法

```bash
go test ./scheduler/ -v          # 运行全部单测，日志可见各业务用例明细
CGO_ENABLED=1 go test -race ./scheduler/   # 并发竞态检测
```

测试覆盖（`scheduler/scheduler_test.go`）：

| 测试 | 覆盖业务 |
|---|---|
| `TestQuotaRejection` | 缺字段/负资源/零资源/未知租户/超配额/非法配额配置的拒绝原因 |
| `TestDeterministicPlacement` | 相同输入多次调度结果一致 |
| `TestPreemption` | 高优先级抢占、进度保留、冷却防抖、容量恢复后稳定 |
| `TestFairnessNoStarvation` | aging 防饿死 |
| `TestRetryBackoff` | 失败退避重试收敛、资源不泄漏、重试耗尽 |
| `TestConcurrentSubmit` | 并发提交不超卖、重复 ID 幂等 |
| `TestRestartRecovery` | 重启后状态/账本恢复，不重复不丢单 |
