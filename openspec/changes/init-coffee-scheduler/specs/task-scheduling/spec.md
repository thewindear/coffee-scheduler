## ADDED Requirements

### Requirement: 任务提交与延迟调度
系统 SHALL 支持通过 HTTP API 提交单条或批量即时任务与延迟任务（支持指定 `priority` 优先级 0~9，默认 5），并将其写入 Redis 热路径。

#### Scenario: 提交即时任务
- **WHEN** 生产端调用 `POST /api/v1/tasks` 且未指定未来执行时间（`delay_seconds=0` 且无 `run_at`）
- **THEN** 系统生成唯一 `task_id`，将任务元数据（含 `priority`）写入 `cs:task:{task_id}`，以 `(9 - priority) * 1e13 + now_ms` 为基础分值写入 `cs:ready:{queue}`，并确保新任务排在已就绪的同优先级任务之后
- **AND** 将创建事件投递至异步刷盘队列等待写入 MySQL

#### Scenario: 提交延迟任务并到期搬运
- **WHEN** 生产端提交带有 `delay_seconds > 0` 或未来 `run_at` 的任务
- **THEN** 系统将任务写入分片延迟队列 `cs:delay:{shard}`，其 `score` 为目标执行毫秒时间戳
- **AND** 当系统时间到达 `score` 时，Scheduler 的 `pump.lua` 自动将其原子搬运至 `cs:ready:{queue}`

### Requirement: 幂等防重提交
系统 SHALL 支持基于 `idempotency_key` 的提交防重机制，避免上游重试导致重复调度。

#### Scenario: 重复提交相同幂等键
- **WHEN** 生产端在有效时间窗口内两次提交带有相同 `idempotency_key` 的任务
- **THEN** 系统通过 Redis `SET NX EX` 拦截重复写入，返回首次创建时对应的 `task_id`
- **AND** 不会在 `cs:ready:{queue}` 或 `cs:delay:{shard}` 中产生重复任务实例

### Requirement: Cron 周期任务调度
系统 SHALL 支持创建、更新、启停 Cron 周期规则，并在到期时自动生成任务实例。

#### Scenario: Cron 规则到期触发
- **WHEN** `cron_jobs` 表中某条启用状态的规则 `next_run_at <= now`
- **THEN** 持有选主锁的 Scheduler 使用 `croniter` 计算下一次执行时间并通过乐观锁更新 `next_run_at`
- **AND** 自动生成一条任务实例投入对应 `cs:ready:{queue}` 供外部消费端拉取
