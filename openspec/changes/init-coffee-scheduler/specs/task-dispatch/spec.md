## ADDED Requirements

### Requirement: 按队列隔离的优先级原子批量拉取
外部消费端 SHALL 通过 HTTP API 按 `queue` 批量拉取就绪任务，优先下发高优先级任务，且同一任务在同一租约周期内绝不会被多个消费端重复拉取。

#### Scenario: 多消费端并发拉取与优先级下发
- **WHEN** 同一 `queue` 中同时存在高优先级（如 `priority=9`）与低优先级（如 `priority=1`）任务，多个外部消费端同时调用 `POST /api/v1/tasks/pull`（指定 `batch_size` 与 `worker_id`）
- **THEN** 服务端执行 Redis `pull.lua` 脚本（`ZPOPMIN`），优先弹出高优先级任务（同优先级按入队时间 FIFO），且各消费端拿到的 `task_id` 列表互不重叠
- **AND** 为每个拉取出的任务写入 `cs:lease:{queue}`，设置 `score = now_ms + lease_ttl * 1000` 并更新状态为 `RUNNING`

### Requirement: 长任务心跳续租
消费端 SHALL 能够通过心跳接口延长正在执行任务的租约有效期，防止长耗时任务被误判超时。

#### Scenario: 正常续租
- **WHEN** 消费端在租约过期前调用 `POST /api/v1/tasks/{task_id}/heartbeat`
- **THEN** `heartbeat.lua` 校验任务仍处于 `RUNNING` 状态后，刷新 `cs:lease:{queue}` 中的过期时间戳

### Requirement: 租约超时回收与指数退避重试
当消费端崩溃或超时未上报回执时，系统 SHALL 自动回收任务并按指数退避策略重投，直至达到最大重试次数后进入死信队列（DLQ）。

#### Scenario: 租约过期触发自动重投
- **WHEN** `cs:lease:{queue}` 中某任务的过期时间戳 `< now_ms` 且 `retry_count < max_retries`
- **THEN** Scheduler 的 `nack_or_reap.lua` 将其移出租约队列，使 `retry_count + 1`
- **AND** 按指数退避公式计算下一次执行时间并重新投入 `cs:delay:{shard}`

#### Scenario: 超过最大重试次数进入死信队列
- **WHEN** 任务超时或被 `nack` 且 `retry_count >= max_retries`
- **THEN** 系统将其移入 `cs:dlq:{queue}`，标记状态为 `DEAD` 并异步写入 MySQL 供人工排查
