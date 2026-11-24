## ADDED Requirements

### Requirement: 标准化任务回执与大小限制
外部消费端 SHALL 在任务执行完成后调用 `ack` 或 `nack` 上报回执，系统仅接收轻量级控制面回执与存储引用。

#### Scenario: 上报成功回执（含多态 artifacts）
- **WHEN** 消费端调用 `POST /api/v1/tasks/{task_id}/ack` 上报包含 `worker_id`、`duration_ms`、`summary` 及多态 `artifacts`（如 `oss`/`kafka`/`redis` 的 `storage` 与 `uri`）的回执且请求体 ≤ 16KB
- **THEN** `ack.lua` 将任务从 `cs:lease:{queue}` 移出，状态置为 `SUCCEEDED` 并为 `cs:task:{task_id}` 设置过期 TTL
- **AND** 将终态与完整回执 JSON 投递至异步刷盘队列等待落库 MySQL

#### Scenario: 回执体积超过 16KB 限制
- **WHEN** 消费端上报的 `ack` 或 `nack` 请求体大小超过 16KB
- **THEN** 网关直接拒绝请求并返回 `413 Payload Too Large`
- **AND** 提示消费端将大体积业务数据写入外部存储后仅回传 `artifacts` 引用指针

### Requirement: 异步批量落库与原生按天分区
系统 SHALL 通过异步批量刷盘机制将任务状态与回执写入 MySQL 原生按天 `RANGE` 分区表 `task_records`，避免单条 SQL 阻塞调度吞吐，同时保证单表语义以支持后续 UI 分页与多维查询。

#### Scenario: 批量刷盘写入 MySQL 分区表
- **WHEN** 内存异步队列中的待落库记录达到批次阈值（如 500 条）或达到刷新间隔（如 100ms）
- **THEN** `Async Flusher` 协程使用批量 `INSERT ... ON DUPLICATE KEY UPDATE` 将数据一次性写入 `task_records`（由 MySQL 按 `created_at` 自动路由至对应日分区）
- **AND** 后台分区维护逻辑提前预建未来 7 天分区，防止跨零点写入失败

#### Scenario: 基于 UUIDv7 的单任务详情冷热联合查询
- **WHEN** 调用 `GET /api/v1/tasks/{task_id}` 查询任务详情与回执
- **THEN** 服务端优先查询 Redis `cs:task:{task_id}` 获取实时状态；若 Redis 未命中，则从 `UUIDv7` 结构的 `task_id` 中解析出创建时间戳，自动附加 `created_at` 范围条件触发 MySQL 分区裁剪，仅扫描单日分区返回结果

### Requirement: 任务列表游标分页与队列实时统计
系统 SHALL 提供支持分区裁剪的任务列表查询接口、队列实时水位统计接口及死信任务重跑接口，满足监控排查与后续 UI 面板接入需求。

#### Scenario: 带时间范围的游标分页查询
- **WHEN** 调用 `GET /api/v1/tasks` 并传入 `queue`、`status`、`start_time`、`end_time` 与游标 `cursor`
- **THEN** 服务端校验时间跨度不超过上限（如 7 天，默认最近 24 小时），基于 `(created_at, task_id)` 游标执行高效分页并返回下一页游标

#### Scenario: 查询队列实时堆积与在线节点
- **WHEN** 调用 `GET /api/v1/queues/{queue}/stats`
- **THEN** 服务端从 Redis 实时读取该队列的 `ready_count`、`running_count`、`dlq_count` 及活跃 `worker_count` 并毫秒级返回
