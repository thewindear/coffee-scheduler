# Change: 初始化 Coffee Scheduler 核心调度系统

## Why
当前需要一个从 0 到 1 构建的 Python 任务调度与分发中心，以支撑日均数百万至上千万级任务（以动态延迟/即时任务为主，辅以 Cron 周期任务）的调度需求。消费端为外部异构系统（如数据采集服务），需要通过通用 HTTP 接口主动拉取任务、维持心跳并上报标准化执行回执（包含异构存储的位置指针）。

## What Changes
- **新增任务调度引擎 (`task-scheduling`)**：支持即时/延迟任务提交、批量提交、任务优先级（Priority）、幂等键防重，以及基于 `croniter` 的 Cron 周期任务扫描与触发。
- **新增拉模式分发与容错 (`task-dispatch`)**：提供按 `queue` 隔离的 HTTP 优先级批量拉取接口（Redis Lua 原子抢占）、租约保活心跳接口、租约过期自动回收、指数退避重试与死信队列（DLQ）。
- **新增标准化回执与异步落库 (`task-receipt`)**：支持 `ack`/`nack` 回执上报，采用多态 `artifacts` 结构描述异构业务存储位置（OSS/Kafka/Redis/MySQL 等），并通过 `Async Flusher` 协程异步批量刷盘至 MySQL 按天分表。
- **新增可观测性与模块化部署**：暴露 `/metrics` Prometheus 指标与 OpenAPI 文档，支持 API 与 Scheduler 同进程启动或拆分为独立进程部署。

## Impact
- **新增代码目录**：`app/`、`tests/`、`pyproject.toml`、`alembic/`
- **外部依赖**：Redis（热路径调度与锁）、MySQL 8.0（规则与异步流水持久化）
- **API 版本**：建立 `/api/v1/` 路由规范，无历史破坏性变更。
