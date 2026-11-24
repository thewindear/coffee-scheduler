# Coffee Scheduler

## Purpose
`coffee-scheduler` 是一个基于 Python 的高吞吐、拉模式（Pull-based）分布式任务调度与分发中心，面向日均数百万至上千万级（10M+/day）任务调度场景，为外部异构消费端系统（如爬虫/采集服务、异步处理服务等）提供统一的定时/延迟任务调度、原子拉取、租约保活、失败重试与回执追踪能力。

## Tech Stack
- **Language**: Python 3.11+
- **Web Framework**: FastAPI + Uvicorn / Gunicorn (`async/await`)
- **Hot-Path Store & Coordination**: Redis (`redis.asyncio` + 内嵌 Lua 脚本)
- **Persistent Store**: MySQL 8.0 (`SQLAlchemy 2.0 async` + `asyncmy` + `Alembic`)
- **Cron Parsing**: `croniter`
- **Validation & Config**: `Pydantic v2` + `pydantic-settings`
- **Observability**: `prometheus-fastapi-instrumentator` + Prometheus 业务指标

## Architecture Principles
1. **控制面与数据面解耦**：调度系统仅负责调度控制与轻量回执（状态 + 摘要 + 多态 `artifacts` 引用指针，单条回执 ≤ 16KB）；业务大体积数据由外部消费端自行写入 OSS / Kafka / Redis / 业务库。
2. **Redis 热路径 + MySQL 异步批量落库**：任务提交、延迟搬运、拉取抢占、租约回收全部在 Redis 中通过 Lua 脚本原子完成，不让 MySQL 承担高频行锁与轮询；MySQL 负责 Cron 规则存储与任务关键状态/终态的异步批量刷盘（按天分表）。
3. **模块化可分可合**：API 网关无状态水平扩展；Scheduler 调度引擎支持单进程内嵌启动（开发期），也支持独立进程多实例部署（生产期通过 Redis 分布式锁选主或分片多活）。
4. **队列隔离与至少一次语义**：按 `queue`（topic）隔离拉取，基于租约超时（Lease）+ 心跳续租 + 指数退避重试 + 死信队列（DLQ）保障任务不丢。

## Conventions
- 代码风格遵循 PEP 8 / Ruff，全量使用类型注解（Type Hints）。
- 所有 Redis 多步状态流转必须封装为 Lua 脚本，严禁客户端读-改-写竞态操作。
- 面向外部消费端的接口必须保持语言无关的 RESTful JSON 契约，并在 OpenAPI/Swagger 中完整暴露。
