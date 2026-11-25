# Tasks: 初始化 Coffee Scheduler

## 1. 工程脚手架与基础设施
- [x] 1.1 初始化 `pyproject.toml` 与项目目录结构（`app/`, `tests/`, `alembic/`）
- [x] 1.2 实现 `app/core/config.py`（基于 `pydantic-settings` 管理 Redis、MySQL、分片数、刷盘批次等配置）
- [x] 1.3 实现 `app/core/redis_client.py`（异步连接池 + Lua 脚本预加载与 SHA 缓存）
- [x] 1.4 实现 `app/core/db.py` 与 ORM 模型（`cron_jobs`、`task_records` 按 `created_at` 建立 MySQL 原生日分区，见 `specs/task-receipt/spec.md`）

## 2. Redis Lua 原子引擎
- [x] 2.1 编写并测试 `pump.lua`（到期 Delay ZSET -> Ready 优先级 ZSET 批量搬运）
- [x] 2.2 编写并测试 `pull.lua`（按 `queue` 使用 `ZPOPMIN` 优先级批量原子抢占并写入 Lease ZSET）
- [x] 2.3 编写并测试 `heartbeat.lua`（刷新 Lease ZSET 过期时间）
- [x] 2.4 编写并测试 `ack.lua`、`nack.lua` 与 `reap_lease.lua`（终态释放、指数退避重投、DLQ 流转）

## 3. 核心服务与后台引擎
- [x] 3.1 实现 `app/services/dispatcher.py`（任务提交、批量提交、幂等校验、拉取、心跳、回执与 16KB 大小限制）
- [x] 3.2 实现 `app/services/scheduler_loop.py`（Delay Pump、Lease Reaper、Cron Scanner 及 Redis 选主锁）
- [x] 3.3 实现 `app/services/flusher.py`（`asyncio.Queue` 内存缓冲 + 定时/定量批量 `UPSERT` 至 MySQL 按天分表）

## 4. FastAPI 网关与可观测性
- [x] 4.1 实现 `app/api/v1/tasks.py`（提交、拉取、心跳、ACK、NACK、单任务冷热查询、分区游标列表查询、队列实时统计、死信手动重跑接口）
- [x] 4.2 实现 `app/api/v1/cron_jobs.py`（Cron 规则 CRUD、查询列表与启停接口）
- [x] 4.3 集成 Prometheus `/metrics` 指标（队列长度、拉取延迟、ACK/NACK 计数、DLQ 数量、刷盘耗时）
- [x] 4.4 配置 `app/main.py` 与 `app/worker_scheduler.py` 双启动入口

## 5. 验证与测试
- [x] 5.1 编写单元测试与集成测试（覆盖并发拉取不重不漏、租约超时重投、DLQ、幂等防重、回执超限拦截）
- [x] 5.2 编写基础压测脚本（验证高并发提交与拉取吞吐及异步刷盘稳定性）
