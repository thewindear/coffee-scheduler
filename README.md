# ☕ Coffee Scheduler

高吞吐、Pull 模式的 Python 异步任务调度器。基于 **Redis**（热路径调度）+ **MySQL**（持久化流水），适用于分布式爬虫调度、异步工作流、定时任务等场景。

## ✨ 核心特性

| 特性 | 说明 |
|------|------|
| **Pull 模式** | Worker 主动拉取任务，天然负载均衡，无需消息推送 |
| **优先级队列** | 10 级优先级（0–9），同级 FIFO |
| **延迟 & 定时** | 支持 `delay_seconds` 延迟投递和 `run_at_ms` 精确定时 |
| **Cron 定时任务** | 标准 Cron 表达式，自动生成任务实例 |
| **租约 & 心跳** | Lease 机制保护执行中任务，Worker 可续租 |
| **自动重试 & 死信** | 指数退避重试，超限进入死信队列（DLQ），支持批量重跑 |
| **幂等投递** | `idempotency_key` 防止重复投递 |
| **Redis Lua 原子操作** | pull / ack / nack / heartbeat / pump 均由 Lua 脚本保证原子性 |
| **MySQL 分区表** | 按天 `RANGE` 分区 + UUIDv7 自动裁剪，支持海量历史查询 |
| **Prometheus 指标** | 内建 `/metrics` 端点，开箱即用 |

## 🏗️ 架构概览

```
                     ┌─────────────────────────┐
                     │   业务生产端 / Cron 触发  │
                     └────────┬────────────────┘
                              │ POST /tasks
                              ▼
                     ┌────────────────────┐
                     │  FastAPI 无状态网关  │◄── Worker: pull / heartbeat / ack / nack
                     └───┬──────────┬─────┘
                         │          │
              ┌──────────▼──┐  ┌───▼──────────┐
              │    Redis    │  │    MySQL      │
              │  热路径调度  │  │  持久化流水    │
              │  Delay ZSET │  │  task_records │
              │  Ready ZSET │  │  cron_jobs    │
              │  Lease ZSET │  └──────────────┘
              │  DLQ List   │
              └──────┬──────┘
                     │
              ┌──────▼──────────────────┐
              │  Scheduler Engine       │
              │  ┌─────────────────┐    │
              │  │ Delay Pump      │    │  到期任务 → Ready
              │  │ Lease Reaper    │    │  过期租约 → 重试/DLQ
              │  │ Cron Scanner    │    │  Cron → 生成新任务
              │  │ Async Flusher   │    │  批量刷盘 → MySQL
              │  └─────────────────┘    │
              └─────────────────────────┘
```

## 🚀 快速开始

### 1. 启动依赖服务

```bash
docker compose up -d
```

这将启动 Redis 7.4 和 MySQL 8.0：

| 服务 | 端口 | 说明 |
|------|------|------|
| Redis | 6379 | 热路径调度存储 |
| MySQL | 3306 | 持久化流水存储（密码：`coffee_dev_password`） |

### 2. 安装项目

```bash
# 创建虚拟环境
python3 -m venv .venv
source .venv/bin/activate

# 安装项目及开发依赖
pip install -e ".[dev]"
```

### 3. 配置环境变量

```bash
cp .env.example .env
# 按需修改 .env 中的 MySQL 密码、Redis 地址等
```

### 4. 启动服务

```bash
alembic upgrade head
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

服务启动后访问：
- **API 文档**：http://localhost:8000/docs
- **健康检查**：http://localhost:8000/health
- **Prometheus 指标**：http://localhost:8000/metrics

## 📡 API 参考

### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/tasks` | 提交单个任务 |
| `POST` | `/api/v1/tasks/batch` | 批量提交任务（最多 500 条） |
| `POST` | `/api/v1/tasks/pull` | Worker 拉取任务 |
| `POST` | `/api/v1/tasks/{task_id}/heartbeat` | Worker 心跳续租 |
| `POST` | `/api/v1/tasks/{task_id}/ack` | 任务完成确认 |
| `POST` | `/api/v1/tasks/{task_id}/nack` | 任务失败上报 |
| `GET`  | `/api/v1/tasks/{task_id}` | 查询任务详情 |
| `GET`  | `/api/v1/tasks` | 任务列表（分页、筛选） |
| `POST` | `/api/v1/tasks/{task_id}/retry` | 手动重试单个任务 |

### 死信队列

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/dlq/{queue}/retry` | 批量重跑死信任务 |

### 队列监控

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/queues/{queue}/stats` | 队列实时水位统计 |

### Cron 定时任务

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/cron-jobs` | 创建 Cron 规则 |
| `GET`  | `/api/v1/cron-jobs` | 列出 Cron 规则 |
| `GET`  | `/api/v1/cron-jobs/{job_id}` | 查询 Cron 规则详情 |
| `PATCH` | `/api/v1/cron-jobs/{job_id}` | 更新 Cron 规则 |
| `DELETE` | `/api/v1/cron-jobs/{job_id}` | 删除 Cron 规则 |

## 📖 使用示例

### 提交任务

```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "crawl",
    "payload": {"url": "https://example.com", "depth": 2},
    "priority": 7,
    "max_retries": 5,
    "lease_seconds": 120
  }'
```

**响应**：
```json
{
  "task_id": "0193a6e0-7c0a-7000-8000-000000000001",
  "queue": "crawl",
  "status": "READY",
  "deduplicated": false
}
```

### 拉取任务（Worker 端）

```bash
curl -X POST http://localhost:8000/api/v1/tasks/pull \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "crawl",
    "worker_id": "worker-node-01",
    "batch_size": 10
  }'
```

### 任务心跳续租

```bash
curl -X POST http://localhost:8000/api/v1/tasks/{task_id}/heartbeat \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "crawl",
    "worker_id": "worker-node-01",
    "extend_seconds": 60
  }'
```

### 任务完成确认（ACK）

```bash
curl -X POST http://localhost:8000/api/v1/tasks/{task_id}/ack \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "crawl",
    "worker_id": "worker-node-01",
    "duration_ms": 1420,
    "summary": {"total": 500, "success": 498, "failed": 2},
    "artifacts": [
      {
        "storage": "oss",
        "uri": "oss://bucket/task_result.parquet",
        "meta": {"format": "parquet", "size_bytes": 204800}
      }
    ]
  }'
```

### 任务失败上报（NACK）

```bash
curl -X POST http://localhost:8000/api/v1/tasks/{task_id}/nack \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "crawl",
    "worker_id": "worker-node-01",
    "error_code": "TIMEOUT",
    "error_message": "Connection timed out after 30s",
    "retryable": true
  }'
```

**响应**：
```json
{
  "task_id": "...",
  "outcome": "RETRIED",
  "retry_count": 2,
  "next_run_at_ms": 1732456789000
}
```

### 创建 Cron 定时任务

```bash
curl -X POST http://localhost:8000/api/v1/cron-jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-report-gen",
    "queue": "reports",
    "cron_expr": "0 2 * * *",
    "payload": {"report_type": "daily_summary"},
    "priority": 8
  }'
```

### 批量重跑死信任务

```bash
# 重跑指定队列的所有死信（默认最多 100 条）
curl -X POST http://localhost:8000/api/v1/dlq/crawl/retry

# 重跑指定任务 ID
curl -X POST http://localhost:8000/api/v1/dlq/crawl/retry \
  -H "Content-Type: application/json" \
  -d '{"task_ids": ["task-id-1", "task-id-2"], "limit": 50}'
```

### 查询队列水位

```bash
curl http://localhost:8000/api/v1/queues/crawl/stats
```

**响应**：
```json
{
  "queue": "crawl",
  "ready_count": 1250,
  "running_count": 48,
  "dlq_count": 3,
  "worker_count": 12
}
```

## 并发压测

从仓库根目录运行。脚本会核对拉取与提交的任务 ID、终态、回执和队列残留。

```bash
python -m scripts.bench_local --tasks 5000 --producers 16 --workers 16
```

默认使用内存中的 FakeRedis 和 SQLite。加上 `--real` 可通过 `REDIS_URL` 和 `MYSQL_DSN` 使用真实服务。真实模式会创建独立测试队列并写入任务记录，请使用测试实例。

## ⚙️ 配置参数

所有参数通过环境变量或 `.env` 文件配置：

### 服务

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `APP_ENV` | `dev` | 运行环境 |
| `HOST` | `0.0.0.0` | 监听地址 |
| `PORT` | `8000` | 监听端口 |
| `EMBEDDED_SCHEDULER` | `true` | 是否内嵌调度引擎（开发模式） |

### Redis

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis 连接 URL |
| `REDIS_KEY_PREFIX` | `cs` | Key 前缀 |
| `DELAY_SHARD_COUNT` | `16` | 延迟队列分片数 |

### MySQL

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MYSQL_DSN` | `mysql+asyncmy://root:...@127.0.0.1:3306/coffee_scheduler` | 数据库连接串 |
| `DB_POOL_SIZE` | `20` | 连接池大小 |
| `DB_MAX_OVERFLOW` | `20` | 溢出连接数 |

### 调度引擎

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PUMP_INTERVAL_MS` | `50` | Delay Pump 轮询间隔 |
| `PUMP_BATCH_SIZE` | `200` | 单次搬运批量 |
| `LEASE_REAP_INTERVAL_MS` | `500` | 租约回收检查间隔 |
| `CRON_SCAN_INTERVAL_MS` | `1000` | Cron 扫描间隔 |
| `FLUSHER_BATCH_SIZE` | `500` | 刷盘批量大小 |
| `FLUSHER_FLUSH_INTERVAL_MS` | `100` | 刷盘间隔 |

### 任务策略

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DEFAULT_MAX_RETRIES` | `3` | 默认最大重试次数 |
| `DEFAULT_LEASE_SECONDS` | `60` | 默认租约时长 |
| `RETRY_BASE_DELAY_MS` | `1000` | 重试指数退避基数 |
| `MAX_RECEIPT_BYTES` | `16384` | 回执最大字节数（16KB） |
| `TASK_HASH_TTL_SECONDS` | `86400` | Redis 任务 Hash 过期时间 |
| `IDEMPOTENCY_TTL_SECONDS` | `86400` | 幂等键过期时间 |
| `WORKER_HEARTBEAT_TTL_SECONDS` | `60` | Worker 心跳 TTL |
| `PARTITION_PRECREATE_DAYS` | `7` | 预创建分区天数 |
| `MAX_QUERY_SPAN_DAYS` | `7` | 查询最大跨度天数 |

## 🏭 生产部署

### 分离 Scheduler 引擎

生产环境建议将 API 网关和 Scheduler 引擎分开部署：

```bash
# API 节点（无状态，可水平扩展）
EMBEDDED_SCHEDULER=false uvicorn app.main:app --workers 4

# Scheduler 节点（独立运行，通过 Redis 锁选主）
python -m app.worker_scheduler
```

### 任务生命周期

```
提交 → PENDING → [延迟等待] → READY → [Worker 拉取] → RUNNING
                                                        │
                                              ┌─────────┼──────────┐
                                              ▼         ▼          ▼
                                          SUCCEEDED   RETRIED     DEAD
                                                    (回到 PENDING)  (进入 DLQ)
```

### 死信队列处理

任务在以下情况进入死信队列：
1. **重试耗尽**：`retry_count >= max_retries` 时，nack 或租约超时会将任务移入 DLQ
2. **Worker 显式拒绝**：nack 请求中设置 `retryable: false`

处理死信任务：
```bash
# 查看队列死信数量
GET /api/v1/queues/{queue}/stats  → dlq_count

# 批量重跑
POST /api/v1/dlq/{queue}/retry

# 重跑指定任务
POST /api/v1/tasks/{task_id}/retry
```

## 🧪 测试

```bash
# 运行全部测试
pytest -q

# 运行代码检查
ruff check app/ tests/
```

测试使用内存后端（`fakeredis` + `aiosqlite`），无需启动 Redis 或 MySQL。

## 📂 项目结构

```
coffee-scheduler/
├── app/
│   ├── api/
│   │   ├── deps.py              # FastAPI 依赖注入
│   │   └── v1/
│   │       ├── tasks.py         # 任务 API 路由
│   │       └── cron_jobs.py     # Cron 规则 API 路由
│   ├── core/
│   │   ├── config.py            # Pydantic Settings 配置
│   │   ├── db.py                # SQLAlchemy 异步引擎 & 分区管理
│   │   ├── metrics.py           # Prometheus 自定义指标
│   │   ├── redis_client.py      # Redis 客户端 & Lua 脚本加载器
│   │   └── uuidv7.py            # UUIDv7 生成器
│   ├── lua/
│   │   ├── pull.lua             # 原子拉取任务
│   │   ├── ack.lua              # 原子确认完成
│   │   ├── nack.lua             # 原子失败重试/死信
│   │   ├── heartbeat.lua        # 原子续租
│   │   ├── pump.lua             # 延迟队列 → 就绪队列
│   │   └── reap_lease.lua       # 过期租约回收
│   ├── models/
│   │   └── tables.py            # SQLAlchemy ORM 表定义
│   ├── schemas/
│   │   ├── tasks.py             # 任务请求/响应 Schema
│   │   └── cron_jobs.py         # Cron 规则 Schema
│   ├── services/
│   │   ├── dispatcher.py        # 核心调度逻辑
│   │   ├── flusher.py           # 异步批量刷盘
│   │   └── scheduler_loop.py    # 调度引擎（Pump/Reaper/Cron）
│   ├── main.py                  # FastAPI 应用入口
│   └── worker_scheduler.py      # 独立 Scheduler 进程入口
├── tests/
│   ├── conftest.py              # 测试 Fixtures
│   ├── test_tasks_lifecycle.py  # 任务生命周期测试
│   └── test_retry_dlq_and_queries.py  # 重试、死信、查询测试
├── openspec/                    # 设计文档
├── docker-compose.yml           # 开发环境 Redis + MySQL
├── pyproject.toml               # 项目元数据 & 依赖
└── .env.example                 # 环境变量模板
```

## 📊 Redis 数据结构

| Key | 类型 | 说明 |
|-----|------|------|
| `cs:task:{task_id}` | Hash | 任务元数据（payload、status、retry 等） |
| `cs:idemp:{key}` | String | 幂等键（`SET NX EX`） |
| `cs:delay:{shard}` | ZSET | 延迟队列（score = 执行时间戳），16 片分片 |
| `cs:ready:{queue}` | ZSET | 就绪优先级队列（score = 优先级复合分值） |
| `cs:lease:{queue}` | ZSET | 执行中租约（score = 过期时间戳） |
| `cs:dlq:{queue}` | List | 死信队列 |

## 📄 License

MIT
