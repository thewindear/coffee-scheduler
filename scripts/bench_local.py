"""Quick local throughput benchmark using FakeRedis + SQLite in-memory (no external DB needed).

Usage:
    python scripts/bench_local.py              # 默认 1000 任务
    python scripts/bench_local.py --tasks 5000 # 自定义数量
    python scripts/bench_local.py --batch 100  # 自定义 pull 批量
"""

import argparse
import asyncio
import sys
import time

import fakeredis.aioredis
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import Settings
from app.core.db import create_session_factory, init_db
from app.core.redis_client import RedisLuaManager
from app.schemas.tasks import TaskAckRequest, TaskPullRequest, TaskSubmitRequest
from app.services.dispatcher import TaskDispatcher
from app.services.flusher import AsyncFlusher


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coffee Scheduler local benchmark")
    parser.add_argument("--tasks", type=int, default=1000, help="Total tasks to submit (default: 1000)")
    parser.add_argument("--batch", type=int, default=50, help="Pull batch size (default: 50)")
    parser.add_argument("--queue", type=str, default="bench", help="Queue name (default: bench)")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    total_tasks = args.tasks
    batch_size = args.batch
    queue = args.queue

    print("☕ Coffee Scheduler Benchmark")
    print(f"   Tasks: {total_tasks}  |  Batch: {batch_size}  |  Queue: {queue}")
    print("-" * 50)

    cfg = Settings(embedded_scheduler=False, mysql_dsn="sqlite+aiosqlite:///:memory:")
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    engine = create_async_engine(cfg.mysql_dsn, echo=False)
    await init_db(engine)
    sf = create_session_factory(engine)
    flusher = AsyncFlusher(engine, sf, batch_size=500, flush_interval_ms=50)
    dispatcher = TaskDispatcher(
        RedisLuaManager(redis, delay_shard_count=cfg.delay_shard_count), flusher, sf, cfg
    )
    flusher.start()

    # --- Submit ---
    t0 = time.perf_counter()
    for i in range(total_tasks):
        await dispatcher.submit_task(
            TaskSubmitRequest(queue=queue, payload={"i": i}, priority=i % 10)
        )
    t_sub = time.perf_counter() - t0

    # --- Pull + Ack ---
    pulled_count = 0
    t1 = time.perf_counter()
    while pulled_count < total_tasks:
        batch = await dispatcher.pull_tasks(
            TaskPullRequest(queue=queue, worker_id="bench-w1", batch_size=batch_size)
        )
        if not batch:
            break
        pulled_count += len(batch)
        for item in batch:
            await dispatcher.ack_task(
                item.task_id, TaskAckRequest(queue=queue, worker_id="bench-w1")
            )
    t_proc = time.perf_counter() - t1

    # --- Cleanup ---
    await flusher.stop()
    await redis.aclose()
    await engine.dispose()

    # --- Report ---
    print(f"✅ Submit : {total_tasks:>6} tasks in {t_sub:.2f}s  → {total_tasks / t_sub:>8,.0f} tasks/s")
    print(f"✅ Process: {pulled_count:>6} tasks in {t_proc:.2f}s  → {pulled_count / t_proc:>8,.0f} tasks/s")

    if pulled_count < total_tasks:
        print(f"⚠️  Warning: only pulled {pulled_count}/{total_tasks} tasks", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
