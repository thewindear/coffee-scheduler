"""Run concurrent scheduler throughput and persistence checks from the repository root."""

import argparse
import asyncio
import time
from uuid import uuid4

from sqlalchemy import select

from app.core.access import Grant, Identity
from app.core.config import Settings, get_settings
from app.main import create_app
from app.models.tables import TaskRecord
from app.schemas.tasks import TaskAckRequest, TaskPullRequest, TaskSubmitRequest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coffee Scheduler concurrent benchmark")
    parser.add_argument("--tasks", type=int, default=1000)
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--producers", type=int, default=8)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--queue", default="bench", help="Prefix for the isolated benchmark queue")
    parser.add_argument("--real", action="store_true", help="Use REDIS_URL and MYSQL_DSN")
    args = parser.parse_args()
    if min(args.tasks, args.producers, args.workers) < 1 or not 1 <= args.batch <= 100:
        parser.error(
            "tasks, producers and workers must be positive, and batch must be between 1 and 100"
        )
    if not 1 <= len(args.queue) <= 51:
        parser.error("queue prefix must contain 1 to 51 characters")
    return args


async def main() -> None:
    args = parse_args()
    queue = f"{args.queue}-{uuid4().hex[:12]}"
    redis = None
    if args.real:
        settings = get_settings().model_copy(update={"embedded_scheduler": False})
    else:
        from fakeredis.aioredis import FakeRedis

        settings = Settings(embedded_scheduler=False, mysql_dsn="sqlite+aiosqlite:///:memory:")
        redis = FakeRedis()
    app = create_app(settings=settings, redis_client=redis)

    try:
        async with app.router.lifespan_context(app):
            dispatcher = app.state.dispatcher
            identity = Identity(
                "local-benchmark",
                None,
                False,
                tuple(Grant(operation, queue=queue) for operation in ("submit", "pull", "monitor")),
            )

            async def produce(offset: int) -> list[str]:
                ids = []
                for i in range(offset, args.tasks, args.producers):
                    task = await dispatcher.submit_task(
                        TaskSubmitRequest(queue=queue, payload={"i": i}, priority=i % 10),
                        identity=identity,
                    )
                    ids.append(task.task_id)
                return ids

            async def consume(index: int) -> list[str]:
                worker_id = f"bench-{index}"
                pull = TaskPullRequest(queue=queue, worker_id=worker_id, batch_size=args.batch)
                ids = []
                while tasks := await dispatcher.pull_tasks(pull, identity=identity):
                    for task in tasks:
                        ack = TaskAckRequest(
                            queue=queue, worker_id=worker_id, lease_generation=task.lease_generation
                        )
                        await dispatcher.ack_task(task.task_id, ack, identity=identity)
                        ids.append(task.task_id)
                return ids

            started = time.perf_counter()
            submitted = await asyncio.gather(*(produce(i) for i in range(args.producers)))
            submit_seconds = time.perf_counter() - started

            started = time.perf_counter()
            consumed = await asyncio.gather(*(consume(i) for i in range(args.workers)))
            consume_seconds = time.perf_counter() - started

            started = time.perf_counter()
            await app.state.flusher.stop()
            drain_seconds = time.perf_counter() - started
            async with app.state.session_factory() as session:
                rows = (
                    await session.execute(
                        select(TaskRecord.task_id, TaskRecord.status, TaskRecord.receipt).where(
                            TaskRecord.queue == queue
                        )
                    )
                ).all()
            stats = await dispatcher.get_queue_stats(queue, identity)
    finally:
        if redis is not None:
            await redis.aclose()

    expected = {task_id for group in submitted for task_id in group}
    pulled = [task_id for group in consumed for task_id in group]
    if len(expected) != args.tasks or len(pulled) != args.tasks or set(pulled) != expected:
        raise RuntimeError("Concurrent pull lost or delivered duplicate tasks")
    if len(rows) != args.tasks or {row.task_id for row in rows} != expected:
        raise RuntimeError(
            f"Persisted task count or IDs differ from submissions: {len(rows)}/{args.tasks}"
        )
    if any(row.status != "SUCCEEDED" or not row.receipt for row in rows):
        raise RuntimeError("Persisted tasks have missing terminal status or receipts")
    if stats.ready_count or stats.running_count or stats.dlq_count:
        raise RuntimeError("Benchmark queue still has ready, leased or dead tasks")

    print(f"Queue: {queue}")
    print(f"Submit ({args.producers} producers): {args.tasks / submit_seconds:,.0f} tasks/s")
    print(f"Pull + ACK ({args.workers} workers): {args.tasks / consume_seconds:,.0f} tasks/s")
    print(f"Drain: {drain_seconds:.3f}s | persisted {len(rows)} SUCCEEDED tasks with receipts")


if __name__ == "__main__":
    asyncio.run(main())
