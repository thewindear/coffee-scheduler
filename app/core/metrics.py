from fastapi import Request
from prometheus_client import Counter, Gauge, Histogram

TASKS_SUBMITTED = Counter(
    "coffee_scheduler_tasks_submitted_total",
    "Total number of tasks submitted",
    ["queue"],
)
TASKS_PULLED = Counter(
    "coffee_scheduler_tasks_pulled_total",
    "Total number of tasks pulled by workers",
    ["queue"],
)
TASKS_ACKED = Counter(
    "coffee_scheduler_tasks_acked_total",
    "Total number of tasks acknowledged as succeeded",
    ["queue"],
)
TASKS_NACKED = Counter(
    "coffee_scheduler_tasks_nacked_total",
    "Total number of tasks nacked or reaped",
    ["queue", "outcome"],
)
QUEUE_READY_GAUGE = Gauge(
    "coffee_scheduler_queue_ready_size",
    "Current ready queue depth",
    ["queue"],
    multiprocess_mode="livemostrecent",
)
QUEUE_DLQ_GAUGE = Gauge(
    "coffee_scheduler_queue_dlq_size",
    "Current dead-letter queue depth",
    ["queue"],
    multiprocess_mode="livemostrecent",
)
FLUSHER_BATCH_HISTOGRAM = Histogram(
    "coffee_scheduler_flusher_batch_size",
    "Batch size flushed to MySQL",
)
FLUSHER_DURATION = Histogram(
    "coffee_scheduler_flusher_duration_seconds",
    "Time spent executing and committing a task record batch",
)


async def refresh_queue_metrics(request: Request) -> None:
    manager = request.app.state.redis_mgr
    async for raw_queue in manager.redis.sscan_iter(manager.active_queues_key(), count=100):
        queue = raw_queue.decode("utf-8") if isinstance(raw_queue, bytes) else raw_queue
        async with manager.redis.pipeline(transaction=False) as pipe:
            pipe.zcard(manager.ready_key(queue))
            pipe.llen(manager.dlq_key(queue))
            ready, dead = await pipe.execute()
        QUEUE_READY_GAUGE.labels(queue=queue).set(ready)
        QUEUE_DLQ_GAUGE.labels(queue=queue).set(dead)
