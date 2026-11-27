import asyncio
import logging
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from croniter import croniter
from fastapi import HTTPException
from redis.exceptions import LockNotOwnedError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.core.access import load_identity
from app.core.config import Settings
from app.core.db import ensure_task_records_partitions
from app.core.metrics import TASKS_NACKED
from app.core.redis_client import RedisLuaManager
from app.models.tables import CronJob, ServiceIdentity
from app.schemas.tasks import TaskSubmitRequest
from app.services.dispatcher import TaskDispatcher, _decode, _task_kind
from app.services.flusher import AsyncFlusher

logger = logging.getLogger(__name__)
CRON_LOCK_SECONDS = 5


class SchedulerEngine:
    """Background engine running Delay Pump, Lease Reaper, Cron Scanner, and Partition Maintainer."""

    def __init__(
        self,
        redis_mgr: RedisLuaManager,
        dispatcher: TaskDispatcher,
        flusher: AsyncFlusher,
        engine: AsyncEngine,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        delivery_dispatcher: Any | None = None,
        kafka_publisher: Any | None = None,
    ) -> None:
        self.redis_mgr = redis_mgr
        self.redis = redis_mgr.redis
        self.dispatcher = dispatcher
        self.flusher = flusher
        self.engine = engine
        self.session_factory = session_factory
        self.settings = settings
        self.delivery_dispatcher = delivery_dispatcher
        self.kafka_publisher = kafka_publisher
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def pump_once(self) -> int:
        now_ms = int(time.time() * 1000)
        script = self.redis_mgr.get_script("pump")
        total = 0
        for shard in range(self.settings.delay_shard_count):
            moved = await script(
                keys=[self.redis_mgr.delay_key(shard)],
                args=[self.redis_mgr.prefix, str(now_ms), str(self.settings.pump_batch_size)],
            )
            total += int(moved)
        return total

    async def reap_once(self) -> int:
        now_ms = int(time.time() * 1000)
        queues = await self.redis.smembers(self.redis_mgr.active_queues_key())
        if not queues:
            return 0
        script = self.redis_mgr.get_script("reap_lease")
        reaped_total = 0
        for q_raw in queues:
            queue = _decode(q_raw)
            rows = await script(
                keys=[self.redis_mgr.lease_key(queue), self.redis_mgr.dlq_key(queue)],
                args=[
                    self.redis_mgr.prefix,
                    queue,
                    str(now_ms),
                    str(self.settings.pump_batch_size),
                    str(self.settings.retry_base_delay_ms),
                    str(self.settings.task_hash_ttl_seconds),
                    str(self.settings.delay_shard_count),
                ],
            )
            for row in rows or []:
                reaped_total += 1
                tid = _decode(row[0])
                outcome = _decode(row[1])
                TASKS_NACKED.labels(queue=queue, outcome=outcome).inc()
                rc = int(_decode(row[2]))
                wid = _decode(row[3]) or "unknown"
                snapshot = await self.redis.hgetall(self.redis_mgr.task_key(tid))
                decoded_snap = {_decode(k): _decode(v) for k, v in (snapshot or {}).items()}
                task_kind = _task_kind(decoded_snap.get("task_kind", "normal"))
                timeout_receipt = {
                    "error_code": "LEASE_TIMEOUT",
                    "error_message": "Worker lease expired without heartbeat or ack",
                }
                if task_kind in ("source", "processing"):
                    async with self.session_factory.begin() as session:
                        db_row = await self.dispatcher._find_task_record(session, tid, lock=True)
                        if (
                            db_row is not None
                            and db_row.status not in ("SUCCEEDED", "DEAD")
                            and db_row.lease_generation <= int(row[4])
                        ):
                            db_row.lease_generation = int(row[4])
                            db_row.worker_id = wid
                            db_row.worker_service = _decode(row[6]) or None
                            db_row.retry_count = rc
                            db_row.receipt = timeout_receipt
                            if outcome == "DEAD":
                                db_row.status = "DEAD"
                                db_row.finished_at = now_ms
                            else:
                                db_row.status = "DELAYED"
                                db_row.scheduled_at = int(row[5])
                elif outcome == "DEAD":
                    await self.dispatcher._enqueue_terminal_flush(
                        tid,
                        snapshot,
                        "DEAD",
                        timeout_receipt,
                        wid,
                        now_ms,
                        retry_count=rc,
                    )
        return reaped_total

    async def scan_cron_once(self) -> int:
        lock_key = f"{self.redis_mgr.prefix}:lock:cron_scanner"
        lock = self.redis.lock(lock_key, timeout=CRON_LOCK_SECONDS)
        if not await lock.acquire(blocking=False):
            return 0

        finished = asyncio.Event()

        async def renew_lock() -> None:
            while not finished.is_set():
                try:
                    await asyncio.wait_for(finished.wait(), timeout=CRON_LOCK_SECONDS / 3)
                except TimeoutError:
                    await lock.reacquire()

        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(renew_lock())
                try:
                    return await self._scan_due_jobs()
                finally:
                    finished.set()
        finally:
            with suppress(LockNotOwnedError):
                await lock.release()

    async def _scan_due_jobs(self) -> int:
        now_ms = int(time.time() * 1000)
        now_dt = datetime.fromtimestamp(now_ms / 1000.0, tz=UTC)
        triggered = 0
        async with self.session_factory() as session:
            ids = (
                await session.scalars(
                    select(CronJob.id).where(
                        CronJob.enabled.is_(True), CronJob.next_run_at <= now_ms
                    )
                )
            ).all()
        for job_id in ids:
            async with self.session_factory.begin() as session:
                job = await session.get(CronJob, job_id, with_for_update=True)
                if job is None or not job.enabled or job.next_run_at > now_ms:
                    continue
                due_at = job.next_run_at
                next_dt = croniter(job.cron_expr, now_dt).get_next(datetime)
                job.next_run_at = int(next_dt.timestamp() * 1000)
                job.version += 1
                owner = (
                    await session.get(ServiceIdentity, job.owner_service, with_for_update=True)
                    if job.owner_service
                    else None
                )
                if owner is None:
                    continue
                identity = await load_identity(session, owner, lock=True)
                request = TaskSubmitRequest(
                    queue=job.queue,
                    business_id=job.business_id,
                    payload=job.payload,
                    priority=job.priority,
                    max_retries=job.max_retries,
                    lease_seconds=job.lease_seconds,
                )
                try:
                    route = await self.dispatcher.submission_route(
                        request, identity, session, use_home_business=False
                    )
                except HTTPException as exc:
                    if exc.status_code != 403:
                        raise
                    continue
                await self.dispatcher.submit_task(
                    request,
                    identity=identity,
                    cron_occurrence=(job.id, due_at),
                    route=route,
                )
                triggered += 1
        return triggered

    async def _run_periodic(self, name: str, interval_ms: int, coro_fn) -> None:
        interval_s = interval_ms / 1000.0
        while not self._stop_event.is_set():
            try:
                await coro_fn()
            except Exception:
                logger.exception("Scheduler loop %s error", name)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_s)
            except TimeoutError:
                pass

    async def _maintain_partitions(self) -> None:
        await ensure_task_records_partitions(
            self.engine, days_ahead=self.settings.partition_precreate_days
        )

    def start(self) -> None:
        if self._tasks:
            return
        self._stop_event.clear()
        self._tasks = [
            asyncio.create_task(
                self._run_periodic("pump", self.settings.pump_interval_ms, self.pump_once),
                name="sched-pump",
            ),
            asyncio.create_task(
                self._run_periodic("reap", self.settings.lease_reap_interval_ms, self.reap_once),
                name="sched-reap",
            ),
            asyncio.create_task(
                self._run_periodic(
                    "cron", self.settings.cron_scan_interval_ms, self.scan_cron_once
                ),
                name="sched-cron",
            ),
            asyncio.create_task(
                self._run_periodic("partitions", 3600_000, self._maintain_partitions),
                name="sched-partitions",
            ),
        ]
        if self.delivery_dispatcher is not None:
            self._tasks.append(
                asyncio.create_task(
                    self._run_periodic(
                        "delivery-dispatch",
                        self.settings.delivery_dispatch_interval_ms,
                        self.delivery_dispatcher.dispatch_once,
                    ),
                    name="sched-delivery-dispatch",
                )
            )
            self._tasks.append(
                asyncio.create_task(
                    self._run_periodic(
                        "delivery-reconcile",
                        self.settings.delivery_reconcile_interval_ms,
                        self.delivery_dispatcher.reconcile_once,
                    ),
                    name="sched-delivery-reconcile",
                )
            )
        if self.kafka_publisher is not None:
            self._tasks.append(
                asyncio.create_task(
                    self._run_periodic(
                        "kafka-publish",
                        self.settings.delivery_dispatch_interval_ms,
                        self.kafka_publisher.publish_pending_once,
                    ),
                    name="sched-kafka-publish",
                )
            )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
